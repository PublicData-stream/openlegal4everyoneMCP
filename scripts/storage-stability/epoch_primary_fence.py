"""One-use, phase-aware READ ONLY primary fences; no bind or admission port.

The native capture clock excludes archival and global SQL-closure observation.
Both happen afterward under the same original operation end. Unknown outcomes
retain descriptors and an operation hold for the caller/controller; no Ready,
release, retry, cancellation SQL or deadline extension is supplied here.
"""
from __future__ import annotations

import copy
import hashlib
import math
import os
from pathlib import Path
import select
import stat
import threading
import time
from types import MappingProxyType

import command_process as processes
import epoch_observation as observation
import initial_driver as driver
import initial_pg_pair as pair
import initial_role_prepare as roles
import interface
import inventory
import linux_backend as linux
from interface import Operation, Refused, freeze_authority, need
from inventory import canonical, decode, digest, pin, private_bytes, valid_uuid

PHASES = frozenset(('initial_epoch_observation', 'initial_epoch_install_verify',
                    'resume_owned_hold', 'strict_resume'))
SOURCES = frozenset(('epoch_primary_fence.py', 'epoch_observation.py', 'initial_pg_pair.py',
    'initial_role_prepare.py', 'command_process.py', 'interface.py', 'inventory.py',
    'linux_backend.py', 'initial_driver.py'))
MAX_RECORDS = 96
MAX_BYTES = 16 * 1024**2
MAX_NATIVE_OUTPUT = 65536
MAX_POLLS = 32
_KEY = object()
_ENV = ('PATH=/usr/local/bin:/usr/bin:/bin', 'HOME=/nonexistent', 'LANG=C', 'LC_ALL=C',
        'PGPASSFILE=/dev/null', 'PGSERVICEFILE=/dev/null')


def _shape(value, fields, code):
    need(type(value) is dict and set(value) == set(fields), code)


def _end(end):
    need(type(end) in (int, float) and math.isfinite(end) and time.monotonic() < end,
         'epoch_fence_original_deadline')


def _stamp(st):
    return tuple(getattr(st, name) for name in ('st_dev', 'st_ino', 'st_size',
        'st_mtime_ns', 'st_ctime_ns', 'st_uid', 'st_gid', 'st_mode', 'st_nlink'))


def _budget(op, records=MAX_RECORDS, size=MAX_BYTES):
    j = op.journal
    need(type(j.count) is int and type(j.bytes) is int and j.count >= 0 and j.bytes >= 0
         and j.count + records <= 4096 and j.bytes + size <= 512 * 1024**2,
         'epoch_fence_global_journal_budget')


def _scope(backend, op, common):
    need(type(backend) in (linux.LinuxBackend, driver.Backend)
         and type(common) is observation.NativeObservation and common.backend is backend
         and not any(callable(v) for v in vars(backend).values())
         and not any(callable(v) for v in vars(common).values()), 'epoch_fence_fixed_backend')
    need(type(op) is Operation and type(op.journal) is driver.journals.DualJournal
         and op.phase in PHASES and valid_uuid(op.operation_id)
         and type(op.inventory) is dict and type(op.authority) is MappingProxyType,
         'epoch_fence_fixed_operation')
    need(op.journal.operation == op.operation_id and op.journal.phase_paths
         and len(op.journal.phase_paths) == 2
         and all(path.name == op.phase for path, _ in op.journal.phase_paths),
         'epoch_fence_current_phase')
    need(os.geteuid() == 0 and threading.current_thread() is threading.main_thread()
         and len(list(Path('/proc/self/task').iterdir())) == 1
         and not processes._children(os.getpid()), 'epoch_fence_dedicated_root')


def _references(envelope, op):
    _shape(envelope, ('sources', 'inventory', 'authority'), 'epoch_fence_dependency_envelope')
    _shape(envelope['sources'], SOURCES, 'epoch_fence_source_registry')
    modules = {'epoch_primary_fence.py': Path(__file__), 'epoch_observation.py': Path(observation.__file__),
        'initial_pg_pair.py': Path(pair.__file__), 'initial_role_prepare.py': Path(roles.__file__),
        'command_process.py': Path(processes.__file__), 'interface.py': Path(interface.__file__),
        'inventory.py': Path(inventory.__file__), 'linux_backend.py': Path(linux.__file__),
        'initial_driver.py': Path(driver.__file__)}
    base = Path(__file__).resolve().parent
    for name, ref in envelope['sources'].items():
        _shape(ref, ('path', 'sha256'), 'epoch_fence_source_reference')
        need(Path(ref['path']) == base / name == modules[name].resolve(), 'epoch_fence_loaded_sibling')
        private_bytes(ref['path'], ref['sha256'])
    raw = driver._reference(envelope['inventory']); authraw = driver._reference(envelope['authority'])
    need(digest(raw) == op.inventory_sha256 and canonical(decode(raw)) == canonical(op.inventory)
         and digest(authraw) == op.authority_sha256
         and freeze_authority(decode(authraw)) == op.authority, 'epoch_fence_raw_operation_binding')
    auth = decode(authraw)
    graph = decode(raw).get('source_graph')
    need(type(graph) is dict and SOURCES <= set(graph) and len(graph) <= 1024
         and all(type(name) is str and 1 <= len(name) <= 1024 and pin(sha)
                 for name, sha in graph.items()), 'epoch_fence_global_source_graph')
    graph_sha = digest(canonical(graph))
    need(graph_sha == op.source_graph_sha256 == auth.get('source_graph_sha256'),
         'epoch_fence_source_graph_binding')
    need(all(envelope['sources'][name]['sha256'] == graph[name] for name in SOURCES),
         'epoch_fence_critical_source_graph_binding')
    need(auth.get('approved') is True
         and auth.get('epoch_certificate_observation_permitted') is True
         and auth.get('operation_id') == op.operation_id
         and auth.get('inventory_sha256') == op.inventory_sha256
         and type(auth.get('valid_until')) is int and time.time() < auth['valid_until'],
         'epoch_fence_observation_authority')
    return raw, authraw


class _Prepared:
    def __init__(self, key):
        need(key is _KEY, 'epoch_fence_private_context')
        self.key = key; self.state = 'PREPARING'; self.fds = []; self.records = []; self.window = None


class _Window:
    __slots__ = ('_key', '_prepared', '_proof_raw', '_records_raw', '_started', '_finished',
                 '_deadline', '_failure', '_archive_started')

    def __init__(self, key, prepared, proof, started, finished, deadline, failure):
        need(key is _KEY, 'epoch_fence_private_window')
        values = {'_key': key, '_prepared': prepared, '_proof_raw': canonical(proof),
            '_records_raw': tuple(canonical(row) for row in prepared.records), '_started': started,
            '_finished': finished, '_deadline': deadline, '_failure': failure, '_archive_started': False}
        for name, value in values.items(): object.__setattr__(self, name, value)

    def __setattr__(self, name, value):
        raise AttributeError('immutable_native_fence_window')

    key = property(lambda self: self._key)
    prepared = property(lambda self: self._prepared)
    proof = property(lambda self: decode(self._proof_raw))
    records = property(lambda self: [decode(raw) for raw in self._records_raw])
    started = property(lambda self: self._started)
    finished = property(lambda self: self._finished)
    deadline = property(lambda self: self._deadline)
    failure = property(lambda self: self._failure)
    archive_started = property(lambda self: self._archive_started)


class FenceUnknown(Refused):
    """The retained window may be archived; it can never become a valid fence."""
    def __init__(self, window):
        super().__init__('epoch_fence_native_unknown'); self.window = window


def _hold(p):
    p.state = 'UNKNOWN'; p.backend._epoch_primary_fence_unknown = True


def _same_op(p, op):
    _scope(p.backend, op, p.common)
    need(op.operation_id == p.op.operation_id and op.action == p.op.action and op.journal is p.op.journal
         and op.inventory_sha256 == p.op.inventory_sha256
         and op.authority_sha256 == p.op.authority_sha256
         and op.source_graph_sha256 == p.op.source_graph_sha256, 'epoch_fence_archive_operation')
    need(_references(p.dependencies, op) == (p.inventory_raw, p.authority_raw),
         'epoch_fence_operation_changed')
    need(canonical({'subject': p.subject, 'expected': p.expected, 'roles': p.role_names,
        'runtime': p.runtime_role, 'dependencies': p.dependencies, 'original_end': p.original_end}) == p.scope_raw,
        'epoch_fence_private_context_changed')


def _save(p, op, label, value):
    _end(p.original_end); op.seal(); _same_op(p, op)
    raw = canonical(value); _budget(op, 1, len(raw))
    name = op.phase + '-primary-fence-%04d-' % (op.journal.count + 1) + label + '.json'
    sha = op.journal.save_bytes(name, raw)
    need(op.journal.read_phase_bytes(op.phase, name, sha) == raw, 'epoch_fence_both_raw_ack')
    _end(p.original_end); op.seal()
    return {'phase': op.phase, 'name': name, 'sha256': sha}


def _args(p):
    return ['exec', '-i', '-u', '70', p.subject['primary']['container_id'], '/usr/bin/env', '-i', *_ENV,
        '/usr/local/bin/psql', '-X', '-w', '-h', '/var/run/postgresql', '-p', '5432', '-qAt',
        '-v', 'ON_ERROR_STOP=1', '-U', p.role_names['recovery_login'], '-d', 'openlegal']


def _query(p, op, kind):
    need(kind in ('catalog', 'sql-closure'), 'epoch_fence_fixed_query')
    _end(p.original_end); op.seal(); _same_op(p, op)
    before = p.common._native(op, p.subject, p.original_end, which='primary')
    sql = (roles.catalog_sql(p.role_names, p.runtime_role) if kind == 'catalog' else
        "SELECT jsonb_build_object('session_user',session_user,'current_user',current_user,"
        "'result',openlegal_checkpoint.client_sessions())")
    prefix = ("BEGIN READ ONLY; SET LOCAL statement_timeout='30s'; SET LOCAL lock_timeout='1s'; "
        "SET LOCAL transaction_timeout='35s'; SET LOCAL search_path=pg_catalog; "
        if kind == 'catalog' else
        "BEGIN READ ONLY; SET LOCAL statement_timeout='2s'; SET LOCAL lock_timeout='1s'; "
        "SET LOCAL transaction_timeout='5s'; SET LOCAL search_path=pg_catalog; ")
    stdin = (prefix + sql + '; ROLLBACK;\n').encode(); args = _args(p)
    raw = p.common._run(op, 'docker', args, p.original_end, stdin=stdin, timeout=31 if kind == 'catalog' else 3)
    need(type(raw) is bytes and len(raw) <= 2 * 1024**2, 'epoch_fence_metadata_bound')
    name = op.phase + '-command-%04d.json' % p.backend._sequence
    sha = op.journal.output_hashes().get(name); need(pin(sha), 'epoch_fence_query_command_missing')
    receipt = decode(op.journal.read_phase_bytes(op.phase, name, sha))
    dispatch = ['--config', str(p.backend._docker_config[0]), '--host', 'unix:///var/run/docker.sock']
    need(receipt.get('binary') == 'docker' and receipt.get('arguments_sha256') == digest(canonical(dispatch + args))
         and receipt.get('stdin_sha256') == digest(stdin) and receipt.get('stdout_sha256') == digest(raw)
         and receipt.get('stdout_bytes') == len(raw) and receipt.get('stdout_hex') == raw.hex(),
         'epoch_fence_query_raw_binding')
    _command_ok(receipt)
    ref = _save(p, op, kind, {'actual_command': {'phase': op.phase, 'name': name, 'sha256': sha},
        'raw_utf8': raw.decode('utf8')})
    value = decode(raw)
    need(p.common._native(op, p.subject, p.original_end, which='primary') == before,
         'epoch_fence_native_changed_after_query')
    _end(p.original_end); return value, ref


def _command_ok(row):
    need(type(row.get('actual_exit_code')) is int and row['actual_exit_code'] == 0
         and row.get('started') is True and row.get('owned_processes_closed') is True
         and all(row.get(k) is False for k in ('expired', 'interrupted', 'unexpected_descendants',
             'output_limit_exceeded')), 'epoch_fence_actual_command_not_closed_zero')


def prepare_primary_fence(backend, op, common, subject, expected, role_names, runtime_role,
                          original_end, dependency_pins):
    """Qualify read-only native/catalog authority outside the native two seconds."""
    _scope(backend, op, common); _end(original_end); op.seal()
    raw, authraw = _references(dependency_pins, op)
    observation._control(expected); observation._control(subject['control'])
    roles.expected_catalog(role_names, runtime_role)
    need(expected == subject['control'] and expected['managed_epoch'] == subject['managed_epoch']
         == op.inventory['managed_epoch'] and subject['host_boot_id'] == op.inventory['host']['boot_id']
         and subject['primary']['container_id'] == op.inventory['linux']['postgres']['primary']['container_id'],
         'epoch_fence_subject_binding')
    pg = op.inventory['linux']['postgres']
    observation._control(pg['binding'])
    need(pg['database'] == 'openlegal' and type(pg['uid']) is int and pg['uid'] == 70
         and pg['admin_user'] == role_names['recovery_login']
         and role_names['peer_login'] == expected['standby_role'], 'epoch_fence_fixed_observer')
    need(expected == pg['binding'] and subject['primary']['image_id'] == pg['primary']['image_id'],
         'epoch_fence_approved_primary_binding')
    if type(backend) is driver.Backend:
        _end(backend._driver_deadline); original_end = min(original_end, backend._driver_deadline)
    ends = getattr(backend, '_epoch_primary_fence_original_ends', {})
    need(type(ends) is dict and (op.operation_id not in ends or ends[op.operation_id] == original_end),
         'epoch_fence_deadline_renewal')
    need(not getattr(backend, '_epoch_primary_fence_unknown', False), 'epoch_fence_operation_held_unknown')
    handles = getattr(backend, '_epoch_primary_fence_handles', [])
    need(type(handles) is list and len(handles) < 16
         and all(type(row) is _Prepared and row.key is _KEY and row.state == 'ARCHIVED' for row in handles),
         'epoch_fence_pending_context')
    _budget(op)
    p = _Prepared(_KEY); p.backend = backend; p.op = op; p.common = common
    p.subject = copy.deepcopy(subject); p.expected = copy.deepcopy(expected)
    p.role_names = copy.deepcopy(role_names); p.runtime_role = runtime_role
    p.dependencies = copy.deepcopy(dependency_pins); p.inventory_raw = raw; p.authority_raw = authraw
    p.original_end = float(original_end); p.parent = os.getpid(); p.ticks = processes._identity(p.parent)
    p.driver_end = backend._driver_deadline if type(backend) is driver.Backend else None
    p.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    p.scope_raw = canonical({'subject': p.subject, 'expected': p.expected, 'roles': p.role_names,
        'runtime': p.runtime_role, 'dependencies': p.dependencies, 'original_end': p.original_end})
    ends = dict(ends); ends[op.operation_id] = p.original_end; backend._epoch_primary_fence_original_ends = ends
    backend._epoch_primary_fence_handles = list(handles) + [p]
    try:
        p.native = common._native(op, p.subject, p.original_end, which='primary')
        catalog, p.catalog_ref = _query(p, op, 'catalog')
        need(catalog['session_user'] == catalog['current_user'] == pg['admin_user'], 'epoch_fence_catalog_login')
        roles.check_catalog(catalog, p.role_names, p.runtime_role)
        anchor = common.anchors[(op.operation_id, p.subject['primary']['container_id'])]
        p.native_fds = []
        for fd in anchor['fds']:
            held = os.dup(fd); p.native_fds.append(held); p.fds.append(held)
        p.native_ids = [observation._fd_identity(fd) for fd in p.native_fds]
        need(len(p.native_fds) == 4, 'epoch_fence_native_anchor')
        p.pidfd = os.pidfd_open(p.subject['primary']['process']['pid']); p.fds.append(p.pidfd)
        config, identity = backend._docker_config
        need(config == Path('/tmp/openlegal-storage-docker-client-' + op.operation_id), 'epoch_fence_private_context_path')
        p.config = config; p.config_fd = observation._directory(config); p.fds.append(p.config_fd)
        p.config_identity = identity
        binding = op.inventory['linux']['binaries']['docker']; p.binary_path = binding['path']; p.binary_hash = binding['sha256']
        linux.secure_path(p.binary_path)
        p.binary_fd = os.open(p.binary_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC); p.fds.append(p.binary_fd)
        info = os.fstat(p.binary_fd)
        need(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022
             and info.st_nlink == 1 and 0 < info.st_size <= 256 * 1024**2 and pin(p.binary_hash),
             'epoch_fence_binary_metadata')
        p.binary_stamp = _stamp(info); p.state = 'PREPARED'
        _light(p, op, p.original_end); _hash_binary(p, p.original_end); _budget(op); op.seal()
        return p
    except BaseException:
        _hold(p); raise


def _light(p, op, end):
    """Local descriptor/private-file checks only; never op.seal or a journal walk."""
    _end(end); _end(p.original_end)
    need(os.getpid() == p.parent and processes._identity(p.parent) == p.ticks
         and Path('/proc/sys/kernel/random/boot_id').read_text().strip() == p.boot,
         'epoch_fence_same_process')
    _same_op(p, op)
    need(p.backend._epoch_primary_fence_original_ends.get(op.operation_id) == p.original_end,
         'epoch_fence_original_end_changed')
    if type(p.backend) is driver.Backend:
        need(p.backend._driver_deadline == p.driver_end, 'epoch_fence_driver_deadline_changed')
        _end(p.backend._driver_deadline)
    process = p.subject['primary']['process']
    need(process['boot_id'] == p.boot and not select.select([p.pidfd], [], [], 0)[0]
         and processes._identity(process['pid']) == process['start_ticks'], 'epoch_fence_primary_lifetime')
    need([observation._fd_identity(fd) for fd in p.native_fds] == p.native_ids, 'epoch_fence_held_native_changed')
    fresh = []
    try:
        fresh.append(observation._directory(p.subject['primary']['data_root']['path']))
        fresh.append(observation._directory(Path(p.subject['primary']['data_root']['path']) / 'pg_wal'))
        fresh.append(linux.LinuxBackend._container_directory(process['pid'], p.subject['primary']['data_directory']))
        fresh.append(linux.LinuxBackend._container_directory(process['pid'], p.subject['primary']['data_directory'] + '/pg_wal'))
        need([observation._fd_identity(fd) for fd in fresh] == p.native_ids, 'epoch_fence_current_native_changed')
    finally:
        for fd in fresh: os.close(fd)
    info = os.fstat(p.config_fd); current = p.config.lstat()
    need((info.st_dev, info.st_ino) == (current.st_dev, current.st_ino) == p.config_identity
         and info.st_uid == info.st_gid == current.st_uid == current.st_gid == 0
         and stat.S_IMODE(info.st_mode) == stat.S_IMODE(current.st_mode) == 0o700
         and not os.listdir(p.config_fd) and not os.listdir(p.config), 'epoch_fence_private_context_changed')
    need(_stamp(os.fstat(p.binary_fd)) == p.binary_stamp == _stamp(Path(p.binary_path).lstat()),
         'epoch_fence_binary_changed')
    _end(end)


def _hash_binary(p, end):
    os.lseek(p.binary_fd, 0, os.SEEK_SET); h = hashlib.sha256()
    while block := os.read(p.binary_fd, 1024**2): _end(end); h.update(block)
    need(h.hexdigest() == p.binary_hash and _stamp(os.fstat(p.binary_fd)) == p.binary_stamp,
         'epoch_fence_binary_hash_changed'); _end(end)


def _capture_sql(kind):
    need(kind in ('proof', 'flush'), 'epoch_fence_read_only_kind')
    functions = ('capture_prefix', 'observe_flush') if kind == 'proof' else ('observe_flush',)
    prefix = ("BEGIN READ ONLY; SET LOCAL statement_timeout='2s'; SET LOCAL lock_timeout='1s'; "
        "SET LOCAL transaction_timeout='5s'; SET LOCAL search_path=pg_catalog; SET LOCAL TIME ZONE 'UTC'; "
        "SET LOCAL DateStyle='ISO,YMD'; SET LOCAL client_encoding='UTF8'; ")
    return (prefix + ' '.join("SELECT jsonb_build_object('session_user',session_user,'current_user',current_user,"
        "'read_only',current_setting('transaction_read_only'),'result',openlegal_replication." + name + '());'
        for name in functions) + ' ROLLBACK;\n').encode()


def _execute(p, kind, end):
    _light(p, p.op, end); _hash_binary(p, end)
    args = _args(p); dispatch = ['--config', str(p.config), '--host', 'unix:///var/run/docker.sock']
    stdin = _capture_sql(kind)
    record = {'kind': 'openlegal_epoch_primary_native_sql', 'query_kind': kind,
        'binary_sha256': p.binary_hash, 'arguments_sha256': digest(canonical(dispatch + args)),
        'stdin_sha256': digest(stdin), 'native_started_at': time.monotonic(), 'native_finished_at': None,
        'native_absolute_deadline': end, 'original_absolute_deadline': p.original_end,
        'actual_exit_code': None, 'started': None, 'owned_processes_closed': None}
    p.records.append(record)
    result = processes.execute([p.binary_path, *dispatch, *args], executable_fd=p.binary_fd,
        stdin=stdin, env=linux.ENV, deadline=end, max_output=MAX_NATIVE_OUTPUT)
    record.update({key: getattr(result, key) for key in ('actual_exit_code', 'started', 'owned_processes_closed',
        'expired', 'interrupted', 'unexpected_descendants', 'output_limit_exceeded', 'elapsed_seconds')})
    record.update(native_finished_at=time.monotonic(), stdout_hex=result.stdout.hex(),
        stdout_sha256=digest(result.stdout), stdout_bytes=len(result.stdout),
        stderr_sha256=digest(result.stderr), stderr_bytes=len(result.stderr))
    _command_ok(record); _end(end)
    values = [decode(row) for row in result.stdout.splitlines()]
    need(len(values) == (2 if kind == 'proof' else 1), 'epoch_fence_fixed_result_count')
    peers = []
    for row in values:
        _shape(row, ('session_user', 'current_user', 'read_only', 'result'), 'epoch_fence_result_envelope')
        need(row['session_user'] == row['current_user'] == p.role_names['recovery_login']
             and row['read_only'] == 'on', 'epoch_fence_query_identity')
        peer = pair._peer(row['result'], p.expected)
        need(peer['postmaster_started_at'] == p.subject['primary']['postmaster_started_at'],
             'epoch_fence_postmaster_changed')
        peers.append(peer)
    return peers


def capture_primary_fence(prepared):
    """Capture once; every native command/poll/actual0 shares ONE <=2s clock."""
    need(type(prepared) is _Prepared and prepared.key is _KEY and prepared.state == 'PREPARED'
         and prepared in getattr(prepared.backend, '_epoch_primary_fence_handles', ())
         and not getattr(prepared.backend, '_epoch_primary_fence_unknown', False), 'epoch_fence_capture_once')
    p = prepared; p.op.seal(); _budget(p.op); _end(p.original_end)
    p.state = 'CAPTURING'; started = time.monotonic(); end = min(p.original_end, started + 2)
    try:
        before, after = _execute(p, 'proof', end); polls = 0
        while pair._lsn(after['flush_lsn']) < pair._lsn(before['prefix_lsn']):
            _end(end); polls += 1; need(polls <= MAX_POLLS, 'epoch_fence_flush_poll_bound')
            need(all(before[k] == after[k] for k in before if k not in ('prefix_lsn', 'flush_lsn')),
                 'epoch_fence_peer_changed_during_poll')
            time.sleep(min(.02, max(0, end - time.monotonic())))
            after = _execute(p, 'flush', end)[0]
        proof = pair._proof(before, after, p.expected)
        _light(p, p.op, end); _end(end)
        window = _Window(_KEY, p, proof, started, time.monotonic(), end, None)
        p.window = window; p.state = 'CAPTURED'; return window
    except BaseException as error:
        _hold(p); window = _Window(_KEY, p, None, started, time.monotonic(), end, type(error).__name__)
        p.window = window; raise FenceUnknown(window) from error


def archive_primary_fence(op, window):
    """Archive historical raw results, then prove global SQL closure; no ACK."""
    need(type(window) is _Window and window.key is _KEY and window.prepared.window is window
         and not window.archive_started, 'epoch_fence_archive_once')
    p = window.prepared; object.__setattr__(window, '_archive_started', True)
    try:
        _end(p.original_end); op.seal(); _same_op(p, op)
        refs = [_save(p, op, 'native', record) for record in window.records]
        value, closure = _query(p, op, 'sql-closure')
        need(value == {'session_user': p.role_names['recovery_login'],
            'current_user': p.role_names['recovery_login'], 'result': {'sessions': []}},
            'epoch_fence_global_sql_not_closed')
        ref = _save(p, op, 'historical', {'schema_version': 1, 'kind': 'openlegal_epoch_primary_fence',
            'operation_id': op.operation_id, 'native_phase': p.op.phase, 'archive_phase': op.phase,
            'inventory_sha256': op.inventory_sha256, 'authority_sha256': op.authority_sha256,
            'source_graph_sha256': op.source_graph_sha256, 'native_started_at': window.started,
            'native_finished_at': window.finished, 'native_absolute_deadline': window.deadline,
            'original_absolute_deadline': p.original_end, 'native_command_refs': refs,
            'native_proof_valid': window.failure is None, 'failure_type': window.failure,
            'proof': window.proof, 'global_sql_closure': closure,
            'observation_status': 'historical_native_window_not_current_after_archive',
            'public_ack': False, 'ready': False})
        op.seal(); _end(p.original_end)
        if window.failure is not None: raise FenceUnknown(window)
        p.state = 'ARCHIVED'; return ref
    except BaseException:
        _hold(p); raise
