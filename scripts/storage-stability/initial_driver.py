"""Live, one-use legacy -> initial migration orchestration; no standalone CLI.

The ROOT caller retains this backend, SharedLock and prefork writer descriptors.
Only fresh operations created here are accepted. Source-owned children execute
the two frozen phases. No roles, strict seed, Ready or provider action exists.
A journal completion is never substituted for waitpid/descendant closure.
"""
from __future__ import annotations

import array
import copy
import ctypes
from dataclasses import dataclass
import fcntl
import math
import os
from pathlib import Path
import re
import select
import signal
import socket
import stat
import sys
import threading
import time

import command_process as processes
import initial_migration as migration
import journal as journals
import legacy_prepare as legacy
import linux_backend as linux
from interface import Operation, Refused, SOURCE_FILES, freeze_authority, need
from inventory import canonical, decode, digest, pin, private_bytes, valid_uuid
from inventory import private_parents

SOURCES = SOURCE_FILES | {'legacy_prepare.py', 'initial_migration.py', 'initial_driver.py'}
MAX_HANDOFF = 4 * 1024**2
MAX_FDS = 3
PHASES = (legacy.PHASE, migration.PHASE)
_SERIAL = threading.Lock()
GUARDS = ('_legacy_prepare_guard_fd', '_initial_migration_guard_fd',
          '_initial_migration_cgroup_fd')


class Backend(linux.LinuxBackend):
    """The sole native adapter caps every command at the parent's original end."""
    def __init__(self):
        super().__init__()
        self._driver_deadline = None
        self._initial_driver_clock = None
        self._initial_driver_clock_started = None
        self._initial_driver_clock_attempted = False

    def _run(self, op, binary, args, *, stdin=None, timeout=30,
             record=True, deadline=None):
        end = self._driver_deadline
        _check_end(self)
        if self._docker_config is not None:
            _docker_binding(self, op.operation_id)
        return super()._run(op, binary, args, stdin=stdin, timeout=timeout, record=record,
                            deadline=min(end, deadline) if deadline is not None else end)


def _reference(value):
    need(type(value) is dict and set(value) == {'path', 'sha256'}
         and pin(value['sha256']), 'driver_private_reference')
    return private_bytes(value['path'], value['sha256'])


def _source_seal(graph):
    need(type(graph) is dict and set(graph) == SOURCES and all(pin(v) for v in graph.values()),
         'driver_exact_source_graph')
    base = Path(__file__).resolve().parent
    for name, sha in graph.items():
        private_bytes(base / name, sha)


def _load(inventory_ref, authority_ref):
    raw = _reference(inventory_ref); config = decode(raw)
    need(type(config) is dict and set(config) == {'schema_version', 'kind', 'operation_id',
         'legacy_inventory', 'initial_template', 'total_deadline_secs', 'source_graph'}
         and type(config['schema_version']) is int and config['schema_version'] == 1
         and config['kind'] == 'openlegal_initial_driver_inventory'
         and valid_uuid(config['operation_id']) and type(config['total_deadline_secs']) is int
         and 1 <= config['total_deadline_secs'] <= 2400, 'driver_inventory')
    _source_seal(config['source_graph'])
    invraw = _reference(config['legacy_inventory']); inv = decode(invraw)
    template_raw = _reference(config['initial_template']); template = decode(template_raw)
    need(type(inv) is dict and inv['source_graph'] == config['source_graph']
         and valid_uuid(inv['host']['boot_id']) and 'initial_migration' not in inv,
         'driver_legacy_inventory')
    fields = {'primary_process', 'socket_directory', 'socket', 'candidate', 'compiled_manifest',
              'prerequisites', 'producer_source', 'legacy_binding', 'deadline_secs'}
    need(type(template) is dict and set(template) == fields
         and all(template[k] is None for k in ('primary_process', 'socket_directory', 'socket',
                                              'prerequisites', 'legacy_binding')),
         'driver_initial_null_placeholders')
    producer = template['producer_source']
    need(producer == {'path': str(Path(__file__).resolve().parent / 'legacy_prepare.py'),
                      'sha256': config['source_graph']['legacy_prepare.py']},
         'driver_actual_producer_source')
    authraw = _reference(authority_ref); authority = decode(authraw)
    required = {'schema_version', 'kind', 'approved', 'operation_id', 'inventory_sha256',
                'source_graph_sha256', 'legacy_inventory_sha256', 'initial_template_sha256',
                'host_boot_id', 'valid_until', 'legacy_fence_snapshot_permitted',
                'initial_migration_permitted', 'preserve_originals', 'automatic_retry',
                'role_provisioning_permitted', 'strict_seed_permitted', 'ready_permitted',
                'provider_calls_permitted'}
    need(type(authority) is dict and set(authority) == required
         and type(authority['schema_version']) is int and authority['schema_version'] == 1
         and authority['kind'] == 'openlegal_initial_driver_authority'
         and authority['approved'] is True and authority['operation_id'] == config['operation_id']
         and authority['inventory_sha256'] == digest(raw)
         and authority['source_graph_sha256'] == digest(canonical(config['source_graph']))
         and authority['legacy_inventory_sha256'] == digest(invraw)
         and authority['initial_template_sha256'] == digest(template_raw)
         and authority['host_boot_id'] == inv['host']['boot_id']
         and type(authority['valid_until']) is int and time.time() < authority['valid_until']
         and all(authority[k] is True for k in ('legacy_fence_snapshot_permitted',
                                               'initial_migration_permitted', 'preserve_originals'))
         and all(authority[k] is False for k in ('automatic_retry', 'role_provisioning_permitted',
                    'strict_seed_permitted', 'ready_permitted', 'provider_calls_permitted')),
         'driver_authority')
    return config, inv, template, authority, raw, invraw, authraw


@dataclass(frozen=True, slots=True)
class _Clock:
    """In-memory timing context, never authority or an external ROOT-exit proof."""
    backend: Backend
    shared_lock: journals.SharedLock
    lock_fd: int
    inventory: tuple
    authority: tuple
    operation_id: str
    duration: int
    started: float
    end: float
    creator_pid: int
    creator_ticks: str
    boot_id: str


def _fresh_backend(backend, shared_lock):
    need(type(backend) is Backend and type(shared_lock) is journals.SharedLock
         and os.geteuid() == 0 and threading.current_thread() is threading.main_thread()
         and len(list(Path('/proc/self/task').iterdir())) == 1
         and not processes._children(os.getpid()), 'driver_native_dedicated_parent')
    need(not any(callable(value) for value in vars(backend).values())
         and backend._driver_deadline is None and not hasattr(backend, '_initial_driver_journal')
         and not backend._initial_driver_clock_attempted, 'driver_no_overrides_or_reuse')
    need(backend._docker_config is None and not backend._owned_sql_targets
         and not backend._pg_directory_fds, 'driver_backend_not_fresh')


def _input_binding(inv, shared_lock):
    need(Path('/proc/sys/kernel/random/boot_id').read_text().strip() == inv['host']['boot_id'],
         'driver_current_boot')
    need(shared_lock.path == Path(inv['shared_lock']['path'])
         and shared_lock.identity == inv['shared_lock']['identity'], 'driver_shared_lock_binding')
    shared_lock.check()


def _clock_reference(ref):
    need(type(ref) is dict and set(ref) == {'path', 'sha256'}
         and type(ref['path']) is str and pin(ref['sha256']), 'driver_private_reference')
    return (ref['path'], ref['sha256'])


def _end(started, duration):
    need(type(started) is float and math.isfinite(started) and started >= 0
         and type(duration) is int and 1 <= duration <= 2400, 'driver_clock_bounds')
    end = started + duration
    need(math.isfinite(end), 'driver_clock_bounds')
    return end


def prepare_clock(backend, shared_lock, inventory_ref, authority_ref):
    """Anchor before qualification/fork; no journal, native command or approval.

    The caller retains these exact objects. A future supervisor may fork one
    direct ROOT child with them; this clock does not provide cross-fork atomic
    admission, FD escrow, or proof that such a supervisor exists.
    """
    started = time.monotonic()
    _fresh_backend(backend, shared_lock)
    need(backend._initial_driver_clock_started is None, 'driver_clock_already_prepared')
    # Even a failed qualification cannot renew this backend's original window.
    backend._initial_driver_clock_started = started
    config, inv, _, _, _, _, _ = _load(inventory_ref, authority_ref)
    _input_binding(inv, shared_lock)
    end = _end(started, config['total_deadline_secs'])
    pid = os.getpid(); ticks = processes._identity(pid)
    need(type(ticks) is str and ticks.isdecimal(), 'driver_clock_creator')
    clock = _Clock(backend, shared_lock, shared_lock.fd,
                   _clock_reference(inventory_ref), _clock_reference(authority_ref),
                   config['operation_id'], config['total_deadline_secs'], started, end,
                   pid, ticks, inv['host']['boot_id'])
    need(time.monotonic() < end, 'driver_original_deadline')
    backend._initial_driver_clock = clock
    return clock


def _consume_clock(backend, shared_lock, clock, inventory_ref, authority_ref, now):
    need(type(clock) is _Clock and backend._initial_driver_clock is clock
         and clock.backend is backend and clock.shared_lock is shared_lock,
         'driver_clock_binding')
    # Burn this process's attempt before further IO. This flag is not a shared
    # cross-fork claim: the future fixed supervisor must create only one ROOT.
    backend._initial_driver_clock_attempted = True
    need(clock.inventory == _clock_reference(inventory_ref)
         and clock.authority == _clock_reference(authority_ref)
         and clock.lock_fd == shared_lock.fd, 'driver_clock_inputs_changed')
    expected_end = _end(clock.started, clock.duration)
    need(type(clock.end) is float and math.isfinite(clock.end) and clock.end == expected_end
         and type(now) is float and math.isfinite(now) and clock.started <= now < clock.end,
         'driver_clock_bounds')
    need(Path('/proc/sys/kernel/random/boot_id').read_text().strip() == clock.boot_id
         and (os.getpid() == clock.creator_pid or os.getppid() == clock.creator_pid)
         and processes._identity(clock.creator_pid) == clock.creator_ticks,
         'driver_clock_creator')
    shared_lock.check()
    return clock.end


def _check_end(backend):
    end = backend._driver_deadline
    clock = getattr(backend, '_initial_driver_clock', None)
    need(type(end) is float and math.isfinite(end) and time.monotonic() < end,
         'driver_original_deadline')
    if clock is not None:
        need(type(clock) is _Clock and clock.backend is backend and end == clock.end,
             'driver_clock_deadline_changed')


def _writer_fds(backend):
    need(backend._cgroup_fds, 'driver_prefork_cgroups')
    for fd, row in backend._cgroup_fds.values():
        st = os.fstat(fd)
        need(stat.S_ISDIR(st.st_mode) and (st.st_dev, st.st_ino) == (row['dev'], row['ino']),
             'driver_parent_cgroup_fd_changed')


def _docker_binding(backend, operation_id):
    path = Path('/tmp/openlegal-storage-docker-client-' + operation_id)
    need(backend._docker_config is not None and hasattr(backend, '_driver_docker_config_fd'),
         'driver_parent_docker_config_missing')
    actual, identity = backend._docker_config
    private_parents(path)
    held = os.fstat(backend._driver_docker_config_fd); current = path.lstat()
    parent = Path('/tmp').lstat()
    need((parent.st_dev, parent.st_ino, parent.st_uid, parent.st_gid,
          stat.S_IMODE(parent.st_mode)) == backend._driver_docker_parent_identity,
         'driver_docker_parent_rebound')
    need(actual == path and stat.S_ISDIR(current.st_mode)
         and current.st_uid == current.st_gid == held.st_uid == held.st_gid == 0
         and stat.S_IMODE(current.st_mode) == stat.S_IMODE(held.st_mode) == 0o700
         and (held.st_dev, held.st_ino) == (current.st_dev, current.st_ino) == identity
         and not os.listdir(backend._driver_docker_config_fd) and not os.listdir(path),
         'driver_parent_docker_config_changed')
    _check_end(backend)


def _create_docker_client(backend, op):
    """Create only the source-fixed empty client directory in ROOT, never Docker."""
    need(backend._docker_config is None and not hasattr(backend, '_driver_docker_config_fd'),
         'driver_docker_config_reuse')
    op.seal(); _check_end(backend)
    path = Path('/tmp/openlegal-storage-docker-client-' + op.operation_id)
    private_parents(path)
    parent = os.open('/tmp', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(parent)
        os.mkdir(path.name, 0o700, dir_fd=parent); os.fsync(parent)
        _check_end(backend)
        fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
        backend._driver_docker_config_fd = fd
        st = os.fstat(fd); backend._docker_config = (path, (st.st_dev, st.st_ino))
        backend._driver_docker_parent_identity = (before.st_dev, before.st_ino,
            before.st_uid, before.st_gid, stat.S_IMODE(before.st_mode))
        now = Path('/tmp').lstat()
        need((before.st_dev, before.st_ino) == (now.st_dev, now.st_ino), 'driver_tmp_rebound')
        _docker_binding(backend, op.operation_id)
        backend._artifact(op, 'parent-docker-client-config', {
            'path': str(path), 'identity': {'dev': st.st_dev, 'ino': st.st_ino},
            'uid': 0, 'gid': 0, 'mode': '0700', 'entries': [], 'parent_pid': os.getpid()})
        op.seal(); _check_end(backend)
    finally:
        os.close(parent)


class _Leader:
    """waitpid exclusively owns this fork leader's actual status."""
    def __init__(self, pid):
        self.pid = pid
        self.returncode = None

    def poll(self):
        if self.returncode is None:
            found, status = os.waitpid(self.pid, os.WNOHANG)
            if found:
                need(found == self.pid, 'driver_wrong_waitpid')
                self.returncode = os.waitstatus_to_exitcode(status)
        return self.returncode


def _child_parent(parent, pidfd):
    # Linux delivers TERM if the ROOT parent dies, including during a syscall.
    libc = ctypes.CDLL(None, use_errno=True)
    need(libc.prctl(1, signal.SIGTERM, 0, 0, 0) == 0, 'driver_parent_death_signal')
    need(os.getppid() == parent and not select.select([pidfd], [], [], 0)[0],
         'driver_parent_lost')


def _phase_index(backend, op):
    need(op.journal.phase_paths is not None, 'driver_live_phase')
    need(not backend._owned_sql_targets and not backend._pg_directory_fds,
         'driver_unexpected_backend_state')
    rows = [{'name': name, 'sha256': row['sha256'], 'bytes': row['bytes']}
            for name, row in sorted(op.journal.artifacts.items())]
    fds = [(name, getattr(backend, name)) for name in GUARDS if hasattr(backend, name)]
    need(len(fds) <= MAX_FDS, 'driver_fd_bound')
    return {'phase': op.phase, 'operation_id': op.operation_id, 'artifacts': rows,
            'sequence': backend._sequence, 'backend_artifacts': backend._artifacts,
            'writer_processes': list(backend._writer_processes.values()),
            'writer_cgroups': {path: row for path, (fd, row) in backend._cgroup_fds.items()},
            'legacy_mount_sequence': getattr(backend, '_legacy_prepare_mount_sequence', 0),
            'guards': [{'field': name, 'dev': os.fstat(fd).st_dev,
                        'ino': os.fstat(fd).st_ino} for name, fd in fds]}, [fd for _, fd in fds]


def _child(backend, op, sock, parent, parentfd):
    status = 1
    try:
        def interrupted(number, frame):
            raise Refused('driver_child_interrupted')
        for number in (signal.SIGINT, signal.SIGTERM):
            signal.signal(number, interrupted)
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        _child_parent(parent, parentfd)
        need(sock.recv(3) == b'go\n', 'driver_start_gate')
        _child_parent(parent, parentfd); _check_end(backend); op.seal()
        if op.phase == legacy.PHASE:
            legacy.run(backend, op)
        elif op.phase == migration.PHASE:
            migration.run(backend, op)
        else:
            raise Refused('driver_fixed_phase')
        _check_end(backend); status = 0
    except BaseException as exc:
        try:
            backend._artifact(op, 'driver-child-failed', {
                'status': 'FAILED_OR_UNKNOWN', 'error_type': type(exc).__name__,
                'code': str(exc) if isinstance(exc, Refused) else None,
                'actual_exit_not_yet_observable': True, 'automatic_retry': False})
        except BaseException:
            pass
        status = 1
    finally:
        try:
            index, fds = _phase_index(backend, op); raw = canonical(index)
            need(len(raw) <= MAX_HANDOFF, 'driver_index_bound')
            ancillary = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', fds))] if fds else []
            sent = sock.sendmsg([raw[:32768]], ancillary)
            need(sent > 0, 'driver_index_send')
            sock.sendall(raw[sent:])
        except BaseException:
            status = 1
        sock.close(); os.close(parentfd)
        os._exit(status)


def _fork_phase(backend, op):
    """Only fixed source phases; no externally supplied callback or PID."""
    need(op.phase in PHASES and threading.current_thread() is threading.main_thread()
         and len(list(Path('/proc/self/task').iterdir())) == 1
         and not processes._children(os.getpid()), 'driver_dedicated_parent')
    need(_SERIAL.acquire(False), 'driver_parent_concurrent')
    oldsub = None; handlers = {}; leader = owned = None
    parentfd = None; channel = child = None; received = []; raw = bytearray()
    ended = False; interrupted = []; stopping = None; unexpected = False
    try:
        _check_end(backend); _writer_fds(backend); op.seal()
        # Establish wait-status ownership BEFORE fork, never restore until reaped.
        handlers[signal.SIGCHLD] = signal.getsignal(signal.SIGCHLD)
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        def interrupt(number, frame):
            interrupted.append(number)
        for number in (signal.SIGINT, signal.SIGTERM):
            handlers[number] = signal.getsignal(number); signal.signal(number, interrupt)
        oldsub = processes._subreaper(); processes._subreaper(1)
        parentfd = os.pidfd_open(os.getpid(), 0)
        channel, child = socket.socketpair()
        parent = os.getpid(); pid = os.fork()
        if pid == 0:
            channel.close()
            _child(backend, op, child, parent, parentfd)
        leader = _Leader(pid)
        child.close(); child = None; os.close(parentfd); parentfd = None
        owned = processes._Ownership(leader)
        witness = {'pid': pid, 'start_ticks': owned.leader_key[1],
                   'boot_id': op.inventory['host']['boot_id']}
        channel.sendall(b'go\n'); channel.setblocking(False)
        while True:
            owned.capture(); owned.reap()
            if leader.returncode is not None and any(e.pid != pid for e in owned.living()):
                unexpected = True
            late = time.monotonic() >= backend._driver_deadline
            if (late or interrupted or unexpected) and stopping is None:
                stopping = time.monotonic(); owned.signal_all(signal.SIGTERM)
            if stopping is not None and time.monotonic() - stopping >= processes.TERM_GRACE:
                owned.signal_all(signal.SIGKILL)
            if not ended and select.select([channel], [], [], .02)[0]:
                body, ancillary, flags, _ = channel.recvmsg(32768,
                    socket.CMSG_SPACE(MAX_FDS * array.array('i').itemsize), socket.MSG_CMSG_CLOEXEC)
                for level, kind, payload in ancillary:
                    need(level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS, 'driver_ancillary_type')
                    values = array.array('i'); values.frombytes(payload[:len(payload) - len(payload) % values.itemsize])
                    received.extend(values)
                    need(len(received) <= MAX_FDS, 'driver_fd_bound')
                need(not flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC), 'driver_index_truncated')
                raw.extend(body); need(len(raw) <= MAX_HANDOFF, 'driver_index_bound')
                ended = not body
            if owned.closed() and ended:
                break
            if ended:
                time.sleep(.01)
        _writer_fds(backend)
        result = dict(witness, actual_exit_code=leader.returncode, remaining_children=[])
        backend._initial_driver_last_exit = result
        need(type(leader.returncode) is int and leader.returncode == 0 and not interrupted
             and stopping is None and not unexpected, 'driver_child_not_successfully_closed')
        _check_end(backend)
        index = decode(bytes(raw))
        need(type(index) is dict and index.get('operation_id') == op.operation_id
             and index.get('phase') == op.phase, 'driver_live_child_index')
        return result, index, received
    except BaseException:
        if leader is not None:
            # Cannot unwind/restore SIGCHLD/subreaper while any owned kernel child remains.
            processes._drain(leader, owned, stopping)
            backend._initial_driver_last_exit = {
                'pid': leader.pid, 'actual_exit_code': leader.returncode,
                'remaining_children': [], 'accepted': False}
        # Ownership transfer is retained even when its metadata/result cannot
        # be accepted. These descriptors confer no successful phase proof.
        backend._initial_driver_failure_fds = list(
            getattr(backend, '_initial_driver_failure_fds', [])) + received
        backend._initial_driver_failure_index = bytes(raw)
        raise
    finally:
        if owned is not None:
            owned.close_fds()
        if channel is not None:
            channel.close()
        if child is not None:
            child.close()
        if parentfd is not None:
            os.close(parentfd)
        for number, handler in handlers.items():
            signal.signal(number, handler)
        if oldsub is not None:
            processes._subreaper(oldsub)
        _SERIAL.release()


def _reconcile(backend, op, index, fds):
    """Rebuild only the fork's live current phase, after actual exit and closure."""
    _check_end(backend)
    j = op.journal
    need(type(j) is journals.DualJournal and set(index) == {'phase', 'operation_id', 'artifacts',
         'sequence', 'backend_artifacts', 'guards', 'writer_processes', 'writer_cgroups',
         'legacy_mount_sequence'} and index['phase'] == op.phase
         and index['operation_id'] == op.operation_id, 'driver_journal_index')
    rows = index['artifacts']
    need(type(rows) is list and 1 <= len(rows) <= 4096
         and type(index['sequence']) is int and backend._sequence <= index['sequence'] <= backend._sequence + 4096,
         'driver_journal_index_bound')
    rebuilt = {}; total = 0
    for row in rows:
        _check_end(backend)
        need(type(row) is dict and set(row) == {'name', 'sha256', 'bytes'}
             and type(row['name']) is str and journals.NAME.fullmatch(row['name'])
             and row['name'] not in rebuilt and pin(row['sha256'])
             and type(row['bytes']) is int and 0 <= row['bytes'] <= journals.MAX_ARTIFACT,
             'driver_artifact_index')
        name = row['name']; total += row['bytes']
        need(total <= journals.MAX_PHASE_BYTES and (name == 'intent.json' or name.startswith(op.phase + '-')),
             'driver_artifact_domain')
        values = []
        for path, identity in j.phase_paths:
            journals.directory(path, identity)
            value = private_bytes(path / name, row['sha256'], limit=row['bytes'])
            need(len(value) == row['bytes'], 'driver_artifact_size')
            values.append(value); journals.directory(path, identity)
        need(values[0] == values[1], 'driver_dual_raw_mismatch')
        rebuilt[name] = dict(row, raw=values[0])
    need(all(name in rebuilt and rebuilt[name]['raw'] == row['raw']
             for name, row in j.artifacts.items()), 'driver_prefork_artifacts_changed')
    oldbytes = sum(r['bytes'] for r in j.artifacts.values())
    newbytes = j.bytes - oldbytes + total
    newcount = j.count - len(j.artifacts) + len(rebuilt)
    need(newbytes <= journals.MAX_PHASE_BYTES and newcount <= 4096, 'driver_total_journal_bound')
    for path, identity in j.phase_paths:
        journals.directory(path, identity)
        need({p.name for p in path.iterdir()} == set(rebuilt), 'driver_partial_or_extra_artifact')
    need(type(index['backend_artifacts']) is dict and
         all(name in rebuilt and rebuilt[name]['sha256'] == sha
             or backend._artifacts.get(name) == sha for name, sha in index['backend_artifacts'].items())
         and all(index['backend_artifacts'].get(k) == v for k, v in backend._artifacts.items()),
         'driver_backend_artifacts')
    need(index['writer_cgroups'] == {path: row for path, (fd, row) in backend._cgroup_fds.items()},
         'driver_unanchored_child_cgroups')
    writers = index['writer_processes']; merged = {}
    need(type(writers) is list and len(writers) <= 4096, 'driver_writer_process_bound')
    from manual_backend import _process_absent
    for row in writers:
        need(type(row) is dict and set(row) == {'pid', 'start_ticks', 'boot_id'}
             and type(row['pid']) is int and row['pid'] > 0 and type(row['start_ticks']) is str
             and row['start_ticks'].isdecimal() and row['boot_id'] == op.inventory['host']['boot_id'],
             'driver_writer_process_identity')
        key = (row['pid'], row['start_ticks'])
        need(key not in merged, 'driver_writer_duplicate'); _process_absent(row); merged[key] = row
    need(all(merged.get(key) == row for key, row in backend._writer_processes.items()),
         'driver_parent_writer_changed')
    mount_sequence = index['legacy_mount_sequence']
    old_sequence = getattr(backend, '_legacy_prepare_mount_sequence', 0)
    need(type(mount_sequence) is int and old_sequence <= mount_sequence <= old_sequence + 4096,
         'driver_mount_sequence')
    if op.phase == legacy.PHASE:
        for sequence in range(old_sequence + 1, mount_sequence + 1):
            need(op.phase + '-dataset-mounts-' + str(sequence) + '.json' in rebuilt,
                 'driver_mount_sequence_unproven')
    else:
        need(mount_sequence == old_sequence, 'driver_initial_changed_legacy_sequence')
    guards = index['guards']
    need(type(guards) is list and len(guards) == len(fds) <= MAX_FDS
         and len({r['field'] for r in guards}) == len(guards), 'driver_guards')
    expected_guards = {'_legacy_prepare_guard_fd'}
    if op.phase == migration.PHASE:
        expected_guards |= {'_initial_migration_guard_fd', '_initial_migration_cgroup_fd'}
    need({row['field'] for row in guards} == expected_guards, 'driver_required_guards')
    primary = op.inventory['linux']['postgres']['primary']['container_id']
    for row, fd in zip(guards, fds):
        need(set(row) == {'field', 'dev', 'ino'} and row['field'] in GUARDS, 'driver_guard_field')
        st = os.fstat(fd)
        need((st.st_dev, st.st_ino) == (row['dev'], row['ino']), 'driver_guard_fd_identity')
        if row['field'].endswith('guard_fd'):
            prefix = 'legacy-prepare' if row['field'].startswith('_legacy') else 'initial-migration'
            path = Path('/tmp/openlegal-' + prefix + '-primary-' + primary + '.lock')
            raw = private_bytes(path)
            need(raw == canonical({'operation_id': op.operation_id, 'unknown_requires_manual_audit': True})
                 and (path.stat().st_dev, path.stat().st_ino) == (st.st_dev, st.st_ino), 'driver_guard_path')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        else:
            owner_raw = rebuilt[op.phase + '-native-gated-owner.json']['raw']
            owner = decode(owner_raw)['witness']['cgroup_identity']
            need(stat.S_ISDIR(st.st_mode) and {'dev': st.st_dev, 'ino': st.st_ino} == owner
                 and os.fstatvfs(fd).f_fsid == os.statvfs('/sys/fs/cgroup').f_fsid,
                 'driver_cgroup_guard')
        old = getattr(backend, row['field'], None)
        if old is not None:
            need((os.fstat(old).st_dev, os.fstat(old).st_ino) == (st.st_dev, st.st_ino), 'driver_guard_changed')
            os.close(fd)
        else:
            setattr(backend, row['field'], fd)
    j.artifacts = {name: {k: row[k] for k in ('sha256', 'bytes', 'raw')} for name, row in rebuilt.items()}
    j.bytes = newbytes; j.count = newcount
    backend._sequence = index['sequence']; backend._artifacts = dict(index['backend_artifacts'])
    backend._writer_processes = merged
    backend._legacy_prepare_mount_sequence = mount_sequence
    _docker_binding(backend, op.operation_id)
    j.check(); _check_end(backend)


def _read(op, name):
    filename = op.phase + '-' + name + '.json'
    sha = op.journal.output_hashes().get(filename)
    need(pin(sha), 'driver_required_child_artifact')
    return decode(op.journal.read_phase_bytes(op.phase, filename, sha))


def _adopt_live_phase(backend, op, index, fds):
    try:
        _reconcile(backend, op, index, fds)
    except BaseException:
        retained = set(getattr(backend, '_initial_driver_failure_fds', []))
        known = {getattr(backend, name) for name in GUARDS if hasattr(backend, name)}
        for fd in fds:
            if fd not in known:
                try:
                    os.fstat(fd)
                except OSError:
                    continue
                retained.add(fd)
        backend._initial_driver_failure_fds = list(retained)
        raise


def _fresh_parent(backend, op, result):
    _check_end(backend); op.seal()
    need(type(result) is dict and set(result) == {'kind', 'status', 'operation_id', 'payloads',
         'fresh_primary', 'original_restart_policy', 'current_restart_policy', 'blob_manifests_sha256',
         'producer_os_exit', 'ready', 'provider_state_modified', 'strict_seed_created'}
         and result['kind'] == 'openlegal_legacy_prepare_payloads'
         and result['status'] == 'PENDING_PARENT_PRODUCER_OS_EXIT'
         and result['operation_id'] == op.operation_id
         and result['producer_os_exit'] == 'NOT_YET_OBSERVABLE'
         and result['current_restart_policy'] == 'no'
         and all(result[k] is False for k in ('ready', 'provider_state_modified', 'strict_seed_created'))
         and set(result['payloads']) == {'drain', 'snapshot', 'preservation'}, 'driver_legacy_result')
    c, expected = legacy._scope(backend, op)
    end = backend._driver_deadline
    fresh = legacy._native_primary(backend, op, c, end)
    backend._legacy_prepare_primary_fds = list(getattr(backend, '_legacy_prepare_primary_fds', [])) + [
        fresh['data_fd'], fresh['wal_fd'], fresh['cgroup_fd']]
    need(fresh['process'] == result['fresh_primary']['primary_process']
         and fresh['restart_policy'] == {'Name': 'no', 'MaximumRetryCount': 0}, 'driver_fresh_primary')
    legacy._datasets(backend, op, c, end, fresh)
    legacy._recheck_native_mounts(backend, op, c, fresh, end)
    legacy._images(backend, op, c, end); legacy._control(backend, op, c, end)
    physical = legacy._blob_state(backend, op, c, end, 'parent-live')
    need(digest(canonical(physical)) == result['blob_manifests_sha256'], 'driver_fresh_blob_state')
    preserved = result['payloads']['preservation']
    need(migration._old_state(backend, op, end) == preserved
         and migration._sql(backend, op, migration.MIGRATIONS_SQL, end) ==
             [dict(row, success=True) for row in expected], 'driver_fresh_schema27')
    migration._sessions(backend, op, end)
    migration._fence(backend, op, result['payloads']['drain'])
    primary = result['fresh_primary']
    fd = backend._container_directory(fresh['process']['pid'], '/var/run/postgresql')
    try:
        directory = os.fstat(fd); node = os.stat('.s.PGSQL.5432', dir_fd=fd, follow_symlinks=False)
        need(stat.S_ISSOCK(node.st_mode) and node.st_uid == node.st_gid == 70
             and primary['socket_directory'] == {'dev': directory.st_dev, 'ino': directory.st_ino}
             and primary['socket'] == {'dev': node.st_dev, 'ino': node.st_ino}, 'driver_fresh_socket')
    finally:
        os.close(fd)
    legacy._recheck_native_mounts(backend, op, c, fresh, end)
    _check_end(backend); _writer_fds(backend)
    return copy.deepcopy(primary)


def run(backend, shared_lock, inventory_ref, authority_ref, *, clock=None):
    """Retain all objects/FDs on failure; caller must not release them to retry.

    The only variable inputs are byte-pinned capability data and an optional
    exact context from prepare_clock. Native callbacks, shell commands, SQL,
    phase handlers and pre-existing journals are not inputs. A prepared backend
    must use its original clock; omission cannot renew the operation.
    """
    started = time.monotonic()
    _fresh_backend(backend, shared_lock)
    if backend._initial_driver_clock_started is not None or clock is not None:
        end = _consume_clock(backend, shared_lock, clock, inventory_ref, authority_ref, started)
    else:
        end = None
    config, inv, template, authority, raw, invraw, authraw = _load(inventory_ref, authority_ref)
    _input_binding(inv, shared_lock)
    if clock is not None:
        need(clock.operation_id == config['operation_id']
             and clock.duration == config['total_deadline_secs'], 'driver_clock_inputs_changed')
    backend._driver_deadline = _end(started, config['total_deadline_secs']) if end is None else end
    _check_end(backend)
    graph_sha = digest(canonical(config['source_graph'])); auth_sha = digest(authraw)
    binding = {'kind': 'openlegal_live_initial_driver', 'operation_id': config['operation_id'],
               'inventory_sha256': digest(raw), 'source_graph_sha256': graph_sha,
               'authority_sha256': auth_sha, 'automatic_retry': False}
    journal = journals.DualJournal(inv['journal_roots'], config['operation_id'], binding)
    backend._initial_driver_journal = journal
    fixed_inventory = copy.deepcopy(inv)
    parent = os.getpid()
    def make_op(action, phase, current):
        frozen_raw = canonical(current)
        def seal():
            _check_end(backend); shared_lock.check()
            if backend._docker_config is not None:
                _docker_binding(backend, config['operation_id'])
            if backend._cgroup_fds:
                _writer_fds(backend)
            need(os.getppid() == parent if os.getpid() != parent else True, 'driver_parent_changed')
            need(_reference(inventory_ref) == raw and _reference(authority_ref) == authraw
                 and _reference(config['legacy_inventory']) == invraw
                 and decode(_reference(config['initial_template'])) == template
                 and time.time() < authority['valid_until']
                 and canonical(current) == frozen_raw, 'driver_sealed_inputs_changed')
            _source_seal(config['source_graph']); journal.check()
        return Operation(config['operation_id'], action, phase, current, digest(frozen_raw),
                         graph_sha, auth_sha, journal, Path(inv['backup_root']['path']), {}, seal,
                         freeze_authority(authority))
    journal.begin('initial_driver_prefork')
    op = make_op('legacy_prepare', 'initial_driver_prefork', fixed_inventory)
    op.seal(); _create_docker_client(backend, op)
    backend._capture_writer_cgroups(op); _writer_fds(backend)
    journal.begin(legacy.PHASE); op = make_op('legacy_prepare', legacy.PHASE, fixed_inventory)
    exitrow, index, fds = _fork_phase(backend, op)
    legacy_exit = exitrow
    _adopt_live_phase(backend, op, index, fds); op.seal()
    result = _read(op, 'returned-payloads')
    primary = _fresh_parent(backend, op, result)
    prerequisites = {}
    for key, payload in result['payloads'].items():
        _check_end(backend); op.seal()
        value = {'schema_version': 1, 'kind': 'openlegal_initial_' + key + '_proof',
                 'operation_id': op.operation_id, 'inventory_sha256': op.inventory_sha256,
                 'source_graph_sha256': graph_sha, 'authority_sha256': auth_sha,
                 'producer_source_sha256': config['source_graph']['legacy_prepare.py'],
                 'producer_exit': exitrow, 'payload': payload}
        name = 'live-' + key + '-proof.json'; sha = journal.save(name, value)
        need(journal.read_phase_bytes(op.phase, name, sha) == canonical(value), 'driver_proof_ack')
        _check_end(backend)
        prerequisites[key] = {'phase': op.phase, 'name': name, 'sha256': sha}
    derived = copy.deepcopy(inv); initial = copy.deepcopy(template); initial.update(primary)
    initial['prerequisites'] = prerequisites
    initial['legacy_binding'] = {'inventory_sha256': op.inventory_sha256,
                                'source_graph_sha256': graph_sha, 'authority_sha256': auth_sha}
    derived['initial_migration'] = initial
    _check_end(backend); op.seal(); _writer_fds(backend)
    journal.begin(migration.PHASE)
    initial_op = make_op('strict_prepare', migration.PHASE, derived)
    exitrow, index, fds = _fork_phase(backend, initial_op)
    _adopt_live_phase(backend, initial_op, index, fds); initial_op.seal()
    complete = _read(initial_op, 'migration-complete')
    created = _read(initial_op, 'native-created')
    need(type(complete) is dict and set(complete) == {'kind', 'candidate_id', 'schema_version',
         'migration_native_exit_code', 'same_database_sessions', 'old_rows_and_references_sha256',
         'seed_created', 'roles_created', 'ready', 'outer_actual_os_exit'}
         and complete['kind'] == 'openlegal_initial_migration_result'
         and type(complete['candidate_id']) is str and re.fullmatch('[0-9a-f]{64}', complete['candidate_id'])
         and created == {'id': complete['candidate_id']}
         and type(complete['schema_version']) is int and complete['schema_version'] == 33
         and type(complete['migration_native_exit_code']) is int and complete['migration_native_exit_code'] == 0
         and complete['old_rows_and_references_sha256'] == digest(canonical(result['payloads']['preservation']))
         and complete['same_database_sessions'] == []
         and all(complete[k] is False for k in ('seed_created', 'roles_created', 'ready'))
         and complete['outer_actual_os_exit'] == 'NOT_YET_OBSERVABLE', 'driver_initial_complete')
    _check_end(backend); _writer_fds(backend)
    final = {'kind': 'openlegal_initial_driver_result', 'operation_id': op.operation_id,
             'legacy_actual_exit': legacy_exit,
             'initial_actual_exit': exitrow, 'schema_version': 33,
             'ready': False, 'roles_created': False, 'strict_seed_created': False,
             'provider_calls': False, 'automatic_retry': False}
    sha = journal.save('driver-complete.json', final)
    need(journal.read_phase_bytes(initial_op.phase, 'driver-complete.json', sha) == canonical(final),
         'driver_final_dual_ack')
    initial_op.seal(); _check_end(backend)
    return final
