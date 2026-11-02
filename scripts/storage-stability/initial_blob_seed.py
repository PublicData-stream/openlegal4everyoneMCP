"""First strict four-root Blob seed from one live completed initial driver.

No CLI, provisioning, role, replica, workload, credential or Ready port. Existing
legacy roots are read only. Failures retain partial destinations and journals.
Every bounded action uses the driver's ORIGINAL absolute deadline.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import time

import bootstrap_backend as blobs
import initial_driver as driver
import initial_migration as migration
import journal as journals
import legacy_prepare as legacy
import manual_backend as manual
import preservation_backend as preservation
import preservation_capture as capture
from copy_tree import root_fd, signature
from interface import Operation, freeze_authority, need
from inventory import canonical, decode, digest, pin, private_bytes, private_parents, valid_uuid

PHASE = 'initial_blob_seed'
ROLES = tuple(blobs.ROLES)
SOURCE_ROLES = frozenset(legacy.ROLES)
CHUNK = 1024**2
MAX_INDEX = 4 * 1024**2
BATCH_FILES = 64
BATCH_BYTES = 4 * 1024**2
BATCH_SECONDS = 0.250


def _end(backend):
    need(type(backend._driver_deadline) is float and math.isfinite(backend._driver_deadline),
         'seed_original_deadline_required')
    driver._check_end(backend)


def _read_large(raw):
    need(type(raw) is bytes and len(raw) <= journals.MAX_ARTIFACT, 'seed_live_artifact_bound')
    def pairs(items):
        value = {}
        for key, item in items:
            need(key not in value, 'seed_duplicate_json_key'); value[key] = item
        return value
    try:
        return json.loads(raw, object_pairs_hook=pairs,
                          parse_constant=lambda _: need(False, 'seed_nonfinite_json'))
    except (ValueError, UnicodeError, RecursionError):
        need(False, 'seed_live_json')


def _phase_raw(journal, phase, name):
    matches = [(rows[name]['sha256'], rows[name]['raw'])
        for paths, rows in journal.history + [(journal.current(), journal.artifacts)]
        if paths and paths[0][0].name == phase and name in rows]
    need(len(matches) == 1, 'seed_live_artifact_missing')
    sha, expected = matches[0]
    raw = journal.read_phase_bytes(phase, name, sha)
    need(raw == expected and digest(raw) == sha, 'seed_live_artifact_changed')
    return raw, sha


def _load(inventory_ref, authority_ref):
    raw = driver._reference(inventory_ref); cfg = decode(raw)
    fields = {'schema_version', 'kind', 'operation_id', 'driver_inventory', 'driver_authority',
              'source_sha256', 'managed_epoch', 'sources', 'targets', 'capacity_reserve_bytes', 'media_profile'}
    need(type(cfg) is dict and set(cfg) == fields
         and type(cfg['schema_version']) is int and cfg['schema_version'] == 2 and cfg['media_profile'] == blobs.MEDIA_PROFILE
         and cfg['kind'] == 'openlegal_initial_blob_seed_inventory'
         and valid_uuid(cfg['operation_id']) and valid_uuid(cfg['managed_epoch'])
         and pin(cfg['source_sha256']) and type(cfg['capacity_reserve_bytes']) is int
         and 0 <= cfg['capacity_reserve_bytes'] <= 2**64 - 1, 'seed_inventory')
    private_bytes(Path(__file__).resolve(), cfg['source_sha256'])
    dc, inv, template, da, draw, invraw, daraw = driver._load(cfg['driver_inventory'], cfg['driver_authority'])
    need(cfg['operation_id'] == dc['operation_id'] and cfg['sources'] == inv['legacy_prepare']['blob_roots']
         and set(cfg['sources']) == SOURCE_ROLES, 'seed_exact_legacy_sources')
    need(type(cfg['targets']) is dict and set(cfg['targets']) == set(ROLES), 'seed_four_targets')
    paths = [Path(row['path']) for row in cfg['sources'].values()]
    container_paths = []
    for role, target in cfg['targets'].items():
        need(type(target) is dict and set(target) == {'host_path', 'container_path', 'parent', 'relative', 'media'},
             'seed_target_shape')
        blobs._blob_target(role, target)
        container = Path(target['container_path'])
        need(container.is_absolute() and str(container) == target['container_path']
             and '..' not in container.parts, 'seed_container_path')
        container_paths.append(container)
        need(target['parent']['kernel_filesystem_id'] <= 2**64 - 1
             and len(target['parent']['filesystem_id']) <= 128, 'seed_target_filesystem_id')
        paths.append(Path(target['parent']['path']))
    for i, a in enumerate(paths):
        need(a.is_absolute() and a.resolve() == a, 'seed_canonical_path')
        for b in paths[i + 1:]:
            need(not a.is_relative_to(b) and not b.is_relative_to(a), 'seed_root_overlap')
    for i, a in enumerate(container_paths):
        for b in container_paths[i + 1:]:
            need(not a.is_relative_to(b) and not b.is_relative_to(a), 'seed_container_root_overlap')
    authraw = driver._reference(authority_ref); auth = decode(authraw)
    authfields = {'schema_version', 'kind', 'approved', 'operation_id', 'inventory_sha256',
        'driver_inventory_sha256', 'driver_authority_sha256', 'source_sha256',
        'driver_managed_epoch', 'seed_managed_epoch', 'epoch_mapping_permitted', 'valid_until', 'media_profile',
        'initial_blob_seed_permitted', 'preserve_originals', 'automatic_retry',
        'roles_permitted', 'postgres_replica_permitted', 'provisioning_permitted',
        'workload_permitted', 'ready_permitted', 'provider_permitted',
        'network_permitted', 'credentials_permitted', 'dataset_creation_permitted'}
    need(type(auth) is dict and set(auth) == authfields
         and type(auth['schema_version']) is int and auth['schema_version'] == 2 and auth['media_profile'] == cfg['media_profile']
         and auth['kind'] == 'openlegal_initial_blob_seed_authority' and auth['approved'] is True
         and auth['operation_id'] == cfg['operation_id'] and auth['inventory_sha256'] == digest(raw)
         and auth['driver_inventory_sha256'] == digest(draw) and auth['driver_authority_sha256'] == digest(daraw)
         and auth['source_sha256'] == cfg['source_sha256']
         and valid_uuid(inv['managed_epoch']) and auth['driver_managed_epoch'] == inv['managed_epoch']
         and auth['seed_managed_epoch'] == cfg['managed_epoch']
         and type(auth['epoch_mapping_permitted']) is bool
         and (cfg['managed_epoch'] == inv['managed_epoch'] or auth['epoch_mapping_permitted'] is True)
         and type(auth['valid_until']) is int and time.time() < auth['valid_until']
         and auth['initial_blob_seed_permitted'] is True and auth['preserve_originals'] is True
         and all(auth[k] is False for k in ('automatic_retry', 'roles_permitted', 'postgres_replica_permitted',
             'provisioning_permitted', 'workload_permitted', 'ready_permitted', 'provider_permitted',
             'network_permitted', 'credentials_permitted', 'dataset_creation_permitted')), 'seed_authority')
    return cfg, auth, raw, authraw, dc, inv, template, draw, invraw, daraw


def _live_context(backend, lock, cfg, dc, inv, template, draw, invraw, daraw):
    j = getattr(backend, '_initial_driver_journal', None)
    need(type(j) is journals.DualJournal and j.operation == cfg['operation_id']
         and j.binding['inventory_sha256'] == digest(draw)
         and j.binding['authority_sha256'] == digest(daraw)
         and j.phase_paths and j.phase_paths[0][0].name == migration.PHASE,
         'seed_live_driver_required')
    lock.check(); j.check(); _end(backend); driver._writer_fds(backend)
    finalraw, _ = _phase_raw(j, migration.PHASE, 'driver-complete.json'); final = decode(finalraw)
    need(final['kind'] == 'openlegal_initial_driver_result' and final['operation_id'] == cfg['operation_id']
         and final['initial_actual_exit'] == backend._initial_driver_last_exit
         and final['schema_version'] == 33 and all(final[k] is False for k in
             ('ready', 'roles_created', 'strict_seed_created', 'provider_calls', 'automatic_retry')),
         'seed_driver_complete')
    for key in ('legacy_actual_exit', 'initial_actual_exit'):
        row = final[key]
        need(type(row) is dict and set(row) == {'pid', 'start_ticks', 'boot_id', 'actual_exit_code', 'remaining_children'}
             and type(row['actual_exit_code']) is int and row['actual_exit_code'] == 0
             and row['remaining_children'] == [] and row['boot_id'] == inv['host']['boot_id'],
             'seed_actual_child_exit')
        manual._process_absent({k: row[k] for k in ('pid', 'start_ticks', 'boot_id')})
    anchorraw, _ = _phase_raw(j, legacy.PHASE, legacy.PHASE + '-parent-fd-anchor.json')
    anchor = decode(anchorraw)
    ticks = Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]
    need(anchor['pid'] == os.getpid() and anchor['start_ticks'] == ticks
         and anchor['boot_id'] == inv['host']['boot_id'], 'seed_same_live_root_parent')
    expected = {(fd, row['dev'], row['ino'], row['path']) for fd, row in backend._cgroup_fds.values()}
    need({(row['fd'], row['dev'], row['ino'], row['path']) for row in anchor['cgroups']} == expected,
         'seed_parent_retained_writer_fds')
    oldraw, _ = _phase_raw(j, legacy.PHASE, legacy.PHASE + '-returned-payloads.json')
    old = decode(oldraw); initial = copy.deepcopy(template); initial.update(old['fresh_primary'])
    prereqs = {}
    for key in ('drain', 'snapshot', 'preservation'):
        name = 'live-' + key + '-proof.json'; raw, sha = _phase_raw(j, legacy.PHASE, name)
        value = decode(raw)
        need(value['producer_exit'] == final['legacy_actual_exit'] and value['payload'] == old['payloads'][key]
             and value['inventory_sha256'] == digest(invraw)
             and value['authority_sha256'] == digest(daraw)
             and value['source_graph_sha256'] == digest(canonical(dc['source_graph']))
             and value['producer_source_sha256'] == dc['source_graph']['legacy_prepare.py']
             and value['operation_id'] == cfg['operation_id'], 'seed_live_prerequisite')
        prereqs[key] = {'phase': legacy.PHASE, 'name': name, 'sha256': sha}
    initial['prerequisites'] = prereqs
    initial['legacy_binding'] = {'inventory_sha256': digest(invraw),
        'source_graph_sha256': digest(canonical(dc['source_graph'])), 'authority_sha256': digest(daraw)}
    work = copy.deepcopy(inv); work['initial_migration'] = initial
    baseraw, _ = _phase_raw(j, legacy.PHASE, legacy.PHASE + '-blob-manifests-parent-live.json')
    baseline = _read_large(baseraw)
    need(type(baseline) is dict and set(baseline) == SOURCE_ROLES, 'seed_complete_live_baseline')
    return j, work, old, baseline


def _identity(st):
    return (st.st_dev, st.st_ino, st.st_uid, st.st_gid, stat.S_IFMT(st.st_mode), stat.S_IMODE(st.st_mode))


def _empty_xattrs(fd):
    """First-seed profile never copies or strips arbitrary ACL/security data."""
    try:
        names = os.listxattr(fd)
    except OSError:
        need(False, 'seed_xattrs_unavailable')
    need(not names, 'seed_xattrs_unsupported')


def _metadata_tree(backend, path, *, new=False):
    """Reopen every selected shard/file; old controls/health are never copied."""
    active = (_active_root(backend, path),); window = _MetadataBatch(backend, active)
    root = root_fd(path, noatime=True)
    def check_file(parent, entry):
        window.before(os.stat(entry, dir_fd=parent, follow_symlinks=False).st_size)
        child = os.open(entry, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                        | os.O_CLOEXEC | os.O_NOATIME, dir_fd=parent)
        try:
            item = os.fstat(child); need(stat.S_ISREG(item.st_mode), 'seed_metadata_regular')
            _empty_xattrs(child)
            need(signature(os.fstat(child)) == signature(item)
                 == signature(os.stat(entry, dir_fd=parent, follow_symlinks=False)),
                 'seed_metadata_file_changed')
        finally: os.close(child)
    try:
        before = os.fstat(root); _empty_xattrs(root)
        names = sorted(os.listdir(root)); need(len(names) <= 258, 'seed_metadata_root_bound')
        for name in names:
            window.before(0)
            if name == blobs.MARKER:
                if not new: continue
                check_file(root, name)
            else:
                if name == blobs.GUARDS and not new: continue
                need(name == blobs.GUARDS and new or re.fullmatch('[0-9a-f]{2}', name),
                     'seed_metadata_directory')
                directory = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                                    | os.O_CLOEXEC | os.O_NOATIME, dir_fd=root)
                try:
                    stamp = os.fstat(directory); _empty_xattrs(directory)
                    entries = sorted(os.listdir(directory))
                    need(len(entries) <= blobs.MAX_FILES, 'seed_metadata_file_bound')
                    for entry in entries:
                        if name == blobs.GUARDS:
                            need(entry == '.directory.lock', 'seed_metadata_foreign_control')
                        elif blobs.HEALTH.fullmatch(name + '/' + entry):
                            if not new: continue
                        else:
                            need(blobs.KEY.fullmatch(name + '/' + entry), 'seed_metadata_generation')
                        check_file(directory, entry)
                    _empty_xattrs(directory)
                    need(entries == sorted(os.listdir(directory))
                         and signature(os.fstat(directory)) == signature(stamp), 'seed_metadata_shard_changed')
                finally:
                    os.close(directory)
        need(names == sorted(os.listdir(root)) and signature(os.fstat(root)) == signature(before),
             'seed_metadata_root_changed')
        _empty_xattrs(root); window.finish()
    finally:
        os.close(root)


def _hold(backend, name, path, expected=None, owner=None):
    fd = root_fd(path, noatime=True); st = os.fstat(fd)
    try:
        _empty_xattrs(fd)
        if expected is not None:
            need((st.st_dev, st.st_ino) == (expected['dev'], expected['ino']), 'seed_directory_identity')
        if owner is not None:
            need(st.st_uid == st.st_gid == owner and stat.S_IMODE(st.st_mode) == 0o700,
                 'seed_directory_metadata')
        chain = [(str(p), _identity(p.lstat())) for p in reversed((Path(path), *Path(path).parents))]
        need(all(stat.S_ISDIR(Path(p).lstat().st_mode) and Path(p).resolve() == Path(p) for p, _ in chain),
             'seed_directory_ancestry')
        backend._initial_blob_seed_fds[name] = (fd, str(path), _identity(st), chain)
        return fd
    except BaseException:
        os.close(fd); raise


def _bound(backend):
    _end(backend)
    for fd, path, identity, chain in backend._initial_blob_seed_fds.values():
        _empty_xattrs(fd)
        need(_identity(os.fstat(fd)) == identity and _identity(Path(path).lstat()) == identity,
             'seed_held_root_rebound')
        for ancestor, pinning in chain:
            need(_identity(Path(ancestor).lstat()) == pinning and Path(ancestor).resolve() == Path(ancestor),
                 'seed_ancestor_rebound')
    driver._writer_fds(backend); _end(backend)


def _fast_bound(backend, active):
    """Chunk guard for owned active roots; complete ancestry is batch-bounded."""
    _end(backend); driver._writer_fds(backend)
    need(type(active) is tuple and active and len(set(active)) == len(active), 'seed_active_roots')
    owned = {row[0]: row for row in backend._initial_blob_seed_fds.values()}
    for fd in active:
        need(type(fd) is int and fd in owned, 'seed_unowned_active_fd')
        _, path, identity, _ = owned[fd]
        _empty_xattrs(fd)
        need(_identity(os.fstat(fd)) == identity and _identity(Path(path).lstat()) == identity,
             'seed_active_root_rebound')
    _end(backend)


def _active_parent(backend, fd):
    rows = [row for row in backend._initial_blob_seed_fds.values() if row[0] == fd]
    need(len(rows) == 1, 'seed_active_root_missing')
    return _active_root(backend, Path(rows[0][1]).parent)


def _active_root(backend, path):
    rows = [row[0] for row in backend._initial_blob_seed_fds.values() if row[1] == str(path)]
    need(len(rows) == 1, 'seed_active_root_missing'); return rows[0]


class _MetadataBatch:
    """All files checked; only repeated global ancestry walks are batched."""
    def __init__(self, backend, active):
        self.backend = backend; self.active = active
        _bound(backend); self.started = time.monotonic(); self.count = self.bytes = 0

    def before(self, amount):
        need(type(amount) is int and amount >= 0, 'seed_metadata_size')
        if self.count and (self.count >= BATCH_FILES or self.bytes + amount > BATCH_BYTES
                           or time.monotonic() - self.started >= BATCH_SECONDS):
            _bound(self.backend); self.started = time.monotonic(); self.count = self.bytes = 0
        _fast_bound(self.backend, self.active); self.count += 1; self.bytes += amount

    def finish(self):
        _bound(self.backend)


def _saved(backend, op, name, value):
    _end(backend); op.seal()
    result = migration._saved(backend, op, name, value)
    _end(backend); return result


def _qualify_media(backend, op, cfg, *, empty):
    _bound(backend); domains, hardware = blobs._media_hardware(backend, op)
    table = legacy._mounts(); proof = {}
    for role, target in cfg['targets'].items():
        _bound(backend); fd = backend._initial_blob_seed_fds['parent-' + role][0]
        need(sorted(os.listdir(fd)) == ([] if empty else ['data']), 'seed_parent_entries')
        proof[role] = blobs._target_media(backend, op, role, target, fd, domains, hardware, table,
                                          deadline=backend._driver_deadline)
        _bound(backend)
    return proof


def _fresh(backend, op, old):
    _end(backend); op.seal()
    c = op.inventory['initial_migration']; end = backend._driver_deadline
    expected = migration._manifest(c)
    before = migration._sessions(backend, op, end)
    migration._fence(backend, op, old['payloads']['drain'])
    socketfd = migration._primary(backend, op, c)
    backend._initial_blob_seed_native_fds.append(socketfd)
    lc = op.inventory['legacy_prepare']
    native = legacy._native_primary(backend, op, lc, end)
    backend._initial_blob_seed_native_fds.extend(native[k] for k in ('data_fd', 'wal_fd', 'cgroup_fd'))
    need(native['process'] == c['primary_process'], 'seed_primary_changed')
    legacy._recheck_native_mounts(backend, op, lc, native, end)
    legacy._datasets(backend, op, lc, end, native)
    legacy._images(backend, op, lc, end); legacy._control(backend, op, lc, end)
    need(migration._sql(backend, op, migration.MIGRATIONS_SQL, end) ==
         [dict(row, success=True) for row in expected], 'seed_schema33_checksums')
    preserved = old['payloads']['preservation']
    need(migration._old_state(backend, op, end, list(preserved['tables'])) == preserved,
         'seed_old_userdata_changed')
    migration._sessions(backend, op, end, before); _bound(backend)
    return {'primary_process': native['process'], 'old_preservation_sha256': digest(canonical(preserved)),
            'schema_version': 33}


def _references(backend, op, label):
    rows = []; pages = []; last = None; source_rows = None
    end = backend._driver_deadline
    while True:
        _end(backend); op.seal()
        need(len(pages) < preservation.MAX_PAGES, 'seed_reference_page_bound')
        offset = len(rows); sql = capture._reference_sql(offset)
        prefix = ("SET statement_timeout='30s'; SET lock_timeout='1s'; SET transaction_timeout='35s'; "
                  "SET TIME ZONE 'UTC'; SET DateStyle='ISO,YMD'; SET bytea_output='hex'; ")
        stdin = (prefix + sql + ';\n').encode()
        primary = op.inventory['linux']['postgres']['primary']['container_id']
        args = ['exec', '-i', '-u', '70', '-e', 'PGPASSFILE=/dev/null',
            '-e', 'PGPASSWORD=', '-e', 'PGSERVICE=', '-e', 'PGSERVICEFILE=/dev/null', '-e', 'PGHOSTADDR=',
            '-e', 'PGOPTIONS=', '-e', 'PGSSLCERT=', '-e', 'PGSSLKEY=', '-e', 'PGSSLROOTCERT=',
            '-e', 'PGAPPNAME=openlegal_initial_blob_seed', primary,
            '/usr/bin/env', '-u', 'PGSERVICE', 'psql', '-X', '-w', '-h', '/var/run/postgresql',
            '-p', '5432', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'openlegal_migration', '-d', 'openlegal']
        raw = backend._run(op, 'docker', args, stdin=stdin, timeout=31, deadline=end)
        need(len(raw) <= preservation.MAX_PAGE_BYTES, 'seed_reference_page_bytes')
        command_name = op.phase + '-command-%04d.json' % backend._sequence
        sha = op.journal.output_hashes().get(command_name); need(pin(sha), 'seed_reference_command_missing')
        command = decode(op.journal.read_phase_bytes(op.phase, command_name, sha))
        dispatch = ['--config', str(backend._docker_config[0]), '--host', 'unix:///var/run/docker.sock']
        need(command['binary'] == 'docker' and command['arguments_sha256'] == digest(canonical(dispatch + args))
             and command['stdin_sha256'] == digest(stdin) and command['stdout_hex'] == raw.hex()
             and command['stdout_sha256'] == digest(raw) and command['stdout_bytes'] == len(raw)
             and type(command['actual_exit_code']) is int and command['actual_exit_code'] == 0
             and command['started'] is True and command['owned_processes_closed'] is True
             and all(command[k] is False for k in ('expired', 'interrupted', 'unexpected_descendants',
                                                  'output_limit_exceeded')), 'seed_reference_actual_command')
        value = decode(raw); page, count = capture._reference_result(value, offset)
        need(source_rows is None or source_rows == count, 'seed_reference_source_changed')
        source_rows = count; last = capture._reference_rows(page, last)
        need(len(rows) + len(page) <= preservation.MAX_ROWS, 'seed_reference_rows_bound')
        ref = _saved(backend, op, 'references-' + label + '-%04d' % len(pages),
            {'offset': offset, 'source_rows': count, 'rows': page, 'actual_command': {'name': command_name, 'sha256': sha}})
        pages.append(dict(ref, offset=offset, rows=len(page))); rows.extend(page)
        if len(page) < preservation.PAGE_SIZE:
            break
    return {'rows': rows, 'source_rows': source_rows, 'pages': pages}


def _scan_sources(backend, cfg, baseline):
    result = {}
    for role, source in cfg['sources'].items():
        _bound(backend)
        _metadata_tree(backend, source['path'])
        actual = blobs.blob_inventory(source['path'], deadline=backend._driver_deadline, controls=True, online=False)
        _metadata_tree(backend, source['path'])
        need(actual == baseline[role] and actual['root_identity'] == source['identity'],
             'seed_original_changed')
        result[role] = actual
    _bound(backend); return result


def _union(source):
    values = {}; origins = {}
    for role, value in source.items():
        for row in value['permanent']:
            preservation.compare_generations([row], [row])
            logical = 'cache' if role == 'cache_hdd' else 'corpus'
            key = (logical, row['storage_key'])
            need(key not in values or values[key] == row, 'seed_conflicting_generation_identity')
            values[key] = row; origins.setdefault(key, role)
    rows = {kind: sorted([v for (role, key), v in values.items() if role == kind],
                        key=lambda r: r['storage_key']) for kind in ('cache', 'corpus')}
    need(all(len(v) <= preservation.MAX_ROWS for v in rows.values()), 'seed_union_bound')
    need(all(sum(row['size'] for row in value) <= blobs.MAX_BYTES for value in rows.values()),
         'seed_union_bytes_bound')
    return rows, origins


def _shard_identity(fd, rootfd, shard):
    st = os.fstat(fd); _empty_xattrs(fd)
    need(stat.S_ISDIR(st.st_mode) and st.st_uid == st.st_gid == 10004
         and stat.S_IMODE(st.st_mode) == 0o700 and st.st_dev == os.fstat(rootfd).st_dev
         and _identity(st) == _identity(os.stat(shard, dir_fd=rootfd, follow_symlinks=False)),
         'seed_shard_identity')
    return _identity(st)


def _copy_file(backend, sourcefd, targetfd, row, dirty):
    """One individually fsynced/read-back file; batch owns dirty shard FDs."""
    active = (sourcefd, targetfd, _active_parent(backend, targetfd)); _fast_bound(backend, active)
    shard, name = row['storage_key'].split('/')
    srcdir = os.open(shard, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NOATIME, dir_fd=sourcefd)
    inp = out = None
    try:
        _shard_identity(srcdir, sourcefd, shard)
        if shard not in dirty:
            need(len(dirty) < BATCH_FILES, 'seed_dirty_shard_bound')
            try:
                _bound(backend); os.mkdir(shard, 0o700, dir_fd=targetfd)
                outdir = os.open(shard, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=targetfd)
                try:
                    _empty_xattrs(outdir)
                    os.fchown(outdir, 10004, 10004); _empty_xattrs(outdir)
                    os.fchmod(outdir, 0o700); _empty_xattrs(outdir)
                    identity = _shard_identity(outdir, targetfd, shard)
                except BaseException:
                    os.close(outdir); raise
            except FileExistsError:
                outdir = os.open(shard, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=targetfd)
                try: identity = _shard_identity(outdir, targetfd, shard)
                except BaseException:
                    os.close(outdir); raise
            dirty[shard] = (outdir, identity)
            _bound(backend)
        outdir, identity = dirty[shard]
        need(_shard_identity(outdir, targetfd, shard) == identity, 'seed_target_shard_rebound')
        inp = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC | os.O_NOATIME, dir_fd=srcdir)
        before = os.fstat(inp); _empty_xattrs(inp)
        need(stat.S_ISREG(before.st_mode) and before.st_uid == before.st_gid == 10004 and before.st_nlink == 1
             and stat.S_IMODE(before.st_mode) == 0o600 and before.st_size == row['size']
             and before.st_dev == os.fstat(sourcefd).st_dev, 'seed_source_file_metadata')
        _fast_bound(backend, active)
        out = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=outdir)
        _empty_xattrs(out)
        os.fchown(out, 10004, 10004); _empty_xattrs(out)
        os.fchmod(out, 0o600); _empty_xattrs(out)
        hashed = hashlib.sha256(); size = 0
        while True:
            _fast_bound(backend, active); data = os.read(inp, CHUNK); _end(backend)
            if not data: break
            size += len(data); need(size <= row['size'], 'seed_source_grew'); hashed.update(data)
            view = memoryview(data)
            while view:
                _fast_bound(backend, active); written = os.write(out, view); _end(backend)
                need(written > 0, 'seed_zero_write'); view = view[written:]
        need(size == row['size'] and hashed.hexdigest() == row['sha256']
             and signature(os.fstat(inp)) == signature(before), 'seed_source_changed')
        _empty_xattrs(inp); _empty_xattrs(srcdir); _empty_xattrs(out); _empty_xattrs(outdir)
        os.utime(out, ns=(before.st_atime_ns, before.st_mtime_ns))
        _fast_bound(backend, active); os.fsync(out); _fast_bound(backend, active); _empty_xattrs(out)
        os.lseek(out, 0, os.SEEK_SET); readback = hashlib.sha256(); count = 0
        while True:
            _fast_bound(backend, active); data = os.read(out, CHUNK); _end(backend)
            if not data: break
            count += len(data); need(count <= row['size'], 'seed_copy_readback_grew'); readback.update(data)
        need(count == row['size'] and readback.hexdigest() == row['sha256']
             and signature(os.fstat(inp)) == signature(before)
             and signature(os.stat(name, dir_fd=srcdir, follow_symlinks=False)) == signature(before),
             'seed_copy_readback')
        copied = os.fstat(out)
        _empty_xattrs(inp); _empty_xattrs(srcdir); _empty_xattrs(out); _empty_xattrs(outdir)
        need(copied.st_uid == copied.st_gid == 10004 and stat.S_IMODE(copied.st_mode) == 0o600
             and copied.st_nlink == 1 and signature(copied) == signature(
                 os.stat(name, dir_fd=outdir, follow_symlinks=False)), 'seed_copied_file_identity')
        _shard_identity(srcdir, sourcefd, shard)
        need(_shard_identity(outdir, targetfd, shard) == identity, 'seed_target_shard_rebound')
        _fast_bound(backend, active)
    finally:
        for fd in (out, inp, srcdir):
            if fd is not None: os.close(fd)


def _copy_barrier(backend, targetfd, dirty):
    """No receipt until file fsyncs AND shard/root/parent names are durable."""
    _bound(backend)
    _fast_bound(backend, (targetfd,))
    parentfd = _active_parent(backend, targetfd)
    active = (targetfd, parentfd)
    for shard, (fd, identity) in sorted(dirty.items()):
        _fast_bound(backend, active)
        need(_shard_identity(fd, targetfd, shard) == identity, 'seed_dirty_shard_rebound')
        os.fsync(fd); _fast_bound(backend, active)
        need(_shard_identity(fd, targetfd, shard) == identity, 'seed_dirty_shard_rebound')
    for fd in active:
        _fast_bound(backend, active); os.fsync(fd); _fast_bound(backend, active)
    _bound(backend)


def _copy_batch(backend, sourcefds, targetfd, rows, origins, logical, offset):
    """At most64files/4MiB; one oversized bounded file alone. No time reset."""
    _bound(backend); started = time.monotonic(); dirty = {}; count = size = 0
    try:
        while offset < len(rows):
            row = rows[offset]
            if count and (count >= BATCH_FILES or size + row['size'] > BATCH_BYTES
                          or time.monotonic() - started >= BATCH_SECONDS): break
            _end(backend); source = origins[(logical, row['storage_key'])]
            _copy_file(backend, sourcefds[source], targetfd, row, dirty)
            count += 1; size += row['size']; offset += 1
        _copy_barrier(backend, targetfd, dirty)
        return offset
    finally:
        for fd, _ in dirty.values(): os.close(fd)


def _write_control(backend, fd, name, raw, uid, gid, mode):
    _bound(backend)
    out = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, mode, dir_fd=fd)
    try:
        _empty_xattrs(fd); _empty_xattrs(out)
        os.fchown(out, uid, gid); _empty_xattrs(out)
        os.fchmod(out, mode); _empty_xattrs(out); pending = memoryview(raw)
        while pending:
            _bound(backend); count = os.write(out, pending); need(count > 0, 'seed_control_zero_write'); pending = pending[count:]
        os.fsync(out); _empty_xattrs(out); _empty_xattrs(fd); _bound(backend); os.lseek(out, 0, os.SEEK_SET)
        need(os.read(out, len(raw) + 1) == raw, 'seed_control_readback')
        held = os.fstat(out); current = os.stat(name, dir_fd=fd, follow_symlinks=False)
        need(signature(held) == signature(current) and held.st_uid == uid and held.st_gid == gid
             and stat.S_IMODE(held.st_mode) == mode and held.st_nlink == 1, 'seed_control_identity')
        os.fsync(fd); _bound(backend)
        _empty_xattrs(out); _empty_xattrs(fd)
    finally:
        os.close(out)


def _marker(backend, op, cfg, role, manifest, references):
    target = cfg['targets'][role]; fd = backend._initial_blob_seed_fds['root-' + role][0]
    st = os.fstat(fd)
    correspondence = digest(canonical([r for r in references if r['role'] == role.split('_')[0]]))
    marker = {'schema_version': 1, 'kind': 'openlegal_blob_seed', 'managed_epoch': cfg['managed_epoch'],
        'seed_operation': op.operation_id, 'role': role, 'manifest_sha256': manifest,
        'correspondence_sha256': correspondence, 'filesystem_id': target['parent']['filesystem_id']}
    raw = canonical(marker); _write_control(backend, fd, blobs.MARKER, raw, 0, 10004, 0o640)
    if role == 'cache_replica':
        _bound(backend); os.mkdir(blobs.GUARDS, 0o700, dir_fd=fd)
        guard = os.open(blobs.GUARDS, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
        try:
            _empty_xattrs(guard)
            os.fchown(guard, 10004, 10004); _empty_xattrs(guard)
            os.fchmod(guard, 0o700); _empty_xattrs(guard)
            _write_control(backend, guard, '.directory.lock', b'', 10004, 10004, 0o600)
        finally: os.close(guard)
        os.fsync(fd); _bound(backend)
    return {'role': role, 'host_path': target['host_path'], 'container_path': target['container_path'],
        'root_dev': st.st_dev, 'root_ino': st.st_ino, 'root_uid': st.st_uid, 'root_gid': st.st_gid,
        'root_mode': stat.S_IMODE(st.st_mode), 'kernel_filesystem_id': os.fstatvfs(fd).f_fsid,
        'filesystem_id': marker['filesystem_id'], 'marker_sha256': digest(raw),
        'manifest_sha256': manifest, 'reference_sha256': correspondence}


def run(backend, shared_lock, inventory_ref, authority_ref):
    need(type(backend) is driver.Backend and type(shared_lock) is journals.SharedLock
         and os.geteuid() == 0 and not any(callable(v) for v in vars(backend).values())
         and not getattr(backend, '_initial_blob_seed_started', False), 'seed_live_native_backend')
    _end(backend)
    cfg, auth, raw, authraw, dc, inv, template, draw, invraw, daraw = _load(inventory_ref, authority_ref)
    need(shared_lock.path == Path(inv['shared_lock']['path']) and shared_lock.identity == inv['shared_lock']['identity'],
         'seed_shared_lock')
    j, work, old, baseline = _live_context(backend, shared_lock, cfg, dc, inv, template, draw, invraw, daraw)
    backend._initial_blob_seed_started = True; backend._initial_blob_seed_fds = {}
    backend._initial_blob_seed_native_fds = []
    j.begin(PHASE)
    frozen = canonical(work)
    def seal():
        _end(backend); shared_lock.check(); driver._writer_fds(backend)
        need(driver._reference(inventory_ref) == raw and driver._reference(authority_ref) == authraw
             and driver._reference(cfg['driver_inventory']) == draw and driver._reference(dc['legacy_inventory']) == invraw
             and driver._reference(cfg['driver_authority']) == daraw and canonical(work) == frozen
             and time.time() < auth['valid_until'], 'seed_sealed_input_changed')
        driver._source_seal(dc['source_graph']); private_bytes(Path(__file__).resolve(), cfg['source_sha256'])
        driver._docker_binding(backend, cfg['operation_id']); _bound(backend); j.check()
    graphsha = digest(canonical({'driver': dc['source_graph'], 'initial_blob_seed.py': cfg['source_sha256']}))
    op = Operation(cfg['operation_id'], 'strict_prepare', PHASE, work, digest(raw), graphsha, digest(authraw),
        j, Path(inv['backup_root']['path']), {}, seal, freeze_authority(auth))
    try:
        for role, source in cfg['sources'].items():
            _hold(backend, 'source-' + role, source['path'], source['identity'], 10004)
        for role, target in cfg['targets'].items():
            private_parents(target['parent']['path'])
            fd = _hold(backend, 'parent-' + role, target['parent']['path'], target['parent']['identity'], 0)
            need(not os.listdir(fd) and not os.path.lexists(target['host_path']), 'seed_prepared_parent_not_empty')
        need(len({(os.fstat(backend._initial_blob_seed_fds['parent-' + role][0]).st_dev,
                   os.fstat(backend._initial_blob_seed_fds['parent-' + role][0]).st_ino) for role in ROLES}) == 4,
             'seed_prepared_parent_alias')
        op.seal(); first = _fresh(backend, op, old)
        media = _qualify_media(backend, op, cfg, empty=True)
        source = _scan_sources(backend, cfg, baseline); union, origins = _union(source)
        before = _references(backend, op, 'before'); references = before['rows']
        union_maps = {kind: {row['storage_key']: row for row in rows} for kind, rows in union.items()}
        for row in references:
            need(union_maps[row['role']].get(row['storage_key']) == {k: v for k, v in row.items() if k != 'role'},
                 'seed_sql_reference_missing_or_conflicting')
        union_pages = {}
        for logical, rows in union.items():
            union_pages[logical] = []
            for offset in range(0, max(1, len(rows)), preservation.PAGE_SIZE):
                union_pages[logical].append(_saved(backend, op, 'union-' + logical + '-%04d' % offset,
                    {'offset': offset, 'rows': rows[offset:offset + preservation.PAGE_SIZE]}))
        _saved(backend, op, 'copy-intent', {'managed_epoch': cfg['managed_epoch'], 'media_profile': cfg['media_profile'], 'media': media,
            'union_pages': union_pages, 'source_manifest_sha256': {k: v['permanent_sha256'] for k, v in source.items()},
            'reference_pages': before['pages'], 'reference_source_rows': before['source_rows'],
            'preserves_all_unreferenced_generations': True, 'original_roots_modified': False})
        manifests = {}
        for role, target in cfg['targets'].items():
            _bound(backend); parent = backend._initial_blob_seed_fds['parent-' + role][0]
            rows = union[role.split('_')[0]]
            blobs._space(parent, sum(row['size'] for row in rows), cfg['capacity_reserve_bytes'])
            os.mkdir('data', 0o700, dir_fd=parent)
            temporary = os.open('data', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
            try:
                _empty_xattrs(temporary)
                os.fchown(temporary, 10004, 10004); _empty_xattrs(temporary)
                os.fchmod(temporary, 0o700); _empty_xattrs(temporary)
                os.fsync(temporary); os.fsync(parent); _empty_xattrs(temporary)
                created = os.fstat(temporary)
            finally: os.close(temporary)
            targetfd = _hold(backend, 'root-' + role, target['host_path'],
                {'dev': created.st_dev, 'ino': created.st_ino}, owner=10004)
            need(created.st_dev == os.fstat(parent).st_dev
                 and os.fstatvfs(targetfd).f_fsid == target['parent']['kernel_filesystem_id'],
                 'seed_new_root_media_rebound')
            sourcefds = {name: backend._initial_blob_seed_fds['source-' + name][0] for name in SOURCE_ROLES}
            offset = 0
            while offset < len(rows):
                offset = _copy_batch(backend, sourcefds, targetfd, rows, origins, role.split('_')[0], offset)
            _bound(backend)
            _metadata_tree(backend, target['host_path'], new=True)
            actual = blobs.blob_inventory(target['host_path'], deadline=backend._driver_deadline, controls=False, online=False)
            _metadata_tree(backend, target['host_path'], new=True)
            _bound(backend)
            need(actual['permanent'] == rows and not actual['health'] and not actual['controls'], 'seed_copy_manifest')
            manual._reference_fd(targetfd, [r for r in references if r['role'] == role.split('_')[0]], backend._driver_deadline)
            manifests[role] = actual['permanent_sha256']
            blobs._space(parent, 0, cfg['capacity_reserve_bytes'])
            _saved(backend, op, 'copy-verified-' + role, {'root_identity': actual['root_identity'],
                'permanent_sha256': actual['permanent_sha256'], 'generations': len(rows),
                'regular_bytes': actual['regular_bytes'], 'private_witness_sha256': actual['private_witness_sha256'],
                'all_bytes_read_back': True})
        _scan_sources(backend, cfg, baseline); _fresh(backend, op, old)
        _qualify_media(backend, op, cfg, empty=False)
        after = _references(backend, op, 'after')
        need(after['rows'] == references and after['source_rows'] == before['source_rows'], 'seed_sql_references_changed')
        for role, target in cfg['targets'].items():
            _bound(backend)
            _metadata_tree(backend, target['host_path'], new=True)
            actual = blobs.blob_inventory(target['host_path'], deadline=backend._driver_deadline, controls=False, online=False)
            _metadata_tree(backend, target['host_path'], new=True)
            _bound(backend)
            need(actual['permanent'] == union[role.split('_')[0]], 'seed_late_destination_changed')
            manual._reference_fd(backend._initial_blob_seed_fds['root-' + role][0],
                [r for r in references if r['role'] == role.split('_')[0]], backend._driver_deadline)
        _saved(backend, op, 'all-copies-correspondence', {'managed_epoch': cfg['managed_epoch'],
            'manifests': manifests, 'references_sha256': digest(canonical(references)),
            'reference_pages_before': before['pages'], 'reference_pages_after': after['pages'],
            'originals_unchanged': True, 'all_four_copies_verified': True})
        roots = []
        for role in ROLES:
            op.seal(); roots.append(_marker(backend, op, cfg, role, manifests[role], references))
        for row in roots:
            _bound(backend)
            _metadata_tree(backend, row['host_path'], new=True)
            actual = blobs.blob_inventory(row['host_path'], deadline=backend._driver_deadline, controls=True, online=False)
            _metadata_tree(backend, row['host_path'], new=True)
            _bound(backend)
            need(actual['permanent'] == union[row['role'].split('_')[0]], 'seed_final_manifest_changed')
            expected_controls = {blobs.MARKER}
            if row['role'] == 'cache_replica':
                expected_controls.add(blobs.GUARDS + '/.directory.lock')
            need(not actual['health'] and {r['path'] for r in actual['controls']} == expected_controls,
                 'seed_unexpected_new_controls_or_health')
            fd = backend._initial_blob_seed_fds['root-' + row['role']][0]
            marker, _ = blobs._regular(fd, blobs.MARKER, 0, (0o640,), 16384)
            need(digest(marker) == row['marker_sha256'], 'seed_final_marker_changed')
        _scan_sources(backend, cfg, baseline); _qualify_media(backend, op, cfg, empty=False)
        for role in ROLES:
            blobs._space(backend._initial_blob_seed_fds['parent-' + role][0], 0, cfg['capacity_reserve_bytes'])
        _fresh(backend, op, old)
        docroots = [{k: row[k] for k in ('container_path', 'filesystem_id', 'kernel_filesystem_id',
            'marker_sha256', 'role', 'root_dev', 'root_gid', 'root_ino', 'root_mode', 'root_uid')} for row in roots]
        document = {'boot_id': inv['host']['boot_id'], 'kind': 'openlegal_blob_seed_binding',
            'managed_epoch': cfg['managed_epoch'], 'roots': sorted(docroots, key=lambda r: r['role']), 'schema_version': 1}
        need(len(canonical(document)) <= MAX_INDEX, 'seed_binding_document_bound')
        binding = digest(b'openlegal-blob-seed-binding-v1\n' + canonical(document))
        result = {'kind': 'openlegal_initial_blob_seed_result', 'operation_id': op.operation_id,
            'managed_epoch': cfg['managed_epoch'], 'media_profile': cfg['media_profile'], 'blob_binding_sha256': binding, 'roots': roots,
            'union_pages': union_pages, 'reference_pages': after['pages'], 'all_original_generations_preserved': True,
            'postgres_replica_created': False, 'roles_created': False, 'workload_started': False,
            'provider_called': False, 'ready': False, 'automatic_retry': False}
        need(len(canonical(result)) <= MAX_INDEX, 'seed_result_index_bound')
        _saved(backend, op, 'complete', result); op.seal(); _bound(backend)
        return result
    except BaseException:
        try:
            _saved(backend, op, 'failed', {'status': 'FAILED_OR_UNKNOWN', 'automatic_retry': False,
                'automatic_cleanup': False, 'original_writes_permitted': False,
                'source_integrity': 'UNKNOWN', 'ready': False})
        except BaseException:
            pass
        raise
