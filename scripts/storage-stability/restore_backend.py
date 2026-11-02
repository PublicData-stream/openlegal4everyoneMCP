"""Isolated, disposable verification of a sealed PostgreSQL/Blob/index backup.

Importing this module has no effects. The workflow must already authenticate all
previous receipts, actual exits, dual journals, authority and physical quiescence.
Only archive copies are read. No production container or SQL endpoint is a target.
All mutations, including the deliberately local replication-policy exception,
are confined to newly created /tmp copies and label-bound disposable containers.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import re
import shutil
import stat
import time
from pathlib import Path

from copy_tree import copy_tree, attributes, metadata, signature
from interface import Refused, need, REQUIRED_TRUE
from inventory import canonical, decode, digest, valid_uuid, private_bytes, all_datasets

HEX = re.compile(r'[0-9a-f]{64}\Z')
IMAGE = re.compile(r'(?:[a-z0-9./:_-]+@)?sha256:[0-9a-f]{64}\Z')
IDENTIFIER = re.compile(r'[a-z][a-z0-9_]{0,62}\Z')
DOCKER = ['--host', 'unix:///var/run/docker.sock']
PURPOSE = 'isolated_restore'
LIMIT = 1200
PG_TARGET = '/var/lib/postgresql'
PG_CONFIG = """listen_addresses='127.0.0.1'
port=5432
unix_socket_directories='/tmp'
hba_file='/fixture/pg_hba.conf'
ident_file='/fixture/pg_ident.conf'
ssl=off
archive_mode=off
archive_command=''
restore_command=''
archive_cleanup_command=''
recovery_end_command=''
primary_conninfo=''
primary_slot_name=''
shared_preload_libraries=''
session_preload_libraries=''
local_preload_libraries=''
ssl_passphrase_command=''
synchronous_standby_names=''
synchronous_commit=on
statement_timeout='30s'
lock_timeout='1s'
transaction_timeout='35s'
idle_in_transaction_session_timeout='5s'
"""

# Keep the container init alive until its physical PID/cgroup receipt is sealed.
# The shell then execs the exact native server in place (same PID/start ticks).
# The fixed timeout is logical; no claim is made that KILL can end kernel D-state.
INDEX_START_GATE = ("timeout 120 /bin/sh -c 'while [ ! -f /restore-control/start ]; "
                    "do sleep 0.1; done' || exit $?; "
                    "exec /usr/local/bin/openlegal-server --rebuild-corpus-index /fixture/server.toml")


def _shape(value, keys, code):
    need(type(value) is dict and set(value) == set(keys), code)


def _relative(value):
    need(type(value) is str and len(value) <= 4096 and '\x00' not in value,
         'restore_relative_type')
    if value == '.':
        return Path('.')
    p = Path(value)
    need(value and not p.is_absolute() and str(p) == value
         and all(part not in ('.', '..') for part in p.parts), 'restore_relative_path')
    return p


def validate(config, inventory):
    """Validate the exact, non-executable restore input language."""
    _shape(config, ('schema_version', 'primary', 'standby', 'blobs', 'index',
                    'dictionary', 'server_image', 'runtime_role'), 'restore_policy_shape')
    need(type(config['schema_version']) is int and config['schema_version'] == 1,
         'restore_policy_version')
    need(type(config['server_image']) is str and IMAGE.fullmatch(config['server_image'])
         and config['server_image'] == inventory['linux']['serving_image'], 'restore_server_image')
    need(type(config['runtime_role']) is str and IDENTIFIER.fullmatch(config['runtime_role'])
         and config['runtime_role'] != 'postgres', 'restore_runtime_role')

    def source(value, side=None, role=None):
        if side is None:
            need(type(value) is dict and set(value) in (
                {'dataset', 'relative'}, {'tree', 'relative'}), 'restore_source_shape')
            side = 'dataset' if 'dataset' in value else 'tree'
        _shape(value, (side, 'relative'), 'restore_source_shape')
        _relative(value['relative'])
        need(type(value[side]) is str and len(value[side]) <= 256, 'restore_source_name')
        records = all_datasets(inventory) if side == 'dataset' else inventory['ssd_trees']
        names = {x['name'] for x in records}
        need(value[side] in names, 'restore_unknown_source')
        if role is not None:
            need(value[side] in inventory['coverage'][role], 'restore_postgres_coverage')
            if side == 'tree':
                tree = next(t for t in records if t['name'] == value[side])
                need(role in tree['roles'], 'restore_postgres_tree_role')
        return side, value[side]
    pg_sources = {}
    for side in ('primary', 'standby'):
        _shape(config[side], ('data', 'wal'), 'restore_postgres_policy')
        data = source(config[side]['data'], role='postgres')
        pg_sources[side] = {data}
        if config[side]['wal'] is not None:
            pg_sources[side].add(source(config[side]['wal'], role='wal'))
        else:
            need(data[1] in inventory['coverage']['wal'], 'restore_inline_wal_coverage')
    need(not pg_sources['primary'] & pg_sources['standby'], 'restore_postgres_sources_alias')
    need({'dataset' if 'dataset' in config[side]['data'] else 'tree'
          for side in ('primary', 'standby')}
         == {'dataset', 'tree'}, 'restore_postgres_independent_media')
    _shape(config['blobs'], ('cache', 'corpus'), 'restore_blob_roles')
    for role, sides in config['blobs'].items():
        _shape(sides, ('hdd', 'ssd'), 'restore_blob_sides')
        source(sides['hdd'], 'dataset')
        source(sides['ssd'], 'tree')
        need(sides['hdd']['dataset'] in inventory['coverage'][role]
             and sides['ssd']['tree'] in inventory['coverage'][role], 'restore_blob_coverage')
        tree = next(t for t in inventory['ssd_trees'] if t['name'] == sides['ssd']['tree'])
        need(role in tree['roles'], 'restore_blob_tree_role')
    source(config['index'], 'dataset')
    source(config['dictionary'], 'dataset')
    for side in ('primary', 'standby'):
        need(IMAGE.fullmatch(inventory['linux']['postgres'][side]['image_id']) is not None,
             'restore_postgres_image')
    return config


def _full_exports(op):
    """Every immutable original and active extra has its own current export."""
    datasets = all_datasets(op.inventory)
    expected = {row['name']: row['guid'] for row in datasets}
    exports = op.previous['export_snapshots']['facts']
    need(len(expected) == len(datasets) and exports['full_self_contained_export'] is True
         and exports['parent_chains'] == [] and set(exports['exports']) == set(expected),
         'restore_requires_full_independent_export')
    snapshots = op.previous['snapshot_same_pool']['facts']['snapshots']
    need(type(snapshots) is list and len(snapshots) == len(expected)
         and {row['dataset'] for row in snapshots} == set(expected),
         'restore_snapshot_dataset_coverage')
    for snapshot in snapshots:
        need(snapshot['guid'] == expected[snapshot['dataset']]
             and snapshot['snapshot_guid'] == exports['exports'][snapshot['dataset']]['snapshot_guid'],
             'restore_snapshot_export_identity')
    return datasets


def _postgres_layout(op, config):
    """Bind each archived selector to the authenticated actual stopped PGDATA.

    The producer obtains data_directory from a protected metadata function,
    maps the actual container's bind mounts and resolves its pg_wal link before
    shutdown. This consumer never infers a live path from archive coverage.
    """
    layout = op.previous['stop_postgres']['facts'].get('postgres_layout')
    _shape(layout, ('primary', 'standby'), 'restore_postgres_layout_shape')
    def host_path(spec):
        dataset = 'dataset' in spec
        kind = 'dataset' if dataset else 'tree'
        records = all_datasets(op.inventory) if dataset else op.inventory['ssd_trees']
        row = next(value for value in records if value['name'] == spec[kind])
        base = row['mountpoint' if dataset else 'path']
        need(type(base) is str and Path(base).is_absolute() and str(Path(base)) == base
             and '..' not in Path(base).parts, 'restore_postgres_layout_root')
        return str(Path(base) / _relative(spec['relative']))
    for side in ('primary', 'standby'):
        row = layout[side]
        _shape(row, ('container_id', 'data_host_path', 'wal_host_path', 'source_data', 'source_wal'),
               'restore_postgres_layout_side')
        expected = config[side]
        data = host_path(expected['data'])
        wal = host_path(expected['wal']) if expected['wal'] is not None else str(Path(data) / 'pg_wal')
        need(row['container_id'] == op.inventory['linux']['postgres'][side]['container_id']
             and row['source_data'] == expected['data'] and row['source_wal'] == expected['wal']
             and row['data_host_path'] == data and row['wal_host_path'] == wal,
             'restore_postgres_layout_not_actual_source')
    return layout


def _directory(path):
    path = Path(path)
    need(path.is_absolute() and path.resolve() == path, 'restore_directory_path')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        info = os.fstat(fd)
        current = path.lstat()
        need((info.st_dev, info.st_ino) == (current.st_dev, current.st_ino), 'restore_directory_rebound')
        return fd, info
    except BaseException:
        os.close(fd)
        raise


def _record(path, manifest, roles):
    fd, info = _directory(path)
    os.close(fd)
    return {'path': str(path), 'identity': {'dev': info.st_dev, 'ino': info.st_ino},
            'uid': info.st_uid, 'gid': info.st_gid, 'mode': stat.S_IMODE(info.st_mode),
            'max_files': max(1, len(manifest['entries']) + 1),
            'max_bytes': max(1, sum(row.get('size', 0) for row in manifest['entries'])),
            'roles': roles, 'symlinks': {row['path']: row['target'] for row in manifest['entries']
                                       if row['kind'] == 'symlink'}}


def _selection(root, manifest, relative):
    relative = _relative(relative)
    entries = manifest['entries']
    need(type(entries) is list and len(entries) <= 200000, 'restore_manifest_bound')
    paths = [row['path'] for row in entries]
    need(len(paths) == len(set(paths)) and paths == sorted(paths), 'restore_manifest_paths')
    for row in entries:
        _relative(row['path'])
        need(row['kind'] in ('directory', 'file', 'metadata_file', 'symlink'), 'restore_manifest_kind')
    if relative == Path('.'):
        return Path(root), {'entries': copy.deepcopy(entries), 'root_metadata': manifest['root_metadata']}
    prefix = str(relative) + '/'
    parent = next((row for row in entries if row['path'] == str(relative)), None)
    need(parent is not None and parent['kind'] == 'directory', 'restore_source_subdirectory')
    # No ancestor may be a symlink, including an otherwise approved archive link.
    for ancestor in relative.parents:
        if ancestor != Path('.'):
            need(any(row['path'] == str(ancestor) and row['kind'] == 'directory' for row in entries),
                 'restore_source_symlink_ancestor')
    selected = [dict(row, path=row['path'][len(prefix):]) for row in entries if row['path'].startswith(prefix)]
    metadata = {key: parent[key] for key in ('uid', 'gid', 'mode', 'mtime_ns', 'xattrs')}
    return Path(root) / relative, {'entries': selected, 'root_metadata': metadata}


def _manifest_decode(raw):
    need(type(raw) is bytes and len(raw) <= 256 * 1024**2, 'restore_manifest_json_bound')
    def pairs(rows):
        value = {}
        for key,item in rows:
            need(key not in value, 'restore_manifest_duplicate')
            value[key] = item
        return value
    def finite(value):
        number = float(value)
        need(math.isfinite(number), 'restore_manifest_nonfinite')
        return number
    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_float=finite,
                           parse_constant=lambda _: (_ for _ in ()).throw(Refused('restore_manifest_nonfinite')))
    except (ValueError,RecursionError,UnicodeError):
        raise Refused('restore_manifest_json') from None
    return value


def _verify_source(record, expected, deadline):
    """Full read-only byte+metadata witness, with no symlink traversal."""
    root, root_info = _directory(record['path'])
    actual, legacy, witnesses = [], [], {}
    source_paths = {row['path']: row for row in expected['entries']}
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
    total = 0
    def walk(fd, prefix, depth):
        nonlocal total
        need(depth <= 64 and time.monotonic() < deadline, 'restore_source_deadline')
        start = os.fstat(fd)
        names = sorted(os.listdir(fd))
        for name in names:
            need(time.monotonic() < deadline and len(actual) < record['max_files'], 'restore_source_bound')
            rel = prefix + name
            need(rel in source_paths, 'restore_source_extra_path')
            before = os.stat(name, dir_fd=fd, follow_symlinks=False)
            witnesses[rel] = signature(before)
            need(before.st_dev == root_info.st_dev, 'restore_nested_source_mount')
            old = source_paths[rel]
            if stat.S_ISLNK(before.st_mode):
                need(old['kind'] == 'symlink' and record['symlinks'].get(rel) == os.readlink(name,dir_fd=fd),
                     'restore_source_link')
                need(not os.listxattr('/proc/self/fd/'+str(fd)+'/'+name,follow_symlinks=False),
                     'restore_source_symlink_xattrs')
                row = {'path':rel,'kind':'symlink','target':os.readlink(name,dir_fd=fd),
                       'uid':before.st_uid,'gid':before.st_gid,'mode':stat.S_IMODE(before.st_mode),
                       'mtime_ns':before.st_mtime_ns}
            else:
                need(stat.S_ISDIR(before.st_mode) or stat.S_ISREG(before.st_mode), 'restore_source_special')
                child = os.open(name, flags | (os.O_DIRECTORY if stat.S_ISDIR(before.st_mode) else 0),dir_fd=fd)
                try:
                    need(signature(os.fstat(child)) == signature(before), 'restore_source_rebound')
                    row = dict(path=rel,kind='directory' if stat.S_ISDIR(before.st_mode) else old['kind'],
                               **metadata(before,attributes(child)))
                    if stat.S_ISDIR(before.st_mode):
                        walk(child,rel+'/',depth+1)
                    else:
                        need(before.st_nlink == 1 and old['kind'] in ('file','metadata_file'), 'restore_source_hardlink')
                        total += before.st_size
                        need(total <= record['max_bytes'], 'restore_source_bytes')
                        h = hashlib.sha256(); count = 0
                        while True:
                            need(time.monotonic() < deadline, 'restore_source_deadline')
                            block = os.read(child,1024*1024)
                            if not block:break
                            count += len(block); need(count <= before.st_size,'restore_source_growth'); h.update(block)
                        need(count == before.st_size,'restore_source_size')
                        row.update(size=count,sha256=h.hexdigest())
                    need(signature(os.fstat(child)) == signature(before), 'restore_source_changed')
                finally:os.close(child)
            need(row == old and signature(os.stat(name,dir_fd=fd,follow_symlinks=False)) == signature(before),
                 'restore_source_manifest_changed')
            actual.append(row)
            item = {key:row[key] for key in ('path','uid','gid','mode')}
            item['kind'] = 'file' if row['kind'] == 'metadata_file' else row['kind']
            for key in ('target','size','sha256'):
                if key in row:item[key]=row[key]
            legacy.append(item)
        need(names == sorted(os.listdir(fd)) and signature(os.fstat(fd)) == signature(start),
             'restore_source_directory_changed')
    try:
        need(metadata(root_info,attributes(root)) == expected['root_metadata'], 'restore_source_root_metadata')
        walk(root,'',0)
        need(sorted(actual,key=lambda row:row['path']) == expected['entries'], 'restore_source_missing_path')
        # An earlier file can change while a later file is being hashed. Retain
        # private inode/ctime signatures, then rebind every descriptor path after
        # ALL hashes; restoring mtime cannot conceal that intervening write.
        for relative,stamp in witnesses.items():
            need(time.monotonic() < deadline, 'restore_source_deadline')
            fd = os.dup(root)
            try:
                parts = Path(relative).parts
                parent = []
                for component in parts[:-1]:
                    parent.append(component)
                    child = os.open(component,flags | os.O_DIRECTORY,dir_fd=fd)
                    os.close(fd);fd=child
                    need(signature(os.fstat(fd)) == witnesses['/'.join(parent)],
                         'restore_source_final_ancestor_changed')
                need(signature(os.stat(parts[-1],dir_fd=fd,follow_symlinks=False)) == stamp,
                     'restore_source_final_coherence')
            finally:os.close(fd)
        need(signature(os.fstat(root)) == signature(root_info), 'restore_source_final_root_changed')
        now = Path(record['path']).lstat()
        need((now.st_dev,now.st_ino)==(root_info.st_dev,root_info.st_ino),'restore_source_root_changed')
    finally:os.close(root)
    legacy.sort(key=lambda row:row['path'])
    return {'sha256':digest(canonical(legacy)), 'entries':len(actual),'regular_bytes':total,
            'full_manifest_sha256':digest(canonical(expected))}


def _source(op, spec):
    if 'dataset' in spec:
        row = op.previous['export_snapshots']['facts']['exports'][spec['dataset']]
        snapshot = next(s for s in op.previous['snapshot_same_pool']['facts']['snapshots']
                        if s['dataset'] == spec['dataset'])
        need(row['snapshot_guid'] == snapshot['snapshot_guid'], 'restore_export_guid')
        root = Path(row['destination'])
        need(root.parent == op.chain_root / 'originals', 'restore_export_namespace')
        manifest = row['manifest']
    else:
        pin = op.previous['copy_ssd']['manifests'][spec['tree']]
        need(type(pin) is str and HEX.fullmatch(pin), 'restore_ssd_manifest_pin')
        raw = [private_bytes(Path(root['path']) / op.operation_id / 'copy_ssd' /
                             ('copy-' + spec['tree'] + '.json'), pin, 256 * 1024**2)
               for root in op.inventory['journal_roots']]
        need(len(raw) == 2 and raw[0] == raw[1], 'restore_ssd_manifest_journals')
        root = op.chain_root / 'ssd' / spec['tree']
        manifest = _manifest_decode(raw[0])
    need(manifest['kind'] == 'verified_stopped_tree_copy'
         and manifest['all_bytes_read_back'] is True, 'restore_unverified_copy')
    fd, info = _directory(root)
    os.close(fd)
    need(manifest['destination_identity'] == {'dev': info.st_dev, 'ino': info.st_ino},
         'restore_archive_identity')
    path, selected = _selection(root, manifest, spec['relative'])
    return path, selected


def _copy(op, spec, destination, roles, deadline):
    source, expected = _source(op, spec)
    record = _record(source, expected, roles)
    before = _verify_source(record, expected, deadline)
    result = copy_tree(record, destination, deadline)
    need(result['entries'] == expected['entries'] and result['root_metadata'] == expected['root_metadata'],
         'restore_archive_manifest_changed')
    return record, result, before, expected


def _write_new(path, raw, mode=0o644):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, mode)
    try:
        view = memoryview(raw)
        while view:
            size = os.write(fd, view)
            need(size > 0, 'restore_zero_write')
            view = view[size:]
        os.fchmod(fd, mode)
        os.fsync(fd)
    finally:
        os.close(fd)


def _replace_cloned_file(root, relative, raw):
    # Only fixed inert local-restore files; never accept an input pathname.
    need(relative in ('postgresql.auto.conf', 'standby.signal', 'recovery.signal'), 'restore_replace_name')
    path = root / relative
    if path.exists() or path.is_symlink():
        info = path.lstat()
        need(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, 'restore_cloned_config_type')
        path.unlink()
    if raw is not None:
        _write_new(path, raw, 0o600)
        info = root.stat()
        os.chown(path, info.st_uid, info.st_gid)
    fd, _ = _directory(root)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _prepare_pg(root, manifest, wal, uid):
    need(root.stat().st_uid == uid, 'restore_postgres_data_owner')
    version = root / 'PG_VERSION'
    fd = os.open(version, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        need(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and stream.read(4) == b'18\n',
             'restore_postgres_version')
    links = [row for row in manifest['entries'] if row['kind'] == 'symlink']
    need(all(row['path'] == 'pg_wal' for row in links), 'restore_pg_external_link')
    if links:
        need(len(links) == 1 and wal is not None, 'restore_pg_wal_missing')
        need((root / 'pg_wal').is_symlink(), 'restore_pg_wal_rebound')
        (root / 'pg_wal').unlink()
        os.symlink('/restore-wal', root / 'pg_wal')
        os.lchown(root / 'pg_wal', uid, root.stat().st_gid)
    else:
        need(wal is None, 'restore_unexpected_wal_binding')
    _replace_cloned_file(root, 'postgresql.auto.conf', b'# isolated restore: no inherited settings\n')
    _replace_cloned_file(root, 'standby.signal', None)
    _replace_cloned_file(root, 'recovery.signal', None)


class Actor:
    """One exact owned container; inspect is mandatory before every exec/stop."""
    def __init__(self, backend, op, kind, image, mounts, user, config, network='none', env=()):
        self.backend, self.op, self.kind = backend, op, kind
        self.image, self.mounts, self.user = image, mounts, user
        self.config, self.network, self.env = config, network, list(env)
        self.name = 'openlegal-restore-' + op.operation_id + '-' + kind
        self.cid = None
        self.groups = None
        self.pid_identity = None
        self.seq = 0
        self.created = False
        self.image_id = None
        self.expected_labels = None
        self.expected_env = None
        self.entrypoint = None
        self.arguments = None
        self.identity_sealed = False

    def run(self, args, **kwargs):
        return self.backend._run(self.op, 'docker', DOCKER + args, **kwargs)

    def evidence(self, label, value):
        self.seq += 1
        self.backend._artifact(self.op, f'restore-{self.kind}-{label}-{self.seq:04d}', value)

    def inspect(self):
        need(self.created, 'restore_inspect_before_create')
        target = self.cid or self.name
        raw = self.run(['inspect', target], record=False)
        need(type(raw) is bytes and len(raw) <= 256 * 1024, 'restore_inspect_bound')
        value = decode(raw)
        need(type(value) is list and len(value) == 1, 'restore_inspect_shape')
        item = value[0]
        cid = item['Id']
        need(type(cid) is str and HEX.fullmatch(cid) and (self.cid is None or cid == self.cid),
             'restore_container_identity')
        need(cid not in [self.op.inventory['linux']['postgres'][x]['container_id'] for x in ('primary', 'standby')],
             'restore_production_container')
        need(item['Name'] == '/' + self.name and item['Image'] == self.image_id
             and item['Config']['Labels'] == self.expected_labels, 'restore_container_binding')
        host = item['HostConfig']
        need(host['NetworkMode'] == self.network and host['ReadonlyRootfs'] is True
             and host['Privileged'] is False and sorted(host['CapDrop']) == ['ALL']
             and not host.get('CapAdd') and host['Memory'] == 512 * 1024**2
             and host['NanoCpus'] == 1_000_000_000 and host['PidsLimit'] == 128
             and host['RestartPolicy']['Name'] == 'no' and not host.get('PortBindings')
             and not host.get('Devices') and not host.get('PidMode')
             and 'no-new-privileges' in host['SecurityOpt']
             and item['Config']['User'] == self.user
             and item['Config']['Entrypoint'] == [self.entrypoint]
             and item['Config']['Cmd'] == self.arguments
             and item['Config']['Healthcheck']['Test'] == ['NONE']
             and sorted(item['Config']['Env']) == self.expected_env,
             'restore_container_isolation')
        actual = sorted([{'source': m['Source'], 'destination': m['Destination'], 'rw': m['RW']}
                         for m in item['Mounts'] if m['Type'] == 'bind'], key=lambda m: m['destination'])
        need(actual == sorted(self.mounts, key=lambda m: m['destination'])
             and all(m['Type'] in ('bind', 'tmpfs') for m in item['Mounts'])
             and all(m['Destination'] == '/tmp' for m in item['Mounts'] if m['Type'] == 'tmpfs'),
             'restore_container_mounts')
        need(not item['NetworkSettings'].get('Ports'), 'restore_published_ports')
        self.cid = cid
        projection = {'id': cid, 'kind': self.kind, 'image': item['Image'], 'network': self.network,
                      'state': item['State'], 'mounts': actual, 'labels': self.labels()}
        self.evidence('inspect', projection)
        return item

    def labels(self):
        return {'openlegal.operation': self.op.operation_id, 'openlegal.purpose': PURPOSE,
                'openlegal.actor': self.kind}

    def create(self, entrypoint, args):
        need(self.run(['ps', '--all', '--filter', 'name=^/' + self.name + '$', '--format', '{{.ID}}']) == b'',
             'restore_container_name_exists')
        # Pin image existence/ID before create: Docker create must never pull.
        image = decode(self.run(['image', 'inspect', self.image], record=False))
        need(type(image) is list and len(image) == 1 and HEX.fullmatch(image[0]['Id'].removeprefix('sha256:'))
             and (image[0]['Id'] == self.image if self.image.startswith('sha256:')
                  else self.image in (image[0].get('RepoDigests') or [])), 'restore_image_unavailable')
        self.image_id = image[0]['Id']
        inherited = image[0].get('Config', {}).get('Labels') or {}
        need(type(inherited) is dict and len(inherited) <= 128
             and all(type(k) is str and type(v) is str and len(k) <= 1024 and len(v) <= 4096
                     for k,v in inherited.items()), 'restore_image_labels')
        self.expected_labels = dict(inherited, **self.labels())
        image_env = image[0].get('Config', {}).get('Env') or []
        need(type(image_env) is list and len(image_env) <= 128
             and all(type(v) is str and '=' in v and len(v) <= 16384 for v in image_env), 'restore_image_env')
        merged = {v.split('=',1)[0]:v for v in image_env}
        merged.update({v.split('=',1)[0]:v for v in self.env})
        self.expected_env = sorted(merged.values())
        self.entrypoint, self.arguments = entrypoint, list(args)
        command = ['create', '--pull=never', '--name', self.name, '--network', self.network,
                   '--no-healthcheck', '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges',
                   '--memory=512m', '--cpus=1', '--pids-limit=128', '--restart=no', '--user', self.user,
                   '--tmpfs', '/tmp:rw,noexec,nosuid,nodev,size=67108864']
        for key, value in self.labels().items():
            command += ['--label', key + '=' + value]
        for mount in self.mounts:
            source = Path(mount['source'])
            need(source.is_relative_to(self.config) and source.resolve() == source, 'restore_mount_outside_workspace')
            command += ['--mount', 'type=bind,src=' + str(source) + ',dst=' + mount['destination']
                        + (',readonly' if not mount['rw'] else '')]
        command += ['--entrypoint', entrypoint]
        for value in self.env:
            command += ['--env', value]
        command += [self.image, *args]
        self.evidence('create-intent', {'arguments_sha256': digest(canonical(command)),
                                      'name': self.name, 'labels': self.labels()})
        # Failed create ACK still gets exact name/spec validation in cleanup.
        self.created = True
        raw = self.run(command, timeout=60)
        need(type(raw) is bytes and re.fullmatch(b'[0-9a-f]{64}\n', raw), 'restore_create_ack')
        self.cid = raw[:-1].decode('ascii')
        self.inspect()

    def start(self):
        self.inspect()
        self.run(['start', self.cid], timeout=30)
        item = self.inspect()
        need(item['State']['Running'] and item['State']['Pid'] > 0, 'restore_not_running')
        from linux_backend import process_cgroup
        pid = item['State']['Pid']
        self.groups = process_cgroup(pid)
        need(bool(self.groups) and all(g.split(':', 2)[-1] != '/' for g in self.groups),
             'restore_missing_cgroup')
        raw = Path(f'/proc/{pid}/stat').read_text()
        self.pid_identity = (pid, raw.rsplit(')', 1)[1].split()[19])
        self.evidence('owned-process', {'pid': pid, 'start_ticks': self.pid_identity[1],
                                      'cgroup': self.groups})
        self.identity_sealed = True

    def ready(self):
        # A fixed read-only readiness command always returns its status as data;
        # failures of the owned local Docker CLI itself are never swallowed.
        deadline = time.monotonic() + 60
        database = self.op.inventory['linux']['postgres']['database']
        need(IDENTIFIER.fullmatch(database), 'restore_database_name')
        script = ('if pg_isready -q -t 2 -h /tmp -p 5432 -U postgres -d "$1"; '
                  "then printf 'ready\\n'; else printf 'waiting\\n'; fi")
        while time.monotonic() < deadline:
            item = self.inspect()
            need(item['State']['Running'], 'restore_postgres_exited_during_startup')
            result = self.run(['exec', '--user', self.user, self.cid,
                               '/bin/sh', '-c', script, 'openlegal-restore-ready', database], timeout=5)
            need(result in (b'ready\n', b'waiting\n'), 'restore_readiness_shape')
            if result == b'ready\n':
                return
            self.op.seal()
            time.sleep(0.1)
        raise Refused('restore_readiness_deadline')

    def sql(self, statement, readonly=True):
        need(self.kind in ('primary', 'standby'), 'restore_sql_actor')
        item = self.inspect()
        need(item['State']['Running'] and item['State']['Pid'] > 0, 'restore_sql_not_running')
        database = self.op.inventory['linux']['postgres']['database']
        need(IDENTIFIER.fullmatch(database), 'restore_database_name')
        from linux_backend import SQL_SESSION_PREFIX
        prefix = SQL_SESSION_PREFIX + ("BEGIN READ ONLY; " if readonly else "BEGIN; ") + (
            "SET LOCAL statement_timeout='30s'; SET LOCAL lock_timeout='1s'; "
            "SET LOCAL transaction_timeout='35s'; SET LOCAL idle_in_transaction_session_timeout='5s'; ")
        raw = self.run(['exec', '-i', '--user', self.user, '--env', 'PGAPPNAME=openlegal_isolated_restore',
                        self.cid, 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1',
                        '-h', '/tmp', '-p', '5432', '-w', '-U', 'postgres', '-d', database],
                       stdin=(prefix + statement + '; COMMIT;\n').encode(), timeout=40)
        return decode(raw)

    def close(self):
        if not self.created:
            return
        item = self.inspect()
        command_failed = False
        if item['State']['Running']:
            # A lost start ACK may still have started the daemon. Capture its
            # actual physical identity before the fixed cleanup attempt.
            if self.groups is None:
                from linux_backend import process_cgroup
                pid = item['State']['Pid']
                self.groups = process_cgroup(pid)
                need(bool(self.groups) and all(g.split(':',2)[-1] != '/' for g in self.groups),
                     'restore_missing_cleanup_cgroup')
                ticks = Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()[19]
                self.pid_identity = (pid,ticks)
                self.evidence('cleanup-owned-process', {'pid':pid,'start_ticks':ticks,'cgroup':self.groups})
            try:
                self.run(['stop', '--time', '30', self.cid], timeout=40)
            except BaseException:
                # Preserve failure, but still inspect the owned resource and
                # attempt the remaining cleanup. This can never produce PASS.
                command_failed = True
            item = self.inspect()
            if item['State']['Running']:
                try:
                    self.run(['kill', '--signal', 'KILL', self.cid], timeout=15)
                except BaseException:
                    command_failed = True
                item = self.inspect()
        need(not item['State']['Running'] and item['State']['Pid'] == 0, 'restore_still_running')
        good_exit = (item['State']['ExitCode'] == 0 and not item['State']['OOMKilled']
                     and not item['State']['Error'])
        self.evidence('actual-exit', {'actual_container_exit_code': item['State']['ExitCode'],
                                     'oom_killed': item['State']['OOMKilled'],
                                     'daemon_error_present': bool(item['State']['Error'])})
        if self.groups is not None:
            from linux_backend import members
            need(not members(self.groups), 'restore_cgroup_not_closed')
        else:
            # Docker wait/Pid0 alone cannot prove kernel closure of a process
            # whose identity was never observed. Only never-started resources
            # have no runtime process to drain.
            need(item['State']['Status'] == 'created'
                 and item['State']['StartedAt'] == '0001-01-01T00:00:00Z',
                 'restore_unobserved_process_closure')
        if self.pid_identity:
            pid, ticks = self.pid_identity
            try:
                current = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]
            except FileNotFoundError:
                current = None
            need(current != ticks, 'restore_pid_not_closed')
        self.run(['rm', self.cid], timeout=30)
        need(self.run(['ps', '--all', '--filter', 'id=' + self.cid, '--format', '{{.ID}}']) == b'',
             'restore_resource_not_removed')
        self.evidence('closed', {'id': self.cid, 'actual_container_exit_code': item['State']['ExitCode'],
                                 'owned_cgroup_empty': True, 'removed': True,
                                 'cleanup_command_failed':command_failed})
        self.created = False
        need(good_exit and not command_failed, 'restore_exit_not_zero_or_cleanup_failed')


class CloneQueries:
    def __init__(self, actor):
        self.actor = actor

    def _sql(self, op, sql, *, container=None, **kwargs):
        need(op is self.actor.op and container == self.actor.cid, 'restore_query_target')
        return self.actor.sql(sql)


def _lsn(value):
    need(type(value) is str and re.fullmatch(r'[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}', value), 'restore_lsn')
    high, low = value.split('/')
    return (int(high, 16) << 32) | int(low, 16)


def _baseline(backend, op, actor, stopped):
    from linux_backend import LinuxBackend
    proxy = CloneQueries(actor)
    tables = LinuxBackend._fingerprints(proxy, op, container=actor.cid)
    references = LinuxBackend._references(proxy, op, container=actor.cid)
    need(tables == stopped['retained_tables'] and references == stopped['retained_references'],
         'restore_retained_baseline_changed')
    checkpoint = actor.sql("SELECT jsonb_build_object('system_identifier',"
        "(SELECT system_identifier::text FROM pg_control_system()),'wal_lsn',pg_current_wal_insert_lsn()::text,"
        "'timeline',(SELECT timeline_id FROM pg_control_checkpoint()),'index_ack',"
        "(SELECT index_ack FROM openlegal.corpus_control),'watermark',"
        "(SELECT next_event-1 FROM openlegal.corpus_control),'in_recovery',pg_is_in_recovery())")
    sealed = stopped['checkpoint']
    need(checkpoint['system_identifier'] == sealed['system_identifier']
         and type(checkpoint['timeline']) is int and checkpoint['timeline'] >= sealed['timeline']
         and _lsn(checkpoint['wal_lsn']) >= _lsn(sealed['wal_lsn'])
         and checkpoint['index_ack'] == sealed['index_ack']
         and checkpoint['in_recovery'] is False, 'restore_checkpoint_mismatch')
    backend._artifact(op, 'restore-' + actor.kind + '-baseline', {
        'tables': tables, 'references': references, 'checkpoint': checkpoint})
    return checkpoint


def _fixture_config(revision):
    need(re.fullmatch(r'[0-9a-f]{40}', revision), 'restore_revision')
    return f'''[source]
url="https://github.com/PublicData-stream/openlegal4everyoneMCP/tree/{revision}"
[http]
bind="127.0.0.1:18080"
allowed_hosts=["localhost"]
allowed_origins=[]
[webtransport]
bind="127.0.0.1:14433"
certificate="/fixture/unused.crt"
private_key="/fixture/unused.key"
allowed_hosts=["localhost"]
allowed_origins=[]
[health]
bind="127.0.0.1:19090"
[text_diff]
widget_html="/opt/openlegal/widgets/text-diff.html"
[cache]
mode="persistent"
[cache.postgres]
url_env="OPENLEGAL_RESTORE_DATABASE_URL"
tls_mode="plaintext"
replication_mode="local"
[cache.blob]
kind="filesystem"
path="/restore-cache"
[database]
auto_collection=false
blob_path="/restore-corpus"
index_path="/restore-index"
widget_html="/opt/openlegal/widgets/database.html"
mecab_dictionary_path="/restore-dictionary"
'''.encode()


def _release_index_gate(actor, control):
    need(actor.kind == 'rebuild' and actor.identity_sealed and actor.pid_identity is not None
         and bool(actor.groups), 'restore_index_identity_not_sealed')
    item = actor.inspect()
    pid,ticks = actor.pid_identity
    need(item['State']['Running'] is True and item['State']['Pid'] == pid
         and Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()[19] == ticks,
         'restore_index_init_changed')
    raw = b'openlegal-isolated-index-start-v1\n'
    _write_new(control / 'start',raw)
    fd,info = _directory(control)
    try:os.fsync(fd)
    finally:os.close(fd)
    actor.evidence('native-index-start-authorized', {'container_id':actor.cid,'pid':pid,
        'start_ticks':ticks,'cgroup':actor.groups,'gate_sha256':digest(raw),
        'gate_directory':{'dev':info.st_dev,'ino':info.st_ino},'identity_was_sealed':True})


def _rebuild(backend, op, primary, workspace, config, clones, checkpoint):
    # This is the ONLY intentional policy exception: a superuser fixed DELETE
    # inside the already compared, label-bound, no-network cloned database.
    # Original protected control, roles and production schema are never targeted.
    need(primary.sql("DELETE FROM openlegal_replication.control; "
                     "SELECT jsonb_build_object('fixture_policy_disabled',true)", readonly=False)
         == {'fixture_policy_disabled':True}, 'restore_fixture_policy_change')
    runtime = config['runtime_role']
    role = primary.sql("SELECT jsonb_build_object('exists',EXISTS(SELECT 1 FROM pg_roles "
                       f"WHERE rolname='{runtime}' AND rolcanlogin AND NOT rolsuper))")
    need(role == {'exists': True}, 'restore_runtime_role_missing')
    fixture = workspace / 'fixture'
    _write_new(fixture / 'server.toml', _fixture_config(op.inventory['revision']))
    index = workspace / 'rebuilt-index'
    index.mkdir(mode=0o700)
    os.chown(index, 10004, 10004)
    mounts = [{'source': str(fixture), 'destination': '/fixture', 'rw': False}]
    for name, destination, writable in [('cache-hdd', '/restore-cache', True),
                                        ('corpus-hdd', '/restore-corpus', True),
                                        ('dictionary', '/restore-dictionary', False)]:
        mounts.append({'source': str(clones[name]), 'destination': destination, 'rw': writable})
    mounts.append({'source': str(index), 'destination': '/restore-index', 'rw': True})
    control = workspace / 'index-start-control'
    control.mkdir(mode=0o755);os.chmod(control,0o755)
    mounts.append({'source':str(control),'destination':'/restore-control','rw':False})
    actor = Actor(backend, op, 'rebuild', config['server_image'], mounts, '10004:10004', workspace,
                  network='container:' + primary.cid,
                  env=['OPENLEGAL_RESTORE_DATABASE_URL=postgres://' + runtime
                       + '@127.0.0.1:5432/' + op.inventory['linux']['postgres']['database']])
    try:
        actor.create('/bin/sh', ['-c', INDEX_START_GATE])
        actor.start()
        _release_index_gate(actor,control)
        exit_code = actor.run(['wait', actor.cid], timeout=LIMIT)
        need(exit_code == b'0\n', 'restore_index_process_failed')
        after = primary.sql("SELECT jsonb_build_object('index_ack',index_ack,'watermark',next_event-1) "
                            "FROM openlegal.corpus_control WHERE singleton")
        need(after == {'index_ack': checkpoint['watermark'], 'watermark': checkpoint['watermark']},
             'restore_rebuilt_index_watermark')
        need(any(index.iterdir()), 'restore_rebuilt_index_empty')
        from linux_backend import tree_digest
        record = {'path': str(index), 'identity': {'dev': index.stat().st_dev, 'ino': index.stat().st_ino},
                  'symlinks': {}, 'max_files': 200000, 'max_bytes': 4 * 1024**4}
        backend._artifact(op, 'restore-index-semantic-proof', {
            'server_image': config['server_image'], 'actual_container_exit_code': 0,
            'generation': checkpoint['watermark'], 'index_witness': tree_digest(record, time.monotonic() + LIMIT),
            'fixture_only_policy_exception': 'deleted_cloned_replication_control_after_baseline_compare',
            'collection_configured': False, 'network_parent': primary.cid})
    finally:
        actor.close()


def verify(backend, op):
    """Execute once; caller owns source/authority/receipt checks and shared lock.

    Completed is returned only after both baseline restores, four Blob witnesses,
    archived index correspondence, one real native index rebuild, actual process
    exits/cgroup closure/removal and unchanged archived source witnesses. Partial
    /tmp workspaces are preserved on error; they are never resumed or recycled.
    """
    need(os.geteuid() == 0 and valid_uuid(op.operation_id) and op.phase == 'verify_isolated_restore',
         'restore_operation_scope')
    config = validate(op.inventory['linux']['restore'], op.inventory)
    op.seal()
    datasets = _full_exports(op)
    _postgres_layout(op, config)
    trees = op.inventory['ssd_trees']
    need(len({row['name'] for row in trees}) == len(trees)
         and set(op.previous['copy_ssd']['manifests']) == {row['name'] for row in trees},
         'restore_ssd_copy_coverage')
    workspace = Path('/tmp') / ('openlegal-storage-restore-' + op.operation_id)
    need(not workspace.exists() and not workspace.is_symlink(), 'restore_workspace_exists')
    workspace.mkdir(mode=0o700)
    need(workspace.resolve() == workspace and workspace.stat().st_uid == 0, 'restore_workspace_identity')
    workspace_identity = workspace.stat()
    _write_new(workspace / 'owned.json', canonical({'operation_id':op.operation_id, 'kind':'isolated_restore_workspace'}), 0o600)
    fixture = workspace / 'fixture'
    fixture.mkdir(mode=0o755)
    os.chmod(fixture,0o755)
    _write_new(fixture / 'postgresql.conf', PG_CONFIG.encode())
    _write_new(fixture / 'pg_hba.conf', b'local all all trust\nhost all all 127.0.0.1/32 trust\n')
    _write_new(fixture / 'pg_ident.conf', b'')
    stopped = op.previous['stop_postgres']['facts']
    clones, records, before, manifests, expected_sources = {}, {}, {}, {}, {}
    deadline = time.monotonic() + LIMIT
    selections = {'pg-primary': (config['primary']['data'], ['postgres']),
                  'pg-standby': (config['standby']['data'], ['postgres']),
                  'index': (config['index'], ['assets']), 'dictionary': (config['dictionary'], ['assets'])}
    for role in ('cache', 'corpus'):
        for side in ('hdd', 'ssd'):
            selections[role + '-' + side] = (config['blobs'][role][side], [role])
    for side in ('primary', 'standby'):
        if config[side]['wal'] is not None:
            selections['wal-' + side] = (config[side]['wal'], ['wal'])
    # Verify every full export, including configuration/trust/assets which are
    # deliberately not mounted into the fixture. No predecessor chain is read.
    for index,dataset in enumerate(datasets):
        source,expected = _source(op, {'dataset':dataset['name'],'relative':'.'})
        key = 'full-export-' + str(index)
        records[key] = _record(source,expected,[])
        expected_sources[key] = expected
        before[key] = _verify_source(records[key],expected,deadline)
    # Unselected prior PostgreSQL trees and every other SSD archive are still
    # part of this full self-contained chain and must remain byte-exact.
    for index, tree in enumerate(trees):
        source, expected = _source(op, {'tree': tree['name'], 'relative': '.'})
        key = 'full-ssd-' + str(index)
        records[key] = _record(source, expected, [])
        expected_sources[key] = expected
        before[key] = _verify_source(records[key], expected, deadline)
    for name, (spec, roles) in selections.items():
        op.seal()
        destination = workspace / name
        record, manifest, witness, expected = _copy(op, spec, destination, roles, deadline)
        records[name], manifests[name], clones[name] = record, manifest, destination
        before[name], expected_sources[name] = witness, expected
        backend._artifact(op, 'restore-copy-' + name, {
            'manifest_sha256':digest(canonical(manifest)), 'source_witness':witness,
            'destination_identity':manifest['destination_identity'],
            'all_bytes_read_back':manifest['all_bytes_read_back']})
    need(before['index']['sha256'] == stopped['index']['sha256'], 'restore_archived_index_mismatch')
    for role in ('cache', 'corpus'):
        rows = [row for row in stopped['retained_references'] if row['role'] == role]
        for side in ('hdd', 'ssd'):
            backend._verify_references(clones[role + '-' + side], rows)
    pg = op.inventory['linux']['postgres']
    for side in ('primary', 'standby'):
        root = clones['pg-' + side]
        wal = clones.get('wal-' + side)
        _prepare_pg(root, manifests['pg-' + side], wal, pg['uid'])
        mounts = [{'source': str(root), 'destination': PG_TARGET, 'rw': True},
                  {'source': str(fixture), 'destination': '/fixture', 'rw': False}]
        if wal:
            mounts.append({'source': str(wal), 'destination': '/restore-wal', 'rw': True})
        actor = Actor(backend, op, side, pg[side]['image_id'], mounts,
                      str(pg['uid']) + ':' + str(root.stat().st_gid), workspace)
        try:
            actor.create('/usr/local/bin/postgres', ['-D', PG_TARGET, '-c', 'config_file=/fixture/postgresql.conf'])
            actor.start()
            actor.ready()
            # Readiness is an actual bounded SQL operation, never a sleep/PID.
            checkpoint = _baseline(backend, op, actor, stopped)
            if side == 'primary':
                _rebuild(backend, op, actor, workspace, config, clones, checkpoint)
        finally:
            actor.close()
    for name, record in records.items():
        need(_verify_source(record, expected_sources[name], time.monotonic() + LIMIT) == before[name],
             'restore_original_archive_changed')
    op.seal()
    backend._artifact(op, 'restore-preserved-sources', before)
    now = workspace.lstat()
    need((now.st_dev,now.st_ino) == (workspace_identity.st_dev,workspace_identity.st_ino)
         and not stat.S_ISLNK(now.st_mode) and shutil.rmtree.avoids_symlink_attacks,
         'restore_workspace_cleanup_identity')
    shutil.rmtree(workspace)
    need(not workspace.exists() and not workspace.is_symlink(), 'restore_workspace_not_removed')
    facts = {key: True for key in REQUIRED_TRUE['verify_isolated_restore']}
    facts.update(workspace=str(workspace), source_witnesses=before,
                 restored_sides=['primary', 'standby'], semantic_index_rebuilt=True,
                 fixture_only_replication_policy_exception=True, collection_configured=False,
                 full_export_restore_verified=True, parent_chains=[],
                 export_receipt_sha256=digest(canonical(op.previous['export_snapshots'])))
    return backend._receipt(op, facts)
