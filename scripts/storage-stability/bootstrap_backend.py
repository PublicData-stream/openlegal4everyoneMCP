"""Fixed planned-drill preparation and reseeding; never general provisioning.

The root backend owns command/process receipts, physical writer closure and the
preprovisioned protected-control function. This module owns the target grammar,
literal generation copies and actual pair readback. No credentials, datasets,
networks, Kubernetes resources, or recovery authority are created here.
"""
from __future__ import annotations

import ipaddress
import hashlib
import os
import re
import socket
import ssl
import stat
import struct
import time
from pathlib import Path

from interface import Refused, need
from inventory import all_datasets, canonical, decode, digest, pin, private_bytes, private_parents, valid_uuid
import manual_backend as manual
from copy_tree import copy_tree, root_fd, signature

ROLES = ('cache_primary', 'cache_replica', 'corpus_primary', 'corpus_replica')
MEDIA_PROFILE = 'cache_hdd_corpus_ssd_primary_v1'
ROLE_MEDIA = {'cache_primary': 'hdd', 'cache_replica': 'ssd',
              'corpus_primary': 'ssd', 'corpus_replica': 'hdd'}
MARKER = '.openlegal-seed-v1.json'
GUARDS = '.openlegal-runtime-guards'
HEALTH = re.compile(r'00/\.stage-' + '0' * 64 + r'-00000000-0000-0000-0000-000000000000\.[0-9a-f]{64}\Z')
KEY = re.compile(r'([0-9a-f]{2})/([0-9a-f]{64})-([0-9a-f-]{36})\Z')
MAX_FILES = 200000
MAX_BYTES = 4 * 1024**4
PGDATA = '/var/lib/postgresql/data'
TRUST = '/run/openlegal-manual'


def _shape(value, fields, code):
    manual._shape(value, fields, code)


def _scope(op):
    return manual._config(op)


def _regular(fd, name, uid, mode, limit):
    before = os.stat(name, dir_fd=fd, follow_symlinks=False)
    need(stat.S_ISREG(before.st_mode) and before.st_uid == uid
         and stat.S_IMODE(before.st_mode) in mode and before.st_nlink == 1
         and before.st_dev == os.fstat(fd).st_dev and before.st_size <= limit,
         'bootstrap_regular_metadata')
    inp = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                  | os.O_CLOEXEC | os.O_NOATIME, dir_fd=fd)
    try:
        need(signature(os.fstat(inp)) == signature(before), 'bootstrap_regular_rebound')
        chunks = []; size = 0
        while block := os.read(inp, 1024 * 1024):
            size += len(block); need(size <= limit, 'bootstrap_regular_bound'); chunks.append(block)
        need(size == before.st_size and signature(os.fstat(inp)) == signature(before)
             and signature(os.stat(name, dir_fd=fd, follow_symlinks=False)) == signature(before),
             'bootstrap_regular_changed')
        return b''.join(chunks), before
    finally:
        os.close(inp)


def _trust(op, doc):
    """Hash preprovisioned files only; never include their contents in receipts."""
    policy = _scope(op); uid = policy['reseed_target']['uid']; result = {}
    refs = dict(doc['server_trust'])
    refs.update({'client_' + k: policy['replication_peer'][k]
                 for k in ('ca', 'certificate', 'private_key')})
    for name, ref in refs.items():
        manual._private_reference(ref); path = manual._path(ref['path']); private_parents(path)
        parent = root_fd(path.parent)
        try:
            info = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
            need(info.st_uid in (0, uid), 'bootstrap_trust_owner')
            # A root-owned key needs an explicitly PG-readable group. A file
            # owned by PG remains private to that UID; neither is rewritten.
            mode = stat.S_IMODE(info.st_mode)
            need(mode == 0o600 and info.st_uid == uid
                 or mode == 0o640 and info.st_uid == 0 and info.st_gid == policy['reseed_target']['gid'],
                 'bootstrap_trust_readability')
            raw, _ = _regular(parent, path.name, info.st_uid, (mode,), 1024 * 1024)
            need(digest(raw) == ref['sha256'], 'bootstrap_trust_raw_hash')
            result[name] = {'path': str(path), 'sha256': digest(raw), 'bytes': len(raw)}
        finally:
            os.close(parent)
    return result


def _target(value):
    _shape(value, ('host_path', 'container_path', 'parent', 'relative'), 'bootstrap_target_shape')
    parent = value['parent']
    _shape(parent, ('path', 'identity', 'kernel_filesystem_id', 'filesystem_id'),
           'bootstrap_target_parent_shape')
    _shape(parent['identity'], ('dev', 'ino'), 'bootstrap_target_parent_identity')
    need(value['relative'] == 'data'
         and manual._path(value['host_path']) == manual._path(parent['path']) / 'data'
         and all(type(v) is int and v >= 0 for v in parent['identity'].values())
         and type(parent['kernel_filesystem_id']) is int and parent['kernel_filesystem_id'] >= 0
         and type(parent['filesystem_id']) is str
         and re.fullmatch('[A-Za-z0-9:._-]{1,256}', parent['filesystem_id']),
         'bootstrap_target_parent_binding')
    manual._path(value['container_path'])


def _blob_target(role, value):
    """Logical read-primary and physical medium are distinct, fixed types."""
    need(role in ROLE_MEDIA and type(value) is dict and set(value) ==
         {'host_path', 'container_path', 'parent', 'relative', 'media'}, 'bootstrap_blob_target_shape')
    _target({k: v for k, v in value.items() if k != 'media'})
    media = value['media']
    need(type(media) is dict and media.get('kind') == ROLE_MEDIA[role], 'bootstrap_blob_media_profile')
    if media['kind'] == 'hdd':
        need(set(media) == {'kind', 'dataset', 'guid', 'mountpoint'}
             and type(media['dataset']) is str
             and re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]{1,200}', media['dataset'])
             and '..' not in Path(media['dataset']).parts
             and type(media['guid']) is str and re.fullmatch('[1-9][0-9]{0,19}', media['guid'])
             and value['parent']['filesystem_id'] == 'zfs:' + media['guid'], 'bootstrap_hdd_media')
        manual._path(media['mountpoint'])
        need(Path(value['parent']['path']).is_relative_to(media['mountpoint']), 'bootstrap_hdd_mount_path')
    else:
        need(set(media) == {'kind', 'physical_leaves'} and type(media['physical_leaves']) is list
             and 0 < len(media['physical_leaves']) <= 16
             and all(type(p) is str and re.fullmatch('/dev/[A-Za-z0-9_.-]{1,128}', p)
                     for p in media['physical_leaves'])
             and len(set(media['physical_leaves'])) == len(media['physical_leaves']), 'bootstrap_ssd_media')


def _media_hardware(backend, op):
    domains = op.inventory['linux']['failure_domains']
    need(type(domains) is dict and set(domains) == {'hdd', 'ssd', 'leaf_devices'}
         and all(type(domains[k]) is list and 0 < len(domains[k]) <= 16
                 and all(type(p) is str for p in domains[k])
                 and len(set(domains[k])) == len(domains[k]) for k in ('hdd', 'ssd'))
         and not set(domains['hdd']) & set(domains['ssd'])
         and type(domains['leaf_devices']) is list and len(domains['leaf_devices']) <= 32,
         'bootstrap_failure_domains')
    hardware = {}
    for row in domains['leaf_devices']:
        need(type(row) is dict and set(row) == {'path', 'major', 'minor', 'sysfs_path', 'identity_file', 'identity_sha256'}
             and type(row['path']) is str and re.fullmatch('/dev/[A-Za-z0-9_.-]{1,128}', row['path'])
             and row['path'] not in hardware
             and all(type(row[k]) is int and 0 <= row[k] < 2**32 for k in ('major', 'minor'))
             and type(row['sysfs_path']) is str and row['sysfs_path'].startswith('/sys/devices/')
             and '..' not in Path(row['sysfs_path']).parts
             and row['identity_file'] == 'wwid' and pin(row['identity_sha256']), 'bootstrap_whole_wwid_policy')
        hardware[row['path']] = backend._device_identity(row)
    need(set(hardware) == set(domains['hdd']) | set(domains['ssd']), 'bootstrap_hardware_coverage')
    need(not {hardware[p] for p in domains['hdd']} & {hardware[p] for p in domains['ssd']},
         'bootstrap_same_physical_hardware')
    need(backend._pool_devices(op) == set(domains['hdd']), 'bootstrap_hdd_leaves_changed')
    return domains, hardware


def _fd_mount_id(fd):
    # Source-local form of legacy_prepare's reviewed FD/mount proof. Manual
    # SOURCE_FILES15 excludes that initial-only module, so never import it here.
    need(type(fd) is int and fd >= 0, 'bootstrap_mount_fd')
    with open('/proc/self/fdinfo/' + str(fd), encoding='ascii') as source:
        raw = source.read(65537)
    need(len(raw) <= 65536, 'bootstrap_fdinfo_bound')
    ids = [r.split(':', 1)[1].strip() for r in raw.splitlines() if r.startswith('mnt_id:')]
    need(len(ids) == 1 and ids[0].isdecimal(), 'bootstrap_mount_id')
    return int(ids[0])


def _mounts():
    with open('/proc/self/mountinfo', encoding='utf-8') as source:
        raw = source.read(16 * 1024**2 + 1)
    lines = raw.splitlines()
    need(len(raw) <= 16 * 1024**2 and len(lines) <= 65536, 'bootstrap_mountinfo_bound')
    def text(value):
        return re.sub(r'\\(040|011|012|134)', lambda m: chr(int(m[1], 8)), value)
    result = {}
    for line in lines:
        row = line.split(); need('-' in row, 'bootstrap_mountinfo_shape'); split = row.index('-')
        need(split >= 6 and len(row) == split + 4 and row[0].isdecimal() and row[0] not in result
             and re.fullmatch('[0-9]+:[0-9]+', row[2]), 'bootstrap_mountinfo_shape')
        result[row[0]] = {'mount_id': int(row[0]), 'device': row[2], 'root': text(row[3]),
            'mountpoint': text(row[4]), 'filesystem': row[split + 1], 'source': text(row[split + 2])}
    return result


def _path_mount(fd, path, table):
    meta = os.fstat(fd); mid = _fd_mount_id(fd); row = table.get(str(mid))
    need(row is not None and row['device'] == str(os.major(meta.st_dev)) + ':' + str(os.minor(meta.st_dev))
         and Path(path).is_relative_to(row['mountpoint']), 'bootstrap_mount_missing_or_device')
    fresh = root_fd(path)
    try:
        now = os.fstat(fresh)
        need((meta.st_dev, meta.st_ino) == (now.st_dev, now.st_ino) and _fd_mount_id(fresh) == mid,
             'bootstrap_mount_path_rebound')
    finally: os.close(fresh)
    return row


def _no_nested_mounts(path, table):
    path = Path(path)
    need(not any(Path(r['mountpoint']) != path and Path(r['mountpoint']).is_relative_to(path)
                 for r in table.values()), 'bootstrap_nested_mount_unsupported')


def _target_media(backend, op, role, target, fd, domains, hardware, table, *, deadline=None):
    """Qualify actual held FD/kernel mount/hardware; caller owns phase fences."""
    _blob_target(role, target); st = os.fstat(fd); parent = target['parent']
    need(stat.S_ISDIR(st.st_mode) and st.st_uid == st.st_gid == 0 and stat.S_IMODE(st.st_mode) == 0o700
         and (st.st_dev, st.st_ino) == (parent['identity']['dev'], parent['identity']['ino'])
         and os.fstatvfs(fd).f_fsid == parent['kernel_filesystem_id'], 'bootstrap_media_parent_rebound')
    mount = _path_mount(fd, parent['path'], table)
    _no_nested_mounts(parent['path'], table)
    media = target['media']
    if media['kind'] == 'hdd':
        pools = {r['pool'] for r in all_datasets(op.inventory)}
        need(len(pools) == 1 and media['dataset'].split('/')[0] in pools, 'bootstrap_hdd_pool')
        raw = backend._run(op, 'zfs', ['get', '-Hp', '-o', 'property,value',
            'guid,mountpoint,mounted', media['dataset']], deadline=deadline)
        rows = [line.split('\t') for line in raw.decode('ascii').splitlines()]
        need(len(rows) == 3 and all(len(row) == 2 for row in rows)
             and len({row[0] for row in rows}) == 3, 'bootstrap_zfs_media_output')
        need(dict(rows) == {'guid': media['guid'], 'mountpoint': media['mountpoint'], 'mounted': 'yes'}
             and mount['filesystem'] == 'zfs' and mount['source'] == media['dataset']
             and mount['mountpoint'] == media['mountpoint'] and mount['root'] == '/', 'bootstrap_actual_zfs_mount')
        leaves = set(domains['hdd'])
    else:
        leaves = backend._device_leaves(st.st_dev)
        need(leaves == set(media['physical_leaves']) and leaves <= set(domains['ssd'])
             and not leaves & set(domains['hdd']) and parent['filesystem_id'] ==
                 'block:' + str(os.major(st.st_dev)) + ':' + str(os.minor(st.st_dev)), 'bootstrap_actual_ssd_mount')
    need((os.fstat(fd).st_dev, os.fstat(fd).st_ino) == (parent['identity']['dev'], parent['identity']['ino'])
         and os.fstatvfs(fd).f_fsid == parent['kernel_filesystem_id']
         and _path_mount(fd, parent['path'], _mounts()) == mount,
         'bootstrap_media_changed_during_probe')
    _no_nested_mounts(parent['path'], _mounts())
    return {'parent': parent, 'physical_leaves': sorted(leaves),
            'hardware': {p: hardware[p] for p in leaves}, 'mount': mount, 'media': media}


def _parent(value, *, absent):
    parent = value['parent']; path = manual._path(parent['path']); private_parents(path)
    fd = root_fd(path)
    try:
        s = os.fstat(fd)
        need((s.st_dev, s.st_ino) == (parent['identity']['dev'], parent['identity']['ino'])
             and s.st_uid == 0 and stat.S_IMODE(s.st_mode) == 0o700
             and os.fstatvfs(fd).f_fsid == parent['kernel_filesystem_id'],
             'bootstrap_parent_actual_identity')
        need(sorted(os.listdir(fd)) == ([] if absent else ['data']), 'bootstrap_parent_entries')
        if absent:
            need(not os.path.lexists(value['host_path']), 'bootstrap_target_exists')
        return fd
    except BaseException:
        os.close(fd); raise


def read_prepared_configuration(op):
    policy = _scope(op); ref = policy['target_configuration']
    doc = decode(private_bytes(ref['path'], ref['sha256'], limit=512 * 1024))
    _shape(doc, ('schema_version', 'kind', 'new_epoch', 'resources', 'serving_workload',
                'scheduler_workload', 'collection_job', 'blob_roots', 'postgres_spec',
                'serving_runtime_image_id', 'network', 'upstream', 'index_target',
                'capacity_reserve_bytes', 'server_trust', 'media_profile'), 'bootstrap_configuration_shape')
    need(type(doc['schema_version']) is int and doc['schema_version'] == 2 and doc['media_profile'] == MEDIA_PROFILE
         and doc['kind'] == 'openlegal_manual_prepared_configuration'
         and doc['new_epoch'] == policy['new_managed_epoch'], 'bootstrap_configuration_identity')
    manual._resources(doc['resources'])
    namespace = op.inventory['linux']['namespace']; roles = []
    for key, kind in (('serving_workload', 'deployment'), ('scheduler_workload', 'deployment'),
                      ('collection_job', 'job')):
        row = doc[key]; _shape(row, ('name', 'uid'), 'bootstrap_workload_shape')
        need(type(row['name']) is str and re.fullmatch('[a-z0-9][a-z0-9.-]{0,252}', row['name'])
             and valid_uuid(row['uid']), 'bootstrap_workload_identity')
        matches = [r for r in doc['resources'] if (r['kind'], r['namespace'], r['name'], r['uid'])
                   == (kind, namespace, row['name'], row['uid'])]
        need(len(matches) == 1, 'bootstrap_workload_not_prepared'); roles.append(row['uid'])
    need(len(set(roles)) == 3 and not set(roles) & {
        r['uid'] for r in op.inventory['linux']['workloads'] + op.inventory['linux']['jobs']},
        'bootstrap_target_reuses_old_writer')
    need(type(doc['blob_roots']) is dict and set(doc['blob_roots']) == set(ROLES),
         'bootstrap_four_targets')
    for role, target in doc['blob_roots'].items(): _blob_target(role, target)
    _target(doc['index_target'])
    paths = [manual._path(r['parent']['path']) for r in
             list(doc['blob_roots'].values()) + [doc['index_target']]]
    old_paths = [manual._path(r['path']) for r in op.inventory['journal_roots']
                 + op.inventory['ssd_trees'] + [op.inventory['backup_root']]]
    old_paths += [manual._path(r['mountpoint']) for r in all_datasets(op.inventory)]
    for i, path in enumerate(paths):
        need(not any(path.is_relative_to(other) or other.is_relative_to(path)
                     for other in paths[i + 1:] + old_paths), 'bootstrap_target_overlap')
    _shape(doc['network'], ('id', 'name', 'replica_address'), 'bootstrap_network_shape')
    network = doc['network']
    need(pin(network['id']) and type(network['name']) is str
         and re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', network['name'])
         and network['replica_address'] == policy['replication_peer']['host'],
         'bootstrap_prepared_network')
    _shape(doc['upstream'], ('host', 'port', 'server_name', 'server_certificate_sha256'),
           'bootstrap_upstream_shape')
    upstream = doc['upstream']
    try: address = ipaddress.ip_address(upstream['host'])
    except (ValueError, TypeError): raise Refused('bootstrap_numeric_upstream') from None
    need(str(address) == upstream['host'] and not address.is_unspecified and not address.is_multicast
         and type(upstream['port']) is int and 1 <= upstream['port'] <= 65535
         and type(upstream['server_name']) is str
         and re.fullmatch('[A-Za-z0-9][A-Za-z0-9.-]{0,252}', upstream['server_name'])
         and pin(upstream['server_certificate_sha256']), 'bootstrap_upstream_identity')
    _shape(doc['server_trust'], ('ca', 'certificate', 'private_key', 'hba'), 'bootstrap_server_trust')
    for ref in doc['server_trust'].values(): manual._private_reference(ref)
    need(type(doc['capacity_reserve_bytes']) is int
         and 1024**3 <= doc['capacity_reserve_bytes'] <= 1024**4,
         'bootstrap_capacity_reserve')
    need(type(doc['serving_runtime_image_id']) is str and re.fullmatch(
        r'[A-Za-z0-9:/._@-]*sha256:[0-9a-f]{64}', doc['serving_runtime_image_id']),
        'bootstrap_native_runtime_image')
    need(doc['postgres_spec'] == postgres_spec(op, doc), 'bootstrap_fixed_postgres_spec')
    return doc


def postgres_spec(op, doc):
    policy = op.inventory['linux']['manual_recovery']; target = policy['reseed_target']
    mounts = [{'source': target['path'], 'destination': PGDATA, 'rw': True, 'type': 'bind'}]
    for name, ref in doc['server_trust'].items():
        mounts.append({'source': ref['path'], 'destination': TRUST + '/' + name,
                       'rw': False, 'type': 'bind'})
    for name in ('ca', 'certificate', 'private_key'):
        mounts.append({'source': policy['replication_peer'][name]['path'],
                       'destination': TRUST + '/client_' + name, 'rw': False, 'type': 'bind'})
    labels = doc.get('postgres_spec', {}).get('labels', {})
    need(type(labels) is dict and len(labels) <= 128
         and all(type(k) is str and type(v) is str and 0 < len(k) <= 512
                 and len(v) <= 4096 for k, v in labels.items()), 'bootstrap_image_label_shape')
    inherited = {k: v for k, v in labels.items() if not k.startswith('openlegal.')}
    return {'name': '/openlegal-manual-standby-' + op.operation_id,
        'image': op.inventory['linux']['postgres']['primary']['image_id'],
        'entrypoint': ['/usr/local/bin/postgres'],
        'arguments': ['-D', PGDATA, '-c', 'config_file=' + PGDATA
                      + '/openlegal-manual-' + op.operation_id + '.conf'],
        'user': str(target['uid']) + ':' + str(target['gid']),
        'labels': dict(inherited, **{'openlegal.operation': op.operation_id, 'openlegal.epoch': doc['new_epoch']}),
        'mounts': sorted(mounts, key=lambda r: r['destination']),
        'restart_policy': {'Name': 'no', 'MaximumRetryCount': 0},
        'network_mode': doc['network']['id']}


def _image_labels(backend, op, doc):
    image = op.inventory['linux']['postgres']['primary']['image_id']
    raw = backend._run(op, 'docker', manual.DOCKER + ['image', 'inspect', image,
        '--format', '{{.Id}}\n{{json .Config.Labels}}'], record=False)
    lines = raw.splitlines()
    need(len(lines) == 2 and lines[0].decode('ascii') == image, 'bootstrap_native_image_changed')
    inherited = decode(lines[1])
    if inherited is None: inherited = {}
    need(type(inherited) is dict and len(inherited) <= 126
         and all(type(k) is str and type(v) is str and 0 < len(k) <= 512
                 and len(v) <= 4096 and not k.startswith('openlegal.')
                 for k, v in inherited.items()), 'bootstrap_conflicting_image_labels')
    expected = dict(inherited, **{'openlegal.operation': op.operation_id,
                                  'openlegal.epoch': doc['new_epoch']})
    need(doc['postgres_spec']['labels'] == expected, 'bootstrap_prepared_image_labels_differ')
    manual._record(backend, op, 'bootstrap-native-image-labels', {
        'image_id': image, 'inherited_labels_sha256': digest(canonical(inherited))})


def _resource_projection(kind, item):
    if kind == 'configmap': return {key: item.get(key) for key in ('data', 'binaryData', 'immutable')}
    if kind == 'secret': return {key: item.get(key) for key in ('type', 'data', 'immutable')}
    return item['spec']


def _resources(backend, op, doc):
    rows = []
    for row in doc['resources']:
        args = ([] if row['kind'] == 'persistentvolume' else ['-n', row['namespace']])
        item = backend._kube(op, args + ['get', row['kind'], row['name'], '-o', 'json'])
        meta = item['metadata']
        need(meta['uid'] == row['uid'] and meta['name'] == row['name']
             and meta.get('namespace', '') == row['namespace']
             and meta['resourceVersion'] == row['resource_version']
             and digest(canonical(_resource_projection(row['kind'], item))) == row['spec_sha256'],
             'bootstrap_precreated_resource_changed')
        if row['kind'] == 'deployment':
            need(item['spec'].get('replicas') == 0
                 and all(item.get('status', {}).get(k, 0) == 0 for k in
                         ('replicas', 'readyReplicas', 'availableReplicas', 'updatedReplicas')),
                 'bootstrap_target_deployment_active')
        if row['kind'] == 'job':
            need(item['spec'].get('suspend') is True
                 and not item.get('status', {}).get('startTime')
                 and all(item.get('status', {}).get(k, 0) == 0 for k in ('active', 'succeeded', 'failed')),
                 'bootstrap_target_job_used')
        rows.append(row)
    # Raw ConfigMaps/Secrets are never passed to the journal.
    manual._record(backend, op, 'bootstrap-prepared-resources', rows)
    return rows


def _network(backend, op, doc):
    result = decode(backend._run(op, 'docker', manual.DOCKER + [
        'network', 'inspect', doc['network']['id']], record=False))
    need(type(result) is list and len(result) == 1, 'bootstrap_network_inspect')
    row = result[0]
    need(row['Id'] == doc['network']['id'] and row['Name'] == doc['network']['name']
         and row['Driver'] == 'bridge' and row['Internal'] is True
         and type(row.get('EnableIPv6', False)) is bool
         and (':' not in doc['network']['replica_address'] or row.get('EnableIPv6') is True)
         and not row.get('Ingress', False), 'bootstrap_network_not_isolated')
    manual._record(backend, op, 'bootstrap-network', {'id': row['Id'], 'name': row['Name'],
                   'driver': row['Driver'], 'internal': row['Internal']})


def _space(fd, amount, reserve):
    vfs = os.fstatvfs(fd)
    need(vfs.f_bavail * vfs.f_frsize >= amount + reserve, 'bootstrap_insufficient_space')


def _leaf_numbers(paths):
    need(type(paths) is set and 0 < len(paths) <= 16, 'bootstrap_physical_leaf_bound')
    result = set()
    for path in paths:
        need(type(path) is str and path.startswith('/dev/'), 'bootstrap_physical_leaf_path')
        item = Path(path).resolve(strict=True).stat()
        need(stat.S_ISBLK(item.st_mode), 'bootstrap_physical_leaf_type')
        result.add(str(os.major(item.st_rdev)) + ':' + str(os.minor(item.st_rdev)))
    return result


def _postgres_tree(op):
    rows = [r for r in op.inventory['ssd_trees'] if 'postgres' in r.get('roles', [])]
    need(len(rows) == 1 and type(rows[0].get('max_bytes')) is int
         and 1 <= rows[0]['max_bytes'] <= MAX_BYTES, 'bootstrap_exact_postgres_source_bound')
    return rows[0]


def _dataset(backend, op, target):
    raw = backend._run(op, 'zfs', ['get', '-H', '-p', '-o', 'property,value',
        'guid,mountpoint,readonly', target['dataset']]).decode('ascii')
    values = dict(row.split('\t') for row in raw.strip().splitlines())
    need(values == {'guid': target['guid'], 'mountpoint': target['path'], 'readonly': 'off'},
         'bootstrap_target_dataset_changed')


def _quota(backend, op, doc, *, before=True):
    target = op.inventory['linux']['manual_recovery']['reseed_target']
    bound = _postgres_tree(op)['max_bytes']
    quota_raw = backend._run(op, 'zfs', ['get', '-H', '-p', '-o', 'value',
                                       'refquota', target['dataset']]).strip()
    need(quota_raw.isdigit() and int(quota_raw) == bound, 'bootstrap_pg_prepared_refquota_required')
    pool_raw = backend._run(op, 'zfs', ['get', '-H', '-p', '-o', 'value',
        'available', target['dataset'].split('/')[0]]).strip()
    need(pool_raw.isdigit(), 'bootstrap_pool_available_shape')
    need(int(pool_raw) >= (bound if before else 0) + doc['capacity_reserve_bytes'],
         'bootstrap_pg_pool_copy_reserve')
    value = {'dataset': target['dataset'], 'copy_bound_bytes': bound, 'actual_refquota_bytes': int(quota_raw),
             'pool_available_bytes': int(pool_raw), 'reserve_bytes': doc['capacity_reserve_bytes'],
             'stage': 'before_copy' if before else 'after_copy'}
    manual._record(backend, op, 'bootstrap-pg-quota-capacity', value)
    return value


def _tree_size(record):
    """Bound current logical bytes through FDs, without reading DB bodies."""
    fd = root_fd(record['path'], noatime=True); count = 0; total = 0
    try:
        root = os.fstat(fd)
        need((root.st_dev, root.st_ino) == (record['identity']['dev'], record['identity']['ino']),
             'bootstrap_pg_size_root_changed')
        def visit(directory, depth):
            nonlocal count, total
            need(depth <= 64, 'bootstrap_pg_size_depth')
            for name in os.listdir(directory):
                count += 1; need(count <= record['max_files'], 'bootstrap_pg_size_file_bound')
                info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                need(info.st_dev == root.st_dev, 'bootstrap_pg_size_nested_mount')
                if stat.S_ISDIR(info.st_mode):
                    child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                                    | os.O_CLOEXEC | os.O_NOATIME, dir_fd=directory)
                    try:
                        need((os.fstat(child).st_dev, os.fstat(child).st_ino) == (info.st_dev, info.st_ino),
                             'bootstrap_pg_size_directory_rebound')
                        visit(child, depth + 1)
                    finally: os.close(child)
                else:
                    need(stat.S_ISREG(info.st_mode) and info.st_nlink == 1,
                         'bootstrap_pg_size_external_or_special')
                    total += info.st_size
                    need(total <= record['max_bytes'], 'bootstrap_pg_size_byte_bound')
        visit(fd, 0)
        need((Path(record['path']).lstat().st_dev, Path(record['path']).lstat().st_ino)
             == (root.st_dev, root.st_ino), 'bootstrap_pg_size_root_rebound')
        return {'entries': count, 'logical_bytes': total}
    finally: os.close(fd)


def _media(backend, op, doc, *, absent):
    """Actual logical-role/media binding; no suffix-based independence guess."""
    need(doc['schema_version'] == 2 and doc['media_profile'] == MEDIA_PROFILE,
         'bootstrap_media_schema')
    domains, hardware = _media_hardware(backend, op)
    table = _mounts(); evidence = {}
    for role, target in doc['blob_roots'].items():
        fd = _parent(target, absent=absent)
        try:
            evidence[role] = _target_media(backend, op, role, target, fd, domains, hardware, table)
        finally: os.close(fd)
    manual._record(backend, op, 'bootstrap-target-media', evidence)
    return evidence


def verify_prepared(backend, op):
    policy = _scope(op); doc = read_prepared_configuration(op)
    _resources(backend, op, doc); _network(backend, op, doc); _trust(op, doc); _image_labels(backend, op, doc)
    target = policy['reseed_target']; _dataset(backend, op, target)
    _quota(backend, op, doc)
    fd = root_fd(target['path'])
    try:
        s = os.fstat(fd)
        need((s.st_dev, s.st_ino, s.st_uid, s.st_gid, stat.S_IMODE(s.st_mode)) == (
            target['identity']['dev'], target['identity']['ino'], target['uid'], target['gid'], 0o700)
             and os.listdir(fd) == [], 'bootstrap_pg_target_not_empty')
        _space(fd, 0, doc['capacity_reserve_bytes'])
        need(target['dataset'].split('/')[0] in {r['pool'] for r in all_datasets(op.inventory)},
             'bootstrap_target_unknown_pool')
        target_leaves = _leaf_numbers(backend._pool_devices(op))
        standby_record = _postgres_tree(op)
        standby_leaves = _leaf_numbers(backend._device_leaves(standby_record['identity']['dev']))
        need(target_leaves and standby_leaves and not target_leaves & standby_leaves,
             'bootstrap_pg_same_physical_media')
    finally: os.close(fd)
    for value in list(doc['blob_roots'].values()) + [doc['index_target']]:
        fd = _parent(value, absent=True)
        try: _space(fd, 0, doc['capacity_reserve_bytes'])
        finally: os.close(fd)
    _media(backend, op, doc, absent=True)
    return manual.save_evidence(backend, op, 'prepared', {
        'new_epoch': doc['new_epoch'], 'target_root': dict(target, entries=[], readonly=False),
        'tls': policy['replication_peer'],
        'configuration': {'raw_sha256': policy['target_configuration']['sha256'],
            'resources': doc['resources'], 'blob_roots': {k: {n: v[n] for n in
                ('host_path', 'container_path')} for k, v in doc['blob_roots'].items()},
            'postgres_spec_sha256': digest(canonical(doc['postgres_spec'])),
            'serving_runtime_image_id': doc['serving_runtime_image_id']},
        'standby_leaves': sorted(standby_leaves), 'target_leaves': sorted(target_leaves)})


def verify_configuration(backend, op, promoted, reseeded):
    del promoted, reseeded
    doc = read_prepared_configuration(op)
    rows = _resources(backend, op, doc)
    return manual.save_evidence(backend, op, 'configured', {
        'new_epoch': doc['new_epoch'], 'resources': rows,
        'provider': manual._provider_state(backend, op,
                     container=op.previous['promote']['facts']['promoted_container_id'])})


def blob_inventory(path, *, deadline, controls=True, online=False):
    """Complete permanent generations, separate health/control domains.

    Legal bodies are streamed into hashes, never returned or journalled. Every
    inode is rechecked after the complete scan, including early files whose
    mtime could otherwise be restored during a later read.
    """
    fd = root_fd(path, noatime=True)
    witness = {}; rows = []; health = []; control_rows = []; total = 0
    try:
        root = os.fstat(fd)
        need(root.st_uid == 10004 and stat.S_IMODE(root.st_mode) == 0o700,
             'bootstrap_blob_root_owner_mode')
        root_sig = signature(root); names = sorted(os.listdir(fd))
        need(len(names) <= 258, 'bootstrap_blob_root_entries')
        for name in names:
            need(time.monotonic() < deadline, 'bootstrap_blob_deadline')
            if name == MARKER:
                need(controls, 'bootstrap_unexpected_control')
                raw, item = _regular(fd, name, 0, (0o640, 0o644), 16384)
                value = decode(raw)
                need(value.get('kind') == 'openlegal_blob_seed' and value.get('schema_version') == 1,
                     'bootstrap_old_seed_shape')
                control_rows.append({'path': name, 'sha256': digest(raw), 'bytes': len(raw)})
                witness[name] = signature(item); continue
            if name == GUARDS:
                need(controls, 'bootstrap_unexpected_control')
                directory = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                                    | os.O_CLOEXEC | os.O_NOATIME, dir_fd=fd)
                try:
                    s = os.fstat(directory)
                    need(s.st_dev == root.st_dev and s.st_uid == root.st_uid
                         and stat.S_IMODE(s.st_mode) == 0o700, 'bootstrap_old_guard_directory')
                    if online:
                        # Current guard records are validated under their real
                        # flocks by LinuxBackend._runtime_guards, separately.
                        # They are never part of a permanent-generation hash.
                        continue
                    witness[name] = signature(s)
                    guard_names = sorted(os.listdir(directory))
                    need('.directory.lock' in guard_names and len(guard_names) <= 513,
                         'bootstrap_old_guard_entries')
                    for entry in guard_names:
                        need(entry == '.directory.lock' or re.fullmatch(r'[a-z0-9.-]{1,123}\.json', entry),
                             'bootstrap_unknown_old_guard')
                        raw, item = _regular(directory, entry, root.st_uid, (0o600,), 4096)
                        if entry == '.directory.lock': need(raw == b'', 'bootstrap_old_guard_lock_bytes')
                        else:
                            value = decode(raw)
                            need(value.get('state') == 'closed_clean', 'bootstrap_old_guard_unclean')
                        rel = name + '/' + entry; witness[rel] = signature(item)
                        control_rows.append({'path': rel, 'sha256': digest(raw), 'bytes': len(raw)})
                    need(sorted(os.listdir(directory)) == guard_names
                         and signature(os.fstat(directory)) == witness[name], 'bootstrap_old_guard_changed')
                finally: os.close(directory)
                continue
            need(re.fullmatch('[0-9a-f]{2}', name), 'bootstrap_unknown_blob_root_entry')
            shard = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                            | os.O_CLOEXEC | os.O_NOATIME, dir_fd=fd)
            try:
                s = os.fstat(shard)
                need(s.st_dev == root.st_dev and s.st_uid == root.st_uid
                     and stat.S_IMODE(s.st_mode) == 0o700, 'bootstrap_blob_shard_metadata')
                witness[name] = signature(s); entries = sorted(os.listdir(shard))
                need(len(entries) <= MAX_FILES, 'bootstrap_blob_entry_bound')
                for entry in entries:
                    need(time.monotonic() < deadline, 'bootstrap_blob_deadline')
                    rel = name + '/' + entry; match = KEY.fullmatch(rel)
                    known_health = HEALTH.fullmatch(rel) is not None
                    need(known_health or match and match[1] == match[2][:2]
                         and valid_uuid(match[3]), 'bootstrap_unknown_blob_generation')
                    item = os.stat(entry, dir_fd=shard, follow_symlinks=False)
                    need(stat.S_ISREG(item.st_mode) and item.st_uid == root.st_uid
                         and item.st_nlink == 1 and item.st_dev == root.st_dev
                         and stat.S_IMODE(item.st_mode) == 0o600
                         and 0 <= item.st_size <= 100 * 1024**2,
                         'bootstrap_blob_file_metadata')
                    if not (online and known_health): witness[rel] = signature(item)
                    file_signature = signature(item)
                    inp = os.open(entry, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                                  | os.O_CLOEXEC | os.O_NOATIME, dir_fd=shard)
                    try:
                        need(signature(os.fstat(inp)) == file_signature, 'bootstrap_blob_file_rebound')
                        h = hashlib.sha256(); size = 0; payload = bytearray()
                        while block := os.read(inp, 1024 * 1024):
                            need(time.monotonic() < deadline, 'bootstrap_blob_deadline')
                            size += len(block); need(size <= item.st_size, 'bootstrap_blob_file_grew')
                            h.update(block)
                            if known_health:
                                payload.extend(block); need(len(payload) <= 12, 'bootstrap_health_payload')
                        need(size == item.st_size and signature(os.fstat(inp)) == file_signature
                             and signature(os.stat(entry, dir_fd=shard, follow_symlinks=False)) == file_signature,
                             'bootstrap_blob_file_changed')
                        row = {'storage_key': rel, 'sha256': h.hexdigest(), 'size': size}
                        if known_health:
                            need(bytes(payload) in (b'', b'blob-health\n') and len(health) < 8,
                                 'bootstrap_health_payload_or_count'); health.append(row)
                        else:
                            need(h.hexdigest() == match[2], 'bootstrap_blob_digest')
                            total += size; rows.append(row)
                            need(len(rows) <= MAX_FILES and total <= MAX_BYTES, 'bootstrap_blob_inventory_bound')
                    finally: os.close(inp)
                current_entries = sorted(os.listdir(shard))
                permanent_names = lambda names: [n for n in names if not HEALTH.fullmatch(name + '/' + n)]
                need((permanent_names(current_entries) == permanent_names(entries)
                      and signature(os.fstat(shard))[:5] == witness[name][:5]
                      if online and name == '00' else
                      current_entries == entries and signature(os.fstat(shard)) == witness[name]),
                     'bootstrap_blob_shard_changed')
            finally: os.close(shard)
        need(sorted(os.listdir(fd)) == names and signature(os.fstat(fd)) == root_sig,
             'bootstrap_blob_root_changed')
        _recheck(fd, witness, root_sig, path, deadline, online=online)
        rows.sort(key=lambda r: r['storage_key']); health.sort(key=lambda r: r['storage_key'])
        return {'permanent': rows, 'permanent_sha256': digest(canonical(rows)),
                'regular_bytes': total, 'health': health, 'controls': control_rows,
                'private_witness_sha256': digest(canonical({'root': root_sig, 'entries': witness})),
                'root_identity': {'dev': root.st_dev, 'ino': root.st_ino}}
    finally: os.close(fd)


def _recheck(root, witness, root_sig, path, deadline, *, online=False):
    for rel, before in witness.items():
        need(time.monotonic() < deadline, 'bootstrap_witness_deadline')
        fd = os.dup(root)
        try:
            parts = Path(rel).parts
            for part in parts[:-1]:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                                | os.O_CLOEXEC | os.O_NOATIME, dir_fd=fd)
                os.close(fd); fd = child
            current = signature(os.stat(parts[-1], dir_fd=fd, follow_symlinks=False))
            need(current[:5] == before[:5] if online and rel == '00' else current == before,
                 'bootstrap_late_tree_change')
        finally: os.close(fd)
    if online and '00' in witness:
        shard = os.open('00', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                        | os.O_CLOEXEC | os.O_NOATIME, dir_fd=root)
        try:
            names = sorted(os.listdir(shard)); count = 0
            permanent = sorted(Path(rel).name for rel in witness
                               if len(Path(rel).parts) == 2 and Path(rel).parts[0] == '00')
            need([n for n in names if not HEALTH.fullmatch('00/' + n)] == permanent,
                 'bootstrap_online_permanent_names_changed')
            for name in names:
                if HEALTH.fullmatch('00/' + name):
                    count += 1; need(count <= 8, 'bootstrap_online_health_bound')
                    raw, _ = _regular(shard, name, 10004, (0o600,), 12)
                    need(raw in (b'', b'blob-health\n'), 'bootstrap_online_health_payload')
            need(sorted(os.listdir(shard)) == names, 'bootstrap_online_health_changed_during_read')
        finally: os.close(shard)
    need(signature(os.fstat(root)) == root_sig
         and signature(Path(path).lstat()) == root_sig, 'bootstrap_root_rebound')


def _old_roots(op):
    """Resolve physical restore sides through authenticated logical role rows.

    Old primary=HDD inventories are incompatible with the fixed corpus SSD-read
    profile. Refuse them, never reinterpret or rewrite their existing markers.
    """
    result = {}; roots = op.inventory['linux']['bound_roots']; seed_operation = None
    need(type(roots) is list and len(roots) == 4 and {r.get('role') for r in roots} == set(ROLES),
         'bootstrap_old_bound_roles')
    for family in ('cache', 'corpus'):
        for side in ('hdd', 'ssd'):
            value = op.inventory['linux']['restore']['blobs'][family][side]
            if side == 'hdd':
                matches = [r for r in all_datasets(op.inventory) if r['name'] == value['dataset']]
                need(len(matches) == 1, 'bootstrap_old_dataset_binding')
                parent = matches[0]['mountpoint']; filesystem = 'zfs:' + matches[0]['guid']
            else:
                matches = [r for r in op.inventory['ssd_trees'] if r['name'] == value['tree']]
                need(len(matches) == 1, 'bootstrap_old_ssd_binding'); parent = matches[0]['path']
            relative = Path(value['relative'])
            need(not relative.is_absolute() and '..' not in relative.parts, 'bootstrap_old_relative_path')
            path = str(Path(parent) / relative)
            pins = [r for r in roots if r['path'] == path]
            need(len(pins) == 1 and pins[0]['role'].split('_')[0] == family
                 and ROLE_MEDIA[pins[0]['role']] == side and pins[0]['role'] not in result,
                 'bootstrap_old_role_media_incompatible')
            pinning = pins[0]; role = pinning['role']; fd = root_fd(path)
            try:
                st = os.fstat(fd)
                need((st.st_dev, st.st_ino, st.st_uid) == (pinning['dev'], pinning['ino'], pinning['uid'])
                     and st.st_uid == 10004 and stat.S_IMODE(st.st_mode) == 0o700, 'bootstrap_old_blob_root_changed')
                if side == 'ssd': filesystem = 'block:' + str(os.major(st.st_dev)) + ':' + str(os.minor(st.st_dev))
                raw, _ = _regular(fd, MARKER, 0, (0o640, 0o644), 16384); marker = decode(raw)
                need(type(marker) is dict and set(marker) == {'schema_version', 'kind', 'managed_epoch',
                     'seed_operation', 'role', 'manifest_sha256', 'correspondence_sha256', 'filesystem_id'}
                     and type(marker['schema_version']) is int and marker['schema_version'] == 1
                     and marker['kind'] == 'openlegal_blob_seed' and marker['managed_epoch'] == op.inventory['managed_epoch']
                     and valid_uuid(marker['seed_operation']) and marker['role'] == role
                     and pin(marker['manifest_sha256']) and pin(marker['correspondence_sha256'])
                     and marker['filesystem_id'] == filesystem, 'bootstrap_old_marker_binding')
                need(seed_operation is None or seed_operation == marker['seed_operation'],
                     'bootstrap_old_marker_operation')
                seed_operation = marker['seed_operation']
                result[role] = {'path': path, 'identity': {'dev': st.st_dev, 'ino': st.st_ino},
                    'uid': st.st_uid, 'gid': st.st_gid, 'mode': stat.S_IMODE(st.st_mode),
                    'roles': [family], 'max_files': MAX_FILES + 4096, 'max_bytes': MAX_BYTES, 'symlinks': {}}
            finally: os.close(fd)
    need(set(result) == set(ROLES), 'bootstrap_old_roles_incomplete')
    return result


def _write_exclusive(fd, name, raw, uid, gid, mode):
    out = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                  mode, dir_fd=fd)
    try:
        os.fchown(out, uid, gid); os.fchmod(out, mode)
        pending = memoryview(raw)
        while pending:
            written = os.write(out, pending); need(written > 0, 'bootstrap_zero_write'); pending = pending[written:]
        os.fsync(out)
    finally: os.close(out)
    os.fsync(fd)


def _retire_copied_controls(op, role, target, inventory):
    """Archive exact copied metadata to BOTH journals before destination-only unlink."""
    root = root_fd(target, noatime=True)
    try:
        info = os.fstat(root)
        need({'dev': info.st_dev, 'ino': info.st_ino} == inventory['root_identity'],
             'bootstrap_copied_root_changed')
        records = []
        # Read and durably archive ALL control bytes before the first unlink.
        for index, row in enumerate(inventory['controls']):
            parts = Path(row['path']).parts; fd = os.dup(root)
            try:
                if len(parts) == 2:
                    child = os.open(parts[0], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                                    | os.O_CLOEXEC, dir_fd=fd); os.close(fd); fd = child
                owner = 0 if row['path'] == MARKER else info.st_uid
                raw, before = _regular(fd, parts[-1], owner, (0o600, 0o640, 0o644), 16384)
                need(digest(raw) == row['sha256'] and len(raw) == row['bytes'],
                     'bootstrap_copied_control_changed')
                artifact = op.phase + '-archived-copied-control-' + role + '-' + str(index) + '.bin'
                sha = op.journal.save_bytes(artifact, raw)
                need(sha == row['sha256'] and op.journal.output_hashes().get(artifact) == sha,
                     'bootstrap_control_archive_not_acknowledged')
                records.append((row['path'], signature(before)))
            finally: os.close(fd)
        for rel, before in records:
            parts = Path(rel).parts; fd = os.dup(root)
            try:
                if len(parts) == 2:
                    child = os.open(parts[0], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                                    | os.O_CLOEXEC, dir_fd=fd); os.close(fd); fd = child
                need(signature(os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)) == before,
                     'bootstrap_control_changed_before_retirement')
                os.unlink(parts[-1], dir_fd=fd); os.fsync(fd)
            finally: os.close(fd)
        if any(rel.startswith(GUARDS + '/') for rel, _ in records):
            os.rmdir(GUARDS, dir_fd=root); os.fsync(root)
    finally: os.close(root)


def _mint_marker(op, role, value, manifest, references):
    root = root_fd(value['host_path'])
    try:
        s = os.fstat(root); fsid = os.fstatvfs(root).f_fsid
        need({'dev': s.st_dev, 'ino': s.st_ino} == manifest['root_identity']
             and s.st_uid == 10004 and stat.S_IMODE(s.st_mode) == 0o700
             and fsid == value['parent']['kernel_filesystem_id'], 'bootstrap_new_root_identity')
        correspondence = digest(canonical([r for r in references if r['role'] == role.split('_')[0]]))
        marker = {'schema_version': 1, 'kind': 'openlegal_blob_seed', 'managed_epoch':
            op.inventory['linux']['manual_recovery']['new_managed_epoch'], 'seed_operation': op.operation_id,
            'role': role, 'manifest_sha256': manifest['permanent_sha256'],
            'correspondence_sha256': correspondence, 'filesystem_id': value['parent']['filesystem_id']}
        raw = canonical(marker); _write_exclusive(root, MARKER, raw, 0, s.st_gid, 0o640)
        row = {'role': role, 'host_path': value['host_path'], 'container_path': value['container_path'],
            'root_dev': s.st_dev, 'root_ino': s.st_ino, 'root_uid': s.st_uid, 'root_gid': s.st_gid,
            'root_mode': stat.S_IMODE(s.st_mode), 'kernel_filesystem_id': fsid,
            'filesystem_id': value['parent']['filesystem_id'], 'marker_sha256': digest(raw),
            'manifest_sha256': manifest['permanent_sha256'], 'reference_sha256': correspondence}
        if role == 'cache_replica':
            os.mkdir(GUARDS, 0o700, dir_fd=root)
            guard = os.open(GUARDS, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=root)
            try:
                os.fchown(guard, s.st_uid, s.st_gid); os.fchmod(guard, 0o700)
                _write_exclusive(guard, '.directory.lock', b'', s.st_uid, s.st_gid, 0o600)
            finally: os.close(guard)
            os.fsync(root)
        return row
    finally: os.close(root)


def _copy_generations(backend, op, doc, sealed):
    manual._closure(sealed['closure'], op.inventory)
    old = _old_roots(op); source = {}; deadline = time.monotonic() + 1200
    # Parent helper proves real locks/closed-clean records, not desired replicas.
    backend._runtime_guards(op, active=False)
    for role, record in old.items(): source[role] = blob_inventory(record['path'], deadline=deadline)
    for prefix in ('cache', 'corpus'):
        need(source[prefix + '_primary']['permanent'] == source[prefix + '_replica']['permanent'],
             'bootstrap_old_mirrors_diverge_requires_audited_union')
    rows = []
    for role in ROLES:
        op.seal(); target = doc['blob_roots'][role]; parent = _parent(target, absent=True)
        try:
            srcrole = role.split('_')[0] + '_primary'; record = old[srcrole]
            _space(parent, source[srcrole]['regular_bytes'], doc['capacity_reserve_bytes'])
            copied = copy_tree(record, target['host_path'], deadline)
            manual._record(backend, op, 'bootstrap-literal-copy-' + role, copied)
            actual = blob_inventory(target['host_path'], deadline=deadline)
            need(actual['permanent'] == source[srcrole]['permanent']
                 and actual['health'] == source[srcrole]['health']
                 and actual['controls'] == source[srcrole]['controls'], 'bootstrap_copy_inventory_mismatch')
            _retire_copied_controls(op, role, target['host_path'], actual)
            row = _mint_marker(op, role, target, actual, sealed['retained_references']); rows.append(row)
            reference_fd = root_fd(target['host_path'])
            try:
                manual._reference_fd(reference_fd,
                    [r for r in sealed['retained_references'] if r['role'] == role.split('_')[0]], deadline)
            finally: os.close(reference_fd)
            need((os.fstat(parent).st_dev, os.fstat(parent).st_ino) == (
                target['parent']['identity']['dev'], target['parent']['identity']['ino']),
                 'bootstrap_parent_rebound_after_copy')
            current_parent = root_fd(target['parent']['path'])
            try:
                need((os.fstat(current_parent).st_dev, os.fstat(current_parent).st_ino) == (
                     os.fstat(parent).st_dev, os.fstat(parent).st_ino), 'bootstrap_parent_path_rebound')
            finally: os.close(current_parent)
            _space(parent, 0, doc['capacity_reserve_bytes'])
        finally: os.close(parent)
    # A full late source inventory detects changes during the other copies.
    for role, record in old.items():
        need(blob_inventory(record['path'], deadline=deadline) == source[role],
             'bootstrap_original_changed_during_reseed')
    return rows


def _upstream_tls(op, doc):
    """PostgreSQL SSLRequest, pinned CA/name AND exact peer DER fingerprint."""
    _trust(op, doc); upstream = doc['upstream']
    peer = op.inventory['linux']['manual_recovery']['replication_peer']
    context = ssl.create_default_context(cafile=peer['ca']['path'])
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(peer['certificate']['path'], peer['private_key']['path'])
    family = socket.AF_INET6 if ipaddress.ip_address(upstream['host']).version == 6 else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        sock.settimeout(5); sock.connect((upstream['host'], upstream['port']))
        sock.sendall(struct.pack('!II', 8, 80877103))
        need(sock.recv(1) == b'S', 'bootstrap_upstream_tls_required')
        with context.wrap_socket(sock, server_hostname=upstream['server_name']) as secured:
            need(digest(secured.getpeercert(binary_form=True)) == upstream['server_certificate_sha256'],
                 'bootstrap_upstream_certificate_changed')
    finally: sock.close()
    _trust(op, doc)


def _promoted_endpoint(backend, op, doc, promoted):
    item = manual._inspect(backend, op, 'standby')
    need(item['Id'] == promoted['promoted_container_id'] and item['State']['Running'] is True,
         'bootstrap_upstream_not_promoted_container')
    attached = item['NetworkSettings']['Networks']
    need(type(attached) is dict and len(attached) <= 16 and doc['network']['name'] in attached,
         'bootstrap_upstream_network_missing')
    endpoint = attached[doc['network']['name']]
    address = endpoint.get('GlobalIPv6Address') if ':' in doc['upstream']['host'] else endpoint.get('IPAddress')
    need(endpoint['NetworkID'] == doc['network']['id'] and address == doc['upstream']['host']
         and doc['upstream']['host'] != doc['network']['replica_address'],
         'bootstrap_upstream_address_not_promoted_container')
    current = manual._checkpoint(backend._sql(op, manual.CHECKPOINT_SQL, container=item['Id']))
    prior = promoted['promoted_checkpoint']
    need(all(current[key] == prior[key] for key in ('system_identifier', 'timeline',
        'postmaster_started_at', 'index_ack', 'watermark', 'in_recovery'))
         and manual._lsn(current['wal_lsn']) >= manual._lsn(prior['wal_lsn']),
         'bootstrap_upstream_database_lifetime_changed')
    source = _postgres_tree(op)
    writable = [row for row in item['Mounts'] if row['RW'] is True]
    need(writable and all(row['Type'] == 'bind'
             and Path(row['Source']).is_relative_to(source['path']) for row in writable),
         'bootstrap_upstream_not_qualified_postgres_tree')
    observed = _tree_size(source)
    need(observed['logical_bytes'] + doc['capacity_reserve_bytes'] <= source['max_bytes'],
         'bootstrap_source_pg_exceeds_prepared_copy_bound')
    manual._record(backend, op, 'bootstrap-promoted-backup-endpoint', {
        'container_id': item['Id'], 'network_id': endpoint['NetworkID'], 'numeric_address': address,
        'checkpoint': current, 'source_size': observed, 'copy_bound_bytes': source['max_bytes']})
    return observed


def _conninfo(op, doc, *, backup):
    peer = op.inventory['linux']['manual_recovery']['replication_peer']; upstream = doc['upstream']
    # Every scalar has a closed grammar. There are no URLs, default DNS,
    # environment password files or user-supplied option fragments.
    application = ('olbackup_' + op.operation_id.replace('-', '')) if backup else peer['application_name']
    return ' '.join((
        'host=' + upstream['server_name'], 'hostaddr=' + upstream['host'],
        'port=' + str(upstream['port']), 'user=' + peer['role'],
        'application_name=' + application, 'sslmode=verify-full',
        'sslrootcert=' + TRUST + '/client_ca', 'sslcert=' + TRUST + '/client_certificate',
        'sslkey=' + TRUST + '/client_private_key', 'connect_timeout=5', 'passfile=/dev/null'))


def _mount_args(mounts):
    args = []
    for row in mounts:
        need(row['type'] == 'bind' and all(type(row[k]) is str and ',' not in row[k]
             for k in ('source', 'destination')), 'bootstrap_mount_grammar')
        # --mount has no bare `rw` option. Writable is its default.
        args += ['--mount', 'type=bind,src=' + row['source'] + ',dst=' + row['destination']
                 + ('' if row['rw'] else ',readonly')]
    return args


def _inspect_owned(backend, op, cid, *, name, spec=None, running=True, network=None):
    need(pin(cid), 'bootstrap_owned_container_id')
    values = decode(backend._run(op, 'docker', manual.DOCKER + ['inspect', cid], record=False))
    need(type(values) is list and len(values) == 1, 'bootstrap_owned_inspect')
    item = values[0]; policy = op.inventory['linux']['manual_recovery']
    mounts = sorted([{'source': r['Source'], 'destination': r['Destination'],
        'rw': r['RW'], 'type': r['Type']} for r in item['Mounts']], key=lambda r: r['destination'])
    projection = {'name': item['Name'], 'image': item['Image'],
        'entrypoint': item['Config']['Entrypoint'], 'arguments': item['Config']['Cmd'],
        'user': item['Config']['User'], 'labels': item['Config']['Labels'],
        'mounts': mounts, 'restart_policy': item['HostConfig']['RestartPolicy'],
        'network_mode': item['HostConfig']['NetworkMode']}
    need(item['Id'] == cid and item['Name'] == '/' + name
         and item['Image'] == op.inventory['linux']['postgres']['primary']['image_id']
         and item['Config']['Labels'].get('openlegal.operation') == op.operation_id
         and item['Config']['Labels'].get('openlegal.epoch') == policy['new_managed_epoch']
         and item['HostConfig']['Privileged'] is False and not item['HostConfig'].get('CapAdd')
         and item['HostConfig']['RestartPolicy'] == {'Name': 'no', 'MaximumRetryCount': 0}
         and item['State']['Running'] is running, 'bootstrap_owned_container_changed')
    if spec is not None: need(projection == spec, 'bootstrap_owned_spec_changed')
    if network is not None and running:
        attached = item['NetworkSettings']['Networks']
        need(type(attached) is dict and set(attached) == {network['name']},
             'bootstrap_owned_network_set')
        endpoint = attached[network['name']]
        actual = endpoint.get('GlobalIPv6Address') if ':' in network['replica_address'] else endpoint.get('IPAddress')
        need(endpoint['NetworkID'] == network['id'] and actual == network['replica_address']
             and not item['HostConfig'].get('PortBindings')
             and not item['HostConfig'].get('PublishAllPorts', False),
             'bootstrap_owned_network_address_or_ports')
    state = {key: item['State'].get(key) for key in ('Status', 'Running', 'Paused', 'Restarting',
        'OOMKilled', 'Dead', 'Pid', 'ExitCode', 'StartedAt', 'FinishedAt')}
    manual._record(backend, op, 'bootstrap-owned-container', {
        'id': cid, 'projection_sha256': digest(canonical(projection)), 'state': state})
    return item, projection


def _created_id(raw):
    need(type(raw) is bytes and re.fullmatch(b'[0-9a-f]{64}\n', raw),
         'bootstrap_create_ack_format')
    return raw[:-1].decode('ascii')


def _basebackup(backend, op, doc, promoted):
    """One basebackup attempt; unknown effects remain for explicit readback."""
    policy = op.inventory['linux']['manual_recovery']; target = policy['reseed_target']
    _dataset(backend, op, target); _quota(backend, op, doc)
    _promoted_endpoint(backend, op, doc, promoted); _upstream_tls(op, doc)
    target_fd = root_fd(target['path'])
    try:
        s = os.fstat(target_fd)
        need((s.st_dev, s.st_ino, s.st_uid, s.st_gid) == (
             target['identity']['dev'], target['identity']['ino'], target['uid'], target['gid'])
             and os.listdir(target_fd) == [], 'bootstrap_basebackup_target_not_empty')
    finally: os.close(target_fd)
    workspace = Path('/tmp/openlegal-manual-bootstrap-' + op.operation_id)
    os.mkdir(workspace, 0o700)
    control = workspace / 'control'; os.mkdir(control, 0o700); os.chown(control, target['uid'], target['gid'])
    # The gate makes a fast-success process observable before native execution.
    # No retry or cleanup occurs on timeout/unknown; all actual effects survive.
    gate_script = ('n=0; while [ ! -f /restore-control/start ]; do '
        'n=$((n+1)); [ "$n" -lt 1200 ] || exit 124; sleep 0.1; done; exec "$@"')
    name = 'openlegal-manual-basebackup-' + op.operation_id
    mounts = [{'source': target['path'], 'destination': PGDATA, 'rw': True, 'type': 'bind'},
              {'source': str(control), 'destination': '/restore-control', 'rw': False, 'type': 'bind'}]
    for key in ('ca', 'certificate', 'private_key'):
        mounts.append({'source': policy['replication_peer'][key]['path'],
            'destination': TRUST + '/client_' + key, 'rw': False, 'type': 'bind'})
    args = ['create', '--pull=never', '--name', name, '--restart=no', '--read-only',
        '--network', doc['network']['id'], '--ip' if ':' not in doc['network']['replica_address'] else '--ip6',
        doc['network']['replica_address'], '--user', str(target['uid']) + ':' + str(target['gid']),
        '--label', 'openlegal.operation=' + op.operation_id,
        '--label', 'openlegal.epoch=' + doc['new_epoch'], '--cap-drop=ALL',
        '--security-opt=no-new-privileges', '--pids-limit=64', '--memory=512m',
        '--tmpfs', '/tmp:rw,noexec,nosuid,size=16m', '-e', 'PGPASSFILE=/dev/null',
        '--entrypoint', '/bin/sh'] + _mount_args(mounts) + [
            op.inventory['linux']['postgres']['primary']['image_id'], '-c', gate_script, '--',
            'pg_basebackup', '--no-password', '--pgdata=' + PGDATA, '--format=plain',
            '--wal-method=stream', '--checkpoint=fast', '--write-recovery-conf',
            '--dbname=' + _conninfo(op, doc, backup=True)]
    manual._record(backend, op, 'bootstrap-basebackup-intent', {
        'name': name, 'arguments_sha256': digest(canonical(args)),
        'target': target, 'automatic_retry': False})
    cid = _created_id(backend._run(op, 'docker', manual.DOCKER + args, record=False))
    # Seal CID before any optional inspect or start can fail.
    manual._record(backend, op, 'bootstrap-basebackup-created', {'container_id': cid, 'name': name})
    backend._run(op, 'docker', manual.DOCKER + ['start', cid], record=False)
    item, _ = _inspect_owned(backend, op, cid, name=name, network=doc['network'])
    actual_mounts = sorted([{'source': r['Source'], 'destination': r['Destination'],
        'rw': r['RW'], 'type': r['Type']} for r in item['Mounts']], key=lambda r: r['destination'])
    need(actual_mounts == sorted(mounts, key=lambda r: r['destination'])
         and item['Config']['User'] == str(target['uid']) + ':' + str(target['gid'])
         and item['HostConfig']['NetworkMode'] == doc['network']['id'],
         'bootstrap_basebackup_mount_or_network_changed')
    from linux_backend import process_cgroup, members
    pid = item['State']['Pid']; need(type(pid) is int and pid > 0, 'bootstrap_basebackup_pid')
    ticks = Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()[19]
    groups = process_cgroup(pid)
    need(any(cid in group and group.startswith('0::/') for group in groups),
         'bootstrap_basebackup_unbound_cgroup')
    process = {'pid': pid, 'start_ticks': ticks, 'boot_id': op.inventory['host']['boot_id']}
    manual._record(backend, op, 'bootstrap-basebackup-process', dict(process, groups=groups))
    control_fd = root_fd(control)
    try: _write_exclusive(control_fd, 'start', b'start\n', 0, target['gid'], 0o644)
    finally: os.close(control_fd)
    raw = backend._run(op, 'docker', manual.DOCKER + ['wait', cid], record=False, timeout=900)
    need(raw == b'0\n', 'bootstrap_basebackup_exit_not_zero')
    item, _ = _inspect_owned(backend, op, cid, name=name, running=False)
    need(item['State']['ExitCode'] == 0 and item['State']['Pid'] == 0,
         'bootstrap_basebackup_unknown_exit')
    manual._process_absent(process); need(not members(groups), 'bootstrap_basebackup_descendants_alive')
    manual._record(backend, op, 'bootstrap-basebackup-closed', {
        'container_id': cid, 'process': process, 'groups': groups, 'actual_exit_code': 0,
        'actual_wait_sha256': digest(raw), 'owned_members': []})
    _quota(backend, op, doc, before=False)
    fd = root_fd(target['path'])
    try:
        s = os.fstat(fd)
        target_record = {'path': target['path'], 'identity': {'dev': s.st_dev, 'ino': s.st_ino},
            'max_files': _postgres_tree(op)['max_files'], 'max_bytes': _postgres_tree(op)['max_bytes']}
        _space(fd, 0, doc['capacity_reserve_bytes'])
        size = _tree_size(target_record)
        need(size['logical_bytes'] + doc['capacity_reserve_bytes'] <= target_record['max_bytes'],
             'bootstrap_target_pg_exceeds_copy_bound')
    finally: os.close(fd)
    manual._record(backend, op, 'bootstrap-basebackup-target-size', size)
    backend._run(op, 'docker', manual.DOCKER + ['rm', cid], record=False)
    return cid


def _postgres_configuration(op, doc):
    policy = op.inventory['linux']['manual_recovery']; path = policy['reseed_target']['path']
    fd = root_fd(path)
    try:
        need(os.stat('PG_VERSION', dir_fd=fd, follow_symlinks=False).st_size <= 8,
             'bootstrap_pgdata_version_file')
        version, _ = _regular(fd, 'PG_VERSION', policy['reseed_target']['uid'], (0o600,), 8)
        need(version.strip() == b'18', 'bootstrap_pgdata_version')
        tablespaces = os.open('pg_tblspc', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
        try: need(os.listdir(tablespaces) == [], 'bootstrap_external_tablespace_unsupported')
        finally: os.close(tablespaces)
        wal = os.stat('pg_wal', dir_fd=fd, follow_symlinks=False)
        need(stat.S_ISDIR(wal.st_mode) and wal.st_dev == os.fstat(fd).st_dev,
             'bootstrap_external_wal_unsupported')
        # pg_basebackup -R writes this new destination file. Record only its
        # hash, never a possibly inherited connection string/password.
        before = os.stat('postgresql.auto.conf', dir_fd=fd, follow_symlinks=False)
        raw, _ = _regular(fd, 'postgresql.auto.conf', policy['reseed_target']['uid'], (0o600,), 1024 * 1024)
        need(signature(os.stat('postgresql.auto.conf', dir_fd=fd, follow_symlinks=False)) == signature(before),
             'bootstrap_destination_config_changed')
        os.unlink('postgresql.auto.conf', dir_fd=fd); os.fsync(fd)
        uid = policy['reseed_target']['uid']; gid = policy['reseed_target']['gid']
        auto = ("primary_conninfo = '" + _conninfo(op, doc, backup=False) + "'\n").encode()
        _write_exclusive(fd, 'postgresql.auto.conf', auto, uid, gid, 0o600)
        fixed = ("listen_addresses='*'\nport=5432\nssl=on\n"
            "ssl_ca_file='" + TRUST + "/ca'\nssl_cert_file='" + TRUST + "/certificate'\n"
            "ssl_key_file='" + TRUST + "/private_key'\nhba_file='" + TRUST + "/hba'\n"
            "fsync=on\nfull_page_writes=on\nsynchronous_commit=on\n"
            "synchronous_standby_names='FIRST 1 (" + policy['replication_peer']['application_name'] + ")'\n"
            "shared_preload_libraries=''\nrestore_command=''\narchive_command=''\narchive_mode=off\n").encode()
        _write_exclusive(fd, 'openlegal-manual-' + op.operation_id + '.conf', fixed, uid, gid, 0o600)
        return {'old_auto_sha256': digest(raw), 'new_auto_sha256': digest(auto),
                'fixed_config_sha256': digest(fixed)}
    finally: os.close(fd)


def _start_standby(backend, op, doc):
    spec = doc['postgres_spec']; _trust(op, doc); _network(backend, op, doc); _image_labels(backend, op, doc)
    args = ['create', '--pull=never', '--name', spec['name'][1:], '--restart=no',
        '--network', spec['network_mode'], '--ip' if ':' not in doc['network']['replica_address'] else '--ip6',
        doc['network']['replica_address'], '--user', spec['user'], '--cap-drop=ALL',
        '--security-opt=no-new-privileges', '--pids-limit=256', '--entrypoint', spec['entrypoint'][0]]
    for key, value in sorted(spec['labels'].items()): args += ['--label', key + '=' + value]
    args += _mount_args(spec['mounts']) + [spec['image']] + spec['arguments']
    manual._record(backend, op, 'bootstrap-standby-create-intent', {
        'arguments_sha256': digest(canonical(args)), 'spec_sha256': digest(canonical(spec)),
        'automatic_retry': False})
    cid = _created_id(backend._run(op, 'docker', manual.DOCKER + args, record=False))
    manual._record(backend, op, 'bootstrap-standby-created', {'container_id': cid})
    backend._run(op, 'docker', manual.DOCKER + ['start', cid], record=False)
    item, _ = _inspect_owned(backend, op, cid, name=spec['name'][1:], spec=spec, network=doc['network'])
    need(item['State']['Pid'] > 0, 'bootstrap_new_standby_no_process')
    # This fixed registry grants only the root SQL helper's exact new CID path.
    backend._owned_sql_targets[cid] = {'image_id': spec['image'], 'epoch': doc['new_epoch'],
                                       'mounts': spec['mounts']}
    # Readiness is only a bounded connection check, not a preservation receipt.
    wait = ('n=0; until pg_isready -q -h /var/run/postgresql -p 5432; do '
            'n=$((n+1)); [ "$n" -lt 600 ] || exit 124; sleep 0.1; done')
    backend._run(op, 'docker', manual.DOCKER + ['exec', '--user', spec['user'], cid,
        '/bin/sh', '-c', wait], timeout=65, record=False)
    return cid


def _index_record(op, doc):
    path = doc['index_target']['host_path']; fd = root_fd(path, noatime=True)
    try:
        info = os.fstat(fd)
        return {'path': path, 'identity': {'dev': info.st_dev, 'ino': info.st_ino},
            'uid': info.st_uid, 'gid': info.st_gid, 'mode': stat.S_IMODE(info.st_mode),
            'roles': ['index'], 'max_files': op.inventory['linux']['index_tree']['max_files'],
            'max_bytes': op.inventory['linux']['index_tree']['max_bytes'], 'symlinks': {}}
    finally: os.close(fd)


def _index_metadata(record, watermark):
    fd = root_fd(record['path'], record, noatime=True)
    try:
        # Tantivy's existing files may use the process umask (0644). The
        # enclosing root is private; literal copy preserves actual metadata.
        raw, _ = _regular(fd, 'meta.json', 10004, (0o600, 0o640, 0o644), 4 * 1024**2)
        value = decode(raw)
        need(type(value.get('payload')) is str, 'bootstrap_index_payload_missing')
        payload = decode(value['payload'].encode('utf-8'))
        _shape(payload, ('format', 'analyzer', 'generation', 'complete'), 'bootstrap_index_payload')
        need(type(payload['format']) is int and payload['format'] == 3
             and payload['complete'] is True and type(payload['generation']) is int
             and payload['generation'] == watermark and type(payload['analyzer']) is str
             and 0 < len(payload['analyzer']) <= 1024, 'bootstrap_index_not_current_complete')
    finally: os.close(fd)


def _copy_index(backend, op, doc, sealed):
    from linux_backend import tree_digest
    source = op.inventory['linux']['index_tree']; deadline = time.monotonic() + 600
    actual = tree_digest(source, deadline)
    expected = dict(sealed['index']); expected.pop('watermark', None)
    need(actual == expected, 'bootstrap_source_index_changed')
    _index_metadata(source, sealed['checkpoint']['watermark'])
    parent = _parent(doc['index_target'], absent=True)
    try:
        _space(parent, actual['regular_bytes'], doc['capacity_reserve_bytes'])
        result = copy_tree(source, doc['index_target']['host_path'], deadline)
        manual._record(backend, op, 'bootstrap-index-copy', result)
        record = _index_record(op, doc); _index_metadata(record, sealed['checkpoint']['watermark'])
        _space(parent, 0, doc['capacity_reserve_bytes'])
        reopened = _parent(doc['index_target'], absent=False); os.close(reopened)
        current = tree_digest(record, deadline); current['watermark'] = sealed['checkpoint']['watermark']
        return current
    finally: os.close(parent)


def _seed_document(op, roots):
    return {'schema_version': 1, 'kind': 'openlegal_manual_seed_correspondence',
        'managed_epoch': op.inventory['linux']['manual_recovery']['new_managed_epoch'],
        'operation_id': op.operation_id,
        'roots': sorted([{k: r[k] for k in ('role', 'manifest_sha256', 'reference_sha256')}
                        for r in roots], key=lambda r: r['role'])}


def _read_root_row(op, role, target, references, *, active):
    actual = blob_inventory(target['host_path'], deadline=time.monotonic() + 600, online=active)
    root = root_fd(target['host_path'])
    try:
        s = os.fstat(root)
        need({'dev': s.st_dev, 'ino': s.st_ino} == actual['root_identity'],
             'bootstrap_inventory_root_replaced_before_marker_read')
        raw, _ = _regular(root, MARKER, 0, (0o640, 0o644), 16384)
        marker = decode(raw)
        reference_sha = digest(canonical([r for r in references if r['role'] == role.split('_')[0]]))
        need(marker == {'schema_version': 1, 'kind': 'openlegal_blob_seed',
            'managed_epoch': op.inventory['linux']['manual_recovery']['new_managed_epoch'],
            'seed_operation': op.operation_id, 'role': role,
            'manifest_sha256': actual['permanent_sha256'], 'correspondence_sha256': reference_sha,
            'filesystem_id': target['parent']['filesystem_id']}, 'bootstrap_current_seed_manifest_mismatch')
        need(s.st_dev == target['parent']['identity']['dev']
             and os.fstatvfs(root).f_fsid == target['parent']['kernel_filesystem_id'],
             'bootstrap_current_root_media_changed')
        manual._reference_fd(root, [r for r in references if r['role'] == role.split('_')[0]],
                             time.monotonic() + 600)
        return {'role': role, 'host_path': target['host_path'], 'container_path': target['container_path'],
            'root_dev': s.st_dev, 'root_ino': s.st_ino, 'root_uid': s.st_uid, 'root_gid': s.st_gid,
            'root_mode': stat.S_IMODE(s.st_mode), 'kernel_filesystem_id': os.fstatvfs(root).f_fsid,
            'filesystem_id': target['parent']['filesystem_id'], 'marker_sha256': digest(raw),
            'manifest_sha256': actual['permanent_sha256'], 'reference_sha256': reference_sha}, actual
    finally: os.close(root)


def _expected_binding(op, promoted, roots, prepared):
    checkpoint = promoted['promoted_checkpoint']; peer = op.inventory['linux']['manual_recovery']['replication_peer']
    blob_sha = manual._root_binding(op, roots, prepared, promoted['sealed_source']['retained_references'])
    return {'managed_epoch': op.inventory['linux']['manual_recovery']['new_managed_epoch'],
        'primary_system_identifier': checkpoint['system_identifier'], 'timeline': checkpoint['timeline'],
        'standby_name': peer['application_name'], 'standby_role': peer['role'], 'standby_addr': peer['host'],
        'standby_client_dn': peer['client_dn'], 'standby_issuer_dn': peer['issuer_dn'],
        'standby_client_serial': peer['client_serial'], 'blob_binding_sha256': blob_sha}


def _candidate(backend, op, promoted, expected):
    """A proposed peer check is NOT a stored binding, COMMIT fence, or ACK."""
    value = backend._manual_candidate_peer(op, promoted['promoted_container_id'], expected)
    _shape(value, ('kind', 'proposed_binding', 'candidate'), 'bootstrap_candidate_shape')
    need(value['kind'] == 'openlegal_replication_candidate_peer'
         and value['proposed_binding'] == expected, 'bootstrap_candidate_intent_changed')
    candidate = value['candidate']
    _shape(candidate, ('primary_system_identifier', 'timeline', 'postmaster_started_at',
        'standby_name', 'standby_role', 'standby_addr', 'standby_client_dn', 'standby_issuer_dn',
        'standby_client_serial', 'sender_pid', 'sender_started_at', 'prefix_lsn', 'flush_lsn'),
        'bootstrap_candidate_actual_fields')
    # Reuse the existing strict actual-field validator. These two proposal
    # fields are for validation only and NEVER serialized as an actual receipt.
    typed = dict(candidate, managed_epoch=expected['managed_epoch'],
                 blob_binding_sha256=expected['blob_binding_sha256'])
    wanted = dict(expected, postmaster_started_at=promoted['promoted_checkpoint']['postmaster_started_at'])
    wanted.pop('standby_addr')  # actual inet may include its full /32 or /128 suffix
    manual._observer(typed, wanted, expected['standby_addr'])
    manual._record(backend, op, 'bootstrap-readonly-candidate-peer', value)
    return value


def _observe_pair(backend, op, promoted, standby, expected):
    primary = promoted['promoted_container_id']
    first = manual._checkpoint(backend._sql(op, manual.CHECKPOINT_SQL, container=primary))
    deadline = time.monotonic() + 30
    while True:
        second = manual._checkpoint(backend._sql(op, manual.STANDBY_SQL, container=standby), recovering=True)
        need(first['system_identifier'] == second['system_identifier']
             and first['timeline'] == second['timeline'] == promoted['promoted_checkpoint']['timeline'],
             'bootstrap_pair_database_identity')
        if manual._lsn(second['replay_lsn']) >= manual._lsn(first['wal_lsn']) \
                and manual._lsn(second['receive_lsn']) >= manual._lsn(first['wal_lsn']): break
        need(time.monotonic() < deadline, 'bootstrap_pair_replay_deadline'); time.sleep(.05)
    observer_expected = dict(expected)
    observer_expected.pop('standby_addr')
    observer_expected['postmaster_started_at'] = first['postmaster_started_at']
    before, after = manual._fresh_barrier(backend, op, primary, observer_expected,
        op.inventory['linux']['manual_recovery']['replication_peer']['host'])
    return first, second, {'before': before, 'after': after}


def read_pair(backend, op, promoted, sealed, prepared, *, active=False):
    """Fresh typed native pair proof; no cached successful health/ACK receipt."""
    doc = read_prepared_configuration(op)
    need(type(active) is bool, 'bootstrap_pair_active_type')
    _media(backend, op, doc, absent=False)
    if op.phase == 'reseed':
        cid = backend._bootstrap_standby_id
    else:
        cid = op.previous['reseed']['facts']['pair']['standby']['container_id']
    item, _ = _inspect_owned(backend, op, cid,
        name='openlegal-manual-standby-' + op.operation_id, spec=doc['postgres_spec'], network=doc['network'])
    backend._owned_sql_targets[cid] = {'image_id': item['Image'], 'epoch': doc['new_epoch'],
                                      'mounts': doc['postgres_spec']['mounts']}
    roots = []; old = _old_roots(op)
    # Preserve unreferenced generations too. Old sources remain closed; an
    # online health artifact is a separate nonpermanent domain, never equality.
    for role in ROLES:
        parent = _parent(doc['blob_roots'][role], absent=False)
        try: _space(parent, 0, doc['capacity_reserve_bytes'])
        finally: os.close(parent)
        row, actual = _read_root_row(op, role, doc['blob_roots'][role],
                                    sealed['retained_references'], active=active)
        original = blob_inventory(old[role]['path'], deadline=time.monotonic() + 600)
        need(actual['permanent'] == original['permanent'], 'bootstrap_full_generation_preservation')
        roots.append(row)
    expected = _expected_binding(op, promoted, roots, prepared)
    first, second, peer = _observe_pair(backend, op, promoted, cid, expected)
    primary = promoted['promoted_container_id']
    data = {'new_epoch': doc['new_epoch'], 'primary': {'container_id': primary,
        'checkpoint': first, 'retained_tables': backend._fingerprints(op, container=primary),
        'retained_references': backend._references(op, container=primary)},
        'standby': {'container_id': cid, 'checkpoint': second,
            'retained_tables': backend._fingerprints(op, container=cid),
            'retained_references': backend._references(op, container=cid)},
        'peer': peer, 'blob_roots': roots,
        'seed_manifest_sha256': digest(canonical(_seed_document(op, roots))),
        'provider': manual._provider_state(backend, op, container=primary)}
    from linux_backend import tree_digest
    record = _index_record(op, doc); _index_metadata(record, first['watermark'])
    data['index'] = tree_digest(record, time.monotonic() + 600)
    data['index']['watermark'] = first['watermark']
    guards = backend._runtime_guards(op, cache_path=doc['blob_roots']['cache_replica']['host_path'],
        epoch=doc['new_epoch'], binding=expected['blob_binding_sha256'], active=active)
    data['runtime_guard'] = guards['guard']
    for target in list(doc['blob_roots'].values()) + [doc['index_target']]:
        parent = _parent(target, absent=False)
        try: _space(parent, 0, doc['capacity_reserve_bytes'])
        finally: os.close(parent)
    pg = root_fd(op.inventory['linux']['manual_recovery']['reseed_target']['path'])
    try: _space(pg, 0, doc['capacity_reserve_bytes'])
    finally: os.close(pg)
    # All long scans precede a NEW final prefix. A stale successful proof is
    # never substituted after reading the last table/file.
    _, _, data['peer'] = _observe_pair(backend, op, promoted, cid, expected)
    return data


def reseed_and_bind(backend, op, promoted, sealed):
    need(op.phase == 'reseed' and op.action == 'manual_failover', 'bootstrap_reseed_scope')
    doc = read_prepared_configuration(op)
    # Repeat all empty-target/prepared checks before the first effect. The
    # manual protocol independently authenticates the returned envelope.
    # verify_prepared itself is called by manual.reseed already; duplicate
    # evidence names would rightly refuse, so use the validated prepared data.
    prepared = promoted['prepared']
    _resources(backend, op, doc); _network(backend, op, doc); _trust(op, doc); _image_labels(backend, op, doc)
    manual._closure(sealed['closure'], op.inventory)
    roots = _copy_generations(backend, op, doc, sealed)
    _copy_index(backend, op, doc, sealed)
    # Before starting even a standby, seal exact root identities/manifests and
    # prospective epoch, through both physical journals.
    expected = _expected_binding(op, promoted, roots, prepared)
    manual._record(backend, op, 'bootstrap-root-copy-intent', {
        'expected_binding': expected, 'roots': roots,
        'seed_correspondence': _seed_document(op, roots), 'writers_inhibited': True})
    _basebackup(backend, op, doc, promoted)
    manual._record(backend, op, 'bootstrap-new-pg-configuration', _postgres_configuration(op, doc))
    cid = _start_standby(backend, op, doc); backend._bootstrap_standby_id = cid
    backend._artifact(op, 'bootstrap-seed-intent', {
        'schema_version': 1, 'kind': 'openlegal_bootstrap_seed_intent',
        'operation_id': op.operation_id, 'new_epoch': doc['new_epoch'],
        'primary_container_id': promoted['promoted_container_id'], 'standby_container_id': cid,
        'blob_roots': roots, 'blob_binding_sha256': expected['blob_binding_sha256'],
        'seed_manifest_sha256': digest(canonical(_seed_document(op, roots))),
        'expected_binding': expected})
    # One bounded READONLY startup wait, then precisely one protected mutator.
    # Failed/not-yet-streaming candidates never cause an M30 retry or local ACK.
    _candidate(backend, op, promoted, expected)
    # Root implements the protected M30 function with a distinct named grant,
    # actual intent, ONE commit, fresh 2s prefix/flush and SQL closure. Never
    # replace it with broad table UPDATE or a superuser fallback.
    backend._manual_bind_replication_control(op, promoted['promoted_container_id'], expected)
    data = read_pair(backend, op, promoted, sealed, prepared, active=False)
    manual._pair(backend, op, data, promoted, prepared)
    return manual.save_evidence(backend, op, 'reseeded', data)
