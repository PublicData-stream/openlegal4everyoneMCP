"""Explicit same-host manual recovery, never automatic failover.

Only the reviewed LinuxBackend supplies preparation/bootstrap operations. Its
fixed helpers return journal references, not success booleans or command hooks.
Every helper reference is read back from BOTH acknowledged phase journals before
the component can advance. This module creates no roles, secrets or datasets.
"""
from __future__ import annotations

import ipaddress
import hashlib
import json
import math
import os
import re
import stat
import time
from pathlib import Path
from typing import Protocol

from interface import Operation, Refused, need, REQUIRED_TRUE
from inventory import canonical, decode, digest, pin, valid_uuid

IDENTIFIER = re.compile(r'[a-z][a-z0-9_]{0,62}\Z')
HEX = re.compile(r'[0-9a-f]{64}\Z')
DATASET = re.compile(r'[A-Za-z0-9_.:/-]{1,240}\Z')
DOCKER = ['--host', 'unix:///var/run/docker.sock']


class PreparedBackend(Protocol):
    """Root-owned fixed helpers; input fields are never executable commands.

    Each returns {artifact,sha256}, made with save_evidence below. The actual
    data schemas are documented at MANUAL_HELPER_CONTRACT. Helpers must retain
    actual command exits and physical/SQL closure in the dual journal, refuse
    unknown ACKs, and validate the prepared private source bytes/FDs BEFORE any
    effect. Returning the claimed fields is not implementation or acceptance.
    """
    def _manual_verify_prepared(self, op: Operation) -> dict: ...
    def _manual_quiesce_and_capture(self, op: Operation) -> dict: ...
    def _manual_reseed_and_bind(self, op: Operation, promoted: dict,
                                sealed: dict) -> dict: ...
    def _manual_apply_target_configuration(self, op: Operation, promoted: dict,
                                            reseeded: dict) -> dict: ...
    def _manual_strict_start(self, op: Operation, promoted: dict, reseeded: dict,
                             configured: dict, *, release_owned_hold: bool,
                             resume_collection: bool) -> dict: ...
    def _manual_lookup_readback(self, op: Operation) -> dict: ...


MANUAL_HELPER_CONTRACT = {
    'prepared': 'new_epoch,target_root,tls,configuration,standby_leaves,target_leaves',
    'captured': 'checkpoint,retained_tables,retained_references,index,provider,closure,provider_receipt_facts',
    'reseeded': 'new_epoch,primary,standby,peer,blob_roots,index,seed_manifest_sha256,runtime_guard,provider',
    'configured': 'new_epoch,resources,provider',
    'started': 'new_epoch,primary,standby,peer,blob_roots,index,seed_manifest_sha256,runtime_guard,provider,pods,endpoints,provider_receipt_facts,preservation_before_start,current_immutable_tables,operational_metadata',
    'lookup': 'pods,endpoints,health,runtime_guard,provider,retained_tables,retained_references',
}


def save_evidence(backend, op, kind, data):
    """Fixed helper journal envelope; no raw credentials or bodies belong here."""
    need(kind in MANUAL_HELPER_CONTRACT and type(data) is dict, 'manual_evidence_kind')
    label = 'manual-' + kind
    backend._artifact(op, label, {
        'schema_version': 1, 'kind': 'openlegal_manual_' + kind,
        'operation_id': op.operation_id, 'phase': op.phase,
        'inventory_sha256': op.inventory_sha256,
        'source_graph_sha256': op.source_graph_sha256,
        'authority_sha256': op.authority_sha256, 'data': data,
    })
    filename = op.phase + '-' + label + '.json'
    hashes = op.journal.output_hashes()
    need(filename in hashes, 'manual_evidence_not_acknowledged')
    return {'artifact': filename, 'sha256': hashes[filename]}


def _record(backend, op, name, value):
    # Repeated fresh checks within one phase retain separate immutable receipts.
    # Never overwrite the earlier actual inspect or observer readback.
    sequence = getattr(backend, '_manual_sequence', 0) + 1
    need(type(sequence) is int and 1 <= sequence <= 4096, 'manual_artifact_bound')
    backend._manual_sequence = sequence
    backend._artifact(op, name + '-' + str(sequence).zfill(4), value)


def _shape(value, keys, code):
    need(type(value) is dict and set(value) == set(keys), code)


def _path(value):
    need(type(value) is str and len(value) <= 4096 and '\x00' not in value,
         'manual_path_type')
    path = Path(value)
    need(path.is_absolute() and path != Path('/') and '..' not in path.parts
         and str(path) == value, 'manual_absolute_path')
    return path


def _private_reference(value):
    _shape(value, ('path', 'sha256'), 'manual_private_reference')
    _path(value['path'])
    need(pin(value['sha256']), 'manual_private_reference_hash')


def validate(config, inventory):
    """Closed prepared-resource policy; no implicit provision/credential scope."""
    _shape(config, ('schema_version', 'mode', 'new_managed_epoch',
                   'old_primary_dataset', 'reseed_target', 'replication_peer',
                   'target_configuration', 'emergency_authority'), 'manual_policy_shape')
    need(type(config['schema_version']) is int and config['schema_version'] == 1
         and config['mode'] in ('planned_drill', 'emergency_attested'), 'manual_policy_version')
    need(valid_uuid(config['new_managed_epoch'])
         and config['new_managed_epoch'] != inventory['managed_epoch'], 'manual_new_epoch')
    names = {d['name'] for d in inventory['original_datasets']}
    need(config['old_primary_dataset'] in names, 'manual_original_primary_dataset')
    target = config['reseed_target']
    _shape(target, ('dataset', 'guid', 'path', 'identity', 'uid', 'gid'), 'manual_target_shape')
    need(type(target['dataset']) is str and DATASET.fullmatch(target['dataset'])
         and '@' not in target['dataset'] and target['dataset'] not in names
         and type(target['guid']) is str and re.fullmatch('[0-9]{1,20}', target['guid']),
         'manual_new_target_dataset')
    path = _path(target['path'])
    _shape(target['identity'], ('dev', 'ino'), 'manual_target_identity')
    need(all(type(v) is int and v >= 0 for v in target['identity'].values())
         and all(type(target[k]) is int and 1 <= target[k] <= 65535 for k in ('uid', 'gid'))
         and target['uid'] == inventory['linux']['postgres']['uid'], 'manual_target_owner')
    for old in inventory['original_datasets']:
        source = _path(old['mountpoint'])
        need(not path.is_relative_to(source) and not source.is_relative_to(path),
             'manual_target_original_overlap')
    for old in inventory['ssd_trees'] + inventory['journal_roots'] + [inventory['backup_root']]:
        source = _path(old['path'])
        need(not path.is_relative_to(source) and not source.is_relative_to(path),
             'manual_target_other_root_overlap')
    peer = config['replication_peer']
    _shape(peer, ('host', 'port', 'role', 'application_name', 'client_serial', 'client_dn', 'issuer_dn',
                  'ca', 'certificate', 'private_key'), 'manual_peer_shape')
    need(type(peer['host']) is str and len(peer['host']) <= 253, 'manual_peer_host')
    try:
        address = ipaddress.ip_address(peer['host'])
    except ValueError:
        raise Refused('manual_peer_numeric_address_required') from None
    need(str(address) == peer['host'] and not address.is_unspecified and not address.is_multicast
         and type(peer['port']) is int and 1 <= peer['port'] <= 65535,
         'manual_peer_endpoint')
    need(all(type(peer[k]) is str and IDENTIFIER.fullmatch(peer[k]) for k in ('role', 'application_name'))
         and type(peer['client_serial']) is str and re.fullmatch('[1-9][0-9]{0,49}', peer['client_serial']),
         'manual_peer_identity')
    need(all(type(peer[k]) is str and 0 < len(peer[k].encode('utf-8')) <= 1024
             for k in ('client_dn', 'issuer_dn')), 'manual_peer_certificate_identity')
    for name in ('ca', 'certificate', 'private_key'):
        _private_reference(peer[name])
    _private_reference(config['target_configuration'])
    if config['emergency_authority'] is not None:
        _private_reference(config['emergency_authority'])
    need(config['mode'] != 'planned_drill' or config['emergency_authority'] is None,
         'manual_planned_authority_ambiguity')
    return config


def _evidence(op, reference, kind):
    _shape(reference, ('artifact', 'sha256'), 'manual_helper_reference')
    need(type(reference['artifact']) is str and pin(reference['sha256']),
         'manual_helper_reference_hash')
    raw = op.journal.read_phase_bytes(op.phase, reference['artifact'], reference['sha256'])
    value = _metadata_decode(raw)
    _shape(value, ('schema_version', 'kind', 'operation_id', 'phase',
                   'inventory_sha256', 'source_graph_sha256', 'authority_sha256', 'data'),
           'manual_helper_envelope')
    need(type(value['schema_version']) is int and value['schema_version'] == 1
         and value['kind'] == 'openlegal_manual_' + kind
         and all(value[key] == getattr(op, key) for key in ('operation_id', 'phase',
                 'inventory_sha256', 'source_graph_sha256', 'authority_sha256')),
         'manual_helper_binding')
    _shape(value['data'], MANUAL_HELPER_CONTRACT[kind].split(','), 'manual_helper_data_shape')
    return value['data']


def _metadata_decode(raw):
    """Bulk retained-reference envelopes exceed the small configuration cap."""
    need(type(raw) is bytes and len(raw) <= 128 * 1024**2, 'manual_metadata_json_bound')
    def pairs(items):
        value = {}
        for key, item in items:
            need(key not in value, 'manual_metadata_duplicate'); value[key] = item
        return value
    try:
        value = json.loads(raw, object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(Refused('manual_metadata_nonfinite')))
    except (ValueError, RecursionError, UnicodeError):
        raise Refused('manual_metadata_json') from None
    count = 0
    def walk(item, depth):
        nonlocal count
        count += 1
        need(depth <= 32 and count <= 8_000_000, 'manual_metadata_structure_bound')
        if type(item) is dict:
            for key, child in item.items():
                need(type(key) is str and len(key) <= 1024, 'manual_metadata_key')
                walk(child, depth + 1)
        elif type(item) is list:
            for child in item: walk(child, depth + 1)
        elif type(item) is float:
            need(math.isfinite(item), 'manual_metadata_nonfinite')
    walk(value, 0)
    return value


CHECKPOINT_SQL = 'SELECT openlegal_checkpoint.database_position()'
STANDBY_SQL = CHECKPOINT_SQL
STORAGE_METADATA_SQL = 'SELECT openlegal_checkpoint.storage_metadata()'


def _provider_state(backend, op, **kwargs):
    need(valid_uuid(op.operation_id), 'manual_provider_operation')
    value = backend._sql(op,
        "SELECT openlegal_checkpoint.observe('" + op.operation_id + "'::uuid)", **kwargs)
    _shape(value, ('state', 'hold_control', 'admission_ownership',
                   'checkpoint_owner', 'checkpoint_readback'), 'manual_provider_observer_shape')
    need(type(value['state']) is dict, 'manual_provider_state_shape')
    return value['state']


def _lsn(value):
    need(type(value) is str and re.fullmatch('[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}', value),
         'manual_lsn')
    high, low = value.split('/')
    return (int(high, 16) << 32) | int(low, 16)


def _peer_address(value, expected):
    # PostgreSQL inet::text may retain its explicit /32 or /128 host mask.
    # Compare the actual host address without accepting a broader network.
    try:
        address = ipaddress.ip_interface(value)
    except (ValueError, TypeError):
        raise Refused('manual_peer_observed_address') from None
    need(address.network.prefixlen == address.max_prefixlen
         and str(address.ip) == expected, 'manual_peer_observed_address')


def _observer(value, expected, address):
    keys = {'managed_epoch', 'blob_binding_sha256', 'primary_system_identifier', 'timeline',
            'postmaster_started_at', 'standby_name', 'standby_role', 'standby_addr',
            'standby_client_dn', 'standby_issuer_dn', 'standby_client_serial', 'sender_pid',
            'sender_started_at', 'prefix_lsn', 'flush_lsn'}
    _shape(value, keys, 'manual_observer_shape')
    need(all(value[k] == v for k, v in expected.items())
         and type(value['timeline']) is int and type(value['sender_pid']) is int
         and value['sender_pid'] > 0
         and type(value['sender_started_at']) is str and 0 < len(value['sender_started_at']) <= 128,
         'manual_pair_authenticated_peer')
    _peer_address(value['standby_addr'], address)
    _lsn(value['prefix_lsn']); _lsn(value['flush_lsn'])
    return value


def _corpus_control_transition(old, new, watermark):
    need(type(old) is dict and type(new) is dict and set(old) == set(new),
         'manual_corpus_control_shape_changed')
    allowed = {'index_ack', 'staged_bytes'}
    need(allowed <= set(old) and all(new[k] == old[k] for k in old if k not in allowed),
         'manual_corpus_control_policy_changed')
    need(all(type(v) is int for v in (old['index_ack'], new['index_ack'],
                                    old['staged_bytes'], new['staged_bytes'], watermark))
         and 0 <= old['index_ack'] <= new['index_ack'] <= watermark
         and 0 <= new['staged_bytes'] <= old['staged_bytes'],
         'manual_corpus_control_progress_invalid')


def _fresh_barrier(backend, op, container, expected, address):
    """One fixed fresh prefix; only definite flush lag may be polled read-only."""
    deadline = time.monotonic() + 2
    before = backend._sql(op, 'SELECT openlegal_replication.capture_prefix()',
                          container=container, timeout=2)
    _observer(before, expected, address)
    while True:
        need(time.monotonic() < deadline, 'manual_pair_ack_deadline')
        after = backend._sql(op, 'SELECT openlegal_replication.observe_flush()',
                             container=container, timeout=1)
        _observer(after, expected, address)
        need(all(before.get(k) == after.get(k) for k in before
                 if k not in ('prefix_lsn', 'flush_lsn')), 'manual_pair_changed_during_ack')
        need(time.monotonic() < deadline, 'manual_pair_ack_deadline')
        if _lsn(after['flush_lsn']) >= _lsn(before['prefix_lsn']):
            _record(backend, op, 'manual-fresh-pair-prefix', {'before': before, 'after': after})
            return before, after
        time.sleep(min(0.02, max(0, deadline - time.monotonic())))


def _checkpoint(value, *, recovering=False):
    expected = ('system_identifier', 'timeline', 'postmaster_started_at',
                'replay_lsn', 'receive_lsn', 'in_recovery') if recovering else (
                    'system_identifier', 'timeline', 'postmaster_started_at',
                    'wal_lsn', 'index_ack', 'watermark', 'in_recovery')
    _shape(value, expected, 'manual_checkpoint_shape')
    need(type(value['system_identifier']) is str
         and re.fullmatch('[1-9][0-9]{0,19}', value['system_identifier'])
         and type(value['timeline']) is int and value['timeline'] > 0
         and type(value['postmaster_started_at']) is str
         and 0 < len(value['postmaster_started_at']) <= 128
         and value['in_recovery'] is recovering, 'manual_checkpoint_identity')
    if recovering:
        _lsn(value['receive_lsn']); _lsn(value['replay_lsn'])
    else:
        _lsn(value['wal_lsn'])
        need(all(type(value[k]) is int and value[k] >= 0 for k in ('index_ack', 'watermark')),
             'manual_checkpoint_watermark')
    return value


def _table_proof(value):
    need(type(value) is dict and 1 <= len(value) <= 256, 'manual_table_inventory')
    for name, row in value.items():
        need(type(name) is str and re.fullmatch('[a-z_][a-z0-9_]{0,62}\\.[a-z_][a-z0-9_]{0,62}', name),
             'manual_table_name')
        _shape(row, ('rows', 'sha256'), 'manual_table_fingerprint')
        need(type(row['rows']) is int and row['rows'] >= 0 and pin(row['sha256']),
             'manual_table_fingerprint')


def _references(value):
    need(type(value) is list and len(value) <= 200000, 'manual_reference_bound')
    for row in value:
        _shape(row, ('role', 'storage_key', 'sha256', 'size'), 'manual_reference_shape')
        key = re.fullmatch(r'([0-9a-f]{2})/([0-9a-f]{64})-[0-9a-f-]{36}', row['storage_key'])
        need(key is not None and key[1] == key[2][:2] and key[2] == row['sha256']
             and row['role'] in ('cache', 'corpus') and type(row['size']) is int
             and 0 <= row['size'] <= 100 * 1024**2, 'manual_reference_identity')


def _process_absent(record):
    _shape(record, ('pid', 'start_ticks', 'boot_id'), 'manual_pid_shape')
    need(type(record['pid']) is int and record['pid'] > 0
         and type(record['start_ticks']) is str and record['start_ticks'].isdecimal()
         and valid_uuid(record['boot_id']), 'manual_pid_identity')
    need(Path('/proc/sys/kernel/random/boot_id').read_text().strip() == record['boot_id'],
         'manual_process_boot_changed')
    try:
        now = Path('/proc/' + str(record['pid']) + '/stat').read_text().rsplit(')', 1)[1].split()[19]
    except FileNotFoundError:
        now = None
    need(now != record['start_ticks'], 'manual_old_process_alive')


def _closure(value, inventory):
    _shape(value, ('workloads', 'jobs', 'cri', 'cgroups', 'processes',
                   'sql_sessions', 'runtime_guards'), 'manual_closure_shape')
    expected = {(r['kind'], r['name'], r['uid']) for r in inventory['linux']['workloads']}
    need(type(value['workloads']) is list and len(value['workloads']) == len(expected),
         'manual_workload_closure')
    actual = set()
    for row in value['workloads']:
        _shape(row, ('kind', 'name', 'uid', 'replicas'), 'manual_workload_closure')
        need(type(row['replicas']) is int and row['replicas'] == 0, 'manual_workload_still_active')
        actual.add((row['kind'], row['name'], row['uid']))
    need(actual == expected and value['sql_sessions'] == [], 'manual_workload_or_sql_closure')
    jobs = {(r['name'], r['uid']) for r in inventory['linux']['jobs']}
    need(type(value['jobs']) is list and len(value['jobs']) == len(jobs), 'manual_job_closure')
    seen = set()
    for row in value['jobs']:
        _shape(row, ('name', 'uid', 'finished', 'suspended'), 'manual_job_closure')
        need(type(row['finished']) is bool and type(row['suspended']) is bool
             and (row['finished'] or row['suspended']), 'manual_job_still_active')
        seen.add((row['name'], row['uid']))
    need(seen == jobs, 'manual_job_identity')
    need(type(value['cri']) is list and len(value['cri']) <= 4096, 'manual_cri_bound')
    for row in value['cri']:
        _shape(row, ('id', 'state'), 'manual_cri_shape')
        need(type(row['id']) is str and HEX.fullmatch(row['id'])
             and row['state'] in ('CONTAINER_EXITED', 'REMOVED'), 'manual_cri_not_closed')
    need(type(value['cgroups']) is list and value['cgroups']
         and len(value['cgroups']) <= 512, 'manual_cgroup_bound')
    groups = []
    for row in value['cgroups']:
        _shape(row, ('groups', 'pids'), 'manual_cgroup_shape')
        need(type(row['groups']) is list and row['groups'] and row['pids'] == []
             and all(type(g) is str and g.count(':') >= 2 and g.split(':', 2)[-1] != '/'
                     for g in row['groups']), 'manual_cgroup_not_closed')
        groups.extend(row['groups'])
    from linux_backend import members
    need(not members(groups), 'manual_cgroup_repopulated')
    need(type(value['processes']) is list and len(value['processes']) <= 4096,
         'manual_pid_bound')
    for record in value['processes']:
        _process_absent(record)
    need(type(value['runtime_guards']) is list and len(value['runtime_guards']) <= 512,
         'manual_runtime_guard_bound')
    for row in value['runtime_guards']:
        _shape(row, ('name', 'sha256', 'state', 'locked'), 'manual_runtime_guard_shape')
        need(type(row['name']) is str and '/' not in row['name'] and row['name'].endswith('.json')
             and pin(row['sha256']) and row['state'] == 'closed_clean'
             and row['locked'] is False, 'manual_runtime_guard_uncertain')


def _prepared(op, backend, config):
    data = _evidence(op, backend._manual_verify_prepared(op), 'prepared')
    need(data['new_epoch'] == config['new_managed_epoch'], 'manual_prepared_epoch')
    target = data['target_root']; expected = config['reseed_target']
    _shape(target, set(expected) | {'entries', 'readonly'}, 'manual_prepared_target')
    need(all(target[k] == v for k, v in expected.items()) and target['entries'] == []
         and target['readonly'] is False, 'manual_prepared_target_not_empty')
    need(data['tls'] == config['replication_peer'], 'manual_prepared_tls')
    _shape(data['configuration'], ('raw_sha256', 'resources', 'blob_roots', 'postgres_spec_sha256',
                                   'serving_runtime_image_id'),
           'manual_configuration_shape')
    need(data['configuration']['raw_sha256'] == config['target_configuration']['sha256'],
         'manual_configuration_raw_hash')
    need(pin(data['configuration']['postgres_spec_sha256']), 'manual_prepared_postgres_spec')
    runtime_image = data['configuration']['serving_runtime_image_id']
    need(type(runtime_image) is str and len(runtime_image) <= 512
         and re.fullmatch(r'[A-Za-z0-9:/._@-]*sha256:[0-9a-f]{64}', runtime_image),
         'manual_prepared_runtime_image')
    _resources(data['configuration']['resources'])
    roots = data['configuration']['blob_roots']
    need(type(roots) is dict and set(roots) == {'cache_primary', 'cache_replica',
                                             'corpus_primary', 'corpus_replica'},
         'manual_prepared_blob_roles')
    for root in roots.values():
        _shape(root, ('host_path', 'container_path'), 'manual_prepared_blob_root')
        _path(root['host_path']); _path(root['container_path'])
    sets = []
    for key in ('standby_leaves', 'target_leaves'):
        rows = data[key]
        need(type(rows) is list and 0 < len(rows) <= 16
             and all(type(r) is str and re.fullmatch('[0-9]+:[0-9]+', r) for r in rows)
             and len(set(rows)) == len(rows), 'manual_physical_leaf_identity')
        sets.append(set(rows))
    need(not sets[0] & sets[1], 'manual_second_copy_same_physical_media')
    return data


def _resources(rows):
    need(type(rows) is list and 2 <= len(rows) <= 512, 'manual_configuration_resources')
    seen = set()
    for row in rows:
        _shape(row, ('kind', 'name', 'namespace', 'uid', 'spec_sha256', 'resource_version', 'active'),
               'manual_configuration_resource')
        need(row['kind'] in ('deployment', 'job', 'configmap', 'secret',
                              'persistentvolume', 'persistentvolumeclaim')
             and all(type(row[k]) is str and len(row[k]) <= 256 for k in ('name', 'namespace'))
             and valid_uuid(row['uid']) and pin(row['spec_sha256'])
             and type(row['resource_version']) is str and row['resource_version'].isdecimal()
             and row['active'] is False, 'manual_resource_identity_or_active')
        key = (row['kind'], row['namespace'], row['name'])
        need(key not in seen, 'manual_resource_duplicate'); seen.add(key)
    return rows


def _config(op):
    need(os.geteuid() == 0 and op.action == 'manual_failover' and valid_uuid(op.operation_id),
         'manual_operation_scope')
    config = validate(op.inventory['linux']['manual_recovery'], op.inventory)
    # This mode cannot manufacture a live-source baseline from standby state.
    # A new source-reviewed emergency attestation mechanism is a separate gate.
    need(config['mode'] == 'planned_drill', 'manual_emergency_latest_ack_proof_required')
    op.seal()
    return config


def _inspect(backend, op, side):
    expected = op.inventory['linux']['postgres'][side]
    raw = backend._run(op, 'docker', DOCKER + ['inspect', expected['container_id']], record=False)
    value = decode(raw)
    need(type(value) is list and len(value) == 1, 'manual_container_inspect')
    item = value[0]
    need(item['Id'] == expected['container_id'] and item['Image'] == expected['image_id'],
         'manual_container_replaced')
    mounts = sorted([{'source': r['Source'], 'destination': r['Destination'],
                      'rw': r['RW'], 'type': r['Type']} for r in item['Mounts']],
                    key=lambda r: r['destination'])
    need(mounts == sorted(expected['mounts'], key=lambda r: r['destination']),
         'manual_container_mount_changed')
    state = {key: item['State'].get(key) for key in (
        'Status', 'Running', 'Paused', 'Restarting', 'OOMKilled', 'Dead', 'Pid',
        'ExitCode', 'StartedAt', 'FinishedAt')}
    _record(backend, op, 'manual-' + side + '-container', {
        'id': item['Id'], 'image': item['Image'], 'state': state,
        'mounts': mounts, 'restart_policy': item['HostConfig']['RestartPolicy']})
    return item


def _previous(op, phase):
    value = op.previous.get(phase)
    need(type(value) is dict and value.get('phase') == phase
         and value.get('operation_id') == op.operation_id
         and value.get('status') == 'passed' and type(value.get('actual_exit_code')) is int
         and value['actual_exit_code'] == 0 and value.get('owned_processes_closed') is True
         and all(value.get(k) == getattr(op, k) for k in (
             'inventory_sha256', 'source_graph_sha256', 'authority_sha256')),
         'manual_prior_phase_unverified')
    return value['facts']


def _emit(backend, op, facts):
    need(op.phase in ('fence_old', 'verify_latest', 'promote', 'reseed', 'strict_resume'),
         'manual_phase')
    need(all(facts.get(k) is True for k in REQUIRED_TRUE[op.phase]), 'manual_phase_proof_missing')
    op.seal()
    return backend._receipt(op, facts)


def _provider_facts(value):
    need(type(value) is dict, 'manual_provider_receipt_shape')
    keys = ('provider_hold_before', 'provider_hold_after', 'hold_created_by_operation',
            'collectors_resumed', 'provider_state_before_artifact', 'provider_state_after_artifact')
    need(all(k in value for k in keys) and type(value['hold_created_by_operation']) is bool
         and type(value['collectors_resumed']) is bool, 'manual_provider_receipt_shape')
    return value


def _owned_release(op):
    authority = getattr(op, 'authority', {})
    if authority.get('release_owned_clean_hold_permitted', False) is not True:
        return False
    origin = _previous(op, 'fence_old')
    hold = origin.get('provider_hold_after', {})
    checkpoint_hold = origin.get('provider_checkpoint_readback', {}).get('hold', {})
    return (origin.get('hold_created_by_operation') is True
            and hold.get('owner_operation_id') == op.operation_id
            and hold.get('recovery_hold') is True and hold.get('clean') is True
            and checkpoint_hold.get('operation_id') == op.operation_id
            and checkpoint_hold.get('owned') is True)


def _post_start_changed(op):
    changed = POST_START_OPERATIONAL_TABLES | {'openlegal_replication.control'}
    if _owned_release(op):
        changed |= {'openlegal_admin.provider_control', 'openlegal_checkpoint.operation'}
    return changed


def _old_fence(backend, op):
    fenced = _previous(op, 'fence_old')
    _process_absent(fenced['old_primary_process'])
    from linux_backend import members
    need(not members(fenced['old_primary_cgroups']), 'manual_old_primary_returned')
    old = _inspect(backend, op, 'primary')
    need(old['State']['Running'] is False and old['State']['Pid'] == 0
         and old['HostConfig']['RestartPolicy']['Name'] == 'no', 'manual_old_primary_returned')
    dataset = next(r for r in op.inventory['original_datasets']
                   if r['name'] == op.inventory['linux']['manual_recovery']['old_primary_dataset'])
    raw = backend._run(op, 'zfs', ['get', '-Hp', '-o', 'property,value',
                                  'guid,readonly,mountpoint', dataset['name']])
    try:
        pairs = [line.split('\t') for line in raw.decode('ascii').splitlines()]
        values = dict(pairs)
    except (ValueError, UnicodeError):
        raise Refused('manual_readonly_fence_shape') from None
    need(len(pairs) == len(values) == 3 and values == fenced['old_dataset_readonly'],
         'manual_old_dataset_fence_changed')
    _closure(fenced['sealed_source']['closure'], op.inventory)
    return fenced


def fence_old(backend, op):
    config = _config(op)
    need(op.phase == 'fence_old', 'manual_phase_order')
    prepared = _prepared(op, backend, config)
    captured = _evidence(op, backend._manual_quiesce_and_capture(op), 'captured')
    _closure(captured['closure'], op.inventory)
    checkpoint = _checkpoint(captured['checkpoint'])
    _table_proof(captured['retained_tables']); _references(captured['retained_references'])
    need(type(captured['index']) is dict and pin(captured['index'].get('sha256')),
         'manual_index_baseline')
    provider_facts = _provider_facts(captured['provider_receipt_facts'])
    need(provider_facts['collectors_resumed'] is False, 'manual_collectors_resumed_before_fence')
    # Independent actual SQL parity before stopping the old primary. A provider
    # helper receipt cannot substitute for the source's latest native readback.
    fresh = _checkpoint(backend._sql(op, CHECKPOINT_SQL, which='primary'))
    need(all(fresh[k] == checkpoint[k] for k in ('system_identifier', 'timeline',
              'postmaster_started_at', 'index_ack', 'watermark'))
         and _lsn(fresh['wal_lsn']) >= _lsn(checkpoint['wal_lsn']), 'manual_source_checkpoint_changed')
    need(backend._fingerprints(op) == captured['retained_tables']
         and backend._references(op) == captured['retained_references'],
         'manual_source_rows_changed_after_drain')
    need(_provider_state(backend, op, which='primary')
         == captured['provider'], 'manual_source_provider_state_changed')
    storage = backend._sql(op, STORAGE_METADATA_SQL, which='primary')
    _shape(storage, ('corpus_control', 'cache_storage', 'cache_totals'), 'manual_storage_metadata')
    control_row = storage['corpus_control']
    need(type(control_row) is dict and control_row.get('index_ack') == fresh['index_ack']
         and control_row.get('next_event') == fresh['watermark'] + 1,
         'manual_source_control_row_mismatch')
    cache_storage_row = storage['cache_storage']
    need(type(cache_storage_row) is dict and valid_uuid(cache_storage_row.get('id')),
         'manual_source_cache_storage_row')
    primary = _inspect(backend, op, 'primary')
    need(primary['State']['Running'] is True and type(primary['State']['Pid']) is int
         and primary['State']['Pid'] > 0, 'manual_primary_not_observable')
    pid = primary['State']['Pid']
    from linux_backend import process_cgroup, members
    groups = process_cgroup(pid)
    need(groups and all(g.count(':') >= 2 and g.split(':', 2)[-1] != '/' for g in groups),
         'manual_primary_cgroup_unscoped')
    need(all(any(part in (primary['Id'], 'docker-' + primary['Id'] + '.scope')
                 for part in g.split(':', 2)[-1].split('/')) for g in groups),
         'manual_primary_cgroup_not_owned')
    ticks = Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()[19]
    identity = {'pid': pid, 'start_ticks': ticks, 'boot_id': op.inventory['host']['boot_id']}
    again = _inspect(backend, op, 'primary')
    need(again['State']['Running'] is True and again['State']['Pid'] == pid
         and Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()[19] == ticks
         and process_cgroup(pid) == groups, 'manual_primary_identity_changed_before_stop')
    _record(backend, op, 'manual-old-primary-identity', {'process': identity, 'cgroups': groups})
    dataset = next(r for r in op.inventory['original_datasets']
                   if r['name'] == config['old_primary_dataset'])
    writable = [r for r in op.inventory['linux']['postgres']['primary']['mounts'] if r['rw']]
    need(writable and all(Path(r['source']).is_relative_to(Path(dataset['mountpoint']))
                          for r in writable), 'manual_primary_writes_outside_fenced_dataset')
    cid = primary['Id']
    need(backend._run(op, 'docker', DOCKER + ['update', '--restart=no', cid])
         == (cid + '\n').encode(), 'manual_restart_fence_ack')
    backend._run(op, 'docker', DOCKER + ['stop', '--time', '30', cid], timeout=40)
    stopped = _inspect(backend, op, 'primary')
    need(stopped['State']['Running'] is False and stopped['State']['Pid'] == 0
         and type(stopped['State']['ExitCode']) is int and stopped['State']['ExitCode'] == 0
         and stopped['State']['OOMKilled'] is False and not stopped['State']['Error']
         and stopped['HostConfig']['RestartPolicy']['Name'] == 'no', 'manual_old_primary_not_closed')
    need(not members(groups), 'manual_old_primary_cgroup_populated')
    _process_absent(identity)
    # Filesystem fence follows physical closure; never set a live PGDATA read-only.
    backend._run(op, 'zfs', ['set', 'readonly=on', dataset['name']])
    rows = backend._run(op, 'zfs', ['get', '-Hp', '-o', 'property,value',
                                  'guid,readonly,mountpoint', dataset['name']])
    try:
        pairs = [line.split('\t') for line in rows.decode('ascii').splitlines()]
        values = dict(pairs)
    except (ValueError, UnicodeError):
        raise Refused('manual_readonly_fence_shape') from None
    need(len(pairs) == len(values) == 3 and values == {
        'guid': dataset['guid'], 'readonly': 'on', 'mountpoint': dataset['mountpoint']},
         'manual_readonly_fence_not_confirmed')
    _closure(captured['closure'], op.inventory)
    sealed = dict(captured, checkpoint=fresh, corpus_control_row=control_row,
                  cache_storage_row=cache_storage_row)
    _record(backend, op, 'manual-sealed-source', sealed)
    facts = dict(provider_facts, old_primary_physically_fenced=True,
                 all_old_writers_physically_fenced=True, no_automatic_promotion=True,
                 all_mcp_processes_absent=True, all_mcp_cgroups_empty=True,
                 all_mcp_cri_containers_terminated=True, sql_application_sessions_zero=True,
                 provider_accounting_preserved=True,
                 sealed_source=sealed, prepared=prepared,
                 old_primary_process=identity, old_primary_cgroups=groups,
                 old_dataset_readonly=values)
    return _emit(backend, op, facts)


def verify_latest(backend, op):
    config = _config(op)
    need(op.phase == 'verify_latest', 'manual_phase_order')
    fenced = _old_fence(backend, op); sealed = fenced['sealed_source']
    standby = _inspect(backend, op, 'standby')
    need(standby['State']['Running'] is True and standby['State']['Pid'] > 0,
         'manual_standby_not_running')
    state = _checkpoint(backend._sql(op, STANDBY_SQL, which='standby'), recovering=True)
    before = sealed['checkpoint']
    need(state['system_identifier'] == before['system_identifier']
         and state['timeline'] == before['timeline']
         and _lsn(state['receive_lsn']) >= _lsn(before['wal_lsn'])
         and _lsn(state['replay_lsn']) >= _lsn(before['wal_lsn']), 'manual_latest_wal_not_replayed')
    tables = backend._fingerprints(op, container=standby['Id'])
    refs = backend._references(op, container=standby['Id'])
    need(tables == sealed['retained_tables'] and refs == sealed['retained_references'],
         'manual_standby_retained_state_differs')
    from linux_backend import tree_digest
    need(tree_digest(op.inventory['linux']['index_tree'], time.monotonic() + 600)
         == sealed['index'], 'manual_latest_index_changed')
    _live_blob_references(backend, op, refs)
    _prepared(op, backend, config)
    _record(backend, op, 'manual-standby-latest', {'checkpoint': state, 'tables': tables, 'references': refs})
    return _emit(backend, op, {'latest_acknowledged_wal_present': True,
        'latest_index_verified': True, 'retained_inventory_verified': True,
        'no_old_snapshot_rollback': True, 'sealed_source': sealed,
        'standby_checkpoint': state, 'standby_container_id': standby['Id'],
        'prepared': fenced['prepared']})


def promote(backend, op):
    _config(op)
    need(op.phase == 'promote', 'manual_phase_order')
    latest = _previous(op, 'verify_latest'); _old_fence(backend, op)
    standby = _inspect(backend, op, 'standby')
    need(standby['Id'] == latest['standby_container_id'] and standby['State']['Running'] is True,
         'manual_promotion_target_changed')
    before = _checkpoint(backend._sql(op, STANDBY_SQL, which='standby'), recovering=True)
    previous = latest['standby_checkpoint']
    need(all(before[k] == previous[k] for k in (
             'system_identifier', 'timeline', 'postmaster_started_at', 'in_recovery'))
         and _lsn(before['replay_lsn']) >= _lsn(previous['replay_lsn'])
         and _lsn(before['receive_lsn']) >= _lsn(previous['receive_lsn'])
         and backend._fingerprints(op, container=standby['Id'])
             == latest['sealed_source']['retained_tables']
         and backend._references(op, container=standby['Id'])
             == latest['sealed_source']['retained_references'],
         'manual_standby_changed_before_promotion')
    # Fixed pg_promote permission is preprovisioned to the operator. No new role,
    # credentials, superuser grant or arbitrary pg_ctl command is minted here.
    result = backend._sql(op, "SELECT jsonb_build_object('promoted',pg_promote(true,30))",
                          which='standby', timeout=40)
    need(result == {'promoted': True}, 'manual_promotion_ack')
    after = _checkpoint(backend._sql(op, CHECKPOINT_SQL, which='standby'))
    floor = latest['sealed_source']['checkpoint']
    need(after['system_identifier'] == floor['system_identifier']
         and after['timeline'] > floor['timeline']
         and after['postmaster_started_at'] == before['postmaster_started_at']
         and _lsn(after['wal_lsn']) >= _lsn(floor['wal_lsn'])
         and after['index_ack'] == floor['index_ack'] and after['watermark'] == floor['watermark'],
         'manual_promoted_checkpoint_mismatch')
    _record(backend, op, 'manual-promoted-checkpoint', after)
    # New epoch is not called durably bound until reseed validates actual control.
    # The phase receipt deliberately carries pending_epoch, not a fabricated bind.
    _closure(latest['sealed_source']['closure'], op.inventory)
    return _emit(backend, op, {'old_fence_reverified': True, 'promoted_exact_owned_standby': True,
        'new_epoch_intent_bound': True, 'application_writers_inhibited': True,
        'no_old_snapshot_rollback': True,
        'pending_epoch': op.inventory['linux']['manual_recovery']['new_managed_epoch'],
        'promoted_container_id': standby['Id'], 'promoted_checkpoint': after,
        'sealed_source': latest['sealed_source'], 'prepared': latest['prepared']})


def _live_blob_references(backend, op, references):
    """Exact prepromotion live roots, resolved only through fixed inventory."""
    policy = op.inventory['linux']['restore']['blobs']
    for role in ('cache', 'corpus'):
        for side in ('hdd', 'ssd'):
            spec = policy[role][side]
            if side == 'hdd':
                root = next(r['mountpoint'] for r in op.inventory['original_datasets']
                            if r['name'] == spec['dataset'])
            else:
                root = next(r['path'] for r in op.inventory['ssd_trees']
                            if r['name'] == spec['tree'])
            relative = Path(spec['relative'])
            need(not relative.is_absolute() and '..' not in relative.parts,
                 'manual_blob_relative_path')
            path = Path(root) / relative
            pinning = next((r for r in op.inventory['linux']['bound_roots']
                            if r['path'] == str(path)), None)
            need(pinning is not None, 'manual_blob_root_not_prepared')
            fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                held = os.fstat(fd)
                need((held.st_dev, held.st_ino) == (pinning['dev'], pinning['ino'])
                     and path.resolve() == path, 'manual_blob_root_changed')
                op.seal()
                _reference_fd(fd, [r for r in references if r['role'] == role], time.monotonic() + 600)
                now = path.lstat()
                need((now.st_dev, now.st_ino) == (held.st_dev, held.st_ino),
                     'manual_blob_root_changed')
            finally:
                os.close(fd)


def _reference_fd(root, rows, deadline):
    """Hash through the authenticated root FD, never reopen its pathname."""
    from copy_tree import signature
    root_info = os.fstat(root); witness = {}
    _references(rows)
    for row in rows:
        need(time.monotonic() < deadline, 'manual_reference_deadline')
        shard, name = row['storage_key'].split('/')
        directory = os.open(shard, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                            dir_fd=root)
        try:
            shard_info = os.fstat(directory)
            need(shard_info.st_dev == root_info.st_dev and shard_info.st_uid == root_info.st_uid
                 and stat.S_IMODE(shard_info.st_mode) == 0o700,
                 'manual_reference_shard_identity')
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                         dir_fd=directory)
            try:
                before = os.fstat(fd)
                need(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
                     and before.st_dev == root_info.st_dev and before.st_size == row['size']
                     and before.st_uid == root_info.st_uid and stat.S_IMODE(before.st_mode) == 0o600,
                     'manual_reference_file_identity')
                h = hashlib.sha256(); total = 0
                while True:
                    need(time.monotonic() < deadline, 'manual_reference_deadline')
                    block = os.read(fd, 1024 * 1024)
                    if not block: break
                    total += len(block); need(total <= row['size'], 'manual_reference_grew'); h.update(block)
                need(total == row['size'] and h.hexdigest() == row['sha256']
                     and signature(os.fstat(fd)) == signature(before)
                     and signature(os.stat(name, dir_fd=directory, follow_symlinks=False))
                         == signature(before), 'manual_reference_changed')
                witness[row['storage_key']] = (signature(shard_info), signature(before))
            finally:
                os.close(fd)
        finally:
            os.close(directory)
    for key, (shard_stamp, stamp) in witness.items():
        need(time.monotonic() < deadline, 'manual_reference_deadline')
        shard, name = key.split('/')
        fd = os.open(shard, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                     dir_fd=root)
        try:
            need(signature(os.fstat(fd)) == shard_stamp
                 and signature(os.stat(name, dir_fd=fd, follow_symlinks=False)) == stamp,
                 'manual_reference_final_coherence')
        finally:
            os.close(fd)


def _root_binding(op, data, prepared, references):
    """Verify actual roots/seed bytes and compare full physical generation pins.

    The fixed reseed helper performs the complete physical inventory/copy and
    records its manifest bytes. These are content pins, never success flags.
    This component independently reopens root/marker identities and references.
    """
    need(type(data) is list and len(data) == 4, 'manual_four_blob_roots')
    expected = prepared['configuration']['blob_roots']; rows = {}; doc_roots = []
    for row in data:
        _shape(row, ('role', 'host_path', 'container_path', 'root_dev', 'root_ino',
                     'root_uid', 'root_gid', 'root_mode', 'kernel_filesystem_id',
                     'filesystem_id', 'marker_sha256', 'manifest_sha256', 'reference_sha256'),
               'manual_blob_root_proof')
        role = row['role']
        need(role in expected and role not in rows
             and row['host_path'] == expected[role]['host_path']
             and row['container_path'] == expected[role]['container_path']
             and all(pin(row[k]) for k in ('marker_sha256', 'manifest_sha256', 'reference_sha256'))
             and all(type(row[k]) is int and row[k] >= 0 for k in (
                 'root_dev', 'root_ino', 'root_uid', 'root_gid', 'root_mode', 'kernel_filesystem_id')),
             'manual_blob_binding_identity')
        path = _path(row['host_path'])
        need(path.resolve() == path, 'manual_blob_root_symlink')
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            info = os.fstat(fd)
            need((info.st_dev, info.st_ino, info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode))
                 == tuple(row[k] for k in ('root_dev', 'root_ino', 'root_uid', 'root_gid', 'root_mode'))
                 and row['root_mode'] == 0o700 and row['root_uid'] == 10004
                 and os.fstatvfs(fd).f_fsid == row['kernel_filesystem_id'],
                 'manual_blob_root_actual_identity')
            marker = os.open('.openlegal-seed-v1.json', os.O_RDONLY | os.O_NOFOLLOW
                             | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=fd)
            with os.fdopen(marker, 'rb') as stream:
                first = os.fstat(stream.fileno())
                need(stat.S_ISREG(first.st_mode) and first.st_uid == 0 and first.st_nlink == 1
                     and stat.S_IMODE(first.st_mode) in (0o640, 0o644)
                     and first.st_dev == info.st_dev and 0 < first.st_size <= 16384,
                     'manual_seed_marker_actual_identity')
                raw = stream.read(16385); last = os.fstat(stream.fileno())
                now = os.stat('.openlegal-seed-v1.json', dir_fd=fd, follow_symlinks=False)
                need(all(getattr(first, k) == getattr(last, k) == getattr(now, k) for k in (
                    'st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns'))
                     and digest(raw) == row['marker_sha256'], 'manual_seed_marker_changed')
            seed = decode(raw)
            _shape(seed, ('schema_version', 'kind', 'managed_epoch', 'seed_operation', 'role',
                          'manifest_sha256', 'correspondence_sha256', 'filesystem_id'),
                   'manual_seed_marker_shape')
            need(type(seed['schema_version']) is int and seed['schema_version'] == 1
                 and seed['kind'] == 'openlegal_blob_seed'
                 and seed['managed_epoch'] == op.inventory['linux']['manual_recovery']['new_managed_epoch']
                 and seed['seed_operation'] == op.operation_id and seed['role'] == role
                 and seed['manifest_sha256'] == row['manifest_sha256']
                 and seed['correspondence_sha256'] == row['reference_sha256']
                 and seed['filesystem_id'] == row['filesystem_id'], 'manual_seed_marker_binding')
            reference_role = role.split('_', 1)[0]
            selected = [r for r in references if r['role'] == reference_role]
            need(row['reference_sha256'] == digest(canonical(selected)), 'manual_seed_reference_domain')
            _reference_fd(fd, selected, time.monotonic() + 600)
            now = path.lstat()
            need((now.st_dev, now.st_ino) == (info.st_dev, info.st_ino), 'manual_blob_root_rebound')
        finally:
            os.close(fd)
        rows[role] = row
        doc_roots.append({k: row[k] for k in ('container_path', 'filesystem_id',
            'kernel_filesystem_id', 'marker_sha256', 'role', 'root_dev', 'root_gid',
            'root_ino', 'root_mode', 'root_uid')})
    for prefix in ('cache', 'corpus'):
        left, right = rows[prefix + '_primary'], rows[prefix + '_replica']
        need(left['manifest_sha256'] == right['manifest_sha256']
             and left['reference_sha256'] == right['reference_sha256']
             and left['root_dev'] != right['root_dev'], 'manual_blob_second_copy_differs')
    need(len({(r['root_dev'], r['root_ino']) for r in rows.values()}) == 4,
         'manual_blob_root_alias')
    document = {'boot_id': op.inventory['host']['boot_id'],
        'kind': 'openlegal_blob_seed_binding',
        'managed_epoch': op.inventory['linux']['manual_recovery']['new_managed_epoch'],
        'roots': sorted(doc_roots, key=lambda r: r['role']), 'schema_version': 1}
    return digest(b'openlegal-blob-seed-binding-v1\n' + canonical(document))


def _pair(backend, op, data, promoted, prepared):
    config = op.inventory['linux']['manual_recovery']; sealed = promoted['sealed_source']
    need(data['new_epoch'] == config['new_managed_epoch'] and pin(data['seed_manifest_sha256']),
         'manual_pair_epoch_or_seed')
    for name in ('primary', 'standby'):
        item = data[name]
        _shape(item, ('container_id', 'checkpoint', 'retained_tables', 'retained_references'),
               'manual_pair_postgres')
        need(type(item['container_id']) is str and HEX.fullmatch(item['container_id']),
             'manual_pair_postgres_identity')
        _checkpoint(item['checkpoint'], recovering=name == 'standby')
        _table_proof(item['retained_tables']); _references(item['retained_references'])
        # Only the expressly new protected replication control may change before
        # lookup starts. Provider usage/history/checkpoint audit remain preserved.
        changed = {'openlegal_replication.control'}
        if 'pods' in data:
            changed = _post_start_changed(op)
        stable = lambda rows: {k: v for k, v in rows.items() if k not in changed}
        need(set(item['retained_tables']) == set(sealed['retained_tables'])
             and stable(item['retained_tables']) == stable(sealed['retained_tables'])
             and item['retained_references'] == sealed['retained_references'],
             'manual_pair_retained_state_differs')
        if 'pods' in data:
            need(type(data['current_immutable_tables']) is dict
                 and type(data['operational_metadata']) is dict
                 and set(data['current_immutable_tables']) == set(data['operational_metadata'])
                     == {'primary', 'standby'}
                 and data['current_immutable_tables'][name] == stable(item['retained_tables'])
                 and data['operational_metadata'][name] == {
                     k: v for k, v in item['retained_tables'].items() if k in changed},
                 'manual_poststart_projection_not_current')
    need(data['primary']['container_id'] == promoted['promoted_container_id']
         and data['standby']['container_id'] not in (
             data['primary']['container_id'], op.inventory['linux']['postgres']['primary']['container_id']),
         'manual_pair_container_alias')
    target_id = data['standby']['container_id']
    inspected = decode(backend._run(op, 'docker', DOCKER + ['inspect', target_id], record=False))
    need(type(inspected) is list and len(inspected) == 1, 'manual_reseed_container_inspect')
    target = inspected[0]; policy = config['reseed_target']
    mounts = sorted([{'source': r['Source'], 'destination': r['Destination'],
                      'rw': r['RW'], 'type': r['Type']} for r in target['Mounts']],
                    key=lambda r: r['destination'])
    projection = {'name': target['Name'], 'image': target['Image'],
        'entrypoint': target['Config']['Entrypoint'], 'arguments': target['Config']['Cmd'],
        'user': target['Config']['User'], 'labels': target['Config']['Labels'],
        'mounts': mounts, 'restart_policy': target['HostConfig']['RestartPolicy'],
        'network_mode': target['HostConfig']['NetworkMode']}
    need(target['Id'] == target_id and target['Name'] == '/openlegal-manual-standby-' + op.operation_id
         and target['Image'] == op.inventory['linux']['postgres']['primary']['image_id']
         and target['State']['Running'] is True and target['State']['Pid'] > 0
         and target['Config']['Entrypoint'] == ['/usr/local/bin/postgres']
         and target['Config']['User'] == str(policy['uid']) + ':' + str(policy['gid'])
         and target['Config']['Labels'].get('openlegal.operation') == op.operation_id
         and target['Config']['Labels'].get('openlegal.epoch') == config['new_managed_epoch']
         and target['HostConfig']['Privileged'] is False
         and not target['HostConfig'].get('CapAdd')
         and digest(canonical(projection)) == prepared['configuration']['postgres_spec_sha256'],
         'manual_reseed_container_not_prepared')
    writable = [r for r in mounts if r['rw']]
    need(writable and all(r['type'] == 'bind'
         and Path(r['source']).is_relative_to(Path(policy['path'])) for r in writable),
         'manual_reseed_writes_outside_target')
    fd = os.open(policy['path'], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        actual = os.fstat(fd)
        need((actual.st_dev, actual.st_ino, actual.st_uid, actual.st_gid) == (
            policy['identity']['dev'], policy['identity']['ino'], policy['uid'], policy['gid']),
             'manual_reseed_target_replaced')
    finally:
        os.close(fd)
    _record(backend, op, 'manual-reseed-container', projection)
    if 'pods' in data:
        for side in ('primary', 'standby'):
            cid = data[side]['container_id']
            storage = backend._sql(op, STORAGE_METADATA_SQL, container=cid)
            _shape(storage, ('corpus_control', 'cache_storage', 'cache_totals'), 'manual_storage_metadata')
            control = storage['corpus_control']
            _corpus_control_transition(sealed['corpus_control_row'], control,
                                       sealed['checkpoint']['watermark'])
            accounting = storage['cache_storage']
            previous = sealed['cache_storage_row']; fields = {'snapshots', 'queries', 'referenced_bytes'}
            need(type(accounting) is dict and set(accounting) == set(previous)
                 and fields <= set(previous)
                 and all(accounting[k] == previous[k] for k in previous if k not in fields),
                 'manual_cache_storage_identity_changed')
            totals = storage['cache_totals']
            need(type(totals) is dict and set(totals) == fields
                 and all(type(totals[k]) is int and totals[k] >= 0
                         and accounting[k] == totals[k] for k in fields)
                 and totals['snapshots'] == data[side]['retained_tables']['openlegal.cache_snapshot']['rows']
                 and totals['queries'] == data[side]['retained_tables']['openlegal.cache_query']['rows'],
                 'manual_cache_accounting_not_derived')
    first = data['primary']['checkpoint']; second = data['standby']['checkpoint']
    need(first['system_identifier'] == second['system_identifier']
         == sealed['checkpoint']['system_identifier']
         and first['timeline'] == second['timeline'] == promoted['promoted_checkpoint']['timeline']
         and _lsn(second['replay_lsn']) >= _lsn(first['wal_lsn'])
         and _lsn(second['receive_lsn']) >= _lsn(first['wal_lsn']), 'manual_pair_latest_wal')
    binding = _root_binding(op, data['blob_roots'], prepared, sealed['retained_references'])
    seed_document = {'schema_version': 1, 'kind': 'openlegal_manual_seed_correspondence',
        'managed_epoch': config['new_managed_epoch'], 'operation_id': op.operation_id,
        'roots': sorted([{k: r[k] for k in ('role', 'manifest_sha256', 'reference_sha256')}
                         for r in data['blob_roots']], key=lambda r: r['role'])}
    need(data['seed_manifest_sha256'] == digest(canonical(seed_document)),
         'manual_seed_correspondence_domain')
    need(data['provider'] == sealed['provider'], 'manual_provider_accounting_changed')
    need(_provider_state(backend, op, container=data['primary']['container_id']) == sealed['provider'],
         'manual_provider_current_state_changed')
    need(type(data['index']) is dict and data['index'].get('watermark') == first['watermark']
         and pin(data['index'].get('sha256')), 'manual_pair_index_not_current')
    guard = data['runtime_guard']
    _shape(guard, ('path', 'dev', 'ino', 'state', 'epoch', 'locked'), 'manual_pair_runtime_guard')
    _path(guard['path'])
    need(all(type(guard[k]) is int and guard[k] >= 0 for k in ('dev', 'ino'))
         and guard['state'] == ('active' if 'pods' in data else 'prepared_clean')
         and guard['locked'] is ('pods' in data)
         and guard['epoch'] == config['new_managed_epoch'],
         'manual_pair_runtime_guard_not_clean')
    peer = data['peer']
    _shape(peer, ('before', 'after'), 'manual_pair_observer_proof')
    expected = {'managed_epoch': config['new_managed_epoch'], 'blob_binding_sha256': binding,
                'primary_system_identifier': first['system_identifier'], 'timeline': first['timeline'],
                'postmaster_started_at': first['postmaster_started_at'],
                'standby_name': config['replication_peer']['application_name'],
                'standby_role': config['replication_peer']['role'],
                'standby_client_dn': config['replication_peer']['client_dn'],
                'standby_issuer_dn': config['replication_peer']['issuer_dn'],
                'standby_client_serial': config['replication_peer']['client_serial']}
    for observed in (peer['before'], peer['after']):
        _observer(observed, expected, config['replication_peer']['host'])
    need(all(peer['before'].get(k) == peer['after'].get(k) for k in peer['before']
             if k not in ('prefix_lsn', 'flush_lsn'))
         and _lsn(peer['after']['flush_lsn']) >= _lsn(peer['before']['prefix_lsn']),
         'manual_pair_fresh_prefix_not_flushed')
    # A helper's typed observations alone are insufficient: perform fresh fixed
    # observer calls on the exact promoted CID, then cover their new WAL prefix.
    _fresh_barrier(backend, op, data['primary']['container_id'], expected,
                   config['replication_peer']['host'])
    return binding


def reseed(backend, op):
    config = _config(op)
    need(op.phase == 'reseed', 'manual_phase_order')
    promoted = _previous(op, 'promote')
    _old_fence(backend, op)
    # Fail-closed physical preparation is checked afresh before basebackup can
    # write anything, including after a prior interrupted/manual operation.
    prepared = _prepared(op, backend, config)
    reference = backend._manual_reseed_and_bind(op, promoted, promoted['sealed_source'])
    data = _evidence(op, reference, 'reseeded')
    binding = _pair(backend, op, data, promoted, prepared)
    _old_fence(backend, op)
    return _emit(backend, op, {'second_copy_durably_seeded': True,
        'both_pg_identities_verified': True, 'both_blob_generations_verified': True,
        'new_epoch_bound': True, 'pair': data, 'blob_binding_sha256': binding,
        'pair_reference': reference, 'promoted': promoted, 'prepared': prepared})


def strict_resume(backend, op):
    _config(op)
    need(op.phase == 'strict_resume', 'manual_phase_order')
    _old_fence(backend, op)
    seeded = _previous(op, 'reseed'); promoted = seeded['promoted']; data = seeded['pair']
    _pair(backend, op, data, promoted, seeded['prepared'])
    baseline_reference = seeded['pair_reference']
    _shape(baseline_reference, ('artifact', 'sha256'), 'manual_prestart_preservation_reference')
    baseline_raw = op.journal.read_phase_bytes('reseed', baseline_reference['artifact'],
                                             baseline_reference['sha256'])
    baseline = _metadata_decode(baseline_raw)
    need(baseline.get('operation_id') == op.operation_id and baseline.get('phase') == 'reseed'
         and baseline.get('kind') == 'openlegal_manual_reseeded' and baseline.get('data') == data
         and all(baseline.get(k) == getattr(op, k) for k in (
             'inventory_sha256', 'source_graph_sha256', 'authority_sha256')),
         'manual_prestart_preservation_reused_or_changed')
    configured = _evidence(op, backend._manual_apply_target_configuration(
        op, promoted, data), 'configured')
    need(configured['new_epoch'] == data['new_epoch']
         and configured['provider'] == promoted['sealed_source']['provider'],
         'manual_configured_epoch_or_provider')
    _resources(configured['resources'])
    identity = lambda row: (row['kind'], row['name'], row['namespace'], row['uid'], row['spec_sha256'])
    need({identity(r) for r in configured['resources']}
         == {identity(r) for r in seeded['prepared']['configuration']['resources']},
         'manual_all_component_configuration_mismatch')
    # Permissions come from the frozen authenticated authority, never config or
    # helper observations. Absent or non-true values deny BEFORE side effects.
    authority = getattr(op, 'authority', {})
    release = _owned_release(op)
    # Collection activation is a separate reviewed stage AFTER the accepted full
    # manual operation. This phase cannot start a provider request, even if a
    # broader standing authority permits a later canary window.
    collection = False
    _old_fence(backend, op)
    started = _evidence(op, backend._manual_strict_start(
        op, promoted, data, configured, release_owned_hold=release,
        resume_collection=collection), 'started')
    reference = started['preservation_before_start']
    _shape(reference, ('phase', 'artifact', 'sha256'), 'manual_prestart_preservation_reference')
    need(reference['phase'] == 'reseed'
         and {k: reference[k] for k in ('artifact', 'sha256')} == seeded['pair_reference'],
         'manual_prestart_preservation_binding')
    raw = op.journal.read_phase_bytes('reseed', reference['artifact'], reference['sha256'])
    prior = _metadata_decode(raw)
    need(prior.get('operation_id') == op.operation_id and prior.get('phase') == 'reseed'
         and prior.get('kind') == 'openlegal_manual_reseeded'
         and prior.get('data') == data, 'manual_prestart_preservation_reused_or_changed')
    # Observer/physical proof must be repeated after actual startup, not reused.
    _pair(backend, op, started, promoted, seeded['prepared'])
    _old_fence(backend, op)
    provider = _provider_facts(started['provider_receipt_facts'])
    need(not provider['collectors_resumed'] or collection,
         'manual_collection_started_without_permission')
    final = _evidence(op, backend._manual_lookup_readback(op), 'lookup')
    need(final['provider'] == promoted['sealed_source']['provider']
         and final['runtime_guard'] == started['runtime_guard'], 'manual_lookup_final_state_changed')
    _table_proof(final['retained_tables']); _references(final['retained_references'])
    baseline_tables = promoted['sealed_source']['retained_tables']; changed = _post_start_changed(op)
    need(set(final['retained_tables']) == set(baseline_tables)
         and {k: v for k, v in final['retained_tables'].items() if k not in changed}
             == {k: v for k, v in baseline_tables.items() if k not in changed}
         and final['retained_references'] == promoted['sealed_source']['retained_references'],
         'manual_final_retained_state_changed')
    need(type(final['pods']) is list and len(final['pods']) == 1, 'manual_lookup_pod_missing')
    for pod in final['pods']:
        _shape(pod, ('uid', 'configured_image', 'runtime_image_id', 'ready', 'restarts', 'epoch'),
               'manual_lookup_pod_shape')
        need(valid_uuid(pod['uid']) and pod['configured_image'] == op.inventory['linux']['serving_image']
             and pod['runtime_image_id'] == seeded['prepared']['configuration']['serving_runtime_image_id']
             and pod['ready'] is True and type(pod['restarts']) is int and pod['restarts'] == 0
             and pod['epoch'] == data['new_epoch'], 'manual_lookup_pod_not_strict_ready')
    need(final['pods'] == started['pods'], 'manual_lookup_restarted_during_final_audit')
    need(type(final['endpoints']) is list and len(final['endpoints']) == 1
         and final['endpoints'][0] in {p['uid'] for p in final['pods']},
         'manual_lookup_endpoint_identity')
    health = final['health']
    _shape(health, ('pod_uid', 'status', 'body_sha256', 'observed_at'), 'manual_lookup_health_shape')
    need(health['pod_uid'] == final['pods'][0]['uid'] and type(health['status']) is int
         and health['status'] == 200 and pin(health['body_sha256'])
         and type(health['observed_at']) in (int, float)
         and 0 <= time.time() - health['observed_at'] < 120, 'manual_lookup_health_not_fresh')
    binding_keys = ('managed_epoch', 'blob_binding_sha256', 'primary_system_identifier',
        'timeline', 'postmaster_started_at', 'standby_name', 'standby_role',
        'standby_client_dn', 'standby_issuer_dn', 'standby_client_serial')
    final_expected = {k: started['peer']['after'][k] for k in binding_keys}
    _fresh_barrier(backend, op, started['primary']['container_id'], final_expected,
                   op.inventory['linux']['manual_recovery']['replication_peer']['host'])
    return _emit(backend, op, dict(provider, old_fence_reverified=True,
        both_durable_copies_ready=True, new_epoch_bound=True, lookup_ready=True,
        provider_accounting_preserved=True, final_pair=started, final_lookup=final,
        blob_binding_sha256=seeded['blob_binding_sha256']))


# Fixed source-owned exception domain, not supplied by an inventory. Startup
# accounting, orphan/staging cleanup, index progress and transient read leases
# are represented separately from immutable retained legal/history/provider rows.
# Exact ready SQL references and four permanent generation manifests remain
# required even for these exceptions; this is not permission to lose a blob.
POST_START_OPERATIONAL_TABLES = frozenset({
    'openlegal.cache_storage', 'openlegal.blob_deletion',
    'openlegal.corpus_control', 'openlegal.corpus_staging', 'openlegal.corpus_blob_deletion',
    'openlegal.corpus_session', 'openlegal.corpus_pin', 'openlegal.corpus_citation_lease',
})
