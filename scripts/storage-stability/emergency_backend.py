"""First, read-only running-standby recovery components.

This does NOT dispatch promotion/reseed/start and cannot return a Ready receipt.
Planned switchover is unchanged. The fixed root producer must supply real native
fencing, SQL and replacement-media receipts; missing ports fail, never synthesize
healthy-primary observations. Original unavailable sources are not opened here.
"""
from __future__ import annotations

import copy
import os
from pathlib import Path
import re
import stat
import time
from types import MappingProxyType
from typing import Protocol

from interface import Refused, need
from inventory import canonical, digest, pin, private_bytes, valid_uuid
import manual_backend as manual
import epoch_observation as observation
from epoch_observation import (_root, _blob_root, _verify_root, _verify_marker,
    CONTROL_FIELDS, _control, _subject_blob_binding)

POSITION_SQL = 'SELECT openlegal_replication.survivor_position()'
SNAPSHOT_SQL = 'SELECT openlegal_checkpoint.survivor_snapshot()'
CLOSURE_SQL = 'SELECT openlegal_checkpoint.client_sessions()'
REPLAY_SECONDS = 15
ROLES = ('cache_primary', 'cache_replica', 'corpus_primary', 'corpus_replica')
POSITION_FIELDS = {'schema_version', 'kind', 'in_recovery', 'system_identifier', 'timeline',
    'postmaster_started_at', 'data_directory', 'control', 'receive_lsn', 'replay_lsn',
    'replay_paused', 'wal_receiver_count'}


class SurvivorReadPort(Protocol):
    """Fixed source-owned ports; no inventory callbacks/SQL fragments.

    Every return is a {phase,artifact,sha256} reference to actual raw evidence
    through BOTH journals. Root owns native CID/mount-FD correspondence, waited
    CLI exits, SQL limits/backend closure, and physical-media/WWID qualification.
    """
    def _emergency_survivor_observe(self, op, *, container_id: str,
                                    deadline: float) -> dict: ...
    def _emergency_survivor_snapshot(self, op, *, container_id: str) -> dict: ...
    def _emergency_survivor_sql_closed(self, op, *, container_id: str) -> dict: ...
    def _emergency_current_configuration(self, op) -> dict: ...


def _shape(value, fields, code): manual._shape(value, fields, code)


def _freeze(value):
    if type(value) is dict: return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if type(value) is list: return tuple(_freeze(v) for v in value)
    return value


def _private(reference, limit=4 * 1024**2):
    manual._private_reference(reference)
    raw = private_bytes(reference['path'], reference['sha256'], limit=limit)
    return raw, manual._metadata_decode(raw)






def validate_authority(op):
    """Read immutable pre-fault authority; NEVER mint it from survivor state."""
    need(os.geteuid() == 0 and op.action == 'manual_failover'
         and op.phase in ('qualify', 'fence_old', 'verify_latest')
         and op.authority.get('standby_survivor_recovery_permitted', False) is True,
         'survivor_explicit_authority_required')
    policy = op.inventory['linux']['manual_recovery']
    need(policy['mode'] == 'emergency_attested', 'survivor_emergency_mode_only')
    reference = policy['emergency_authority']
    need(reference is not None and reference['sha256'] == op.authority.get('standby_recovery_authority_sha256'),
         'survivor_authority_raw_pin')
    _, value = _private(reference)
    _shape(value, ('schema_version', 'kind', 'operation_id', 'profile', 'new_managed_epoch',
        'certificate', 'pre_ack_install_proof', 'original_media_escrow',
        'replacement_qualification'), 'survivor_authority_shape')
    need(type(value['schema_version']) is int and value['schema_version'] == 1
         and value['kind'] == 'openlegal_standby_recovery_authority'
         and value['operation_id'] == op.operation_id and valid_uuid(op.operation_id)
         and value['profile'] == 'running_bound_standby_v1'
         and value['new_managed_epoch'] == policy['new_managed_epoch'], 'survivor_authority_identity')
    cert_raw, certificate = _private(value['certificate'])
    need(digest(cert_raw) == op.authority.get('current_epoch_certificate_sha256'),
         'survivor_current_certificate_authority')
    _shape(certificate, ('schema_version', 'kind', 'subject'), 'survivor_certificate_shape')
    need(type(certificate['schema_version']) is int and certificate['schema_version'] == 1
         and certificate['kind'] == 'openlegal_strict_epoch_certificate', 'survivor_certificate_kind')
    subject = certificate['subject']
    _shape(subject, ('managed_epoch', 'host_boot_id', 'runtime', 'control', 'primary', 'standby',
                     'blob_roots', 'original_inventory'), 'survivor_certificate_subject')
    _control(subject['control'])
    need(subject['managed_epoch'] == subject['control']['managed_epoch']
         == op.inventory['managed_epoch'] and valid_uuid(value['new_managed_epoch'])
         and value['new_managed_epoch'] != subject['managed_epoch']
         and subject['host_boot_id'] == op.inventory['host']['boot_id']
         == Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
         'survivor_epoch_or_boot_changed')
    _shape(subject['runtime'], ('revision', 'image_id', 'configuration_sha256', 'migration_table_sha256'),
           'survivor_runtime_subject')
    need(type(subject['runtime']['revision']) is str and re.fullmatch('[0-9a-f]{40}', subject['runtime']['revision'])
         and type(subject['runtime']['image_id']) is str
         and re.fullmatch('sha256:[0-9a-f]{64}', subject['runtime']['image_id'])
         and all(pin(subject['runtime'][k]) for k in ('configuration_sha256', 'migration_table_sha256')),
         'survivor_runtime_pins')
    for side in ('primary', 'standby'):
        row = subject[side]
        _shape(row, ('container_id', 'image_id', 'postmaster_started_at', 'data_directory', 'data_root', 'process'),
               'survivor_native_subject')
        need(pin(row['container_id']) and type(row['image_id']) is str
             and re.fullmatch('sha256:[0-9a-f]{64}', row['image_id'])
             and type(row['postmaster_started_at']) is str and 0 < len(row['postmaster_started_at']) <= 128,
             'survivor_native_identity')
        manual._path(row['data_directory']); _root(row['data_root'])
        _shape(row['process'], ('pid', 'start_ticks', 'boot_id'), 'survivor_native_process_identity')
        need(type(row['process']['pid']) is int and row['process']['pid'] > 0
             and type(row['process']['start_ticks']) is str and row['process']['start_ticks'].isdecimal()
             and row['process']['boot_id'] == subject['host_boot_id'], 'survivor_native_process_identity')
        need(row['container_id'] == op.inventory['linux']['postgres'][side]['container_id']
             and row['image_id'] == op.inventory['linux']['postgres'][side]['image_id'],
             'survivor_native_inventory_changed')
    need(subject['primary']['container_id'] != subject['standby']['container_id'], 'survivor_native_alias')
    roots = subject['blob_roots']
    need(type(roots) is dict and set(roots) == set(ROLES), 'survivor_four_original_blob_roots')
    for root in roots.values(): _blob_root(root)
    need(all(roots[r]['uid'] == 10004 for r in roots), 'survivor_blob_owner')
    need(_subject_blob_binding(subject) == subject['control']['blob_binding_sha256'],
         'survivor_certificate_blob_binding')
    # This branch reads only the surviving source, never an unavailable primary.
    _verify_root(subject['standby']['data_root'])
    process = subject['standby']['process']
    try:
        actual_ticks = Path('/proc/' + str(process['pid']) + '/stat').read_text().rsplit(')', 1)[1].split()[19]
    except FileNotFoundError:
        raise Refused('survivor_native_process_gone') from None
    need(actual_ticks == process['start_ticks'], 'survivor_native_process_reused')
    for role in observation.SSD_SURVIVORS:
        _verify_root(roots[role]); _verify_marker(roots[role], role, subject['managed_epoch'])
    _, prepared = _private(value['pre_ack_install_proof'])
    _shape(prepared, ('schema_version', 'kind', 'certificate_sha256', 'subject_sha256',
        'status', 'actual_exit_code', 'owned_processes_closed', 'head_installs'),
        'survivor_pre_ack_proof')
    need(type(prepared['schema_version']) is int and prepared['schema_version'] == 1
         and prepared['kind'] == 'openlegal_pre_ack_epoch_install'
         and prepared['certificate_sha256'] == digest(cert_raw)
         and prepared['subject_sha256'] == digest(canonical(subject))
         and prepared['status'] == 'passed' and type(prepared['actual_exit_code']) is int
         and prepared['actual_exit_code'] == 0 and prepared['owned_processes_closed'] is True,
         'survivor_pre_ack_not_completed')
    heads = prepared['head_installs']
    need(type(heads) is list and len(heads) == 2, 'survivor_two_pre_ack_installs')
    for head in heads:
        _shape(head, ('root_dev', 'root_ino', 'head_sha256', 'readback_sha256', 'directory_fsynced'),
               'survivor_head_install')
        expected_head = digest(canonical({'schema_version': 1, 'kind': 'openlegal_strict_epoch_head',
            'managed_epoch': subject['managed_epoch'], 'certificate_sha256': digest(cert_raw)}))
        need(all(type(head[k]) is int and head[k] >= 0 for k in ('root_dev', 'root_ino'))
             and head['head_sha256'] == expected_head and head['readback_sha256'] == expected_head
             and head['directory_fsynced'] is True, 'survivor_head_not_durable')
    need(heads[0]['root_dev'] != heads[1]['root_dev'], 'survivor_pre_ack_same_medium')
    _, original = _private(subject['original_inventory'])
    originals = original['original_datasets']; extras = original['active_datasets']
    need(type(originals) is list and len(originals) == 7 and type(extras) is list and len(extras) <= 16,
         'survivor_original_inventory')
    _, escrow = _private(value['original_media_escrow'])
    _shape(escrow, ('schema_version', 'kind', 'original_inventory_sha256', 'datasets',
                    'unavailable_blob_roles', 'original_controls_preserved'), 'survivor_escrow_shape')
    expected = {(r['name'], r['guid']) for r in originals + extras}
    rows = escrow['datasets']
    need(type(rows) is list and len(rows) == len(expected), 'survivor_escrow_dataset_coverage')
    for row in rows:
        _shape(row, ('name', 'guid', 'state', 'last_snapshot_guid'), 'survivor_escrow_dataset')
        need(row['state'] in ('available_retained', 'unavailable_retained')
             and (row['last_snapshot_guid'] is None or type(row['last_snapshot_guid']) is str
                  and re.fullmatch('[0-9]{1,20}', row['last_snapshot_guid'])), 'survivor_escrow_state')
    need(type(escrow['schema_version']) is int and escrow['schema_version'] == 1
         and escrow['kind'] == 'openlegal_original_media_escrow'
         and escrow['original_inventory_sha256'] == subject['original_inventory']['sha256']
         and {(r['name'], r['guid']) for r in rows} == expected
         and escrow['unavailable_blob_roles'] == list(observation.HDD_LOST)
         and escrow['original_controls_preserved'] is True, 'survivor_escrow_not_preserved')
    _, replacement = _private(value['replacement_qualification'])
    _shape(replacement, ('schema_version', 'kind', 'operation_id', 'new_managed_epoch',
        'journal_roots', 'physical_media_sha256', 'status', 'actual_exit_code',
        'owned_processes_closed'), 'survivor_replacement_qualification')
    need(type(replacement['schema_version']) is int and replacement['schema_version'] == 1
         and replacement['kind'] == 'openlegal_survivor_replacement_qualification'
         and replacement['operation_id'] == op.operation_id
         and replacement['new_managed_epoch'] == value['new_managed_epoch']
         and replacement['journal_roots'] == op.inventory['journal_roots']
         and pin(replacement['physical_media_sha256']) and replacement['status'] == 'passed'
         and type(replacement['actual_exit_code']) is int and replacement['actual_exit_code'] == 0
         and replacement['owned_processes_closed'] is True, 'survivor_replacement_not_completed')
    return _freeze({'authority': value, 'certificate': certificate, 'escrow': escrow,
                    'replacement': replacement})


def _verify_journals(op):
    roots = op.inventory['journal_roots']
    need(type(roots) is list and len(roots) == 2, 'survivor_two_journals_required')
    devices = []
    for row in roots:
        _shape(row, ('path', 'identity'), 'survivor_journal_descriptor')
        _shape(row['identity'], ('dev', 'ino'), 'survivor_journal_identity')
        path = manual._path(row['path']); need(path.resolve() == path, 'survivor_journal_symlink')
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            info = os.fstat(fd)
            need(info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o700
                 and (info.st_dev, info.st_ino) == (row['identity']['dev'], row['identity']['ino'])
                 and (path.lstat().st_dev, path.lstat().st_ino) == (info.st_dev, info.st_ino),
                 'survivor_journal_actual_identity')
            devices.append(info.st_dev)
        finally: os.close(fd)
    need(devices[0] != devices[1], 'survivor_journal_same_filesystem')


def save_evidence(op, kind, payload, *, ordinal=None):
    need(kind in ('fence', 'position', 'snapshot', 'sql-closure', 'replacement', 'configuration', 'survivor-target',
                  'survivor-seal', 'preserved-sources'), 'survivor_evidence_kind')
    need(ordinal is None or kind in ('position', 'configuration', 'sql-closure')
         and type(ordinal) is int and 1 <= ordinal <= 1024, 'survivor_evidence_ordinal')
    name = op.phase + '-emergency-' + kind + ('' if ordinal is None else '-' + str(ordinal).zfill(4)) + '.json'
    value = {'schema_version': 1, 'kind': 'openlegal_emergency_' + kind,
        'operation_id': op.operation_id, 'phase': op.phase, 'inventory_sha256': op.inventory_sha256,
        'source_graph_sha256': op.source_graph_sha256, 'authority_sha256': op.authority_sha256,
        'observed_monotonic': time.monotonic(), 'payload': payload}
    sha = op.journal.save(name, value)
    need(op.journal.output_hashes().get(name) == sha
         and op.journal.read_phase_bytes(op.phase, name, sha) == canonical(value),
         'survivor_evidence_not_dual_acknowledged')
    return {'phase': op.phase, 'artifact': name, 'sha256': sha}


def read_evidence(op, reference, kind, *, fresh=True):
    _shape(reference, ('phase', 'artifact', 'sha256'), 'survivor_evidence_reference')
    need(type(reference['phase']) is str and pin(reference['sha256']), 'survivor_evidence_pin')
    raw = op.journal.read_phase_bytes(reference['phase'], reference['artifact'], reference['sha256'])
    value = manual._metadata_decode(raw)
    _shape(value, ('schema_version', 'kind', 'operation_id', 'phase', 'inventory_sha256',
        'source_graph_sha256', 'authority_sha256', 'observed_monotonic', 'payload'), 'survivor_evidence_envelope')
    need(type(value['schema_version']) is int and value['schema_version'] == 1
         and value['kind'] == 'openlegal_emergency_' + kind and value['phase'] == reference['phase']
         and all(value[k] == getattr(op, k) for k in ('operation_id', 'inventory_sha256',
                 'source_graph_sha256', 'authority_sha256')), 'survivor_evidence_reused')
    stamp = value['observed_monotonic']
    need(type(stamp) in (int, float) and 0 <= time.monotonic() - stamp <= (120 if fresh else 86400),
         'survivor_evidence_stale')
    return value['payload']


def validate_fence(op, reference, subject):
    value = read_evidence(op, reference, 'fence')
    _shape(value, ('primary_container_id', 'primary_image_id', 'primary_state', 'restart_policy',
        'primary_sql', 'closed_processes', 'closed_cgroups', 'closed_workload_uids',
        'closed_cri_containers', 'closed_operator_processes', 'operator_autostart_inhibited'),
        'survivor_fence_shape')
    need(value['primary_container_id'] == subject['primary']['container_id']
         and value['primary_image_id'] == subject['primary']['image_id']
         and value['primary_state'] in ('exited', 'removed') and value['restart_policy'] == 'no'
         and value['primary_sql'] == 'unavailable_not_queried'
         and value['operator_autostart_inhibited'] is True, 'survivor_primary_not_fenced')
    for key in ('closed_processes', 'closed_operator_processes'):
        need(type(value[key]) is list and len(value[key]) <= 4096, 'survivor_fence_process_bound')
        for row in value[key]: manual._process_absent(row)
    need(value['closed_processes'] and type(value['closed_cgroups']) is list
         and value['closed_cgroups'], 'survivor_fence_physical_proof_missing')
    need(subject['primary']['process'] in value['closed_processes'],
         'survivor_primary_process_not_in_closure')
    expected = {r['uid'] for r in op.inventory['linux']['workloads'] + op.inventory['linux']['jobs']}
    cri = value['closed_cri_containers']
    need(type(cri) is list and len(cri) <= 4096, 'survivor_cri_bound')
    for row in cri:
        _shape(row, ('id', 'state', 'owner_uid'), 'survivor_cri_record')
        need(pin(row['id']) and row['state'] in ('CONTAINER_EXITED', 'REMOVED')
             and row['owner_uid'] in expected, 'survivor_cri_not_closed')
    ids = {row['id'] for row in cri} | {subject['primary']['container_id']}
    covered = set()
    for row in value['closed_cgroups']:
        _shape(row, ('path', 'dev', 'ino', 'state', 'container_id'), 'survivor_cgroup_proof')
        path = manual._path(row['path'])
        need(row['container_id'] in ids and str(path).startswith('/sys/fs/cgroup/')
             and row['container_id'] in str(path)
             and all(type(row[k]) is int and row[k] >= 0 for k in ('dev', 'ino')),
             'survivor_cgroup_not_bound')
        covered.add(row['container_id'])
        if row['state'] == 'removed': need(not path.exists(), 'survivor_cgroup_reappeared')
        else:
            need(row['state'] == 'empty', 'survivor_cgroup_state')
            fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                info = os.fstat(fd)
                need((info.st_dev, info.st_ino) == (row['dev'], row['ino']), 'survivor_cgroup_reused')
                event = os.open('cgroup.events', os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
                try: raw = os.read(event, 4096).decode('ascii')
                finally: os.close(event)
                need(dict(line.split() for line in raw.splitlines()).get('populated') == '0',
                     'survivor_cgroup_populated')
            finally: os.close(fd)
    need(covered == ids, 'survivor_cgroup_coverage')
    need(type(value['closed_workload_uids']) is list and len(value['closed_workload_uids']) == len(expected)
         and set(value['closed_workload_uids']) == expected, 'survivor_writer_inventory_incomplete')
    return value


def _position(value, subject):
    _shape(value, POSITION_FIELDS, 'survivor_position_shape')
    expected = subject['standby']
    need(type(value['schema_version']) is int and value['schema_version'] == 1
         and value['kind'] == 'openlegal_survivor_position' and value['in_recovery'] is True
         and value['system_identifier'] == subject['control']['primary_system_identifier']
         and value['timeline'] == subject['control']['timeline']
         and type(value['timeline']) is int
         and value['postmaster_started_at'] == expected['postmaster_started_at']
         and value['data_directory'] == expected['data_directory']
         and value['control'] == subject['control'] and value['replay_paused'] is False
         and type(value['wal_receiver_count']) is int and 0 <= value['wal_receiver_count'] <= 1,
         'survivor_position_identity')
    manual._lsn(value['receive_lsn']); manual._lsn(value['replay_lsn'])
    return value


def _query(op, reference, kind, sql, cid):
    payload = read_evidence(op, reference, kind)
    _shape(payload, ('container_id', 'sql', 'result'), 'survivor_query_shape')
    need(payload['container_id'] == cid and payload['sql'] == sql, 'survivor_query_wrong_target')
    return payload['result']


def _configuration(backend, op, subject):
    reference = backend._emergency_current_configuration(op)
    value = read_evidence(op, reference, 'configuration')
    _shape(value, ('configuration_sha256', 'runtime_image_id', 'runtime_revision',
                  'primary_container_id', 'standby_container_id', 'blob_binding_sha256'),
           'survivor_current_configuration_shape')
    need(value == {'configuration_sha256': subject['runtime']['configuration_sha256'],
        'runtime_image_id': subject['runtime']['image_id'], 'runtime_revision': subject['runtime']['revision'],
        'primary_container_id': subject['primary']['container_id'],
        'standby_container_id': subject['standby']['container_id'],
        'blob_binding_sha256': subject['control']['blob_binding_sha256']},
        'survivor_current_configuration_differs')
    return reference


def capture_survivor(backend: SurvivorReadPort, op, fence_reference, replacement_reference):
    """Only seal read-only evidence. This returns NO promotion or Ready fact."""
    need(op.phase == 'verify_latest', 'survivor_capture_phase')
    bound = validate_authority(op); subject = bound['certificate']['subject']
    op.seal()
    validate_fence(op, fence_reference, subject)
    configuration_reference = _configuration(backend, op, subject)
    replacement = read_evidence(op, replacement_reference, 'replacement')
    _shape(replacement, ('qualification_sha256', 'journal_roots', 'physical_media_sha256'),
           'survivor_replacement_proof')
    need(replacement['qualification_sha256'] == bound['authority']['replacement_qualification']['sha256']
         and replacement['physical_media_sha256'] == bound['replacement']['physical_media_sha256'],
         'survivor_replacement_not_qualified')
    roots = replacement['journal_roots']
    need(type(roots) is list and len(roots) == 2
         and roots == op.inventory['journal_roots'] and roots[0]['identity']['dev'] != roots[1]['identity']['dev'],
         'survivor_two_new_journal_media_required')
    _verify_journals(op)
    cid = subject['standby']['container_id']; deadline = time.monotonic() + REPLAY_SECONDS
    target = None
    while True:
        need(time.monotonic() < deadline, 'survivor_fixed_replay_deadline')
        result = _query(op, backend._emergency_survivor_observe(op, container_id=cid, deadline=deadline),
                        'position', POSITION_SQL, cid)
        position = _position(result, subject)
        if target is None:
            # First wait for the old streaming channel to end; no moving target
            # is selected while bytes from its fenced sender can still arrive.
            if position['wal_receiver_count'] != 0:
                time.sleep(.05); continue
            target = copy.deepcopy(position)
            target_reference = save_evidence(op, 'survivor-target', target)
        need(position['wal_receiver_count'] == 0, 'survivor_receiver_reconnected_after_fixed_end')
        need(position['receive_lsn'] == target['receive_lsn'], 'survivor_new_wal_after_fixed_end')
        if manual._lsn(position['replay_lsn']) >= manual._lsn(target['receive_lsn']): break
        time.sleep(.05)
    snapshot_reference = backend._emergency_survivor_snapshot(op, container_id=cid)
    snapshot = _query(op, snapshot_reference, 'snapshot', SNAPSHOT_SQL, cid)
    _shape(snapshot, ('position_before', 'position_after', 'transaction', 'retained_tables',
                     'retained_references', 'provider', 'storage_metadata'), 'survivor_snapshot_shape')
    expected_transaction = {'isolation': 'repeatable read', 'read_only': True,
        'statement_timeout_ms': 30000, 'lock_timeout_ms': 1000,
        'transaction_timeout_ms': 35000, 'idle_in_transaction_session_timeout_ms': 5000}
    need(snapshot['transaction'] == expected_transaction, 'survivor_snapshot_not_repeatable_readonly')
    for key in ('position_before', 'position_after'):
        position = _position(snapshot[key], subject)
        need(position['wal_receiver_count'] == 0, 'survivor_receiver_reconnected_after_fixed_end')
        need(position['receive_lsn'] == target['receive_lsn']
             and manual._lsn(position['replay_lsn']) >= manual._lsn(target['receive_lsn']),
             'survivor_snapshot_wal_not_sealed')
    manual._table_proof(snapshot['retained_tables']); manual._references(snapshot['retained_references'])
    need(snapshot['retained_tables']['public._sqlx_migrations']['sha256']
         == subject['runtime']['migration_table_sha256'], 'survivor_schema_changed')
    storage = snapshot['storage_metadata']
    _shape(storage, ('corpus_control', 'cache_storage', 'cache_totals'), 'survivor_storage_metadata')
    corpus = storage['corpus_control']; cache = storage['cache_storage']; totals = storage['cache_totals']
    need(type(corpus) is dict and type(corpus.get('next_event')) is int and corpus['next_event'] >= 1
         and type(corpus.get('index_ack')) is int and 0 <= corpus['index_ack'] < corpus['next_event']
         and type(cache) is dict and valid_uuid(cache.get('id')), 'survivor_storage_identity')
    _shape(totals, ('snapshots', 'queries', 'referenced_bytes'), 'survivor_cache_totals')
    need(all(type(v) is int and v >= 0 for v in totals.values()), 'survivor_cache_totals')
    provider = snapshot['provider']
    _shape(provider, ('state', 'hold_control', 'lock_provenance'), 'survivor_provider_readonly_shape')
    need(provider['lock_provenance'] == 'primary_advisory_locks_not_observable_on_standby'
         and type(provider['state']) is dict and type(provider['hold_control']) is dict,
         'survivor_provider_no_cleanup_inference')
    state = provider['state']; control = provider['hold_control']
    _shape(state, ('budget', 'control', 'admissions', 'canary_control', 'canaries'),
           'survivor_provider_state_shape')
    need(type(state['budget']) is dict and type(state['control']) is dict
         and type(state['budget'].get('unresolved_response')) is bool
         and (state['budget'].get('admission_owner') is None
              or valid_uuid(state['budget']['admission_owner']))
         and type(state['admissions']) is list and len(state['admissions']) <= 16
         and type(state['canaries']) is list and len(state['canaries']) <= 1
         and valid_uuid(control.get('generation')) and type(control.get('recovery_hold')) is bool
         and state['control'] == {k: v for k, v in control.items()
             if k not in ('generation', 'recovery_hold', 'hold_started_at')},
         'survivor_provider_state_not_preserved')
    closure_reference = backend._emergency_survivor_sql_closed(op, container_id=cid)
    need(_query(op, closure_reference, 'sql-closure', CLOSURE_SQL, cid) == {'sessions': []},
         'survivor_operator_sql_not_closed')
    # Recheck the actual available roots and immutable authority after all long
    # SQL work. No unavailable original is substituted with an empty manifest.
    validate_authority(op); validate_fence(op, fence_reference, subject)
    _configuration(backend, op, subject); _verify_journals(op); op.seal()
    return save_evidence(op, 'survivor-seal', {
        'certificate_sha256': bound['authority']['certificate']['sha256'],
        'fence_reference': fence_reference, 'replacement_reference': replacement_reference,
        'configuration_reference': configuration_reference,
        'fixed_target_reference': target_reference, 'snapshot_reference': snapshot_reference,
        'sql_closure_reference': closure_reference,
        'fixed_receive_lsn': target['receive_lsn'], 'provider_uncertainty_preserved': True,
        'provider_unresolved_response': state['budget']['unresolved_response'],
        'provider_ledger_entries': len(state['admissions']),
        'collection_activation_permitted': False, 'promotion_permitted': False,
        'lookup_ready_claimed': False, 'preservation_and_cold_restore_pending': True})
