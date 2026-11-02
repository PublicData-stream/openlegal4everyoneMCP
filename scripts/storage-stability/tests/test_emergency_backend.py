"""Synthetic authority/port tests plus real private/tmp descriptor checks.

No Docker/SQL/provider/network/promotion/Ready proof. Positive multi-media port
fixtures explicitly mock only that external physical qualification; the actual
same-filesystem journal gate is separately required to refuse.
"""
import copy
import os
from pathlib import Path
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import emergency_backend as e
from interface import Refused
from inventory import canonical, digest


def uid(): return str(uuid.uuid4())


class Journal:
    def __init__(self): self.values = {}
    def save(self, name, value):
        if name in self.values: raise FileExistsError(name)
        raw = canonical(value); self.values[name] = raw; return digest(raw)
    def output_hashes(self): return {k: digest(v) for k, v in self.values.items()}
    def read_phase_bytes(self, phase, name, sha):
        raw = self.values[name]
        if digest(raw) != sha: raise Refused('synthetic_raw_hash_mismatch')
        return raw


@unittest.skipUnless(os.geteuid() == 0, 'real root-private authority/owner fixtures require root')
class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='emergency-contract-', dir='/tmp')
        self.addCleanup(self.tmp.cleanup); self.base = Path(self.tmp.name); self.base.chmod(0o700)
        self.epoch = uid(); self.new_epoch = uid(); self.operation = uid()
        self.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        ticks = Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]
        process = {'pid': os.getpid(), 'start_ticks': ticks, 'boot_id': self.boot}
        absent = {'pid': 2147483647, 'start_ticks': '1', 'boot_id': self.boot}
        self.subject = {'managed_epoch': self.epoch, 'host_boot_id': self.boot,
            'runtime': {'revision': 'a' * 40, 'image_id': 'sha256:' + 'b' * 64,
                'configuration_sha256': 'c' * 64, 'migration_table_sha256': 'd' * 64},
            'control': {'managed_epoch': self.epoch, 'primary_system_identifier': '123', 'timeline': 1,
                'standby_name': 'mcp_peer', 'standby_role': 'mcp_replication', 'standby_addr': '192.0.2.7',
                'standby_client_dn': '/CN=peer', 'standby_issuer_dn': '/CN=ca',
                'standby_client_serial': '123', 'blob_binding_sha256': 'e' * 64},
            'primary': {'container_id': '1' * 64, 'image_id': 'sha256:' + '2' * 64,
                'postmaster_started_at': 'fixed-primary-start', 'data_directory': '/pg/data',
                'data_root': self.root('unavailable-pg', 70, missing=True), 'process': absent},
            'standby': {'container_id': '3' * 64, 'image_id': 'sha256:' + '2' * 64,
                'postmaster_started_at': 'fixed-standby-start', 'data_directory': '/pg/data',
                'data_root': self.root('available-pg', 70), 'process': process},
            'blob_roots': {}, 'original_inventory': {}}
        for role in e.ROLES:
            row = self.root(role, 10004, missing=role in e.observation.HDD_LOST)
            row.update(container_path='/blob/' + role, filesystem_id='fixture:' + role, marker_sha256='f' * 64)
            if role in e.observation.SSD_SURVIVORS:
                marker = {'schema_version': 1, 'kind': 'openlegal_blob_seed', 'managed_epoch': self.epoch,
                    'seed_operation': uid(), 'role': role, 'manifest_sha256': 'a' * 64,
                    'correspondence_sha256': 'b' * 64, 'filesystem_id': row['filesystem_id']}
                raw = canonical(marker); path = Path(row['path']) / '.openlegal-seed-v1.json'
                path.write_bytes(raw); path.chmod(0o640); os.chown(path, 0, 10004)
                row['marker_sha256'] = digest(raw)
            self.subject['blob_roots'][role] = row
        self.subject['control']['blob_binding_sha256'] = e._subject_blob_binding(self.subject)
        self.original = {'original_datasets': [{'name': 'old/d' + str(i), 'guid': str(100 + i)} for i in range(7)],
                         'active_datasets': []}
        self.subject['original_inventory'] = self.write('original.json', self.original)
        self.certificate = {'schema_version': 1, 'kind': 'openlegal_strict_epoch_certificate', 'subject': self.subject}
        self.certificate_ref = self.write('certificate.json', self.certificate)
        head_sha = digest(canonical({'schema_version': 1, 'kind': 'openlegal_strict_epoch_head',
            'managed_epoch': self.epoch, 'certificate_sha256': self.certificate_ref['sha256']}))
        self.pre_ack = {'schema_version': 1, 'kind': 'openlegal_pre_ack_epoch_install',
            'certificate_sha256': self.certificate_ref['sha256'], 'subject_sha256': digest(canonical(self.subject)),
            'status': 'passed', 'actual_exit_code': 0, 'owned_processes_closed': True,
            'head_installs': [{'root_dev': d, 'root_ino': 1, 'head_sha256': head_sha,
                'readback_sha256': head_sha, 'directory_fsynced': True} for d in (111, 222)]}
        self.escrow = {'schema_version': 1, 'kind': 'openlegal_original_media_escrow',
            'original_inventory_sha256': self.subject['original_inventory']['sha256'],
            'datasets': [dict(r, state='unavailable_retained', last_snapshot_guid=None)
                         for r in self.original['original_datasets']],
            'unavailable_blob_roles': list(e.observation.HDD_LOST), 'original_controls_preserved': True}
        self.journals = [{'path': str(self.base / ('journal-' + str(d))), 'identity': {'dev': d, 'ino': 1}}
                         for d in (111, 222)]
        self.replacement = {'schema_version': 1, 'kind': 'openlegal_survivor_replacement_qualification',
            'operation_id': self.operation, 'new_managed_epoch': self.new_epoch, 'journal_roots': self.journals,
            'physical_media_sha256': '9' * 64, 'status': 'passed', 'actual_exit_code': 0,
            'owned_processes_closed': True}
        self.authority = {'schema_version': 1, 'kind': 'openlegal_standby_recovery_authority',
            'operation_id': self.operation, 'profile': 'running_bound_standby_v1',
            'new_managed_epoch': self.new_epoch, 'certificate': self.certificate_ref,
            'pre_ack_install_proof': self.write('pre-ack.json', self.pre_ack),
            'original_media_escrow': self.write('escrow.json', self.escrow),
            'replacement_qualification': self.write('replacement.json', self.replacement)}
        self.authority_ref = self.write('authority.json', self.authority)
        self.op = types.SimpleNamespace(operation_id=self.operation, action='manual_failover', phase='verify_latest',
            inventory_sha256='4' * 64, source_graph_sha256='5' * 64, authority_sha256='6' * 64,
            inventory={'managed_epoch': self.epoch, 'host': {'boot_id': self.boot}, 'journal_roots': self.journals,
                'linux': {'manual_recovery': {'mode': 'emergency_attested', 'new_managed_epoch': self.new_epoch,
                    'emergency_authority': self.authority_ref},
                    'postgres': {side: {k: self.subject[side][k] for k in ('container_id', 'image_id')}
                                 for side in ('primary', 'standby')}, 'workloads': [{'uid': uid()}], 'jobs': []}},
            authority={'standby_survivor_recovery_permitted': True,
                'standby_recovery_authority_sha256': self.authority_ref['sha256'],
                'current_epoch_certificate_sha256': self.certificate_ref['sha256']},
            journal=Journal(), seal=lambda: None)
        self.fence = {'primary_container_id': self.subject['primary']['container_id'],
            'primary_image_id': self.subject['primary']['image_id'], 'primary_state': 'exited',
            'restart_policy': 'no', 'primary_sql': 'unavailable_not_queried',
            'closed_processes': [absent], 'closed_operator_processes': [],
            'closed_cgroups': [{'path': '/sys/fs/cgroup/emergency-fixture-' + '1' * 64,
                               'dev': 1, 'ino': 2, 'state': 'removed', 'container_id': '1' * 64}],
            'closed_cri_containers': [],
            'closed_workload_uids': [self.op.inventory['linux']['workloads'][0]['uid']],
            'operator_autostart_inhibited': True}
        self.fence_ref = e.save_evidence(self.op, 'fence', self.fence)
        self.replacement_ref = e.save_evidence(self.op, 'replacement', {
            'qualification_sha256': self.authority['replacement_qualification']['sha256'],
            'journal_roots': self.journals, 'physical_media_sha256': self.replacement['physical_media_sha256']})
        self.position = {'schema_version': 1, 'kind': 'openlegal_survivor_position', 'in_recovery': True,
            'system_identifier': '123', 'timeline': 1, 'postmaster_started_at': 'fixed-standby-start',
            'data_directory': '/pg/data', 'control': copy.deepcopy(self.subject['control']),
            'receive_lsn': '0/100', 'replay_lsn': '0/100', 'replay_paused': False, 'wal_receiver_count': 0}
        control = {'generation': uid(), 'recovery_hold': False, 'hold_started_at': None, 'foreground_wait_until': 0}
        self.provider = {'state': {'budget': {'unresolved_response': False, 'admission_owner': None,
            'daily_used': 664, 'utc_day': 20757}, 'control': {'foreground_wait_until': 0},
            'admissions': [], 'canary_control': {'required_id': None}, 'canaries': []},
            'hold_control': control, 'lock_provenance': 'primary_advisory_locks_not_observable_on_standby'}
        self.snapshot = {'position_before': copy.deepcopy(self.position), 'position_after': copy.deepcopy(self.position),
            'transaction': {'isolation': 'repeatable read', 'read_only': True, 'statement_timeout_ms': 30000,
                'lock_timeout_ms': 1000, 'transaction_timeout_ms': 35000, 'idle_in_transaction_session_timeout_ms': 5000},
            'retained_tables': {'public._sqlx_migrations': {'rows': 33, 'sha256': 'd' * 64}},
            'retained_references': [], 'provider': self.provider, 'storage_metadata': {
                'corpus_control': {'next_event': 1, 'index_ack': 0}, 'cache_storage': {'id': uid()},
                'cache_totals': {'snapshots': 0, 'queries': 0, 'referenced_bytes': 0}}}

    def write(self, name, value):
        raw = canonical(value); path = self.base / name; path.write_bytes(raw); path.chmod(0o600)
        return {'path': str(path), 'sha256': digest(raw)}
    def root(self, name, owner, *, missing=False):
        path = self.base / name
        if missing: return {'path': str(path), 'dev': 333, 'ino': 444, 'uid': owner, 'gid': owner,
                            'mode': 0o700, 'kernel_filesystem_id': 555}
        path.mkdir(mode=0o700); os.chown(path, owner, owner); s = path.stat()
        return {'path': str(path), 'dev': s.st_dev, 'ino': s.st_ino, 'uid': s.st_uid, 'gid': s.st_gid,
                'mode': 0o700, 'kernel_filesystem_id': os.statvfs(path).f_fsid}
    def backend(self, positions=None, snapshot=None, closure=None, configuration=None):
        calls = []; counter = {'position': 0, 'configuration': 0}; values = list(positions or [self.position])
        cid = self.subject['standby']['container_id']
        def observe(op, **kwargs):
            self.assertEqual(kwargs['container_id'], cid); calls.append('observe')
            counter['position'] += 1; value = values.pop(0) if len(values) > 1 else values[0]
            return e.save_evidence(op, 'position', {'container_id': cid, 'sql': e.POSITION_SQL,
                'result': value}, ordinal=counter['position'])
        def snap(op, **kwargs):
            calls.append('snapshot'); return e.save_evidence(op, 'snapshot', {
                'container_id': cid, 'sql': e.SNAPSHOT_SQL, 'result': snapshot or self.snapshot})
        def closed(op, **kwargs):
            calls.append('closure'); return e.save_evidence(op, 'sql-closure', {
                'container_id': cid, 'sql': e.CLOSURE_SQL, 'result': {'sessions': []} if closure is None else closure})
        def config(op):
            counter['configuration'] += 1; calls.append('configuration')
            value = configuration or {'configuration_sha256': self.subject['runtime']['configuration_sha256'],
                'runtime_image_id': self.subject['runtime']['image_id'], 'runtime_revision': self.subject['runtime']['revision'],
                'primary_container_id': self.subject['primary']['container_id'], 'standby_container_id': cid,
                'blob_binding_sha256': self.subject['control']['blob_binding_sha256']}
            return e.save_evidence(op, 'configuration', value, ordinal=counter['configuration'])
        return types.SimpleNamespace(_emergency_survivor_observe=observe, _emergency_survivor_snapshot=snap,
            _emergency_survivor_sql_closed=closed, _emergency_current_configuration=config, calls=calls)
    def capture(self, backend=None):
        with patch.object(e, '_verify_journals'):
            return e.capture_survivor(backend or self.backend(), self.op, self.fence_ref, self.replacement_ref)

    def test_root_private_authority_accepts_without_reading_failed_original_paths(self):
        value = e.validate_authority(self.op)
        self.assertFalse(Path(self.subject['primary']['data_root']['path']).exists())
        self.assertFalse(Path(self.subject['blob_roots']['cache_primary']['path']).exists())
        with self.assertRaises(TypeError): value['certificate']['subject']['managed_epoch'] = uid()
    def test_missing_or_false_permission_refuses(self):
        for value in (None, False, 1, 'true'):
            self.op.authority['standby_survivor_recovery_permitted'] = value
            with self.subTest(value=value), self.assertRaises(Refused): e.validate_authority(self.op)
    def test_private_raw_tamper_wrong_hash_or_world_readable_refuse(self):
        path = Path(self.authority_ref['path']); original = path.read_bytes()
        path.write_bytes(original + b' ')
        with self.assertRaises(Refused): e.validate_authority(self.op)
        path.write_bytes(original); path.chmod(0o644)
        with self.assertRaises(Refused): e.validate_authority(self.op)
    def test_old_certificate_epoch_or_replaced_available_root_refuse(self):
        self.op.inventory['managed_epoch'] = uid()
        with self.assertRaises(Refused): e.validate_authority(self.op)
        self.op.inventory['managed_epoch'] = self.epoch
        root = Path(self.subject['blob_roots']['cache_replica']['path'])
        root.rename(root.with_name('old-cache')); root.mkdir(mode=0o700); os.chown(root, 10004, 10004)
        with self.assertRaises(Refused): e.validate_authority(self.op)
    def repin_authority_role(self, role, value):
        self.authority[role] = self.write(role + '-renewed.json', value)
        self.authority_ref = self.write('authority-renewed.json', self.authority)
        self.op.inventory['linux']['manual_recovery']['emergency_authority'] = self.authority_ref
        self.op.authority['standby_recovery_authority_sha256'] = self.authority_ref['sha256']
    def test_pre_ack_missing_exit_fsync_or_second_head_refuses(self):
        for key in ('exit', 'fsync', 'head'):
            value = copy.deepcopy(self.pre_ack)
            if key == 'exit': value['actual_exit_code'] = None
            if key == 'fsync': value['head_installs'][1]['directory_fsynced'] = False
            if key == 'head': value['head_installs'].pop()
            self.repin_authority_role('pre_ack_install_proof', value)
            with self.subTest(key=key), self.assertRaises(Refused): e.validate_authority(self.op)
    def test_replacement_wrong_epoch_failure_or_single_journal_refuses(self):
        for key in ('epoch', 'exit', 'journal'):
            value = copy.deepcopy(self.replacement)
            if key == 'epoch': value['new_managed_epoch'] = uid()
            if key == 'exit': value['actual_exit_code'] = 1
            if key == 'journal': value['journal_roots'].pop()
            self.repin_authority_role('replacement_qualification', value)
            with self.subTest(key=key), self.assertRaises(Refused): e.validate_authority(self.op)
    def test_unavailable_original_escrow_cannot_drop_dataset(self):
        value = copy.deepcopy(self.escrow); value['datasets'].pop()
        self.repin_authority_role('original_media_escrow', value)
        with self.assertRaisesRegex(Refused, 'escrow_dataset_coverage'): e.validate_authority(self.op)
    def test_available_marker_replacement_refuses(self):
        path = Path(self.subject['blob_roots']['corpus_primary']['path']) / '.openlegal-seed-v1.json'
        path.write_bytes(b'{}\n')
        with self.assertRaises(Refused): e.validate_authority(self.op)
    def test_fence_requires_actual_primary_process_and_complete_workload_set(self):
        e.validate_fence(self.op, self.fence_ref, self.subject)
        for field, value in (('closed_processes', []), ('closed_workload_uids', []),
                              ('primary_sql', 'healthy'), ('restart_policy', 'always')):
            changed = copy.deepcopy(self.fence); changed[field] = value
            reference = self.changed_evidence('fence', changed)
            with self.subTest(field=field), self.assertRaises(Refused): e.validate_fence(self.op, reference, self.subject)
    def changed_evidence(self, kind, payload):
        value = {'schema_version': 1, 'kind': 'openlegal_emergency_' + kind,
            'operation_id': self.operation, 'phase': self.op.phase, 'inventory_sha256': self.op.inventory_sha256,
            'source_graph_sha256': self.op.source_graph_sha256, 'authority_sha256': self.op.authority_sha256,
            'observed_monotonic': time.monotonic(), 'payload': payload}
        name = self.op.phase + '-negative-' + uid() + '.json'; sha = self.op.journal.save(name, value)
        return {'phase': self.op.phase, 'artifact': name, 'sha256': sha}
    def test_cri_container_requires_matching_closed_cgroup(self):
        changed = copy.deepcopy(self.fence)
        changed['closed_cri_containers'] = [{'id': '9' * 64, 'state': 'CONTAINER_EXITED',
            'owner_uid': self.op.inventory['linux']['workloads'][0]['uid']}]
        with self.assertRaisesRegex(Refused, 'cgroup_coverage'):
            e.validate_fence(self.op, self.changed_evidence('fence', changed), self.subject)
        changed['closed_cgroups'].append({'path': '/sys/fs/cgroup/emergency-fixture-' + '9' * 64,
            'dev': 1, 'ino': 3, 'state': 'removed', 'container_id': '9' * 64})
        e.validate_fence(self.op, self.changed_evidence('fence', changed), self.subject)
        changed['closed_cri_containers'][0]['state'] = 'CONTAINER_RUNNING'
        with self.assertRaisesRegex(Refused, 'cri_not_closed'):
            e.validate_fence(self.op, self.changed_evidence('fence', changed), self.subject)
    def test_alive_primary_process_cannot_be_declared_closed(self):
        changed = copy.deepcopy(self.fence); changed['closed_processes'] = [self.subject['standby']['process']]
        with self.assertRaises(Refused): e.validate_fence(self.op, self.changed_evidence('fence', changed), self.subject)
    def test_synthetic_capture_seals_only_readonly_evidence_never_ready(self):
        backend = self.backend(); reference = self.capture(backend)
        result = e.read_evidence(self.op, reference, 'survivor-seal')
        self.assertFalse(result['promotion_permitted']); self.assertFalse(result['lookup_ready_claimed'])
        self.assertFalse(result['collection_activation_permitted'])
        self.assertTrue(result['preservation_and_cold_restore_pending'])
        self.assertEqual(backend.calls.count('configuration'), 2)
    def test_replay_polls_one_original_target_not_moving_prefix(self):
        lag = dict(self.position, replay_lsn='0/80')
        reference = self.capture(self.backend([lag, self.position]))
        self.assertEqual(e.read_evidence(self.op, reference, 'survivor-seal')['fixed_receive_lsn'], '0/100')
        self.assertEqual(len([n for n in self.op.journal.values if 'survivor-target' in n]), 1)
    def test_receiver_end_is_not_captured_until_old_channel_absent(self):
        connected = dict(self.position, wal_receiver_count=1, receive_lsn='0/80', replay_lsn='0/80')
        reference = self.capture(self.backend([connected, self.position]))
        self.assertEqual(e.read_evidence(self.op, reference, 'survivor-seal')['fixed_receive_lsn'], '0/100')
    def test_receiver_reconnect_after_fixed_target_refuses_even_without_new_wal(self):
        lag = dict(self.position, replay_lsn='0/80')
        reconnected = dict(self.position, wal_receiver_count=1)
        backend = self.backend([lag, reconnected])
        with self.assertRaisesRegex(Refused, 'receiver_reconnected_after_fixed_end'):
            self.capture(backend)
        self.assertEqual(len([n for n in self.op.journal.values if 'survivor-target' in n]), 1)
        self.assertNotIn('snapshot', backend.calls)
        self.assertFalse(any('survivor-seal' in n for n in self.op.journal.values))
    def test_snapshot_receiver_reconnect_refuses_before_or_after_without_new_wal(self):
        for position_key in ('position_before', 'position_after'):
            self.op.journal = Journal(); self.fence_ref = e.save_evidence(self.op, 'fence', self.fence)
            self.replacement_ref = e.save_evidence(self.op, 'replacement', {
                'qualification_sha256': self.authority['replacement_qualification']['sha256'],
                'journal_roots': self.journals, 'physical_media_sha256': '9' * 64})
            snapshot = copy.deepcopy(self.snapshot)
            snapshot[position_key]['wal_receiver_count'] = 1
            backend = self.backend(snapshot=snapshot)
            with self.subTest(position=position_key):
                with self.assertRaisesRegex(Refused, 'receiver_reconnected_after_fixed_end'):
                    self.capture(backend)
                self.assertEqual(len([n for n in self.op.journal.values if 'survivor-target' in n]), 1)
                self.assertNotIn('closure', backend.calls)
                self.assertFalse(any('survivor-seal' in n for n in self.op.journal.values))
    def test_receive_advance_or_postmaster_restart_refuses_no_recapture(self):
        lag = dict(self.position, replay_lsn='0/80')
        changed = dict(self.position, receive_lsn='0/200', replay_lsn='0/200')
        with self.assertRaisesRegex(Refused, 'new_wal_after_fixed_end'): self.capture(self.backend([lag, changed]))
        self.assertEqual(len([n for n in self.op.journal.values if 'survivor-target' in n]), 1)
    def test_missing_wal_lsn_paused_replay_or_new_lifetime_refuse(self):
        for key, value in (('receive_lsn', None), ('replay_paused', True),
                            ('postmaster_started_at', 'new-start'), ('in_recovery', False)):
            with self.subTest(key=key), self.assertRaises(Refused):
                e._position(dict(self.position, **{key: value}), self.subject)
    def test_snapshot_read_committed_readwrite_or_wal_change_refuse(self):
        for change in ('isolation', 'read_only', 'wal'):
            self.op.journal = Journal(); self.fence_ref = e.save_evidence(self.op, 'fence', self.fence)
            self.replacement_ref = e.save_evidence(self.op, 'replacement', {'qualification_sha256':
                self.authority['replacement_qualification']['sha256'], 'journal_roots': self.journals,
                'physical_media_sha256': '9' * 64})
            snapshot = copy.deepcopy(self.snapshot)
            if change == 'isolation': snapshot['transaction']['isolation'] = 'read committed'
            if change == 'read_only': snapshot['transaction']['read_only'] = False
            if change == 'wal': snapshot['position_after']['receive_lsn'] = '0/200'
            with self.subTest(change=change), self.assertRaises(Refused): self.capture(self.backend(snapshot=snapshot))
    def test_uncertain_provider_charge_and_ledger_are_preserved_never_cleaned(self):
        self.provider['state']['budget'].update(unresolved_response=True, admission_owner=uid())
        self.provider['state']['admissions'] = [{'owner': uid(), 'slot': 1}]
        before = copy.deepcopy(self.provider)
        reference = self.capture()
        value = e.read_evidence(self.op, reference, 'survivor-seal')
        self.assertTrue(value['provider_unresolved_response']); self.assertEqual(value['provider_ledger_entries'], 1)
        self.assertEqual(self.provider, before); self.assertEqual(self.provider['state']['budget']['daily_used'], 664)
    def test_storage_identity_or_migration_manifest_placeholder_refuses(self):
        for key in ('storage', 'schema'):
            self.op.journal = Journal(); self.fence_ref = e.save_evidence(self.op, 'fence', self.fence)
            self.replacement_ref = e.save_evidence(self.op, 'replacement', {'qualification_sha256':
                self.authority['replacement_qualification']['sha256'], 'journal_roots': self.journals,
                'physical_media_sha256': '9' * 64})
            value = copy.deepcopy(self.snapshot)
            if key == 'storage': value['storage_metadata'] = {}
            if key == 'schema': value['retained_tables']['public._sqlx_migrations']['sha256'] = 'a' * 64
            with self.subTest(key=key), self.assertRaises(Refused): self.capture(self.backend(snapshot=value))
    def test_operator_sql_session_refuses(self):
        with self.assertRaisesRegex(Refused, 'operator_sql_not_closed'):
            self.capture(self.backend(closure={'sessions': [{'pid': 1}]}))
    def test_wrong_current_configuration_refuses(self):
        bad = {'configuration_sha256': 'f' * 64, 'runtime_image_id': self.subject['runtime']['image_id'],
            'runtime_revision': self.subject['runtime']['revision'], 'primary_container_id': '1' * 64,
            'standby_container_id': '3' * 64, 'blob_binding_sha256': self.subject['control']['blob_binding_sha256']}
        with self.assertRaisesRegex(Refused, 'current_configuration_differs'): self.capture(self.backend(configuration=bad))
    def test_actual_single_filesystem_journals_refuse(self):
        roots = []
        for name in ('ja', 'jb'):
            path = self.base / name; path.mkdir(mode=0o700); s = path.stat()
            roots.append({'path': str(path), 'identity': {'dev': s.st_dev, 'ino': s.st_ino}})
        self.op.inventory['journal_roots'] = roots
        with self.assertRaisesRegex(Refused, 'same_filesystem'): e._verify_journals(self.op)
    def test_query_wrong_target_stale_or_other_operation_refuse(self):
        reference = self.changed_evidence('position', {'container_id': '1' * 64, 'sql': e.POSITION_SQL,
                                                     'result': self.position})
        with self.assertRaisesRegex(Refused, 'wrong_target'):
            e._query(self.op, reference, 'position', e.POSITION_SQL, '3' * 64)
        raw = self.op.journal.values[reference['artifact']]
        value = e.manual._metadata_decode(raw); value['observed_monotonic'] -= 121
        altered = canonical(value); self.op.journal.values[reference['artifact']] = altered
        reference['sha256'] = digest(altered)
        with self.assertRaisesRegex(Refused, 'stale'): e.read_evidence(self.op, reference, 'position')
    def test_fixed_target_reuse_cannot_emit_another_seal(self):
        self.capture()
        with self.assertRaises(FileExistsError): self.capture()
        self.assertEqual(len([n for n in self.op.journal.values if 'survivor-seal' in n]), 1)
    def test_missing_fixed_native_port_cannot_emit_seal(self):
        with self.assertRaises(AttributeError): self.capture(types.SimpleNamespace())
        self.assertFalse(any('survivor-seal' in name for name in self.op.journal.values))


class SqlSourceTests(unittest.TestCase):
    def test_new_sql_is_unregistered_and_readonly_with_public_revoked(self):
        source = Path(__file__).resolve().parents[3] / 'crates/adapters/migrations/0033_survivor_observers.sql'
        text = source.read_text()
        self.assertEqual(text.count('CREATE FUNCTION '), 3)
        self.assertEqual(text.count(' FROM PUBLIC;'), 3)
        for fragment in ('FOR SHARE', 'FOR UPDATE', 'pg_promote(', 'pg_wal_replay_resume(',
                         'CREATE ROLE', 'INSERT INTO', 'UPDATE openlegal', 'DELETE FROM'):
            self.assertNotIn(fragment, text)
        self.assertIn("current_setting('transaction_isolation')<>'repeatable read'", text)
        self.assertIn("'primary_advisory_locks_not_observable_on_standby'", text)


if __name__ == '__main__': unittest.main()
