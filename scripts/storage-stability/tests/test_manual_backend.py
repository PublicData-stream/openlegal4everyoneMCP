"""Offline manual-recovery protocol tests: no Docker, SQL, ZFS or network.

Fake journal/command responses are explicitly synthetic; they do not prove a
real promotion, two physical media, provider recovery or native readiness.
"""
import copy
import hashlib
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
import manual_backend as m
from interface import Refused
from inventory import canonical, decode, digest


def uid():
    return str(uuid.uuid4())


class Journal:
    def __init__(self): self.values = {}
    def save(self, name, value):
        if name in self.values: raise FileExistsError(name)
        raw = canonical(value); self.values[name] = raw; return digest(raw)
    def output_hashes(self): return {k: digest(v) for k, v in self.values.items()}
    def read_phase_bytes(self, phase, name, pin):
        raw = self.values[name]
        if digest(raw) != pin: raise Refused('synthetic_journal_hash_mismatch')
        return raw


class FakeBackend:
    def __init__(self): self.calls = []
    def _artifact(self, op, name, value): op.journal.save(op.phase + '-' + name + '.json', value)
    def _receipt(self, op, facts): return facts


def policy(root):
    reference = lambda name: {'path': str(root / name), 'sha256': 'a' * 64}
    return {'schema_version': 1, 'mode': 'planned_drill', 'new_managed_epoch': uid(),
        'old_primary_dataset': 'tank/mcp-pg',
        'reseed_target': {'dataset': 'tank/new-mcp-pg', 'guid': '456', 'path': str(root / 'new-pg'),
            'identity': {'dev': 1, 'ino': 2}, 'uid': 70, 'gid': 70},
        'replication_peer': {'host': '192.0.2.7', 'port': 5432, 'role': 'mcp_replica',
            'application_name': 'mcp_peer', 'client_serial': '123',
            'client_dn': '/CN=mcp_replica', 'issuer_dn': '/CN=fixture-ca',
            'ca': reference('ca.crt'), 'certificate': reference('replica.crt'),
            'private_key': reference('replica.key')},
        'target_configuration': reference('prepared.json'), 'emergency_authority': None}


def inventory(root, config):
    return {'managed_epoch': uid(), 'original_datasets': [
        {'name': 'tank/mcp-pg', 'guid': '123', 'mountpoint': str(root / 'old-pg')}],
        'ssd_trees': [{'path': str(root / 'ssd')}], 'journal_roots': [
            {'path': str(root / 'journal-a')}, {'path': str(root / 'journal-b')}],
        'backup_root': {'path': str(root / 'backup')},
        'host': {'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()},
        'linux': {'postgres': {'uid': 70}, 'manual_recovery': config, 'workloads': [], 'jobs': []}}


def operation(inv, phase='fence_old'):
    return types.SimpleNamespace(operation_id=uid(), phase=phase, action='manual_failover',
        inventory=inv, inventory_sha256='1' * 64, source_graph_sha256='2' * 64,
        authority_sha256='3' * 64, authority={}, journal=Journal(), previous={}, seal=lambda: None)


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='manual-policy-', dir='/tmp')
        self.addCleanup(self.tmp.cleanup); self.root = Path(self.tmp.name)
        self.config = policy(self.root); self.inventory = inventory(self.root, self.config)
    def test_closed_policy_accepts_only_prepared_metadata(self):
        self.assertIs(m.validate(self.config, self.inventory), self.config)
        for key in ('command', 'sql', 'shell', 'create_credentials'):
            changed = copy.deepcopy(self.config); changed[key] = 'forbidden'
            with self.subTest(key=key), self.assertRaises(Refused): m.validate(changed, self.inventory)
    def test_old_or_nested_dataset_cannot_be_reseed_target(self):
        for changes in ({'dataset': 'tank/mcp-pg'}, {'path': str(self.root / 'old-pg/new')},
                        {'path': str(self.root / 'ssd/new')}, {'path': str(self.root / 'journal-a/new')}):
            changed = copy.deepcopy(self.config); changed['reseed_target'].update(changes)
            with self.subTest(changes=changes), self.assertRaises(Refused): m.validate(changed, self.inventory)
    def test_epoch_and_identity_are_not_implicitly_reused(self):
        changed = copy.deepcopy(self.config); changed['new_managed_epoch'] = self.inventory['managed_epoch']
        with self.assertRaises(Refused): m.validate(changed, self.inventory)
        changed = copy.deepcopy(self.config); changed['reseed_target']['uid'] = 0
        with self.assertRaises(Refused): m.validate(changed, self.inventory)
    def test_replication_requires_numeric_endpoint_pinned_tls_identity(self):
        for changes in ({'host': 'https://example.invalid'}, {'host': '0.0.0.0'},
                        {'port': True}, {'role': 'x;SELECT'}, {'client_serial': '0'}, {'issuer_dn': ''}):
            changed = copy.deepcopy(self.config); changed['replication_peer'].update(changes)
            with self.subTest(changes=changes), self.assertRaises(Refused): m.validate(changed, self.inventory)
    def test_replication_serial_accepts_canonical_one_to_sixty_digits_without_coercion(self):
        for digits in (1, 50, 51, 60):
            serial = '1' + '0' * (digits - 1)
            changed = copy.deepcopy(self.config); changed['replication_peer']['client_serial'] = serial
            with self.subTest(digits=digits):
                self.assertIs(m.validate(changed, self.inventory), changed)
                self.assertIs(changed['replication_peer']['client_serial'], serial)

    def test_replication_serial_rejects_out_of_bound_or_noncanonical_values(self):
        for serial in ('1' * 61, '0', '01', '', 123, None, True, False,
                       '+1', '-1', ' 1', '1 ', '1\n', '1.0', '1e2', '1E+2'):
            changed = copy.deepcopy(self.config); changed['replication_peer']['client_serial'] = serial
            with self.subTest(serial=serial), self.assertRaisesRegex(Refused, 'manual_peer_identity'):
                m.validate(changed, self.inventory)

    def test_credential_paths_cannot_be_commands_or_parent_paths(self):
        for path in ('../../secret', '/tmp/../etc/key', '/', '/tmp/key\x00'):
            changed = copy.deepcopy(self.config); changed['replication_peer']['private_key']['path'] = path
            with self.subTest(path=path), self.assertRaises(Refused): m.validate(changed, self.inventory)
    def test_emergency_does_not_manufacture_a_source_baseline(self):
        config = copy.deepcopy(self.config); config['mode'] = 'emergency_attested'
        config['emergency_authority'] = {'path': str(self.root / 'attestation'), 'sha256': 'f' * 64}
        inv = inventory(self.root, config); op = operation(inv)
        with patch.object(m.os, 'geteuid', return_value=0), self.assertRaisesRegex(
                Refused, 'manual_emergency_latest_ack_proof_required'): m._config(op)
    def test_foreign_action_refuses_before_helper(self):
        op = operation(self.inventory); op.action = 'checkpoint'
        with self.assertRaisesRegex(Refused, 'manual_operation_scope'): m.fence_old(FakeBackend(), op)


class EnvelopeTests(unittest.TestCase):
    def setUp(self):
        self.op = operation({'linux': {}}); self.backend = FakeBackend()
        self.data = {k: None for k in m.MANUAL_HELPER_CONTRACT['captured'].split(',')}
    def test_helper_output_must_be_acknowledged_hash_bound_journal_bytes(self):
        ref = m.save_evidence(self.backend, self.op, 'captured', self.data)
        self.assertEqual(m._evidence(self.op, ref, 'captured'), self.data)
        self.op.journal.values[ref['artifact']] = b'{}\n'
        with self.assertRaises(Refused): m._evidence(self.op, ref, 'captured')
    def test_wrong_operation_phase_source_or_kind_is_rejected(self):
        ref = m.save_evidence(self.backend, self.op, 'captured', self.data)
        raw = self.op.journal.values[ref['artifact']]
        for key, value in [('operation_id', uid()), ('phase', 'strict_resume'),
                           ('source_graph_sha256', '4' * 64), ('kind', 'openlegal_manual_started')]:
            changed = decode(raw); changed[key] = value; data = canonical(changed)
            self.op.journal.values[ref['artifact']] = data
            with self.subTest(key=key), self.assertRaises(Refused):
                m._evidence(self.op, dict(ref, sha256=digest(data)), 'captured')
    def test_untyped_boolean_or_failed_prior_exit_cannot_advance(self):
        receipt = {'phase': 'promote', 'operation_id': self.op.operation_id, 'status': 'passed',
            'actual_exit_code': 0, 'owned_processes_closed': True,
            'inventory_sha256': self.op.inventory_sha256,
            'source_graph_sha256': self.op.source_graph_sha256,
            'authority_sha256': self.op.authority_sha256, 'facts': {}}
        for value in (True, 1, -1, None):
            self.op.previous['promote'] = dict(receipt, actual_exit_code=value)
            with self.subTest(value=value), self.assertRaises(Refused): m._previous(self.op, 'promote')
        self.op.previous['promote'] = receipt
        self.assertEqual(m._previous(self.op, 'promote'), {})
    def test_phase_minimums_do_not_accept_a_single_copy_epoch_claim(self):
        self.op.phase = 'reseed'
        with self.assertRaises(Refused): m._emit(self.backend, self.op, {
            'second_copy_durably_seeded': True, 'both_pg_identities_verified': True,
            'both_blob_generations_verified': True, 'new_epoch_bound': False})
    def test_repeated_fresh_observations_keep_distinct_exclusive_files(self):
        m._record(self.backend, self.op, 'fresh', {'sequence': 1})
        m._record(self.backend, self.op, 'fresh', {'sequence': 2})
        self.assertEqual(len(self.op.journal.values), 2)
    def test_bulk_reference_metadata_has_bounded_distinct_parser(self):
        row = {'role': 'corpus', 'storage_key': 'ab/' + 'a' * 64 + '-' + uid(),
               'sha256': 'a' * 64, 'size': 12}
        raw = canonical({'references': [row] * 20000})
        self.assertGreater(len(raw), 4 * 1024**2)
        self.assertEqual(len(m._metadata_decode(raw)['references']), 20000)
        for bad in (b'{"x":1,"x":2}', b'{"x":1e999}', b'{"x":NaN}', b'[' * 34 + b'0' + b']' * 34):
            with self.subTest(bad=bad[:32]), self.assertRaises(Refused): m._metadata_decode(bad)


class ObserverTests(unittest.TestCase):
    @staticmethod
    def frame():
        expected = {'managed_epoch': uid(), 'blob_binding_sha256': 'a' * 64,
            'primary_system_identifier': '123', 'timeline': 1,
            'postmaster_started_at': 'fixed-primary-start', 'standby_name': 'peer',
            'standby_role': 'peer_role', 'standby_client_serial': '5',
            'standby_client_dn': '/CN=peer', 'standby_issuer_dn': '/CN=ca'}
        row = dict(expected, standby_addr='192.0.2.7/32', sender_pid=123,
                   sender_started_at='fixed-sender-start', prefix_lsn='1/10', flush_lsn='1/5')
        return expected, row
    def test_host_inet_masks_are_exact_addresses_not_network_permissions(self):
        for observed in ('192.0.2.7', '192.0.2.7/32'): m._peer_address(observed, '192.0.2.7')
        m._peer_address('2001:db8::1/128', '2001:db8::1')
        for observed in ('192.0.2.7/24', '192.0.2.8', 'host.invalid', None):
            with self.subTest(observed=observed), self.assertRaises(Refused):
                m._peer_address(observed, '192.0.2.7')
    def test_incomplete_or_boolean_sender_is_not_an_observer_receipt(self):
        expected = {'managed_epoch': uid(), 'blob_binding_sha256': 'a' * 64,
            'primary_system_identifier': '123', 'timeline': 1,
            'postmaster_started_at': 'fixed-primary-start', 'standby_name': 'peer',
            'standby_role': 'peer_role', 'standby_client_serial': '5',
            'standby_client_dn': '/CN=peer', 'standby_issuer_dn': '/CN=ca'}
        row = dict(expected, standby_addr='192.0.2.7/32', sender_pid=123,
                   sender_started_at='fixed-sender-start', prefix_lsn='1/5', flush_lsn='1/5')
        self.assertIs(m._observer(row, expected, '192.0.2.7'), row)
        for key, value in [('sender_pid', True), ('timeline', True), ('flush_lsn', 'bad'),
                           ('standby_issuer_dn', '/CN=other'), ('standby_client_serial', '6')]:
            changed = dict(row, **{key: value})
            with self.subTest(key=key), self.assertRaises(Refused): m._observer(changed, expected, '192.0.2.7')
        changed = dict(row); del changed['sender_started_at']
        with self.assertRaises(Refused): m._observer(changed, expected, '192.0.2.7')
    def test_readonly_lag_poll_keeps_original_prefix_and_sender(self):
        expected, row = self.frame(); calls = []; op = operation({}); backend = FakeBackend()
        def sql(op, query, **kwargs):
            calls.append(query)
            if 'capture_prefix' in query: return copy.deepcopy(row)
            return dict(row, prefix_lsn='1/20', flush_lsn='1/8' if len(calls) == 2 else '1/10')
        backend._sql = sql
        before, after = m._fresh_barrier(backend, op, 'a' * 64, expected, '192.0.2.7')
        self.assertEqual(before['prefix_lsn'], '1/10'); self.assertEqual(after['flush_lsn'], '1/10')
        self.assertEqual(sum('capture_prefix' in query for query in calls), 1)
        self.assertEqual(len(calls), 3)
    def test_changed_peer_during_lag_is_refused_without_retry(self):
        expected, row = self.frame(); calls = []; backend = FakeBackend()
        def sql(op, query, **kwargs):
            calls.append(query)
            return copy.deepcopy(row) if len(calls) == 1 else dict(row, sender_pid=999)
        backend._sql = sql
        with self.assertRaisesRegex(Refused, 'manual_pair_changed_during_ack'):
            m._fresh_barrier(backend, operation({}), 'a' * 64, expected, '192.0.2.7')
        self.assertEqual(len(calls), 2)
    def test_poll_budget_does_not_rebase_prefix_or_raise_timeout(self):
        expected, row = self.frame(); calls = []; backend = FakeBackend()
        def sql(op, query, **kwargs): calls.append((query, kwargs['timeout'])); return copy.deepcopy(row)
        backend._sql = sql
        with patch.object(m.time, 'monotonic', side_effect=[0, 0.1, 0.2, 0.3, 2.1]), \
             patch.object(m.time, 'sleep'), self.assertRaisesRegex(Refused, 'manual_pair_ack_deadline'):
            m._fresh_barrier(backend, operation({}), 'a' * 64, expected, '192.0.2.7')
        self.assertEqual([timeout for _, timeout in calls], [2, 1])


class PairProjectionTests(unittest.TestCase):
    def setUp(self):
        self.old = {'openlegal.corpus_capture': {'rows': 1, 'sha256': 'a' * 64},
                    'openlegal.cache_storage': {'rows': 1, 'sha256': 'b' * 64}}
        epoch = uid()
        self.op = operation({'linux': {'manual_recovery': {'new_managed_epoch': epoch}}})
        point = {'system_identifier': '123', 'timeline': 2, 'postmaster_started_at': 'start',
                 'in_recovery': False, 'wal_lsn': '1/5', 'index_ack': 1, 'watermark': 1}
        standby = {'system_identifier': '123', 'timeline': 2, 'postmaster_started_at': 'peer-start',
                   'in_recovery': True, 'receive_lsn': '1/5', 'replay_lsn': '1/5'}
        self.data = {'new_epoch': epoch, 'seed_manifest_sha256': 'c' * 64,
            'primary': {'container_id': 'd' * 64, 'checkpoint': point,
                        'retained_tables': copy.deepcopy(self.old), 'retained_references': []},
            'standby': {'container_id': 'e' * 64, 'checkpoint': standby,
                        'retained_tables': copy.deepcopy(self.old), 'retained_references': []}}
        self.promoted = {'sealed_source': {'retained_tables': self.old, 'retained_references': []}}
    def test_immutable_history_hash_change_refuses_before_container_or_sql(self):
        self.data['primary']['retained_tables']['openlegal.corpus_capture']['sha256'] = 'f' * 64
        backend = FakeBackend()
        with self.assertRaisesRegex(Refused, 'manual_pair_retained_state_differs'):
            m._pair(backend, self.op, self.data, self.promoted, {})
        self.assertEqual(backend.calls, [])
    def test_poststart_operational_projection_cannot_be_old_hashes(self):
        self.data.update(pods=[], current_immutable_tables={'primary': {}, 'standby': {}},
                         operational_metadata={'primary': {}, 'standby': {}})
        with self.assertRaisesRegex(Refused, 'manual_poststart_projection_not_current'):
            m._pair(FakeBackend(), self.op, self.data, self.promoted, {})
    def test_missing_table_is_not_whitelisted(self):
        del self.data['primary']['retained_tables']['openlegal.corpus_capture']
        with self.assertRaisesRegex(Refused, 'manual_pair_retained_state_differs'):
            m._pair(FakeBackend(), self.op, self.data, self.promoted, {})
    def test_cache_history_loss_cannot_hide_in_both_current_projection_maps(self):
        for table in ('cache_snapshot', 'cache_query', 'cache_head', 'blob_object'):
            baseline = dict(self.old, **{'openlegal.' + table: {'rows': 2, 'sha256': 'a' * 64}})
            self.promoted['sealed_source']['retained_tables'] = baseline
            data = copy.deepcopy(self.data)
            for side in ('primary', 'standby'):
                data[side]['retained_tables'] = copy.deepcopy(baseline)
                data[side]['retained_tables']['openlegal.' + table] = {'rows': 1, 'sha256': 'f' * 64}
            data['pods'] = []
            changed = m.POST_START_OPERATIONAL_TABLES | {'openlegal_replication.control'}
            data['current_immutable_tables'] = {side: {k: v for k, v in data[side]['retained_tables'].items()
                if k not in changed} for side in ('primary', 'standby')}
            data['operational_metadata'] = {side: {k: v for k, v in data[side]['retained_tables'].items()
                if k in changed} for side in ('primary', 'standby')}
            with self.subTest(table=table), self.assertRaisesRegex(Refused, 'manual_pair_retained_state_differs'):
                m._pair(FakeBackend(), self.op, data, self.promoted, {})


class ControlTransitionTests(unittest.TestCase):
    def test_only_monotonic_bounded_index_ack_and_stage_cleanup_can_change(self):
        old = {'id': uid(), 'singleton': True, 'next_event': 6, 'index_ack': 3,
               'staged_bytes': 12, 'max_raw_bytes': 100, 'raw_bytes': 50, 'historical_bytes': 25}
        m._corpus_control_transition(old, dict(old, index_ack=5, staged_bytes=0), 5)
        for key, value in [('id', uid()), ('next_event', 1), ('max_raw_bytes', 999),
                           ('raw_bytes', 0), ('historical_bytes', 0), ('index_ack', 2),
                           ('index_ack', 6), ('staged_bytes', 13)]:
            with self.subTest(key=key, value=value), self.assertRaises(Refused):
                m._corpus_control_transition(old, dict(old, **{key: value}), 5)


class ClosureTests(unittest.TestCase):
    def test_desired_zero_does_not_substitute_for_physical_closure(self):
        inv = {'linux': {'workloads': [], 'jobs': []}}
        closure = {'workloads': [], 'jobs': [], 'cri': [], 'cgroups': [],
                   'processes': [], 'sql_sessions': [], 'runtime_guards': []}
        with self.assertRaisesRegex(Refused, 'manual_cgroup_bound'): m._closure(closure, inv)
        closure['cgroups'] = [{'groups': ['0::/synthetic'], 'pids': []}]
        with patch('linux_backend.members', return_value=[1234]), self.assertRaisesRegex(
                Refused, 'manual_cgroup_repopulated'): m._closure(closure, inv)
    def test_unknown_runtime_guard_and_unfinished_job_refuse(self):
        job = {'name': 'collect', 'uid': uid()}
        inv = {'linux': {'workloads': [], 'jobs': [job]}}
        closure = {'workloads': [], 'jobs': [dict(job, finished=False, suspended=False)],
            'cri': [], 'cgroups': [{'groups': ['0::/synthetic'], 'pids': []}],
            'processes': [], 'sql_sessions': [], 'runtime_guards': []}
        with self.assertRaises(Refused): m._closure(closure, inv)
        closure['jobs'][0]['suspended'] = True
        closure['runtime_guards'] = [{'name': 'serve.json', 'sha256': 'a' * 64,
                                     'state': 'active', 'locked': False}]
        with patch('linux_backend.members', return_value=[]), self.assertRaisesRegex(
                Refused, 'manual_runtime_guard_uncertain'): m._closure(closure, inv)


class ReferenceFileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='manual-reference-', dir='/tmp')
        self.addCleanup(self.tmp.cleanup); self.root = Path(self.tmp.name)
        self.root.chmod(0o700); self.raw = b'fictional retained bytes'
        sha = hashlib.sha256(self.raw).hexdigest(); self.shard = self.root / sha[:2]
        self.shard.mkdir(mode=0o700); self.name = sha + '-11111111-1111-4111-8111-111111111111'
        self.file = self.shard / self.name; self.file.write_bytes(self.raw)
        self.file.chmod(0o600)
        self.rows = [{'role': 'corpus', 'storage_key': sha[:2] + '/' + self.name,
                      'sha256': sha, 'size': len(self.raw)}]
    def verify(self):
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try: m._reference_fd(fd, self.rows, time.monotonic() + 3)
        finally: os.close(fd)
    def test_real_tiny_generation_and_corruption_without_any_backend(self):
        self.verify(); self.file.write_bytes(b'X' * len(self.raw))
        with self.assertRaisesRegex(Refused, 'manual_reference_changed'): self.verify()
    def test_hardlink_or_symlink_is_not_adopted(self):
        os.link(self.file, self.root / 'other')
        with self.assertRaisesRegex(Refused, 'manual_reference_file_identity'): self.verify()
        (self.root / 'other').unlink(); self.file.unlink(); self.file.symlink_to('/etc/passwd')
        with self.assertRaises(OSError): self.verify()
    def test_world_readable_file_or_wrong_shard_mode_is_unsupported(self):
        self.file.chmod(0o644)
        with self.assertRaisesRegex(Refused, 'manual_reference_file_identity'): self.verify()
        self.file.chmod(0o600); self.shard.chmod(0o755)
        with self.assertRaisesRegex(Refused, 'manual_reference_shard_identity'): self.verify()
    @unittest.skipUnless(os.geteuid() == 0, 'wrong-owner filesystem fixture requires root')
    def test_wrong_generation_owner_is_rejected(self):
        os.chown(self.file, 1, -1)
        with self.assertRaisesRegex(Refused, 'manual_reference_file_identity'): self.verify()
    def test_already_open_root_does_not_follow_a_rebound_path(self):
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        old = self.root.with_name(self.root.name + '-retained')
        try:
            self.root.rename(old); self.root.mkdir(mode=0o700)
            # Verification still reads original held root, not replacement path.
            m._reference_fd(fd, self.rows, time.monotonic() + 3)
        finally:
            os.close(fd); self.root.rmdir(); old.rename(self.root)


class FencingCommandTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='manual-fence-', dir='/tmp')
        self.addCleanup(self.tmp.cleanup); self.root = Path(self.tmp.name)
        config = policy(self.root); self.inv = inventory(self.root, config)
        self.cid = 'a' * 64
        mount = {'source': str(self.root / 'old-pg'), 'destination': '/var/lib/postgresql',
                 'rw': True, 'type': 'bind'}
        self.inv['linux']['postgres']['primary'] = {'container_id': self.cid,
            'image_id': 'sha256:' + 'b' * 64, 'mounts': [mount]}
        self.op = operation(self.inv); self.backend = FakeBackend(); self.running = True
        self.exit = 0; self.fail_stop = False; self.guid = '123'
        self.point = {'system_identifier': '1234', 'timeline': 1,
            'postmaster_started_at': '2026-11-02 00:00:00+00', 'wal_lsn': '1/5',
            'index_ack': 3, 'watermark': 3, 'in_recovery': False}
        self.tables = {'openlegal.corpus_capture': {'rows': 0, 'sha256': 'c' * 64}}
        provider_facts = {'provider_hold_before': {}, 'provider_hold_after': {},
            'hold_created_by_operation': False, 'collectors_resumed': False,
            'provider_state_before_artifact': 'before.json', 'provider_state_after_artifact': 'after.json'}
        self.data = {'checkpoint': self.point, 'retained_tables': self.tables,
            'retained_references': [], 'index': {'sha256': 'd' * 64}, 'provider': {},
            'provider_receipt_facts': provider_facts,
            'closure': {'workloads': [], 'jobs': [], 'cri': [],
                'cgroups': [{'groups': ['0::/synthetic-old'], 'pids': []}],
                'processes': [], 'sql_sessions': [], 'runtime_guards': []}}
        self.backend._manual_quiesce_and_capture = lambda op: m.save_evidence(
            self.backend, op, 'captured', self.data)
        self.backend._fingerprints = lambda op: self.tables
        self.backend._references = lambda op: []
        def sql(op, statement, **kwargs):
            if statement == m.CHECKPOINT_SQL: return self.point
            if statement == m.STORAGE_METADATA_SQL:
                return {'corpus_control': {'index_ack': 3, 'next_event': 4, 'staged_bytes': 0},
                    'cache_storage': {'id': uid(), 'snapshots': 0, 'queries': 0, 'referenced_bytes': 0},
                    'cache_totals': {'snapshots': 0, 'queries': 0, 'referenced_bytes': 0}}
            if 'openlegal_checkpoint.observe(' in statement:
                return {'state': {}, 'hold_control': {}, 'admission_ownership': {},
                        'checkpoint_owner': None, 'checkpoint_readback': None}
            return {}
        self.backend._sql = sql
        self.backend._run = self.run_command
    def run_command(self, op, binary, args, **kwargs):
        self.backend.calls.append((binary, args))
        if binary == 'docker':
            self.assertEqual(args[:2], m.DOCKER); command = args[2:]
            if command[0] == 'inspect':
                return canonical([{'Id': self.cid, 'Image': 'sha256:' + 'b' * 64,
                    'Mounts': [{'Source': str(self.root / 'old-pg'), 'Destination': '/var/lib/postgresql',
                                'RW': True, 'Type': 'bind'}],
                    'HostConfig': {'RestartPolicy': {'Name': 'no'}},
                    'State': {'Running': self.running, 'Pid': os.getpid() if self.running else 0,
                              'ExitCode': self.exit, 'OOMKilled': False, 'Error': ''}}])
            if command[0] == 'update': return (self.cid + '\n').encode()
            if command[0] == 'stop':
                if self.fail_stop: raise Refused('synthetic_unknown_stop_ack')
                self.running = False; return (self.cid + '\n').encode()
        if binary == 'zfs':
            if args[0] == 'set': return b''
            return ('guid\t' + self.guid + '\nreadonly\ton\nmountpoint\t'
                    + str(self.root / 'old-pg') + '\n').encode()
        raise AssertionError((binary, args))
    def invoke(self):
        with patch.object(m.os, 'geteuid', return_value=0), \
             patch.object(m, '_prepared', return_value={'synthetic_prepared': True}), \
             patch.object(m, '_process_absent', side_effect=lambda value: self.backend.calls.append(
                 ('physical_closed', []))), \
             patch('linux_backend.process_cgroup', return_value=['0::/docker/' + self.cid]), \
             patch('linux_backend.members', return_value=[]):
            return m.fence_old(self.backend, self.op)
    def test_explicit_command_order_fences_only_after_actual_closure(self):
        result = self.invoke()
        self.assertTrue(result['old_primary_physically_fenced'])
        calls = self.backend.calls
        update = next(i for i, (binary, args) in enumerate(calls) if binary == 'docker' and args[2] == 'update')
        stop = next(i for i, (binary, args) in enumerate(calls) if binary == 'docker' and args[2] == 'stop')
        closed = next(i for i, (binary, _) in enumerate(calls) if binary == 'physical_closed')
        readonly = next(i for i, (binary, args) in enumerate(calls) if binary == 'zfs' and args[0] == 'set')
        self.assertLess(update, stop); self.assertLess(stop, closed); self.assertLess(closed, readonly)
        self.assertEqual(calls[readonly][1], ['set', 'readonly=on', 'tank/mcp-pg'])
    def test_unknown_stop_ack_never_sets_dataset_readonly(self):
        self.fail_stop = True
        with self.assertRaisesRegex(Refused, 'synthetic_unknown_stop_ack'): self.invoke()
        self.assertFalse(any(binary == 'zfs' for binary, _ in self.backend.calls))
    def test_nonzero_actual_exit_never_calls_readonly_fence(self):
        self.exit = 1
        with self.assertRaisesRegex(Refused, 'manual_old_primary_not_closed'): self.invoke()
        self.assertFalse(any(binary == 'zfs' for binary, _ in self.backend.calls))
    def test_rebound_dataset_guid_cannot_complete_fence(self):
        self.guid = '999'
        with self.assertRaisesRegex(Refused, 'manual_readonly_fence_not_confirmed'): self.invoke()
    def test_docker_health_and_error_payloads_never_enter_journal(self):
        original = self.backend._run
        sentinel = 'private-health-credential-sentinel'
        def run(op, binary, args, **kwargs):
            raw = original(op, binary, args, **kwargs)
            if binary == 'docker' and args[2] == 'inspect':
                value = decode(raw); value[0]['State']['Health'] = {'Log': [{'Output': sentinel}]}
                value[0]['State']['Error'] = sentinel
                return canonical(value)
            return raw
        self.backend._run = run
        m._inspect(self.backend, self.op, 'primary')
        self.assertNotIn(sentinel.encode(), b''.join(self.op.journal.values.values()))


class ResumePermissionTests(unittest.TestCase):
    """Pure ordering tests; pair/physical validation is separately stubbed here."""
    def setUp(self):
        self.op = operation({'linux': {'serving_image': 'repo@sha256:' + 'a' * 64,
            'manual_recovery': {'replication_peer': {'host': '192.0.2.7'}}}}, 'reseed')
        self.backend = FakeBackend(); self.calls = []
        self.pair = {k: None for k in m.MANUAL_HELPER_CONTRACT['reseeded'].split(',')}
        self.pair['new_epoch'] = uid()
        self.pair['primary'] = {'container_id': 'a' * 64}
        self.pair['peer'] = {'after': {k: 'synthetic-only' for k in (
            'managed_epoch', 'blob_binding_sha256', 'primary_system_identifier', 'timeline',
            'postmaster_started_at', 'standby_name', 'standby_role', 'standby_client_dn',
            'standby_issuer_dn', 'standby_client_serial')}}
        self.ref = m.save_evidence(self.backend, self.op, 'reseeded', self.pair)
        resources = [{'kind': 'deployment', 'name': name, 'namespace': 'serving', 'uid': uid(),
                      'spec_sha256': 'b' * 64, 'resource_version': '1', 'active': False}
                     for name in ('serving', 'scheduler')]
        prepared = {'configuration': {'resources': resources,
                    'serving_runtime_image_id': 'sha256:' + 'c' * 64}}
        self.seeded = {'pair': self.pair, 'pair_reference': self.ref,
            'promoted': {'sealed_source': {'provider': {}, 'retained_references': [],
                'retained_tables': {'openlegal.corpus_capture': {'rows': 0, 'sha256': 'f' * 64}}}},
            'prepared': prepared,
            'blob_binding_sha256': 'd' * 64}
        self.op.previous['reseed'] = {'phase': 'reseed', 'operation_id': self.op.operation_id,
            'status': 'passed', 'actual_exit_code': 0, 'owned_processes_closed': True,
            'inventory_sha256': self.op.inventory_sha256,
            'source_graph_sha256': self.op.source_graph_sha256,
            'authority_sha256': self.op.authority_sha256, 'facts': self.seeded}
        self.op.previous['fence_old'] = dict(self.op.previous['reseed'], phase='fence_old', facts={
            'hold_created_by_operation': True,
            'provider_hold_after': {'owner_operation_id': self.op.operation_id,
                                   'recovery_hold': True, 'clean': True},
            'provider_checkpoint_readback': {'hold': {'operation_id': self.op.operation_id, 'owned': True}}})
        self.op.phase = 'strict_resume'; self.resumed = False
        self.resources = resources
        self.backend._manual_apply_target_configuration = self.configure
        self.backend._manual_strict_start = self.start
        self.backend._manual_lookup_readback = self.readback
        self.restart_during_audit = False; self.stale_health = False
    def configure(self, op, *args):
        self.calls.append(('configure',))
        return m.save_evidence(self.backend, op, 'configured', {
            'new_epoch': self.pair['new_epoch'], 'resources': self.resources, 'provider': {}})
    def start(self, op, promoted, data, configured, *, release_owned_hold, resume_collection):
        self.calls.append(('start', release_owned_hold, resume_collection))
        result = {k: None for k in m.MANUAL_HELPER_CONTRACT['started'].split(',')}
        result.update(self.pair)
        pod_id = uid(); result['pods'] = [{'uid': pod_id, 'configured_image': op.inventory['linux']['serving_image'],
            'runtime_image_id': 'sha256:' + 'c' * 64, 'ready': True, 'restarts': 0,
            'epoch': self.pair['new_epoch']}]
        result['endpoints'] = [pod_id]
        result['preservation_before_start'] = dict(self.ref, phase='reseed')
        result['provider_receipt_facts'] = {'provider_hold_before': {}, 'provider_hold_after': {},
            'hold_created_by_operation': False, 'collectors_resumed': self.resumed,
            'provider_state_before_artifact': 'before.json', 'provider_state_after_artifact': 'after.json'}
        self.last_started = result
        return m.save_evidence(self.backend, op, 'started', result)
    def readback(self, op):
        self.calls.append(('readback',))
        pods = copy.deepcopy(self.last_started['pods'])
        if self.restart_during_audit: pods[0]['uid'] = uid()
        return m.save_evidence(self.backend, op, 'lookup', {
            'pods': pods, 'endpoints': [pods[0]['uid']], 'provider': {},
            'runtime_guard': self.last_started['runtime_guard'],
            'retained_tables': self.seeded['promoted']['sealed_source']['retained_tables'],
            'retained_references': [],
            'health': {'pod_uid': pods[0]['uid'], 'status': 200, 'body_sha256': 'e' * 64,
                       'observed_at': time.time() - (121 if self.stale_health else 0)}})
    def invoke(self):
        with patch.object(m, '_config'), patch.object(m, '_old_fence'), \
             patch.object(m, '_pair', side_effect=lambda *a: self.calls.append(('pair',))), \
             patch.object(m, '_fresh_barrier', side_effect=lambda *a: self.calls.append(('final_barrier',))):
            return m.strict_resume(self.backend, self.op)
    def test_missing_authority_denies_release_and_collection_before_effect(self):
        self.invoke()
        self.assertIn(('start', False, False), self.calls)
        self.assertLess(self.calls.index(('pair',)), self.calls.index(('configure',)))
        self.assertLess(self.calls.index(('readback',)), self.calls.index(('final_barrier',)))
    def test_true_release_permission_still_does_not_start_collection(self):
        self.op.authority = {'release_owned_clean_hold_permitted': True,
                             'collection_resume_permitted': True}
        self.invoke(); self.assertIn(('start', True, False), self.calls)
    def test_truthy_nonboolean_is_not_authority(self):
        self.op.authority = {'release_owned_clean_hold_permitted': 'true'}
        self.invoke(); self.assertIn(('start', False, False), self.calls)
    def test_scope_permission_cannot_release_a_foreign_or_uncertain_hold(self):
        self.op.authority = {'release_owned_clean_hold_permitted': True}
        self.op.previous['fence_old']['facts']['provider_hold_after']['owner_operation_id'] = uid()
        self.invoke(); self.assertIn(('start', False, False), self.calls)
    def test_tampered_prestart_journal_refuses_before_configuration_or_start(self):
        self.op.journal.values[self.ref['artifact']] = b'{}\n'
        with self.assertRaises(Refused): self.invoke()
        self.assertFalse(any(row[0] in ('configure', 'start') for row in self.calls))
    def test_unexpected_collector_start_is_never_green(self):
        self.resumed = True
        with self.assertRaisesRegex(Refused, 'manual_collection_started_without_permission'): self.invoke()
    def test_active_resource_during_configuration_is_refused_before_start(self):
        self.resources[0]['active'] = True
        with self.assertRaisesRegex(Refused, 'manual_resource_identity_or_active'): self.invoke()
        self.assertFalse(any(row[0] == 'start' for row in self.calls))
    def test_pod_replacement_during_pair_audit_is_not_ready_acceptance(self):
        self.restart_during_audit = True
        with self.assertRaisesRegex(Refused, 'manual_lookup_restarted_during_final_audit'): self.invoke()
    def test_old_http_health_cannot_complete_current_phase(self):
        self.stale_health = True
        with self.assertRaisesRegex(Refused, 'manual_lookup_health_not_fresh'): self.invoke()


if __name__ == '__main__': unittest.main()
