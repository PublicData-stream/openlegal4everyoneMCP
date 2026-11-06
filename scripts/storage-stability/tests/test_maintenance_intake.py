"""Actual /tmp/FD/fork/dual-byte intake tests with explicitly synthetic native ports.

The original driver really forks/waits both children; seed really copies owned
Blob generations; roles.run emits its real result and consumer evidence. SQL,
Kubernetes, independent-media qualification and the role native command are
synthetic ports. No Docker, PostgreSQL, provider, maintenance mutation or Ready
acceptance executes. Only installed-module metadata checks are bypassed, after
checking their real source SHA; private input/journal metadata stays real.
"""
import copy
import errno
import json
import os
from pathlib import Path
import sys
import time
import unittest
import uuid
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(BASE), str(Path(__file__).resolve().parent)]
import initial_provider_maintenance as m
import initial_role_prepare as roles
import test_initial_blob_seed as seed_tests
import test_initial_driver as driver_tests
import test_initial_role_prepare as role_tests
from interface import Refused
from inventory import canonical, decode, digest


@unittest.skipUnless(os.geteuid() == 0, 'actual root private /tmp metadata and UID10004 Blob files')
class MaintenanceIntake(unittest.TestCase):
    def setUp(self):
        self.seedcase = seed_tests.SeedTests('test_live_driver_actual_exit_and_all_physical_union_generations_seeded')
        self.addCleanup(self.seedcase.doCleanups)
        original_run = driver_tests.DriverTests.run_driver
        original_artifact = m.driver.Backend._artifact
        def anchored_driver(case, **kwargs):
            # This shared integration fixture covers driver, seed and roles
            # before approval work (and maintenance/Pair in its consumers).
            # Pin the whole-chain allowance before preparing the one clock;
            # driver-only unit cases keep their original 30-second fixture.
            case.config['total_deadline_secs'] = 180
            case.configref = case.save('driver.json', case.config)
            case.auth['inventory_sha256'] = case.configref['sha256']
            case.authref = case.save('authority.json', case.auth)
            clock = case.prepare_clock()
            return original_run(case, clock=clock, **kwargs)
        def native_artifact(backend, op, label, value):
            if op.phase == m.legacy.PHASE and label == 'returned-payloads':
                value = copy.deepcopy(value)
                value['payloads']['preservation'] = {
                    'tables': {}, 'references': {'synthetic_rows': 0},
                    'migration_history': {'synthetic_schema': 27}}
            return original_artifact(backend, op, label, value)
        with patch.object(driver_tests.DriverTests, 'run_driver', anchored_driver), \
             patch.object(m.driver.Backend, '_artifact', native_artifact):
            self.seedcase.setUp()
        self.root = self.seedcase.root
        self.case = self.seedcase.case
        self.b = self.seedcase.backend
        self.lock = self.case.lock
        self.seed_result = self.seedcase.run_seed()
        self.clock = self.b._initial_driver_clock
        self.j = self.b._initial_driver_journal
        self.addCleanup(self.cleanup_role_guard)
        for module in (roles, m):
            original = module.private_bytes
            def source_private(path, sha=None, _original=original, **kwargs):
                if Path(path) in (BASE / 'initial_role_prepare.py', BASE / 'initial_provider_maintenance.py'):
                    raw = Path(path).read_bytes()
                    self.assertEqual(digest(raw), sha)
                    return raw
                return _original(path, sha, **kwargs)
            p = patch.object(module, 'private_bytes', side_effect=source_private)
            p.start(); self.addCleanup(p.stop)
        c = role_tests.config()
        c.update(schema_version=1, kind='openlegal_initial_role_prepare_inventory',
                 operation_id=self.case.opid, source_sha256=digest((BASE / 'initial_role_prepare.py').read_bytes()),
                 seed_inventory=self.seedcase.cfgref, seed_authority=self.seedcase.authref)
        self.role_cfg = c
        role_ref = self.write('role-input.json', c)
        role_auth = dict(schema_version=1, kind='openlegal_initial_role_prepare_authority', approved=True,
            operation_id=self.case.opid, inventory_sha256=role_ref['sha256'], source_sha256=c['source_sha256'],
            seed_inventory_sha256=self.seedcase.cfgref['sha256'], seed_authority_sha256=self.seedcase.authref['sha256'],
            driver_inventory_sha256=self.case.configref['sha256'], driver_authority_sha256=self.case.authref['sha256'],
            valid_until=int(time.time()) + 300,
            **dict.fromkeys(('role_prepare_permitted', 'dba_uds_ddl_permitted', 'peer_role_permitted',
                            'runtime_new_execute_and_schema_usage_permitted', 'preserve_originals'), True),
            **dict.fromkeys(('automatic_retry', 'credentials_permitted', 'trust_permitted', 'ready_permitted',
                            'provider_permitted', 'workload_permitted', 'drop_permitted'), False))
        self.role_authority_ref = self.write('role-authority.json', role_auth)
        self.ddl_seen = False
        def synthetic_sql(backend, op, sql):
            op.seal()
            if sql == roles.catalog_sql(c['roles'], c['runtime_role']):
                return role_tests.catalog(c, before=not self.ddl_seen)
            if sql == roles._old_catalog_sql(c):
                return {'synthetic_old_catalog': 'unchanged'}
            if "'required',openlegal.replication_required()" in sql:
                return {'required': False, 'rows': 0}
            raise AssertionError('unexpected synthetic role SQL port')
        def synthetic_command(backend, op, sql, *, ddl=False):
            self.assertTrue(ddl)
            self.assertEqual(sql, roles._ddl(c, role_tests.catalog(c, before=True)))
            self.ddl_seen = True
            ref = roles._saved(backend, op, 'synthetic-native-command', {
                'actual_exit_code': 0, 'started': True, 'owned_processes_closed': True,
                **dict.fromkeys(('expired', 'interrupted', 'unexpected_descendants', 'output_limit_exceeded'), False),
                'fixture_only': True, 'stdin_sha256': digest(sql)})
            return b'synthetic native command only', ref
        preservation = decode(m.seed._phase_raw(self.j, m.legacy.PHASE,
            m.legacy.PHASE + '-returned-payloads.json')[0])['payloads']['preservation']
        with patch.object(roles, '_query', side_effect=synthetic_sql), \
             patch.object(roles, '_command', side_effect=synthetic_command), \
             patch.object(roles.migration, '_old_state', side_effect=lambda *a: copy.deepcopy(preservation)), \
             patch.object(roles.migration, '_sessions', return_value={'sessions': []}):
            self.role_result = roles.run(self.b, self.lock, role_ref, self.role_authority_ref)
        self.assertEqual(roles.consume_live(self.b, self.lock,
            self.b._initial_role_prepare_complete['reference']), self.role_result)
        hba = self.write('preprovision-hba.json', b'synthetic protected local rules\n')
        ident = self.write('preprovision-ident.json', b'synthetic protected ident rules\n')
        self.provision_ref = self.write('preprovision.json', {
            'schema_version': 1, 'kind': 'openlegal_initial_provider_maintenance_preprovision',
            'candidate': copy.deepcopy(self.case.template['candidate']),
            'operator_role': 'fixture_provider_operator', 'operator_uid': 10004,
            'authentication': {'kind': 'protected_uds_trust_v1', 'hba_file': hba, 'ident_file': ident,
                'local_rules_sha256': '1' * 64, 'ident_rules_sha256': '2' * 64},
            'function_owners': {name: {'name': 'migration_fixture', 'body_sha256': '3' * 64}
                                for name in m.FUNCTIONS}})
        self.request = {'actor': 'fixture operator', 'reason': 'synthetic held maintenance',
            'quiescence': {'writers_stopped_at': int(time.time()) - 1, 'deployment_revision': 'fixture-revision',
                'stopped_writer_ids': ['fixture-serving', 'fixture-scheduler'],
                'evidence_reference': 'fixture:actual-driver-parent-anchor'},
            'resolve_legacy': False, 'owners': [], 'resume': False, 'waits': []}
        self.request_ref = self.write('operator-request.json', (json.dumps(self.request, indent=2) + '\n').encode())
        inbox = self.root / 'approval-inbox'; inbox.mkdir(mode=0o700); st = inbox.stat()
        self.inbox = {'path': str(inbox), 'identity': {'dev': st.st_dev, 'ino': st.st_ino}}
        self.source_sha = digest((BASE / 'initial_provider_maintenance.py').read_bytes())
        self.counter = 0
        self.addCleanup(self.cleanup_intake)

    def cleanup_role_guard(self):
        fd = getattr(self.b, '_initial_role_prepare_guard_fd', None)
        if fd is not None:
            try: os.close(fd)
            except OSError: pass
        Path('/tmp/openlegal-initial-role-prepare-primary-' + self.case.cid + '.lock').unlink(missing_ok=True)

    def cleanup_intake(self):
        s = getattr(self.b, '_initial_provider_maintenance_intake', None)
        if s is not None and s.inbox_fd is not None:
            try: os.close(s.inbox_fd)
            except OSError: pass

    def write(self, name, value):
        raw = value if type(value) is bytes else canonical(value)
        path = self.root / name; path.write_bytes(raw); path.chmod(0o600)
        return {'path': str(path), 'sha256': digest(raw)}

    def prepare(self):
        return m.prepare_maintenance_request(self.b, self.lock, provision_ref=self.provision_ref,
            request_ref=self.request_ref, source_sha256=self.source_sha, inbox=self.inbox)

    def approval(self, s, *, change=None):
        self.counter += 1
        auth = dict(s.scope, approved=True, valid_until=int(time.time()) + 300)
        if change: change(auth)
        return self.write('external-authority-%d.json' % self.counter, auth)

    def envelope(self, s, *, change=None, raw=None, authority_ref=None):
        value = {'schema_version': 1, 'kind': 'openlegal_initial_stage_capability_envelope',
            'operation_id': s.cfg['operation_id'], 'stage': 'provider_maintenance', 'stage_nonce': s.nonce,
            'request_sha256': digest(s.request_raw), 'inventory': copy.deepcopy(s.inventory_ref),
            'authority': authority_ref or self.approval(s)}
        if change: change(value)
        data = raw if raw is not None else canonical(value)
        path = Path(self.inbox['path']) / m.INTAKE_ENVELOPE
        path.write_bytes(data); path.chmod(0o600)
        return {'path': str(path), 'sha256': digest(data)}

    def assert_held_unknown(self, s, *, original_clock=True):
        self.assertEqual(s.state, 'UNKNOWN')
        if original_clock: self.assertEqual(self.b._driver_deadline, self.clock.end)
        self.lock.check()
        if s.inbox_fd is not None: self.assertEqual(os.fstat(s.inbox_fd).st_ino, self.inbox['identity']['ino'])
        self.assertEqual(m.processes._children(os.getpid()), set())
        with self.assertRaises(Refused): self.prepare()

    def readonly_dispatch(self, b, lock, invref, authref):
        # Stop at the genuine run admission/readonly qualification boundary;
        # no stage body, native SQL command or fabricated native result executes.
        m._dedicated_root(b, lock)
        m._intake_run_gate(b, lock, invref, authref)
        m.driver._check_end(b)
        loaded = m._load(invref, authref)
        actual = m._live(b, lock, loaded[0], loaded[6])
        self.assertIs(actual[0], self.j)
        return {'status': 'SYNTHETIC_DISPATCH_QUALIFIED_NONREADY', 'ready': False,
                'authority_sha256': digest(loaded[3])}

    def test_actual_driver_seed_role_candidate_has_no_approval_or_execution(self):
        before_end = self.b._driver_deadline
        with patch.object(m, 'run', side_effect=AssertionError('no dispatch during prepare')) as run:
            s, raw = self.prepare()
        run.assert_not_called()
        value = decode(raw)
        self.assertEqual(s.state, 'WAITING')
        self.assertEqual(value['status'], 'UNAPPROVED_CANDIDATE_DATA')
        self.assertFalse(value['ready'])
        self.assertNotIn('approved', s.scope); self.assertNotIn('valid_until', s.scope)
        self.assertEqual(s.cfg['role_prepare'], self.b._initial_role_prepare_complete['reference'])
        self.assertEqual(s.cfg['seed_inventory'], self.seedcase.cfgref)
        self.assertEqual(s.cfg['driver_inventory_sha256'], self.case.configref['sha256'])
        self.assertEqual(s.cfg['driver_authority_sha256'], self.case.authref['sha256'])
        self.assertEqual(s.cfg['legacy_inventory_sha256'], self.case.invref['sha256'])
        self.assertEqual(self.b._driver_deadline, before_end)
        self.assertIs(s.clock, self.clock)
        self.assertEqual(self.j.phase_paths[0][0].name, roles.PHASE)
        for path, _ in self.j.phase_paths:
            self.assertEqual((path / Path(s.inventory_ref['path']).name).read_bytes(), canonical(s.cfg))
            self.assertEqual((path / s.request_ref['name']).read_bytes(), raw)
        self.assertEqual(s.scope['request_sha256'], digest(canonical(self.request)))
        self.assertNotEqual(s.scope['request_sha256'], self.request_ref['sha256'])
        self.assertEqual(m.processes._children(os.getpid()), set())

    def test_inbox_inside_current_role_journal_is_unknown_before_open_or_candidate(self):
        inputs = {ref['path']: Path(ref['path']).read_bytes() for ref in (self.provision_ref, self.request_ref)}
        path = self.j.phase_paths[0][0]; st = path.stat()
        self.inbox = {'path': str(path), 'identity': {'dev': st.st_dev, 'ino': st.st_ino}}
        with patch.object(m, 'run', side_effect=AssertionError('journal inbox dispatched')) as run:
            with self.assertRaisesRegex(Refused, 'inbox_inside_journal'): self.prepare()
            run.assert_not_called()
        s = self.b._initial_provider_maintenance_intake
        self.assert_held_unknown(s)
        self.assertIsNone(s.inbox_fd)
        self.assertIsNotNone(s.budget)
        self.assertIs(getattr(self.j, '_reservation', None), s.budget)
        for root, _ in self.j.phase_paths:
            self.assertFalse((root / 'maintenance-intake-candidate.json').exists())
        for name, raw in inputs.items(): self.assertEqual(Path(name).read_bytes(), raw)

    def test_exact_external_authority_uses_full_loader_then_fixed_readonly_dispatch(self):
        s, _ = self.prepare(); authority_ref = self.approval(s)
        self.assertEqual(m._load(s.inventory_ref, authority_ref)[0], s.cfg)
        envelope_ref = self.envelope(s, authority_ref=authority_ref)
        with patch.object(m, 'run', side_effect=self.readonly_dispatch) as run:
            result = m.execute_approved_maintenance(s, envelope_ref)
            self.assertEqual(result['status'], 'SYNTHETIC_DISPATCH_QUALIFIED_NONREADY')
            self.assertFalse(result['ready']); self.assertEqual(s.state, 'CONSUMED')
            self.assertTrue(s.entered_run); self.assertIsNotNone(s.finished)
            run.assert_called_once_with(self.b, self.lock, s.inventory_ref, authority_ref)
            with self.assertRaises(Refused): m.execute_approved_maintenance(s, envelope_ref)
            self.assertEqual(run.call_count, 1)
        self.assertEqual(self.j.phase_paths[0][0].name, roles.PHASE)
        self.assertEqual(self.b._driver_deadline, self.clock.end)


    def refuse(self, s, ref):
        with patch.object(m, 'run', side_effect=AssertionError('invalid capability reached execution')) as run:
            with self.assertRaises((Refused, OSError)):
                m.execute_approved_maintenance(s, ref)
            run.assert_not_called()
        self.assert_held_unknown(s)
        with self.assertRaises(Refused): m.execute_approved_maintenance(s, ref)

    def test_every_authority_permission_and_strict_bool_uses_existing_validator(self):
        s, _ = self.prepare()
        mutations = [(key, not value) for key, value in s.scope.items() if type(value) is bool]
        mutations += [(key, int(value)) for key, value in s.scope.items() if type(value) is bool]
        mutations += [('approved', 1), ('approved', False), ('valid_until', True),
                      ('schema_version', True), ('unexpected_permission', False),
                      ('inventory_sha256', 'f' * 64), ('request_sha256', self.request_ref['sha256']),
                      ('provision_sha256', 'f' * 64), ('source_sha256', 'f' * 64)]
        with patch.object(m, 'run', side_effect=AssertionError('validation is readonly')) as run:
            for key, value in mutations:
                authref = self.approval(s, change=lambda a, k=key, v=value: a.update({k: v}))
                with self.subTest(key=key, value=value), self.assertRaises(Refused):
                    m._load(s.inventory_ref, authref)
            run.assert_not_called()
        self.assertEqual(s.state, 'WAITING')
        self.assertFalse(s.entered_run)

    def test_expired_external_authority_is_terminal_before_dispatch(self):
        s, _ = self.prepare()
        auth = self.approval(s, change=lambda a: a.update(valid_until=int(time.time()) - 1))
        self.refuse(s, self.envelope(s, authority_ref=auth))

    def test_direct_run_cannot_bypass_waiting_intake(self):
        s, _ = self.prepare(); auth = self.approval(s)
        with patch.object(m, '_load', side_effect=AssertionError('pending request cannot load grant')) as load:
            with self.assertRaisesRegex(Refused, 'dispatch_required'):
                m.run(self.b, self.lock, s.inventory_ref, auth)
            load.assert_not_called()
        self.assert_held_unknown(s)

    def test_original_request_bytes_cannot_change_during_approval_wait(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        Path(self.request_ref['path']).write_bytes(canonical(dict(self.request, reason='changed after preparation')))
        self.refuse(s, ref)

    def test_original_provision_bytes_cannot_change_during_approval_wait(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        path = Path(self.provision_ref['path']); value = decode(path.read_bytes())
        value['operator_role'] = 'another_operator'; path.write_bytes(canonical(value))
        self.refuse(s, ref)

    def test_actual_role_predecessor_raw_cannot_be_replaced_by_complete_shaped_json(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        value = decode(self.b._initial_role_prepare_complete['raw']); value['ready'] = True
        self.b._initial_role_prepare_complete['raw'] = canonical(value)
        self.refuse(s, ref)

    def test_current_role_phase_cannot_be_rebound(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        # A different live phase tuple is refused without creating/adopting one.
        self.j.phase_paths = [(path.parent, pin) for path, pin in self.j.phase_paths]
        self.refuse(s, ref)

    def test_expired_original_clock_remains_expired_and_never_dispatches(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        self.b._driver_deadline = time.monotonic() - 1
        with patch.object(m, 'run', side_effect=AssertionError('expired clock dispatched')) as run:
            with self.assertRaisesRegex(Refused, 'original_deadline'):
                m.execute_approved_maintenance(s, ref)
            run.assert_not_called()
        self.assertLess(self.b._driver_deadline, time.monotonic())
        self.assert_held_unknown(s, original_clock=False)

    def test_changed_original_root_identity_blocks_approval(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        foreign = dict(s.parent, start_ticks=str(int(s.parent['start_ticks']) + 1))
        with patch.object(m, '_intake_parent', return_value=foreign):
            self.refuse(s, ref)

    def test_duplicate_ofd_number_cannot_replace_original_shared_lock_capability(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        old = self.lock.fd; duplicate = os.dup(old)
        try:
            self.lock.fd = duplicate
            with patch.object(m, 'run', side_effect=AssertionError('rebound lock dispatched')) as run:
                with self.assertRaisesRegex(Refused, 'original_context'):
                    m.execute_approved_maintenance(s, ref)
                run.assert_not_called()
        finally:
            self.lock.fd = old; os.close(duplicate)
        self.assert_held_unknown(s)

    def test_inbox_path_replacement_cannot_replace_held_directory(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        path = Path(self.inbox['path']); path.rename(self.root / 'old-inbox'); path.mkdir(mode=0o700)
        self.refuse(s, ref)

    def test_same_bytes_envelope_replacement_after_first_read_is_refused(self):
        s, _ = self.prepare(); ref = self.envelope(s); real = m._intake_read_envelope; calls = []
        def replaced(context, reference, expected=None):
            result = real(context, reference, expected)
            calls.append(True)
            if len(calls) == 1:
                path = Path(ref['path']); path.rename(path.with_name('old-envelope.json'))
                path.write_bytes(result[0]); path.chmod(0o600)
            return result
        with patch.object(m, '_intake_read_envelope', side_effect=replaced):
            self.refuse(s, ref)
        self.assertGreaterEqual(len(calls), 1)

    def test_inbox_mount_identity_must_remain_equal_to_actual_held_mount(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        with patch.object(m, '_intake_mount', return_value=s.inbox_mount + 1):
            self.refuse(s, ref)

    def test_second_media_candidate_failure_retains_real_bytes_and_unknown(self):
        second = self.j.phase_paths[1][0]; real = m.journals.new_file
        def failed(path, raw):
            result = real(path, raw)
            if Path(path).parent == second and Path(path).name == 'maintenance-intake-candidate.json':
                raise OSError(errno.EIO, 'synthetic second-media candidate barrier')
            return result
        with patch.object(m.journals, 'new_file', side_effect=failed), \
             patch.object(m, 'run', side_effect=AssertionError('failed prepare dispatched')) as run:
            with self.assertRaisesRegex(OSError, 'second-media candidate'):
                self.prepare()
            run.assert_not_called()
        s = self.b._initial_provider_maintenance_intake
        self.assert_held_unknown(s)
        for path, _ in self.j.phase_paths:
            self.assertTrue((path / 'maintenance-intake-candidate.json').exists())
        with self.assertRaisesRegex(Refused, 'poisoned'): self.j.check()
        self.assertIsNotNone(s.budget)
        self.assertEqual(self.j.phase_paths[0][0].name, roles.PHASE)

    def test_second_media_authority_failure_retains_approval_without_dispatch(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        second = self.j.phase_paths[1][0]; real = m.journals.new_file
        def failed(path, raw):
            result = real(path, raw)
            if Path(path).parent == second and Path(path).name == 'maintenance-intake-authority.json':
                raise OSError(errno.EIO, 'synthetic second-media authority barrier')
            return result
        with patch.object(m.journals, 'new_file', side_effect=failed): self.refuse(s, ref)
        for path, _ in self.j.phase_paths:
            self.assertTrue((path / 'maintenance-intake-authority.json').exists())
        self.assertIsNone(s.finished); self.assertFalse(s.entered_run)
        with self.assertRaisesRegex(Refused, 'poisoned'): self.j.check()


    def test_isolated_real_fd_envelope_metadata_reader_matrix(self):
        # Reuse one genuine pending context for READONLY FD qualification only.
        # A rejected explicit execute is separately tested as terminal UNKNOWN.
        s, _ = self.prepare(); path = Path(self.inbox['path']) / m.INTAKE_ENVELOPE
        for kind in ('symlink', 'hardlink', 'fifo', 'mode', 'owner', 'oversized', 'empty', 'raw_hash', 'other_path'):
            if path.exists() or path.is_symlink(): path.unlink()
            ref = self.envelope(s)
            if kind == 'symlink':
                alias = path.with_name('real-envelope.json'); path.rename(alias); path.symlink_to(alias.name)
            elif kind == 'hardlink': os.link(path, path.with_name('linked-envelope.json'))
            elif kind == 'fifo': path.unlink(); os.mkfifo(path, 0o600)
            elif kind == 'mode': path.chmod(0o640)
            elif kind == 'owner': os.chown(path, 10004, 10004)
            elif kind == 'oversized':
                raw = b'x' * (m.INTAKE_ENVELOPE_MAX + 1); path.write_bytes(raw); ref['sha256'] = digest(raw)
            elif kind == 'empty': path.write_bytes(b''); ref['sha256'] = digest(b'')
            elif kind == 'raw_hash': ref['sha256'] = 'f' * 64
            elif kind == 'other_path': ref['path'] = str(path.with_name('other-envelope.json'))
            with self.subTest(kind=kind), self.assertRaises((Refused, OSError)):
                m._intake_read_envelope(s, ref)
            self.assertEqual(s.state, 'WAITING')
            self.lock.check(); self.assertEqual(m.processes._children(os.getpid()), set())
        path.unlink(); self.refuse(s, self.envelope(s, change=lambda v: v.update(stage_nonce=str(uuid.uuid4()))))

    def test_noncanonical_envelope_bytes_are_terminal(self):
        s, _ = self.prepare(); ref = self.envelope(s); path = Path(ref['path'])
        raw = json.dumps(decode(path.read_bytes()), indent=2).encode()
        path.write_bytes(raw); ref['sha256'] = digest(raw)
        self.refuse(s, ref)

    def test_duplicate_prepare_poisons_pending_capability_and_blocks_direct_run(self):
        s, raw = self.prepare(); candidate_before = Path(s.inventory_ref['path']).read_bytes()
        with self.assertRaisesRegex(Refused, 'no_replay'): self.prepare()
        self.assert_held_unknown(s)
        self.assertEqual(Path(s.inventory_ref['path']).read_bytes(), candidate_before)
        for path, _ in self.j.phase_paths: self.assertEqual((path / s.request_ref['name']).read_bytes(), raw)
        with self.assertRaises(Refused): m.run(self.b, self.lock, s.inventory_ref, self.approval(s))

    def test_expiry_after_request_dual_readback_cannot_return_waiting_capability(self):
        real_read = self.j.read_phase_bytes; real_time = m.time.monotonic; late = []
        def read(phase, name, sha):
            raw = real_read(phase, name, sha)
            if name == 'maintenance-intake-request.json': late.append(True)
            return raw
        with patch.object(self.j, 'read_phase_bytes', side_effect=read), \
             patch.object(m.time, 'monotonic', side_effect=lambda: self.clock.end + 1 if late else real_time()), \
             patch.object(m, 'run', side_effect=AssertionError('late preparation dispatched')) as run:
            with self.assertRaisesRegex(Refused, 'original_deadline'): self.prepare()
            run.assert_not_called()
        s = self.b._initial_provider_maintenance_intake
        self.assertTrue(late); self.assert_held_unknown(s)
        self.assertIsNone(s.finished)
        for path, _ in self.j.phase_paths: self.assertTrue((path / 'maintenance-intake-request.json').exists())

    def test_expiry_after_successful_reservation_finish_retains_receipt_and_blocks_dispatch(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        real_finish = self.j.finish_reservation; real_time = m.time.monotonic; late = []
        def finished(token):
            result = real_finish(token); late.append(True); return result
        with patch.object(self.j, 'finish_reservation', side_effect=finished), \
             patch.object(m.time, 'monotonic', side_effect=lambda: self.clock.end + 1 if late else real_time()):
            self.refuse(s, ref)
        self.assertTrue(late); self.assertIsNotNone(s.finished); self.assertFalse(s.entered_run)
        # A completed scope releases unused allowance; expiry afterward still
        # cannot dispatch and does not erase the already dual-ACKed receipt.
        self.assertIsNone(getattr(self.j, '_reservation', None)); self.j.check()
        raw = self.j.read_phase_bytes(s.finished['phase'], s.finished['name'], s.finished['sha256'])
        self.assertEqual(digest(raw), s.finished['sha256'])

    def test_malformed_attached_none_cannot_use_legacy_run_bypass(self):
        self.b._initial_provider_maintenance_intake = None
        with patch.object(m, '_load', side_effect=AssertionError('malformed intake cannot load grant')) as load:
            with self.assertRaisesRegex(Refused, 'dispatch_required'):
                m.run(self.b, self.lock, {}, {})
            load.assert_not_called()


def envelope_binding_case(change):
    def test(self):
        s, _ = self.prepare()
        self.refuse(s, self.envelope(s, change=change))
    return test


for _name, _change in {
    'foreign_operation': lambda v: v.update(operation_id=str(uuid.uuid4())),
    'wrong_stage': lambda v: v.update(stage='pg_pair'),
    'stale_nonce': lambda v: v.update(stage_nonce=str(uuid.uuid4())),
    'wrong_request_raw': lambda v: v.update(request_sha256='f' * 64),
    'wrong_candidate': lambda v: v['inventory'].update(sha256='f' * 64),
    'extra_field': lambda v: v.update(ready=True),
    'bool_schema': lambda v: v.update(schema_version=True),
}.items():
    setattr(MaintenanceIntake, 'test_envelope_' + _name + '_is_terminal', envelope_binding_case(_change))



if __name__ == '__main__': unittest.main()
