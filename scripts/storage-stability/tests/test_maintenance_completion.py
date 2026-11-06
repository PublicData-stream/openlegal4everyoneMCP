"""Actual driver/seed/role/maintenance fork history; synthetic native ports only.

The real maintenance producer writes and reconciles its records, and ROOT really
waits for its child before minting completion. SQL, provider CLI, image, fence and
socket qualification are labelled synthetic ports. No Docker, database, provider,
independent-media durability, pair binding or Ready acceptance executes.
"""
import copy
from contextlib import ExitStack, contextmanager
from dataclasses import FrozenInstanceError
import os
from pathlib import Path
import platform
import shutil
import sys
import time
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(BASE), str(Path(__file__).resolve().parent)]
import initial_provider_maintenance as m
import test_maintenance_intake as intake_tests
import test_initial_provider_maintenance as native_tests
from interface import Refused
from inventory import canonical, decode, digest


@unittest.skipUnless(os.geteuid() == 0, 'actual root private /tmp files and producer fork')
class GenuineMaintenance(unittest.TestCase):
    """Reusable fixture composition without inheriting the earlier test methods."""
    def setUp(self):
        self.h = intake_tests.MaintenanceIntake('test_actual_driver_seed_role_candidate_has_no_approval_or_execution')
        self.addCleanup(self.h.doCleanups)
        original_setup = intake_tests.driver_tests.DriverTests.setUp
        def explicit_candidate(case):
            original_setup(case)
            # Earlier readonly tests use candidate={}. Supply fictional image
            # DATA before the actual prepared clock, input pins and forks exist.
            case.template['candidate'] = {'reference': 'ghcr.io/fixture/candidate@sha256:' + 'c' * 64,
                'image_id': 'sha256:' + 'c' * 64, 'executable_sha256': 'd' * 64}
            case.tref = case.save('template.json', case.template)
            case.config['initial_template'] = case.tref
            case.configref = case.save('driver.json', case.config)
            case.auth.update(inventory_sha256=case.configref['sha256'], initial_template_sha256=case.tref['sha256'])
            case.authref = case.save('authority.json', case.auth)
        with patch.object(intake_tests.driver_tests.DriverTests, 'setUp', explicit_candidate): self.h.setUp()
        for key in ('b', 'lock', 'j', 'clock', 'root', 'case', 'seedcase', 'role_cfg', 'role_result'):
            setattr(self, key, getattr(self.h, key))
        # Checkout source files are not installed root:0600 modules. Bypass only
        # their installation metadata after checking each actual current raw pin.
        original = m.driver.private_bytes
        fixed = {BASE / name for name in ('initial_blob_seed.py', 'initial_role_prepare.py',
                                         'initial_provider_maintenance.py', 'initial_pg_pair.py')}
        def source_private(path, sha=None, **kwargs):
            if Path(path) in fixed:
                raw = Path(path).read_bytes(); self.assertEqual(digest(raw), sha); return raw
            return original(path, sha, **kwargs)
        item = patch.object(m.driver, 'private_bytes', side_effect=source_private)
        item.start(); self.addCleanup(item.stop)
        self.addCleanup(shutil.rmtree, Path('/tmp/openlegal-initial-provider-maintenance-' + self.case.opid), True)
        data = list(native_tests.audit_fixture())
        plan, applied, _, audit, before, after, _ = data
        before['state']['ledger'] = []; after['uncertainty'] = []
        plan['request'] = copy.deepcopy(self.h.request)
        plan['snapshot']['state'] = copy.deepcopy(before['state'])
        applied['resolved_owners'] = []
        audit.update(plan=copy.deepcopy(plan), actor=plan['request']['actor'],
            database_actor='fixture_provider_operator',
            actual_writers_stopped_at=plan['request']['quiescence']['writers_stopped_at'],
            before_state=copy.deepcopy(before['state']), result=copy.deepcopy(applied))
        self.plan, self.applied, self.audit, self.before, self.after = plan, applied, audit, before, after
        self.native_calls = []; self.parent_sessions = []

    def write(self, name, value): return self.h.write(name, value)

    @contextmanager
    def native_ports(self):
        observations = 0
        root_pid = os.getpid()
        def sessions(owner):
            owner.seal(); owner.sql_uncertain = False
            if os.getpid() == root_pid: self.parent_sessions.append(True)
            return {'sessions': []}
        def sql(owner, payload):
            nonlocal observations
            owner.seal()
            if payload == m._observe(owner.cfg['request']['owners']):
                observations += 1
                return copy.deepcopy(self.before if observations <= 2 else self.after)
            expected = ('SELECT to_jsonb(a) FROM openlegal_admin.provider_recovery_audit a WHERE operation_id='
                        + m._quote(self.plan['operation_id']) + '::uuid')
            self.assertEqual(payload, expected)
            return copy.deepcopy(self.audit)
        def helper(owner, action):
            owner.seal(); self.assertIn(action, ('plan', 'apply', 'readback'))
            receipt = owner.save('synthetic-' + action + '-native', {
                'actual_exit_code': 0, 'started': True, 'owned_processes_closed': True,
                **dict.fromkeys(('expired', 'interrupted', 'unexpected_descendants', 'output_limit_exceeded'), False),
                'fixture_only': True, 'action': action})
            return copy.deepcopy(self.plan if action == 'plan' else self.applied), receipt
        def command(owner, args, **kwargs):
            owner.seal()
            self.assertEqual(args, ['image', 'inspect', '--format', m.migration.IMAGE_FORMAT,
                                   owner.p['candidate']['reference']])
            value = {'Id': owner.p['candidate']['image_id'], 'Os': 'linux', 'Volumes': None,
                     'Architecture': {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(platform.machine())}
            return canonical(value), owner.save('synthetic-image-port', {'fixture_only': True})
        with ExitStack() as stack:
            for item in (patch.object(m._Maintenance, 'sessions', sessions),
                         patch.object(m._Maintenance, 'sql', sql),
                         patch.object(m._Maintenance, 'command', command),
                         patch.object(m, '_helper', side_effect=helper),
                         patch.object(m, '_catalog', side_effect=lambda owner: owner.seal()),
                         patch.object(m, '_preserved', side_effect=lambda owner, *args: owner.seal()),
                         patch.object(m.migration, '_fence', return_value=None),
                         patch.object(m.migration, '_primary', side_effect=lambda *args: os.open(self.root, os.O_RDONLY | os.O_DIRECTORY))):
                stack.enter_context(item)
            yield

    def run_maintenance(self):
        self.maintenance_intake, _ = self.h.prepare()
        envelope = self.h.envelope(self.maintenance_intake)
        with self.native_ports():
            self.complete_ref = m.execute_approved_maintenance(self.maintenance_intake, envelope)
        self.owner = self.b._initial_provider_maintenance_live
        self.result = decode(self.j.read_phase_bytes(self.complete_ref['phase'], self.complete_ref['name'],
                                                     self.complete_ref['sha256']))
        return self.complete_ref


class MaintenanceCompletion(GenuineMaintenance):
    def test_actual_child_wait_reconciliation_and_final_readonly_completion(self):
        ref = self.run_maintenance()
        row = self.result['producer_exit']
        self.assertIs(type(row['actual_exit_code']), int); self.assertEqual(row['actual_exit_code'], 0)
        self.assertEqual(row, self.b._initial_provider_maintenance_last_exit)
        self.assertNotEqual(row['pid'], os.getpid()); self.assertEqual(row['remaining_children'], [])
        with self.assertRaises(ChildProcessError): os.waitpid(row['pid'], os.WNOHANG)
        self.assertEqual(m.processes._children(os.getpid()), set())
        self.assertGreaterEqual(len(self.parent_sessions), 2)
        with patch.object(m._Maintenance, 'sql', side_effect=AssertionError('consumer SQL')) as sql, \
             patch.object(m._Maintenance, 'sessions', side_effect=AssertionError('consumer sessions')) as sessions:
            self.assertEqual(m.completed_reference(self.b, self.lock), ref)
            self.assertEqual(m.consume_completed_live(self.b, self.lock, ref), self.result)
        sql.assert_not_called(); sessions.assert_not_called()
        self.assertEqual(self.clock.duration, 180)
        self.assertIs(self.owner.original_clock, self.clock)
        self.assertEqual(self.b._driver_deadline, self.clock.end)
        self.assertEqual(self.j.phase_paths[0][0].name, m.PHASE)

    def test_capability_is_frozen_and_reference_result_are_detached(self):
        self.run_maintenance(); cap = self.b._initial_provider_maintenance_complete
        with self.assertRaises((FrozenInstanceError, AttributeError)): cap.raw = b'changed'
        ref = m.completed_reference(self.b, self.lock); ref['sha256'] = '0' * 64
        self.assertEqual(m.completed_reference(self.b, self.lock), self.complete_ref)
        result = m.consume_completed_live(self.b, self.lock, self.complete_ref)
        result['producer_exit']['actual_exit_code'] = 7
        self.assertEqual(m.consume_completed_live(self.b, self.lock, self.complete_ref), self.result)

    def test_final_closure_error_does_not_mint_even_after_actual_child_zero(self):
        self.maintenance_intake, _ = self.h.prepare(); envelope = self.h.envelope(self.maintenance_intake)
        real = m._retain_sql_closed; parent = os.getpid()
        def failed(owner):
            if os.getpid() == parent: raise Refused('synthetic_final_sql_closure_error')
            return real(owner)
        with self.native_ports(), patch.object(m, '_retain_sql_closed', side_effect=failed):
            with self.assertRaisesRegex(Refused, 'final_sql_closure_error'):
                m.execute_approved_maintenance(self.maintenance_intake, envelope)
        self.assertEqual(self.b._initial_provider_maintenance_last_exit['actual_exit_code'], 0)
        self.assertFalse(hasattr(self.b, '_initial_provider_maintenance_complete'))
        with self.assertRaises(Refused): m.completed_reference(self.b, self.lock)

    def test_final_original_end_expiry_does_not_mint_after_actual_child_zero(self):
        self.maintenance_intake, _ = self.h.prepare(); envelope = self.h.envelope(self.maintenance_intake)
        real = m._retain_sql_closed; parent = os.getpid(); real_time = m.time.monotonic
        def expired(owner):
            real(owner)
            if os.getpid() == parent:
                item = patch.object(m.time, 'monotonic', return_value=self.clock.end + 1)
                item.start(); self.addCleanup(item.stop)
        try:
            with self.native_ports(), patch.object(m, '_retain_sql_closed', side_effect=expired):
                with self.assertRaisesRegex(Refused, 'deadline'):
                    m.execute_approved_maintenance(self.maintenance_intake, envelope)
            self.assertEqual(self.b._initial_provider_maintenance_last_exit['actual_exit_code'], 0)
            self.assertFalse(hasattr(self.b, '_initial_provider_maintenance_complete'))
        finally:
            # Later fixture cleanup must observe the actual system clock.
            m.time.monotonic = real_time

    def test_actual_child_failure_leaves_no_completion(self):
        self.maintenance_intake, _ = self.h.prepare(); envelope = self.h.envelope(self.maintenance_intake)
        with self.native_ports(), patch.object(m, '_helper', side_effect=Refused('synthetic_child_failure')):
            with self.assertRaisesRegex(Refused, 'producer_not_successfully_closed'):
                m.execute_approved_maintenance(self.maintenance_intake, envelope)
        row = self.b._initial_provider_maintenance_last_exit
        self.assertIs(type(row['actual_exit_code']), int); self.assertNotEqual(row['actual_exit_code'], 0)
        with self.assertRaises(ChildProcessError): os.waitpid(row['pid'], os.WNOHANG)
        self.assertFalse(hasattr(self.b, '_initial_provider_maintenance_complete'))

    def test_final_raw_validation_expiry_cannot_mint_completion(self):
        s, _ = self.h.prepare(); envelope = self.h.envelope(s)
        real = m._completed_result; clock_patch = None
        def expire(*args):
            nonlocal clock_patch
            result = real(*args)
            clock_patch = patch.object(m.time, 'monotonic', return_value=self.clock.end + 1)
            clock_patch.start()
            return result
        try:
            with self.native_ports(), patch.object(m, '_completed_result', side_effect=expire):
                with self.assertRaisesRegex(Refused, 'deadline'): m.execute_approved_maintenance(s, envelope)
        finally:
            if clock_patch is not None: clock_patch.stop()
        self.assertEqual(self.b._initial_provider_maintenance_last_exit['actual_exit_code'], 0)
        self.assertFalse(hasattr(self.b, '_initial_provider_maintenance_complete'))

    def test_consumer_final_raw_validation_expiry_cannot_return_success(self):
        self.run_maintenance(); cap = self.b._initial_provider_maintenance_complete
        real = m._completed_result; clock_patch = None
        def expire(*args):
            nonlocal clock_patch
            result = real(*args)
            clock_patch = patch.object(m.time, 'monotonic', return_value=self.clock.end + 1)
            clock_patch.start()
            return result
        try:
            with patch.object(m, '_completed_result', side_effect=expire), self.assertRaisesRegex(Refused, 'deadline'):
                m.consume_completed_live(self.b, self.lock, self.complete_ref)
        finally:
            if clock_patch is not None: clock_patch.stop()
        self.assertIs(self.b._initial_provider_maintenance_complete, cap)

    def test_bool_parent_exit_or_reference_substitution_is_rejected(self):
        self.run_maintenance()
        original = copy.deepcopy(self.b._initial_provider_maintenance_last_exit)
        for code in (False, 7):
            self.b._initial_provider_maintenance_last_exit = dict(original, actual_exit_code=code)
            with self.subTest(code=code), self.assertRaises(Refused):
                m.consume_completed_live(self.b, self.lock, self.complete_ref)
        self.b._initial_provider_maintenance_last_exit = original
        for key, value in (('sha256', '0' * 64), ('name', 'invented-complete.json'), ('phase', 'initial_pg_pair')):
            ref = dict(self.complete_ref); ref[key] = value
            with self.subTest(key=key), self.assertRaises(Refused):
                m.consume_completed_live(self.b, self.lock, ref)

    def test_original_clock_parent_live_owner_and_started_state_are_required(self):
        self.run_maintenance()
        changes = ((self.b, '_initial_driver_clock', object()),
                   (self.b, '_driver_deadline', self.clock.end + 1),
                   (self.b, '_initial_provider_maintenance_live', object()),
                   (self.b, '_initial_provider_maintenance_started', False),
                   (self.b, '_initial_driver_journal', object()),
                   (self.owner, 'parent', dict(self.owner.parent, pid=os.getpid() + 1)),
                   (self.owner, 'cancelled', True),
                   (self.owner, 'sql_uncertain', True))
        for obj, key, value in changes:
            with self.subTest(key=key), patch.object(obj, key, value), self.assertRaises(Refused):
                m.consume_completed_live(self.b, self.lock, self.complete_ref)
        self.assertEqual(m.completed_reference(self.b, self.lock), self.complete_ref)

    def test_completed_native_budget_cannot_be_rebased(self):
        self.run_maintenance(); old = dict(self.owner.budget)
        for key in ('start_count', 'start_bytes', 'records', 'bytes'):
            self.owner.budget[key] += 1
            with self.subTest(key=key), self.assertRaisesRegex(Refused, 'frozen_accounting'):
                m.consume_completed_live(self.b, self.lock, self.complete_ref)
            self.owner.budget = dict(old)

    def test_new_same_phase_data_can_exceed_completed_local_byte_allowance(self):
        self.run_maintenance(); cap = self.b._initial_provider_maintenance_complete
        # Real bounded DATA under the global 512MiB ceiling, saved after native
        # completion. No counter/terminal-budget rebasing is used to simulate it.
        self.j.save_bytes('post-completion-data.bin', b'x' * (64 * 1024**2 + 1))
        self.assertGreater(self.j.bytes, cap.budget[1] + cap.budget[3])
        with self.assertRaisesRegex(Refused, 'reserved_journal_exhausted'): self.owner.seal()
        self.assertEqual(m.consume_completed_live(self.b, self.lock, self.complete_ref), self.result)
        self.assertIs(self.b._initial_provider_maintenance_complete, cap)
        self.assertEqual(self.owner.budget['records'], 1024)

    def test_raw_complete_native_audit_and_closure_tampering_refuse(self):
        self.run_maintenance()
        for ref in (self.complete_ref, self.result['native_command'], self.result['audit'], self.result['sql_closure']):
            path = self.j.phase_paths[0][0] / ref['name']; raw = path.read_bytes()
            try:
                path.write_bytes(raw + b' ')
                with self.subTest(name=ref['name']), self.assertRaises(Refused):
                    m.consume_completed_live(self.b, self.lock, self.complete_ref)
            finally: path.write_bytes(raw)


if __name__ == '__main__':
    unittest.main()
