"""Pair-owned completion from actual _run history and fixed synthetic native ports.

The history fixture runs the real producer, strict ACK capture and helper waits.
Docker/PostgreSQL/media/mount observations remain explicitly synthetic. These
checks establish no enrollment, HDD fit, deployment, public ACK or ROOT OS exit.
"""
import copy
from dataclasses import FrozenInstanceError, replace
import os
from pathlib import Path
import signal
import sys
import tempfile
import types
import time
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(BASE), str(Path(__file__).resolve().parent)]
import initial_pg_pair as p
import pair_history_fixture as history
import test_initial_driver as driver_tests
from interface import Refused
from inventory import canonical, decode


class CompletedHistory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.history = history.GenuinePair()
        try:
            cls.history.setUp()
            cls.history.run_pair()
        except BaseException:
            cls.history.doCleanups()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.history.doCleanups()

    def setUp(self):
        h = self.history
        self.b, self.lock, self.j = h.b, h.lock, h.j
        self.owner, self.ref, self.result = h.owner, h.pair_complete_ref, h.result
        self.cap = self.b._initial_pg_pair_complete

    def consume(self):
        return p.consume_completed_live(self.b, self.lock, self.ref)

    def test_actual_inner_run_outer_finally_and_raw_proofs_mint_frozen_completion(self):
        self.assertIs(type(self.owner), p._Pair)
        self.assertIs(type(self.cap), p._Completed)
        self.assertIs(self.cap.owner, self.owner)
        self.assertIs(self.owner._completed, self.cap)
        self.assertEqual(self.cap.pending, canonical(self.history.pair_pending))
        self.assertEqual(self.cap.raw, canonical(self.result))
        self.assertEqual(self.result['outer_actual_os_exit'], 'NOT_OBSERVABLE_IN_PROCESS')
        self.assertFalse(self.result['ready'])
        self.assertEqual(p.completed_reference(self.b, self.lock), self.ref)
        self.assertEqual(self.consume(), self.result)
        self.assertEqual(sorted(Path(h['spec']['command'][0]).name for h in self.owner.helpers),
                         ['pg_basebackup', 'pg_verifybackup'])
        self.assertEqual(self.b._initial_pg_pair_pending_ownership, {})
        for first, key in ((True, 'first_bind_ack'), (False, 'final_ack')):
            records = p._completed_ack(self.j, self.result[key], self.result['binding'], first=first)
            self.assertTrue(records)
            self.assertTrue(all(type(row['actual_exit_code']) is int and row['actual_exit_code'] == 0
                                for row in records))
        with self.assertRaises(FrozenInstanceError):
            self.cap.raw = b'{}'

    def test_complete_shaped_data_and_copied_or_foreign_capability_cannot_complete(self):
        try:
            self.b._initial_pg_pair_complete = copy.deepcopy(self.result)
            with self.assertRaisesRegex(Refused, 'live_field'): self.consume()
            self.b._initial_pg_pair_complete = replace(self.cap)
            with self.assertRaisesRegex(Refused, 'same_owner'): self.consume()
            self.b._initial_pg_pair_complete = replace(self.cap, backend=object())
            with self.assertRaisesRegex(Refused, 'same_owner'): self.consume()
        finally:
            self.b._initial_pg_pair_complete = self.cap
        for wrong in ({**self.ref, 'name': 'complete-shaped.json'}, {**self.ref, 'sha256': 'a' * 64}):
            with self.assertRaisesRegex(Refused, 'reference'):
                p.consume_completed_live(self.b, self.lock, wrong)
        with self.assertRaises(Refused):
            p.consume_completed_live(self.b, object(), self.ref)
        self.assertEqual(self.consume(), self.result)

    def test_frozen_native_reservation_and_global_caps_survive_later_reserved_data(self):
        budget = canonical(self.owner.budget); original_count, original_bytes = self.cap.count, self.cap.bytes
        reservation = self.j.reserve('pair-completion-test-data', 3, 16384)
        self.j.save_bytes('pair-completion-test-data.json', b'{}\n', reservation=reservation)
        self.j.finish_reservation(reservation)
        self.assertEqual(self.consume(), self.result)
        self.assertEqual(canonical(self.owner.budget), budget)
        self.assertEqual((self.cap.count, self.cap.bytes), (original_count, original_bytes))
        self.assertGreater(self.j.count, original_count)
        self.assertGreater(self.j.bytes, original_bytes)
        for key, value in (('records', self.owner.budget['records'] + 1), ('bytes', self.owner.budget['bytes'] + 1)):
            old = self.owner.budget[key]
            try:
                self.owner.budget[key] = value
                with self.assertRaisesRegex(Refused, 'frozen_accounting'): self.consume()
            finally: self.owner.budget[key] = old
        for key, value in (('count', 4097), ('bytes', p.journals.MAX_PHASE_BYTES + 1)):
            old = getattr(self.j, key)
            try:
                setattr(self.j, key, value)
                with self.assertRaisesRegex(Refused, 'frozen_accounting'): self.consume()
            finally: setattr(self.j, key, old)

    def test_changed_original_clock_context_pending_ownership_and_native_state_refuse(self):
        with patch.object(self.b, '_driver_deadline', self.b._driver_deadline + 1):
            with self.assertRaisesRegex(Refused, 'same_owner'): self.consume()
        with patch.object(self.b, '_initial_driver_clock', replace(self.cap.clock)):
            with self.assertRaisesRegex(Refused, 'same_owner'): self.consume()
        with patch.object(self.owner, '_run_return', canonical({'forged': True})):
            with self.assertRaisesRegex(Refused, 'frozen_context'): self.consume()
        with patch.object(self.b, '_initial_pg_pair_pending_ownership', {'unknown': {}}):
            with self.assertRaisesRegex(Refused, 'pending_ownership'): self.consume()
        native = self.b._initial_pg_pair_standby_native
        old = native['observed_mounts']
        try:
            native['observed_mounts'] = old + [{'type': 'tmpfs', 'source': '', 'destination': '/invented', 'rw': True}]
            with self.assertRaisesRegex(Refused, 'frozen_context'): self.consume()
        finally: native['observed_mounts'] = old
        old = self.owner.preservation_baseline['page_hashes']
        try:
            self.owner.preservation_baseline['page_hashes'] = {}
            with self.assertRaisesRegex(Refused, 'frozen_context'): self.consume()
        finally: self.owner.preservation_baseline['page_hashes'] = old
        actual_groups = p.linux.process_cgroup
        def groups(pid):
            return ['0::/changed-primary'] if pid == self.owner.native[0]['process']['pid'] else actual_groups(pid)
        with patch.object(p.linux, 'process_cgroup', side_effect=groups):
            with self.assertRaisesRegex(Refused, 'primary_cgroup'): self.consume()
        self.assertEqual(self.consume(), self.result)

    def test_historical_consumer_has_zero_native_commands_or_sql_and_checks_end_after_reads(self):
        with patch.object(p._Pair, 'seal', side_effect=AssertionError('completed consumer used active native budget')), \
             patch.object(p._Pair, 'command', side_effect=AssertionError('completed consumer dispatched native command')), \
             patch.object(p._Pair, 'sql', side_effect=AssertionError('completed consumer dispatched SQL')), \
             patch.object(self.b.__class__, '_run', side_effect=AssertionError('completed consumer dispatched backend')):
            self.assertEqual(self.consume(), self.result)
        real_read = self.j.read_phase_bytes; real_monotonic = time.monotonic; expired = [False]
        def read(phase, name, sha):
            value = real_read(phase, name, sha)
            if (phase, name) == (self.ref['phase'], self.ref['name']): expired[0] = True
            return value
        def now(): return self.cap.end + 1 if expired[0] else real_monotonic()
        with patch.object(self.j, 'read_phase_bytes', side_effect=read), patch.object(p.time, 'monotonic', side_effect=now):
            with self.assertRaises(Refused): self.consume()
        self.assertEqual(self.consume(), self.result)

    def test_z_only_fixed_archived_certificate_phases_can_consume_completion(self):
        for phase in ('initial_epoch_observation', 'initial_epoch_install_verify'):
            self.j.begin(phase)
            with self.assertRaisesRegex(Refused, 'current_phase'): self.consume()
            self.assertEqual(p.consume_archived_completed_live(self.b, self.lock, self.ref), self.result)
        self.j.begin('unrelated_complete_shaped_phase')
        with self.assertRaisesRegex(Refused, 'current_phase'):
            p.consume_archived_completed_live(self.b, self.lock, self.ref)


class OuterCompletion(history.GenuinePair):
    def test_signal_restoration_error_after_genuine_inner_success_cannot_mint(self):
        s, _ = self.prepare_pair(); envelope = self.pair_envelope(s)
        real_signal = signal.signal
        originals = {n: signal.getsignal(n) for n in (signal.SIGTERM, signal.SIGINT)}
        restored = []
        def handler(number, value):
            if value is originals[number]:
                self.assertFalse(hasattr(self.b, '_initial_pg_pair_complete'))
                restored.append(number)
                if number == signal.SIGINT: raise OSError('synthetic final handler restoration failure')
            return real_signal(number, value)
        try:
            with self.pair_native_ports(), patch.object(p.signal, 'signal', side_effect=handler):
                with self.assertRaisesRegex(OSError, 'handler restoration failure'):
                    p.execute_approved_pair(s, envelope)
            owner = self.b._initial_pg_pair_live
            self.assertIsNotNone(owner._run_return)
            self.assertIsNone(owner._completed)
            self.assertFalse(hasattr(self.b, '_initial_pg_pair_complete'))
            self.assertEqual(restored, [signal.SIGTERM, signal.SIGINT])
            self.assertEqual(s.state, 'UNKNOWN')
            with self.assertRaisesRegex(Refused, 'live_field'): p.completed_reference(self.b, self.lock)
        finally:
            for number, original in originals.items(): real_signal(number, original)

    def test_cancellation_during_final_restoration_prevents_completion(self):
        s, _ = self.prepare_pair(); envelope = self.pair_envelope(s)
        real_signal = signal.signal
        originals = {n: signal.getsignal(n) for n in (signal.SIGTERM, signal.SIGINT)}
        def handler(number, value):
            if value is originals[number]:
                self.assertFalse(hasattr(self.b, '_initial_pg_pair_complete'))
                self.b._initial_pg_pair_cancelled = True
            return real_signal(number, value)
        try:
            with self.pair_native_ports(), patch.object(p.signal, 'signal', side_effect=handler):
                with self.assertRaisesRegex(Refused, 'cancelled'):
                    p.execute_approved_pair(s, envelope)
            self.assertIsNotNone(self.b._initial_pg_pair_live._run_return)
            self.assertFalse(hasattr(self.b, '_initial_pg_pair_complete'))
            self.assertEqual(s.state, 'UNKNOWN')
        finally:
            for number, original in originals.items(): real_signal(number, original)

class NamespaceMountIdentity(unittest.TestCase):
    """Real directory FDs with synthetic distinct host/native mount witnesses."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir='/tmp'); self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.host = root / 'host'; self.host.mkdir(); (self.host / 'pg_wal').mkdir()
        self.other = root / 'rebound'; self.other.mkdir(); (self.other / 'pg_wal').mkdir()
        self.data = os.open(self.host, os.O_RDONLY | os.O_DIRECTORY)
        self.wal = os.open(self.host / 'pg_wal', os.O_RDONLY | os.O_DIRECTORY)
        self.addCleanup(os.close, self.data); self.addCleanup(os.close, self.wal)
        process = {'pid':os.getpid(),'start_ticks':p.driver.processes._identity(os.getpid()),
            'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
        self.native = {'process':process,'data_fd':self.data,'wal_fd':self.wal}
        self.pair = types.SimpleNamespace(work={'host':{'boot_id':process['boot_id']}})

    def observations(self, *, rebound=False):
        opened = set(); original = p._fd_id
        def directory(pid, path):
            self.assertEqual(pid, os.getpid())
            self.assertIn(path, (p.PGDATA, p.PGDATA + '/pg_wal'))
            root = self.other if rebound else self.host
            fd = os.open(root if path == p.PGDATA else root / 'pg_wal', os.O_RDONLY | os.O_DIRECTORY)
            opened.add(fd); return fd
        def identity(fd):
            observed = list(original(fd))
            observed[3] = 901 if fd in opened else 301
            return tuple(observed)
        self.pair.b = types.SimpleNamespace(_container_directory=directory)
        return patch.object(p, '_fd_id', side_effect=identity)

    def test_same_underlying_file_with_distinct_mount_witness_reaches_namespace_validator(self):
        with self.observations(), patch.object(p.legacy, '_native_mounts') as namespace:
            p._completed_local_mounts(self.pair, self.native, p.PGDATA)
            self.assertEqual(namespace.call_count, 1)
            self.assertEqual(namespace.call_args.args[:2], (os.getpid(), p.PGDATA))

    def test_changed_underlying_inode_refuses_before_namespace_validation(self):
        with self.observations(rebound=True), patch.object(p.legacy, '_native_mounts') as namespace:
            with self.assertRaisesRegex(Refused, 'native_mount_rebound'):
                p._completed_local_mounts(self.pair, self.native, p.PGDATA)
            namespace.assert_not_called()

    def test_namespace_validator_refusal_is_not_waived_by_matching_file_identity(self):
        with self.observations(), patch.object(p.legacy, '_native_mounts', side_effect=Refused('namespace mount changed')):
            with self.assertRaisesRegex(Refused, 'namespace mount changed'):
                p._completed_local_mounts(self.pair, self.native, p.PGDATA)

class CompletionClock(unittest.TestCase):
    """Clock context guards only; these units mint no successful Pair capability."""
    def setUp(self):
        self.case = driver_tests.DriverTests('test_clock_start_and_duration_are_bounded')
        self.addCleanup(self.case.doCleanups); self.case.setUp()
        self.b = self.case.backend; self.lock = self.case.lock

    def owner(self, clock):
        return types.SimpleNamespace(b=self.b,lock=self.lock,original_clock=clock,
            original_end=self.b._driver_deadline,work={'host':{'boot_id':self.case.boot}})

    def consume_prepared(self, clock):
        self.b._driver_deadline = p.driver._consume_clock(self.b,self.lock,clock,
            self.case.configref,self.case.authref,time.monotonic())
        return self.owner(clock)

    def test_prepared_clock_created_by_current_root_is_retained(self):
        clock = self.case.prepare_clock(); owner = self.consume_prepared(clock)
        self.assertIsNone(p._completed_clock(owner))
        self.assertIs(owner.original_clock, clock)
        self.assertEqual(self.b._driver_deadline, clock.end)
        with patch.object(self.b,'_initial_driver_clock_attempted',False):
            with self.assertRaisesRegex(Refused, 'original_clock'): p._completed_clock(owner)
        with patch.object(self.b,'_initial_driver_clock_started',clock.started+1):
            with self.assertRaisesRegex(Refused, 'original_clock'): p._completed_clock(owner)
        self.assertFalse(hasattr(self.b,'_initial_pg_pair_complete'))

    def test_actual_direct_child_accepts_supervisor_clock_and_rejects_forged_creator(self):
        clock = self.case.prepare_clock(); supervisor = os.getpid()
        previous = signal.signal(signal.SIGCHLD,signal.SIG_DFL)
        pid = None; reaped = False
        try:
            pid = os.fork()
            if pid == 0:
                status = 1
                try:
                    owner = self.consume_prepared(clock)
                    p._completed_clock(owner)
                    if os.getppid() != supervisor or clock.creator_pid != supervisor:
                        raise AssertionError('not the actual direct child')
                    forged = replace(clock,creator_pid=supervisor+1000000)
                    self.b._initial_driver_clock = forged; owner.original_clock = forged
                    try:
                        p._completed_clock(owner)
                    except Refused as error:
                        status = 0 if 'clock_creator' in str(error) else 3
                    else:
                        status = 2
                except BaseException:
                    status = 4
                finally:
                    os._exit(status)
            actual, status = os.waitpid(pid,0); reaped = True
            self.assertEqual((actual,os.waitstatus_to_exitcode(status)),(pid,0))
        finally:
            if pid is not None and pid > 0 and not reaped:
                os.kill(pid,signal.SIGKILL); os.waitpid(pid,0)
            signal.signal(signal.SIGCHLD,previous)
        self.assertFalse(self.b._initial_driver_clock_attempted)
        self.assertFalse(hasattr(self.b,'_initial_pg_pair_complete'))
        self.lock.check()

    def test_legacy_unprepared_clock_is_allowed_but_never_a_preparation_downgrade(self):
        self.b._driver_deadline = time.monotonic()+30
        owner = self.owner(None)
        self.assertIsNone(p._completed_clock(owner))
        for started,attempted in ((time.monotonic(),False),(None,True),(time.monotonic(),True)):
            with patch.object(self.b,'_initial_driver_clock_started',started), \
                 patch.object(self.b,'_initial_driver_clock_attempted',attempted):
                with self.assertRaisesRegex(Refused, 'unprepared_clock'): p._completed_clock(owner)
        self.assertFalse(hasattr(self.b,'_initial_pg_pair_complete'))

    def test_prepared_clock_cannot_be_cleared_into_legacy_none(self):
        clock = self.case.prepare_clock(); owner = self.consume_prepared(clock)
        self.b._initial_driver_clock = None; owner.original_clock = None
        with self.assertRaisesRegex(Refused, 'unprepared_clock'): p._completed_clock(owner)
        self.assertFalse(hasattr(self.b,'_initial_pg_pair_complete'))


if __name__ == '__main__': unittest.main()
