"""Real private /tmp writes and harmless forks; no native operation acceptance.

The fixture bypasses ONLY the physical constructor's distinct-device gate.
Both legs are deliberately synthetic directories on the same filesystem.
"""
import copy
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import journal as journals
from interface import Refused


@unittest.skipUnless(os.geteuid()==0,'root-private journal fixture')
class JournalBudgetTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='journal-budget-synthetic-',dir='/tmp')
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.j=self.fixture('first')

    def fixture(self,label):
        root=self.root/label;root.mkdir(mode=0o700)
        j=journals.DualJournal.__new__(journals.DualJournal)
        j.base=[];j.paths=[];j.operation='synthetic-operation';j.binding={'synthetic':True}
        j.phase_paths=None;j.bytes=0;j.count=0;j.artifacts={};j.history=[];j.phase_names=set()
        for name in ('synthetic-a','synthetic-b'):
            base=root/name;base.mkdir(mode=0o700);j.base.append((base,journals.directory(base)))
            op=base/j.operation;j.paths.append((op,journals.new_directory(op)))
        j.save('operation.json',j.binding);j.begin('role-prepared')
        return j

    def reserve(self,records=4,byte_limit=16384,label='maintenance-input'):
        return self.j.reserve(label,records,byte_limit)

    def files(self):
        return [{p.name:p.read_bytes() for p in path.iterdir()} for path,_ in self.j.current()]

    def assert_poisoned(self,token):
        self.assertIs(self.j._reservation,token)
        self.assertTrue(self.j._reservation_poisoned)
        for action in (lambda:self.j.check(),
                       lambda:self.j.finish_reservation(token),
                       lambda:self.j.save_bytes('later.json',b'{}',reservation=token),
                       lambda:self.j.save('ordinary.json',{}),
                       lambda:self.j.begin('maintenance'),
                       lambda:self.j.reserve('retry',4,16384)):
            with self.assertRaisesRegex(Refused,'reservation_poisoned'):action()

    def test_legacy_new_fixture_and_ordinary_calls_need_no_reservation_fields(self):
        self.assertFalse(hasattr(self.j,'_reservation'))
        count,size=self.j.count,self.j.bytes
        sha=self.j.save_bytes('ordinary.json',b'{}')
        self.assertEqual((self.j.count,self.j.bytes),(count+1,size+2))
        self.assertEqual(self.j.read_phase_bytes('role-prepared','ordinary.json',sha),b'{}')
        self.j.begin('next-phase');self.j.check()

    def test_success_counts_both_metadata_records_and_preserves_actual_counters(self):
        before=self.j.count,self.j.bytes
        token=self.reserve()
        self.j.check()  # Read-only validation remains available inside the scope.
        sha=self.j.save('candidate.json',{'approved_input':False},reservation=token)
        self.assertEqual(self.j.read_phase_bytes('role-prepared','candidate.json',sha),b'{"approved_input":false}\n')
        ref=self.j.finish_reservation(token)
        self.assertEqual(set(ref),{'phase','name','sha256'})
        self.assertEqual(ref['phase'],'role-prepared')
        self.assertEqual(self.j.count,before[0]+3)
        added=sum(row['bytes'] for name,row in self.j.artifacts.items() if name!='intent.json')
        self.assertEqual(self.j.bytes,before[1]+added)
        self.assertIsNone(self.j._reservation)
        self.assertEqual(self.files()[0],self.files()[1])
        self.assertLessEqual(len(token.begin_raw),4096)
        self.assertLessEqual(len(token.finish_raw),4096)
        self.assertEqual(self.j.read_phase_bytes(ref['phase'],ref['name'],ref['sha256']),token.finish_raw)
        # Unused LOCAL input allowance releases; actual ACK counts never reset.
        actual=self.j.count,self.j.bytes
        self.j.begin('maintenance')
        self.assertEqual(self.j.count,actual[0]+1)
        self.assertGreater(self.j.bytes,actual[1])

    def test_two_record_allowance_can_only_begin_and_finish(self):
        token=self.reserve(records=2)
        self.j.finish_reservation(token)
        self.assertEqual(self.j.count,4)

    def test_record_limit_preserves_terminal_capacity_before_creating_data(self):
        token=self.reserve(records=3)
        self.j.save_bytes('one.json',b'{}',reservation=token)
        before=self.files();counts=self.j.count,self.j.bytes
        with self.assertRaisesRegex(Refused,'reservation_bound'):
            self.j.save_bytes('two.json',b'{}',reservation=token)
        self.assertEqual(self.files(),before)
        self.assertEqual((self.j.count,self.j.bytes),counts)
        self.assert_poisoned(token)

    def test_stream_size_allowance_includes_exact_terminal_bytes(self):
        token=self.reserve(records=3,byte_limit=4096)
        available=token.byte_limit-len(token.begin_raw)-len(token.finish_raw)
        self.j.save_bytes('exact.bin',b'x'*available,reservation=token)
        before=self.j.bytes
        self.j.finish_reservation(token)
        self.assertEqual(self.j.bytes,before+len(token.finish_raw))
        self.assertEqual(self.j.bytes-token.start_bytes,4096)

    def test_one_byte_over_allowance_refuses_before_any_data_file(self):
        token=self.reserve(records=3,byte_limit=4096)
        available=token.byte_limit-len(token.begin_raw)-len(token.finish_raw)
        before=self.files()
        with self.assertRaisesRegex(Refused,'reservation_bound'):
            self.j.save_bytes('over.bin',b'x'*(available+1),reservation=token)
        self.assertEqual(self.files(),before)
        self.assert_poisoned(token)

    def test_invalid_scope_arguments_never_write(self):
        invalid=[(None,4,16384),('',4,16384),('../input',4,16384),('a'*129,4,16384),
                 ('input',True,16384),('input',1,16384),('input',4097,16384),
                 ('input',4,True),('input',4,-1),('input',4,journals.MAX_PHASE_BYTES+1),
                 ('input',2,0)]
        before=self.files()
        with patch.object(journals,'new_file',wraps=journals.new_file) as write:
            for args in invalid:
                with self.subTest(args=args),self.assertRaises(Refused):self.j.reserve(*args)
            self.assertEqual(write.call_count,0)
        self.assertEqual(self.files(),before)

    def test_global_record_and_byte_capacity_are_checked_before_begin_receipt(self):
        for count,size,records,amount in ((4095,0,2,16384),(0,journals.MAX_PHASE_BYTES-1023,2,1024)):
            with self.subTest(count=count,size=size):
                self.j.count=count;self.j.bytes=size
                before=self.files()
                with self.assertRaisesRegex(Refused,'reservation_capacity'):self.reserve(records,amount)
                self.assertEqual(self.files(),before)
                self.assertIsNone(getattr(self.j,'_reservation',None))

    def test_metadata_over_4096_bytes_is_rejected_before_any_write(self):
        self.j.operation='synthetic-'+('x'*4096)
        before=self.files()
        with patch.object(journals,'new_file',wraps=journals.new_file) as write:
            with self.assertRaisesRegex(Refused,'reservation_overhead'):self.reserve()
            self.assertEqual(write.call_count,0)
        self.assertEqual(self.files(),before)

    def test_malformed_data_is_refused_before_io_and_poisons_active_scope(self):
        for index,raw in enumerate((None,True,bytearray(b'{}'))):
            self.j=self.fixture('malformed-'+str(index));token=self.reserve();before=self.files()
            with self.subTest(raw=raw),self.assertRaisesRegex(Refused,'artifact_bound'):
                self.j.save_bytes('bad.json',raw,reservation=token)
            self.assertEqual(self.files(),before)
            self.assert_poisoned(token)

    def test_global_record_boundary_does_not_reset_after_finish(self):
        self.j.count=4094
        token=self.reserve(records=2)
        self.j.finish_reservation(token)
        self.assertEqual(self.j.count,4096)
        with self.assertRaisesRegex(Refused,'artifact_bound'):self.j.save('overflow.json',{})

    def test_ordinary_save_is_refused_before_writes_and_poison_is_sticky(self):
        token=self.reserve();before=self.files()
        with patch.object(journals,'new_file',wraps=journals.new_file) as write:
            with self.assertRaisesRegex(Refused,'reservation_token'):self.j.save('ordinary.json',{})
            self.assertEqual(write.call_count,0)
        self.assertEqual(self.files(),before)
        self.assert_poisoned(token)

    def test_begin_is_refused_before_mkdir_and_cannot_escape_the_scope(self):
        token=self.reserve();before=self.files()
        with patch.object(journals,'new_directory',wraps=journals.new_directory) as mkdir:
            with self.assertRaisesRegex(Refused,'reservation_active'):self.j.begin('escape')
            self.assertEqual(mkdir.call_count,0)
        self.assertEqual(self.files(),before)
        self.assert_poisoned(token)

    def test_nested_reservation_cannot_replace_the_original(self):
        token=self.reserve()
        with self.assertRaisesRegex(Refused,'reservation_active'):self.j.reserve('nested',3,16384)
        self.assert_poisoned(token)

    def test_foreign_copied_dictionary_and_missing_tokens_do_not_write(self):
        other=self.fixture('other');foreign=other.reserve('other',4,16384)
        for index,make_token in enumerate((lambda t:foreign,copy.copy,lambda t:{'label':t.label},lambda t:None)):
            self.j=self.fixture('token-'+str(index));token=self.reserve();before=self.files()
            with self.subTest(index=index),self.assertRaisesRegex(Refused,'reservation_token'):
                self.j.save_bytes('forged.json',b'{}',reservation=make_token(token))
            self.assertEqual(self.files(),before)
            self.assert_poisoned(token)

    def test_closed_label_and_token_are_single_use(self):
        token=self.reserve();self.j.finish_reservation(token)
        before=self.files()
        with self.assertRaisesRegex(Refused,'reservation_label'):self.reserve()
        with self.assertRaisesRegex(Refused,'reservation_token'):self.j.finish_reservation(token)
        with self.assertRaisesRegex(Refused,'reservation_token'):
            self.j.save_bytes('replay.json',b'{}',reservation=token)
        self.assertEqual(self.files(),before)
        self.j.save('ordinary-after.json',{})

    def test_caller_cannot_write_begin_or_terminal_metadata_through_public_save(self):
        token=self.reserve();before=self.files()
        with self.assertRaisesRegex(Refused,'reservation_metadata'):
            self.j.save_bytes(token.finish_name,token.finish_raw,reservation=token)
        self.assertEqual(self.files(),before)
        self.assert_poisoned(token)

    def test_failed_json_encoding_poisons_without_writing(self):
        token=self.reserve();before=self.files()
        with self.assertRaises(TypeError):self.j.save('bad.json',{'bad':object()},reservation=token)
        self.assertEqual(self.files(),before)
        self.assert_poisoned(token)

    def test_explicit_validation_failure_burns_scope_without_a_terminal(self):
        token=self.reserve();before=self.files()
        self.j.fail_reservation(token)
        self.assertEqual(self.files(),before)
        self.assert_poisoned(token)

    def test_readback_check_failure_poisons_even_after_files_are_restored(self):
        token=self.reserve()
        path=self.j.current()[1][0]/token.begin_name
        original=path.read_bytes();path.write_bytes(b'x'*len(original))
        with self.assertRaises(Refused):self.j.check()
        path.write_bytes(original)
        self.assert_poisoned(token)

    def test_phase_or_counter_rebinding_poisons_without_new_files(self):
        for index,field in enumerate(('phase_paths','count','bytes')):
            self.j=self.fixture('rebind-'+str(index));token=self.reserve()
            if field=='phase_paths':self.j.phase_paths=list(reversed(self.j.phase_paths))
            else:setattr(self.j,field,getattr(self.j,field)+1)
            with self.subTest(field=field),self.assertRaisesRegex(Refused,'phase_or_accounting'):self.j.check()
            self.assert_poisoned(token)

    def second_leg_fault(self,name):
        original=journals.new_file;second=self.j.current()[1][0]
        def write(path,raw):
            if path.parent==second and path.name==name:raise OSError('synthetic second-leg failure')
            return original(path,raw)
        return patch.object(journals,'new_file',side_effect=write)

    def test_second_leg_data_failure_retains_first_file_without_ack_or_refund(self):
        token=self.reserve();before=self.j.count,self.j.bytes
        with self.second_leg_fault('partial.json'):
            with self.assertRaisesRegex(OSError,'second-leg'):self.j.save_bytes('partial.json',b'{}',reservation=token)
        self.assertEqual((self.j.count,self.j.bytes),before)
        self.assertNotIn('partial.json',self.j.artifacts)
        self.assertEqual((self.j.current()[0][0]/'partial.json').read_bytes(),b'{}')
        self.assertFalse((self.j.current()[1][0]/'partial.json').exists())
        self.assert_poisoned(token)

    def test_failed_begin_keeps_the_installed_scope_and_partial_receipt(self):
        before=self.j.count,self.j.bytes
        name='reservation-'+journals.digest(b'maintenance-input')+'-begin.json'
        with self.second_leg_fault(name):
            with self.assertRaisesRegex(OSError,'second-leg'):self.reserve()
        token=self.j._reservation
        self.assertEqual((self.j.count,self.j.bytes),before)
        self.assertTrue((self.j.current()[0][0]/name).exists())
        self.assert_poisoned(token)

    def test_failed_terminal_second_ack_never_releases_capacity(self):
        token=self.reserve();before=self.j.count,self.j.bytes
        with self.second_leg_fault(token.finish_name):
            with self.assertRaisesRegex(OSError,'second-leg'):self.j.finish_reservation(token)
        self.assertEqual((self.j.count,self.j.bytes),before)
        self.assertNotIn(token.finish_name,self.j.artifacts)
        self.assertTrue((self.j.current()[0][0]/token.finish_name).exists())
        self.assert_poisoned(token)

    def test_final_check_failure_after_both_acks_keeps_ack_counts_and_poisoned_scope(self):
        token=self.reserve();before=self.j.count,self.j.bytes
        original=self.j._check
        def check():
            original()
            if token.finish_name in self.j.artifacts:raise Refused('synthetic final check failure')
        with patch.object(self.j,'_check',side_effect=check):
            with self.assertRaisesRegex(Refused,'final check'):self.j.finish_reservation(token)
        self.assertEqual((self.j.count,self.j.bytes),(before[0]+1,before[1]+len(token.finish_raw)))
        self.assertTrue(all((path/token.finish_name).read_bytes()==token.finish_raw for path,_ in self.j.current()))
        self.assert_poisoned(token)

    def test_fsync_failure_after_scope_activation_keeps_charge_and_files(self):
        token=self.reserve();before=self.j.count,self.j.bytes
        with patch.object(journals.os,'fsync',side_effect=OSError('synthetic fsync failure')):
            with self.assertRaisesRegex(OSError,'fsync'):self.j.save_bytes('unsynced.json',b'{}',reservation=token)
        self.assertEqual((self.j.count,self.j.bytes),before)
        self.assertTrue((self.j.current()[0][0]/'unsynced.json').exists())
        self.assert_poisoned(token)

    def test_foreign_pid_cannot_perform_ordinary_or_reserved_writes(self):
        # This proves rejection before child IO, not a shared cross-fork poison.
        for reserved in (False,True):
            self.j=self.fixture('fork-'+str(reserved));token=self.reserve();before=self.files()
            pid=os.fork()
            if pid==0:
                status=1
                try:
                    try:self.j.save_bytes('child.json',b'{}',reservation=token if reserved else None)
                    except Refused:status=0
                finally:os._exit(status)
            end=time.monotonic()+3;status=None
            try:
                while time.monotonic()<end:
                    actual,value=os.waitpid(pid,os.WNOHANG)
                    if actual==pid:status=value;break
                    time.sleep(0.01)
                self.assertIsNotNone(status,'harmless child exceeded bounded wait')
                self.assertTrue(os.WIFEXITED(status));self.assertEqual(os.WEXITSTATUS(status),0)
            finally:
                if status is None:
                    os.kill(pid,signal.SIGKILL);os.waitpid(pid,0)
            self.assertEqual(self.files(),before)
            self.j.finish_reservation(token)


if __name__=='__main__':unittest.main()
