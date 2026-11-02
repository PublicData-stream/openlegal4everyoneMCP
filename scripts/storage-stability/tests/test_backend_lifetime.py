"""Metadata FD teardown order only: synthetic backend and /tmp shared locks."""
import fcntl
import os
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import checkpoint,interface
import test_storage_stability as base


class BackendLifetimeTests(base.Fixture):
    prepared=base.WorkflowTests.prepared
    roots=base.JournalTests.roots
    fake_independent=base.JournalTests.fake_independent
    fake_backend=base.WorkflowTests.fake_backend
    def run_fixture(self,*,fail=None,close_failure=False):
        inv,authority,ip,ih,ap,ah,op,actors,devices=self.prepared()
        modules,_=self.fake_backend(fail=fail);parent=modules['linux_backend'].LinuxBackend;events=[]
        class Backend(parent):
            def close(inner):
                fd=os.open(inv['shared_lock']['path'],os.O_RDWR|os.O_CLOEXEC)
                try:
                    try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    except BlockingIOError:events.append('close-under-operation-lock')
                    else:raise AssertionError('backend FD teardown after lock release')
                finally:os.close(fd)
                if close_failure:raise OSError('synthetic retained FD close failure')
        modules['linux_backend'].LinuxBackend=Backend
        self.events=events;self.inv=inv
        with devices:return checkpoint.execute(modules,inv,authority,ip,ih,ap,ah,op,'checkpoint',actors)
    def test_success_backend_close_occurs_once_before_shared_lock_release(self):
        value=self.run_fixture();self.assertEqual(value['status'],'completed_pending_actual_operator_exit')
        self.assertEqual(self.events,['close-under-operation-lock'])
    def test_backend_closes_on_original_phase_failure_and_preserves_that_error(self):
        with self.assertRaisesRegex(interface.Refused,'synthetic_backend_failed'):self.run_fixture(fail='stop',close_failure=True)
        self.assertEqual(self.events,['close-under-operation-lock'])
        fd=os.open(self.inv['shared_lock']['path'],os.O_RDWR|os.O_CLOEXEC)
        try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        finally:os.close(fd)
    def test_close_failure_refuses_success_result(self):
        with self.assertRaisesRegex(OSError,'synthetic retained FD'):self.run_fixture(close_failure=True)
        self.assertEqual(self.events,['close-under-operation-lock'])


if __name__=='__main__':unittest.main()
