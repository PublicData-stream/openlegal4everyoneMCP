"""Real harmless local process ownership tests; no Docker/SQL/network.

All data paths are temporary /tmp children. Test-only child code is a fixture,
not an operator-configurable command hook. Every cleanup targets exact known
local PIDs and start times; the command runner never sends a process-group kill.
"""
import os
import errno
import fcntl
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import command_process as c
from interface import Refused


class ProcessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='command-ownership-', dir='/tmp')
        self.addCleanup(self.tmp.cleanup); self.root = Path(self.tmp.name)
        self.fd = os.open(Path(sys.executable).resolve(), os.O_RDONLY | os.O_CLOEXEC)
        self.addCleanup(os.close, self.fd)
        self.old_subreaper = c._subreaper()
        self.old_handlers = {number: signal.getsignal(number) for number in
                             (signal.SIGINT, signal.SIGTERM, signal.SIGCHLD)}
    def tearDown(self):
        self.assertEqual(c._subreaper(), self.old_subreaper)
        self.assertEqual(c._children(os.getpid()), set())
        for number, handler in self.old_handlers.items(): self.assertEqual(signal.getsignal(number), handler)
        self.assertTrue(c._SERIAL.acquire(blocking=False)); c._SERIAL.release()
    def run_child(self, code, *, stdin=None, timeout=3, cap=1024 * 1024):
        return c.execute([sys.executable, '-c', code], executable_fd=self.fd,
            stdin=stdin, env={'PATH': '/usr/bin:/bin', 'LANG': 'C', 'TMPDIR': '/tmp',
                             'PYTHONDONTWRITEBYTECODE': '1'},
            deadline=time.monotonic() + timeout, max_output=cap)
    def assert_clean(self, result):
        self.assertTrue(result.started); self.assertTrue(result.owned_processes_closed)
        self.assertFalse(result.expired); self.assertFalse(result.output_limit_exceeded)
        self.assertFalse(result.unexpected_descendants); self.assertFalse(result.interrupted)

    def retained(self, *, child_code=None, timeout=.15, cap=4096, inject=False, observe_failure=False):
        readfd,writefd=os.pipe2(os.O_CLOEXEC)
        lock=self.root/'retained.lock';lockfd=os.open(lock,os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600)
        fcntl.flock(lockfd,fcntl.LOCK_EX|fcntl.LOCK_NB);ownerfd=os.pidfd_open(os.getpid())
        child=self.root/'retained-child.pid'
        code=("import os,signal,time,pathlib;signal.signal(signal.SIGTERM,lambda *a:exit(0));"
              "pathlib.Path("+repr(str(child))+").write_text(str(os.getpid()));"+
              (child_code or 'time.sleep(3)'))
        script=f'''import os,sys,time,json,dataclasses
sys.path.insert(0,{str(Path(c.__file__).parent)!r});import command_process as c
fd=os.open(sys.executable,os.O_RDONLY)
if {inject!r}:
 def broken(self):raise RuntimeError('injected ownership observation failure')
 c._Ownership.capture=broken
if {observe_failure!r}:
 original=c.subprocess.Popen
 actual_poll=c.select.poll
 def launch(*a,**kw):
  process=original(*a,**kw)
  def broken():
   if sys._getframe(1).f_code.co_name=='observe':raise OSError('injected permanent observation failure')
   return actual_poll()
  c.select.poll=broken
  return process
 c.subprocess.Popen=launch
try:
 result=c.execute_retained_runtime([sys.executable,'-I','-B','-c',{code!r}],executable_fd=fd,
  stdin=None,env={{'PATH':'/usr/bin','TMPDIR':'/tmp'}},dispatch_deadline=time.monotonic()+{timeout!r},
  shutdown_permission_fd={readfd},permission_token=b'fixture-stop\\n',owner_pidfd={ownerfd},
  operation_lock_fd={lockfd},max_output={cap})
 print(json.dumps(dataclasses.asdict(result),default=lambda b:b.decode('ascii','replace')))
except BaseException as error:print(json.dumps({{'failure':str(error),'children':list(c._children(os.getpid()))}}))
finally:os.close(fd)
'''
        try:
            process=subprocess.Popen([sys.executable,'-I','-B','-c',script],pass_fds=(readfd,ownerfd,lockfd),
                stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        finally:
            for fd in (readfd,ownerfd,lockfd):os.close(fd)
        end=time.monotonic()+3
        while not child.exists() and process.poll() is None and time.monotonic()<end:time.sleep(.01)
        self.assertTrue(child.exists())
        return process,writefd,int(child.read_text()),lock

    def stop_retained(self,process,writefd):
        os.write(writefd,b'fixture-stop\n');os.close(writefd)
        raw,err=process.communicate(timeout=8)
        self.assertEqual(process.returncode,0,err.decode())
        return json.loads(raw)

    def test_retained_deadline_keeps_runtime_and_inherited_lock_until_explicit_permission(self):
        process,writefd,pid,lock=self.retained()
        time.sleep(.25);self.assertIsNone(process.poll());self.assertTrue(Path('/proc/'+str(pid)).exists())
        probe=os.open(lock,os.O_RDWR)
        try:
            with self.assertRaises(BlockingIOError):fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
        finally:os.close(probe)
        result=self.stop_retained(process,writefd)
        self.assertTrue(result['expired']);self.assertTrue(result['owned_processes_closed'])
        self.assertEqual(result['actual_exit_code'],0);self.assertFalse(Path('/proc/'+str(pid)).exists())
        probe=os.open(lock,os.O_RDWR)
        try:fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
        finally:os.close(probe)

    def test_retained_normal_permission_observes_zero_and_no_error_flags(self):
        process,writefd,pid,_=self.retained(timeout=3)
        result=self.stop_retained(process,writefd)
        self.assertEqual(result['actual_exit_code'],0);self.assertTrue(result['owned_processes_closed'])
        for key in ('expired','interrupted','unexpected_descendants','output_limit_exceeded'):self.assertFalse(result[key])
        self.assertFalse(Path('/proc/'+str(pid)).exists())

    def test_retained_overflow_and_sigterm_do_not_bypass_permission(self):
        process,writefd,pid,_=self.retained(child_code='\nwhile True:os.write(1,b"x"*65536)',timeout=3,cap=128)
        process.send_signal(signal.SIGTERM);time.sleep(.15)
        self.assertIsNone(process.poll());self.assertTrue(Path('/proc/'+str(pid)).exists())
        result=self.stop_retained(process,writefd)
        self.assertTrue(result['interrupted']);self.assertTrue(result['output_limit_exceeded'])
        self.assertLessEqual(len(result['stdout'])+len(result['stderr']),128)
        self.assertEqual(result['actual_exit_code'],0)

    def test_retained_exception_finally_waits_for_permission_before_drain(self):
        process,writefd,pid,_=self.retained(inject=True,timeout=3)
        time.sleep(.15);self.assertTrue(Path('/proc/'+str(pid)).exists());self.assertIsNone(process.poll())
        result=self.stop_retained(process,writefd)
        self.assertEqual(result,{'failure':'injected ownership observation failure','children':[]})
        self.assertFalse(Path('/proc/'+str(pid)).exists())

    def test_retained_eof_or_malformed_token_never_kills_a_live_runtime(self):
        process,writefd,pid,_=self.retained(child_code='time.sleep(.65)',timeout=3)
        os.write(writefd,b'not-the-token\n');os.close(writefd);time.sleep(.15)
        self.assertIsNone(process.poll());self.assertTrue(Path('/proc/'+str(pid)).exists())
        raw,err=process.communicate(timeout=4);self.assertEqual(process.returncode,0,err.decode())
        result=json.loads(raw);self.assertTrue(result['interrupted']);self.assertEqual(result['actual_exit_code'],0)
        self.assertFalse(Path('/proc/'+str(pid)).exists())

    def test_retained_plain_eof_is_not_stop_permission(self):
        process,writefd,pid,_=self.retained(child_code='time.sleep(.55)',timeout=3)
        os.close(writefd);time.sleep(.12)
        self.assertIsNone(process.poll());self.assertTrue(Path('/proc/'+str(pid)).exists())
        raw,err=process.communicate(timeout=4);self.assertEqual(process.returncode,0,err.decode())
        result=json.loads(raw);self.assertTrue(result['interrupted']);self.assertEqual(result['actual_exit_code'],0)

    def test_retained_permanent_observation_error_allows_only_proven_natural_closure(self):
        process,writefd,pid,_=self.retained(child_code='time.sleep(.55)',timeout=3,observe_failure=True)
        time.sleep(.12);self.assertIsNone(process.poll());self.assertTrue(Path('/proc/'+str(pid)).exists())
        os.close(writefd)
        raw,err=process.communicate(timeout=4);self.assertEqual(process.returncode,0,err.decode())
        self.assertEqual(json.loads(raw),{'failure':'injected permanent observation failure','children':[]})
        self.assertFalse(Path('/proc/'+str(pid)).exists())

    def test_retained_crashed_owner_keeps_native_runtime_until_its_actual_natural_exit(self):
        # Become the next subreaper only for this owned owner->worker fixture.
        # Owner exits normally; the worker must retain its native runtime rather
        # than treating owner-pidfd readiness or pipe EOF as shutdown permission.
        output=self.root/'orphan-result.json';child=self.root/'orphan-child.pid';worker=self.root/'orphan-worker.pid'
        readfd,writefd=os.pipe2(os.O_CLOEXEC)
        lockfd=os.open(self.root/'orphan.lock',os.O_RDWR|os.O_CREAT|os.O_EXCL,0o600)
        fcntl.flock(lockfd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        code="import os,time,pathlib;pathlib.Path("+repr(str(child))+").write_text(str(os.getpid()));time.sleep(.8)"
        worker_code=f'''import sys,os,time,json,dataclasses
sys.path.insert(0,{str(Path(c.__file__).parent)!r});import command_process as c
fd=os.open(sys.executable,os.O_RDONLY)
r=c.execute_retained_runtime([sys.executable,'-I','-B','-c',{code!r}],executable_fd=fd,stdin=None,
 env={{'PATH':'/usr/bin'}},dispatch_deadline=time.monotonic()+3,shutdown_permission_fd={readfd},
 permission_token=b'fixture-stop\\n',owner_pidfd=int(sys.argv[1]),operation_lock_fd={lockfd},max_output=4096)
print(json.dumps(dataclasses.asdict(r),default=lambda b:b.decode()))
'''
        owner_code=f'''import subprocess,os,sys,time,pathlib
fd=os.pidfd_open(os.getpid())
out=open({str(output)!r},'wb')
p=subprocess.Popen([sys.executable,'-I','-B','-c',{worker_code!r},str(fd)],pass_fds=({readfd},{lockfd},fd),stdout=out,stderr=out)
pathlib.Path({str(worker)!r}).write_text(str(p.pid))
end=time.monotonic()+3
while not pathlib.Path({str(child)!r}).exists() and time.monotonic()<end:time.sleep(.01)
os.kill(os.getpid(),9)
'''
        c._subreaper(1)
        try:
            owner=subprocess.Popen([sys.executable,'-I','-B','-c',owner_code],pass_fds=(readfd,lockfd))
            os.close(readfd);os.close(lockfd)
            self.assertEqual(owner.wait(timeout=4),-9);self.assertTrue(child.exists())
            os.close(writefd)
            native=int(child.read_text());adopted=int(worker.read_text())
            time.sleep(.1);self.assertTrue(Path('/proc/'+str(native)).exists())
            self.assertTrue(Path('/proc/'+str(adopted)).exists())
            end=time.monotonic()+4
            while True:
                found,status=os.waitpid(adopted,os.WNOHANG)
                if found:break
                self.assertLess(time.monotonic(),end);time.sleep(.02)
            self.assertEqual(os.waitstatus_to_exitcode(status),0)
            result=json.loads(output.read_bytes());self.assertTrue(result['interrupted'])
            self.assertEqual(result['actual_exit_code'],0);self.assertTrue(result['owned_processes_closed'])
            self.assertFalse(Path('/proc/'+str(native)).exists())
        finally:c._subreaper(self.old_subreaper)
    def test_actual_nonzero_leader_status_is_not_guessed_zero(self):
        result = self.run_child("import sys; print('out'); print('err',file=sys.stderr);sys.exit(23)")
        self.assert_clean(result); self.assertEqual(result.actual_exit_code, 23)
        self.assertEqual(result.stdout, b'out\n'); self.assertEqual(result.stderr, b'err\n')
    def test_real_stdin_and_two_output_streams(self):
        data = b'finite\0body' * 20000
        result = self.run_child('import sys; x=sys.stdin.buffer.read();sys.stdout.buffer.write(x);sys.stderr.write("ok")', stdin=data)
        self.assert_clean(result); self.assertEqual(result.actual_exit_code, 0)
        self.assertEqual(result.stdout, data); self.assertEqual(result.stderr, b'ok')
    def test_expired_deadline_never_dispatches(self):
        sentinel = self.root / 'not-created'
        with patch.object(c.subprocess, 'Popen', wraps=c.subprocess.Popen) as launch:
            result = c.execute([sys.executable, '-c', f'open({str(sentinel)!r},"w").close()'],
                executable_fd=self.fd, stdin=None, env={'PATH': '/usr/bin:/bin'},
                deadline=time.monotonic() - 1)
        self.assertFalse(result.started); self.assertTrue(result.expired)
        self.assertIsNone(result.actual_exit_code); launch.assert_not_called(); self.assertFalse(sentinel.exists())
    def test_preexisting_child_is_never_adopted_signalled_or_reaped(self):
        process = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(30)'])
        try:
            with self.assertRaisesRegex(Refused, 'preexisting_children'): self.run_child('pass')
            self.assertIsNone(process.poll())
        finally: process.terminate(); process.wait(timeout=5)
    def test_non_main_thread_and_concurrent_gate_refuse_before_launch(self):
        errors = []
        thread = threading.Thread(target=lambda: self.thread_call(errors)); thread.start(); thread.join()
        self.assertEqual(errors, ['command_process_dedicated_main_thread'])
        self.assertTrue(c._SERIAL.acquire(blocking=False))
        try:
            with self.assertRaisesRegex(Refused, 'concurrent_call'): self.run_child('pass')
        finally: c._SERIAL.release()
    def thread_call(self, errors):
        try: self.run_child('pass')
        except Refused as error: errors.append(str(error))
    def test_ignored_sigchld_cannot_turn_real_failure_into_success(self):
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        try:
            result = self.run_child('import sys;sys.exit(17)')
            self.assertEqual(result.actual_exit_code, 17)
            self.assertEqual(signal.getsignal(signal.SIGCHLD), signal.SIG_IGN)
        finally: signal.signal(signal.SIGCHLD, self.old_handlers[signal.SIGCHLD])
    def test_setsid_grandchild_is_reaped_and_makes_zero_leader_fail(self):
        path = self.root / 'escaped'
        code = ('import os,time,pathlib; p=os.fork();\n'
            'if p==0:\n os.setsid();pathlib.Path(' + repr(str(path)) + ').write_text(str(os.getpid()));time.sleep(30);os._exit(0)\n'
            'else:\n time.sleep(.08);os._exit(0)\n')
        result = self.run_child(code)
        self.assertEqual(result.actual_exit_code, 0)
        self.assertTrue(result.owned_processes_closed); self.assertTrue(result.unexpected_descendants)
        self.assertFalse(result.expired)
        pid = int(path.read_text()); self.assertFalse(Path('/proc/' + str(pid)).exists())
    def test_double_fork_closed_stdio_cannot_escape_ownership(self):
        path = self.root / 'double-escaped'
        code = ('import os,time,pathlib; p=os.fork();\n'
            'if p==0:\n os.setsid();q=os.fork()\n'
            ' if q==0:\n  os.close(0);os.close(1);os.close(2);pathlib.Path(' + repr(str(path)) + ').write_text(str(os.getpid()));time.sleep(30);os._exit(0)\n'
            ' else:\n  time.sleep(.08);os._exit(0)\n'
            'else:\n time.sleep(.16);os._exit(7)\n')
        result = self.run_child(code)
        self.assertEqual(result.actual_exit_code, 7); self.assertTrue(result.unexpected_descendants)
        self.assertTrue(result.owned_processes_closed)
        self.assertFalse(Path('/proc/' + path.read_text()).exists())
    def test_timeout_owns_blocked_fifo_reader(self):
        fifo = self.root / 'blocking'; os.mkfifo(fifo, 0o600)
        result = self.run_child('import os;os.open(' + repr(str(fifo)) + ',os.O_RDONLY)', timeout=.15)
        self.assertTrue(result.expired); self.assertTrue(result.owned_processes_closed)
        self.assertLess(result.actual_exit_code, 0)
    def test_unbounded_body_is_capped_and_child_closed(self):
        result = self.run_child('import os;\nwhile True: os.write(1,b"x"*65536)', cap=128)
        self.assertTrue(result.output_limit_exceeded); self.assertTrue(result.owned_processes_closed)
        self.assertLessEqual(len(result.stdout), 128)
    def test_malformed_text_is_bytes_for_the_fixed_consumer_to_reject(self):
        result = self.run_child('import os;os.write(1,bytes([255,0,254]))')
        self.assert_clean(result); self.assertEqual(result.actual_exit_code, 0)
        self.assertEqual(result.stdout, bytes([255,0,254]))
        with self.assertRaises(UnicodeDecodeError): result.stdout.decode('utf-8')
    def test_parent_sigterm_drains_before_reporting_interrupted(self):
        result = self.run_child('import os,signal,time;os.kill(os.getppid(),signal.SIGTERM);time.sleep(30)')
        self.assertTrue(result.interrupted); self.assertTrue(result.owned_processes_closed)
        self.assertLess(result.actual_exit_code, 0)
    def test_sigterm_ignoring_child_requires_actual_kill_and_reap(self):
        start = time.monotonic()
        result = self.run_child('import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(30)', timeout=.2)
        self.assertTrue(result.expired); self.assertTrue(result.owned_processes_closed)
        self.assertEqual(result.actual_exit_code, -signal.SIGKILL)
        self.assertGreaterEqual(time.monotonic() - start, c.TERM_GRACE)
    def test_leader_is_reaped_only_by_popen_poll_not_waitpid_minus_one(self):
        original = os.waitpid; seen = []
        def checked(pid, flags):
            seen.append(pid); self.assertNotEqual(pid, -1); return original(pid, flags)
        with patch.object(c.os, 'waitpid', side_effect=checked):
            result = self.run_child('import os,time; p=os.fork();\nif p==0: os._exit(17)\nelse: time.sleep(.1);os._exit(29)')
        self.assertEqual(result.actual_exit_code, 29); self.assertTrue(seen)
    def test_setup_exception_still_drains_and_restores_state(self):
        original = c.selectors.DefaultSelector
        class FailingSelector:
            def __init__(self): self.real = original()
            def register(self, *a): raise RuntimeError('synthetic-pipe-setup')
            def close(self): self.real.close()
        with patch.object(c.selectors, 'DefaultSelector', FailingSelector):
            with self.assertRaisesRegex(RuntimeError, 'synthetic-pipe-setup'):
                self.run_child('import time;time.sleep(30)')
    def test_selector_constructor_failure_releases_serial_before_any_child(self):
        with patch.object(c.selectors, 'DefaultSelector', side_effect=OSError('synthetic-constructor')):
            with self.assertRaisesRegex(OSError, 'synthetic-constructor'): self.run_child('pass')
        self.assertTrue(c._SERIAL.acquire(blocking=False)); c._SERIAL.release()
    def test_overlimit_discovery_still_physically_closes_every_owned_child(self):
        pids = self.root / 'overlimit-pids'
        code = ('import os,time; f=os.open(' + repr(str(pids)) + ',os.O_CREAT|os.O_WRONLY|os.O_APPEND,0o600);'
            'os.write(f,(str(os.getpid())+"\\n").encode());\n'
            'for _ in range(3):\n p=os.fork()\n if p==0:\n  os.setsid();os.write(f,(str(os.getpid())+"\\n").encode());time.sleep(30);os._exit(0)\n'
            'time.sleep(30)\n')
        with patch.object(c, 'MAX_CHILDREN', 2):
            with self.assertRaises(Refused): self.run_child(code)
        values = [int(x) for x in pids.read_text().split()]
        self.assertGreaterEqual(len(values), 3)
        self.assertTrue(all(not Path('/proc/' + str(pid)).exists() for pid in values))
        self.assertEqual(c._children(os.getpid()), set())
    def test_cumulative_reaped_descendant_pidfds_are_retired(self):
        class Leader:
            pid = 100
            returncode = None
            def poll(self): return None
        class Entry:
            def __init__(self, pid, ticks): self.pid=pid;self.ticks=ticks;self.fd=pid
            def alive(self): return False
        ownership = object.__new__(c._Ownership)
        ownership.leader=Leader();ownership.leader_key=(100,'1');ownership.entries={};ownership.reaped=set()
        closed=[]
        with patch.object(c.os, 'close', side_effect=lambda fd: closed.append(fd)), \
             patch.object(c, '_identity', return_value=None):
            for pid in range(200, 1200):
                ownership.entries[(pid,'1')]=Entry(pid,'1')
                ownership.reaped.add((pid,'1'));ownership.retire()
                self.assertEqual(ownership.entries,{})
                self.assertEqual(ownership.reaped,set())
        self.assertEqual(len(closed),1000)
    def test_tracking_fd_cap_refusal_also_uses_uncapped_physical_cleanup(self):
        with patch.object(c, 'MAX_PIDFDS', 1):
            with self.assertRaisesRegex(Refused, 'retained_pidfd_bound'):
                self.run_child('import os,time;p=os.fork();time.sleep(30)')
        self.assertEqual(c._children(os.getpid()),set())
    def test_many_successive_real_children_do_not_accumulate_pidfds(self):
        code = ('import os,time;\nfor _ in range(50):\n'
            ' p=os.fork()\n if p==0: time.sleep(.02);os._exit(0)\n os.waitpid(p,0)\n')
        with patch.object(c, 'MAX_PIDFDS', 3): result = self.run_child(code, timeout=5)
        self.assert_clean(result); self.assertEqual(result.actual_exit_code, 0)
    def test_proc_identity_disappearing_during_read_is_absent_without_masking_io_errors(self):
        for error in (FileNotFoundError(), ProcessLookupError(errno.ESRCH, 'gone')):
            with self.subTest(error=type(error).__name__), patch.object(Path, 'read_text', side_effect=error):
                self.assertIsNone(c._identity(123))
        for error in (PermissionError(), OSError(errno.EIO, 'io')):
            with self.subTest(error=type(error).__name__), patch.object(Path, 'read_text', side_effect=error):
                with self.assertRaises(type(error)): c._identity(123)
    def test_proc_children_disappearing_during_enumeration_or_read_is_absent(self):
        with patch.object(Path, 'iterdir', side_effect=ProcessLookupError(errno.ESRCH, 'gone')):
            self.assertEqual(c._children(123), set())
        with patch.object(Path, 'iterdir', return_value=iter([Path('/proc/123/task/123')])), \
             patch.object(Path, 'read_text', side_effect=ProcessLookupError(errno.ESRCH, 'gone')):
            self.assertEqual(c._children(123), set())
        with patch.object(Path, 'iterdir', return_value=iter([Path('/proc/123/task/123')])), \
             patch.object(Path, 'read_text', side_effect=PermissionError()):
            with self.assertRaises(PermissionError): c._children(123)
    def test_real_pidfds_above_select_fdset_limit_are_supported(self):
        import resource
        if resource.getrlimit(resource.RLIMIT_NOFILE)[0] < 2048: self.skipTest('test requires high FD numbers')
        original = os.pidfd_open
        def high(pid, flags):
            fd = original(pid, flags)
            try: return fcntl.fcntl(fd, fcntl.F_DUPFD_CLOEXEC, 1100)
            finally: os.close(fd)
        with patch.object(c.os, 'pidfd_open', side_effect=high): result = self.run_child('pass')
        self.assert_clean(result); self.assertEqual(result.actual_exit_code, 0)
    def test_wrong_types_and_nonfinite_deadlines_refuse(self):
        for value in (float('inf'), float('nan'), True, '1'):
            with self.subTest(value=value), self.assertRaises(Refused):
                c.execute([sys.executable, '-c', 'pass'], executable_fd=self.fd,
                    stdin=None, env={'PATH': '/usr/bin:/bin'}, deadline=value)
    def test_fd_count_is_stable_across_many_actual_commands(self):
        before = len(list(Path('/proc/self/fd').iterdir()))
        for _ in range(12): self.assert_clean(self.run_child('pass'))
        self.assertEqual(len(list(Path('/proc/self/fd').iterdir())), before)


if __name__ == '__main__': unittest.main()
