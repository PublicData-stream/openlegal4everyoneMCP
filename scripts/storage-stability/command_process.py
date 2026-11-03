"""One fixed CLI, with Linux subreaper ownership beyond process groups.

Only a dedicated single-threaded operator process may call execute(). The caller
pins executable bytes, supplies its held executable FD and an ORIGINAL absolute
deadline. This module does not authorize commands or log raw output. Escaped
setsid descendants are adopted, identified with pidfds, signalled individually
and reaped. A lingering descendant makes the operation fail even after cleanup.

Kernel-uninterruptible work can outlive the logical deadline. In that case this
process retains its ownership, subreaper state and serialization lock until the
kernel actually closes the owned children. It never reports guessed closure.
"""
from __future__ import annotations

import ctypes
from dataclasses import dataclass
import fcntl
import math
import os
from pathlib import Path
import resource
import select
import selectors
import signal
import stat
import subprocess
import threading
import time

from interface import Refused, need

_SERIAL = threading.Lock()
PR_SET_CHILD_SUBREAPER = 36
PR_GET_CHILD_SUBREAPER = 37
MAX_CHILDREN = 4096
MAX_PIDFDS = 256
MAX_OUTPUT = 16 * 1024**2
TERM_GRACE = 5.0


@dataclass(frozen=True)
class CommandResult:
    stdout: bytes
    stderr: bytes
    actual_exit_code: int | None
    expired: bool
    output_limit_exceeded: bool
    owned_processes_closed: bool
    unexpected_descendants: bool
    elapsed_seconds: float
    started: bool
    interrupted: bool


def _subreaper(value=None):
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.restype = ctypes.c_int
    # prctl is variadic. Explicit fixed-width argument objects avoid Python's
    # default int conversion truncating a pointer on a 64-bit Linux process.
    if value is None:
        result = ctypes.c_int()
        rc = libc.prctl(ctypes.c_int(PR_GET_CHILD_SUBREAPER), ctypes.byref(result),
                        ctypes.c_ulong(0), ctypes.c_ulong(0), ctypes.c_ulong(0))
        if rc != 0: raise OSError(ctypes.get_errno(), 'get_child_subreaper')
        return result.value
    rc = libc.prctl(ctypes.c_int(PR_SET_CHILD_SUBREAPER), ctypes.c_ulong(value),
                    ctypes.c_ulong(0), ctypes.c_ulong(0), ctypes.c_ulong(0))
    if rc != 0: raise OSError(ctypes.get_errno(), 'set_child_subreaper')


def _children(pid):
    """All tasks, since a descendant may fork from a non-leader thread."""
    result = set()
    try: tasks = list(Path('/proc/' + str(pid) + '/task').iterdir())
    except (FileNotFoundError, ProcessLookupError): return result
    need(len(tasks) <= 4096, 'command_task_bound')
    for task in tasks:
        try: text = (task / 'children').read_text()
        except (FileNotFoundError, ProcessLookupError): continue
        fields = text.split()
        need(len(fields) <= MAX_CHILDREN and all(s.isdecimal() for s in fields), 'command_children_shape')
        result.update(map(int, fields))
    need(len(result) <= MAX_CHILDREN, 'command_children_bound')
    return result


def _identity(pid):
    try: tail = Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()
    # An opened procfs file can return ESRCH when its task exits during read.
    # This is absence of this observation, never a guessed process exit code.
    except (FileNotFoundError, ProcessLookupError): return None
    need(len(tail) >= 20 and tail[19].isdecimal(), 'command_proc_stat_shape')
    return tail[19]


@dataclass
class _Owned:
    pid: int
    ticks: str
    fd: int
    sent: int | None = None

    def alive(self):
        # A pidfd becomes readable at process exit, even if this process has
        # not yet adopted or reaped that descendant's zombie.
        poll = select.poll(); poll.register(self.fd, select.POLLIN)
        events = poll.poll(0)
        need(not any(flags & select.POLLNVAL for _, flags in events), 'command_pidfd_invalid')
        return not events

    def send(self, value):
        if self.sent == value or self.sent == signal.SIGKILL: return
        try: signal.pidfd_send_signal(self.fd, value, None, 0)
        except ProcessLookupError: pass
        self.sent = value


class _Ownership:
    def __init__(self, leader):
        self.leader = leader
        self.entries = {}
        self.reaped = set()
        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        room = (soft if soft != resource.RLIM_INFINITY else 65536) - len(list(Path('/proc/self/fd').iterdir())) - 16
        need(room >= 1, 'command_pidfd_capacity')
        self.fd_limit = min(MAX_PIDFDS, MAX_CHILDREN, room)
        ticks = _identity(leader.pid)
        need(ticks is not None, 'command_leader_identity_missing')
        fd = os.pidfd_open(leader.pid, 0)
        if _identity(leader.pid) != ticks:
            os.close(fd); raise Refused('command_leader_identity_changed')
        self.leader_key = (leader.pid, ticks)
        self.entries[self.leader_key] = _Owned(leader.pid, ticks, fd)

    def retire(self):
        """Close only reaped/gone identities, never a not-yet-adopted zombie."""
        for key, entry in list(self.entries.items()):
            if key == self.leader_key or entry.alive(): continue
            if key in self.reaped or _identity(entry.pid) != entry.ticks:
                os.close(entry.fd); del self.entries[key]; self.reaped.discard(key)

    def capture(self):
        self.leader.poll(); self.retire()
        todo = list(_children(os.getpid())); seen = set()
        while todo:
            pid = todo.pop()
            if pid in seen: continue
            seen.add(pid); need(len(seen) <= MAX_CHILDREN, 'command_descendant_bound')
            ticks = _identity(pid)
            if ticks is None: continue
            key = (pid, ticks)
            if key not in self.entries:
                need(len(self.entries) < self.fd_limit, 'command_retained_pidfd_bound')
                try: fd = os.pidfd_open(pid, 0)
                except ProcessLookupError: continue
                if _identity(pid) != ticks:
                    os.close(fd); continue
                self.entries[key] = _Owned(pid, ticks, fd)
            todo.extend(_children(pid))
        return self.entries

    def reap(self):
        # Popen must observe its OWN leader status before any adopted reap.
        self.leader.poll()
        for key, entry in list(self.entries.items()):
            if key == self.leader_key or key in self.reaped: continue
            try: found, _ = os.waitpid(entry.pid, os.WNOHANG)
            except ChildProcessError:
                # It may still be a child of another owned process, or that
                # owned parent may already have reaped it. Do NOT mark a dead
                # nonchild as reaped: an orphan zombie may be adopted later.
                continue
            if found: self.reaped.add(key)
        self.retire()

    def living(self):
        return [entry for entry in self.entries.values() if entry.alive()]

    def closed(self):
        self.capture(); self.reap()
        return self.leader.returncode is not None and not self.living() and not _children(os.getpid())

    def signal_all(self, value):
        self.capture()
        # Signal descendants first. A living leader cannot hide a setsid child
        # by exiting between this scan and its own signal: subreaper adopts it.
        entries = sorted(self.living(), key=lambda e: e.pid == self.leader.pid)
        for entry in entries: entry.send(value)

    def close_fds(self):
        for entry in self.entries.values(): os.close(entry.fd)


def _cleanup_children():
    """Stream own direct children WITHOUT the ordinary discovery size cap.

    This is only used while this dedicated single-threaded process retains
    subreaper ownership. All children were absent before dispatch. Each token
    is a kernel PID; memory and simultaneously open pidfds remain bounded.
    """
    path = '/proc/self/task/' + str(os.getpid()) + '/children'
    with open(path, 'rb', buffering=0) as stream:
        pending = b''
        while block := stream.read(4096):
            fields = (pending + block).split(b' ')
            pending = fields.pop()
            for token in fields:
                if token:
                    need(token.isdigit() and len(token) <= 20, 'command_kernel_children_shape')
                    yield int(token)
        if pending.strip():
            token = pending.strip()
            need(token.isdigit() and len(token) <= 20, 'command_kernel_children_shape')
            yield int(token)


def _drain(process, owned, stopping, *, service_output=None):
    """Error teardown never calls capped discovery/parser/closed predicates.

    The known leader/descendants get a single TERM. Newly adopted direct
    children are processed one pidfd at a time, even beyond the tracking cap.
    No parser/resource error can unwind this physical-ownership loop.
    Retained runtimes may supply bounded, nonblocking output servicing so a
    cooperative TERM handler can finish writing. Callback errors are reported
    only after physical closure; they never release ownership early.
    """
    first = True
    output_failure = None
    if stopping is None: stopping = time.monotonic()
    while True:
        if service_output is not None:
            try: service_output()
            except BaseException as error:
                if output_failure is None: output_failure = error
        if process is not None: process.poll()
        killing = time.monotonic() - stopping >= TERM_GRACE
        if owned is not None:
            for entry in owned.entries.values():
                try:
                    if entry.alive(): entry.send(signal.SIGKILL if killing else signal.SIGTERM)
                except (OSError, Refused):
                    pass  # retain ownership and retry; never infer closure
        try:
            for pid in _cleanup_children():
                # Poll the original leader before any adopted wait. Once its
                # returncode is set, a reused same PID is just another owned
                # child and MUST NOT be skipped by numeric PID alone.
                original_leader = process is not None and pid == process.pid and process.returncode is None
                if original_leader:
                    process.poll()
                    if process.returncode is not None: continue
                ticks = _identity(pid)
                if ticks is None: continue
                try: fd = os.pidfd_open(pid, 0)
                except ProcessLookupError: continue
                try:
                    if _identity(pid) != ticks: continue
                    killing = time.monotonic() - stopping >= TERM_GRACE
                    if killing or first:
                        known = owned.entries.get((pid, ticks)) if owned is not None else None
                        if known is not None:
                            known.send(signal.SIGKILL if killing else signal.SIGTERM)
                        else:
                            try: signal.pidfd_send_signal(fd, signal.SIGKILL if killing else signal.SIGTERM, None, 0)
                            except ProcessLookupError: pass
                    if not original_leader:
                        try: os.waitpid(pid, os.WNOHANG)
                        except ChildProcessError: pass
                finally: os.close(fd)
            first = False
            if process is not None: process.poll()
            # A missing /proc is an exception, never an empty child set.
            none_left = next(_cleanup_children(), None) is None
            if none_left and (process is None or process.returncode is not None):
                if owned is not None:
                    # Any retained descendant still living would contradict
                    # closure. Stay owned until both observations agree.
                    if any(entry.alive() for entry in owned.entries.values()):
                        time.sleep(.05); continue
                break
        except (OSError, Refused):
            # E.g. temporary descriptor exhaustion or unavailable /proc. The
            # process cannot prove closure: keep subreaper/mutex and keep trying.
            pass
        time.sleep(.05)
    if output_failure is not None: raise output_failure


def _validate(command, fd, stdin, env, deadline, max_output):
    need(type(command) is list and 1 <= len(command) <= 8192
         and all(type(x) is str and '\0' not in x and len(x) <= 16384 for x in command),
         'command_process_arguments')
    need(type(fd) is int and fd >= 0 and stat.S_ISREG(os.fstat(fd).st_mode), 'command_process_executable_fd')
    need(stdin is None or type(stdin) is bytes and len(stdin) <= 128 * 1024**2, 'command_process_stdin_bound')
    need(type(env) is dict and 1 <= len(env) <= 256
         and all(type(k) is str and type(v) is str and k and '=' not in k
                 and '\0' not in k + v and len(k) <= 1024 and len(v) <= 16384 for k, v in env.items()),
         'command_process_environment')
    need(type(deadline) in (int, float) and math.isfinite(deadline), 'command_process_deadline')
    need(type(max_output) is int and 1 <= max_output <= MAX_OUTPUT, 'command_process_output_bound')
    need(threading.current_thread() is threading.main_thread()
         and len(list(Path('/proc/self/task').iterdir())) == 1, 'command_process_dedicated_main_thread')
    need(hasattr(os, 'pidfd_open') and hasattr(signal, 'pidfd_send_signal'), 'command_pidfd_required')


def execute(command, *, executable_fd, stdin, env, deadline, max_output=MAX_OUTPUT):
    """Run one authorized command and retain all descendant ownership to closure.

    No child is dispatched after deadline. Errors after dispatch drain/reap
    before propagating. SIGINT/SIGTERM become result.interrupted after drainage;
    the caller MUST reject that result even if the leader happened to exit zero.
    Never invoke this from a process also creating unrelated children/threads.
    """
    _validate(command, executable_fd, stdin, env, deadline, max_output)
    need(_SERIAL.acquire(blocking=False), 'command_process_concurrent_call')
    old_subreaper = None; previous_handlers = {}; process = None; owned = None
    selector = None; stdout = bytearray(); stderr = bytearray()
    expired = False; exceeded = False; interrupted = []; unexpected = False
    failure = None; started_at = time.monotonic(); stopping = None; killing = False
    def interrupt(number, frame):
        del frame
        if not interrupted: interrupted.append(number)
    def stop():
        nonlocal stopping
        if stopping is None:
            stopping = time.monotonic(); owned.signal_all(signal.SIGTERM)
    try:
        selector = selectors.DefaultSelector()
        need(not _children(os.getpid()), 'command_process_preexisting_children')
        old_subreaper = _subreaper(); _subreaper(1)
        # SIG_IGN or a caller's waitpid handler can silently auto-reap the CLI
        # and make Popen report a guessed zero. Own SIGCHLD until all reaps end.
        previous_handlers[signal.SIGCHLD] = signal.getsignal(signal.SIGCHLD)
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        for number in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[number] = signal.getsignal(number); signal.signal(number, interrupt)
        if time.monotonic() >= deadline:
            expired = True
        elif interrupted:
            pass
        else:
            # Do not alter cwd, system environment, caller FD ownership, or use
            # preexec_fn. The very held executable inode is used for dispatch.
            process = subprocess.Popen(command, executable='/proc/self/fd/' + str(executable_fd),
                pass_fds=(executable_fd,), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, start_new_session=True, env=env)
            owned = _Ownership(process); owned.capture()
            need(any(entry.pid == process.pid for entry in owned.entries.values()),
                 'command_process_leader_identity_missing')
            outgoing = memoryview(stdin or b'')
            for pipe, target in ((process.stdout, stdout), (process.stderr, stderr)):
                os.set_blocking(pipe.fileno(), False); selector.register(pipe, selectors.EVENT_READ, target)
            os.set_blocking(process.stdin.fileno(), False)
            if outgoing: selector.register(process.stdin, selectors.EVENT_WRITE, None)
            else: process.stdin.close()
            while True:
                owned.capture(); owned.reap()
                now = time.monotonic()
                if now >= deadline: expired = True
                if expired or exceeded or interrupted: stop()
                if process.returncode is not None:
                    live = [entry for entry in owned.living() if entry.pid != process.pid]
                    if live:
                        unexpected = True; stop()
                if stopping is not None:
                    if now - stopping >= TERM_GRACE:
                        killing = True
                    owned.signal_all(signal.SIGKILL if killing else signal.SIGTERM)
                if owned.closed() and not selector.get_map(): break
                # The logical deadline is never extended by output or signals.
                # Physical cleanup continues under retained ownership afterwards.
                wait = .05 if stopping is not None else min(.1, max(0, deadline - now))
                for key, _ in selector.select(wait):
                    if key.fileobj is process.stdin:
                        try:
                            count = os.write(key.fd, outgoing[:65536]); outgoing = outgoing[count:]
                        except BrokenPipeError: outgoing = memoryview(b'')
                        if not outgoing:
                            selector.unregister(key.fileobj); key.fileobj.close()
                    else:
                        block = os.read(key.fd, 65536)
                        if not block:
                            selector.unregister(key.fileobj); key.fileobj.close()
                        elif len(key.data) + len(block) > max_output:
                            exceeded = True
                        else: key.data.extend(block)
    except BaseException as error:
        failure = error
    finally:
        # This also covers exceptions in parsing/pipe setup. Popen may have
        # succeeded before construction of the ordinary ownership object.
        if old_subreaper is not None:
            _drain(process, owned, stopping)
        if process is not None:
            process.wait()
            for pipe in (process.stdin, process.stdout, process.stderr):
                if pipe is not None and not pipe.closed: pipe.close()
        if owned is not None: owned.close_fds()
        if selector is not None: selector.close()
        if old_subreaper is not None:
            need(next(_cleanup_children(), None) is None, 'command_process_unclosed_children')
        for number, handler in previous_handlers.items(): signal.signal(number, handler)
        if old_subreaper is not None: _subreaper(old_subreaper)
        _SERIAL.release()
    if failure is not None: raise failure
    return CommandResult(bytes(stdout), bytes(stderr), process.returncode if process else None,
        expired, exceeded, True, unexpected, time.monotonic() - started_at,
        process is not None, bool(interrupted))


def execute_retained_runtime(command, *, executable_fd, stdin, env, dispatch_deadline,
                             shutdown_permission_fd, permission_token, owner_pidfd,
                             operation_lock_fd, max_output=MAX_OUTPUT):
    """One runtime, held until a source-owned controller permits physical stop.

    Expiry/error/owner death is sticky UNKNOWN but never permission to kill a
    runtime still needed by another actor. EOF is NOT permission. All exceptional
    teardown retains subreaper/FD/flock ownership until a valid token or actual
    spontaneous physical closure. This is not the finite execute() contract.
    """
    _validate(command, executable_fd, stdin, env, dispatch_deadline, max_output)
    need(stdin is None and type(permission_token) is bytes and 1 <= len(permission_token) <= 128,
         'runtime_permission_shape')
    need(len({shutdown_permission_fd, owner_pidfd, operation_lock_fd, executable_fd}) == 4,
         'runtime_distinct_descriptors')
    need(stat.S_ISFIFO(os.fstat(shutdown_permission_fd).st_mode), 'runtime_permission_pipe')
    need(os.readlink('/proc/self/fd/' + str(owner_pidfd)) == 'anon_inode:[pidfd]', 'runtime_owner_pidfd')
    info = dict(line.split(':', 1) for line in Path('/proc/self/fdinfo/' + str(owner_pidfd)).read_text().splitlines())
    need(info.get('Pid', '').strip() == str(os.getppid()), 'runtime_owner_parent')
    locked = os.fstat(operation_lock_fd)
    need(stat.S_ISREG(locked.st_mode) and locked.st_nlink == 1, 'runtime_operation_lock')
    # A separately opened description must be excluded by the inherited flock.
    probe = os.open('/proc/self/fd/' + str(operation_lock_fd), os.O_RDWR | os.O_CLOEXEC)
    try:
        try: fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: pass
        else:
            fcntl.flock(probe, fcntl.LOCK_UN); raise Refused('runtime_operation_lock_unheld')
    finally: os.close(probe)
    need(_SERIAL.acquire(blocking=False), 'command_process_concurrent_call')
    old_subreaper = None; previous_handlers = {}; process = None; owned = None
    stdout = bytearray(); stderr = bytearray(); streams = []; stream_eof = set()
    expired = False; exceeded = False; interrupted = False; unexpected = False
    failure = None; started_at = time.monotonic(); permission = bytearray()
    permission_eof = False; permission_invalid = False; permitted = False
    def interrupt(number, frame):
        nonlocal interrupted
        del number, frame
        interrupted = True
    def read_output():
        nonlocal exceeded
        # Keep draining after the cap; fixed RAM, no pipe backpressure.
        for stream, target in streams:
            if stream.fileno() in stream_eof: continue
            for _ in range(4):
                try: raw = os.read(stream.fileno(), 65536)
                except BlockingIOError: break
                if not raw:
                    stream_eof.add(stream.fileno()); break
                room = max(0, max_output - len(stdout) - len(stderr))
                target.extend(raw[:room])
                if len(raw) > room: exceeded = True
    def observe():
        nonlocal expired, exceeded, interrupted, permission_eof, permission_invalid, permitted
        expired |= time.monotonic() >= dispatch_deadline
        poll = select.poll(); poll.register(owner_pidfd, select.POLLIN)
        events = poll.poll(0)
        if events: interrupted = True
        need(not any(flags & select.POLLNVAL for _, flags in events), 'runtime_owner_pidfd_invalid')
        if not permission_eof:
            try: raw = os.read(shutdown_permission_fd, 129)
            except BlockingIOError: raw = None
            if raw == b'': permission_eof = True
            elif raw is not None:
                if len(permission) + len(raw) > 128: permission_invalid = True
                else: permission.extend(raw)
            if permission_eof:
                permitted = not permission_invalid and bytes(permission) == permission_token
                if not permitted: interrupted = True
        read_output()
    def physically_closed():
        if process is not None: process.poll()
        # Passive adoption/reaping is safe before permission; no signals. Poll
        # Popen's own leader first so its actual status cannot be stolen.
        for pid in _cleanup_children():
            if process is not None and pid == process.pid and process.returncode is None: continue
            try: os.waitpid(pid, os.WNOHANG)
            except ChildProcessError: pass
        return (process is None or process.returncode is not None) and \
            next(_cleanup_children(), None) is None and \
            (owned is None or not any(entry.alive() for entry in owned.entries.values()))
    try:
        need(not _children(os.getpid()), 'command_process_preexisting_children')
        old_subreaper = _subreaper(); _subreaper(1)
        for number in (signal.SIGCHLD, signal.SIGTERM, signal.SIGINT):
            previous_handlers[number] = signal.getsignal(number)
            signal.signal(number, signal.SIG_DFL if number == signal.SIGCHLD else interrupt)
        os.set_blocking(shutdown_permission_fd, False)
        observe()
        if not expired and not interrupted and not permitted:
            process = subprocess.Popen(command, executable='/proc/self/fd/' + str(executable_fd),
                pass_fds=(executable_fd,), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, start_new_session=True, env=env)
            streams = [(process.stdout, stdout), (process.stderr, stderr)]
            for stream, _ in streams: os.set_blocking(stream.fileno(), False)
            owned = _Ownership(process); owned.capture()
            while True:
                observe(); owned.capture(); owned.reap()
                if process.poll() is not None and any(e.pid != process.pid for e in owned.living()):
                    unexpected = True
                if physically_closed():
                    if not permitted: unexpected = True
                    break
                if permitted: break
                time.sleep(.02)
    except BaseException as error:
        failure = error
    finally:
        if old_subreaper is not None:
            # EVERY error/finally route must cross the same stop barrier. Even
            # discovery errors retain ownership; ordinary _drain is forbidden
            # until permission. A runtime that exits itself needs no signal.
            while True:
                try:
                    if physically_closed():
                        try: observe()
                        except BaseException as error:
                            if failure is None: failure = error
                        try: read_output()
                        except BaseException as error:
                            if failure is None: failure = error
                            break  # physical closure proved; report output failure
                        if len(stream_eof) == len(streams): break
                        time.sleep(.02); continue
                    try: observe()
                    except BaseException as error:
                        if failure is None: failure = error
                    if permitted:
                        _drain(process, owned, None, service_output=read_output); continue
                except BaseException as error:
                    if failure is None: failure = error
                time.sleep(.02)
        if process is not None:
            process.wait()
            for stream, _ in streams: stream.close()
        if owned is not None: owned.close_fds()
        if old_subreaper is not None:
            need(next(_cleanup_children(), None) is None, 'runtime_unclosed_children')
        for number, handler in previous_handlers.items(): signal.signal(number, handler)
        if old_subreaper is not None: _subreaper(old_subreaper)
        _SERIAL.release()
    if failure is not None: raise failure
    return CommandResult(bytes(stdout), bytes(stderr), process.returncode if process else None,
        expired, exceeded, True, unexpected, time.monotonic() - started_at,
        process is not None, interrupted)
