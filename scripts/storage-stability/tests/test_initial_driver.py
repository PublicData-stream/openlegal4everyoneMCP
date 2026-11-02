"""Synthetic native ports plus real fork/waitpid/pidfd and private /tmp files.
No Docker, PostgreSQL, ZFS, provider, network or migration command is executed.
Two same-filesystem journal dirs are a labelled fixture only: production creates
DualJournal normally and requires distinct media. No callback is an API input.
"""
import copy
import fcntl
import importlib
import os
from pathlib import Path
import signal
import select
import shutil
import socket
import stat
import sys
import tempfile
import time
import types
import unittest
import uuid
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))
import initial_driver as d
import journal as j
import initial_migration as migration
from interface import Refused
from inventory import canonical, digest

MODE = None
FRESH_MARKER = None
FAKE_CGROUP = None


def fixture_journal_init(self, roots, operation, binding):
    # Same-FS only in this pure fixture; real constructor's media check is intact.
    self.base = [(Path(r['path']), r['identity']) for r in roots]
    self.paths = []; self.operation = operation; self.binding = binding
    for path, identity in self.base:
        j.directory(path, identity)
        self.paths.append((path / operation, j.new_directory(path / operation)))
    self.phase_paths = None; self.bytes = self.count = 0
    self.artifacts = {}; self.history = []; self.phase_names = set()
    self.save('operation.json', binding)


def fixture_capture(backend, op):
    fd = os.open(FAKE_CGROUP, os.O_RDONLY | os.O_DIRECTORY)
    st = os.fstat(fd)
    backend._cgroup_fds[str(FAKE_CGROUP)] = (fd, {'dev': st.st_dev, 'ino': st.st_ino})
    backend._artifact(op, 'fixture-prefork-capture', {'pid': os.getpid()})


def fixture_guard(backend, op, field):
    prefix = 'legacy-prepare' if field.startswith('_legacy') else 'initial-migration'
    path = Path('/tmp/openlegal-' + prefix + '-primary-' +
                op.inventory['linux']['postgres']['primary']['container_id'] + '.lock')
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.write(fd, canonical({'operation_id': op.operation_id, 'unknown_requires_manual_audit': True}))
    os.fsync(fd); setattr(backend, field, fd)


def fixture_legacy(backend, op):
    fixture_guard(backend, op, '_legacy_prepare_guard_fd')
    backend._legacy_prepare_mount_sequence = 1
    backend._artifact(op, 'dataset-mounts-1', {'fixture': True})
    if MODE == 'docker-client':
        backend._run(op, 'docker', ['version'])
    if MODE == 'nonzero':
        # A completion-like artifact must not override this actual OS exit7.
        backend._artifact(op, 'returned-payloads', {'looks_complete': True})
        raise RuntimeError('synthetic body failure')
    if MODE == 'exit7':
        backend._artifact(op, 'returned-payloads', {'looks_complete': True})
        os._exit(7)
    if MODE == 'late':
        time.sleep(2)
    if MODE == 'descendant':
        pid = os.fork()
        if pid == 0:
            os.setsid()
            while True:
                time.sleep(.05)
    primary = {'primary_process': {'pid': os.getpid(), 'start_ticks': '1',
                                   'boot_id': op.inventory['host']['boot_id']},
               'socket_directory': {'dev': 1, 'ino': 2}, 'socket': {'dev': 1, 'ino': 3}}
    value = {'kind': 'openlegal_legacy_prepare_payloads', 'status': 'PENDING_PARENT_PRODUCER_OS_EXIT',
             'operation_id': op.operation_id, 'payloads': {'drain': {'fixture': 'drain'},
             'snapshot': {'fixture': 'snapshot'}, 'preservation': {'fixture': 'preservation'}},
             'fresh_primary': primary, 'original_restart_policy': {'Name': 'no', 'MaximumRetryCount': 0},
             'current_restart_policy': 'no', 'blob_manifests_sha256': 'a' * 64,
             'producer_os_exit': 'NOT_YET_OBSERVABLE', 'ready': False,
             'provider_state_modified': False, 'strict_seed_created': False}
    backend._artifact(op, 'returned-payloads', value)


def fixture_fresh(backend, op, result):
    # Explicit synthetic native qualification port, absent from production inputs.
    op.seal()
    assert backend._legacy_prepare_mount_sequence == 1
    backend._legacy_prepare_mount_sequence += 1
    backend._artifact(op, 'dataset-mounts-2', {'fixture_parent': True})
    if MODE == 'docker-client':
        backend._run(op, 'docker', ['version'])
    backend._artifact(op, 'fixture-fresh-parent', {'pid': os.getpid(), 'parent': True})
    return copy.deepcopy(result['fresh_primary'])


def fixture_initial(backend, op):
    # Frozen prior reader verifies BOTH raw artifacts with actual reaped PID.
    c = op.inventory['initial_migration']
    for key in ('drain', 'snapshot', 'preservation'):
        migration._prior(op, c, key)
    op.journal.read_phase_bytes(d.legacy.PHASE,
        d.legacy.PHASE + '-fixture-fresh-parent.json',
        op.journal.history[-1][1][d.legacy.PHASE + '-fixture-fresh-parent.json']['sha256'])
    fixture_guard(backend, op, '_initial_migration_guard_fd')
    backend._initial_migration_cgroup_fd = os.open(FAKE_CGROUP, os.O_RDONLY | os.O_DIRECTORY)
    st = os.fstat(backend._initial_migration_cgroup_fd)
    backend._artifact(op, 'native-created', {'id': 'c' * 64})
    backend._artifact(op, 'native-gated-owner', {'witness': {'cgroup_identity': {'dev': st.st_dev, 'ino': st.st_ino}}})
    backend._artifact(op, 'migration-complete', {'kind': 'openlegal_initial_migration_result',
        'candidate_id': 'c' * 64, 'schema_version': 33, 'migration_native_exit_code': 0,
        'same_database_sessions': [], 'old_rows_and_references_sha256': digest(canonical(migration._prior(op, c, 'preservation'))),
        'seed_created': False, 'roles_created': False, 'ready': False,
        'outer_actual_os_exit': 'NOT_YET_OBSERVABLE'})
    if MODE == 'initial7':
        os._exit(7)


@unittest.skipUnless(os.geteuid() == 0, 'root private fixture ownership')
class DriverTests(unittest.TestCase):
    def setUp(self):
        global MODE, FAKE_CGROUP
        MODE = None
        self.temp = tempfile.TemporaryDirectory(dir='/tmp', prefix='initial-driver-pure-')
        self.addCleanup(self.temp.cleanup); self.root = Path(self.temp.name); self.root.chmod(0o700)
        FAKE_CGROUP = self.root / 'synthetic-cgroup'; FAKE_CGROUP.mkdir(mode=0o700)
        self.backend = d.Backend(); self.opid = str(uuid.uuid4()); self.cid = uuid.uuid4().hex * 2
        self.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        lock = self.root / 'lock'; lock.write_bytes(b''); lock.chmod(0o600); st = lock.stat()
        self.record = {'path': str(lock), 'identity': {'dev': st.st_dev, 'ino': st.st_ino}}
        self.lock = j.SharedLock(self.record).acquire(); self.addCleanup(self.lock.close)
        roots = []
        for name in ('journal-a', 'journal-b'):
            path = self.root / name; path.mkdir(mode=0o700); st = path.stat()
            roots.append({'path': str(path), 'identity': {'dev': st.st_dev, 'ino': st.st_ino}})
        self.graph = {name: digest((BASE / name).read_bytes()) for name in d.SOURCES}
        harmless = shutil.which('true'); self.assertIsNotNone(harmless)
        true_binary = Path(harmless).resolve(strict=True)
        self.inv = {'host': {'boot_id': self.boot}, 'source_graph': self.graph,
                    'shared_lock': self.record, 'journal_roots': roots,
                    'backup_root': {'path': str(self.root)},
                    'linux': {'postgres': {'primary': {'container_id': self.cid}},
                        'binaries': {'docker': {'path': str(true_binary),
                            'sha256': digest(true_binary.read_bytes())}}}}
        self.template = dict.fromkeys(('primary_process', 'socket_directory', 'socket',
                                      'prerequisites', 'legacy_binding'))
        self.template.update(candidate={}, compiled_manifest={}, deadline_secs=30,
            producer_source={'path': str(BASE / 'legacy_prepare.py'), 'sha256': self.graph['legacy_prepare.py']})
        self.invref = self.save('legacy.json', self.inv); self.tref = self.save('template.json', self.template)
        self.config = {'schema_version': 1, 'kind': 'openlegal_initial_driver_inventory', 'operation_id': self.opid,
            'legacy_inventory': self.invref, 'initial_template': self.tref, 'total_deadline_secs': 30, 'source_graph': self.graph}
        self.configref = self.save('driver.json', self.config)
        self.auth = {'schema_version': 1, 'kind': 'openlegal_initial_driver_authority', 'approved': True,
            'operation_id': self.opid, 'inventory_sha256': self.configref['sha256'],
            'source_graph_sha256': digest(canonical(self.graph)), 'legacy_inventory_sha256': self.invref['sha256'],
            'initial_template_sha256': self.tref['sha256'], 'host_boot_id': self.boot, 'valid_until': int(time.time()) + 300,
            'legacy_fence_snapshot_permitted': True, 'initial_migration_permitted': True, 'preserve_originals': True,
            'automatic_retry': False, 'role_provisioning_permitted': False, 'strict_seed_permitted': False,
            'ready_permitted': False, 'provider_calls_permitted': False}
        self.authref = self.save('authority.json', self.auth)
        self.addCleanup(self.cleanup_fds)
        patches = [
            patch.object(j.DualJournal, '__init__', fixture_journal_init),
            patch.object(d, '_source_seal', lambda graph: None),
            patch.object(d.Backend, '_capture_writer_cgroups', fixture_capture),
            patch.object(d.legacy, 'run', fixture_legacy),
            patch.object(d.migration, 'run', fixture_initial),
            patch.object(d, '_fresh_parent', fixture_fresh),
            # Synthetic cgroup FS qualification only; never used by the native API.
            patch.object(d.os, 'fstatvfs', lambda fd: types.SimpleNamespace(f_fsid=1)),
            patch.object(d.os, 'statvfs', lambda path: types.SimpleNamespace(f_fsid=1))]
        for item in patches:
            item.start(); self.addCleanup(item.stop)

    def save(self, name, value):
        raw = canonical(value); path = self.root / name; path.write_bytes(raw); path.chmod(0o600)
        return {'path': str(path), 'sha256': digest(raw)}

    def cleanup_fds(self):
        fds = {fd for fd, row in self.backend._cgroup_fds.values()}
        for name in d.GUARDS:
            if hasattr(self.backend, name): fds.add(getattr(self.backend, name))
        fds.update(getattr(self.backend, '_initial_driver_failure_fds', []))
        if hasattr(self.backend, '_driver_docker_config_fd'):
            fds.add(self.backend._driver_docker_config_fd)
        for fd in fds:
            try: os.close(fd)
            except OSError: pass
        self.backend._cgroup_fds.clear()
        for prefix in ('legacy-prepare', 'initial-migration'):
            Path('/tmp/openlegal-' + prefix + '-primary-' + self.cid + '.lock').unlink(missing_ok=True)
        config = Path('/tmp/openlegal-storage-docker-client-' + self.opid)
        if config.exists(): config.rmdir()

    def run_driver(self):
        return d.run(self.backend, self.lock, self.configref, self.authref)

    def test_live_two_children_actual_exit_and_raw_prerequisites(self):
        result = self.run_driver()
        self.assertEqual(result['operation_id'], self.opid)
        for key in ('legacy_actual_exit', 'initial_actual_exit'):
            self.assertEqual(result[key]['actual_exit_code'], 0)
            self.assertEqual(result[key]['remaining_children'], [])
            self.assertEqual(result[key]['boot_id'], self.boot)
        self.assertNotEqual(result['legacy_actual_exit']['pid'], result['initial_actual_exit']['pid'])
        self.assertFalse(result['ready']); self.assertFalse(result['strict_seed_created'])
        self.lock.check(); d._writer_fds(self.backend)
        for name in d.GUARDS:
            self.assertTrue(fcntl.fcntl(getattr(self.backend, name), fcntl.F_GETFD) & fcntl.FD_CLOEXEC)
        self.backend._initial_driver_journal.check()
        with self.assertRaises(Refused): self.run_driver()

    def test_ignored_sigchld_still_observes_actual_exit7(self):
        global MODE
        MODE = 'exit7'; previous = signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        try:
            with self.assertRaisesRegex(Refused, 'child_not_successfully_closed'): self.run_driver()
            self.assertEqual(self.backend._initial_driver_last_exit['actual_exit_code'], 7)
            self.assertEqual(self.backend._initial_driver_last_exit['remaining_children'], [])
            self.assertIs(signal.getsignal(signal.SIGCHLD), signal.SIG_IGN)
            self.assertEqual(d.processes._children(os.getpid()), set())
        finally: signal.signal(signal.SIGCHLD, previous)

    def test_nonzero_child_index_does_not_get_adopted_and_guard_retained(self):
        global MODE
        MODE = 'nonzero'
        with self.assertRaises(Refused): self.run_driver()
        self.assertTrue(self.backend._initial_driver_failure_fds)
        self.assertFalse(any('live-drain' in p.name for p in self.root.rglob('*')))
        self.assertEqual(self.backend._initial_driver_last_exit['actual_exit_code'], 1)
        self.lock.check(); d._writer_fds(self.backend)

    def test_initial_exit7_completion_does_not_become_parent_success(self):
        global MODE
        MODE = 'initial7'
        with self.assertRaisesRegex(Refused, 'child_not_successfully_closed'): self.run_driver()
        self.assertEqual(self.backend._initial_driver_last_exit['actual_exit_code'], 7)
        self.assertFalse(any(p.name == 'driver-complete.json' for p in self.root.rglob('*')))
        self.lock.check(); d._writer_fds(self.backend)

    def test_detached_descendant_is_drained_and_rejected(self):
        global MODE
        MODE = 'descendant'
        with self.assertRaisesRegex(Refused, 'child_not_successfully_closed'): self.run_driver()
        self.assertEqual(d.processes._children(os.getpid()), set())
        self.assertEqual(self.backend._initial_driver_last_exit['remaining_children'], [])

    def test_no_historical_operation_adoption(self):
        for row in self.inv['journal_roots']:
            (Path(row['path']) / self.opid).mkdir(mode=0o700)
        with self.assertRaises(FileExistsError): self.run_driver()
        self.assertFalse(self.backend._cgroup_fds)

    def test_parent_original_deadline_refuses_late_child_and_drains(self):
        global MODE
        MODE = 'late'; self.config['total_deadline_secs'] = 1
        self.configref = self.save('short-driver.json', self.config)
        self.auth['inventory_sha256'] = self.configref['sha256']
        self.authref = self.save('short-authority.json', self.auth)
        started = time.monotonic()
        with self.assertRaises(Refused): self.run_driver()
        self.assertGreaterEqual(time.monotonic() - started, 1)
        self.assertLess(time.monotonic() - started, 10)
        self.assertEqual(d.processes._children(os.getpid()), set())
        self.assertFalse(any(p.name == 'driver-complete.json' for p in self.root.rglob('*')))

    def test_real_native_docker_client_branch_shared_by_child_and_fresh_parent(self):
        global MODE
        MODE = 'docker-client'
        # Real frozen LinuxBackend._run, empty-dir branch, binary hashing and
        # actual reviewed process ownership execute the canonical host true.
        # binary is explicitly a harmless stand-in; no Docker is invoked.
        result = self.run_driver()
        self.assertEqual(result['initial_actual_exit']['actual_exit_code'], 0)
        directory, identity = self.backend._docker_config
        self.assertEqual(directory, Path('/tmp/openlegal-storage-docker-client-' + self.opid))
        self.assertEqual((directory.stat().st_dev, directory.stat().st_ino), identity)
        self.assertEqual(list(directory.iterdir()), [])
        self.assertEqual(self.backend._legacy_prepare_mount_sequence, 2)
        self.assertTrue(os.fstat(self.backend._driver_docker_config_fd))

    def test_fixed_docker_client_rebinding_refused(self):
        original = d._fresh_parent
        moved = Path('/tmp/openlegal-storage-docker-client-' + self.opid + '-detached-fixture')
        def rebind(backend, op, result):
            path, identity = backend._docker_config
            path.rename(moved); path.mkdir(mode=0o700)
            return original(backend, op, result)
        try:
            with patch.object(d, '_fresh_parent', side_effect=rebind):
                with self.assertRaisesRegex(Refused, 'docker_config_changed'):
                    self.run_driver()
        finally:
            if moved.exists(): moved.rmdir()

    def test_one_media_raw_tamper_refuses_handoff_and_retains_guard(self):
        original = d._reconcile
        def corrupt(backend, op, index, fds):
            path = op.journal.phase_paths[1][0] / (op.phase + '-returned-payloads.json')
            path.write_bytes(b'{"tampered":true}\n')
            return original(backend, op, index, fds)
        with patch.object(d, '_reconcile', side_effect=corrupt):
            with self.assertRaisesRegex(Refused, 'private_raw_hash'): self.run_driver()
        self.assertTrue(self.backend._initial_driver_failure_fds)
        self.assertEqual(self.backend._initial_driver_last_exit['actual_exit_code'], 0)
        self.assertFalse(any(p.name == 'driver-complete.json' for p in self.root.rglob('*')))

    def test_parent_death_kernel_signal_closes_gated_child(self):
        # Actual harmless three-generation fork: this test owns every PID via
        # subreaper/pidfds, and restores state only after kernel child closure.
        self.assertEqual(d.processes._children(os.getpid()), set())
        oldsub = d.processes._subreaper(); oldchld = signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        d.processes._subreaper(1); readfd, writefd = os.pipe(); parentfd = None
        try:
            pid = os.fork()
            if pid == 0:
                os.close(readfd); parent = os.getpid(); own = os.pidfd_open(parent, 0)
                child = os.fork()
                if child == 0:
                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    d._child_parent(parent, own)
                    os.write(writefd, (str(os.getpid()) + '\n').encode())
                    while True: signal.pause()
                os.close(writefd)
                while True: signal.pause()
            os.close(writefd); writefd = None; parentfd = os.pidfd_open(pid, 0)
            self.assertTrue(select.select([readfd], [], [], 5)[0])
            childpid = int(os.read(readfd, 64).strip())
            signal.pidfd_send_signal(parentfd, signal.SIGKILL, None, 0)
            actual, status = os.waitpid(pid, 0)
            self.assertEqual((actual, os.waitstatus_to_exitcode(status)), (pid, -signal.SIGKILL))
            actual, status = os.waitpid(childpid, 0)
            self.assertEqual((actual, os.waitstatus_to_exitcode(status)), (childpid, -signal.SIGTERM))
        finally:
            d.processes._drain(None, None, None)
            if parentfd is not None: os.close(parentfd)
            os.close(readfd)
            if writefd is not None: os.close(writefd)
            signal.signal(signal.SIGCHLD, oldchld); d.processes._subreaper(oldsub)

    def test_template_cannot_preseed_live_identities(self):
        self.template['primary_process'] = {'pid': 1}
        self.tref = self.save('badtemplate.json', self.template)
        self.config['initial_template'] = self.tref
        ref = self.save('badconfig.json', self.config)
        with self.assertRaisesRegex(Refused, 'null_placeholders'):
            d.run(self.backend, self.lock, ref, self.authref)

    def test_authority_denies_provider_role_seed_and_ready(self):
        for field in ('provider_calls_permitted', 'role_provisioning_permitted', 'strict_seed_permitted', 'ready_permitted'):
            with self.subTest(field=field):
                value = dict(self.auth); value[field] = True
                ref = self.save(field + '.json', value)
                with self.assertRaisesRegex(Refused, 'driver_authority'):
                    d.run(self.backend, self.lock, self.configref, ref)
        self.assertIsNone(self.backend._driver_deadline)

    def test_native_api_rejects_arbitrary_subclass_and_instance_callback(self):
        class Other(d.Backend):
            pass
        with self.assertRaisesRegex(Refused, 'native_dedicated'):
            d.run(Other(), self.lock, self.configref, self.authref)
        self.backend._run = lambda *args, **kwargs: b''
        with self.assertRaisesRegex(Refused, 'overrides'):
            self.run_driver()

    def test_deadline_cap_forwards_only_native_arguments(self):
        self.backend._driver_deadline = time.monotonic() + 3
        cap = self.backend._driver_deadline
        with patch.object(d.linux.LinuxBackend, '_run', return_value=b'proof') as native:
            self.assertEqual(self.backend._run(None, 'docker', ['inspect'], deadline=cap + 60), b'proof')
        self.assertEqual(native.call_args.kwargs['deadline'], cap)
        self.assertEqual(native.call_args.args[1:], ('docker', ['inspect']))
        self.backend._driver_deadline = time.monotonic() - 1
        with self.assertRaisesRegex(Refused, 'original_deadline'):
            self.backend._run(None, 'docker', ['inspect'])

if __name__ == '__main__':
    unittest.main(verbosity=2)
