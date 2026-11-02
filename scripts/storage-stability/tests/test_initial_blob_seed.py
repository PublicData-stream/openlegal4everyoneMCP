"""Real /tmp generation copies, real live two-child driver, synthetic native ports.
The driver fixture's SQL/Kube/PG operations and independent media are explicitly
mocked. Its fork/waitpid/journal/parent-FD handoff and all Blob bytes are real.
No Docker, PostgreSQL, ZFS, network, role or Ready operation executes.
"""
import copy
import errno
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import stat
import tempfile
import types
import sys
import time
import unittest
import uuid
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_initial_driver as live
import initial_blob_seed as seed
import bootstrap_backend as blobs
from interface import Refused
from inventory import canonical, digest

REAL_STATVFS = os.statvfs
REAL_FSTATVFS = os.fstatvfs


@unittest.skipUnless(os.geteuid() == 0, 'root private and UID10004 fixture metadata')
class SeedTests(unittest.TestCase):
    def setUp(self):
        self.case = live.DriverTests('test_live_two_children_actual_exit_and_raw_prerequisites')
        self.case.setUp(); self.addCleanup(self.case.doCleanups)
        self.root = self.case.root; self.backend = self.case.backend
        self.sources = {}
        for role in sorted(seed.SOURCE_ROLES):
            path = self.root / role; path.mkdir(mode=0o700); os.chown(path, 10004, 10004)
            st = path.stat(); self.sources[role] = {'path': str(path), 'identity': {'dev': st.st_dev, 'ino': st.st_ino}}
        empty_cache = self._testMethodName == 'test_genuine_empty_cache_baseline_still_requires_live_empty_scan_and_sql'
        self.cache = None if empty_cache else self.generation('cache_hdd', b'fictional cache history')
        self.corpus_a = self.generation('corpus_hdd', b'fictional retained corpus')
        self.corpus_b = self.generation('corpus_ssd_lookup', b'fictional unreferenced old corpus')
        self.generation('corpus_ssd_lookup', b'fictional retained corpus', self.corpus_a['storage_key'])
        tagged = Path(self.sources['corpus_hdd']['path']) / self.corpus_a['storage_key']
        if self._testMethodName == 'test_existing_source_file_xattr_is_unsupported_not_silently_lost':
            os.setxattr(tagged, 'user.openlegal_review', b'fictional review metadata')
        elif self._testMethodName == 'test_existing_source_shard_xattr_is_unsupported':
            os.setxattr(tagged.parent, 'user.openlegal_review', b'fictional shard metadata')
        elif self._testMethodName == 'test_existing_source_root_xattr_is_unsupported':
            os.setxattr(tagged.parent.parent, 'user.openlegal_review', b'fictional root metadata')
        if self._testMethodName == 'test_old_controls_and_health_remain_original_and_are_never_adopted':
            root = Path(self.sources['corpus_hdd']['path'])
            marker = root / blobs.MARKER
            marker.write_bytes(canonical({'schema_version': 1, 'kind': 'openlegal_blob_seed',
                'managed_epoch': str(uuid.uuid4()), 'seed_operation': str(uuid.uuid4()),
                'role': 'corpus_primary', 'manifest_sha256': 'a' * 64,
                'correspondence_sha256': 'b' * 64, 'filesystem_id': 'old-fixture'}))
            marker.chmod(0o640); os.chown(marker, 0, 10004)
            shard = root / '00'; shard.mkdir(mode=0o700); os.chown(shard, 10004, 10004)
            health = shard / ('.stage-' + '0' * 64 + '-00000000-0000-0000-0000-000000000000.' + 'a' * 64)
            health.write_bytes(b'blob-health\n'); health.chmod(0o600); os.chown(health, 10004, 10004)
        self.case.inv['managed_epoch'] = str(uuid.uuid4())
        self.case.inv['legacy_prepare'] = {'blob_roots': self.sources}
        self.targets = {}
        for n, role in enumerate(seed.ROLES):
            path = self.root / ('prepared-' + role); path.mkdir(mode=0o700)
            st = path.stat(); fsid = os.statvfs(path).f_fsid
            media = ({'kind': 'hdd', 'dataset': 'cachepool/prepared-' + role,
                      'guid': str(1000 + n), 'mountpoint': str(path)}
                     if blobs.ROLE_MEDIA[role] == 'hdd' else
                     {'kind': 'ssd', 'physical_leaves': ['/dev/fixture_ssd']})
            filesystem = 'zfs:' + str(1000 + n) if blobs.ROLE_MEDIA[role] == 'hdd' else (
                'block:' + str(os.major(st.st_dev)) + ':' + str(os.minor(st.st_dev)))
            self.targets[role] = {'host_path': str(path / 'data'), 'container_path': '/fixture/' + role,
                'relative': 'data', 'parent': {'path': str(path), 'identity': {'dev': st.st_dev, 'ino': st.st_ino},
                    'kernel_filesystem_id': fsid, 'filesystem_id': filesystem}, 'media': media}
        # Rebind the sealed synthetic driver inputs before its real two forks.
        self.case.invref = self.case.save('seed-driver-legacy.json', self.case.inv)
        self.case.config['legacy_inventory'] = self.case.invref
        self.case.configref = self.case.save('seed-driver.json', self.case.config)
        self.case.auth.update(inventory_sha256=self.case.configref['sha256'],
                              legacy_inventory_sha256=self.case.invref['sha256'])
        self.case.authref = self.case.save('seed-driver-authority.json', self.case.auth)
        self.baseline = None
        original_capture = live.fixture_capture
        def capture(backend, op):
            original_capture(backend, op)
            for path, (fd, row) in backend._cgroup_fds.items():
                row['path'] = path
        def legacy(backend, op):
            live.fixture_legacy(backend, op)
            backend._artifact(op, 'parent-fd-anchor', seed.legacy._parent_anchor(backend, op))
        def fresh_driver(backend, op, result):
            primary = live.fixture_fresh(backend, op, result)
            self.baseline = self.scan_sources()
            backend._artifact(op, 'blob-manifests-parent-live', self.baseline)
            return primary
        for item in (patch.object(seed.driver.Backend, '_capture_writer_cgroups', capture),
                     patch.object(seed.legacy, 'run', legacy),
                     patch.object(seed.driver, '_fresh_parent', fresh_driver)):
            item.start(); self.addCleanup(item.stop)
        self.driver_result = self.case.run_driver()
        # End the driver's labelled synthetic cgroup-filesystem port before
        # exercising real seed capacity, fsid and byte-copy behavior.
        for item in (patch.object(seed.os, 'statvfs', REAL_STATVFS),
                     patch.object(seed.os, 'fstatvfs', REAL_FSTATVFS)):
            item.start(); self.addCleanup(item.stop)
        for target in self.targets.values():
            target['parent']['kernel_filesystem_id'] = REAL_STATVFS(target['parent']['path']).f_fsid
        self.baseline = self.scan_sources()
        self.saved_originals = copy.deepcopy(self.baseline)
        self.references = [dict(role='corpus', **self.corpus_a)]
        if self.cache is not None: self.references.append(dict(role='cache', **self.cache))
        self.references.sort(key=lambda row: (row['role'], row['storage_key']))
        self.cfg = {'schema_version': 2, 'media_profile': blobs.MEDIA_PROFILE, 'kind': 'openlegal_initial_blob_seed_inventory',
            'operation_id': self.case.opid, 'driver_inventory': self.case.configref,
            'driver_authority': self.case.authref, 'source_sha256': digest((BASE / 'initial_blob_seed.py').read_bytes()),
            'managed_epoch': self.case.inv['managed_epoch'], 'sources': self.sources,
            'targets': self.targets, 'capacity_reserve_bytes': 0}
        self.cfgref = self.case.save('seed.json', self.cfg)
        self.authority = {'schema_version': 2, 'media_profile': blobs.MEDIA_PROFILE, 'kind': 'openlegal_initial_blob_seed_authority',
            'approved': True, 'operation_id': self.case.opid, 'inventory_sha256': self.cfgref['sha256'],
            'driver_inventory_sha256': self.case.configref['sha256'],
            'driver_authority_sha256': self.case.authref['sha256'], 'source_sha256': self.cfg['source_sha256'],
            'driver_managed_epoch': self.case.inv['managed_epoch'], 'seed_managed_epoch': self.cfg['managed_epoch'],
            'epoch_mapping_permitted': False, 'valid_until': int(time.time()) + 300,
            'initial_blob_seed_permitted': True, 'preserve_originals': True,
            'automatic_retry': False, 'roles_permitted': False, 'postgres_replica_permitted': False,
            'provisioning_permitted': False, 'workload_permitted': False, 'ready_permitted': False,
            'provider_permitted': False, 'network_permitted': False, 'credentials_permitted': False,
            'dataset_creation_permitted': False}
        self.authref = self.case.save('seed-authority.json', self.authority)
        original_private = seed.private_bytes
        def private(path, sha=None, **kwargs):
            if Path(path) == BASE / 'initial_blob_seed.py':
                raw = Path(path).read_bytes()
                self.assertEqual(digest(raw), sha)
                return raw
            return original_private(path, sha, **kwargs)
        self.events = []
        self._original_reference_reader = seed._references
        def media(backend, op, cfg, *, empty):
            # Explicit synthetic independent-device/WWID port: no native device
            # is opened. Actual same-FS fixture dirs never prove independence.
            self.events.append(('media', empty)); seed._bound(backend)
            return {'fixture_independent_media': True}
        def fresh(backend, op, old):
            self.events.append(('fresh', op.phase)); op.seal()
            return {'fixture_native_fence': True, 'schema_version': 33}
        def references(backend, op, label):
            self.events.append(('references', label)); self.op = op
            ref = seed._saved(backend, op, 'fixture-reference-' + label,
                {'source_rows': len(self.references), 'rows': self.references})
            return {'rows': copy.deepcopy(self.references), 'source_rows': len(self.references), 'pages': [ref]}
        for item in (patch.object(seed, 'private_bytes', side_effect=private),
                     patch.object(seed, '_qualify_media', side_effect=media),
                     patch.object(seed, '_fresh', side_effect=fresh),
                     patch.object(seed, '_references', side_effect=references),
                     patch.object(seed.migration, '_fence', return_value=None),
                     patch.object(seed.migration, '_sessions', return_value={'fixture': True})):
            item.start(); self.addCleanup(item.stop)
        self.reference_port = references
        self.addCleanup(self.cleanup_seed_fds)

    def cleanup_seed_fds(self):
        fds = {row[0] for row in getattr(self.backend, '_initial_blob_seed_fds', {}).values()}
        fds.update(getattr(self.backend, '_initial_blob_seed_native_fds', []))
        for fd in fds:
            try: os.close(fd)
            except OSError: pass

    def generation(self, role, raw, key=None):
        sha = digest(raw); key = key or sha[:2] + '/' + sha + '-' + str(uuid.uuid4())
        root = Path(self.sources[role]['path']); shard = root / key.split('/')[0]
        if not shard.exists():
            shard.mkdir(mode=0o700); os.chown(shard, 10004, 10004)
        path = root / key; path.write_bytes(raw); path.chmod(0o600); os.chown(path, 10004, 10004)
        return {'storage_key': key, 'sha256': sha, 'size': len(raw)}

    def scan_sources(self):
        return {role: blobs.blob_inventory(row['path'], deadline=time.monotonic() + 30, controls=True, online=False)
                for role, row in self.sources.items()}

    def run_seed(self):
        return seed.run(self.backend, self.case.lock, self.cfgref, self.authref)

    def markers(self):
        return [Path(row['host_path']) / blobs.MARKER for row in self.targets.values()
                if (Path(row['host_path']) / blobs.MARKER).exists()]

    def test_live_driver_actual_exit_and_all_physical_union_generations_seeded(self):
        result = self.run_seed()
        self.assertFalse(result['ready']); self.assertFalse(result['roles_created'])
        self.assertFalse(result['postgres_replica_created'])
        self.assertEqual(len(result['roots']), 4)
        for role, target in self.targets.items():
            actual = blobs.blob_inventory(target['host_path'], deadline=time.monotonic() + 30)
            expected = [self.cache] if role.startswith('cache') else sorted(
                [self.corpus_a, self.corpus_b], key=lambda row: row['storage_key'])
            self.assertEqual(actual['permanent'], expected)
            root = Path(target['host_path']); self.assertEqual((root.stat().st_uid, root.stat().st_gid), (10004, 10004))
            marker = root / blobs.MARKER
            self.assertEqual((marker.stat().st_uid, stat.S_IMODE(marker.stat().st_mode)), (0, 0o640))
            value = json.loads(marker.read_bytes())
            self.assertEqual((value['seed_operation'], value['managed_epoch'], value['role']),
                             (self.case.opid, self.cfg['managed_epoch'], role))
            self.assertEqual(value['manifest_sha256'], digest(canonical(expected)))
        self.assertEqual(self.scan_sources(), self.saved_originals)
        self.assertEqual(len(self.markers()), 4)
        self.assertTrue((Path(self.targets['cache_replica']['host_path']) / blobs.GUARDS / '.directory.lock').exists())
        self.assertEqual(self.events[:3], [('fresh', seed.PHASE), ('media', True), ('references', 'before')])
        self.case.lock.check(); self.backend._initial_driver_journal.check()

    def test_same_key_conflicting_identity_refuses_without_selector(self):
        source = copy.deepcopy(self.baseline)
        row = next(r for r in source['corpus_ssd_lookup']['permanent'] if r['storage_key'] == self.corpus_a['storage_key'])
        row['size'] += 1
        with self.assertRaisesRegex(Refused, 'conflicting_generation'): seed._union(source)

    def test_existing_source_file_xattr_is_unsupported_not_silently_lost(self):
        path = Path(self.sources['corpus_hdd']['path']) / self.corpus_a['storage_key']
        self.assertEqual(os.getxattr(path, 'user.openlegal_review'), b'fictional review metadata')
        with self.assertRaisesRegex(Refused, 'xattrs_unsupported'): self.run_seed()
        self.assertEqual(os.getxattr(path, 'user.openlegal_review'), b'fictional review metadata')
        self.assertEqual(self.markers(), [])
        self.assertFalse(any(Path(row['host_path']).exists() for row in self.targets.values()))

    def test_existing_source_shard_xattr_is_unsupported(self):
        with self.assertRaisesRegex(Refused, 'xattrs_unsupported'): self.run_seed()
        self.assertEqual(self.markers(), [])

    def test_existing_source_root_xattr_is_unsupported(self):
        with self.assertRaisesRegex(Refused, 'xattrs_unsupported'): self.run_seed()
        self.assertEqual(self.markers(), [])

    def test_prepared_parent_attribute_refused_before_destination_creation(self):
        parent = Path(self.targets['cache_primary']['parent']['path'])
        os.setxattr(parent, 'user.openlegal_inherited_policy', b'fictional parent policy')
        with self.assertRaisesRegex(Refused, 'xattrs_unsupported'): self.run_seed()
        self.assertFalse(Path(self.targets['cache_primary']['host_path']).exists())
        self.assertEqual(os.getxattr(parent, 'user.openlegal_inherited_policy'), b'fictional parent policy')

    def test_inherited_destination_attribute_is_refused_before_metadata_can_strip_it(self):
        opened = seed.os.open; injected = []
        # Generation shard validation needs its real SHA prefix; intercept
        # actual new permanent files using the complete UUID filename shape.
        def with_attribute(path, flags, *args, **kwargs):
            fd = opened(path, flags, *args, **kwargs)
            if flags & os.O_CREAT and __import__('re').fullmatch('[0-9a-f]{64}-[0-9a-f-]{36}', str(path)):
                os.setxattr(fd, 'user.openlegal_inherited_policy', b'fictional inherited metadata')
                injected.append(fd)
            return fd
        with patch.object(seed.os, 'open', side_effect=with_attribute):
            with self.assertRaisesRegex(Refused, 'xattrs_unsupported'): self.run_seed()
        self.assertTrue(injected); self.assertEqual(self.markers(), [])
        path = Path(self.targets['cache_primary']['host_path']) / self.cache['storage_key']
        self.assertEqual(os.getxattr(path, 'user.openlegal_inherited_policy'), b'fictional inherited metadata')
        self.assertEqual(self.scan_sources(), self.saved_originals)

    def test_late_source_attribute_addition_refuses_and_preserves_attribute(self):
        original = seed._copy_file; changed = False
        path = Path(self.sources['corpus_ssd_lookup']['path']) / self.corpus_b['storage_key']
        def annotate(*args):
            nonlocal changed
            original(*args)
            if not changed:
                before = path.stat(); os.setxattr(path, 'user.openlegal_review', b'late fictional review')
                os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns)); changed = True
        with patch.object(seed, '_copy_file', side_effect=annotate):
            with self.assertRaisesRegex(Refused, 'xattrs_unsupported'): self.run_seed()
        self.assertEqual(self.markers(), [])
        self.assertEqual(os.getxattr(path, 'user.openlegal_review'), b'late fictional review')

    def test_late_destination_attribute_refuses_before_marker(self):
        def references(backend, op, label):
            value = self.reference_port(backend, op, label)
            if label == 'after':
                path = Path(self.targets['corpus_primary']['host_path']) / self.corpus_b['storage_key']
                os.setxattr(path, 'user.openlegal_review', b'late target attribute')
            return value
        with patch.object(seed, '_references', side_effect=references):
            with self.assertRaisesRegex(Refused, 'xattrs_unsupported'): self.run_seed()
        self.assertEqual(self.markers(), [])

    def test_unavailable_xattr_syscall_is_never_treated_as_empty(self):
        with patch.object(seed.os, 'listxattr', side_effect=OSError(errno.EIO, 'fixture xattrs unavailable')):
            with self.assertRaisesRegex(Refused, 'xattrs_unavailable'): self.run_seed()
        self.assertEqual(self.markers(), [])
        self.assertFalse(any(Path(row['host_path']).exists() for row in self.targets.values()))

    def sql_page_port(self, rows, *, sentinel=None, bad_witness=False):
        def run(backend, op, binary, args, *, stdin=None, timeout=30, record=True, deadline=None):
            self.assertEqual(binary, 'docker'); self.assertEqual(timeout, 31)
            self.assertEqual(deadline, backend._driver_deadline)
            self.assertIn(b"statement_timeout='30s'", stdin)
            self.assertIn(b"transaction_timeout='35s'", stdin)
            self.assertIn(b"lock_timeout='1s'", stdin)
            self.assertIn('/usr/bin/env', args)
            self.assertEqual(args[args.index('/usr/bin/env'):args.index('psql')], ['/usr/bin/env', '-u', 'PGSERVICE'])
            self.assertIn('openlegal_migration', args)
            offset = int(__import__('re').search(rb'OFFSET ([0-9]+) LIMIT 128', stdin)[1])
            value = {'source_rows': len(rows) if sentinel is None else sentinel,
                     'rows': rows[offset:offset + 128]}
            raw = canonical(value); backend._sequence += 1
            dispatch = ['--config', str(backend._docker_config[0]), '--host', 'unix:///var/run/docker.sock']
            backend._artifact(op, 'command-%04d' % backend._sequence, {
                'binary': 'docker', 'arguments_sha256': digest(canonical(dispatch + args)),
                'stdin_sha256': digest(stdin), 'stdout_hex': raw.hex(), 'stdout_sha256': digest(raw),
                'stdout_bytes': len(raw), 'actual_exit_code': 7 if bad_witness else 0,
                'started': True, 'owned_processes_closed': True, 'expired': False,
                'interrupted': False, 'unexpected_descendants': False, 'output_limit_exceeded': False})
            self.events.append(('native-reference-page', offset))
            return raw
        return run

    def test_bounded_reference_pages_actual_command_pins_and_original_deadline(self):
        self.run_seed()
        # End the labelled whole-reader fixture port for this focused native
        # reader test. Only execution/SQL output is synthetic; all raw command
        # and page artifacts are really acknowledged/read from BOTH journals.
        original = self._original_reference_reader
        rows = []
        for n in range(257):
            sha = digest(('fictional-ref-' + str(n)).encode())
            rows.append({'role': 'corpus', 'storage_key': sha[:2] + '/' + sha + '-' + str(uuid.uuid4()),
                         'sha256': sha, 'size': n})
        rows.sort(key=lambda row: (row['role'], row['storage_key']))
        with patch.object(seed.driver.Backend, '_run', self.sql_page_port(rows)):
            result = original(self.backend, self.op, 'bounded-probe')
        self.assertEqual(result['rows'], rows)
        self.assertEqual(len(result['pages']), 3)
        self.assertEqual([ref['offset'] for ref in result['pages']], [0, 128, 256])
        self.backend._initial_driver_journal.check()

    def test_reference_raw_truncation_sentinel_and_false_exit_never_accepted(self):
        self.run_seed()
        for label, sentinel, false_exit in (('sentinel-probe', 200001, False), ('exit-probe', None, True)):
            with patch.object(seed.driver.Backend, '_run', self.sql_page_port([], sentinel=sentinel, bad_witness=false_exit)):
                with self.assertRaises(Refused):
                    self._original_reference_reader(self.backend, self.op, label)
            self.assertFalse(any(label in name for name in self.backend._initial_driver_journal.artifacts))

    def test_genuine_empty_cache_baseline_still_requires_live_empty_scan_and_sql(self):
        self.assertEqual(self.baseline['cache_hdd']['permanent'], [])
        result = self.run_seed()
        for role in ('cache_primary', 'cache_replica'):
            actual = blobs.blob_inventory(self.targets[role]['host_path'], deadline=time.monotonic() + 30)
            self.assertEqual(actual['permanent'], [])
            marker = json.loads((Path(self.targets[role]['host_path']) / blobs.MARKER).read_bytes())
            self.assertEqual(marker['manifest_sha256'], digest(canonical([])))
        self.assertFalse(result['ready'])
        self.assertEqual(self.scan_sources(), self.saved_originals)

    def test_old_controls_and_health_remain_original_and_are_never_adopted(self):
        self.assertTrue(self.baseline['corpus_hdd']['controls'])
        self.assertTrue(self.baseline['corpus_hdd']['health'])
        self.run_seed()
        self.assertEqual(self.scan_sources(), self.saved_originals)
        for role in ('corpus_primary', 'corpus_replica'):
            actual = blobs.blob_inventory(self.targets[role]['host_path'], deadline=time.monotonic() + 30)
            self.assertEqual(actual['health'], [])
            marker = json.loads((Path(self.targets[role]['host_path']) / blobs.MARKER).read_bytes())
            self.assertEqual(marker['seed_operation'], self.case.opid)
            self.assertEqual(marker['managed_epoch'], self.cfg['managed_epoch'])

    def test_prepared_parent_rebinding_midcopy_is_refused_without_marker(self):
        original = seed._copy_file; rebound = False
        def rebind(*args):
            nonlocal rebound
            original(*args)
            if not rebound:
                path = Path(self.targets['cache_primary']['parent']['path'])
                path.rename(path.with_name(path.name + '-detached')); path.mkdir(mode=0o700); rebound = True
        with patch.object(seed, '_copy_file', side_effect=rebind):
            with self.assertRaisesRegex(Refused, 'rebound'): self.run_seed()
        self.assertEqual(self.markers(), [])
        self.assertEqual(self.scan_sources(), self.saved_originals)

    def test_partial_copy_failure_retains_files_and_no_markers_or_retry(self):
        original = seed._copy_file
        def fail(*args):
            original(*args); raise OSError('synthetic write acknowledgement unknown')
        with patch.object(seed, '_copy_file', side_effect=fail):
            with self.assertRaises(OSError): self.run_seed()
        self.assertTrue(any(Path(row['host_path']).exists() for row in self.targets.values()))
        self.assertEqual(self.markers(), [])
        self.assertEqual(self.scan_sources(), self.saved_originals)
        with self.assertRaisesRegex(Refused, 'live_native_backend'): self.run_seed()

    def test_directory_barrier_failure_never_acknowledges_copy_or_markers(self):
        def fail(backend, targetfd, dirty):
            self.assertTrue(dirty)
            self.assertTrue(any(Path(row['host_path']).exists() for row in self.targets.values()))
            raise OSError(errno.EIO, 'synthetic directory durability acknowledgement failed')
        with patch.object(seed, '_copy_barrier', side_effect=fail):
            with self.assertRaisesRegex(OSError, 'directory durability'): self.run_seed()
        names = self.backend._initial_driver_journal.artifacts
        self.assertFalse(any('copy-verified-' in name or 'all-copies-correspondence' in name for name in names))
        self.assertEqual(self.markers(), [])
        self.assertEqual(self.scan_sources(), self.saved_originals)

    def test_late_source_byte_change_even_restored_mtime_refuses_markers(self):
        original = seed._copy_file; changed = False
        path = Path(self.sources['corpus_ssd_lookup']['path']) / self.corpus_b['storage_key']
        def alter(*args):
            nonlocal changed
            original(*args)
            if not changed:
                before = path.stat(); path.write_bytes(b'late fictional mutation')
                os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns)); changed = True
        with patch.object(seed, '_copy_file', side_effect=alter):
            with self.assertRaises(Refused): self.run_seed()
        self.assertEqual(self.markers(), [])
        raw = self.backend._initial_driver_journal.artifacts[seed.PHASE + '-failed.json']['raw']
        failed = json.loads(raw)
        self.assertFalse(failed['original_writes_permitted'])
        self.assertEqual(failed['source_integrity'], 'UNKNOWN')
        self.assertNotIn('original_roots_modified', failed)

    def test_changed_sql_references_refuses_all_markers_after_copy(self):
        def references(backend, op, label):
            value = self.reference_port(backend, op, label)
            if label == 'after': value['rows'] = []
            return value
        with patch.object(seed, '_references', side_effect=references):
            with self.assertRaisesRegex(Refused, 'sql_references_changed'): self.run_seed()
        self.assertEqual(self.markers(), [])
        self.assertTrue(all(Path(row['host_path']).exists() for row in self.targets.values()))

    def test_late_destination_mutation_refuses_markers(self):
        def references(backend, op, label):
            value = self.reference_port(backend, op, label)
            if label == 'after':
                path = Path(self.targets['corpus_primary']['host_path']) / self.corpus_b['storage_key']
                path.write_bytes(b'late destination mutation')
            return value
        with patch.object(seed, '_references', side_effect=references):
            with self.assertRaises(Refused): self.run_seed()
        self.assertEqual(self.markers(), [])

    def test_marker_late_ack_never_returns_success_preserves_partial(self):
        original = seed._write_control
        def expire(*args):
            journal = self.backend._initial_driver_journal
            self.assertIn(seed.PHASE + '-all-copies-correspondence.json', journal.output_hashes())
            original(*args); self.backend._driver_deadline = time.monotonic() - 1
        with patch.object(seed, '_write_control', side_effect=expire):
            with self.assertRaisesRegex(Refused, 'original_deadline'): self.run_seed()
        self.assertTrue(self.markers())
        self.assertNotIn(seed.PHASE + '-complete.json', self.backend._initial_driver_journal.artifacts)

    def test_preexisting_target_is_never_adopted(self):
        path = Path(self.targets['cache_primary']['host_path']); path.mkdir(mode=0o700)
        (path / 'foreign').write_bytes(b'not adopted')
        with self.assertRaisesRegex(Refused, 'parent_not_empty'): self.run_seed()
        self.assertEqual((path / 'foreign').read_bytes(), b'not adopted')
        self.assertEqual(self.markers(), [])

    def test_wrong_epoch_mapping_and_dangerous_authority_refuse_before_phase(self):
        cfg = dict(self.cfg); cfg['managed_epoch'] = str(uuid.uuid4())
        ref = self.case.save('wrong-epoch-seed.json', cfg)
        auth = dict(self.authority, inventory_sha256=ref['sha256'], seed_managed_epoch=cfg['managed_epoch'])
        authref = self.case.save('wrong-epoch-authority.json', auth)
        with self.assertRaisesRegex(Refused, 'seed_authority'):
            seed.run(self.backend, self.case.lock, ref, authref)
        for field in ('roles_permitted', 'ready_permitted', 'dataset_creation_permitted', 'provider_permitted'):
            value = dict(self.authority); value[field] = True
            authref = self.case.save('bad-' + field + '.json', value)
            with self.assertRaisesRegex(Refused, 'seed_authority'):
                seed.run(self.backend, self.case.lock, self.cfgref, authref)
        self.assertFalse(getattr(self.backend, '_initial_blob_seed_started', False))

    def test_schema1_or_wrong_read_media_profile_refuses_before_phase(self):
        for label in ('schema', 'profile', 'corpus', 'cache'):
            cfg = copy.deepcopy(self.cfg)
            if label == 'schema': cfg['schema_version'] = 1
            elif label == 'profile': cfg['media_profile'] = 'primary_hdd'
            else:
                role = label + '_primary'
                cfg['targets'][role]['media'] = copy.deepcopy(cfg['targets'][label + '_replica']['media'])
            ref = self.case.save('bad-profile-' + label + '.json', cfg)
            auth = dict(self.authority, inventory_sha256=ref['sha256'])
            authref = self.case.save('bad-profile-auth-' + label + '.json', auth)
            with self.subTest(label=label), self.assertRaises(Refused):
                seed.run(self.backend, self.case.lock, ref, authref)
        self.assertFalse(getattr(self.backend, '_initial_blob_seed_started', False))

    def test_raw_initial_driver_completion_tamper_refuses_live_handoff(self):
        journal = self.backend._initial_driver_journal
        path = journal.phase_paths[1][0] / 'driver-complete.json'
        path.write_bytes(b'{}\n')
        with self.assertRaises(Refused): self.run_seed()
        self.assertFalse(getattr(self.backend, '_initial_blob_seed_started', False))

@unittest.skipUnless(os.geteuid() == 0, 'UID10004 and real root cgroup-FD fixtures')
class BatchIOTests(unittest.TestCase):
    """Private helper calibration context, not a valid initial-seed operation.

    Real /tmp bytes/FDs/xattrs and real self cgroup FD; no MCP writer-closure,
    independent hardware, journal, SQL, Docker or Ready claim. Scheduling-only
    tests explicitly stub copy/barrier; durability tests execute real fsync.
    """
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='seed-batch-test-', dir='/tmp')
        self.addCleanup(self.tmp.cleanup); self.root = Path(self.tmp.name); self.root.chmod(0o700)
        self.source = self.root / 'source'; self.source.mkdir(mode=0o700); os.chown(self.source, 10004, 10004)
        self.parent = self.root / 'prepared'; self.parent.mkdir(mode=0o700)
        self.target = self.parent / 'data'; self.target.mkdir(mode=0o700); os.chown(self.target, 10004, 10004)
        self.backend = types.SimpleNamespace(_driver_deadline=time.monotonic() + 30,
            _initial_blob_seed_fds={}, _cgroup_fds={})
        group = Path('/proc/self/cgroup').read_text().splitlines()
        unified = [r[3:] for r in group if r.startswith('0::')]
        self.assertEqual(len(unified), 1)
        cgroup = Path('/sys/fs/cgroup') / unified[0].lstrip('/')
        # A namespace exposes its own cgroup at /sys/fs/cgroup even when the
        # /proc member name is host-relative; use its real opened kernel FD.
        if not cgroup.is_dir(): cgroup = Path('/sys/fs/cgroup')
        cgfd = os.open(cgroup, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        self.addCleanup(os.close, cgfd); st = os.fstat(cgfd)
        self.backend._cgroup_fds[str(cgroup)] = (cgfd, {'dev': st.st_dev, 'ino': st.st_ino})
        self.srcfd = seed._hold(self.backend, 'source', self.source, owner=10004)
        self.parentfd = seed._hold(self.backend, 'parent', self.parent, owner=0)
        self.dstfd = seed._hold(self.backend, 'target', self.target, owner=10004)
        for n in range(8):
            path = self.root / ('aux-' + str(n)); path.mkdir(mode=0o700); os.chown(path, 10004, 10004)
            seed._hold(self.backend, 'aux-' + str(n), path, owner=10004)
        self.assertEqual(len(self.backend._initial_blob_seed_fds), 11)
        self.addCleanup(lambda: [os.close(r[0]) for r in self.backend._initial_blob_seed_fds.values()])
        self.rows = []; self.origins = {}

    def generation(self, raw=b'fictional batch payload'):
        sha = digest(raw); key = sha[:2] + '/' + sha + '-' + str(uuid.uuid4())
        path = self.source / key; path.parent.mkdir(mode=0o700, exist_ok=True); os.chown(path.parent, 10004, 10004)
        path.write_bytes(raw); path.chmod(0o600); os.chown(path, 10004, 10004)
        row = {'storage_key': key, 'sha256': sha, 'size': len(raw)}
        self.rows.append(row); self.origins[('corpus', key)] = 'corpus_hdd'; return row

    def batch(self, rows=None, offset=0):
        return seed._copy_batch(self.backend, {'corpus_hdd': self.srcfd}, self.dstfd,
            self.rows if rows is None else rows, self.origins, 'corpus', offset)

    def test_real_partial_writes_fsync_each_file_then_dirty_shard_root_parent(self):
        self.generation(); self.generation()
        original_write = os.write; original_sync = os.fsync; events = []
        def partial(fd, data): return original_write(fd, data[:3])
        def sync(fd):
            events.append((stat.S_ISDIR(os.fstat(fd).st_mode), os.readlink('/proc/self/fd/' + str(fd))))
            original_sync(fd)
        # Keep both tiny files in one batch without changing its elapsed clock.
        with patch.object(seed, 'BATCH_SECONDS', 30), patch.object(seed.os, 'write', side_effect=partial), \
                patch.object(seed.os, 'fsync', side_effect=sync):
            self.assertEqual(self.batch(), 2)
        self.assertEqual([isdir for isdir, _ in events], [False, False, True, True, True])
        self.assertEqual([p for _, p in events[-2:]], [str(self.target), str(self.parent)])
        for row in self.rows:
            self.assertEqual((self.target / row['storage_key']).read_bytes(), (self.source / row['storage_key']).read_bytes())
            self.assertEqual((self.target / row['storage_key']).stat().st_uid, 10004)
        self.assertFalse((self.target / blobs.MARKER).exists())

    def test_scheduling_port_limits_64_files_4mib_and_elapsed_before_new_file(self):
        rows = [{'storage_key': str(n), 'size': 1} for n in range(65)]
        self.origins = {('corpus', r['storage_key']): 'corpus_hdd' for r in rows}
        calls = []
        with patch.object(seed, '_copy_file', side_effect=lambda *a: calls.append(a[3]['storage_key'])), \
                patch.object(seed, '_copy_barrier'), patch.object(seed, 'BATCH_SECONDS', 30):
            self.assertEqual(self.batch(rows), 64); self.assertEqual(len(calls), 64)
            calls.clear(); rows[0]['size'] = 4 * 1024**2
            self.assertEqual(self.batch(rows), 1); self.assertEqual(len(calls), 1)
            calls.clear(); rows[0]['size'] += 1
            self.assertEqual(self.batch(rows), 1); self.assertEqual(len(calls), 1)
        rows[0]['size'] = 1
        def slow(*args):
            calls.append(args[3]['storage_key']); clock[0] += 0.250
        clock = [time.monotonic()]; calls.clear()
        with patch.object(seed.time, 'monotonic', side_effect=lambda: clock[0]), \
                patch.object(seed, '_copy_file', side_effect=slow), patch.object(seed, '_copy_barrier'):
            self.assertEqual(self.batch(rows), 1); self.assertEqual(len(calls), 1)

    def test_real_fsync_failure_preserves_partial_and_closes_dirty_shard_fd(self):
        self.generation(); original = os.fsync; dirty_fds = []
        def fail(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                dirty_fds.append(fd); raise OSError(errno.EIO, 'fixture shard fsync failed')
            return original(fd)
        with patch.object(seed.os, 'fsync', side_effect=fail):
            with self.assertRaisesRegex(OSError, 'shard fsync'): self.batch()
        self.assertTrue((self.target / self.rows[0]['storage_key']).exists())
        self.assertFalse((self.target / blobs.MARKER).exists())
        for fd in dirty_fds:
            with self.assertRaises(OSError): os.fstat(fd)

    def test_deadline_expired_inside_actual_file_fsync_cannot_return_success(self):
        self.generation(); original = os.fsync
        def expire(fd):
            original(fd); self.backend._driver_deadline = time.monotonic() - 1
        with patch.object(seed.os, 'fsync', side_effect=expire):
            with self.assertRaisesRegex(Refused, 'original_deadline'): self.batch()
        self.assertFalse((self.target / blobs.MARKER).exists())

    def test_parent_rebind_after_file_refuses_before_directory_barrier(self):
        self.generation(); original = seed._copy_file
        def rebind(*args):
            original(*args); self.parent.rename(self.root / 'detached'); self.parent.mkdir(mode=0o700)
        with patch.object(seed, '_copy_file', side_effect=rebind):
            with self.assertRaisesRegex(Refused, 'rebound'): self.batch()
        self.assertTrue((self.root / 'detached' / 'data' / self.rows[0]['storage_key']).exists())
        self.assertFalse(self.target.exists())

    def test_dirty_shard_rebind_or_late_attribute_refuses_before_acceptance(self):
        self.generation(); original = seed._copy_file
        def replace(*args):
            original(*args); path = self.target / self.rows[0]['storage_key'].split('/')[0]
            path.rename(path.with_name('detached-shard')); path.mkdir(mode=0o700); os.chown(path, 10004, 10004)
        with patch.object(seed, '_copy_file', side_effect=replace):
            with self.assertRaisesRegex(Refused, 'shard_identity'): self.batch()

    def test_late_destination_xattr_inside_fsync_and_unowned_active_fd_refuse(self):
        self.generation(); original = os.fsync
        def annotate(fd):
            original(fd); os.setxattr(fd, 'user.openlegal_review', b'late unsupported metadata')
        with patch.object(seed.os, 'fsync', side_effect=annotate):
            with self.assertRaisesRegex(Refused, 'xattrs_unsupported'): self.batch()
        with self.assertRaisesRegex(Refused, 'unowned_active_fd'):
            seed._fast_bound(self.backend, (123456789,))

    def test_metadata_walk_checks_every_file_with_batched_global_ancestry(self):
        for _ in range(3): self.generation()
        original = seed._empty_xattrs; visited = []
        def attrs(fd):
            if stat.S_ISREG(os.fstat(fd).st_mode): visited.append(os.readlink('/proc/self/fd/' + str(fd)))
            return original(fd)
        with patch.object(seed, '_empty_xattrs', side_effect=attrs):
            seed._metadata_tree(self.backend, self.source)
        self.assertEqual(set(visited), {str(self.source / row['storage_key']) for row in self.rows})
        path = self.source / self.rows[0]['storage_key']; os.setxattr(path, 'user.openlegal_review', b'unsupported')
        with self.assertRaisesRegex(Refused, 'xattrs_unsupported'):
            seed._metadata_tree(self.backend, self.source)


if __name__ == '__main__':
    unittest.main(verbosity=2)
