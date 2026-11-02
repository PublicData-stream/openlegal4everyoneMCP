"""Offline bootstrap grammar/real tiny filesystem tests; no Docker/PG/network.

Command ordering fixtures are synthetic. They do not establish a real basebackup,
promotion, independently backed media, protected role grant or native readiness.
"""
import copy
import os
from pathlib import Path
import stat
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bootstrap_backend as b
from interface import Refused
from inventory import canonical, digest
from copy_tree import copy_tree


def uid(): return str(uuid.uuid4())


class Journal:
    def __init__(self): self.raw = {}
    def save_bytes(self, name, raw):
        if name in self.raw: raise FileExistsError(name)
        self.raw[name] = raw; return digest(raw)
    def save(self, name, value): return self.save_bytes(name, canonical(value))
    def output_hashes(self): return {k: digest(v) for k, v in self.raw.items()}


class Fixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='manual-bootstrap-test-', dir='/tmp')
        self.addCleanup(self.tmp.cleanup); self.root = Path(self.tmp.name); self.root.chmod(0o700)
        self.policy = {'new_managed_epoch': uid(), 'replication_peer': {
            'host': '192.0.2.7', 'port': 5432, 'role': 'mcp_replica', 'application_name': 'mcp_peer',
            'client_dn': '/CN=replica', 'issuer_dn': '/CN=ca', 'client_serial': '123',
            **{key: {'path': str(self.root / key), 'sha256': 'a' * 64}
               for key in ('ca', 'certificate', 'private_key')}},
            'reseed_target': {'dataset': 'tank/reseed', 'path': str(self.root / 'new-pg'), 'uid': 70, 'gid': 70},
            'target_configuration': {'path': str(self.root / 'prepared.json'), 'sha256': 'a' * 64}}
        self.op = types.SimpleNamespace(operation_id=uid(), phase='reseed', action='manual_failover',
            inventory={'managed_epoch': uid(), 'linux': {'manual_recovery': self.policy,
                'namespace': 'openlegal-test', 'workloads': [], 'jobs': [],
                'postgres': {'primary': {'image_id': 'sha256:' + 'f' * 64}}},
                'original_datasets': [{'mountpoint': str(self.root / 'old')}], 'active_datasets': [],
                'ssd_trees': [{'path': str(self.root / 'ssd'), 'roles': ['postgres'],
                               'max_bytes': 64 * 1024**3, 'max_files': 4096}],
                'journal_roots': [{'path': str(self.root / 'ja')}, {'path': str(self.root / 'jb')}],
                'backup_root': {'path': str(self.root / 'backup')}},
            journal=Journal(), inventory_sha256='1' * 64, source_graph_sha256='2' * 64,
            authority_sha256='3' * 64, seal=lambda: None)
        self.doc = self.document()

    def target(self, name):
        parent = self.root / name; parent.mkdir(mode=0o700)
        s = parent.stat()
        return {'host_path': str(parent / 'data'), 'container_path': '/data/' + name,
            'parent': {'path': str(parent), 'identity': {'dev': s.st_dev, 'ino': s.st_ino},
                'kernel_filesystem_id': os.statvfs(parent).f_fsid, 'filesystem_id': 'fixture-' + name},
            'relative': 'data'}

    def document(self):
        resources = []; workloads = {}
        for role, kind in (('serving_workload', 'deployment'), ('scheduler_workload', 'deployment'),
                           ('collection_job', 'job')):
            name = 'new-' + role.replace('_', '-'); identity = uid()
            workloads[role] = {'name': name, 'uid': identity}
            resources.append({'kind': kind, 'name': name, 'namespace': 'openlegal-test',
                'uid': identity, 'spec_sha256': 'a' * 64, 'resource_version': '12', 'active': False})
        doc = {'schema_version': 2, 'media_profile': b.MEDIA_PROFILE, 'kind': 'openlegal_manual_prepared_configuration',
            'new_epoch': self.policy['new_managed_epoch'], 'resources': resources, **workloads,
            'blob_roots': {role: self.target(role) for role in b.ROLES},
            'index_target': self.target('index'), 'capacity_reserve_bytes': 1024**3,
            'serving_runtime_image_id': 'sha256:' + 'e' * 64,
            'network': {'id': 'b' * 64, 'name': 'fixture-internal', 'replica_address': '192.0.2.7'},
            'upstream': {'host': '192.0.2.8', 'port': 5432, 'server_name': 'primary.example.invalid',
                         'server_certificate_sha256': 'c' * 64},
            'server_trust': {key: {'path': str(self.root / ('server-' + key)), 'sha256': 'd' * 64}
                             for key in ('ca', 'certificate', 'private_key', 'hba')}}
        for number, (role, target) in enumerate(doc['blob_roots'].items()):
            if b.ROLE_MEDIA[role] == 'hdd':
                target['media'] = {'kind': 'hdd', 'dataset': 'tank/new-' + role,
                                   'guid': str(123 + number), 'mountpoint': target['parent']['path']}
                target['parent']['filesystem_id'] = 'zfs:' + str(123 + number)
            else:
                target['media'] = {'kind': 'ssd', 'physical_leaves': ['/dev/ssd']}
                dev = target['parent']['identity']['dev']
                target['parent']['filesystem_id'] = 'block:' + str(os.major(dev)) + ':' + str(os.minor(dev))
        doc['postgres_spec'] = b.postgres_spec(self.op, doc)
        return doc

    def read_doc(self, doc=None):
        raw = canonical(self.doc if doc is None else doc)
        with patch.object(b, '_scope', return_value=self.policy), \
                patch.object(b, 'private_bytes', return_value=raw):
            return b.read_prepared_configuration(self.op)


class GrammarTests(Fixture):
    def test_positive_closed_metadata_only_document(self):
        self.assertEqual(self.read_doc(), self.doc)
    def test_schema1_and_primary_hdd_corpus_profile_are_incompatible(self):
        for field, value in (('schema_version', 1), ('media_profile', 'primary_hdd')):
            changed = copy.deepcopy(self.doc); changed[field] = value
            with self.subTest(field=field), self.assertRaises(Refused): self.read_doc(changed)
        changed = copy.deepcopy(self.doc)
        changed['blob_roots']['corpus_primary']['media'] = copy.deepcopy(changed['blob_roots']['corpus_replica']['media'])
        with self.assertRaisesRegex(Refused, 'media_profile'): self.read_doc(changed)

    def test_unknown_hooks_or_raw_kube_objects_refuse(self):
        for key in ('sql', 'command', 'create_roles', 'object', 'template'):
            changed = copy.deepcopy(self.doc); changed[key] = 'forbidden'
            with self.subTest(key=key), self.assertRaises(Refused): self.read_doc(changed)
    def test_wrong_epoch_and_reused_writer_uid_refuse(self):
        changed = copy.deepcopy(self.doc); changed['new_epoch'] = uid()
        with self.assertRaises(Refused): self.read_doc(changed)
        self.op.inventory['linux']['workloads'] = [self.doc['resources'][0]]
        with self.assertRaises(Refused): self.read_doc()
    def test_non_numeric_upstream_or_unpinned_network_refuse(self):
        for section, key, value in (('upstream', 'host', 'db.example.invalid'),
                                   ('upstream', 'host', '0.0.0.0'),
                                   ('upstream', 'server_name', 'x password=secret'),
                                   ('network', 'id', 'bridge'),
                                   ('network', 'replica_address', '192.0.2.99')):
            changed = copy.deepcopy(self.doc); changed[section][key] = value
            with self.subTest(section=section, key=key), self.assertRaises(Refused): self.read_doc(changed)
    def test_no_arbitrary_postgres_arguments_mounts_or_labels(self):
        for key, value in (('arguments', ['-c', 'shared_preload_libraries=evil']),
                           ('mounts', []), ('labels', {}), ('network_mode', 'host')):
            changed = copy.deepcopy(self.doc); changed['postgres_spec'][key] = value
            with self.subTest(key=key), self.assertRaises(Refused): self.read_doc(changed)
    def test_targets_cannot_overlap_original_or_each_other(self):
        changed = copy.deepcopy(self.doc)
        changed['blob_roots']['cache_replica'] = copy.deepcopy(changed['blob_roots']['cache_primary'])
        with self.assertRaises(Refused): self.read_doc(changed)
    def test_active_dataset_is_preserved_and_not_a_new_target(self):
        self.op.inventory['active_datasets'] = [{'mountpoint': self.doc['index_target']['parent']['path']}]
        with self.assertRaises(Refused): self.read_doc()
        changed = copy.deepcopy(self.doc); row = changed['blob_roots']['cache_primary']
        row['parent']['path'] = str(self.root / 'old' / 'child')
        row['host_path'] = row['parent']['path'] + '/data'
        with self.assertRaises(Refused): self.read_doc(changed)
    def test_traversal_target_or_missing_resource_refuse(self):
        changed = copy.deepcopy(self.doc); changed['index_target']['relative'] = '../elsewhere'
        with self.assertRaises(Refused): self.read_doc(changed)
        changed = copy.deepcopy(self.doc); changed['resources'] = changed['resources'][:2]
        with self.assertRaises(Refused): self.read_doc(changed)
    def test_conninfo_uses_numeric_hostaddr_verifyfull_and_no_password(self):
        backup = b._conninfo(self.op, self.doc, backup=True)
        normal = b._conninfo(self.op, self.doc, backup=False)
        self.assertIn('hostaddr=192.0.2.8', backup); self.assertIn('sslmode=verify-full', backup)
        self.assertIn('passfile=/dev/null', backup)
        self.assertNotIn('application_name=mcp_peer', backup)
        self.assertIn('application_name=mcp_peer', normal)
        self.assertNotIn('password=', normal)
    def test_mount_cli_omits_invalid_bare_rw_and_rejects_comma(self):
        args = b._mount_args([{'source': '/tmp/source', 'destination': '/data', 'rw': True, 'type': 'bind'},
                              {'source': '/tmp/cert', 'destination': '/cert', 'rw': False, 'type': 'bind'}])
        self.assertEqual(args, ['--mount', 'type=bind,src=/tmp/source,dst=/data',
                               '--mount', 'type=bind,src=/tmp/cert,dst=/cert,readonly'])
        with self.assertRaises(Refused): b._mount_args([
            {'source': '/tmp/x,ro', 'destination': '/data', 'rw': True, 'type': 'bind'}])
    def test_create_ack_is_exact_cid_plus_lf(self):
        self.assertEqual(b._created_id(b'a' * 64 + b'\n'), 'a' * 64)
        for raw in (b'a' * 64, b'a' * 64 + b'\r\n', b'a' * 64 + b'\nextra', b'', b'Z' * 64 + b'\n'):
            with self.subTest(raw=raw), self.assertRaises(Refused): b._created_id(raw)
    def test_postgres_config_filename_is_operation_unique(self):
        first = b.postgres_spec(self.op, self.doc)
        self.op.operation_id = uid(); second = b.postgres_spec(self.op, self.doc)
        self.assertNotEqual(first['arguments'], second['arguments'])
        self.assertEqual(first['entrypoint'], ['/usr/local/bin/postgres'])


@unittest.skipUnless(os.geteuid() == 0, 'production-owner tiny filesystem fixtures need root')
class FilesystemTests(Fixture):
    def blob(self, name='source', payload=b'legal-body-sentinel-not-journalled'):
        path = self.root / name; path.mkdir(mode=0o700); os.chown(path, 10004, 10004)
        sha = digest(payload); shard = path / sha[:2]; shard.mkdir(mode=0o700); os.chown(shard, 10004, 10004)
        file = shard / (sha + '-' + uid()); file.write_bytes(payload); file.chmod(0o600); os.chown(file, 10004, 10004)
        return path, file
    def inventory(self, path, **kwargs): return b.blob_inventory(path, deadline=time.monotonic() + 10, **kwargs)
    def test_actual_file_digest_and_no_body_returned(self):
        path, file = self.blob(); value = self.inventory(path)
        self.assertEqual(value['permanent'][0]['sha256'], digest(file.read_bytes()))
        self.assertNotIn(b'legal-body-sentinel', canonical(value))
        self.assertEqual(len(value['permanent']), 1)
    def test_wrong_file_mode_uid_hardlink_and_symlink_refuse(self):
        for variant in ('mode', 'uid', 'hardlink', 'symlink'):
            path, file = self.blob(variant)
            if variant == 'mode': file.chmod(0o644)
            elif variant == 'uid': os.chown(file, 0, 0)
            elif variant == 'hardlink': os.link(file, path / 'alias')
            else:
                payload = file.read_bytes(); file.unlink(); actual = self.root / ('outside-' + variant)
                actual.write_bytes(payload); file.symlink_to(actual)
            with self.subTest(variant=variant), self.assertRaises(Refused): self.inventory(path)
    def test_wrong_shard_mode_and_unknown_root_control_refuse(self):
        path, file = self.blob(); file.parent.chmod(0o755)
        with self.assertRaises(Refused): self.inventory(path)
        file.parent.chmod(0o700); (path / '.unknown-control').write_bytes(b'')
        with self.assertRaises(Refused): self.inventory(path)
    def test_content_digest_mismatch_refuses(self):
        path, file = self.blob(); file.write_bytes(b'changed'); file.chmod(0o600)
        with self.assertRaises(Refused): self.inventory(path)
    def health(self, path, payload):
        shard = path / '00'
        if not shard.exists(): shard.mkdir(mode=0o700); os.chown(shard, 10004, 10004)
        name = '.stage-' + '0' * 64 + '-00000000-0000-0000-0000-000000000000.' + uuid.uuid4().hex * 2
        file = shard / name; file.write_bytes(payload); file.chmod(0o600); os.chown(file, 10004, 10004)
        return file
    def test_exact_health_bytes_are_separate_nonpermanent(self):
        path, _ = self.blob(); self.health(path, b''); self.health(path, b'blob-health\n')
        value = self.inventory(path)
        self.assertEqual(len(value['permanent']), 1); self.assertEqual(len(value['health']), 2)
        self.assertEqual(sorted(r['size'] for r in value['health']), [0, 12])
    def test_health_wrong_content_unknown_stage_or_ninth_refuse(self):
        path, _ = self.blob(); health = self.health(path, b'wrong')
        with self.assertRaises(Refused): self.inventory(path)
        health.unlink()
        for _ in range(9): self.health(path, b'')
        with self.assertRaises(Refused): self.inventory(path)
    def test_online_final_gate_validates_late_health_bytes_and_unknown_names(self):
        path, _ = self.blob(); self.health(path, b'')
        original = b._recheck
        def inject(payload, unknown=False):
            def check(*args, **kwargs):
                file = self.health(path, payload)
                if unknown: file.rename(file.with_name('.stage-unknown'))
                return original(*args, **kwargs)
            return check
        with patch.object(b, '_recheck', side_effect=inject(b'bad')):
            with self.assertRaises(Refused): self.inventory(path, online=True)
        for file in (path / '00').iterdir(): file.unlink()
        self.health(path, b'')
        with patch.object(b, '_recheck', side_effect=inject(b'', unknown=True)):
            with self.assertRaises(Refused): self.inventory(path, online=True)
    def test_online_known_health_is_not_a_permanent_or_fulltree_equality_claim(self):
        path, _ = self.blob(); self.health(path, b'')
        before = self.inventory(path); original = b._recheck
        def check(*args, **kwargs):
            self.health(path, b'blob-health\n'); return original(*args, **kwargs)
        with patch.object(b, '_recheck', side_effect=check): after = self.inventory(path, online=True)
        self.assertEqual(before['permanent'], after['permanent'])
        self.assertEqual(len(self.inventory(path)['health']), 2)
    def test_source_witness_detects_equal_bytes_inode_replacement(self):
        path, file = self.blob(); first = self.inventory(path)
        payload = file.read_bytes(); original_time = file.stat().st_mtime_ns
        file.unlink(); file.write_bytes(payload); file.chmod(0o600); os.chown(file, 10004, 10004)
        os.utime(file, ns=(original_time, original_time))
        second = self.inventory(path)
        self.assertEqual(first['permanent'], second['permanent'])
        self.assertNotEqual(first['private_witness_sha256'], second['private_witness_sha256'])
    def test_mint_marker_refuses_manifest_for_another_actual_root(self):
        source, _ = self.blob('one'); other, _ = self.blob('two')
        manifest = self.inventory(source)
        target = self.doc['blob_roots']['cache_primary']; target = copy.deepcopy(target)
        target['host_path'] = str(other)
        target['parent']['kernel_filesystem_id'] = os.statvfs(other).f_fsid
        with self.assertRaises(Refused): b._mint_marker(self.op, 'cache_primary', target, manifest, [])
        self.assertFalse((other / b.MARKER).exists())
    def test_inventory_reopen_rebind_cannot_drop_unreferenced_generation(self):
        source, first = self.blob('identity-a'); replacement, _ = self.blob('identity-b')
        extra = first.parent / (digest(b'orphan') + '-' + uid())
        shard = source / digest(b'orphan')[:2]
        if not shard.exists(): shard.mkdir(mode=0o700); os.chown(shard, 10004, 10004)
        extra = shard / extra.name; extra.write_bytes(b'orphan'); extra.chmod(0o600); os.chown(extra, 10004, 10004)
        before = self.inventory(source)
        marker = {'schema_version': 1, 'kind': 'openlegal_blob_seed',
            'managed_epoch': self.policy['new_managed_epoch'], 'seed_operation': self.op.operation_id,
            'role': 'cache_primary', 'manifest_sha256': before['permanent_sha256'],
            'correspondence_sha256': digest(canonical([])), 'filesystem_id': 'fixture'}
        (replacement / b.MARKER).write_bytes(canonical(marker)); (replacement / b.MARKER).chmod(0o640)
        os.chown(replacement / b.MARKER, 0, 10004)
        target = {'host_path': str(source), 'container_path': '/blob', 'parent': {
            'identity': {'dev': source.stat().st_dev}, 'kernel_filesystem_id': os.statvfs(source).f_fsid,
            'filesystem_id': 'fixture'}}
        original = b.blob_inventory
        def rebound(path, **kwargs):
            observed = original(path, **kwargs)
            source.rename(source.with_name('identity-old')); replacement.rename(source)
            return observed
        with patch.object(b, 'blob_inventory', side_effect=rebound):
            with self.assertRaisesRegex(Refused, 'inventory_root_replaced'):
                b._read_root_row(self.op, 'cache_primary', target, [], active=False)
    def test_parent_absent_identity_mode_and_rebind_gates(self):
        value = self.doc['blob_roots']['cache_primary']
        fd = b._parent(value, absent=True); os.close(fd)
        Path(value['host_path']).mkdir()
        with self.assertRaises(Refused): b._parent(value, absent=True)
        Path(value['host_path']).rmdir()
        parent = Path(value['parent']['path']); moved = parent.with_name(parent.name + '-old')
        parent.rename(moved); parent.mkdir(mode=0o700)
        with self.assertRaises(Refused): b._parent(value, absent=True)
    def test_exact_copy_control_archive_precedes_new_destination_only_retirement(self):
        source, _ = self.blob(); marker = source / b.MARKER
        marker_raw = canonical({'schema_version': 1, 'kind': 'openlegal_blob_seed'})
        marker.write_bytes(marker_raw); marker.chmod(0o640); os.chown(marker, 0, 10004)
        s = source.stat(); record = {'path': str(source), 'identity': {'dev': s.st_dev, 'ino': s.st_ino},
            'uid': s.st_uid, 'gid': s.st_gid, 'mode': 0o700, 'roles': ['cache'],
            'max_files': 100, 'max_bytes': 4096, 'symlinks': {}}
        target = self.root / 'copied'; copy_tree(record, target, time.monotonic() + 10)
        before = self.inventory(target)
        b._retire_copied_controls(self.op, 'cache_primary', target, before)
        self.assertTrue(marker.exists()); self.assertEqual(marker.read_bytes(), marker_raw)
        self.assertFalse((target / b.MARKER).exists())
        self.assertIn(marker_raw, self.op.journal.raw.values())
        self.assertEqual(self.inventory(target)['permanent'], self.inventory(source)['permanent'])
    def test_archive_failure_never_unlinks_copied_control(self):
        path, _ = self.blob(); marker = path / b.MARKER
        marker.write_bytes(canonical({'schema_version': 1, 'kind': 'openlegal_blob_seed'}))
        marker.chmod(0o640); os.chown(marker, 0, 10004)
        inventory = self.inventory(path)
        with patch.object(self.op.journal, 'save_bytes', side_effect=OSError('synthetic-fsync')):
            with self.assertRaises(OSError): b._retire_copied_controls(self.op, 'cache_primary', path, inventory)
        self.assertTrue(marker.exists())
    def test_exclusive_writer_never_overwrites(self):
        fd = b.root_fd(self.root)
        try:
            b._write_exclusive(fd, 'one', b'first', 0, 0, 0o600)
            with self.assertRaises(FileExistsError): b._write_exclusive(fd, 'one', b'second', 0, 0, 0o600)
        finally: os.close(fd)
        self.assertEqual((self.root / 'one').read_bytes(), b'first')
    def postgres_data(self):
        path = Path(self.policy['reseed_target']['path']); path.mkdir(mode=0o700); os.chown(path, 70, 70)
        for name in ('pg_tblspc', 'pg_wal'):
            (path / name).mkdir(mode=0o700); os.chown(path / name, 70, 70)
        for name, raw in (('PG_VERSION', b'18\n'), ('postgresql.auto.conf', b"primary_conninfo='password=SENTINEL'\n")):
            (path / name).write_bytes(raw); (path / name).chmod(0o600); os.chown(path / name, 70, 70)
        return path
    def test_new_destination_configuration_removes_inherited_hooks_without_journalling_secret(self):
        path = self.postgres_data(); result = b._postgres_configuration(self.op, self.doc)
        raw = canonical(result)
        self.assertNotIn(b'SENTINEL', raw)
        self.assertNotIn(b'password=', (path / 'postgresql.auto.conf').read_bytes())
        self.assertIn(b'application_name=mcp_peer', (path / 'postgresql.auto.conf').read_bytes())
        config = path / ('openlegal-manual-' + self.op.operation_id + '.conf')
        self.assertIn(b'full_page_writes=on', config.read_bytes())
        self.assertIn(b'fsync=on', config.read_bytes())
        self.assertEqual(stat.S_IMODE(config.stat().st_mode), 0o600)
        self.assertEqual(config.stat().st_uid, 70)
    def test_external_tablespace_or_wal_symlink_refuses_before_config_replacement(self):
        path = self.postgres_data(); original = (path / 'postgresql.auto.conf').read_bytes()
        (path / 'pg_tblspc' / 'outside').symlink_to('/tmp')
        with self.assertRaises(Refused): b._postgres_configuration(self.op, self.doc)
        self.assertEqual((path / 'postgresql.auto.conf').read_bytes(), original)
        (path / 'pg_tblspc' / 'outside').unlink(); (path / 'pg_wal').rmdir()
        (path / 'pg_wal').symlink_to('/tmp')
        with self.assertRaises(Refused): b._postgres_configuration(self.op, self.doc)
        self.assertEqual((path / 'postgresql.auto.conf').read_bytes(), original)
    def test_corrupt_or_linked_config_refuses_without_touching_external_file(self):
        path = self.postgres_data(); original = path / 'postgresql.auto.conf'; original.unlink()
        outside = self.root / 'outside-config'; outside.write_bytes(b'do-not-change')
        original.symlink_to(outside)
        with self.assertRaises(Refused): b._postgres_configuration(self.op, self.doc)
        self.assertEqual(outside.read_bytes(), b'do-not-change')


class ResourceTests(Fixture):
    def items(self):
        result = {}
        for row in self.doc['resources']:
            spec = {'replicas': 0} if row['kind'] == 'deployment' else {'suspend': True}
            row['spec_sha256'] = digest(canonical(spec))
            result[row['name']] = {'metadata': {'name': row['name'], 'uid': row['uid'],
                'namespace': row['namespace'], 'resourceVersion': row['resource_version']}, 'spec': spec}
        return result
    def backend(self, items):
        return types.SimpleNamespace(_kube=lambda op, args: items[args[-3]],
            _artifact=lambda op, name, value: op.journal.save(op.phase + '-' + name + '.json', value))
    def test_real_spec_projection_and_no_secret_plaintext_journal(self):
        items = self.items(); backend = self.backend(items)
        self.assertEqual(b._resources(backend, self.op, self.doc), self.doc['resources'])
        self.assertEqual(set(b._resource_projection('secret', {'type': 'Opaque',
            'data': {'password': 'sentinel-secret'}, 'metadata': {}})), {'type', 'data', 'immutable'})
        self.assertNotIn(b'sentinel-secret', b''.join(self.op.journal.raw.values()))
    def test_active_or_uid_spec_rv_changed_refuse(self):
        for change in ('active', 'uid', 'spec', 'rv'):
            items = self.items(); item = items[self.doc['serving_workload']['name']]
            if change == 'active': item['status'] = {'readyReplicas': 1}
            if change == 'uid': item['metadata']['uid'] = uid()
            if change == 'spec': item['spec']['injected'] = True
            if change == 'rv': item['metadata']['resourceVersion'] = '13'
            with self.subTest(change=change), self.assertRaises(Refused):
                b._resources(self.backend(items), self.op, self.doc)
    def test_job_with_prior_start_refuses(self):
        items = self.items(); items[self.doc['collection_job']['name']]['status'] = {'startTime': 'old'}
        with self.assertRaises(Refused): b._resources(self.backend(items), self.op, self.doc)


class PhysicalMediaTests(Fixture):
    """Actual held /tmp FDs; kernel-mount and independent-hardware ports synthetic."""
    def setUp(self):
        super().setUp()
        self.op.inventory['original_datasets'][0]['pool'] = 'tank'
        leaves = [{'path': '/dev/' + name, 'major': 8, 'minor': number * 16,
                   'sysfs_path': '/sys/devices/fixture/' + name, 'identity_file': 'wwid',
                   'identity_sha256': str(number + 1) * 64} for number, name in enumerate(('hdd', 'ssd'))]
        self.op.inventory['linux']['failure_domains'] = {'hdd': ['/dev/hdd'], 'ssd': ['/dev/ssd'],
                                                        'leaf_devices': leaves}
        item = patch.object(b, '_mounts', return_value=[]); item.start(); self.addCleanup(item.stop)
        item = patch.object(b, '_path_mount', side_effect=self.mount); item.start(); self.addCleanup(item.stop)
        item = patch.object(b, '_no_nested_mounts'); item.start(); self.addCleanup(item.stop)

    def mount(self, fd, path, table):
        media = next(r['media'] for r in self.doc['blob_roots'].values() if r['parent']['path'] == path)
        return {'filesystem': 'zfs' if media['kind'] == 'hdd' else 'ext4',
                'source': media.get('dataset', '/dev/ssd'), 'mountpoint': path, 'root': '/'}

    def media_backend(self, *, ssd=None, wrong_guid=False, same_wwid=False):
        def run(op, binary, args, **kw):
            media = next(r['media'] for r in self.doc['blob_roots'].values() if r['media'].get('dataset') == args[-1])
            return ('guid\t' + ('999' if wrong_guid else media['guid']) + '\nmountpoint\t'
                    + media['mountpoint'] + '\nmounted\tyes\n').encode()
        return types.SimpleNamespace(_run=run, _pool_devices=lambda op: {'/dev/hdd'},
            _device_identity=lambda row: 'same' if same_wwid else row['path'],
            _device_leaves=lambda dev: {'/dev/ssd'} if ssd is None else ssd,
            _artifact=lambda op, name, value: None)

    def test_synthetic_mount_hardware_ports_fixed_corpus_ssd_profile(self):
        result = b._media(self.media_backend(), self.op, self.doc, absent=True)
        self.assertEqual(result['cache_primary']['media']['kind'], 'hdd')
        self.assertEqual(result['corpus_primary']['physical_leaves'], ['/dev/ssd'])
        self.assertEqual(result['corpus_replica']['physical_leaves'], ['/dev/hdd'])

    def test_same_wwid_different_device_names_refuses(self):
        with self.assertRaisesRegex(Refused, 'same_physical_hardware'):
            b._media(self.media_backend(same_wwid=True), self.op, self.doc, absent=True)

    def test_same_physical_disk_different_partitions_cannot_pass(self):
        with self.assertRaises(Refused):
            b._media(self.media_backend(ssd={'/dev/hdd'}), self.op, self.doc, absent=True)

    def test_wrong_dataset_guid_or_unqualified_new_ssd_refuse(self):
        for backend in (self.media_backend(wrong_guid=True), self.media_backend(ssd={'/dev/unknown'})):
            with self.assertRaises(Refused): b._media(backend, self.op, self.doc, absent=True)

    def test_kernel_mount_overwrite_cannot_pass_zfs_string_metadata(self):
        with patch.object(b, '_path_mount', return_value={
                'filesystem': 'ext4', 'source': '/dev/unrelated', 'mountpoint': '/', 'root': '/'}):
            with self.assertRaisesRegex(Refused, 'actual_zfs_mount'):
                b._media(self.media_backend(), self.op, self.doc, absent=True)

    def test_kernel_mount_change_during_zfs_probe_is_refused(self):
        original = self.mount; count = 0
        def changed(fd, path, table):
            nonlocal count
            row = original(fd, path, table); count += 1
            if count == 2: row['source'] = 'tank/rebound'
            return row
        with patch.object(b, '_path_mount', side_effect=changed):
            with self.assertRaisesRegex(Refused, 'media_changed_during_probe'):
                b._media(self.media_backend(), self.op, self.doc, absent=True)

    def test_duplicate_hardware_row_or_unexpected_nested_mount_refuses(self):
        rows = self.op.inventory['linux']['failure_domains']['leaf_devices']; rows.append(dict(rows[0]))
        with self.assertRaises(Refused): b._media(self.media_backend(), self.op, self.doc, absent=True)
        rows.pop()
        with patch.object(b, '_no_nested_mounts', side_effect=Refused('fixture_nested_mount')):
            with self.assertRaisesRegex(Refused, 'nested_mount'):
                b._media(self.media_backend(), self.op, self.doc, absent=True)


@unittest.skipUnless(os.geteuid() == 0, 'root-owned marker metadata fixture')
class OldLogicalMediaTests(Fixture):
    def setUp(self):
        super().setUp()
        self.op.inventory['linux']['bound_roots'] = []
        self.op.inventory['linux']['restore'] = {'blobs': {'cache': {}, 'corpus': {}}}
        self.op.inventory['original_datasets'] = []; self.op.inventory['ssd_trees'] = []
        self.paths = {}; self.seed_operation = uid()
        for number, role in enumerate(b.ROLES):
            path = self.root / ('old-' + role); path.mkdir(mode=0o700); os.chown(path, 10004, 10004)
            st = path.stat(); self.paths[role] = path; side = b.ROLE_MEDIA[role]; family = role.split('_')[0]
            self.op.inventory['linux']['bound_roots'].append({'role': role, 'path': str(path), 'uid': 10004,
                                                              'dev': st.st_dev, 'ino': st.st_ino})
            if side == 'hdd':
                self.op.inventory['original_datasets'].append({'name': 'fixture-' + role,
                    'guid': str(800 + number), 'pool': 'tank', 'mountpoint': str(path)})
                value = {'dataset': 'fixture-' + role, 'relative': '.'}; fsid = 'zfs:' + str(800 + number)
            else:
                self.op.inventory['ssd_trees'].append({'name': 'fixture-' + role, 'path': str(path)})
                value = {'tree': 'fixture-' + role, 'relative': '.'}
                fsid = 'block:' + str(os.major(st.st_dev)) + ':' + str(os.minor(st.st_dev))
            self.op.inventory['linux']['restore']['blobs'][family][side] = value
            marker = {'schema_version': 1, 'kind': 'openlegal_blob_seed', 'managed_epoch': self.op.inventory['managed_epoch'],
                      'seed_operation': self.seed_operation, 'role': role, 'manifest_sha256': digest(canonical([])),
                      'correspondence_sha256': digest(canonical([])), 'filesystem_id': fsid}
            out = path / b.MARKER; out.write_bytes(canonical(marker)); out.chmod(0o640); os.chown(out, 0, 10004)

    def test_old_ssd_corpus_primary_maps_exact_marker_without_changes(self):
        before = {role: (path / b.MARKER).read_bytes() for role, path in self.paths.items()}
        roots = b._old_roots(self.op)
        self.assertEqual(roots['corpus_primary']['path'], str(self.paths['corpus_primary']))
        self.assertEqual(roots['corpus_replica']['path'], str(self.paths['corpus_replica']))
        self.assertEqual(before, {role: (path / b.MARKER).read_bytes() for role, path in self.paths.items()})

    def test_old_hdd_primary_corpus_inventory_must_not_be_reinterpreted(self):
        rows = self.op.inventory['linux']['bound_roots']
        for row in rows:
            if row['role'].startswith('corpus_'):
                row['role'] = 'corpus_replica' if row['role'] == 'corpus_primary' else 'corpus_primary'
        with self.assertRaisesRegex(Refused, 'role_media_incompatible'): b._old_roots(self.op)

    def test_mixed_old_seed_operations_refuse_without_changing_markers(self):
        path = self.paths['corpus_primary'] / b.MARKER; value = __import__('json').loads(path.read_bytes())
        value['seed_operation'] = uid(); path.write_bytes(canonical(value)); raw = path.read_bytes()
        with self.assertRaisesRegex(Refused, 'old_marker_operation'): b._old_roots(self.op)
        self.assertEqual(path.read_bytes(), raw)

    def test_same_path_duplicate_or_wrong_marker_role_refuses(self):
        rows = self.op.inventory['linux']['bound_roots']; old = rows[1]['path']; rows[1]['path'] = rows[0]['path']
        with self.assertRaises(Refused): b._old_roots(self.op)
        rows[1]['path'] = old
        path = self.paths['corpus_primary'] / b.MARKER; value = __import__('json').loads(path.read_bytes())
        value['role'] = 'corpus_replica'; path.write_bytes(canonical(value))
        with self.assertRaisesRegex(Refused, 'old_marker_binding'): b._old_roots(self.op)


class KernelMountTests(Fixture):
    """Actual Linux /tmp descriptor proof, no ZFS/device/native-service claim."""
    def test_real_mount_proof_matches_reviewed_initial_helper_and_rebind_refuses(self):
        import legacy_prepare as legacy
        path = self.root / 'kernel-proof'; path.mkdir(mode=0o700)
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            self.assertEqual(b._fd_mount_id(fd), legacy._fd_mount_id(fd))
            actual = b._mounts(); old = legacy._mounts()
            self.assertEqual(b._path_mount(fd, str(path), actual), legacy._path_mount(fd, str(path), old))
            b._no_nested_mounts(path, actual); legacy._no_nested_mounts(path, old)
            path.rename(self.root / 'detached'); path.mkdir(mode=0o700)
            with self.assertRaisesRegex(Refused, 'mount_path_rebound'): b._path_mount(fd, str(path), b._mounts())
        finally: os.close(fd)

    def test_equivalent_escaped_mount_fields_and_nested_mount_rejection(self):
        import io
        import legacy_prepare as legacy
        raw = '24 1 8:1 / /tmp/a\\040b rw - zfs tank/a\\134b rw\n'
        with patch('builtins.open', return_value=io.StringIO(raw)), \
                patch.object(legacy, '_kernel_mountinfo', return_value=raw):
            self.assertEqual(b._mounts(), legacy._mounts())
        table = {'1': {'mountpoint': str(self.root / 'nested')}}
        for module in (b, legacy):
            with self.assertRaises(Refused): module._no_nested_mounts(self.root, table)

    def test_bounded_proc_reader_rejects_oversize_duplicate_and_malformed_rows(self):
        import io
        for raw in ('x' * (16 * 1024**2 + 1), '24 1 8:1 / /tmp rw - ext4 /dev/a rw\n' * 2,
                    'bad mountinfo\n'):
            with patch('builtins.open', return_value=io.StringIO(raw)):
                with self.assertRaises(Refused): b._mounts()


class CapacityTests(Fixture):
    def backend(self, quota, available):
        return types.SimpleNamespace(_run=lambda op, binary, args: (
            str(quota) if 'refquota' in args else str(available)).encode() + b'\n',
            _artifact=lambda *a: None)
    def test_actual_quota_and_pool_free_must_cover_approved_worst_case(self):
        bound = self.op.inventory['ssd_trees'][0]['max_bytes']; reserve = self.doc['capacity_reserve_bytes']
        result = b._quota(self.backend(bound, bound + reserve), self.op, self.doc)
        self.assertEqual(result['actual_refquota_bytes'], bound)
        b._quota(self.backend(bound, reserve), self.op, self.doc, before=False)
        for quota, available in (('none', bound + reserve), (bound + 1, bound + reserve),
                                  (bound, bound + reserve - 1)):
            with self.subTest(quota=quota, available=available), self.assertRaises(Refused):
                b._quota(self.backend(quota, available), self.op, self.doc)
    def test_exactly_one_generic_postgres_tree_is_required(self):
        self.assertEqual(b._postgres_tree(self.op)['roles'], ['postgres'])
        self.op.inventory['ssd_trees'].append(copy.deepcopy(self.op.inventory['ssd_trees'][0]))
        with self.assertRaises(Refused): b._postgres_tree(self.op)


@unittest.skipUnless(os.geteuid() == 0, 'actual source-tree fixtures need root')
class EndpointTests(Fixture):
    def setUp(self):
        super().setUp()
        self.source = self.root / 'ssd'; self.source.mkdir(mode=0o700); os.chown(self.source, 70, 70)
        s = self.source.stat(); self.op.inventory['ssd_trees'][0]['identity'] = {'dev': s.st_dev, 'ino': s.st_ino}
        self.current = {'system_identifier': '123', 'timeline': 2, 'postmaster_started_at': 'fixed-start',
                        'wal_lsn': '0/100', 'index_ack': 0, 'watermark': 0, 'in_recovery': False}
        self.promoted = {'promoted_container_id': 'a' * 64, 'promoted_checkpoint': dict(self.current)}
        self.item = {'Id': 'a' * 64, 'State': {'Running': True},
            'NetworkSettings': {'Networks': {self.doc['network']['name']: {
                'NetworkID': self.doc['network']['id'], 'IPAddress': self.doc['upstream']['host']}}},
            'Mounts': [{'Source': str(self.source), 'Destination': '/var/lib/postgresql', 'Type': 'bind', 'RW': True}]}
        self.backend = types.SimpleNamespace(_sql=lambda *a, **kw: self.current, _artifact=lambda *a: None)
    def test_endpoint_is_actual_promoted_cid_numeric_network_and_lifetime(self):
        with patch.object(b.manual, '_inspect', return_value=self.item):
            self.assertEqual(b._promoted_endpoint(self.backend, self.op, self.doc, self.promoted)['logical_bytes'], 0)
    def test_wrong_cid_shared_cert_other_ip_or_restart_refuse_before_backup(self):
        for change in ('cid', 'ip', 'network', 'restart', 'sysid'):
            item = copy.deepcopy(self.item); current = dict(self.current)
            if change == 'cid': item['Id'] = 'b' * 64
            if change == 'ip': item['NetworkSettings']['Networks'][self.doc['network']['name']]['IPAddress'] = '192.0.2.99'
            if change == 'network': item['NetworkSettings']['Networks'][self.doc['network']['name']]['NetworkID'] = 'c' * 64
            if change == 'restart': current['postmaster_started_at'] = 'new-start'
            if change == 'sysid': current['system_identifier'] = '999'
            with self.subTest(change=change), patch.object(b.manual, '_inspect', return_value=item), \
                 patch.object(self.backend, '_sql', return_value=current), self.assertRaises(Refused):
                b._promoted_endpoint(self.backend, self.op, self.doc, self.promoted)
    def test_size_bound_and_external_tablespace_refuse_without_reading_bodies(self):
        source = b._postgres_tree(self.op); source['max_bytes'] = 8
        (self.source / 'data').write_bytes(b'x' * 9)
        with self.assertRaisesRegex(Refused, 'byte_bound'): b._tree_size(source)
        (self.source / 'data').unlink(); (self.source / 'outside').symlink_to('/tmp')
        with self.assertRaisesRegex(Refused, 'external_or_special'): b._tree_size(source)


class CommandTests(Fixture):
    def inspected(self):
        spec = self.doc['postgres_spec']
        return {'Id': 'a' * 64, 'Name': spec['name'], 'Image': spec['image'],
            'Config': {'Entrypoint': spec['entrypoint'], 'Cmd': spec['arguments'],
                       'User': spec['user'], 'Labels': spec['labels']},
            'Mounts': [{'Source': r['source'], 'Destination': r['destination'],
                        'RW': r['rw'], 'Type': r['type']} for r in spec['mounts']],
            'HostConfig': {'RestartPolicy': spec['restart_policy'], 'Privileged': False,
                           'CapAdd': None, 'NetworkMode': spec['network_mode'], 'PortBindings': {}},
            'NetworkSettings': {'Networks': {self.doc['network']['name']: {
                'NetworkID': self.doc['network']['id'], 'IPAddress': self.doc['network']['replica_address']}}},
            'State': {'Running': True, 'Pid': 123, 'Health': {'Log': [{'Output': 'SENTINEL_SECRET'}]},
                      'Error': 'SENTINEL_ERROR'}}
    def backend(self, item):
        calls = []
        def run(op, binary, args, **kwargs):
            calls.append(args)
            if 'create' in args: return b'a' * 64 + b'\n'
            if 'inspect' in args: return canonical([item])
            return b''
        return types.SimpleNamespace(_run=run, _owned_sql_targets={}, calls=calls,
            _artifact=lambda op, name, value: op.journal.save(op.phase + '-' + name + '.json', value))
    def test_actual_inspect_projection_hides_health_error_and_env(self):
        item = self.inspected(); item['Config']['Env'] = ['PASSWORD=SENTINEL_ENV']
        backend = self.backend(item)
        b._inspect_owned(backend, self.op, item['Id'], name=item['Name'][1:],
                         spec=self.doc['postgres_spec'], network=self.doc['network'])
        raw = b''.join(self.op.journal.raw.values())
        self.assertNotIn(b'SENTINEL_', raw)
    def test_wrong_cid_mount_label_network_or_published_ports_refuse(self):
        for change in ('cid', 'mount', 'label', 'network', 'address', 'port'):
            item = self.inspected()
            if change == 'cid': item['Id'] = 'b' * 64
            if change == 'mount': item['Mounts'][0]['Source'] = '/tmp/wrong'
            if change == 'label': item['Config']['Labels']['openlegal.operation'] = uid()
            if change == 'network': item['NetworkSettings']['Networks'] = {'other': {}}
            if change == 'address':
                item['NetworkSettings']['Networks'][self.doc['network']['name']]['IPAddress'] = '192.0.2.99'
            if change == 'port': item['HostConfig']['PortBindings'] = {'5432/tcp': [{'HostPort': '5432'}]}
            with self.subTest(change=change), self.assertRaises(Refused):
                b._inspect_owned(self.backend(item), self.op, 'a' * 64,
                    name=self.doc['postgres_spec']['name'][1:], spec=self.doc['postgres_spec'],
                    network=self.doc['network'])
    def test_standby_registry_only_after_exact_inspect_then_bounded_ready(self):
        item = self.inspected(); backend = self.backend(item)
        with patch.object(b, '_trust'), patch.object(b, '_network'), patch.object(b, '_image_labels'):
            cid = b._start_standby(backend, self.op, self.doc)
        self.assertEqual(cid, 'a' * 64); self.assertIn(cid, backend._owned_sql_targets)
        create = backend.calls[0]
        self.assertIn('--pull=never', create); self.assertIn('--restart=no', create)
        self.assertNotIn(',rw', ' '.join(create)); self.assertNotIn('--publish', create)
        journal_names = list(self.op.journal.raw)
        self.assertLess(next(i for i, n in enumerate(journal_names) if 'standby-created' in n),
                        next(i for i, n in enumerate(journal_names) if 'owned-container' in n))
    def test_inspect_failure_preserves_created_receipt_and_no_registry_or_rm(self):
        item = self.inspected(); item['Config']['Cmd'] = ['evil']; backend = self.backend(item)
        with patch.object(b, '_trust'), patch.object(b, '_network'), patch.object(b, '_image_labels'), self.assertRaises(Refused):
            b._start_standby(backend, self.op, self.doc)
        self.assertEqual(backend._owned_sql_targets, {})
        self.assertTrue(any('standby-created' in n for n in self.op.journal.raw))
        self.assertFalse(any('rm' in command for command in backend.calls))
    def test_reseed_exact_intent_after_registry_before_bind_and_pair(self):
        order = []; roots = [{'synthetic': 'root'}]
        promoted = {'prepared': {'synthetic': 'prepared'}, 'promoted_container_id': 'b' * 64}
        sealed = {'closure': {}}; expected = {'blob_binding_sha256': 'c' * 64}
        backend = types.SimpleNamespace(_artifact=lambda op, name, value: (
            order.append(('artifact', name)), op.journal.save(op.phase + '-' + name + '.json', value))[1],
            _manual_bind_replication_control=lambda *args: order.append(('bind', args[-1])))
        def start(*args): order.append(('start', 'registered')); return 'a' * 64
        with patch.object(b, 'read_prepared_configuration', return_value=self.doc), \
             patch.object(b, '_resources'), patch.object(b, '_network'), patch.object(b, '_trust'), \
             patch.object(b, '_image_labels'), \
             patch.object(b.manual, '_closure'), patch.object(b, '_copy_generations', return_value=roots), \
             patch.object(b, '_copy_index'), patch.object(b, '_expected_binding', return_value=expected), \
             patch.object(b, '_basebackup'), patch.object(b, '_postgres_configuration', return_value={}), \
             patch.object(b, '_start_standby', side_effect=start), \
             patch.object(b, '_candidate', side_effect=lambda *a: order.append(('candidate', None))), \
             patch.object(b, '_seed_document', return_value={'synthetic': 'manifest'}), \
             patch.object(b, 'read_pair', side_effect=lambda *a, **kw: (order.append(('pair', None)), {})[1]), \
             patch.object(b.manual, '_pair'), patch.object(b.manual, 'save_evidence', return_value={'synthetic': 'only'}):
            b.reseed_and_bind(backend, self.op, promoted, sealed)
        index = lambda name: next(i for i, entry in enumerate(order) if entry[0] == name)
        intent_index = next(i for i, entry in enumerate(order) if entry == ('artifact', 'bootstrap-seed-intent'))
        self.assertLess(index('start'), intent_index); self.assertLess(intent_index, index('bind'))
        self.assertLess(intent_index, index('candidate')); self.assertLess(index('candidate'), index('bind'))
        self.assertLess(index('bind'), index('pair'))
        intent = b.decode(self.op.journal.raw['reseed-bootstrap-seed-intent.json'])
        self.assertEqual(set(intent), {'schema_version', 'kind', 'operation_id', 'new_epoch',
            'primary_container_id', 'standby_container_id', 'blob_roots', 'blob_binding_sha256',
            'seed_manifest_sha256', 'expected_binding'})
        self.assertEqual(intent['standby_container_id'], 'a' * 64)
    def test_source_has_no_broad_sql_or_process_fallback(self):
        # Same fixed effect path; specifically no fake fallback to table UPDATE.
        text = Path(b.__file__).read_text()
        self.assertNotIn('UPDATE openlegal_replication', text)
        self.assertNotIn('ALTER SYSTEM', text)
        self.assertNotIn('CREATE ROLE', text)
        self.assertNotIn('subprocess.', text)

    def test_image_labels_exact_inheritance_and_conflicts(self):
        image = self.op.inventory['linux']['postgres']['primary']['image_id']
        for labels in (None, {}, {'org.opencontainers.image.title': 'postgres'}):
            inherited = labels or {}
            self.doc['postgres_spec']['labels'] = dict(inherited, **{
                'openlegal.operation': self.op.operation_id, 'openlegal.epoch': self.doc['new_epoch']})
            backend = types.SimpleNamespace(_run=lambda *a, **kw: image.encode() + b'\n' + canonical(labels),
                _artifact=lambda op, name, value: None)
            b._image_labels(backend, self.op, self.doc)
        for labels in ({'openlegal.operation': 'old'}, {'unexpected': 'label'}):
            backend = types.SimpleNamespace(_run=lambda *a, **kw: image.encode() + b'\n' + canonical(labels))
            with self.subTest(labels=labels), self.assertRaises(Refused): b._image_labels(backend, self.op, self.doc)

    def test_candidate_has_no_claim_of_stored_binding_or_public_ack(self):
        peer = self.policy['replication_peer']
        expected = {'managed_epoch': self.doc['new_epoch'], 'blob_binding_sha256': 'd' * 64,
            'primary_system_identifier': '123', 'timeline': 2, 'standby_name': peer['application_name'],
            'standby_role': peer['role'], 'standby_addr': peer['host'],
            'standby_client_dn': peer['client_dn'], 'standby_issuer_dn': peer['issuer_dn'],
            'standby_client_serial': peer['client_serial']}
        candidate = {k: v for k, v in expected.items() if k not in ('managed_epoch', 'blob_binding_sha256')}
        candidate.update(postmaster_started_at='2026-11-02 00:00:00+00', sender_pid=12,
                         sender_started_at='2026-11-02 00:01:00+00', prefix_lsn='0/100', flush_lsn='0/100')
        value = {'kind': 'openlegal_replication_candidate_peer', 'proposed_binding': expected,
                 'candidate': candidate}
        backend = types.SimpleNamespace(_manual_candidate_peer=lambda *a: value,
            _artifact=lambda op, name, item: op.journal.save(op.phase + '-' + name + '.json', item))
        promoted = {'promoted_container_id': 'b' * 64,
                    'promoted_checkpoint': {'postmaster_started_at': candidate['postmaster_started_at']}}
        self.assertEqual(b._candidate(backend, self.op, promoted, expected), value)
        saved = b.decode(next(iter(self.op.journal.raw.values())))
        self.assertNotIn('managed_epoch', saved['candidate'])
        self.assertNotIn('blob_binding_sha256', saved['candidate'])
        for field, change in (('standby_client_serial', '999'), ('postmaster_started_at', 'changed'),
                              ('sender_pid', 0)):
            old = candidate[field]; candidate[field] = change
            with self.subTest(field=field), self.assertRaises(Refused):
                b._candidate(backend, self.op, promoted, expected)
            candidate[field] = old


if __name__ == '__main__': unittest.main()
