"""Local command and guard fixtures; no Docker, SQL, Kubernetes or ZFS access.

The SQL responses below are explicitly synthetic. They verify refusal/order,
never establish a physical replica or production acknowledgment.
"""
import copy
import fcntl
import hashlib
import json
import os
import stat
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock
import uuid
import subprocess

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from linux_backend import LinuxBackend, MAX_OUTPUT, canonical
from interface import Refused


class PostgresLayoutBinding(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='linux-pg-layout-',dir='/tmp')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.hdd = self.root/'hdd'; self.ssd = self.root/'ssd'
        for path in (self.hdd/'18'/'docker',self.ssd/'data'):
            (path/'pg_wal').mkdir(parents=True)
        self.config = {
            'primary':{'data':{'dataset':'tank/pg','relative':'18/docker'},'wal':None},
            'standby':{'data':{'tree':'peer','relative':'data'},'wal':None}}
        self.op = types.SimpleNamespace(phase='stop_postgres',journal=Journal(),
            inventory={'host':{'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip()},
                'original_datasets':[{'name':'tank/pg','mountpoint':str(self.hdd)}],
                'active_datasets':[], 'ssd_trees':[{'name':'peer','path':str(self.ssd)}],
                'linux':{'restore':self.config,'postgres':{'uid':os.geteuid(),
                    'primary':{'container_id':'1'*64},'standby':{'container_id':'2'*64}}}})
        self.backend = LinuxBackend()
        self.native_data = {'primary':'/var/lib/postgresql/18/docker',
                            'standby':'/var/lib/postgresql/data'}
        self.mounts = {side:[{'source':str(root),'destination':'/var/lib/postgresql',
                             'type':'bind','rw':True}]
                       for side,root in (('primary',self.hdd),('standby',self.ssd))}
        def inspect(op,side):
            self.current_side = side
            return {'mounts':self.mounts[side],
                    'state':{'Running':True,'Pid':os.getpid(),'StartedAt':'fixture'}}
        self.backend._inspect = inspect
        # Explicitly synthetic native namespace projection. A separate Docker
        # bind-rename gate is needed to establish native inode provenance.
        def native(pid,path):
            self.assertEqual(pid,os.getpid())
            mount = max((m for m in self.mounts[self.current_side]
                         if Path(path).is_relative_to(m['destination'])),
                        key=lambda m:len(Path(m['destination']).parts))
            host = Path(mount['source'])/Path(path).relative_to(mount['destination'])
            return os.open(host,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
        self.backend._container_directory = native
        def close_native():
            for row in self.backend._pg_directory_fds.values():
                os.close(row['data_fd']);os.close(row['wal_fd'])
        self.addCleanup(close_native)
        self.backend._sql = lambda op,sql,which: {'data_directory':self.native_data[which]}
        self.validation = mock.patch('restore_backend.validate',side_effect=lambda config,inv:config)
        self.validation.start();self.addCleanup(self.validation.stop)

    def test_actual_effective_paths_select_both_current_media(self):
        result = self.backend._postgres_layout(self.op)
        self.assertEqual(result['primary']['data_host_path'],str(self.hdd/'18'/'docker'))
        self.assertEqual(result['standby']['wal_host_path'],str(self.ssd/'data'/'pg_wal'))
        self.assertEqual(result['primary']['container_id'],'1'*64)

    def test_role_flip_uses_new_active_hdd_instead_of_original(self):
        new = self.root/'new-hdd'
        (new/'data'/'pg_wal').mkdir(parents=True)
        self.op.inventory['active_datasets'] = [{'name':'tank/new','mountpoint':str(new)}]
        self.config['primary']['data'] = {'tree':'peer','relative':'data'}
        self.config['standby']['data'] = {'dataset':'tank/new','relative':'data'}
        self.mounts['primary'][0]['source'] = str(self.ssd)
        self.mounts['standby'][0]['source'] = str(new)
        self.native_data['primary'] = self.native_data['standby']
        result = self.backend._postgres_layout(self.op)
        self.assertEqual(result['standby']['data_host_path'],str(new/'data'))
        self.config['standby']['data'] = {'dataset':'tank/pg','relative':'18/docker'}
        with self.assertRaisesRegex(Refused,'wrong_data_source'):
            self.backend._postgres_layout(self.op)

    def test_two_hdd_selections_cannot_certify_independent_peer(self):
        self.config['standby']['data'] = copy.deepcopy(self.config['primary']['data'])
        with self.assertRaisesRegex(Refused,'requires_both_media'):
            self.backend._postgres_layout(self.op)

    def test_actual_native_inode_must_match_selected_host_directory(self):
        other = self.root/'old-native-data';other.mkdir()
        self.backend._container_directory = lambda pid,path: os.open(other,os.O_RDONLY|os.O_DIRECTORY)
        with self.assertRaisesRegex(Refused,'native_data_inode_mismatch'):
            self.backend._postgres_layout(self.op)

    def test_retained_native_directory_rejects_later_host_replacement(self):
        self.backend._postgres_layout(self.op)
        data = self.hdd/'18'/'docker'
        data.rename(data.with_name('retained-original'))
        (data/'pg_wal').mkdir(parents=True)
        with self.assertRaisesRegex(Refused,'native_directory_rebound'):
            self.backend._revalidate_postgres_directories(self.op,running=True)

    def test_unbound_physical_wal_link_is_refused(self):
        wal = self.hdd/'18'/'docker'/'pg_wal'
        wal.rmdir();wal.symlink_to('/unapproved-wal')
        with self.assertRaisesRegex(Refused,'unbound_wal_link'):
            self.backend._postgres_layout(self.op)

    def test_explicit_wal_link_resolves_through_actual_container_mount(self):
        wal = self.root/'wal';wal.mkdir()
        old = self.hdd/'18'/'docker'/'pg_wal'
        old.rmdir();old.symlink_to('/actual-wal')
        self.op.inventory['active_datasets'] = [{'name':'tank/wal','mountpoint':str(wal)}]
        self.config['primary']['wal'] = {'dataset':'tank/wal','relative':'.'}
        self.mounts['primary'].append({'source':str(wal),'destination':'/actual-wal',
                                      'type':'bind','rw':True})
        result = self.backend._postgres_layout(self.op)
        self.assertEqual(result['primary']['wal_host_path'],str(wal))
        self.mounts['primary'][-1]['rw'] = False
        with self.assertRaisesRegex(Refused,'mount_readonly'):
            self.backend._postgres_layout(self.op)


class SnapshotMountBinding(unittest.TestCase):
    def test_fixed_kernel_projection_accepts_zfs_superblock_readonly(self):
        # Synthetic kernel projections only, not a native ZFS acceptance.
        snapshot = 'tank/pg@checkpoint'
        source = Path('/mnt/pg/.zfs/snapshot/checkpoint')
        def text(path):
            if str(path).startswith('/proc/self/fdinfo/'):
                return 'pos:\t0\nmnt_id:\t2346\n'
            return f'2346 62 0:379 / {source} rw,relatime - zfs {snapshot} ro,xattr\n'
        with mock.patch.object(Path,'read_text',text),mock.patch('linux_backend.os.fstatvfs',
                return_value=types.SimpleNamespace(f_flag=os.ST_RDONLY)):
            result = LinuxBackend._snapshot_mount(42,snapshot,source)
            self.assertEqual(result['source'],snapshot)
            self.assertTrue(result['readonly'])
            for wrong in ('tank/other@checkpoint','tank/pg@other'):
                with self.assertRaisesRegex(Refused,'snapshot_mount_source'):
                    LinuxBackend._snapshot_mount(42,wrong,source)
        with mock.patch.object(Path,'read_text',text),mock.patch('linux_backend.os.fstatvfs',
                return_value=types.SimpleNamespace(f_flag=0)):
            with self.assertRaisesRegex(Refused,'snapshot_mount_source'):
                LinuxBackend._snapshot_mount(42,snapshot,source)

    def test_actual_tmp_descriptor_cannot_impersonate_zfs_snapshot(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            fd = os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
            try:
                with self.assertRaisesRegex(Refused,'snapshot_mount_source'):
                    LinuxBackend._snapshot_mount(fd,'tank/pg@checkpoint',Path(directory))
            finally:
                os.close(fd)


class WorkloadRevisionFence(unittest.TestCase):
    def setUp(self):
        self.op = types.SimpleNamespace(operation_id=str(uuid.uuid4()),
            phase='failure_stop',journal=Journal(),inventory={'linux':{'namespace':'fixture'}})
        self.backend = LinuxBackend()
        self.uid = str(uuid.uuid4())
        self.item = {'metadata':{'uid':self.uid,'resourceVersion':'1'},
                     'spec':{'replicas':0,'template':{'spec':{'containers':[]}}}}
        self.pin = hashlib.sha256(canonical(self.item['spec']['template']['spec'])).hexdigest()
        self.patches = []
        def kube(op,args):
            if 'get' in args:
                return copy.deepcopy(self.item)
            patch = json.loads(args[args.index('-p')+1]);self.patches.append(patch)
            for row in patch:
                if row['op'] == 'test':
                    path = row['path'].strip('/').split('/')
                    value = self.item
                    for key in path:value = value[key]
                    self.assertEqual(value,row['value'])
                elif row['path'] == '/spec/replicas':
                    self.item['spec']['replicas'] = row['value']
                elif row['path'] == '/metadata/annotations':
                    self.item['metadata']['annotations'] = row['value']
                else:
                    self.item['metadata']['annotations']['openlegal4everyone.stream/operation-fence'] = row['value']
            self.item['metadata']['resourceVersion'] = '2'
            return copy.deepcopy(self.item)
        self.backend._kube = kube

    def test_zero_to_zero_forces_revision_before_prior_queued_start_can_apply(self):
        nonces = []
        for annotations in (None,{'existing':'preserved'}):
            with self.subTest(annotations=annotations):
                self.item['metadata']['resourceVersion'] = '1'
                if annotations is None:self.item['metadata'].pop('annotations',None)
                else:self.item['metadata']['annotations'] = dict(annotations)
                result = self.backend._cas_replicas(self.op,'serve',self.uid,replicas=0,
                                                   current=0,template_sha256=self.pin)
                self.assertEqual(result['metadata']['resourceVersion'],'2')
                self.assertTrue(result['metadata']['annotations']['openlegal4everyone.stream/operation-fence']
                                .startswith(self.op.operation_id+'.'))
                nonces.append(result['metadata']['annotations']['openlegal4everyone.stream/operation-fence'])
                self.assertNotEqual(result['metadata']['resourceVersion'],'1',
                                    'an earlier start CAS testing RV1 must now conflict')
                if annotations:self.assertEqual(result['metadata']['annotations']['existing'],'preserved')
        self.assertEqual(len(set(nonces)),2,'same-operation stop requests need distinct revision fences')
        self.assertEqual(len(self.op.journal.values),2)

    def test_replaced_uid_refuses_before_any_mutation(self):
        self.item['metadata']['uid'] = str(uuid.uuid4())
        with self.assertRaisesRegex(Refused,'scale_workload_identity_changed'):
            self.backend._cas_replicas(self.op,'serve',self.uid,replicas=0,current=0,
                                       template_sha256=self.pin)
        self.assertEqual(self.patches,[])


class ClosedCanaryLookup(unittest.TestCase):
    """Synthetic SQL/control plane; actual runner/ACL gates remain separate."""
    def setUp(self):
        self.backend = LinuxBackend();self.op = operation()
        self.op.action = 'canary';self.op.phase = 'final_lookup'
        self.op.authority = {'canary_held_lookup_resume_permitted':True}
        self.canary = str(uuid.uuid4());self.generation = str(uuid.uuid4())
        self.finished = {'canary_id':self.canary,'recovery_hold':True,
            'final_checkpoint_operation':self.op.operation_id,
            'managed_epoch':self.op.inventory['managed_epoch'],'blob_binding_sha256':'a'*64,
            'hold_generation':self.generation}
        control = {'generation':self.generation,'recovery_hold':True,'hold_started_at':1}
        self.provider = {'state':{'budget':{'unresolved_response':False,'admission_owner':None},
                'control':{},'admissions':[],'canary_control':{},'canaries':[]},
            'hold_control':control,'admission_ownership':{'legacy_locked':False,'slots':[]},
            'checkpoint_owner':self.op.operation_id,'checkpoint_readback':None}
        self.workloads = []
        for name in ('serve','scheduler'):
            template = {'containers':[{'name':name}]}
            self.workloads.append({'kind':'deployment','name':name,'uid':str(uuid.uuid4()),
                'pod_spec_sha256':hashlib.sha256(canonical(template)).hexdigest(),
                'fixture_template':template})
        self.op.inventory['linux'].update(namespace='fixture',serving_workload='serve',workloads=[
            {k:v for k,v in row.items() if k!='fixture_template'} for row in self.workloads])
        self.cm = {'metadata':{'uid':str(uuid.uuid4())},'data':{'policy':'pinned'}}
        self.op.inventory['linux']['config_maps'] = [{'name':'fixture-config',
            'uid':self.cm['metadata']['uid'],'data_sha256':hashlib.sha256(canonical(self.cm['data'])).hexdigest()}]
        self.cm_reads = []
        self.backend._closed_writers = lambda op: None
        self.backend._sql = lambda op,query: ({'activation':{},'finish':self.finished}
            if 'canary_readback' in query else copy.deepcopy(self.provider))
        self.backend._read_replica_barrier = lambda op: {'synthetic':True}
        def kube(op,args):
            if args[3] == 'configmap':
                self.cm_reads.append(args[4]);return copy.deepcopy(self.cm)
            return next({'metadata':{'uid':row['uid']},
                'spec':{'replicas':0,'template':{'spec':row['fixture_template']}}}
                for row in self.workloads if row['name']==args[4])
        self.backend._kube = kube
        self.scales = []
        self.backend._cas_replicas = lambda op,name,uid,**kwargs:self.scales.append((name,kwargs))
        self.backend._wait_lookup = lambda op:{'synthetic_new_ready_lookup':True}

    def test_restart_only_lookup_under_same_final_hold(self):
        result = self.backend._canary_restart_lookup_held(self.op,canary_id=self.canary)
        self.assertEqual([name for name,_ in self.scales],['serve'])
        self.assertFalse(result['collection_resumed'])
        self.assertFalse(result['final_acceptance'])
        self.assertEqual(self.cm_reads,['fixture-config','fixture-config'])

    def test_changed_config_before_start_or_after_ready_refuses_success(self):
        self.cm['data']['policy'] = 'changed'
        with self.assertRaisesRegex(Refused,'runtime_configuration_changed'):
            self.backend._canary_restart_lookup_held(self.op,canary_id=self.canary)
        self.assertEqual(self.scales,[])
        self.cm['data']['policy'] = 'pinned'
        def ready_then_config_changes(op):
            self.cm['data']['policy'] = 'changed-after-ready'
            return {'synthetic_new_ready_lookup':True}
        self.backend._wait_lookup = ready_then_config_changes
        with self.assertRaisesRegex(Refused,'runtime_configuration_changed'):
            self.backend._canary_restart_lookup_held(self.op,canary_id=self.canary)
        self.assertEqual([name for name,_ in self.scales],['serve'])

    def test_changed_finish_or_foreign_hold_never_starts_workload(self):
        self.finished['managed_epoch'] = str(uuid.uuid4())
        with self.assertRaisesRegex(Refused,'finish_binding'):
            self.backend._canary_restart_lookup_held(self.op,canary_id=self.canary)
        self.assertEqual(self.scales,[])
        self.finished['managed_epoch'] = self.op.inventory['managed_epoch']
        self.provider['hold_control']['generation'] = str(uuid.uuid4())
        with self.assertRaisesRegex(Refused,'final_hold_changed'):
            self.backend._canary_restart_lookup_held(self.op,canary_id=self.canary)
        self.assertEqual(self.scales,[])

    def test_missing_authority_refuses_before_queries(self):
        self.op.authority = {}
        self.backend._sql = mock.Mock(side_effect=AssertionError('must not query'))
        with self.assertRaisesRegex(Refused,'resume_scope'):
            self.backend._canary_restart_lookup_held(self.op,canary_id=self.canary)
        self.backend._sql.assert_not_called()


class JobRevisionFence(unittest.TestCase):
    def setUp(self):
        self.backend = LinuxBackend()
        self.op = types.SimpleNamespace(operation_id=str(uuid.uuid4()),phase='failure_stop',
            journal=Journal(),inventory={'linux':{'namespace':'fixture'}})
        self.item = {'metadata':{'uid':str(uuid.uuid4()),'resourceVersion':'1'},
                     'spec':{'suspend':True,'backoffLimit':0},'status':{}}
        normalized = dict(self.item['spec']);normalized.pop('suspend')
        self.pin = {'name':'canary','uid':self.item['metadata']['uid'],
                    'spec_sha256':hashlib.sha256(canonical(normalized)).hexdigest()}
        self.patches = []
        def kube(op,args):
            if 'get' in args:return copy.deepcopy(self.item)
            patch = json.loads(args[args.index('-p')+1]);self.patches.append(patch)
            self.assertTrue(self.op.journal.values,'intent must precede dispatch')
            old = self.item['metadata']['resourceVersion']
            for row in patch:
                if row['op'] == 'test':
                    value = self.item
                    for part in row['path'].strip('/').split('/'):value = value[part]
                    self.assertEqual(value,row['value'])
                elif row['path'] == '/spec/suspend':self.item['spec']['suspend'] = row['value']
                elif row['path'] == '/metadata/annotations':self.item['metadata']['annotations'] = row['value']
                else:self.item['metadata']['annotations']['openlegal4everyone.stream/operation-fence'] = row['value']
            self.item['metadata']['resourceVersion'] = str(int(old)+1)
            return copy.deepcopy(self.item)
        self.backend._kube = kube

    def test_already_suspended_job_still_fences_queued_unsuspend(self):
        first = self.backend._cas_suspend_job(self.op,self.pin,normalize_suspend=True)
        self.assertTrue(first['spec']['suspend'])
        self.assertEqual(first['metadata']['resourceVersion'],'2')
        self.assertNotEqual(first['metadata']['resourceVersion'],'1',
                            'queued unsuspend carrying old RV1 must conflict')
        second = self.backend._cas_suspend_job(self.op,self.pin,normalize_suspend=True)
        self.assertEqual(second['metadata']['resourceVersion'],'3')
        self.assertNotEqual(first['metadata']['annotations'],second['metadata']['annotations'])
        self.assertEqual(len(self.patches),2)

    def test_terminal_job_is_preserved_without_mutation(self):
        self.item['status'] = {'conditions':[{'type':'Complete','status':'True'}]}
        result = self.backend._cas_suspend_job(self.op,self.pin,normalize_suspend=True)
        self.assertTrue(result['terminal_untouched']);self.assertFalse(self.patches)

    def test_changed_spec_refuses_before_mutation(self):
        self.item['spec']['backoffLimit'] = 1
        with self.assertRaisesRegex(Refused,'job_suspend_identity_changed'):
            self.backend._cas_suspend_job(self.op,self.pin,normalize_suspend=True)
        self.assertFalse(self.patches)


class Journal:
    def __init__(self):
        self.values = {}

    def save(self, name, value):
        if name in self.values:
            raise AssertionError('journal overwrite')
        self.values[name] = value
        return hashlib.sha256(canonical(value)).hexdigest()


def operation():
    binding = {'managed_epoch': str(uuid.uuid4()), 'blob_binding_sha256': 'a' * 64,
        'primary_system_identifier': '123', 'timeline': 2, 'standby_name': 'replica',
        'standby_role': 'replica_role', 'standby_addr': '192.0.2.1',
        'standby_client_dn': 'CN=fixture', 'standby_issuer_dn': 'CN=fixture_ca',
        'standby_client_serial': '4'}
    return types.SimpleNamespace(operation_id=str(uuid.uuid4()), phase='drain_writers',
        action='checkpoint', seal=lambda: None, journal=Journal(),
        inventory={'managed_epoch': binding['managed_epoch'], 'linux': {
            'blob_binding_sha256': 'a' * 64, 'postgres': {'binding': binding}}})


def observation(op):
    return dict(op.inventory['linux']['postgres']['binding'],
        postmaster_started_at='2026-11-02 00:00:00+00', sender_pid=1234,
        sender_started_at='2026-11-02 00:00:01+00', prefix_lsn='1/100', flush_lsn='1/100')


class StaticReplicationSerialDomain(unittest.TestCase):
    """Structural inventory checks only; every path and identity is synthetic."""

    def config(self, serial):
        binding = copy.deepcopy(operation().inventory['linux']['postgres']['binding'])
        binding['standby_client_serial'] = serial
        image = 'sha256:' + 'c' * 64
        uid = '00000000-0000-4000-8000-000000000001'
        return {
            'binaries': {name: {'path': '/unopened/' + name, 'sha256': 'a' * 64}
                         for name in ('kubectl', 'docker', 'crictl', 'zfs', 'zpool')},
            'kubeconfig': {'path': '/unopened/kubeconfig', 'sha256': 'a' * 64},
            'context': 'fixture', 'namespace': 'fixture',
            'serving_workload': 'fixture-serving', 'serving_selector': 'app=fixture',
            'health_port': 8080, 'blob_binding_sha256': binding['blob_binding_sha256'],
            'serving_image': image, 'serving_runtime_image_id': image,
            'serving_service': {'name': 'fixture', 'uid': uid, 'spec_sha256': 'a' * 64},
            'cri_endpoint': 'unix:///unopened/cri.sock',
            'postgres': {'database': 'fixture', 'admin_user': 'fixture_admin', 'uid': 70,
                'binding': binding, **{side: {'container_id': digit * 64, 'image_id': image,
                    'mounts': [{'source': '/unopened/' + side, 'destination': '/pgdata',
                                'rw': True, 'type': 'bind'}]}
                    for side, digit in (('primary', '1'), ('standby', '2'))}},
            'workloads': [{'kind': 'deployment', 'name': name, 'uid': uid,
                           'pod_spec_sha256': 'a' * 64}
                          for name in ('fixture-serving', 'fixture-scheduler')],
            'jobs': [], 'writer_cgroups': [],
            'config_maps': [{'name': 'fixture', 'uid': uid, 'data_sha256': 'a' * 64}],
            'bound_roots': [{'role': role, 'path': '/unopened/' + role,
                             'uid': 0, 'dev': 1, 'ino': number}
                            for number, role in enumerate(('cache_primary', 'cache_replica',
                                                           'corpus_primary', 'corpus_replica'), 1)],
            'persistent_volumes': [], 'index_tree': 'fixture-index',
            'original_tree_policies': {}, 'restore': {}, 'manual_recovery': {},
            'failure_domains': {'hdd': ['/dev/fixture-hdd'], 'ssd': ['/dev/fixture-ssd'],
                'leaf_devices': [{'path': '/dev/fixture-' + medium, 'major': 8,
                    'minor': number, 'sysfs_path': '/sys/devices/fixture/' + medium,
                    'identity_file': 'wwid', 'identity_sha256': 'a' * 64}
                    for number, medium in enumerate(('hdd', 'ssd'), 1)]},
        }

    def validate(self, config):
        with mock.patch('linux_backend.os.open', side_effect=AssertionError('unexpected open')), \
                mock.patch.object(LinuxBackend, '_run', side_effect=AssertionError('unexpected command')):
            LinuxBackend._validate_linux(config)

    def test_positive_decimal_serials_preserve_all_digits(self):
        for length in (1, 50, 51, 60):
            with self.subTest(length=length):
                serial = ('1234567890' * 6)[:length]
                config = self.config(serial)
                before = canonical(config)
                self.validate(config)
                self.assertEqual(config['postgres']['binding']['standby_client_serial'], serial)
                self.assertEqual(canonical(config), before)

    def test_out_of_domain_serials_refuse_without_coercion(self):
        for serial in ('9' * 61, '0', '00', '-1', '+1', True, False, 1,
                       int('9' * 60), 1.0, None, '', '1.0', '1e59', ' 1', '1 ', '1\n', '\u0661'):
            with self.subTest(serial=serial):
                config = self.config(serial)
                before = canonical(config)
                with self.assertRaisesRegex(Refused, 'postgres_replication_identity'):
                    self.validate(config)
                self.assertEqual(canonical(config), before)


class MutationOrdering(unittest.TestCase):
    def setUp(self):
        self.op = operation()
        self.backend = LinuxBackend()
        self.calls = []

    def install(self, responses):
        def sql(op, query, **kwargs):
            self.calls.append((query, kwargs))
            result = responses.pop(0)
            if isinstance(result, BaseException):
                raise result
            return copy.deepcopy(result)
        self.backend._sql = sql

    def test_wrong_initial_binding_refuses_before_mutation(self):
        bad = observation(self.op); bad['managed_epoch'] = str(uuid.uuid4())
        self.install([bad])
        with self.assertRaisesRegex(Refused, 'binding_changed'):
            self.backend._sql_mutation(self.op, 'SELECT fixed_function()')
        self.assertEqual(len(self.calls), 1)
        self.assertNotIn('BEGIN', self.calls[0][0])
        self.assertFalse(self.op.journal.values)

    def test_post_commit_prefix_and_all_checks_share_original_deadline(self):
        before = observation(self.op)
        fresh = dict(before, prefix_lsn='1/200')
        flushed = dict(fresh, flush_lsn='1/200')
        self.install([before, {'owned': True}, fresh, flushed])
        self.assertEqual(self.backend._sql_mutation(self.op, 'SELECT fixed_function()'), {'owned': True})
        self.assertEqual(len(self.calls), 4)
        self.assertTrue(self.calls[1][0].startswith('BEGIN; '))
        self.assertIn('capture_prefix', self.calls[2][0])
        self.assertIn('observe_flush', self.calls[3][0])
        self.assertEqual(len({call[1]['deadline'] for call in self.calls}), 1)
        self.assertEqual(len(self.op.journal.values), 1)
        proof = next(iter(self.op.journal.values.values()))
        self.assertEqual(proof['initial'], before)
        self.assertEqual(proof['before'], fresh)

    def test_local_commit_without_current_flush_never_acknowledges_or_replays(self):
        before = observation(self.op)
        fresh = dict(before, prefix_lsn='1/200')
        self.install([before, {'owned': True}, fresh, before])
        with self.assertRaisesRegex(Refused, 'ack_unproven'):
            self.backend._sql_mutation(self.op, 'SELECT fixed_function()')
        self.assertEqual(sum(query.startswith('BEGIN') for query, _ in self.calls), 1)
        self.assertFalse(self.op.journal.values)

    def test_primary_restart_after_commit_refuses(self):
        before = observation(self.op)
        changed = dict(before, postmaster_started_at='2026-11-02 00:00:02+00')
        self.install([before, {'owned': True}, changed, changed])
        with self.assertRaisesRegex(Refused, 'identity_changed'):
            self.backend._sql_mutation(self.op, 'SELECT fixed_function()')
        self.assertFalse(self.op.journal.values)

    def test_unknown_mutation_ack_never_repeats(self):
        self.install([observation(self.op), Refused('lost_ack')])
        with self.assertRaisesRegex(Refused, 'lost_ack'):
            self.backend._sql_mutation(self.op, 'SELECT fixed_function()')
        self.assertEqual(len(self.calls), 2)
        self.assertFalse(self.op.journal.values)


@unittest.skipUnless(os.geteuid() == 0, 'requires root ownership fixture')
class LocalCommandBoundary(unittest.TestCase):
    def setUp(self):
        self.op = operation()
        self.backend = LinuxBackend()
        executable = Path(sys.executable).resolve()
        self.op.inventory['linux']['binaries'] = {'fixture': {
            'path': str(executable), 'sha256': hashlib.sha256(executable.read_bytes()).hexdigest()}}

    def tearDown(self):
        if self.backend._docker_config is not None:
            directory,identity = self.backend._docker_config
            info = directory.lstat()
            self.assertEqual((info.st_dev,info.st_ino),identity)
            test_config = directory/'config.json'
            if test_config.exists():
                test_config.unlink()
            directory.rmdir()

    def test_hash_only_command_still_records_actual_exit_and_output_hash(self):
        value = self.backend._run(self.op, 'fixture', ['-I','-B','-c','print("fixture")'], record=False)
        self.assertEqual(value, b'fixture\n')
        row = next(iter(self.op.journal.values.values()))
        self.assertEqual(row['actual_exit_code'], 0)
        self.assertTrue(row['owned_processes_closed'])
        self.assertEqual(row['stdout_sha256'], hashlib.sha256(value).hexdigest())
        self.assertNotIn('stdout_hex', row)

    def test_streaming_output_cap_refuses_without_unbounded_buffer(self):
        with self.assertRaises(Refused):
            self.backend._run(self.op, 'fixture', ['-I','-B','-c',
                'import os; b=b"x"*65536\nwhile True: os.write(1,b)'], record=False, timeout=10)
        row = next(iter(self.op.journal.values.values()))
        self.assertTrue(row['output_limit_exceeded'])
        self.assertLessEqual(row['stdout_bytes'], MAX_OUTPUT)

    def test_expiry_kills_and_reaps_owned_command(self):
        with self.assertRaises(Refused):
            self.backend._run(self.op, 'fixture', ['-I','-B','-c','import time; time.sleep(30)'],
                              record=False, timeout=.05)
        row = next(iter(self.op.journal.values.values()))
        self.assertTrue(row['expired'])
        self.assertTrue(row['owned_processes_closed'])
        self.assertNotEqual(row['actual_exit_code'], 0)

    def test_docker_dispatch_uses_one_local_socket_and_actual_empty_private_config(self):
        # Explicitly synthetic argv adapter: execute the hash-pinned Python
        # fixture, never Docker. The production dispatch/FD/output paths run.
        self.op.inventory['linux']['binaries']['docker'] = self.op.inventory['linux']['binaries']['fixture']
        original = subprocess.Popen
        seen = []
        def spawn(argv, **kwargs):
            seen.append(argv)
            return original([argv[0],'-I','-B','-c',
                'import json,sys;print(json.dumps(sys.argv[1:]))',*argv[1:]],**kwargs)
        with mock.patch('linux_backend.subprocess.Popen',side_effect=spawn):
            raw = self.backend._run(self.op,'docker',
                ['--host','unix:///var/run/docker.sock','inspect','1'*64],record=False)
        args = json.loads(raw)
        self.assertEqual(args[:1],['--config'])
        config = Path(args[1])
        self.assertEqual(args[2:4],['--host','unix:///var/run/docker.sock'])
        self.assertEqual(args.count('--host'),1)
        self.assertEqual(stat.S_IMODE(config.stat().st_mode),0o700)
        self.assertEqual(list(config.iterdir()),[])
        self.assertEqual(len(seen),1)
        (config/'config.json').write_text('{"currentContext":"forbidden-fixture"}')
        with mock.patch('linux_backend.subprocess.Popen') as forbidden:
            with self.assertRaisesRegex(Refused,'docker_config_changed'):
                self.backend._run(self.op,'docker',['inspect','1'*64],record=False)
            forbidden.assert_not_called()

    def test_docker_external_context_flags_are_rejected_before_dispatch(self):
        self.op.inventory['linux']['binaries']['docker'] = self.op.inventory['linux']['binaries']['fixture']
        for args in (['--host','tcp://192.0.2.2:2375','ps'],['--context=foreign','ps'],
                     ['--config','/root/.docker','ps']):
            with self.subTest(args=args),mock.patch('linux_backend.subprocess.Popen') as forbidden:
                with self.assertRaisesRegex(Refused,'docker_fixed_local_context'):
                    self.backend._run(self.op,'docker',args,record=False)
                forbidden.assert_not_called()


@unittest.skipUnless(os.geteuid() == 0, 'requires root ownership fixture')
class RuntimeGuardReadback(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='linux-guard-test-', dir='/tmp')
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.directory = self.root / '.openlegal-runtime-guards'
        self.directory.mkdir(mode=0o700)
        lock = self.directory / '.directory.lock'; lock.touch(mode=0o600)
        self.op = operation(); self.backend = LinuxBackend()
        self.op.inventory['host'] = {'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
        self.op.inventory['linux']['bound_roots'] = [{'role':'cache_replica','path':str(self.root),'uid':0}]
        root = self.root.stat(); directory = self.directory.stat()
        self.record = {'schema_version':1,'managed_epoch':self.op.inventory['managed_epoch'],
            'binding_sha256':'a'*64,'key':'serve','operation_id':str(uuid.uuid4()),
            'parent':{'dev':root.st_dev,'ino':root.st_ino,'fsid':os.statvfs(self.root).f_fsid},
            'directory':{'dev':directory.st_dev,'ino':directory.st_ino,'fsid':os.statvfs(self.directory).f_fsid},
            'boot_id':self.op.inventory['host']['boot_id'],'pid':123,'process_start_ticks':10,
            'state':'closed_clean'}
        self.path = self.directory / ('serve.' + self.record['operation_id'] + '.json')
        self.write()

    def write(self):
        self.path.write_bytes(canonical(self.record)); self.path.chmod(0o600)

    def tearDown(self):
        self.temp.cleanup()

    def test_closed_clean_actual_records_are_read_only(self):
        original = self.path.read_bytes()
        result = self.backend._runtime_guards(self.op)
        self.assertEqual(result['records'][0]['state'], 'closed_clean')
        self.assertFalse(result['records'][0]['locked'])
        self.assertEqual(self.path.read_bytes(), original)

    def test_unlocked_active_record_blocks_without_being_rewritten(self):
        self.record['state'] = 'active_unclean'; self.write()
        original = self.path.read_bytes()
        with self.assertRaises(Refused):
            self.backend._runtime_guards(self.op)
        with self.assertRaises(Refused):
            self.backend._runtime_guards(self.op, active=True)
        self.assertEqual(self.path.read_bytes(), original)

    def test_actual_locked_active_serve_is_required_for_lookup(self):
        self.record['state'] = 'active_unclean'; self.write()
        with self.path.open('r+b') as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = self.backend._runtime_guards(self.op, active=True)
            self.assertTrue(result['guard']['locked'])
            self.assertEqual(result['guard']['state'], 'active')
            with self.assertRaises(Refused):
                self.backend._runtime_guards(self.op)

    def test_forged_identity_or_record_filename_refuses(self):
        self.record['directory']['ino'] += 1; self.write()
        with self.assertRaises(Refused):
            self.backend._runtime_guards(self.op)
        self.record['directory']['ino'] -= 1; self.write()
        self.path.rename(self.path.with_name('wrong.json'))
        with self.assertRaises(Refused):
            self.backend._runtime_guards(self.op)


if __name__ == '__main__':
    unittest.main()
