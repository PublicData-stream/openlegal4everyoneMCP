"""Synthetic control-plane tests and real tiny /tmp file-copy tests only.

These never invoke Docker, PostgreSQL, Kubernetes, network or a live inventory.
Mock completion envelopes are test data, not operational restore evidence.
"""
import copy
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import shlex
import sys
import tempfile
import time
import tomllib
import types
import unittest
from unittest import mock
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import restore_backend as r
from copy_tree import copy_tree
from interface import Refused, REQUIRED_TRUE
from inventory import canonical, digest
from linux_backend import tree_digest


class Fixture:
    def __init__(self):
        self.temp = tempfile.TemporaryDirectory(prefix='restore-unit-', dir='/tmp')
        self.root = Path(self.temp.name)
        self.chain = self.root / 'chain'
        (self.chain / 'originals').mkdir(parents=True)
        (self.chain / 'ssd').mkdir()
        self.operation = str(uuid.uuid4())
        self.workspace = Path('/tmp/openlegal-storage-restore-' + self.operation)
        self.datasets = []
        self.trees = []
        self.exports = {}
        self.copies = {}
        self.snapshots = []
        self.journals = []
        for index in range(2):
            root = self.root / ('journal-' + str(index))
            (root / self.operation / 'copy_ssd').mkdir(parents=True)
            self.journals.append({'path':str(root)})
        self.blob = b'synthetic evidence\n'
        self.hash = digest(self.blob)
        self.key = self.hash[:2] + '/' + self.hash + '-11111111-1111-4111-8111-111111111111'
        for i,name in enumerate(('pg','cache','corpus','index','dictionary','config','trust')):
            source = self.root / ('source-' + name)
            source.mkdir(mode=0o700)
            if name in ('cache','corpus'):
                (source / self.hash[:2]).mkdir(mode=0o700)
                (source / self.key).write_bytes(self.blob)
                (source / self.key).chmod(0o600)
            elif name == 'pg':
                (source / 'PG_VERSION').write_bytes(b'18\n')
                (source / 'global').mkdir()
                (source / 'global' / 'dummy').write_bytes(b'PG fixture')
            else:
                (source / 'data').write_bytes(('synthetic-' + name).encode())
            record = self.record(source, ['cache'] if name == 'cache' else ['corpus'] if name == 'corpus' else ['assets'])
            destination = self.chain / 'originals' / name
            manifest = copy_tree(record,destination,time.monotonic()+10)
            dataset = 'tank/' + name
            self.datasets.append({'name':dataset,'mountpoint':str(source),'guid':str(100+i),'pool':'tank'})
            self.exports[dataset] = {'destination':str(destination),'manifest':manifest,'snapshot_guid':str(200+i)}
            self.snapshots.append({'dataset':dataset,'guid':str(100+i),'snapshot':'tank/'+name+'@test',
                                   'snapshot_guid':str(200+i)})
        for name,source_name,role in [('standby','pg','postgres'),('cache_ssd','cache','cache'),('corpus_ssd','corpus','corpus')]:
            source = self.root / ('source-' + source_name)
            live = self.root / ('live-ssd-' + name)
            copy_tree(self.record(source,[role]),live,time.monotonic()+10)
            destination = self.chain / 'ssd' / name
            manifest = copy_tree(self.record(live,[role]),destination,time.monotonic()+10)
            self.trees.append({'name':name,'roles':[role],'path':str(live)})
            raw = canonical(manifest)
            self.copies[name] = digest(raw)
            for root in self.journals:
                path = Path(root['path']) / self.operation / 'copy_ssd' / ('copy-' + name + '.json')
                path.write_bytes(raw);path.chmod(0o600)
        self.config = {
            'schema_version':1,
            'primary':{'data':{'dataset':'tank/pg','relative':'.'},'wal':None},
            'standby':{'data':{'tree':'standby','relative':'.'},'wal':None},
            'blobs':{role:{'hdd':{'dataset':'tank/'+role,'relative':'.'},
                           'ssd':{'tree':role+'_ssd','relative':'.'}} for role in ('cache','corpus')},
            'index':{'dataset':'tank/index','relative':'.'},
            'dictionary':{'dataset':'tank/dictionary','relative':'.'},
            'server_image':'example.invalid/server@sha256:'+'b'*64,'runtime_role':'runtime'}
        self.inventory = {'revision':'1'*40,'original_datasets':self.datasets,'active_datasets':[],
            'ssd_trees':self.trees,
            'journal_roots':self.journals,
            'coverage':{**{role:['tank/'+role,role+'_ssd'] for role in ('cache','corpus')},
                'postgres':['tank/pg','standby'],'wal':['tank/pg','standby']},
            'linux':{'serving_image':self.config['server_image'],'restore':self.config,
                'postgres':{'uid':999,'database':'fixture','admin_user':'operator',
                    'primary':{'image_id':'sha256:'+'a'*64,'container_id':'1'*64},
                    'standby':{'image_id':'sha256:'+'a'*64,'container_id':'2'*64}}}}
        self.references = [{'role':role,'storage_key':self.key,'sha256':self.hash,'size':len(self.blob)}
                           for role in ('cache','corpus')]
        index = self.chain / 'originals' / 'index'
        index_record = self.record(index,['assets'])
        self.stopped = {'retained_tables':{'openlegal.example':{'rows':1,'sha256':'c'*64}},
                        'retained_references':self.references,'index':tree_digest(index_record,time.monotonic()+10),
                        'checkpoint':{'system_identifier':'1234','wal_lsn':'1/100','timeline':1,'index_ack':3}}
        self.refresh_layout()
        self.op = types.SimpleNamespace(operation_id=self.operation,phase='verify_isolated_restore',
            action='checkpoint',chain_root=self.chain,inventory=self.inventory,
            previous={'stop_postgres':{'facts':self.stopped},
                      'snapshot_same_pool':{'facts':{'snapshots':self.snapshots}},
                      'copy_ssd':{'manifests':self.copies},
                      'export_snapshots':{'facts':{'exports':self.exports,'full_self_contained_export':True,'parent_chains':[]}}},
            seal=lambda:None)
    def refresh_layout(self):
        def host(spec):
            dataset='dataset' in spec
            rows=self.datasets+self.inventory['active_datasets'] if dataset else self.trees
            key='dataset' if dataset else 'tree'
            row=next(value for value in rows if value['name']==spec[key])
            return str(Path(row['mountpoint' if dataset else 'path'])/spec['relative'])
        self.stopped['postgres_layout']={side:{
            'container_id':self.inventory['linux']['postgres'][side]['container_id'],
            'data_host_path':host(self.config[side]['data']),
            'wal_host_path':host(self.config[side]['wal']) if self.config[side]['wal'] is not None
                else str(Path(host(self.config[side]['data']))/'pg_wal'),
            'source_data':copy.deepcopy(self.config[side]['data']),
            'source_wal':copy.deepcopy(self.config[side]['wal'])} for side in ('primary','standby')}

    @staticmethod
    def record(path,roles):
        s=path.stat()
        return {'path':str(path),'identity':{'dev':s.st_dev,'ino':s.st_ino},'uid':s.st_uid,'gid':s.st_gid,
                'mode':stat.S_IMODE(s.st_mode),'max_files':100,'max_bytes':1024**2,'symlinks':{},'roles':roles}
    def close(self):
        if self.workspace.exists():shutil.rmtree(self.workspace)
        self.temp.cleanup()

    def flip_postgres_topology(self):
        """Synthetic SSD primary plus a new HDD standby; original seven stay."""
        source=self.root/'source-active-pg'
        copy_tree(self.record(self.root/'source-pg',['postgres']),source,time.monotonic()+10)
        (source/'global'/'dummy').write_bytes(b'active HDD standby PG fixture')
        dataset={'name':'tank/new-pg','guid':'999','pool':'tank','mountpoint':str(source)}
        self.inventory['active_datasets'].append(dataset)
        destination=self.chain/'originals'/'new-pg'
        manifest=copy_tree(self.record(source,['postgres']),destination,time.monotonic()+10)
        self.exports[dataset['name']]={'destination':str(destination),'manifest':manifest,
                                      'snapshot_guid':'1999'}
        self.snapshots.append({'dataset':dataset['name'],'guid':dataset['guid'],
            'snapshot':dataset['name']+'@test','snapshot_guid':'1999'})
        self.inventory['coverage']['postgres'].append(dataset['name'])
        self.inventory['coverage']['wal'].append(dataset['name'])
        self.config['primary']['data']={'tree':'standby','relative':'.'}
        self.config['standby']['data']={'dataset':dataset['name'],'relative':'.'}
        self.refresh_layout()
        return dataset


class FileAndPolicyTests(unittest.TestCase):
    def setUp(self):self.f=Fixture()
    def tearDown(self):self.f.close()
    def test_static_reference_verifier_rejects_wrong_modes_without_byte_changes(self):
        from linux_backend import LinuxBackend
        root=self.f.root/'source-cache';shard=root/self.f.hash[:2];path=root/self.f.key
        rows=[r for r in self.f.references if r['role']=='cache']
        original=path.read_bytes();wanted=digest(original)
        LinuxBackend._verify_references(root,rows)
        for target,wrong,correct in ((root,0o755,0o700),(shard,0o755,0o700),(path,0o644,0o600)):
            with self.subTest(target=target.name):
                target.chmod(wrong)
                try:
                    with self.assertRaises(Refused):LinuxBackend._verify_references(root,rows)
                    self.assertEqual(path.read_bytes(),original)
                    self.assertEqual(digest(path.read_bytes()),wanted)
                finally:target.chmod(correct)
        LinuxBackend._verify_references(root,rows)
    @unittest.skipUnless(os.geteuid()==0,'wrong-owner filesystem fixture requires root')
    def test_static_reference_verifier_rejects_wrong_shard_owner_with_same_bytes(self):
        from linux_backend import LinuxBackend
        root=self.f.root/'source-corpus';shard=root/self.f.hash[:2];path=root/self.f.key
        rows=[r for r in self.f.references if r['role']=='corpus']
        owner=shard.stat().st_uid;original=path.read_bytes();wanted=digest(original)
        os.chown(shard,owner+1,-1)
        try:
            with self.assertRaises(Refused):LinuxBackend._verify_references(root,rows)
            self.assertEqual(path.read_bytes(),original)
            self.assertEqual(digest(path.read_bytes()),wanted)
        finally:os.chown(shard,owner,-1)
        LinuxBackend._verify_references(root,rows)
    def test_static_reference_verifier_rejects_fifo_without_blocking_open(self):
        root=self.f.root/'source-cache';path=root/self.f.key
        rows=[r for r in self.f.references if r['role']=='cache']
        path.unlink();os.mkfifo(path,0o600)
        program="""import json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from linux_backend import LinuxBackend
from interface import Refused
try:
    LinuxBackend._verify_references(Path(sys.argv[2]),json.loads(sys.argv[3]))
except Refused:
    print('FIFO_REFUSED')
else:
    raise AssertionError('FIFO accepted as a blob')
"""
        child=subprocess.run([sys.executable,'-I','-B','-c',program,
            str(Path(__file__).resolve().parents[1]),str(root),json.dumps(rows)],
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=5,check=False,
            env={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','TMPDIR':'/tmp'})
        self.assertEqual(child.returncode,0,child.stderr.decode(errors='replace'))
        self.assertEqual(child.stdout,b'FIFO_REFUSED\n')
        self.assertTrue(stat.S_ISFIFO(path.lstat().st_mode))
    def test_exact_policy_and_no_executable_extensions(self):
        self.assertIs(r.validate(self.f.config,self.f.inventory),self.f.config)
        for key,value in [('command',['sh']),('sql','DROP'),('server_image','tag:latest')]:
            changed=copy.deepcopy(self.f.config);changed[key]=value
            with self.assertRaises(Refused):r.validate(changed,self.f.inventory)
        for path in ('../pg','/etc','data/../pg','data//nested','./pg'):
            changed=copy.deepcopy(self.f.config);changed['primary']['data']['relative']=path
            with self.assertRaises(Refused):r.validate(changed,self.f.inventory)
    def test_runtime_role_and_source_coverage_are_not_arbitrary(self):
        for role in ('postgres','role; DROP','UPPER','a'*64):
            changed=copy.deepcopy(self.f.config);changed['runtime_role']=role
            with self.assertRaises(Refused):r.validate(changed,self.f.inventory)
        changed=copy.deepcopy(self.f.config);changed['blobs']['corpus']['ssd']['tree']='cache_ssd'
        with self.assertRaises(Refused):r.validate(changed,self.f.inventory)
    def test_postgres_either_side_uses_covered_distinct_data_and_wal_bindings(self):
        self.f.flip_postgres_topology()
        r.validate(self.f.config,self.f.inventory)
        changed=copy.deepcopy(self.f.config)
        changed['standby']['data']=copy.deepcopy(changed['primary']['data'])
        with self.assertRaisesRegex(Refused,'restore_postgres_sources_alias'):
            r.validate(changed,self.f.inventory)
        changed=copy.deepcopy(self.f.config)
        changed['primary']['data']['dataset']='tank/pg'
        with self.assertRaisesRegex(Refused,'restore_source_shape'):
            r.validate(changed,self.f.inventory)
        self.f.inventory['active_datasets'].append({'name':'tank/new-wal','guid':'998',
            'pool':'tank','mountpoint':str(self.f.root/'unused-wal-source')})
        self.f.inventory['ssd_trees'].append({'name':'wal-copy','roles':['wal']})
        self.f.inventory['coverage']['wal']+=['tank/new-wal','wal-copy']
        self.f.config['primary']['wal']={'tree':'wal-copy','relative':'.'}
        self.f.config['standby']['wal']={'dataset':'tank/new-wal','relative':'.'}
        r.validate(self.f.config,self.f.inventory)
        self.f.inventory['coverage']['wal'].remove('wal-copy')
        with self.assertRaisesRegex(Refused,'restore_postgres_coverage'):
            r.validate(self.f.config,self.f.inventory)
    def test_inline_wal_and_postgres_tree_roles_are_required(self):
        self.f.inventory['coverage']['wal'].remove('standby')
        with self.assertRaisesRegex(Refused,'restore_inline_wal_coverage'):
            r.validate(self.f.config,self.f.inventory)
        self.f.inventory['coverage']['wal'].append('standby')
        self.f.trees[0]['roles']=['assets']
        with self.assertRaisesRegex(Refused,'restore_postgres_tree_role'):
            r.validate(self.f.config,self.f.inventory)
    def test_actual_stopped_layout_binds_cid_selectors_and_data_wal_host_paths(self):
        r._postgres_layout(self.f.op,self.f.config)
        original=copy.deepcopy(self.f.stopped['postgres_layout'])
        changes={'container_id':'f'*64,'source_data':{'dataset':'tank/config','relative':'.'},
                 'source_wal':{'dataset':'tank/pg','relative':'.'},
                 'data_host_path':str(self.f.root/'another-pg'),
                 'wal_host_path':str(self.f.root/'another-wal')}
        for key,value in changes.items():
            with self.subTest(field=key):
                self.f.stopped['postgres_layout']=copy.deepcopy(original)
                self.f.stopped['postgres_layout']['primary'][key]=value
                with self.assertRaisesRegex(Refused,'restore_postgres_layout_not_actual_source'):
                    r._postgres_layout(self.f.op,self.f.config)
        self.f.stopped['postgres_layout']=original
        self.f.flip_postgres_topology()
        r._postgres_layout(self.f.op,self.f.config)
        # Coverage alone must not authorize the inactive former HDD primary.
        self.f.config['standby']['data']={'dataset':'tank/pg','relative':'.'}
        r.validate(self.f.config,self.f.inventory)
        with self.assertRaisesRegex(Refused,'restore_postgres_layout_not_actual_source'):
            r._postgres_layout(self.f.op,self.f.config)
    def test_explicit_wal_layout_uses_actual_separate_source_path(self):
        self.f.inventory['active_datasets'].append({'name':'tank/wal','guid':'998','pool':'tank',
            'mountpoint':str(self.f.root/'live-wal')})
        self.f.inventory['coverage']['wal'].append('tank/wal')
        self.f.config['primary']['wal']={'dataset':'tank/wal','relative':'pg-wal'}
        self.f.refresh_layout()
        r.validate(self.f.config,self.f.inventory)
        r._postgres_layout(self.f.op,self.f.config)
        self.f.stopped['postgres_layout']['primary']['wal_host_path']=str(
            Path(self.f.stopped['postgres_layout']['primary']['data_host_path'])/'pg_wal')
        with self.assertRaisesRegex(Refused,'restore_postgres_layout_not_actual_source'):
            r._postgres_layout(self.f.op,self.f.config)

    def test_postgres_data_requires_one_dataset_and_one_tree(self):
        self.f.flip_postgres_topology()
        self.f.config['primary']['data']={'dataset':'tank/pg','relative':'.'}
        with self.assertRaisesRegex(Refused,'restore_postgres_independent_media'):
            r.validate(self.f.config,self.f.inventory)
    def test_missing_layout_refuses_before_workspace_or_actor(self):
        del self.f.stopped['postgres_layout']
        with self.assertRaisesRegex(Refused,'restore_postgres_layout_shape'):
            r._postgres_layout(self.f.op,self.f.config)
        self.assertFalse(self.f.workspace.exists())

    def test_real_copy_readback_and_source_metadata_unchanged(self):
        spec=self.f.config['blobs']['corpus']['hdd'];target=self.f.root/'new-copy'
        source,expected=r._source(self.f.op,spec)
        original=(source/self.f.key).stat()
        record,manifest,before,again=r._copy(self.f.op,spec,target,['corpus'],time.monotonic()+10)
        self.assertEqual((target/self.f.key).read_bytes(),self.f.blob)
        self.assertEqual(manifest['entries'],expected['entries'])
        self.assertEqual(r._verify_source(record,again,time.monotonic()+10),before)
        now=(source/self.f.key).stat()
        self.assertEqual((original.st_ino,original.st_mtime_ns,original.st_ctime_ns),
                         (now.st_ino,now.st_mtime_ns,now.st_ctime_ns))
        with self.assertRaises(Refused):r._copy(self.f.op,spec,target,['corpus'],time.monotonic()+10)
    def test_copy_deadline_and_archive_tampering_fail_before_pass(self):
        spec=self.f.config['blobs']['cache']['hdd'];target=self.f.root/'never-created'
        with self.assertRaises(Refused):r._copy(self.f.op,spec,target,['cache'],time.monotonic()-1)
        self.assertFalse(target.exists())
        source,expected=r._source(self.f.op,spec);record=r._record(source,expected,['cache'])
        (source/self.f.key).write_bytes(b'altered')
        with self.assertRaises(Refused):r._verify_source(record,expected,time.monotonic()+10)
    def test_hardlink_fifo_and_symlink_substitution_fail(self):
        spec=self.f.config['blobs']['cache']['hdd'];source,expected=r._source(self.f.op,spec)
        record=r._record(source,expected,['cache']);path=source/self.f.key
        path.unlink();os.mkfifo(path)
        with self.assertRaises(Refused):r._verify_source(record,expected,time.monotonic()+10)
        path.unlink();path.symlink_to('/etc/passwd')
        with self.assertRaises(Refused):r._verify_source(record,expected,time.monotonic()+10)
        path.unlink();path.write_bytes(self.f.blob);os.link(path,source/'extra-link')
        with self.assertRaises(Refused):r._verify_source(record,expected,time.monotonic()+10)
    def test_earlier_file_write_during_later_hash_cannot_hide_by_restoring_mtime(self):
        source=self.f.root/'coherence-source';source.mkdir()
        (source/'a').write_bytes(b'AAAA');(source/'b').write_bytes(b'BBBB')
        target=self.f.root/'coherence-archive'
        manifest=copy_tree(self.f.record(source,['assets']),target,time.monotonic()+10)
        expected={'entries':manifest['entries'],'root_metadata':manifest['root_metadata']}
        record=r._record(target,expected,['assets']);read=os.read;changed=[]
        def race(fd,size):
            data=read(fd,size)
            if data==b'BBBB' and not changed:
                old=(target/'a').stat();(target/'a').write_bytes(b'XXXX')
                os.utime(target/'a',ns=(old.st_atime_ns,old.st_mtime_ns));changed.append(True)
            return data
        with mock.patch.object(r.os,'read',side_effect=race):
            with self.assertRaisesRegex(Refused,'restore_source_final_coherence'):
                r._verify_source(record,expected,time.monotonic()+10)
        self.assertEqual(changed,[True])

    def test_directory_identity_failure_closes_its_new_descriptor(self):
        original=self.f.root.stat();closed=[];close=os.close
        def closing(fd):closed.append(fd);close(fd)
        with mock.patch.object(r.Path,'lstat',return_value=types.SimpleNamespace(
                st_dev=original.st_dev,st_ino=original.st_ino+1)), \
             mock.patch.object(r.os,'close',side_effect=closing):
            with self.assertRaisesRegex(Refused,'restore_directory_rebound'):r._directory(self.f.root)
        self.assertEqual(len(closed),1)
        with self.assertRaises(OSError):os.fstat(closed[0])

    def test_ssd_requires_two_equal_hash_pinned_manifest_journals(self):
        if os.geteuid()!=0:self.skipTest('production private manifest reader requires root')
        spec=self.f.config['blobs']['cache']['ssd']
        self.assertEqual(r._source(self.f.op,spec)[0],self.f.chain/'ssd/cache_ssd')
        second=Path(self.f.journals[1]['path'])/self.f.operation/'copy_ssd/copy-cache_ssd.json'
        second.write_bytes(b'{}\n')
        with self.assertRaises(Refused):r._source(self.f.op,spec)
    def test_manifest_json_duplicates_nonfinite_and_extra_path_refused(self):
        for raw in (b'{"entries":[],"entries":[]}',b'{"x":NaN}',b'{"x":1e999}'):
            with self.assertRaises(Refused):r._manifest_decode(raw)
        root=self.f.chain/'originals/index';manifest=copy.deepcopy(self.f.exports['tank/index']['manifest'])
        manifest['entries'][0]['path']='../escape'
        with self.assertRaises(Refused):r._selection(root,manifest,'.')
    def test_fixed_fixture_config_has_no_provider_or_external_endpoint(self):
        value=tomllib.loads(r._fixture_config('1'*40).decode())
        self.assertNotIn('ingestion',value['database']);self.assertFalse(value['database']['auto_collection'])
        self.assertEqual(value['cache']['postgres']['replication_mode'],'local')
        self.assertNotIn('url',value['cache']['postgres'])
        self.assertEqual(value['database']['index_path'],'/restore-index')
        self.assertIn("primary_conninfo=''",r.PG_CONFIG)
        self.assertIn("shared_preload_libraries=''",r.PG_CONFIG)
    def test_pg_local_config_changes_only_clone_and_denies_other_links(self):
        source=self.f.chain/'originals/pg';root=self.f.root/'pg-clone'
        manifest=copy_tree(self.f.record(source,['postgres']),root,time.monotonic()+10)
        (root/'standby.signal').write_bytes(b'')
        (root/'postgresql.auto.conf').write_bytes(b"primary_conninfo='unsafe'\n")
        r._prepare_pg(root,manifest,None,os.geteuid())
        self.assertFalse((root/'standby.signal').exists())
        self.assertIn(b'no inherited settings',(root/'postgresql.auto.conf').read_bytes())
        self.assertFalse((source/'postgresql.auto.conf').exists())
        manifest['entries'].append({'kind':'symlink','path':'pg_tblspc/123','target':'/outside'})
        with self.assertRaises(Refused):r._prepare_pg(root,manifest,None,os.geteuid())
    def test_final_source_witness_does_not_silently_ignore_symlink_xattrs(self):
        source=self.f.root/'link-source';source.mkdir();(source/'link').symlink_to('/never-follow')
        record=self.f.record(source,['assets']);record['symlinks']={'link':'/never-follow'}
        archive=self.f.root/'link-archive'
        manifest=copy_tree(record,archive,time.monotonic()+10)
        expected={'entries':manifest['entries'],'root_metadata':manifest['root_metadata']}
        record=r._record(archive,expected,['assets']);original=os.listxattr
        def changed(path,*args,**kwargs):
            if isinstance(path,str) and path.endswith('/link') and kwargs.get('follow_symlinks')is False:
                return ['user.unrepresented']
            return original(path,*args,**kwargs)
        with mock.patch.object(r.os,'listxattr',side_effect=changed):
            with self.assertRaisesRegex(Refused,'restore_source_symlink_xattrs'):
                r._verify_source(record,expected,time.monotonic()+10)

    def test_external_wal_rebound_only_in_fresh_clone(self):
        root=self.f.root/'wal-clone';root.mkdir();(root/'PG_VERSION').write_bytes(b'18\n')
        (root/'pg_wal').symlink_to('/original/wal')
        manifest={'entries':[{'kind':'symlink','path':'pg_wal','target':'/original/wal'}]}
        with self.assertRaises(Refused):r._prepare_pg(root,manifest,None,os.geteuid())
        r._prepare_pg(root,manifest,self.f.root/'cloned-wal',os.geteuid())
        self.assertEqual(os.readlink(root/'pg_wal'),'/restore-wal')


class StartGateTests(unittest.TestCase):
    def test_gate_creation_requires_sealed_physical_identity_and_is_exclusive(self):
        with tempfile.TemporaryDirectory(prefix='restore-gate-',dir='/tmp')as directory:
            root=Path(directory);pid=os.getpid()
            ticks=Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()[19]
            actor=types.SimpleNamespace(kind='rebuild',identity_sealed=False,pid_identity=(pid,ticks),
                groups=['0::/synthetic'],cid='a'*64,evidence=lambda *args:None,
                inspect=lambda:{'State':{'Running':True,'Pid':pid}})
            with self.assertRaisesRegex(Refused,'restore_index_identity_not_sealed'):r._release_index_gate(actor,root)
            self.assertFalse((root/'start').exists())
            actor.identity_sealed=True;r._release_index_gate(actor,root)
            self.assertEqual((root/'start').read_bytes(),b'openlegal-isolated-index-start-v1\n')
            with self.assertRaises(FileExistsError):r._release_index_gate(actor,root)

    def test_harmless_local_gated_exec_preserves_observed_pid_and_start_ticks(self):
        # Demonstrates only Linux exec ownership, not Docker/native-server proof.
        with tempfile.TemporaryDirectory(prefix='restore-exec-',dir='/tmp')as directory:
            gate=Path(directory)/'start'
            native="/usr/local/bin/openlegal-server --rebuild-corpus-index /fixture/server.toml"
            harmless=shlex.quote(sys.executable)+" -c "+shlex.quote('import time; time.sleep(0.3)')
            script=r.INDEX_START_GATE.replace('/restore-control/start',str(gate)).replace(native,harmless)
            child=subprocess.Popen(['/bin/sh','-c',script],start_new_session=True,
                stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                before=Path(f'/proc/{child.pid}/stat').read_text().rsplit(')',1)[1].split()[19]
                gate.write_bytes(b'start')
                deadline=time.monotonic()+3;observed=False
                while time.monotonic()<deadline and child.poll() is None:
                    try:
                        image=os.readlink(f'/proc/{child.pid}/exe')
                        if Path(image).resolve()==Path(sys.executable).resolve():
                            after=Path(f'/proc/{child.pid}/stat').read_text().rsplit(')',1)[1].split()[19]
                            self.assertEqual(before,after);observed=True;break
                    except FileNotFoundError:pass
                    time.sleep(0.005)
                self.assertTrue(observed)
                stdout,stderr=child.communicate(timeout=3)
                self.assertEqual((child.returncode,stdout,stderr),(0,b'',b''))
            finally:
                if child.poll() is None:
                    import signal
                    os.killpg(child.pid,signal.SIGKILL);child.communicate(timeout=3)


class DockerFixture:
    def __init__(self):self.actor=None;self.calls=[];self.artifacts=[];self.removed=False;self.exit=0
    def _artifact(self,op,name,value):self.artifacts.append((name,value))
    def _run(self,op,binary,args,**kwargs):
        self.calls.append((binary,args,kwargs));assert args[:2]==r.DOCKER
        args=args[2:]
        if args[:2]==['image','inspect']:
            return canonical([{'Id':'sha256:'+'d'*64,'RepoDigests':[self.actor.image],
                               'Config':{'Labels':{'image':'fixed'},'Env':['PATH=/usr/bin']}}])
        if args[0]=='ps':return b''
        if args[0]=='create':return ('e'*64+'\n').encode()
        if args[0]=='inspect':return canonical([self.item()])
        if args[0]=='rm':self.removed=True;return ('e'*64+'\n').encode()
        if args[0]=='exec':return b'{"ok":true}\n'
        raise AssertionError(args)
    def item(self):
        a=self.actor
        return {'Id':'e'*64,'Name':'/'+a.name,'Image':'sha256:'+'d'*64,
                'Config':{'Labels':a.expected_labels,'User':a.user,'Entrypoint':[a.entrypoint],
                          'Cmd':a.arguments,'Healthcheck':{'Test':['NONE']},'Env':a.expected_env},
                'HostConfig':{'NetworkMode':a.network,'ReadonlyRootfs':True,'Privileged':False,'CapDrop':['ALL'],
                    'CapAdd':None,'Memory':512*1024**2,'NanoCpus':1_000_000_000,'PidsLimit':128,
                    'RestartPolicy':{'Name':'no'},'PortBindings':{},'Devices':[],'PidMode':'',
                    'SecurityOpt':['no-new-privileges']},
                'Mounts':[dict(Type='bind',Source=m['source'],Destination=m['destination'],RW=m['rw'])for m in a.mounts],
                'NetworkSettings':{'Ports':{}},
                'State':{'Running':False,'Pid':0,'ExitCode':self.exit,'OOMKilled':False,'Error':'',
                         'Status':'created','StartedAt':'0001-01-01T00:00:00Z'}}


class ActorBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.f=Fixture();self.backend=DockerFixture()
        self.actor=r.Actor(self.backend,self.f.op,'primary',self.f.config['server_image'],
            [{'source':str(self.f.root),'destination':r.PG_TARGET,'rw':True}],
            '999:999',self.f.root)
        self.backend.actor=self.actor
        self.actor.create('/usr/local/bin/postgres',['-D',r.PG_TARGET])
    def tearDown(self):self.f.close()
    def test_pinned_manifest_digest_resolves_config_id_without_conflating_domains(self):
        self.assertEqual(self.actor.image_id,'sha256:'+'d'*64)
        command=next(args for _,args,_ in self.backend.calls if args[2]=='create')
        self.assertIn('--pull=never',command);self.assertIn('--no-healthcheck',command)
        self.assertIn('--read-only',command)
        self.assertEqual(self.actor.expected_labels['image'],'fixed')
        self.actor.inspect()
    def test_isolation_and_command_mutations_are_rejected_before_exec(self):
        original=self.backend.item
        mutations=[lambda i:i['HostConfig'].__setitem__('NetworkMode','host'),
            lambda i:i['HostConfig'].__setitem__('Privileged',True),
            lambda i:i['Mounts'][0].__setitem__('Source','/production'),
            lambda i:i['Config'].__setitem__('Cmd',['arbitrary']),
            lambda i:i['Config'].__setitem__('Healthcheck',{'Test':['CMD','unsafe']}),
            lambda i:i['Config'].__setitem__('Env',['PGHOST=production']),
            lambda i:i['Config']['Labels'].__setitem__('openlegal.operation',str(uuid.uuid4()))]
        for mutation in mutations:
            def changed():
                item=copy.deepcopy(original());mutation(item);return item
            with mock.patch.object(self.backend,'item',changed):
                with self.assertRaises(Refused):self.actor.inspect()
    def test_owned_failure_exit_is_recorded_removed_and_never_passed(self):
        self.backend.exit=7
        with self.assertRaisesRegex(Refused,'restore_exit_not_zero_or_cleanup_failed'):self.actor.close()
        self.assertTrue(self.backend.removed)
        self.assertFalse(self.actor.created)
        self.assertTrue(any(value.get('actual_container_exit_code')==7 for _,value in self.backend.artifacts))
    def test_unobserved_exited_process_is_not_called_physically_closed(self):
        original=self.backend.item
        def changed():
            item=original();item['State'].update(Status='exited',StartedAt='2026-11-02T00:00:00Z');return item
        with mock.patch.object(self.backend,'item',changed):
            with self.assertRaisesRegex(Refused,'restore_unobserved_process_closure'):self.actor.close()
        self.assertFalse(self.backend.removed)

    def test_production_cid_and_unowned_sql_targets_cannot_be_adopted(self):
        self.actor.cid=self.f.inventory['linux']['postgres']['primary']['container_id']
        with self.assertRaises(Refused):self.actor.sql('SELECT true')
        self.assertFalse(any(args[2]=='exec'for _,args,_ in self.backend.calls))


class SyntheticFlowTests(unittest.TestCase):
    def setUp(self):self.f=Fixture();self.events=[];self.receipts=[]
    def tearDown(self):self.f.close()
    def backend(self):
        from linux_backend import LinuxBackend
        return types.SimpleNamespace(_artifact=lambda *args:None,
            _verify_references=LinuxBackend._verify_references,
            _receipt=lambda op,facts:self.receipts.append(facts)or facts)
    def mocks(self,fail=False):
        events=self.events
        class SyntheticActor:
            def __init__(self,backend,op,side,*args,**kwargs):self.kind=side;self.cid=('a'if side=='primary'else'b')*64
            def create(self,*args):events.append(self.kind+':create')
            def start(self):events.append(self.kind+':start')
            def ready(self):events.append(self.kind+':ready')
            def close(self):events.append(self.kind+':close')
        def baseline(backend,op,actor,stopped):
            events.append(actor.kind+':baseline')
            if fail:raise Refused('synthetic_baseline_failure')
            return {'watermark':3}
        return mock.patch.object(r,'Actor',SyntheticActor),mock.patch.object(r,'_prepare_pg'),\
            mock.patch.object(r,'_baseline',baseline),mock.patch.object(r,'_rebuild',side_effect=lambda *args:events.append('index:rebuild'))
    def test_synthetic_all_gates_precede_receipt_and_workspace_is_removed(self):
        if os.geteuid()!=0:self.skipTest('verify root-only production scope; synthetic flow run under root')
        a,b,c,d=self.mocks()
        with a,b,c,d:result=r.verify(self.backend(),self.f.op)
        self.assertTrue(all(result[k]is True for k in REQUIRED_TRUE['verify_isolated_restore']))
        self.assertEqual(result['parent_chains'],[])
        self.assertEqual(result['export_receipt_sha256'],digest(canonical(self.f.op.previous['export_snapshots'])))
        self.assertLess(self.events.index('primary:baseline'),self.events.index('index:rebuild'))
        self.assertLess(self.events.index('primary:close'),self.events.index('standby:create'))
        self.assertEqual(len(self.receipts),1);self.assertFalse(self.f.workspace.exists())
    def test_flipped_active_pair_restores_ssd_primary_and_new_hdd_standby(self):
        if os.geteuid()!=0:self.skipTest('root-only synthetic flow')
        active=self.f.flip_postgres_topology();selected={};original=r._copy
        def copied(op,spec,destination,roles,deadline):
            result=original(op,spec,destination,roles,deadline)
            if destination.name in ('pg-primary','pg-standby'):
                selected[destination.name]=(copy.deepcopy(spec),(destination/'global'/'dummy').read_bytes())
            return result
        a,b,c,d=self.mocks()
        with a,b,c,d,mock.patch.object(r,'_copy',side_effect=copied):
            result=r.verify(self.backend(),self.f.op)
        self.assertEqual(selected['pg-primary'],({'tree':'standby','relative':'.'},b'PG fixture'))
        self.assertEqual(selected['pg-standby'],({'dataset':active['name'],'relative':'.'},
            b'active HDD standby PG fixture'))
        self.assertEqual(len([k for k in result['source_witnesses'] if k.startswith('full-export-')]),8)
        self.assertEqual(len(self.f.inventory['original_datasets']),7)
        self.assertEqual(result['restored_sides'],['primary','standby'])
        self.assertEqual(len(self.receipts),1);self.assertFalse(self.f.workspace.exists())
    def test_missing_active_export_refuses_before_workspace_or_actor(self):
        if os.geteuid()!=0:self.skipTest('root-only synthetic flow')
        active=self.f.flip_postgres_topology();del self.f.exports[active['name']]
        with mock.patch.object(r,'Actor')as actor:
            with self.assertRaisesRegex(Refused,'restore_requires_full_independent_export'):
                r.verify(self.backend(),self.f.op)
            actor.assert_not_called()
        self.assertFalse(self.f.workspace.exists());self.assertEqual(self.receipts,[])
    def test_missing_or_changed_active_snapshot_refuses_before_workspace_or_actor(self):
        if os.geteuid()!=0:self.skipTest('root-only synthetic flow')
        self.f.flip_postgres_topology();active=self.f.snapshots.pop()
        with mock.patch.object(r,'Actor')as actor:
            with self.assertRaisesRegex(Refused,'restore_snapshot_dataset_coverage'):
                r.verify(self.backend(),self.f.op)
            self.f.snapshots.append(dict(active,guid='changed'))
            with self.assertRaisesRegex(Refused,'restore_snapshot_export_identity'):
                r.verify(self.backend(),self.f.op)
            actor.assert_not_called()
        self.assertFalse(self.f.workspace.exists());self.assertEqual(self.receipts,[])
    def test_inactive_original_pg_archive_is_still_fully_verified(self):
        if os.geteuid()!=0:self.skipTest('root-only synthetic flow')
        self.f.flip_postgres_topology()
        original=Path(self.f.exports['tank/pg']['destination'])/'global'/'dummy'
        original.write_bytes(b'original archive tampering')
        with mock.patch.object(r,'Actor')as actor:
            with self.assertRaises(Refused):r.verify(self.backend(),self.f.op)
            actor.assert_not_called()
        self.assertEqual(self.receipts,[])
    def add_unselected_ssd_tree(self):
        name='retired-pg';source=self.f.root/'retired-live-pg'
        copy_tree(self.f.record(self.f.root/'source-pg',['postgres']),source,time.monotonic()+10)
        target=self.f.chain/'ssd'/name
        manifest=copy_tree(self.f.record(source,['postgres']),target,time.monotonic()+10)
        self.f.trees.append({'name':name,'path':str(source),'roles':['postgres']})
        raw=canonical(manifest);self.f.copies[name]=digest(raw)
        for root in self.f.journals:
            p=Path(root['path'])/self.f.operation/'copy_ssd'/('copy-'+name+'.json')
            p.write_bytes(raw);p.chmod(0o600)
        return target
    def test_all_unselected_ssd_trees_have_before_and_after_witnesses(self):
        if os.geteuid()!=0:self.skipTest('root-only synthetic flow')
        self.add_unselected_ssd_tree()
        a,b,c,d=self.mocks()
        with a,b,c,d:result=r.verify(self.backend(),self.f.op)
        self.assertEqual(len([k for k in result['source_witnesses'] if k.startswith('full-ssd-')]),4)
    def test_unselected_ssd_archive_corruption_refuses_before_actor(self):
        if os.geteuid()!=0:self.skipTest('root-only synthetic flow')
        target=self.add_unselected_ssd_tree();(target/'global'/'dummy').write_bytes(b'tampered')
        with mock.patch.object(r,'Actor')as actor:
            with self.assertRaises(Refused):r.verify(self.backend(),self.f.op)
            actor.assert_not_called()
        self.assertEqual(self.receipts,[])
    def test_unselected_ssd_change_after_native_baselines_refuses_final_receipt(self):
        if os.geteuid()!=0:self.skipTest('root-only synthetic flow')
        target=self.add_unselected_ssd_tree()
        a,b,c,d=self.mocks()
        def rebuild(*args):
            (target/'global'/'dummy').write_bytes(b'changed after initial archive witness')
        with a,b,c,d,mock.patch.object(r,'_rebuild',side_effect=rebuild):
            with self.assertRaises(Refused):r.verify(self.backend(),self.f.op)
        self.assertIn('standby:close',self.events)
        self.assertEqual(self.receipts,[])

    def test_missing_ssd_manifest_refuses_before_workspace(self):
        if os.geteuid()!=0:self.skipTest('root-only synthetic flow')
        self.add_unselected_ssd_tree();del self.f.copies['retired-pg']
        with self.assertRaisesRegex(Refused,'restore_ssd_copy_coverage'):
            r.verify(self.backend(),self.f.op)
        self.assertFalse(self.f.workspace.exists());self.assertEqual(self.receipts,[])

    def test_synthetic_baseline_failure_closes_actor_preserves_partial_and_has_no_receipt(self):
        if os.geteuid()!=0:self.skipTest('verify root-only production scope; synthetic flow run under root')
        a,b,c,d=self.mocks(True)
        with a,b,c,d:
            with self.assertRaisesRegex(Refused,'synthetic_baseline_failure'):r.verify(self.backend(),self.f.op)
        self.assertIn('primary:close',self.events);self.assertNotIn('standby:create',self.events)
        self.assertTrue(self.f.workspace.is_dir());self.assertEqual(self.receipts,[])
    def test_unknown_partial_workspace_refuses_before_actor(self):
        if os.geteuid()!=0:self.skipTest('verify root-only production scope; synthetic flow run under root')
        self.f.workspace.mkdir()
        with mock.patch.object(r,'Actor')as actor:
            with self.assertRaises(Refused):r.verify(self.backend(),self.f.op)
        actor.assert_not_called()


if __name__=='__main__':unittest.main()
