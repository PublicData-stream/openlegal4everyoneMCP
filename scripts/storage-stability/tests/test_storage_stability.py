"""Offline tests. Every written path is a disposable /tmp fixture; no host actions.

Dual-journal positive tests mock ONLY the independent-device identity because
all test files must stay under /tmp. Actual two-media durability is NOT proved.
The workflow fake backend is explicitly synthetic and never executes commands.
"""
import copy
import contextlib
import types
import hashlib
import importlib.util
import json
import os
import stat
import shutil
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE))
import interface,inventory,journal,copy_tree,retention,checkpoint


def put(path,data,mode=0o600):
    path=Path(path);path.write_bytes(data);path.chmod(mode);return path

def rec(path,roles=('cache',)):
    s=path.stat();return {'name':path.name,'roles':list(roles),'path':str(path),'identity':{'dev':s.st_dev,'ino':s.st_ino},'uid':s.st_uid,'gid':s.st_gid,'mode':stat.S_IMODE(s.st_mode),'max_files':100,'max_bytes':1024*1024,'symlinks':{}}

def provider_fixture(projection):
    state={'budget':{'utc_day':20000,'daily_used':5,'on_demand_used':0,'pilot_used':0,'next_request_at_ms':10,'next_allowed_at':0,'unresolved_response':not projection['clean'],'admission_owner':None},'control':{},'admissions':[],'canary_control':None,'canaries':[]}
    projection=dict(projection,accounting_sha256=inventory.digest(inventory.canonical(state)))
    return projection,state

def observation(operation,phase,stage,projection,state):
    return {'schema_version':1,'kind':'openlegal_checkpoint_provider_observation','operation_id':operation,'phase':phase,'stage':stage,'projection':copy.deepcopy(projection),'state':copy.deepcopy(state),'admission_ownership':{'legacy_locked':False,'slots':[{'owner':r['owner'],'slot':r['slot'],'locked':True}for r in state['admissions']]},'hold_control':dict(state['control'],generation=projection['generation'],recovery_hold=projection['recovery_hold'],hold_started_at=1 if projection['recovery_hold']else None)}

def closure_fixture(operation,phase,after_pin):
    return {'schema_version':1,'kind':'openlegal_checkpoint_provider_settlement_closure','operation_id':operation,'phase':phase,'provider_state_after_sha256':after_pin,'cri_containers':[{'id':'synthetic-exited','state':'CONTAINER_EXITED'}],'cgroups':[{'path':'/synthetic/cgroup','dev':1,'ino':2,'pids':[]}],'sql_sessions':[],'runtime_guard_directory':{'path':'/synthetic/guard','dev':1,'ino':3},'runtime_guards':[{'name':'serve.synthetic.json','sha256':'f'*64,'state':'closed_clean','locked':False}]}

class Fixture(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='openlegal-storage-stability-tests-',dir='/tmp');self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name);self.root.chmod(0o700)
    def mkdir(self,name):
        p=self.root/name;p.mkdir(mode=0o700);return p

class CopyTests(Fixture):
    def test_copy_exact_bytes_metadata_xattrs_and_empty_directory(self):
        source=self.mkdir('source');(source/'empty').mkdir(mode=0o700);file=put(source/'evidence',b'fictional retained bytes')
        os.setxattr(file,'user.fixture',b'opaque-metadata');target=self.root/'copy'
        result=copy_tree.copy_tree(rec(source),target,time.monotonic()+5)
        self.assertEqual((target/'evidence').read_bytes(),b'fictional retained bytes');self.assertTrue((target/'empty').is_dir());self.assertEqual(os.getxattr(target/'evidence','user.fixture'),b'opaque-metadata')
        self.assertTrue(result['all_bytes_read_back']);self.assertFalse(result['source_writers_quiescent_claimed']);self.assertFalse(result['legal_permanent_file_count_claimed'])
    def test_seed_marker_preserved_but_never_accepted_as_new_root_seed(self):
        source=self.mkdir('source');put(source/'.openlegal-seed-v1.json',b'{"synthetic_fixture":true}\n',0o644)
        result=copy_tree.copy_tree(rec(source),self.root/'copy',time.monotonic()+5)
        self.assertEqual(result['entries'][0]['kind'],'metadata_file');self.assertFalse(result['copied_seed_marker_accepted_for_new_root'])
    def test_existing_destination_not_overwritten(self):
        source=self.mkdir('source');target=self.mkdir('target');put(target/'sentinel',b'preserved')
        with self.assertRaises(interface.Refused):copy_tree.copy_tree(rec(source),target,time.monotonic()+5)
        self.assertEqual((target/'sentinel').read_bytes(),b'preserved')
    def test_overlap_refuses_before_source_mutation(self):
        source=self.mkdir('source')
        with self.assertRaises(interface.Refused):copy_tree.copy_tree(rec(source),source/'copy',time.monotonic()+5)
        self.assertEqual(list(source.iterdir()),[])
    def test_hardlink_is_unsupported_not_silently_flattened(self):
        source=self.mkdir('source');put(source/'a',b'fictional');os.link(source/'a',source/'b')
        with self.assertRaisesRegex(interface.Refused,'hardlink'):copy_tree.copy_tree(rec(source),self.root/'copy',time.monotonic()+5)
    def test_symlink_requires_exact_explicit_policy_and_is_never_followed(self):
        source=self.mkdir('source');os.symlink('/nonexistent/external/secret',source/'link')
        with self.assertRaisesRegex(interface.Refused,'symlink_unsupported'):copy_tree.copy_tree(rec(source),self.root/'rejected',time.monotonic()+5)
        record=rec(source);record['symlinks']={'link':'/nonexistent/external/secret'}
        result=copy_tree.copy_tree(record,self.root/'accepted',time.monotonic()+5)
        self.assertEqual(os.readlink(self.root/'accepted/link'),'/nonexistent/external/secret');self.assertFalse(result['restore_link_targets_accepted'])
    def test_fifo_refused_without_blocking(self):
        source=self.mkdir('source');os.mkfifo(source/'pipe',0o600)
        with self.assertRaisesRegex(interface.Refused,'special'):copy_tree.copy_tree(rec(source),self.root/'copy',time.monotonic()+5)
    def test_count_byte_deadline_and_identity_bounds(self):
        source=self.mkdir('source');put(source/'a',b'bytes');record=rec(source)
        for n,changes,end in [('bytes',{'max_bytes':1},time.monotonic()+5),('count',{'max_files':0},time.monotonic()+5),('time',{},time.monotonic()-1),('identity',{'identity':{'dev':0,'ino':0}},time.monotonic()+5)]:
            with self.subTest(n=n),self.assertRaises(interface.Refused):copy_tree.copy_tree(dict(record,**changes),self.root/n,end)
    def test_source_changes_during_read_are_refused_and_partial_is_preserved(self):
        source=self.mkdir('source');file=put(source/'a',b'first');real=copy_tree.os.read;changed=False
        def read(fd,size):
            nonlocal changed
            data=real(fd,size)
            if data==b'first'and not changed:changed=True;file.write_bytes(b'other')
            return data
        with patch.object(copy_tree.os,'read',side_effect=read):
            with self.assertRaises(interface.Refused):copy_tree.copy_tree(rec(source),self.root/'copy',time.monotonic()+5)
        self.assertTrue((self.root/'copy').exists());self.assertEqual(file.read_bytes(),b'other')
    def test_invalid_parent_does_not_leak_source_fd(self):
        source=self.mkdir('source');before=len(os.listdir('/proc/self/fd'))
        with self.assertRaises(OSError):copy_tree.copy_tree(rec(source),self.root/'missing/copy',time.monotonic()+5)
        self.assertEqual(len(os.listdir('/proc/self/fd')),before)

    def test_final_destination_mutation_refuses_even_if_size_and_mtime_restored(self):
        source=self.mkdir('source');put(source/'a',b'original');b=put(source/'b',b'later');target=self.root/'copy';real_read=os.read;changed=[]
        def read(fd,n):
            if not changed and os.fstat(fd).st_ino==b.stat().st_ino:
                a=target/'a';st=a.stat();a.write_bytes(b'modified');os.utime(a,ns=(st.st_atime_ns,st.st_mtime_ns));changed.append(True)
            return real_read(fd,n)
        with patch.object(copy_tree.os,'read',side_effect=read),self.assertRaises(interface.Refused):copy_tree.copy_tree(rec(source),target,time.monotonic()+5)
        self.assertTrue(changed);self.assertTrue(target.exists())
    def test_source_file_and_directory_atime_unchanged(self):
        source=self.mkdir('source');file=put(source/'data',b'synthetic');os.utime(file,ns=(1000000000,2000000000));os.utime(source,ns=(1000000000,2000000000));before=(source.stat().st_atime_ns,file.stat().st_atime_ns)
        copy_tree.copy_tree(rec(source),self.root/'copy',time.monotonic()+5)
        self.assertEqual((source.stat().st_atime_ns,file.stat().st_atime_ns),before)
    def test_seed_root0640_mode_preserved(self):
        source=self.mkdir('source');put(source/'.openlegal-seed-v1.json',b'{"synthetic_fixture":true}\n',0o640)
        target=self.root/'copy';result=copy_tree.copy_tree(rec(source),target,time.monotonic()+5)
        self.assertEqual(result['entries'][0]['mode'],0o640);self.assertEqual(stat.S_IMODE((target/'.openlegal-seed-v1.json').stat().st_mode),0o640)

class JournalTests(Fixture):
    def roots(self):
        a=self.mkdir('journal-a');b=self.mkdir('journal-b');return [{'path':str(p),'identity':{'dev':p.stat().st_dev,'ino':p.stat().st_ino}}for p in (a,b)]
    def fake_independent(self,roots):
        real=journal.directory;fake=copy.deepcopy(roots);fake[1]['identity']['dev']+=1;path=Path(fake[1]['path'])
        def directory(p,expected=None):
            if Path(p)==path:
                actual=real(p,roots[1]['identity'] if expected is not None else None)
                return fake[1]['identity']if expected is None else expected
            return real(p,expected)
        real_fstat=os.fstat
        def fstat(fd):
            value=real_fstat(fd)
            if (value.st_dev,value.st_ino)==(roots[1]['identity']['dev'],roots[1]['identity']['ino']):
                fields={key:getattr(value,key)for key in dir(value)if key.startswith('st_')};fields['st_dev']=fake[1]['identity']['dev'];return types.SimpleNamespace(**fields)
            return value
        @contextlib.contextmanager
        def identity_only_mock():
            with patch.object(journal,'directory',side_effect=directory),patch.object(journal.os,'fstat',side_effect=fstat):yield
        return fake,identity_only_mock()
    def test_same_media_refused_without_mock(self):
        with self.assertRaisesRegex(interface.Refused,'independent'):journal.DualJournal(self.roots(),str(uuid.uuid4()),{})
    def test_dual_write_prior_mutation_and_no_overwrite(self):
        roots,device_mock=self.fake_independent(self.roots());op=str(uuid.uuid4())
        with device_mock:
            j=journal.DualJournal(roots,op,{'synthetic_fixture':True});j.begin('qualify');j.save('proof.json',{'actual_fixture_file':True});j.begin('stop')
            for r in roots:self.assertTrue((Path(r['path'])/op/'qualify/proof.json').exists())
            with self.assertRaises(FileExistsError):j.begin('qualify')
    def test_late_old_proof_tamper_refuses_next_save(self):
        roots,device_mock=self.fake_independent(self.roots());op=str(uuid.uuid4())
        with device_mock:
            j=journal.DualJournal(roots,op,{});j.begin('qualify');j.save('proof.json',{'x':1});j.begin('stop')
            put(Path(roots[0]['path'])/op/'qualify/proof.json',b'changed')
            with self.assertRaises(interface.Refused):j.save('no-effect.json',{})
    def test_partial_second_media_failure_preserves_first_and_never_publishes(self):
        roots,device_mock=self.fake_independent(self.roots());op=str(uuid.uuid4())
        with device_mock:
            j=journal.DualJournal(roots,op,{});j.begin('stop');real=journal.new_file
            def write(path,raw):
                if str(path).startswith(roots[1]['path'])and Path(path).name=='effect-intent.json':raise OSError('synthetic second medium unavailable')
                return real(path,raw)
            with patch.object(journal,'new_file',side_effect=write):
                with self.assertRaises(OSError):j.save('effect-intent.json',{})
            self.assertTrue((Path(roots[0]['path'])/op/'stop/effect-intent.json').exists());self.assertNotIn('effect-intent.json',j.artifacts)
            with self.assertRaises(interface.Refused):j.check()
    def test_lock_is_shared_exclusive_and_rebinding_refused(self):
        path=put(self.root/'phase.lock',b'');s=path.stat();r={'path':str(path),'identity':{'dev':s.st_dev,'ino':s.st_ino}}
        with journal.SharedLock(r)as first:
            with self.assertRaises(BlockingIOError):journal.SharedLock(r).acquire()
            path.rename(self.root/'old-lock');put(path,b'')
            with self.assertRaises(interface.Refused):first.check()

class RetentionTests(Fixture):
    def setup_chains(self):
        root=self.mkdir('chains');put(root/'managed.json',inventory.canonical({'kind':'openlegal_storage_stability_owned_backup_namespace','schema_version':1}));return root
    def chain(self,root,age,verified=True):
        path=root/str(uuid.uuid4());path.mkdir(mode=0o700);now=time.time()
        record={'schema_version':1,'kind':'openlegal_storage_stability_chain','operation_id':path.name,'action':'checkpoint','created_at':now-age,'inventory_sha256':'a'*64,'source_graph_sha256':'b'*64,'authority_sha256':'c'*64,'managed_epoch':str(uuid.uuid4()),'revision':'1'*40,'automatic_retry':False,'automatic_failover':False,'chain_format':'full_self_contained_v1','parent_chains':[]};put(path/'chain.json',inventory.canonical(record))
        if verified:
            receipts={}
            for phase,name in(('export_snapshots','export-receipt.json'),('verify_isolated_restore','restore-receipt.json')):
                facts={key:True for key in interface.REQUIRED_TRUE[phase]};facts['parent_chains']=[]
                if phase=='verify_isolated_restore':facts['export_receipt_sha256']=receipts['export_snapshots']
                r={'schema_version':1,'kind':interface.RECEIPT_KIND,'operation_id':path.name,'action':'checkpoint','phase':phase,'inventory_sha256':'a'*64,'source_graph_sha256':'b'*64,'authority_sha256':'c'*64,'status':'passed','actual_exit_code':0,'owned_processes_closed':True,'facts':facts,'artifacts':{'synthetic.json':'f'*64}}
                raw=inventory.canonical(r);put(path/name,raw);receipts[phase]=inventory.digest(raw)
            put(path/'verified.json',inventory.canonical({'schema_version':1,'kind':'openlegal_storage_stability_verified_chain','operation_id':path.name,'inventory_sha256':'a'*64,'source_graph_sha256':'b'*64,'status':'passed','actual_exit_code':0,'owned_processes_closed':True,'verified_at':now-age,'chain_format':'full_self_contained_v1','parent_chains':[],'export_receipt_sha256':receipts['export_snapshots'],'restore_receipt_sha256':receipts['verify_isolated_restore']}))
        put(path/'bytes',b'synthetic backup');return path
    def test_fourteen_days_latest_verified_and_every_unverified_protected(self):
        root=self.setup_chains();old=self.chain(root,30*86400);recent=self.chain(root,2*86400);unverified=self.chain(root,50*86400,False)
        self.assertEqual(retention.plan(root,time.time()),[old]);retention.prune(old,root,time.time(),time.monotonic()+5)
        self.assertTrue(recent.exists());self.assertTrue(unverified.exists());self.assertFalse(old.exists())
    def test_only_latest_verified_is_never_pruned_even_if_old(self):
        root=self.setup_chains();only=self.chain(root,100*86400)
        self.assertEqual(retention.plan(root,time.time()),[])
        with self.assertRaises(interface.Refused):retention.prune(only,root,time.time(),time.monotonic()+5)
    def test_unknown_namespace_and_future_clock_refused(self):
        root=self.setup_chains();self.chain(root,-3600)
        with self.assertRaises(interface.Refused):retention.plan(root,time.time())
        put(root/'managed.json',inventory.canonical({'kind':'some-original-directory','schema_version':1}))
        with self.assertRaises(interface.Refused):retention.plan(root,time.time())
    def test_mount_guard_blocks_deletion(self):
        root=self.setup_chains();old=self.chain(root,30*86400);self.chain(root,86400)
        with patch.object(retention,'mounted_below',return_value=True):
            with self.assertRaises(interface.Refused):retention.prune(old,root,time.time(),time.monotonic()+5)
        self.assertTrue(old.exists())

    def test_parent_dependency_or_unknown_chain_schema_protects_all_chains(self):
        root=self.setup_chains();old=self.chain(root,30*86400);latest=self.chain(root,86400)
        for path,change in((latest,{'parent_chains':[old.name]}),(latest,{'unexpected_incremental_source':old.name})):
            raw=(path/'chain.json').read_bytes();value=inventory.decode(raw);value.update(change);put(path/'chain.json',inventory.canonical(value))
            with self.assertRaises(interface.Refused):retention.plan(root,time.time())
            self.assertTrue(old.exists());put(path/'chain.json',raw)
    def test_unverified_dependent_chain_also_protects_its_unknown_ancestor(self):
        root=self.setup_chains();old=self.chain(root,30*86400);self.chain(root,86400);partial=self.chain(root,100,False)
        record=inventory.decode((partial/'chain.json').read_bytes());record['parent_chains']=[old.name];put(partial/'chain.json',inventory.canonical(record))
        with self.assertRaises(interface.Refused):retention.plan(root,time.time())
        self.assertTrue(old.exists());self.assertTrue(partial.exists())
    def test_failed_or_rebound_restore_receipt_never_allows_pruning(self):
        root=self.setup_chains();old=self.chain(root,30*86400);self.chain(root,86400)
        raw=(old/'restore-receipt.json').read_bytes();value=inventory.decode(raw);value['owned_processes_closed']=False;changed=inventory.canonical(value);put(old/'restore-receipt.json',changed)
        verified=inventory.decode((old/'verified.json').read_bytes());verified['restore_receipt_sha256']=inventory.digest(changed);put(old/'verified.json',inventory.canonical(verified))
        with self.assertRaises(interface.Refused):retention.plan(root,time.time())
        self.assertTrue(old.exists())

class ParserTests(unittest.TestCase):
    def test_duplicate_nonfinite_and_deep_json_refused(self):
        for raw in (b'{"x":1,"x":2}',b'{"x":NaN}',b'['*40+b'0'+b']'*40):
            with self.subTest(raw=raw),self.assertRaises(interface.Refused):inventory.decode(raw)
    def test_relative_paths_and_nil_uuid_refused(self):
        for p in ('relative','/tmp/../etc','/'):
            with self.assertRaises(interface.Refused):inventory.absolute(p)
        self.assertFalse(inventory.valid_uuid('00000000-0000-0000-0000-000000000000'))


# Defined after the direct-run guard for unittest discovery; this suite is run
# through discovery below so all workflow cases are collected as well.
class WorkflowTests(Fixture):
    roots=JournalTests.roots
    fake_independent=JournalTests.fake_independent
    def prepared(self):
        actors=self.mkdir('actors')
        for name in interface.SOURCE_FILES:put(actors/name,(BASE/name).read_bytes())
        roots=self.roots();fake,device_mock=self.fake_independent(roots);backup=self.mkdir('backup');lock=put(self.root/'shared.lock',b'');ls=lock.stat()
        trees=[]
        for role in ('cache','corpus','postgres'):
            source=self.mkdir('ssd-'+role);put(source/'bytes',b'synthetic stopped tree');r=rec(source,(role,));r['name']=role;trees.append(r)
        datasets=[{'name':'pool/component'+str(n),'guid':str(n+1),'pool':'pool','mountpoint':str(self.root/('zfs-source-'+str(n)))}for n in range(7)]
        graph={name:inventory.digest((actors/name).read_bytes())for name in interface.SOURCE_FILES}
        inv={'schema_version':1,'kind':'openlegal_storage_stability_inventory','revision':'1'*40,'managed_epoch':str(uuid.uuid4()),'host':{'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'machine_id':Path('/etc/machine-id').read_text().strip(),'node_name':'synthetic-node'},'shared_lock':{'path':str(lock),'identity':{'dev':ls.st_dev,'ino':ls.st_ino}},'journal_roots':fake,'backup_root':{'path':str(backup),'identity':{'dev':backup.stat().st_dev,'ino':backup.stat().st_ino}},'original_datasets':datasets,'active_datasets':[],'ssd_trees':trees,'coverage':{'cache':['cache'],'corpus':['corpus'],'postgres':['postgres',datasets[4]['name']],'config':[datasets[0]['name']],'assets':[datasets[1]['name']],'trust':[datasets[2]['name']],'wal':[datasets[3]['name']]},'kubernetes':{'synthetic_fixture':True},'postgres':{'synthetic_fixture':True},'linux':{'synthetic_fixture':True},'provider_hold':{'generation':str(uuid.uuid4()),'owner_operation_id':str(uuid.uuid4()),'clean':False,'recovery_hold':True,'accounting_sha256':'e'*64},'source_graph':graph}
        inventory.validate_inventory(inv);op=str(uuid.uuid4());raw=inventory.canonical(inv);ip=put(self.root/'inventory.json',raw);ih=inventory.digest(raw)
        authority={'schema_version':1,'kind':'openlegal_storage_stability_authority','approved':True,'inventory_sha256':ih,'source_graph_sha256':inventory.digest(inventory.canonical(graph)),'managed_epoch':inv['managed_epoch'],'actions':['checkpoint'],'operation_id':op,'issued_at':time.time()-1,'valid_until':time.time()+3600,'daily_checkpoint_permitted':False,'release_owned_clean_hold_permitted':False,'collection_resume_permitted':False,'preserve_originals':True,'automatic_failover':False,'provider_recovery_permitted':False,'credential_provisioning_permitted':False}
        ar=inventory.canonical(authority);ap=put(self.root/'authority.json',ar)
        return inv,authority,ip,ih,ap,inventory.digest(ar),op,actors,device_mock
    def fake_backend(self,fail=None,overrides=None):
        calls=[]
        class Backend:
            def close(self):pass  # Synthetic fixture owns no retained FDs.
            def __getattr__(self,name):
                def phase(op):
                    calls.append(name)
                    if name==fail:raise interface.Refused('synthetic_backend_failed')
                    facts={k:True for k in interface.REQUIRED_TRUE[name]};facts['synthetic_fixture']=True
                    if name in interface.HOLD_PHASES:facts.update(provider_hold_before=provider_fixture(op.inventory['provider_hold'])[0],provider_hold_after=provider_fixture(op.inventory['provider_hold'])[0],hold_created_by_operation=False,collectors_resumed=False)
                    if name in('export_snapshots','verify_isolated_restore'):facts['parent_chains']=[]
                    if name=='verify_isolated_restore':facts['export_receipt_sha256']=inventory.digest(inventory.canonical(op.previous['export_snapshots']))
                    if name=='snapshot_same_pool':facts.update(snapshots=[{'dataset':d['name'],'guid':d['guid'],'snapshot':d['name']+'@openlegal-checkpoint-synthetic-'+op.operation_id.replace('-','')[:8],'snapshot_guid':str(n+100)}for n,d in enumerate(inventory.all_datasets(op.inventory))],index_sha256='f'*64,latest_wal_lsn='1/ABC')
                    if overrides and name in overrides:facts.update(overrides[name])
                    artifacts={}
                    if name in interface.HOLD_PHASES:
                        for which in ('before','after'):
                            key='provider_state_'+which+'_artifact';filename='synthetic-provider-'+which+'.json';facts[key]=filename
                            projection=facts['provider_hold_'+which];state=provider_fixture(projection)[1]
                            artifacts[filename]=op.journal.save(filename,observation(op.operation_id,name,which,projection,state))
                    if name in('stop','drain_writers'):
                        filename='synthetic-closure.json';facts['provider_settlement_closure_artifact']=filename;artifacts[filename]=op.journal.save(filename,closure_fixture(op.operation_id,name,artifacts['synthetic-provider-after.json']))
                    artifact='synthetic-backend.json';pin=op.journal.save(artifact,{'synthetic_fixture':True,'phase':name,'NO_HOST_COMMANDS':True});artifacts[artifact]=pin
                    return {'schema_version':1,'kind':interface.RECEIPT_KIND,'operation_id':op.operation_id,'action':op.action,'phase':name,'inventory_sha256':op.inventory_sha256,'source_graph_sha256':op.source_graph_sha256,'authority_sha256':op.authority_sha256,'status':'passed','actual_exit_code':0,'owned_processes_closed':True,'facts':facts,'artifacts':artifacts}
                return phase
        import types
        modules={'interface':interface,'inventory':inventory,'journal':journal,'copy_tree':copy_tree,'retention':retention,'linux_backend':types.SimpleNamespace(LinuxBackend=Backend)}
        return modules,calls
    def test_full_synthetic_workflow_real_copies_dual_journals_and_verified_chain(self):
        inv,a,ip,ih,ap,ah,op,base,mock=self.prepared();mods,calls=self.fake_backend()
        with mock:result=checkpoint.execute(mods,inv,a,ip,ih,ap,ah,op,'checkpoint',base)
        self.assertEqual(result['phase_count'],10);self.assertEqual(calls,['qualify','stop','drain_writers','stop_postgres','snapshot_same_pool','resume_owned_hold','export_snapshots','verify_isolated_restore'])
        chain=Path(inv['backup_root']['path'])/'storage-stability-chains'/op
        self.assertTrue((chain/'verified.json').exists());self.assertEqual((chain/'ssd/corpus/bytes').read_bytes(),b'synthetic stopped tree')
    def test_failure_never_resumes_copies_or_adopts_old_failed_operation(self):
        inv,a,ip,ih,ap,ah,op,base,mock=self.prepared();mods,calls=self.fake_backend('stop_postgres')
        with mock:
            with self.assertRaises(interface.Refused):checkpoint.execute(mods,inv,a,ip,ih,ap,ah,op,'checkpoint',base)
            with self.assertRaises(FileExistsError):checkpoint.execute(mods,inv,a,ip,ih,ap,ah,op,'checkpoint',base)
        self.assertNotIn('resume_owned_hold',calls);self.assertFalse((Path(inv['backup_root']['path'])/'storage-stability-chains'/op/'ssd').exists())
        for r in inv['journal_roots']:self.assertTrue((Path(r['path'])/op/'stop_postgres/failed.json').exists())
    def test_uncertain_foreign_hold_can_only_stay_unchanged(self):
        inv,a,ip,ih,ap,ah,op,base,mock=self.prepared();after=dict(inv['provider_hold'],recovery_hold=False)
        mods,calls=self.fake_backend(overrides={'stop':{'provider_hold_after':after}})
        with mock,self.assertRaises(interface.Refused):checkpoint.execute(mods,inv,a,ip,ih,ap,ah,op,'checkpoint',base)
        self.assertNotIn('drain_writers',calls)
    def test_mutated_source_after_qualification_blocks_stop(self):
        inv,a,ip,ih,ap,ah,op,base,mock=self.prepared();mods,calls=self.fake_backend();real=checkpoint.validate_receipt
        def change(module,operation,receipt):
            out=real(module,operation,receipt)
            if operation.phase=='qualify':put(base/'copy_tree.py',b'synthetic changed source')
            return out
        with mock,patch.object(checkpoint,'validate_receipt',side_effect=change),self.assertRaises(interface.Refused):checkpoint.execute(mods,inv,a,ip,ih,ap,ah,op,'checkpoint',base)
        self.assertEqual(calls,['qualify'])
    def test_authority_cannot_enable_automatic_failover_or_other_inventory(self):
        inv,a,ip,ih,ap,ah,op,base,mock=self.prepared()
        for change in ({'inventory_sha256':'0'*64},{'automatic_failover':True},{'provider_recovery_permitted':True},{'operation_id':str(uuid.uuid4())}):
            with self.subTest(change=change),self.assertRaises(interface.Refused):inventory.validate_authority(dict(a,**change),inv,ih,'checkpoint',op)
    def test_component_coverage_seven_datasets_and_independent_media_required(self):
        inv,*_=self.prepared()
        variants=[dict(inv,original_datasets=inv['original_datasets'][:-1]),dict(inv,coverage={**inv['coverage'],'trust':[]}),dict(inv,journal_roots=[inv['journal_roots'][0]]*2)]
        for bad in variants:
            with self.assertRaises(interface.Refused):inventory.validate_inventory(bad)

    def test_rebound_backup_root_refuses_before_next_backend_phase(self):
        inv,a,ip,ih,ap,ah,op,base,mock=self.prepared();mods,calls=self.fake_backend();real=journal.new_file;rebound=[]
        def replace(path,raw):
            real(path,raw)
            if Path(path).name=='chain.json'and not rebound:
                root=Path(inv['backup_root']['path']);moved=root.with_name('original-backup');root.rename(moved);shutil.copytree(moved,root);rebound.append(True)
        with mock,patch.object(journal,'new_file',side_effect=replace),self.assertRaises(interface.Refused):checkpoint.execute(mods,inv,a,ip,ih,ap,ah,op,'checkpoint',base)
        self.assertEqual(calls,['qualify']);self.assertTrue(rebound)

class OperationAuthorityTests(unittest.TestCase):
    def test_authority_copy_is_deeply_immutable_and_detached(self):
        source={'collection_resume_permitted':False,'release_owned_clean_hold_permitted':False,'actions':['checkpoint'],'nested':{'proof':['original']}}
        frozen=interface.freeze_authority(copy.deepcopy(source));source['collection_resume_permitted']=True;source['nested']['proof'].append('changed')
        self.assertFalse(frozen['collection_resume_permitted']);self.assertEqual(frozen['nested']['proof'],('original',))
        with self.assertRaises(TypeError):frozen['collection_resume_permitted']=True
        with self.assertRaises(TypeError):frozen['nested']['proof']=('changed',)
    def test_missing_or_false_permission_refuses_before_backend_effect(self):
        for authority in ({},{'release_owned_clean_hold_permitted':False,'collection_resume_permitted':False}):
            op=interface.Operation('synthetic','checkpoint','resume_owned_hold',{},'a'*64,'b'*64,'c'*64,None,Path('/tmp/synthetic-never-created'),{},lambda:None,interface.freeze_authority(authority));effects=[]
            def effect():
                interface.need(op.authority.get('release_owned_clean_hold_permitted',False)is True,'release_not_authorized');effects.append('must_not_run')
            with self.assertRaises(interface.Refused):effect()
            self.assertEqual(effects,[])
        default=interface.Operation('synthetic','checkpoint','resume_owned_hold',{},'a'*64,'b'*64,'c'*64,None,Path('/tmp/synthetic-never-created'),{},lambda:None)
        self.assertFalse(default.authority.get('collection_resume_permitted',False))

class ManualEpochContractTests(unittest.TestCase):
    def test_promotion_accepts_only_intent_with_writers_inhibited_and_reseed_needs_real_binding(self):
        class J:
            def output_hashes(self):return {'synthetic.json':'f'*64}
        for phase,required in(('promote',('new_epoch_intent_bound','application_writers_inhibited')),('reseed',('new_epoch_bound',))):
            op=interface.Operation('synthetic','manual_failover',phase,{},'a'*64,'b'*64,'c'*64,J(),Path('/tmp/never-created'),{},lambda:None)
            facts={key:True for key in interface.REQUIRED_TRUE[phase]}
            receipt={'schema_version':1,'kind':interface.RECEIPT_KIND,'operation_id':'synthetic','action':'manual_failover','phase':phase,'inventory_sha256':'a'*64,'source_graph_sha256':'b'*64,'authority_sha256':'c'*64,'status':'passed','actual_exit_code':0,'owned_processes_closed':True,'facts':facts,'artifacts':{'synthetic.json':'f'*64}}
            checkpoint.validate_receipt({'interface':interface,'inventory':inventory},op,receipt)
            for key in required:
                bad=copy.deepcopy(receipt);bad['facts'].pop(key)
                with self.subTest(phase=phase,key=key),self.assertRaises(interface.Refused):checkpoint.validate_receipt({'interface':interface,'inventory':inventory},op,bad)
        self.assertNotIn('new_epoch_bound',interface.REQUIRED_TRUE['promote'])

class HoldChainTests(unittest.TestCase):
    def setUp(self):
        import types
        self.opid=str(uuid.uuid4());self.modules={'interface':interface,'inventory':inventory}
        self.base={'generation':str(uuid.uuid4()),'owner_operation_id':None,'clean':True,'recovery_hold':False,'accounting_sha256':'a'*64}
        self.authority={'release_owned_clean_hold_permitted':True,'collection_resume_permitted':False}
        self.previous={};self.raws={};self.states={};self.action='checkpoint'
        class Journal:
            def read_phase_bytes(inner,phase,name,pin):
                raw=self.raws[(phase,name)];interface.need(inventory.digest(raw)==pin,'synthetic_journal_hash');return raw
        self.journal=Journal()
        self.op=lambda phase:types.SimpleNamespace(phase=phase,operation_id=self.opid,action=self.action,previous=dict(self.previous),journal=self.journal,inventory={'provider_hold':dict(self.base,accounting_sha256='f'*64)})
    def receipt(self,phase,before,after,created=False,readback=None):
        facts={'provider_hold_before':copy.deepcopy(before),'provider_hold_after':copy.deepcopy(after),'hold_created_by_operation':created,'collectors_resumed':False,'provider_state_before_artifact':'state-before.json','provider_state_after_artifact':'state-after.json'}
        artifacts={}
        for which,projection in (('before',before),('after',after)):
            state=self.states.get(projection['accounting_sha256'],provider_fixture(projection)[1]);raw=inventory.canonical(observation(self.opid,phase,which,projection,state));name='state-'+which+'.json';self.raws[(phase,name)]=raw;artifacts[name]=inventory.digest(raw)
        if phase in('stop','drain_writers','fence_old'):
            name='closure.json';facts['provider_settlement_closure_artifact']=name;raw=inventory.canonical(closure_fixture(self.opid,phase,artifacts['state-after.json']));self.raws[(phase,name)]=raw;artifacts[name]=inventory.digest(raw)
        if readback is not None:
            facts.update(provider_checkpoint_readback=readback,provider_checkpoint_readback_artifact='checkpoint.json');artifacts['checkpoint.json']=inventory.digest(inventory.canonical(readback))
        return {'phase':phase,'facts':facts,'artifacts':artifacts}
    def accept(self,phase,receipt):
        checkpoint.check_hold(self.modules,self.op(phase),receipt,self.authority);self.previous[phase]=receipt
    def qualify(self):
        self.base,state=provider_fixture(self.base);self.states[self.base['accounting_sha256']]=state
        self.accept('qualify',self.receipt('qualify',self.base,self.base))
    def install(self):
        self.qualify();self.accept('stop',self.receipt('stop',self.base,self.base))
        held=dict(self.base,generation=str(uuid.uuid4()),owner_operation_id=self.opid,recovery_hold=True)
        hold={'operation_id':self.opid,'intent_sha256':'c'*64,'owned':True,'original_generation':self.base['generation'],'installed_generation':held['generation'],'provider_recovery_hold':True,'released':False}
        rb={'hold':hold,'release':None,'created_at':'synthetic timestamp','released_at':None}
        self.accept('drain_writers',self.receipt('drain_writers',self.base,held,True,rb));return held,rb
    def test_fresh_daily_baseline_ignores_initial_inventory_counts(self):
        self.qualify();self.accept('stop',self.receipt('stop',self.base,self.base))
    def test_own_clean_hold_install_and_release_changes_generation_twice(self):
        held,rb=self.install();released=dict(self.base,generation=str(uuid.uuid4()))
        rb=dict(rb,release=dict(rb['hold'],released=True,provider_recovery_hold=False,released_generation=released['generation']),released_at='synthetic later')
        self.accept('resume_owned_hold',self.receipt('resume_owned_hold',held,released,True,rb))
    def test_foreign_uncertain_hold_preserved_but_never_released(self):
        self.base.update(clean=False,recovery_hold=True,owner_operation_id=str(uuid.uuid4()))
        self.qualify();self.accept('stop',self.receipt('stop',self.base,self.base))
        with self.assertRaises(interface.Refused):self.accept('drain_writers',self.receipt('drain_writers',self.base,dict(self.base,clean=True,recovery_hold=False)))
    def test_chain_drift_and_changed_accounting_refused(self):
        self.qualify()
        for before,after in ((dict(self.base,generation=str(uuid.uuid4())),self.base),(self.base,dict(self.base,accounting_sha256='b'*64))):
            with self.subTest(before=before,after=after),self.assertRaises(interface.Refused):self.accept('stop',self.receipt('stop',before,after))
    def test_fake_ownership_and_missing_raw_readback_refused(self):
        self.qualify()
        with self.assertRaises(interface.Refused):self.accept('stop',self.receipt('stop',self.base,self.base,True))
        after=dict(self.base,generation=str(uuid.uuid4()),owner_operation_id=self.opid,recovery_hold=True)
        with self.assertRaises(interface.Refused):self.accept('drain_writers',self.receipt('drain_writers',self.base,after,True))
    def test_release_wrong_scope_generation_or_readback_hash_refused(self):
        held,rb=self.install();released=dict(self.base,generation=str(uuid.uuid4()))
        rb=dict(rb,release=dict(rb['hold'],released=True,provider_recovery_hold=False,released_generation=released['generation']),released_at='synthetic later')
        valid=self.receipt('resume_owned_hold',held,released,True,rb)
        for case in ('authority','generation','hash','collector'):
            bad=copy.deepcopy(valid);authority=dict(self.authority)
            if case=='authority':authority['release_owned_clean_hold_permitted']=False
            elif case=='generation':bad['facts']['provider_hold_after']['generation']=held['generation']
            elif case=='hash':bad['artifacts']['checkpoint.json']='0'*64
            else:bad['facts']['collectors_resumed']=True
            with self.subTest(case=case),self.assertRaises(interface.Refused):checkpoint.check_hold(self.modules,self.op('resume_owned_hold'),bad,authority)

    def test_monotonic_active_settlement_requires_actual_closed_clean_proof(self):
        self.base,state=provider_fixture(self.base);owner=str(uuid.uuid4());state['admissions']=[{'owner':owner,'slot':1}]
        self.base['accounting_sha256']=inventory.digest(inventory.canonical(state));self.states[self.base['accounting_sha256']]=state
        self.accept('qualify',self.receipt('qualify',self.base,self.base))
        after_state=copy.deepcopy(state);after_state['admissions']=[];after_state['budget']['daily_used']+=1
        after=dict(self.base,accounting_sha256=inventory.digest(inventory.canonical(after_state)));self.states[after['accounting_sha256']]=after_state
        receipt=self.receipt('stop',self.base,after)
        receipt['facts'].pop('provider_settlement_closure_artifact')
        with self.assertRaises(interface.Refused):self.accept('stop',receipt)
        closure=closure_fixture(self.opid,'stop',receipt['artifacts']['state-after.json'])
        raw=inventory.canonical(closure);self.raws[('stop','closure.json')]=raw;receipt['artifacts']['closure.json']=inventory.digest(raw);receipt['facts']['provider_settlement_closure_artifact']='closure.json'
        self.accept('stop',receipt)
        self.previous.pop('stop');closure['runtime_guards'][0]['state']='active_unclean';raw=inventory.canonical(closure);self.raws[('stop','closure.json')]=raw;receipt['artifacts']['closure.json']=inventory.digest(raw)
        with self.assertRaises(interface.Refused):self.accept('stop',receipt)

    def test_raw_counter_underflow_rollover_policy_and_unknown_clear_refused(self):
        self.qualify();original=self.states[self.base['accounting_sha256']]
        for case in('counter','rollover','policy','canary'):
            state=copy.deepcopy(original)
            if case=='counter':state['budget']['daily_used']-=1
            elif case=='rollover':state['budget']['utc_day']+=1
            elif case=='policy':state['budget']['operator_suspended']=True
            else:state['canary_control']={'required_id':str(uuid.uuid4())}
            after=dict(self.base,accounting_sha256=inventory.digest(inventory.canonical(state)));self.states[after['accounting_sha256']]=state
            with self.subTest(case=case),self.assertRaises(interface.Refused):self.accept('stop',self.receipt('stop',self.base,after))
    def test_intermediate_new_active_admission_settles_only_at_final_stopped_state(self):
        self.qualify();state=copy.deepcopy(self.states[self.base['accounting_sha256']]);state['budget']['daily_used']+=1;state['admissions']=[{'owner':str(uuid.uuid4()),'slot':2}]
        before=dict(self.base,accounting_sha256=inventory.digest(inventory.canonical(state)));self.states[before['accounting_sha256']]=state
        end=copy.deepcopy(state);end['admissions']=[];after=dict(before,accounting_sha256=inventory.digest(inventory.canonical(end)));self.states[after['accounting_sha256']]=end
        self.accept('stop',self.receipt('stop',before,after))
    def test_unlocked_admission_cannot_disappear_even_with_clean_process_proof(self):
        self.base,state=provider_fixture(self.base);state['admissions']=[{'owner':str(uuid.uuid4()),'slot':1}];self.base['accounting_sha256']=inventory.digest(inventory.canonical(state));self.base['clean']=False;self.states[self.base['accounting_sha256']]=state
        r=self.receipt('qualify',self.base,self.base)
        for which in('before','after'):
            name='state-'+which+'.json';v=inventory.decode(self.raws[('qualify',name)]);v['admission_ownership']['slots'][0]['locked']=False;raw=inventory.canonical(v);self.raws[('qualify',name)]=raw;r['artifacts'][name]=inventory.digest(raw)
        self.accept('qualify',r)
        end=copy.deepcopy(state);end['admissions']=[];after=dict(self.base,clean=True,accounting_sha256=inventory.digest(inventory.canonical(end)));self.states[after['accounting_sha256']]=end
        receipt=self.receipt('stop',self.base,after);v=inventory.decode(self.raws[('stop','state-before.json')]);v['admission_ownership']['slots'][0]['locked']=False;raw=inventory.canonical(v);self.raws[('stop','state-before.json')]=raw;receipt['artifacts']['state-before.json']=inventory.digest(raw)
        with self.assertRaises(interface.Refused):self.accept('stop',receipt)

    def test_unledgered_legacy_lock_cannot_be_declared_clean(self):
        self.qualify();r=self.receipt('stop',self.base,self.base)
        name='state-before.json';value=inventory.decode(self.raws[('stop',name)]);value['admission_ownership']['legacy_locked']=True;raw=inventory.canonical(value);self.raws[('stop',name)]=raw;r['artifacts'][name]=inventory.digest(raw)
        with self.assertRaises(interface.Refused):self.accept('stop',r)

    def test_preserved_hold_cannot_change_unhashed_control_stamp(self):
        self.base.update(clean=False,recovery_hold=True,owner_operation_id=str(uuid.uuid4()));self.qualify();r=self.receipt('stop',self.base,self.base)
        name='state-after.json';value=inventory.decode(self.raws[('stop',name)]);value['hold_control']['hold_started_at']=99;raw=inventory.canonical(value);self.raws[('stop',name)]=raw;r['artifacts'][name]=inventory.digest(raw)
        closure=closure_fixture(self.opid,'stop',r['artifacts'][name]);raw=inventory.canonical(closure);self.raws[('stop','closure.json')]=raw;r['artifacts']['closure.json']=inventory.digest(raw)
        with self.assertRaises(interface.Refused):self.accept('stop',r)

    def test_only_explicit_manual_fence_can_install_owned_hold_then_release(self):
        self.action='manual_failover';self.qualify();held=dict(self.base,generation=str(uuid.uuid4()),owner_operation_id=self.opid,recovery_hold=True)
        hold={'operation_id':self.opid,'intent_sha256':'c'*64,'owned':True,'original_generation':self.base['generation'],'installed_generation':held['generation'],'provider_recovery_hold':True,'released':False}
        rb={'hold':hold,'release':None,'created_at':'synthetic timestamp','released_at':None}
        self.accept('fence_old',self.receipt('fence_old',self.base,held,True,rb))
        after=dict(self.base,generation=str(uuid.uuid4()));rb=dict(rb,release=dict(hold,released=True,provider_recovery_hold=False,released_generation=after['generation']),released_at='synthetic later')
        self.accept('strict_resume',self.receipt('strict_resume',held,after,True,rb))
    def test_fence_hold_never_enters_daily_automatic_workflow(self):
        self.qualify();r=self.receipt('fence_old',self.base,self.base)
        with self.assertRaises(interface.Refused):self.accept('fence_old',r)
        self.assertNotIn('fence_old',interface.CHECKPOINT_PHASES)

if __name__=='__main__':unittest.main(verbosity=2)
