"""Synthetic command/SQL ports + real disposable /tmp files, never native acceptance.

Device independence is explicitly mocked ONLY for two journal fixture roots on
one /tmp filesystem. Blob bytes, metadata, hashes and both-file readback are real.
No Docker, PostgreSQL, network, provider or production operation is executed.
"""
import contextlib
import copy
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import journal
import preservation_capture as c
import preservation_backend as p
from interface import Refused
from linux_backend import LinuxBackend,SQL_SESSION_PREFIX


def uid():return str(uuid.uuid4())


def row(kind,key='a'*64,validated='1'):
    uuid_key='11111111-1111-4111-8111-111111111111'
    keys = {
        'corpus_capture_blob':[key,'0'],'corpus_revision_identity':[key,key],
        'provider_canary_audit':[uuid_key,uuid_key,'init']}
    if kind in ('cache_snapshot','cache_query_identity','current_inventory_audit',
                'provider_recovery_audit','checkpoint_operation','canary_lifecycle','provider_canary'):
        key=uuid_key
    fields = {
        'corpus_source_observation':{'validated_at':validated},
        'corpus_object_identity':{'version':'0','catalog_version':'0','next_capture':'1','validated_at':None},
        'corpus_revision_identity':{'last_sequence':'0','captured_at':None,'last_validated_at':None},
        'cache_query_identity':{'next_sequence':'1','mutation_revision':'0'}}
    return {'key':keys.get(kind,[key]),'sha256':'a'*64,'monotonic':fields.get(kind)}


class SyntheticBackend:
    _check_replica_observation=LinuxBackend._check_replica_observation
    _verify_references=staticmethod(LinuxBackend._verify_references)
    def __init__(self):
        self._artifacts={};self.sequence=0;self.reads=[];self.deadlines=[];self.extra=False
        self.mutation=False;self.positions=0;self.bad_owner=False;self.no_hold=False
        self.pages={kind:[row(kind)] for kind in p.KINDS};self.references=[]
    def _artifact(self,op,name,value):
        filename=op.phase+'-'+name+'.json'
        self._artifacts[filename]=op.journal.save(filename,value)
    def _record(self,op,sql,value):
        self.sequence+=1;raw=p.canonical(value)
        self._artifact(op,'command-%04d'%self.sequence,{
            'binary':'docker','actual_exit_code':0,'started':True,'owned_processes_closed':True,
            'expired':False,'output_limit_exceeded':False,'unexpected_descendants':False,'interrupted':False,
            'stdin_sha256':p.digest((SQL_SESSION_PREFIX+sql+';\n').encode()),
            'stdout_bytes':len(raw),'stdout_sha256':p.digest(raw),'stdout_hex':raw.hex()})
        return copy.deepcopy(value)
    def _sql(self,op,sql,**kwargs):
        self.reads.append(sql);self.deadlines.append((sql,kwargs.get('deadline')))
        if sql=='SELECT openlegal_checkpoint.client_sessions()':value={'sessions':[]}
        elif sql.startswith('SELECT openlegal_checkpoint.observe('):
            value={'state':{'budget':{'unresolved_response':False,'admission_owner':None},'control':{},
                    'admissions':[],'canary_control':None,'canaries':[]},
                'hold_control':{'generation':'22222222-2222-4222-8222-222222222222',
                    'recovery_hold':not self.no_hold,'hold_started_at':None},
                'admission_ownership':{'legacy_locked':False,'slots':[]},
                'checkpoint_owner':uid() if self.bad_owner else (op.operation_id if op.action=='canary' else None),
                'checkpoint_readback':None}
        elif sql=='SELECT openlegal_checkpoint.database_position()':
            value={'system_identifier':'1','timeline':1,'postmaster_started_at':'2026-11-02 00:00:00+00',
                'wal_lsn':'0/10','index_ack':0,'watermark':self.positions if self.mutation else 0,'in_recovery':False}
            self.positions+=1
        elif sql.startswith('SELECT openlegal_replication.'):
            value={**op.inventory['linux']['postgres']['binding'],
                'postmaster_started_at':'2026-11-02 00:00:00+00','sender_pid':100,
                'sender_started_at':'2026-11-02 00:00:00+00','prefix_lsn':'0/10','flush_lsn':'0/10'}
        elif sql.startswith('SELECT openlegal_checkpoint.preservation_page('):
            kind=re.search("preservation_page\\('([^']+)'",sql)[1]
            rows=copy.deepcopy(self.pages[kind])
            value={'schema_version':1,'kind':'openlegal_preservation_page','relation':kind,
                'rows':rows,'next_key':None,'has_more':False}
        elif sql.startswith('WITH source AS '):
            offset=int(re.search(r'OFFSET ([0-9]+)',sql)[1])
            value={'source_rows':len(self.references),'rows':self.references[offset:offset+128]}
        else:raise AssertionError('unexpected fixed synthetic SQL')
        return self._record(op,sql,value)
    def _closed_writers(self,op):
        self._sql(op,'SELECT openlegal_checkpoint.client_sessions()')
        return {'cri_containers':[],'cgroups':[],'sql_sessions':[],
                'runtime_guard_directory':{'synthetic_fixture':True},'runtime_guards':[]}


@unittest.skipUnless(os.geteuid()==0,'root metadata fixture gate required')
class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='preservation-capture-',dir='/tmp')
        self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name);self.root.chmod(0o700)
        real_roots=[]
        for name in ('ja','jb'):
            path=self.root/name;path.mkdir(mode=0o700);meta=path.stat()
            real_roots.append({'path':str(path),'identity':{'dev':meta.st_dev,'ino':meta.st_ino}})
        self.roots=copy.deepcopy(real_roots);self.roots[1]['identity']['dev']+=1
        real_directory=journal.directory;second=Path(self.roots[1]['path'])
        def directory(path,expected=None):
            if Path(path)==second:
                real_directory(path,real_roots[1]['identity'] if expected is not None else None)
                return copy.deepcopy(self.roots[1]['identity'])
            return real_directory(path,expected)
        real_fstat=os.fstat
        def fstat(fd):
            value=real_fstat(fd)
            if (value.st_dev,value.st_ino)==tuple(real_roots[1]['identity'].values()):
                fields={key:getattr(value,key) for key in dir(value) if key.startswith('st_')}
                fields['st_dev']=self.roots[1]['identity']['dev'];return SimpleNamespace(**fields)
            return value
        self.enterContext(patch.object(journal,'directory',side_effect=directory))
        self.enterContext(patch.object(c,'directory',side_effect=directory))
        self.enterContext(patch.object(os,'fstat',side_effect=fstat))
        # Commands are synthetic, so model their clock explicitly. The source
        # keeps the real shared2s deadline; fixture disk speed is not acceptance.
        self.clock=self.enterContext(patch.object(c.time,'monotonic',return_value=1.0))
        self.inventory={'managed_epoch':uid(),'journal_roots':self.roots,
            'source_graph':{'synthetic_capture_source.py':'a'*64},'linux':{'bound_roots':[],
                'postgres':{'primary':{'container_id':'d'*64},'binding':{}}}}
        self.inventory['linux']['postgres']['binding']={
            'managed_epoch':self.inventory['managed_epoch'],'blob_binding_sha256':'b'*64,
            'primary_system_identifier':'1','timeline':1,'standby_name':'peer','standby_role':'replica',
            'standby_addr':'192.0.2.2','standby_client_dn':'CN=replica','standby_issuer_dn':'CN=ca',
            'standby_client_serial':'1'}
        for role in c.ROLES:
            path=self.root/role;path.mkdir(mode=0o700);os.chown(path,10004,10004);meta=path.stat()
            self.inventory['linux']['bound_roots'].append({'role':role,'path':str(path),'uid':10004,
                                                        'dev':meta.st_dev,'ino':meta.st_ino})
        self.backend=SyntheticBackend();self.deadline=time.monotonic()+60
        for role in c.ROLES:self.generation(role,b'referenced',first=True)
        for role in c.ROLES:self.generation(role,b'unreferenced',first=False)
        for family in ('cache','corpus'):
            key=self.keys[(family+'_primary',b'referenced')]
            self.backend.references.append({'role':family,'storage_key':key,
                'sha256':p.digest(b'referenced'),'size':len(b'referenced')})
        self.baseline_op=self.operation(False)
    def generation(self,role,data,first):
        if not hasattr(self,'keys'):self.keys={}
        sha=p.digest(data);generation='11111111-1111-4111-8111-111111111111' if first else '22222222-2222-4222-8222-222222222222'
        key=sha[:2]+'/'+sha+'-'+generation;root=self.root/role;shard=root/sha[:2]
        if not shard.exists():shard.mkdir(mode=0o700);os.chown(shard,10004,10004)
        path=root/key;path.write_bytes(data);path.chmod(0o600);os.chown(path,10004,10004)
        self.keys[(role,data)]=key;return path
    def operation(self,final):
        operation=uid();phase='preservation_final' if final else 'preservation_baseline'
        j=journal.DualJournal(self.roots,operation,{'synthetic_fixture':True});j.begin(phase)
        return SimpleNamespace(operation_id=operation,phase=phase,action='canary' if final else 'strict_prepare',
            inventory=self.inventory,inventory_sha256='a'*64,
            source_graph_sha256=p.digest(p.canonical(self.inventory['source_graph'])),authority_sha256='c'*64,
            authority={('canary_final_preservation_permitted' if final else 'preservation_baseline_permitted'):True},
            journal=j,previous={},seal=lambda:None)
    def baseline(self):return c.capture_baseline(self.backend,self.baseline_op,deadline=self.deadline)
    def test_full_baseline_and_final_preserve_all15_relations_and_four_roots(self):
        baseline=self.baseline();before=copy.deepcopy(baseline)
        self.backend.pages['corpus_capture'].append(row('corpus_capture','b'*64))
        for role in c.ROLES:self.generation(role,b'new generation',first=True)
        final_op=self.operation(True)
        final=c.capture_final(self.backend,final_op,baseline['reference'],deadline=self.deadline)
        self.assertEqual(baseline,before)
        self.assertEqual(len(final['index']['relation_pages']),15)
        self.assertEqual({r['role'] for r in final['index']['physical_roots']},set(c.ROLES))
        result=p.decode(final_op.journal.read_phase_bytes(final_op.phase,
            final['comparison']['name'],final['comparison']['sha256']))
        self.assertTrue(result['all_baseline_generations_preserved'])
        self.assertFalse(result['public_acknowledgement_claimed'])
        self.assertEqual([r['references_verified'] for r in final['index']['physical_roots']],[1,1,1,1])
        replica_deadlines=[d for sql,d in self.backend.deadlines if sql.startswith('SELECT openlegal_replication.')]
        self.assertTrue(replica_deadlines);self.assertEqual(set(replica_deadlines),{3.0})
    def test_unreferenced_old_generation_removal_refuses_without_baseline_replacement(self):
        baseline=self.baseline();ref=copy.deepcopy(baseline['reference'])
        (self.root/'corpus_replica'/self.keys[('corpus_replica',b'unreferenced')]).unlink()
        final_op=self.operation(True)
        with self.assertRaisesRegex(Refused,'old_generation_missing'):
            c.capture_final(self.backend,final_op,ref,deadline=self.deadline)
        self.assertEqual(c.load_capture(self.backend,final_op,ref,deadline=self.deadline)['index'],baseline['index'])
        self.assertNotIn(final_op.phase+'-capture-subset-result.json',final_op.journal.output_hashes())
    def test_old_row_and_monotonic_regression_refuse(self):
        baseline=self.baseline();final_op=self.operation(True)
        self.backend.pages['corpus_source_observation'][0]['monotonic']['validated_at']='0'
        with self.assertRaisesRegex(Refused,'counter_regressed'):
            c.capture_final(self.backend,final_op,baseline['reference'],deadline=self.deadline)
    def test_second_journal_page_tampering_refuses(self):
        baseline=self.baseline();ref=baseline['index']['relation_pages'][0]
        path=Path(baseline['reference']['journal_phase_roots'][1]['path'])/ref['name']
        path.write_bytes(b'changed')
        with self.assertRaises(Refused):
            c.load_capture(self.backend,self.operation(True),baseline['reference'],deadline=self.deadline)
    def test_arbitrary_phase_path_or_phase_inode_refuses(self):
        baseline=self.baseline();bad=copy.deepcopy(baseline['reference'])
        bad['journal_phase_roots'][1]['path']=str(self.root/'arbitrary')
        with self.assertRaisesRegex(Refused,'not_derived'):
            c.load_capture(self.backend,self.operation(True),bad,deadline=self.deadline)
        bad=copy.deepcopy(baseline['reference']);bad['journal_phase_roots'][0]['identity']['ino']+=1
        with self.assertRaisesRegex(Refused,'rebound'):
            c.load_capture(self.backend,self.operation(True),bad,deadline=self.deadline)
    def test_root_private_file_modes_and_hardlinks_are_required(self):
        baseline=self.baseline();path=Path(baseline['reference']['journal_phase_roots'][1]['path'])/c.INDEX_NAME
        path.chmod(0o640)
        with self.assertRaises(Refused):
            c.load_capture(self.backend,self.operation(True),baseline['reference'],deadline=self.deadline)
        path.chmod(0o600);os.link(path,path.parent/'hardlink')
        with self.assertRaises(Refused):
            c.load_capture(self.backend,self.operation(True),baseline['reference'],deadline=self.deadline)
    def test_wrong_authority_and_expired_deadline_never_dispatch(self):
        self.baseline_op.authority={}
        with self.assertRaisesRegex(Refused,'authority'):self.baseline()
        self.assertEqual(self.backend.reads,[])
        self.baseline_op.authority={'preservation_baseline_permitted':True}
        with self.assertRaisesRegex(Refused,'deadline'):
            c.capture_baseline(self.backend,self.baseline_op,deadline=time.monotonic()-1)
        self.assertEqual(self.backend.reads,[])
    def test_foreign_final_hold_owner_refuses_before_scanning(self):
        baseline=self.baseline();self.backend.bad_owner=True;final_op=self.operation(True)
        with self.assertRaisesRegex(Refused,'held_clean_provider'):
            c.capture_final(self.backend,final_op,baseline['reference'],deadline=self.deadline)
        self.assertNotIn(c.INDEX_NAME,final_op.journal.output_hashes())
    def test_changed_watermark_during_full_scan_refuses_index(self):
        self.backend.mutation=True
        with self.assertRaisesRegex(Refused,'database_changed_during_scan'):self.baseline()
        self.assertNotIn(c.INDEX_NAME,self.baseline_op.journal.output_hashes())
    def test_missing_second_physical_reference_copy_refuses(self):
        (self.root/'cache_replica'/self.keys[('cache_replica',b'referenced')]).unlink()
        with self.assertRaises(FileNotFoundError):self.baseline()
        self.assertNotIn(c.INDEX_NAME,self.baseline_op.journal.output_hashes())
    def test_root_identity_replacement_refuses(self):
        self.inventory['linux']['bound_roots'][0]['ino']+=10000
        with self.assertRaisesRegex(Refused,'root_changed'):self.baseline()
    def test_global_rows_are_bounded_across_relations(self):
        with patch.object(p,'MAX_ROWS',14):
            with self.assertRaisesRegex(Refused,'global_row_bound'):self.baseline()
        self.assertNotIn(c.INDEX_NAME,self.baseline_op.journal.output_hashes())
    def test_second_media_index_failure_never_returns_success(self):
        original=journal.new_file
        def write(path,raw):
            if Path(path).name==c.INDEX_NAME and str(path).startswith(self.roots[1]['path']):
                raise OSError('synthetic_second_media_index_failure')
            return original(path,raw)
        with patch.object(journal,'new_file',side_effect=write):
            with self.assertRaisesRegex(OSError,'second_media_index_failure'):self.baseline()
        self.assertNotIn(c.INDEX_NAME,self.baseline_op.journal.artifacts)
    def test_missing_actual_closure_command_witness_refuses(self):
        closure={'cri_containers':[],'cgroups':[],'sql_sessions':[],
                 'runtime_guard_directory':{},'runtime_guards':[]}
        with patch.object(self.backend,'_closed_writers',return_value=closure):
            with self.assertRaisesRegex(Refused,'command_records'):self.baseline()
        self.assertNotIn(c.INDEX_NAME,self.baseline_op.journal.output_hashes())
    def test_reference_pagination_requires_real_terminal_page(self):
        sha='a'*64
        self.backend.references=[{'role':'cache','storage_key':'aa/'+sha+'-'+str(uuid.UUID(int=i+1)),
                                 'sha256':sha,'size':1} for i in range(128)]
        refs,rows,source_rows=c._references(self.backend,self.baseline_op,self.deadline)
        self.assertEqual(source_rows,128)
        self.assertEqual(len(rows),128)
        self.assertEqual([r['rows'] for r in refs],[128,0])
        self.assertEqual([r['offset'] for r in refs],[0,128])
    def test_reference_observer_overflow_cannot_hide_behind_deduplication(self):
        original=self.backend._sql
        def overflow(op,sql,**kwargs):
            if sql.startswith('WITH source AS '):
                return self.backend._record(op,sql,{'source_rows':200001,'rows':[]})
            return original(op,sql,**kwargs)
        with patch.object(self.backend,'_sql',side_effect=overflow):
            with self.assertRaisesRegex(Refused,'reference_source_bound'):self.baseline()
        self.assertNotIn(c.INDEX_NAME,self.baseline_op.journal.output_hashes())
    def test_stored_consumer_validates_stage_order_after_both_index_hashes_repin(self):
        baseline=self.baseline();index=copy.deepcopy(baseline['index']);reference=copy.deepcopy(baseline['reference'])
        index['stage_proofs']['before'],index['stage_proofs']['after']=index['stage_proofs']['after'],index['stage_proofs']['before']
        raw=p.canonical(index);reference['sha256']=p.digest(raw)
        for root in reference['journal_phase_roots']:(Path(root['path'])/c.INDEX_NAME).write_bytes(raw)
        with self.assertRaisesRegex(Refused,'stage_artifact_sequence'):
            c.load_capture(self.backend,self.operation(True),reference,deadline=self.deadline)
    def test_baseline_media_are_rechecked_after_final_physical_scan(self):
        baseline=self.baseline();final_op=self.operation(True);original=c.bootstrap.blob_inventory;count=0
        def scan(*args,**kwargs):
            nonlocal count
            result=original(*args,**kwargs);count+=1
            if count==4:
                root=baseline['reference']['journal_phase_roots'][1]
                name=baseline['index']['relation_pages'][0]['name']
                (Path(root['path'])/name).write_bytes(b'late original-media tamper')
            return result
        with patch.object(c.bootstrap,'blob_inventory',side_effect=scan):
            with self.assertRaises(Refused):
                c.capture_final(self.backend,final_op,baseline['reference'],deadline=self.deadline)
        self.assertNotIn(final_op.phase+'-capture-subset-result.json',final_op.journal.output_hashes())
    def test_baseline_index_ack_after_original_deadline_never_returns_success(self):
        original=self.baseline_op.journal.save
        def save(name,value):
            result=original(name,value)
            if name==c.INDEX_NAME:self.clock.return_value=self.deadline+1
            return result
        with patch.object(self.baseline_op.journal,'save',side_effect=save):
            with self.assertRaisesRegex(Refused,'capture_deadline'):self.baseline()
        # Immutable late candidate remains diagnostic; no external pointer is made.
        self.assertIn(c.INDEX_NAME,self.baseline_op.journal.artifacts)
    def test_row_comparison_elapsed_deadline_never_saves_subset_success(self):
        baseline=self.baseline();final_op=self.operation(True);original=p.compare_subset;calls=0
        def compare(*args):
            nonlocal calls
            result=original(*args);calls+=1;self.clock.return_value=self.deadline+1
            return result
        with patch.object(p,'compare_subset',side_effect=compare):
            with self.assertRaisesRegex(Refused,'capture_deadline'):
                c.capture_final(self.backend,final_op,baseline['reference'],deadline=self.deadline)
        self.assertEqual(calls,1)
        self.assertNotIn(final_op.phase+'-capture-subset-result.json',final_op.journal.output_hashes())
    def test_generation_comparison_elapsed_deadline_never_saves_subset_success(self):
        baseline=self.baseline();final_op=self.operation(True);original=p.compare_generations;calls=0
        for role in c.ROLES:self.generation(role,b'new generation',first=True)
        def compare(before,after):
            nonlocal calls
            result=original(before,after)
            if len(before)!=len(after):calls+=1;self.clock.return_value=self.deadline+1
            return result
        with patch.object(p,'compare_generations',side_effect=compare):
            with self.assertRaisesRegex(Refused,'capture_deadline'):
                c.capture_final(self.backend,final_op,baseline['reference'],deadline=self.deadline)
        self.assertEqual(calls,1)
        self.assertNotIn(final_op.phase+'-capture-subset-result.json',final_op.journal.output_hashes())
    def test_subset_result_ack_elapsed_deadline_never_returns_success(self):
        baseline=self.baseline();final_op=self.operation(True);original=final_op.journal.save
        name=final_op.phase+'-capture-subset-result.json'
        def save(filename,value):
            result=original(filename,value)
            if filename==name:self.clock.return_value=self.deadline+1
            return result
        with patch.object(final_op.journal,'save',side_effect=save):
            with self.assertRaisesRegex(Refused,'capture_deadline'):
                c.capture_final(self.backend,final_op,baseline['reference'],deadline=self.deadline)
        self.assertIn(name,final_op.journal.artifacts)
    def test_reference_hash_loop_rechecks_original_deadline_between_blocks(self):
        import hashlib
        data=b'x'*(1024*1024+1);self.generation('cache_primary',data,first=True)
        key=self.keys[('cache_primary',data)];sha=p.digest(data);original=hashlib.sha256;updates=[]
        test=self
        class Hash:
            def __init__(inner):inner.value=original()
            def update(inner,block):
                updates.append(len(block));inner.value.update(block);test.clock.return_value=test.deadline+1
            def hexdigest(inner):return inner.value.hexdigest()
        with patch('linux_backend.hashlib.sha256',side_effect=Hash):
            with self.assertRaisesRegex(Refused,'reference_deadline'):
                LinuxBackend._verify_references(self.root/'cache_primary',[
                    {'storage_key':key,'sha256':sha,'size':len(data)}],deadline=self.deadline)
        self.assertEqual(updates,[1024*1024])
    def test_source_graph_or_inventory_mismatch_refuses_old_index(self):
        baseline=self.baseline();final_op=self.operation(True);final_op.inventory_sha256='e'*64
        with self.assertRaisesRegex(Refused,'index_identity'):
            c.load_capture(self.backend,final_op,baseline['reference'],deadline=self.deadline)


if __name__=='__main__':unittest.main()
