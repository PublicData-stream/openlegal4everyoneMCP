"""Pure synthetic SQL/command ports and real canonical-byte journal contracts.
No SQL, Docker, roles, credentials, provider, or host effects are executed.
"""
import copy
import importlib.util
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE))
import interface,inventory,checkpoint,provider_backend as p


class Journal:
    def __init__(self):self.phase='qualify';self.raw={}
    def save(self,name,value):
        key=(self.phase,name)
        if key in self.raw:raise FileExistsError(name)
        raw=inventory.canonical(value);self.raw[key]=raw;return inventory.digest(raw)
    def output_hashes(self):return {n:inventory.digest(raw)for (phase,n),raw in self.raw.items()if phase==self.phase}
    def read_phase_bytes(self,phase,name,pin):
        raw=self.raw[(phase,name)];interface.need(inventory.digest(raw)==pin,'synthetic_changed');return raw


def source():
    return {'state':{'budget':{'utc_day':20000,'daily_used':5,'on_demand_used':0,'pilot_used':0,'next_request_at_ms':10,'next_allowed_at':0,'unresolved_response':False,'admission_owner':None},'control':{'singleton':True},'admissions':[],'canary_control':None,'canaries':[]},'hold_control':{'singleton':True,'generation':str(uuid.uuid4()),'recovery_hold':False,'hold_started_at':None},'admission_ownership':{'legacy_locked':False,'slots':[]},'checkpoint_owner':None,'checkpoint_readback':None}


def wal():
    result={'managed_epoch':str(uuid.uuid4()),'blob_binding_sha256':'f'*64,'primary_system_identifier':'123456','timeline':1,'postmaster_started_at':'synthetic postmaster','standby_name':'synthetic_standby','standby_role':'synthetic_peer','standby_addr':'127.0.0.1/32','standby_client_dn':'CN=synthetic','standby_issuer_dn':'CN=syntheticCA','standby_client_serial':'1','sender_pid':123,'sender_started_at':'synthetic sender','prefix_lsn':'0/10','flush_lsn':'0/10'}
    initial=copy.deepcopy(result);before=copy.deepcopy(result);after=copy.deepcopy(result)
    before.update(prefix_lsn='0/20',flush_lsn='0/20');after.update(prefix_lsn='0/20',flush_lsn='0/20')
    return {'initial':initial,'before':before,'after':after}


class Backend:
    def __init__(self):self.value=source();self.sql=[];self.mutations=[];self.mutation_targets=[];self.mode=None;self._artifacts={}
    def _artifact(self,op,name,value):
        filename=op.phase+'-'+name+'.json';self._artifacts[filename]=op.journal.save(filename,value)
    def _sql(self,op,sql,*,container=None,timeout=None):
        self.sql.append((sql,container,timeout))
        if sql.startswith('SELECT openlegal_checkpoint.readback'):return copy.deepcopy(self.value['checkpoint_readback'])
        return copy.deepcopy(self.value)
    def _sql_mutation(self,op,sql,*,container=None):
        import re
        self.mutations.append(sql);self.mutation_targets.append(container)
        if self.mode=='unknown_ack':raise RuntimeError('synthetic actual transport unknown')
        if 'checkpoint.hold' in sql:
            intent=re.search("'([0-9a-f]{64})'",sql)[1]
            old=self.value['hold_control']['generation'];new=str(uuid.uuid4())
            hold={'operation_id':op.operation_id,'intent_sha256':intent,'owned':True,'original_generation':old,'installed_generation':new,'provider_recovery_hold':True,'released':False}
            self.value['hold_control'].update(generation=new,recovery_hold=True,hold_started_at=123)
            self.value.update(checkpoint_owner=op.operation_id,checkpoint_readback={'hold':hold,'release':None,'created_at':'synthetic time','released_at':None})
            result=hold
        else:
            hold=self.value['checkpoint_readback']['hold'];new=str(uuid.uuid4());result=dict(hold,released=True,provider_recovery_hold=False,released_generation=new)
            self.value['hold_control'].update(generation=new,recovery_hold=False,hold_started_at=None)
            self.value['checkpoint_owner']=None;self.value['checkpoint_readback'].update(release=result,released_at='synthetic later')
        if self.mode!='missing_ack':
            proof=wal()
            if self.mode=='wrong_sender':proof['after']['sender_pid']+=1
            if self.mode=='unflushed':proof['after']['flush_lsn']='0/1'
            self._artifact(op,'operator-mutation-ack-0001',proof)
        return copy.deepcopy(result)


def closure():
    return {'cri_containers':[{'id':'synthetic','state':'CONTAINER_EXITED'}],'cgroups':[{'path':'/synthetic/group','dev':1,'ino':2,'pids':[]}],'sql_sessions':[],'runtime_guard_directory':{'path':'/synthetic/guard','dev':1,'ino':3},'runtime_guards':[{'name':'serve.synthetic.json','sha256':'a'*64,'state':'closed_clean','locked':False}]}


class ProviderTests(unittest.TestCase):
    def setUp(self):
        self.journal=Journal();self.backend=Backend();self.opid=str(uuid.uuid4());self.previous={};self.action='checkpoint'
        self.authority={'approved':True,'actions':['checkpoint','manual_failover'],'release_owned_clean_hold_permitted':True,'collection_resume_permitted':False}
    def op(self,phase):
        self.journal.phase=phase
        return interface.Operation(self.opid,self.action,phase,{'linux':{'postgres':{'primary':{'container_id':'1'*64}}}},'a'*64,'b'*64,'c'*64,self.journal,Path('/tmp/synthetic-never-created'),dict(self.previous),lambda:None,interface.freeze_authority(copy.deepcopy(self.authority)))
    def accept(self,op,facts):
        receipt={'phase':op.phase,'facts':facts,'artifacts':self.journal.output_hashes()}
        checkpoint.check_hold({'interface':interface,'inventory':inventory},op,receipt,self.authority)
        self.previous[op.phase]=receipt
        return receipt
    def qualify(self):
        op=self.op('qualify');before=p.observe(self.backend,op,'before');after=p.observe(self.backend,op,'after',baseline=before);self.accept(op,p.phase_facts(self.backend,op,before,after))
    def installed(self):
        self.qualify();op=self.op('stop');before=p.observe(self.backend,op,'before');after=p.observe(self.backend,op,'after');self.accept(op,p.phase_facts(self.backend,op,before,after,closure=closure()))
        op=self.op('drain_writers');before=p.observe(self.backend,op,'before');mutation=p.hold(self.backend,op,before);after=p.observe(self.backend,op,'after');self.accept(op,p.phase_facts(self.backend,op,before,after,closure=closure(),mutation=mutation));return mutation
    def test_import_is_inert(self):
        with patch.object(Backend,'_sql',side_effect=AssertionError('no SQL')):
            spec=importlib.util.spec_from_file_location('synthetic_provider_import',BASE/'provider_backend.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    def test_single_actual_qualification_snapshot_is_truthfully_reused(self):
        op=self.op('qualify');before=p.observe(self.backend,op,'before');self.backend.value['state']['budget']['daily_used']+=1
        after=p.observe(self.backend,op,'after',baseline=before)
        self.assertEqual(len(self.backend.sql),1);self.assertEqual(before['projection'],after['projection'])
        self.assertIn('qualify-provider-single-snapshot.json',self.journal.output_hashes())
        self.accept(op,p.phase_facts(self.backend,op,before,after))
    def test_baseline_reuse_is_qualify_only_and_target_bound(self):
        op=self.op('stop');before=p.observe(self.backend,op,'before')
        with self.assertRaises(interface.Refused):p.observe(self.backend,op,'after',baseline=before)
        op=self.op('qualify');before=p.observe(self.backend,op,'before')
        with self.assertRaises(interface.Refused):p.observe(self.backend,op,'after',baseline=before,container='2'*64)
    def test_observation_uses_only_named_protected_api_and_backend_two_second_cap(self):
        op=self.op('qualify');p.observe(self.backend,op,'before');sql,_,timeout=self.backend.sql[0]
        self.assertEqual(timeout,2);self.assertEqual(sql,"SELECT openlegal_checkpoint.observe('"+self.opid+"'::uuid)");self.assertNotIn('FOR SHARE',sql);self.assertNotIn('DO ',sql);self.assertNotIn('provider_request_budget',sql);self.assertNotIn('slot_locked',sql)
        self.assertNotIn('provider_apply',sql);self.assertNotIn('UPDATE ',sql)
    def test_unknown_legacy_and_unlocked_slot_are_never_projected_clean(self):
        for kind in('legacy','slot'):
            self.setUp();value=self.backend.value
            if kind=='legacy':value['admission_ownership']['legacy_locked']=True
            else:
                owner=str(uuid.uuid4());value['state']['admissions']=[{'owner':owner,'slot':1}];value['admission_ownership']['slots']=[{'owner':owner,'slot':1,'locked':False}]
            op=self.op('drain_writers');before=p.observe(self.backend,op,'before');self.assertFalse(before['projection']['clean'])
            with self.assertRaises(interface.Refused):p.hold(self.backend,op,before)
            self.assertEqual(self.backend.mutations,[])
    def test_live_known_owner_is_clean_but_not_drained_for_hold(self):
        owner=str(uuid.uuid4());self.backend.value['state']['admissions']=[{'owner':owner,'slot':1}];self.backend.value['admission_ownership']['slots']=[{'owner':owner,'slot':1,'locked':True}]
        op=self.op('drain_writers');before=p.observe(self.backend,op,'before');self.assertTrue(before['projection']['clean'])
        with self.assertRaises(interface.Refused):p.hold(self.backend,op,before)
        self.assertFalse(self.backend.mutations)
    def test_foreign_uncertain_hold_is_preserved_without_new_mutation(self):
        self.backend.value['hold_control'].update(recovery_hold=True,hold_started_at=1);self.backend.value['state']['budget']['unresolved_response']=True;self.backend.value['checkpoint_owner']=str(uuid.uuid4())
        self.qualify();op=self.op('stop');before=p.observe(self.backend,op,'before');after=p.observe(self.backend,op,'after');self.accept(op,p.phase_facts(self.backend,op,before,after,closure=closure()))
        op=self.op('drain_writers');before=p.observe(self.backend,op,'before');mutation=p.hold(self.backend,op,before);after=p.observe(self.backend,op,'after');facts=p.phase_facts(self.backend,op,before,after,closure=closure(),mutation=mutation);self.accept(op,facts)
        self.assertFalse(facts['hold_created_by_operation']);self.assertEqual(self.backend.mutations,[])
    def test_checked_owned_hold_and_release_bind_actual_readback_and_wal(self):
        mutation=self.installed();self.assertTrue(mutation['created'])
        op=self.op('resume_owned_hold');before=p.observe(self.backend,op,'before');mutation=p.release(self.backend,op,before);after=p.observe(self.backend,op,'after');facts=p.phase_facts(self.backend,op,before,after,mutation=mutation);self.accept(op,facts)
        self.assertTrue(facts['hold_created_by_operation']);self.assertFalse(after['projection']['recovery_hold']);self.assertEqual(len(self.backend.mutations),2)
    def test_release_missing_permission_refuses_before_effect(self):
        self.installed();self.authority['release_owned_clean_hold_permitted']=False
        op=self.op('resume_owned_hold');before=p.observe(self.backend,op,'before');count=len(self.backend.mutations)
        with self.assertRaises(interface.Refused):p.release(self.backend,op,before)
        self.assertEqual(len(self.backend.mutations),count)
    def test_actual_unknown_ack_missing_ack_wrong_sender_and_unflushed_are_not_pass(self):
        for mode in('unknown_ack','missing_ack','wrong_sender','unflushed'):
            self.setUp();self.backend.mode=mode;op=self.op('drain_writers');before=p.observe(self.backend,op,'before')
            with self.subTest(mode=mode),self.assertRaises((interface.Refused,RuntimeError)):p.hold(self.backend,op,before)
            self.assertEqual(len(self.backend.mutations),1)
            with self.assertRaises(FileExistsError):p.hold(self.backend,op,before)
            self.assertEqual(len(self.backend.mutations),1)
    def test_tampered_observation_or_slot_map_refuses_before_sql_mutation(self):
        op=self.op('drain_writers');before=p.observe(self.backend,op,'before');before['projection']['accounting_sha256']='0'*64
        with self.assertRaises(interface.Refused):p.hold(self.backend,op,before)
        self.assertFalse(self.backend.mutations)
        self.setUp();self.backend.value['admission_ownership']['slots']=[{'owner':str(uuid.uuid4()),'slot':1,'locked':True}]
        with self.assertRaises(interface.Refused):p.observe(self.backend,self.op('qualify'),'before')
    def test_release_no_prior_success_or_changed_accounting_refuses(self):
        self.installed();self.backend.value['state']['budget']['daily_used']+=1
        op=self.op('resume_owned_hold');before=p.observe(self.backend,op,'before');count=len(self.backend.mutations)
        with self.assertRaises(interface.Refused):p.release(self.backend,op,before)
        self.assertEqual(len(self.backend.mutations),count)
        self.previous={};op=self.op('strict_resume')
        with self.assertRaises(interface.Refused):p.release(self.backend,op,before)
    def test_closure_and_collection_claims_cannot_be_fabricated(self):
        op=self.op('stop');before=p.observe(self.backend,op,'before');after=p.observe(self.backend,op,'after');bad=closure();bad['runtime_guards'][0]['state']='active_unclean'
        with self.assertRaises(interface.Refused):p.phase_facts(self.backend,op,before,after,closure=bad)
        with self.assertRaises(interface.Refused):p.phase_facts(self.backend,op,before,after,collectors_resumed=True)
    def test_each_stage_and_mutation_are_exclusive_not_replayed(self):
        op=self.op('drain_writers');before=p.observe(self.backend,op,'before')
        with self.assertRaises(FileExistsError):p.observe(self.backend,op,'before')
        p.hold(self.backend,op,before)
        with self.assertRaises(FileExistsError):p.hold(self.backend,op,before)
        self.assertEqual(len(self.backend.mutations),1)


    def test_raw_target_binding_refuses_opaque_cid_swap_before_mutation(self):
        op=self.op('drain_writers');before=p.observe(self.backend,op,'before');before['target_container']='2'*64
        with self.assertRaises(interface.Refused):p.hold(self.backend,op,before,container='2'*64)
        self.assertEqual(self.backend.mutations,[])
        self.assertEqual(len(self.backend.sql),1)
    def test_normal_observation_cannot_select_another_even_well_formed_container(self):
        op=self.op('qualify')
        with self.assertRaises(interface.Refused):p.observe(self.backend,op,'before',container='2'*64)
        self.assertEqual(self.backend.sql,[])
    def test_manual_promoted_target_requires_unchanged_verified_raw_reseed_receipt(self):
        self.action='manual_failover';op=self.op('reseed');cid='2'*64
        facts={k:True for k in interface.REQUIRED_TRUE['reseed']};facts.update(pair={'primary':{'container_id':cid}},promoted={'promoted_container_id':cid})
        receipt={'schema_version':1,'kind':interface.RECEIPT_KIND,'operation_id':self.opid,'action':self.action,'phase':'reseed','inventory_sha256':op.inventory_sha256,'source_graph_sha256':op.source_graph_sha256,'authority_sha256':op.authority_sha256,'status':'passed','actual_exit_code':0,'owned_processes_closed':True,'facts':facts,'artifacts':{'synthetic-proof.json':'f'*64}}
        self.journal.save('receipt.json',receipt);self.previous['reseed']=receipt
        op=self.op('strict_resume');before=p.observe(self.backend,op,'before')
        self.assertEqual(before['target_container'],cid);self.assertEqual(self.backend.sql[-1][1],cid)
        raw=inventory.decode(self.journal.read_phase_bytes(op.phase,before['query_artifact'],before['query_sha256']))
        self.assertEqual(raw['target_container'],cid);self.assertEqual(raw['result']['state'],self.backend.value['state'])
        with self.assertRaises(interface.Refused):p.observe(self.backend,op,'after',container='1'*64)
        self.previous['reseed']['facts']['pair']['primary']['container_id']='3'*64;self.previous['reseed']['facts']['promoted']['promoted_container_id']='3'*64
        op=self.op('strict_resume')
        with self.assertRaises(interface.Refused):p.observe(self.backend,op,'after',container='3'*64)

    def test_manual_default_release_dispatches_observation_and_mutation_to_verified_promoted_cid(self):
        self.action='manual_failover';self.qualify();op=self.op('fence_old');before=p.observe(self.backend,op,'before');mutation=p.hold(self.backend,op,before);after=p.observe(self.backend,op,'after');self.accept(op,p.phase_facts(self.backend,op,before,after,closure=closure(),mutation=mutation))
        op=self.op('reseed');cid='2'*64;facts={k:True for k in interface.REQUIRED_TRUE['reseed']};facts.update(pair={'primary':{'container_id':cid}},promoted={'promoted_container_id':cid})
        receipt={'schema_version':1,'kind':interface.RECEIPT_KIND,'operation_id':self.opid,'action':self.action,'phase':'reseed','inventory_sha256':op.inventory_sha256,'source_graph_sha256':op.source_graph_sha256,'authority_sha256':op.authority_sha256,'status':'passed','actual_exit_code':0,'owned_processes_closed':True,'facts':facts,'artifacts':{'synthetic-proof.json':'f'*64}}
        self.journal.save('receipt.json',receipt);self.previous['reseed']=receipt
        op=self.op('strict_resume');before=p.observe(self.backend,op,'before');self.assertEqual(self.backend.sql[-1][1],cid)
        p.release(self.backend,op,before)
        self.assertEqual(self.backend.mutation_targets[-1],cid);self.assertEqual(self.backend.sql[-1][1],cid)

if __name__=='__main__':unittest.main(verbosity=2)
