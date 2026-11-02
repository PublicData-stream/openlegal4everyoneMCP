"""Synthetic lifecycle ports only: no DB, role, provider, cluster or real watch.
No result here is actual 21600-second or publication/acceptance evidence.
"""
import copy
import importlib.util
import os
import sys
import time
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE))
import interface,inventory,provider_backend,preservation_backend,preservation_capture
import canary_watch as w
from test_provider_backend import source,wal
spec=importlib.util.spec_from_file_location('canary_supervisor',BASE/'canary_supervisor.py')
s=importlib.util.module_from_spec(spec);spec.loader.exec_module(s)
NOW=1793577600.0


def uid():return str(uuid.uuid4())


class Journal:
    def __init__(self):self.phase='canary-watch';self.values={};self.phases=[]
    def begin(self,name):
        if name in self.phases:raise FileExistsError(name)
        self.phase=name;self.phases.append(name)
    def save(self,name,value):
        key=(self.phase,name)
        if key in self.values:raise FileExistsError(name)
        self.values[key]=copy.deepcopy(value);return inventory.digest(inventory.canonical(value))
    def output_hashes(self):return {n:inventory.digest(inventory.canonical(v))for(p,n),v in self.values.items()if p==self.phase}
    def read_phase_bytes(self,phase,name,pin):
        raw=inventory.canonical(self.values[(phase,name)]);assert inventory.digest(raw)==pin;return raw
    def check(self):pass


def prepared():
    op,canary,recovery,epoch=uid(),uid(),uid(),uid();held=uid();request=uid()
    policy={'allowed_active_guard_keys':['serve','collection.'+request+'.0'],
        'workload_pins':[{'kind':'Deployment','name':'server','uid':uid(),'role':'serve','runtime_image_id':'sha256:'+'e'*64}],
        'collection_job_pins':[{'name':'openlegal-request-'+request.replace('-','')+'-0','uid':uid(),'request_id':request,'epoch':0,
                              'spec_sha256':inventory.digest(inventory.canonical({'backoffLimit':0})),'runtime_image_id':'sha256:'+'e'*64}]}
    watch={'operation_id':op,'canary_id':canary,'recovery_operation_id':recovery,'canary_policy':policy,
           'storage_inventory':{'path':'/tmp/synthetic-storage.json','sha256':'c'*64}}
    value={'schema_version':1,'kind':'openlegal_canary_supervisor_inventory','operation_id':op,
        'watch_inventory':{'path':'/tmp/synthetic-watch.json','sha256':'d'*64},
        'watch_authority':{'path':'/tmp/synthetic-watch-authority.json','sha256':'e'*64},
        'watch_source_sha256':'f'*64,'expected_held_generation':held,'operator_login':'synthetic_canary'}
    intents=s.semantic_intents('1'*64,'2'*64,'d'*64,'e'*64)
    auth={'schema_version':1,'kind':'openlegal_canary_supervisor_authority','approved':True,'operation_id':op,
        'canary_id':canary,'recovery_operation_id':recovery,'inventory_sha256':'1'*64,'supervisor_source_sha256':'2'*64,
        'watch_source_sha256':'f'*64,'managed_epoch':epoch,'expected_held_generation':held,
        'activation_intent_sha256':s.digest(s.canonical(intents['activate'])),
        'final_hold_intent_sha256':s.digest(s.canonical(intents['final_hold_and_finish'])),
        'canary_activate_permitted':True,'canary_collection_jobs_activate_permitted':True,'canary_final_stop_permitted':True,
        'canary_final_hold_permitted':True,'canary_final_preservation_permitted':True,'canary_finish_permitted':True,'canary_held_lookup_resume_permitted':True,'duration_secs':21600,'max_global_requests':100,
        'automatic_retry':False,'credential_provisioning_permitted':False,'final_acceptance_permitted':False,'valid_until':NOW+30000}
    p=types.SimpleNamespace(value=value,watch=watch,authority=auth,storage={'managed_epoch':epoch,'source_graph':{},
        'linux':{'postgres':{'admin_user':'synthetic_canary','primary':{'container_id':'a'*64}},'blob_binding_sha256':'f'*64,'namespace':'openlegal-serving','serving_image':'synthetic@sha256:'+'e'*64,'serving_runtime_image_id':'sha256:'+'e'*64}},
        intents=intents,modules={'interface':interface,'inventory':inventory,'provider_backend':provider_backend,
            'preservation_backend':preservation_backend,'preservation_capture':preservation_capture},files={},watch_module=w,
        source_sha='2'*64,inventory_sha='1'*64,authority_sha='3'*64)
    baseline_op=uid()
    p.baseline_reference={'schema_version':1,'kind':'openlegal_preservation_reference',
        'operation_id':baseline_op,'phase':'preservation_baseline','name':'preservation-index.json','sha256':'b'*64,
        'journal_phase_roots':[{'path':'/tmp/synthetic-journal-'+str(i)+'/'+baseline_op+'/preservation_baseline',
            'identity':{'dev':i+1,'ino':10},'operation_identity':{'dev':i+1,'ino':11}} for i in range(2)]}
    p.watch['preservation_baseline']={'path':'/tmp/synthetic-preservation-reference.json',
        'sha256':s.digest(s.canonical(p.baseline_reference))}
    return p


def closure():return {'cri_containers':[{'id':'a'*64,'state':'CONTAINER_EXITED'}],
    'cgroups':[{'path':'/tmp/synthetic-cgroup','dev':1,'ino':1,'pids':[]}],'sql_sessions':[],
    'runtime_guard_directory':{'path':'/tmp/synthetic-guards','dev':1,'ino':2},
    'runtime_guards':[{'name':'synthetic','state':'closed_clean','locked':False}]}


class Fixture(unittest.TestCase):
    def setup_ports(self,mode=None):
        self.p=prepared();self.events=[];self.mode=mode;journal=Journal();test=self
        lock=types.SimpleNamespace(held=True,valid=True)
        def lockcheck():s.need(lock.held and lock.valid,'synthetic_invalid_lock')
        lock.check=lockcheck
        class Session:
            nonce=uid();pid=os.getpid();ticks=1;boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
            _ticks=staticmethod(lambda:1)
            binding={'authority_sha256':'e'*64}
            def __init__(inner):inner.journal=journal;inner.lock=lock;inner.timeline=types.SimpleNamespace(before=None);inner.failed=False
            def _seal(inner):s.need(lock.held and lock.valid and not inner.failed,'synthetic_session_failed')
            def before_activation(inner):inner.timeline.before={'wall_time':NOW};test.events.append('before')
            def after_activation(inner):test.events.append('after')
            def observe(inner):
                test.events.append('synthetic-observe')
                if mode=='watch_error':inner.failed=True;raise s.Refused('synthetic watch unknown')
                return {'status':'OBSERVATION_COMPLETE_PENDING_FINAL_GATES','elapsed_monotonic_seconds':21600,'consumed':1,'fixture_only':True}
        self.session=Session()
        class Backend:
            def __init__(inner):
                test.backend=inner;inner._sequence=0;inner.provider=source();inner.activation=None;inner.finish=None;inner.close_count=0;inner.stop_count=0;inner.mutations=[]
                inner.provider['hold_control'].update(generation=test.p.value['expected_held_generation'],recovery_hold=True)
            def _validate_linux(inner,value):pass
            def _artifact(inner,op,name,value):
                if mode=='job_intent_error'and name.startswith('canary-job-activation-intent-'):
                    raise OSError('synthetic second-media intent failure')
                op.journal.save(op.phase+'-'+name+'.json',value)
            def _sql(inner,op,sql,**kwargs):
                if sql==s.IDENTITY_SQL:return {'session_user':'synthetic_canary','current_user':'synthetic_canary','superuser':False,'create_role':False,'create_db':False,'replication':False,'bypass_rls':False,'login':True,'lifecycle_functions_installed':mode!='missing_hold_function','lifecycle_execute_permitted':mode!='missing_hold_execute'}
                if 'checkpoint.observe('in sql:return copy.deepcopy(inner.provider)
                if 'checkpoint.canary_readback('in sql:return copy.deepcopy({'activation':inner.activation,'finish':inner.finish})
                if 'checkpoint.readback('in sql:return copy.deepcopy(inner.provider['checkpoint_readback'])
                if 'checkpoint.canary_observation('in sql:
                    return {'required_id':test.p.watch['canary_id'],'canary':{'id':test.p.watch['canary_id'],
                        'operation_id':test.p.watch['recovery_operation_id'],'ended_at':inner.finish['closed_at'],'consumed':inner.finish['consumed']}}
                raise AssertionError('no arbitrary SQL')
            def _sql_mutation(inner,op,sql):
                inner.mutations.append((op.phase,sql));test.events.append(op.phase+'-mutation')
                if op.phase=='activate':
                    released=uid();inner.provider['hold_control'].update(recovery_hold=False,generation=released,hold_started_at=None)
                    inner.activation={'canary_id':test.p.watch['canary_id'],'recovery_operation_id':test.p.watch['recovery_operation_id'],
                        'intent_sha256':test.p.authority['activation_intent_sha256'],'managed_epoch':test.p.storage['managed_epoch'],
                        'blob_binding_sha256':'f'*64,'original_hold_generation':test.p.value['expected_held_generation'],
                        'released_generation':released,'started_at':'2026-11-02T00:00:00Z','deadline':'2026-11-02T06:00:00Z','consumed':0,'recovery_hold':False}
                    result=inner.activation
                    if mode in('unknown_ack','invalid_seal'):
                        if mode=='invalid_seal':lock.valid=False
                        raise RuntimeError('synthetic commit outcome unknown')
                elif op.phase=='final_hold':
                    old=inner.provider['hold_control']['generation'];new=uid()
                    result={'operation_id':op.operation_id,'intent_sha256':test.p.authority['final_hold_intent_sha256'],'owned':True,
                        'original_generation':old,'installed_generation':new,'provider_recovery_hold':True,'released':False}
                    inner.provider['hold_control'].update(generation=new,recovery_hold=True,hold_started_at=123)
                    inner.provider['checkpoint_owner']=op.operation_id
                    inner.provider['checkpoint_readback']={'hold':result,'release':None,'created_at':'synthetic held','released_at':None}
                elif op.phase=='finish':
                    test.assertEqual(test.supervisor.deadline,inner.original_final_deadline)
                    test.assertIn(('preservation_final','preservation_final-capture-subset-result.json'),journal.values)
                    result={'canary_id':test.p.watch['canary_id'],'recovery_operation_id':test.p.watch['recovery_operation_id'],
                        'final_checkpoint_operation':op.operation_id,'intent_sha256':test.p.authority['final_hold_intent_sha256'],
                        'managed_epoch':test.p.storage['managed_epoch'],'blob_binding_sha256':'f'*64,
                        'hold_generation':inner.provider['hold_control']['generation'],'started_at':inner.activation['started_at'],
                        'deadline':inner.activation['deadline'],'closed_at':'2026-11-02T06:00:01Z','consumed':1,
                        'recovery_hold':True,'physical_closure_and_publication_acceptance':'separate_required_evidence'}
                    inner.finish=result
                else:raise AssertionError('no other mutation')
                inner._sequence+=1;ack=wal()
                for row in ack.values():row['managed_epoch']=test.p.storage['managed_epoch']
                inner._artifact(op,'operator-mutation-ack-%04d'%inner._sequence,ack)
                return copy.deepcopy(result)
            def _kube(inner,op,args):
                pin=test.p.watch['canary_policy']['collection_job_pins'][0]
                item={'metadata':{'uid':pin['uid'],'resourceVersion':'1'},'spec':{'suspend':True,'backoffLimit':0},'status':{'active':0}}
                if args[2]=='patch':
                    intent_name='activate-canary-job-activation-intent-'+pin['uid']+'.json'
                    test.assertIn(('activate',intent_name),journal.values)
                    intent=journal.values[('activate',intent_name)]
                    test.assertEqual(intent['patch_sha256'],s.digest(args[args.index('-p')+1].encode()))
                    item['metadata']['resourceVersion']='2';item['spec']['suspend']=False;test.events.append('job-cas')
                    if mode=='job_unknown_ack':raise RuntimeError('synthetic Job patch ACK unknown')
                return item
            def _preservation_load_baseline(inner,op,reference,*,deadline):
                test.events.append('baseline-authentication')
                test.assertEqual(op.phase,'preflight');test.assertEqual(reference,test.p.baseline_reference)
                if mode=='baseline_error':raise s.Refused('synthetic old second-media hash mismatch')
                return {'index':{'phase':'preservation_baseline','operation_id':reference['operation_id']}}
            def _preservation_final(inner,op,reference,*,deadline):
                test.events.append('preservation-final')
                test.assertEqual(op.phase,'preservation_final');test.assertEqual(deadline,inner.original_final_deadline)
                test.assertEqual(reference,test.p.baseline_reference)
                test.assertTrue(inner.provider['hold_control']['recovery_hold'])
                test.assertEqual(inner.provider['checkpoint_owner'],op.operation_id)
                if mode=='preservation_error':raise s.Refused('synthetic original generation lost')
                identity={key:getattr(op,key) for key in ('operation_id','action','phase','inventory_sha256',
                    'source_graph_sha256','authority_sha256')}
                identity.update(managed_epoch=op.inventory['managed_epoch'],
                    target_container_id=op.inventory['linux']['postgres']['primary']['container_id'])
                roots=[dict(row,path=row['path'].replace(reference['operation_id'],op.operation_id).replace(
                    'preservation_baseline','preservation_final')) for row in reference['journal_phase_roots']]
                index={'schema_version':1,'kind':'openlegal_preservation_capture_index',**identity,'journal_phase_roots':roots,
                    'producer_os_exit_zero_claimed':False,'public_acknowledgement_claimed':False,
                    'checkpoint_claimed':False,'final_acceptance_claimed':False}
                index_sha=op.journal.save('preservation-index.json',index)
                final={'schema_version':1,'kind':'openlegal_preservation_reference','operation_id':op.operation_id,
                    'phase':op.phase,'name':'preservation-index.json','sha256':index_sha,'journal_phase_roots':roots}
                proof={'schema_version':1,'kind':'openlegal_preservation_subset_result',**identity,
                    'baseline':reference,'final':final,
                    'relations':[{'relation':kind,'baseline_rows':1,'final_rows':1,
                        'every_baseline_row_unchanged':True,'monotonic_fields_preserved':True} for kind in preservation_backend.KINDS],
                    'generations':{role:{'baseline_generations':1,'final_generations':1,
                        'all_baseline_generations_preserved':True} for role in preservation_capture.ROLES},
                    'every_baseline_row_unchanged':True,'all_baseline_generations_preserved':True,
                    'public_acknowledgement_claimed':False,'checkpoint_claimed':False,'final_acceptance_claimed':False}
                if mode=='preservation_incomplete_proof':proof['relations'].pop()
                if mode=='preservation_claims_public_ack':proof['public_acknowledgement_claimed']=True
                name=op.phase+'-capture-subset-result.json'
                if mode=='preservation_ack_error':raise OSError('synthetic second-media preservation ACK failure')
                sha=op.journal.save(name,proof)
                return {'index':index,'reference':final,'comparison':{'name':name,'sha256':sha}}
            def _canary_stop_and_drain(inner,op):
                lockcheck();inner.stop_count+=1;test.events.append('physical-stop')
                if op.phase=='final_stop':inner.original_final_deadline=test.supervisor.deadline
                if mode=='stop_error':raise RuntimeError('synthetic stop unknown')
                return closure()
            def _canary_restart_lookup_held(inner,op,*,canary_id):
                test.events.append('held-lookup')
                if mode=='lookup_unknown':raise RuntimeError('synthetic lookup start ACK unknown')
                return {'kind':'openlegal_canary_closed_lookup_resume','canary_id':canary_id,
                    'finish_readback':{'activation':inner.activation,'finish':inner.finish},
                    'provider_before':copy.deepcopy(inner.provider),'provider_after':copy.deepcopy(inner.provider),
                    'replica_barrier':wal(),'lookup':{'pods':[{'uid':uid(),'ready':True,'restarts':0,
                        'epoch':test.p.storage['managed_epoch'],'configured_image':test.p.storage['linux']['serving_image'],
                        'runtime_image_id':test.p.storage['linux']['serving_runtime_image_id']}]},
                    'collection_resumed':False,'final_acceptance':False,'remaining_gates':'external_checkpoint_preservation_public_acceptance'}
            def close(inner):
                inner.close_count+=1;test.events.append('backend-close');test.assertTrue(lock.held)
                if mode=='close_error':raise OSError('synthetic metadata FD failure')
        self.p.modules['linux_backend']=types.SimpleNamespace(LinuxBackend=Backend,SQL_SESSION_PREFIX='fixed ')
        self.supervisor=s.Supervisor(self.p,self.session)
        self.supervisor.seal=lambda:s.need(not self.supervisor.failed and lock.held and lock.valid and not self.session.failed,'synthetic source/authority/session seal')
        return self.supervisor


class SchemaTests(unittest.TestCase):
    def test_preflight_fixed_signatures_cover_every_required_lifecycle_port(self):
        expected={'openlegal_checkpoint.activate_canary(uuid,uuid,text,uuid)',
            'openlegal_checkpoint.finish_canary(uuid,text,uuid)','openlegal_checkpoint.canary_readback(uuid)',
            'openlegal_checkpoint.hold(uuid,text)','openlegal_checkpoint.readback(uuid)',
            'openlegal_checkpoint.observe(uuid)','openlegal_checkpoint.canary_observation(uuid)',
            'openlegal_checkpoint.preservation_page(text,jsonb,integer)',
            'openlegal_checkpoint.retained_references()','openlegal_checkpoint.database_position()',
            'openlegal_checkpoint.client_sessions()',
            'openlegal_replication.capture_prefix()','openlegal_replication.observe_flush()'}
        self.assertEqual(set(s.LIFECYCLE_FUNCTIONS),expected)
        for name in expected:
            self.assertIn("to_regprocedure('"+name+"') IS NOT NULL",s.IDENTITY_SQL)
            self.assertIn("has_function_privilege(current_user,to_regprocedure('"+name+"')::oid,'EXECUTE')",s.IDENTITY_SQL)
        self.assertIn("has_schema_privilege(current_user,'openlegal_checkpoint','USAGE')",s.IDENTITY_SQL)
    def test_exact_authority_and_non_circular_intents(self):
        p=prepared();s.validate_inputs(p.value,p.authority,p.watch,p.storage,inventory_sha='1'*64,source_sha='2'*64,now=NOW)
        for key,value in (('canary_activate_permitted',False),('canary_final_preservation_permitted',False),('max_global_requests',101),('duration_secs',21599),('final_acceptance_permitted',True),('credential_provisioning_permitted',True)):
            bad=dict(p.authority,**{key:value})
            with self.subTest(key=key),self.assertRaises(s.Refused):s.validate_inputs(p.value,bad,p.watch,p.storage,inventory_sha='1'*64,source_sha='2'*64,now=NOW)
    def test_no_superuser_or_login_override(self):
        safe={'session_user':'specific','current_user':'specific','superuser':False,'create_role':False,'create_db':False,'replication':False,'bypass_rls':False,'login':True,'lifecycle_functions_installed':True,'lifecycle_execute_permitted':True}
        s._login(safe,'specific')
        for changes in ({'superuser':True},{'current_user':'postgres'},{'login':False},{'replication':True},{'lifecycle_functions_installed':False},{'lifecycle_execute_permitted':False}):
            with self.assertRaises(s.Refused):s._login(dict(safe,**changes),'specific')
    def test_closure_is_actual_raw_data_not_generic_true(self):
        s._closure(closure(),provider_backend.CLOSURE_FIELDS)
        for change in('sql','guard','cri','cgroup'):
            bad=closure()
            if change=='sql':bad['sql_sessions']=[{'pid':1}]
            elif change=='guard':bad['runtime_guards'][0]['state']='active_unclean'
            elif change=='cri':bad['cri_containers'][0]['state']='CONTAINER_RUNNING'
            else:bad['cgroups']=[]
            with self.subTest(change=change),self.assertRaises(s.Refused):s._closure(bad,provider_backend.CLOSURE_FIELDS)


class LifecycleTests(Fixture):
    def run_fixed(self):
        def raw(path,expected):
            self.assertEqual(path,self.p.watch['preservation_baseline']['path'])
            self.assertEqual(expected,self.p.watch['preservation_baseline']['sha256'])
            return s.canonical(self.p.baseline_reference)
        with patch.object(s,'time',types.SimpleNamespace(time=lambda:NOW,monotonic=time.monotonic)),patch.object(s,'_raw',side_effect=raw):
            return self.supervisor.run()
    def test_fixed_order_keeps_hold_and_all_final_acceptance_gates_pending(self):
        self.setup_ports();result=self.run_fixed()
        self.assertEqual(self.events,['baseline-authentication','before','activate-mutation','job-cas','after','synthetic-observe','physical-stop','final_hold-mutation','preservation-final','finish-mutation','held-lookup','backend-close'])
        self.assertEqual(result['status'],'PENDING_FINAL_GATES');self.assertFalse(result['final_acceptance'])
        self.assertEqual(set(result['missing_final_executors']),set(s.PENDING)-{'fresh_full_old_preservation_subset'});self.assertEqual(self.backend.close_count,1)
        self.assertEqual(self.backend.stop_count,1);self.assertTrue(self.backend.provider['hold_control']['recovery_hold'])
        self.assertEqual(self.session.journal.phases,['preflight','activate','observation','final_stop','final_hold','preservation_final','finish','final_lookup'])
        with self.assertRaises(s.Refused):self.run_fixed()
    def test_missing_hold_function_or_execute_refuses_before_activation(self):
        for mode in ('missing_hold_function','missing_hold_execute'):
            with self.subTest(mode=mode):
                self.setup_ports(mode)
                with self.assertRaises(s.Refused):self.run_fixed()
                self.assertEqual(self.backend.mutations,[]);self.assertEqual(self.backend.stop_count,0)
                self.assertEqual(self.events,['backend-close'])
    def test_unreadable_original_baseline_refuses_before_activation(self):
        self.setup_ports('baseline_error')
        with self.assertRaisesRegex(s.Refused,'second-media hash mismatch'):self.run_fixed()
        self.assertEqual(self.backend.mutations,[]);self.assertEqual(self.backend.stop_count,0)
        self.assertEqual(self.events,['baseline-authentication','backend-close'])
    def test_final_preservation_failure_keeps_hold_drained_and_never_finishes_or_starts_lookup(self):
        for mode,error in (('preservation_error',s.Refused),('preservation_ack_error',OSError),
                           ('preservation_incomplete_proof',s.Refused),('preservation_claims_public_ack',s.Refused)):
            with self.subTest(mode=mode):
                self.setup_ports(mode)
                with self.assertRaises(error):self.run_fixed()
                self.assertEqual([phase for phase,_ in self.backend.mutations],['activate','final_hold'])
                self.assertEqual(self.backend.stop_count,1);self.assertTrue(self.backend.provider['hold_control']['recovery_hold'])
                self.assertIsNone(self.backend.finish);self.assertNotIn('held-lookup',self.events)
                self.assertEqual(self.supervisor.contain_once()['reason'],'stop_already_attempted_no_retry')
                self.assertIn(('preservation_final','supervisor-failed.json'),self.session.journal.values)
    def test_job_intent_failure_prevents_patch_and_attempts_stop_once(self):
        self.setup_ports('job_intent_error')
        with self.assertRaises(OSError):self.run_fixed()
        self.assertNotIn('job-cas',self.events);self.assertEqual(self.backend.stop_count,1)
        self.assertEqual([phase for phase,_ in self.backend.mutations],['activate'])
        self.assertFalse(any('job-activation-intent' in name for phase,name in self.session.journal.values))
    def test_job_unknown_ack_retains_exact_predispatch_intent_no_replay(self):
        self.setup_ports('job_unknown_ack')
        with self.assertRaises(RuntimeError):self.run_fixed()
        self.assertEqual(self.events.count('job-cas'),1);self.assertEqual(self.backend.stop_count,1)
        self.assertEqual([phase for phase,_ in self.backend.mutations],['activate'])
        pin=self.p.watch['canary_policy']['collection_job_pins'][0]
        intent=self.session.journal.values[('activate','activate-canary-job-activation-intent-'+pin['uid']+'.json')]
        self.assertEqual(intent['uid'],pin['uid']);self.assertEqual(intent['original_resource_version'],'1')
        self.assertEqual(intent['normalized_spec_sha256'],pin['spec_sha256'])
        self.assertEqual(intent['watch_policy_sha256'],s.digest(s.canonical(self.p.watch['canary_policy'])))
        self.assertEqual(intent['actual_ack'],'NOT_YET_OBSERVED')
        self.assertEqual(self.supervisor.contain_once()['reason'],'stop_already_attempted_no_retry')
    def test_unknown_dispatch_ack_runs_single_physical_stop_no_hold_finish_or_retry(self):
        self.setup_ports('unknown_ack')
        with self.assertRaises(RuntimeError):self.run_fixed()
        self.assertEqual([stage for stage,_ in self.backend.mutations],['activate'])
        self.assertEqual(self.backend.stop_count,1);self.assertEqual(self.backend.close_count,1)
        self.assertEqual(self.supervisor.contain_once()['reason'],'stop_already_attempted_no_retry')
    def test_invalid_seal_after_possible_dispatch_does_not_mutate_containment(self):
        self.setup_ports('invalid_seal')
        with self.assertRaises(RuntimeError):self.run_fixed()
        self.assertEqual(self.backend.stop_count,0);self.assertEqual(len(self.backend.mutations),1)
        record=self.session.journal.values[('activate','supervisor-failure-containment.json')]
        self.assertEqual(record['reason'],'seal_invalid');self.assertFalse(record['fenced'])
    def test_poisoned_watch_still_allows_separately_valid_stop_only_capability(self):
        self.setup_ports('watch_error')
        with self.assertRaises(s.Refused):self.run_fixed()
        self.assertTrue(self.session.failed);self.assertEqual(self.backend.stop_count,1)
        self.assertEqual([stage for stage,_ in self.backend.mutations],['activate'])
    def test_unknown_final_stop_is_not_retried_and_does_not_hold_or_finish(self):
        self.setup_ports('stop_error')
        with self.assertRaises(RuntimeError):self.run_fixed()
        self.assertEqual(self.backend.stop_count,1);self.assertEqual(len(self.backend.mutations),1)
    def test_backend_close_failure_refuses_pending_result_and_is_recorded(self):
        self.setup_ports('close_error')
        with self.assertRaises(OSError):self.run_fixed()
        self.assertEqual(self.backend.close_count,1)
        self.assertIn(('final_lookup','supervisor-backend-close-failed.json'),self.session.journal.values)
    def test_unknown_held_lookup_dispatch_has_separate_once_only_failure_stop(self):
        self.setup_ports('lookup_unknown')
        with self.assertRaisesRegex(RuntimeError,'lookup start ACK unknown'):self.run_fixed()
        self.assertEqual(self.backend.stop_count,2) # normal stop, then new failure_stop once
        self.assertEqual([stage for stage,_ in self.backend.mutations],['activate','final_hold','finish'])
        self.assertIn('failure_stop',self.session.journal.phases)
        self.assertEqual(self.supervisor.contain_once()['reason'],'stop_already_attempted_no_retry')

    def test_original_watch_error_survives_backend_close_error(self):
        self.setup_ports('watch_error')
        def close():self.backend.close_count+=1;raise OSError('synthetic extra close failure')
        self.backend.close=close
        with self.assertRaisesRegex(s.Refused,'synthetic watch unknown'):self.run_fixed()
        self.assertEqual(self.backend.close_count,1);self.assertEqual(self.backend.stop_count,1)

    def test_closed_mutation_grammar_rejects_unknown_phase_without_dispatch(self):
        self.setup_ports();op=types.SimpleNamespace(phase='arbitrary_sql')
        with self.assertRaises(s.Refused):self.supervisor.mutation(op)
        self.assertEqual(self.backend.mutations,[])


class NativePreservationWrapperTests(unittest.TestCase):
    def test_fixed_wrappers_require_exact_phase_and_explicit_permission(self):
        from linux_backend import LinuxBackend
        backend=LinuxBackend();seals=[];reference={'phase':'preservation_baseline'}
        cases=(('_preservation_baseline','strict_prepare','preservation_baseline','preservation_baseline_permitted','capture_baseline'),
               ('_preservation_load_baseline','canary','preflight','canary_final_preservation_permitted','load_capture'),
               ('_preservation_final','canary','preservation_final','canary_final_preservation_permitted','capture_final'))
        for method,action,phase,permission,delegate in cases:
            with self.subTest(method=method):
                op=types.SimpleNamespace(action=action,phase=phase,authority={permission:True},seal=lambda:seals.append(phase))
                args=(op,) if method=='_preservation_baseline' else (op,reference)
                with patch.object(preservation_capture,delegate,return_value={'synthetic_fixture':True}) as fixed:
                    self.assertEqual(getattr(backend,method)(*args,deadline=100),{'synthetic_fixture':True})
                    fixed.assert_called_once_with(backend,*args,deadline=100)
                for changes in ({'phase':'arbitrary'},{'action':'checkpoint'},{'authority':{permission:False}}):
                    bad=types.SimpleNamespace(**{**vars(op),**changes})
                    badargs=(bad,) if method=='_preservation_baseline' else (bad,reference)
                    with patch.object(preservation_capture,delegate) as fixed:
                        with self.assertRaises(interface.Refused):getattr(backend,method)(*badargs,deadline=100)
                        fixed.assert_not_called()
        self.assertEqual(len(seals),3)


class SqlBudgetTests(unittest.TestCase):
    def test_real_native_sql_prefix_composes_with_supervisor_and_actual_query_witness(self):
        # Only the innermost OS command dispatch is synthetic. The real native
        # _sql constructs observe/readback prefixes and the real query-bundle
        # validates the exact forwarded stdin/output/exit witness.
        from linux_backend import LinuxBackend,SQL_SESSION_PREFIX
        calls=[];value=source()
        class ForwardOnly(LinuxBackend):
            def _inspect(inner,*args,**kwargs):return {'synthetic_no_container_access':True}
            def _run(inner,op,binary,args,**kwargs):
                calls.append((binary,args,kwargs));inner._sequence+=1;raw=inventory.canonical(value)
                inner._artifact(op,'command-%04d'%inner._sequence,{'binary':binary,
                    'stdin_sha256':inventory.digest(kwargs['stdin']),'actual_exit_code':0,
                    'started':True,'owned_processes_closed':True,'expired':False,'output_limit_exceeded':False,
                    'unexpected_descendants':False,'interrupted':False,'stdout_hex':raw.hex(),
                    'stdout_bytes':len(raw),'stdout_sha256':inventory.digest(raw)})
                return raw
        p=prepared();p.modules['linux_backend']=types.SimpleNamespace(LinuxBackend=ForwardOnly,SQL_SESSION_PREFIX=SQL_SESSION_PREFIX)
        supervisor=s.Supervisor(p,types.SimpleNamespace());supervisor.deadline=time.monotonic()+5
        j=Journal();j.begin('preservation_final')
        storage=copy.deepcopy(p.storage);storage['linux']['postgres'].update(database='openlegal',uid=70,
            standby={'container_id':'b'*64})
        op=types.SimpleNamespace(operation_id=p.value['operation_id'],action='canary',phase='preservation_final',
            inventory=storage,inventory_sha256='a'*64,source_graph_sha256='b'*64,authority_sha256='c'*64,
            authority={},previous={},seal=lambda:None,journal=j)
        for kind,sql in (('observe',"SELECT openlegal_checkpoint.observe('%s'::uuid)"%op.operation_id),
                         ('readback',"SELECT openlegal_checkpoint.readback('%s'::uuid)"%op.operation_id),
                         ('position','SELECT openlegal_checkpoint.database_position()')):
            actual=preservation_backend._query_bundle(supervisor.backend,op,sql,'composed-'+kind,supervisor.deadline)
            self.assertEqual(actual,value)
        extra=b"SET statement_timeout='2s'; SET lock_timeout='1s'; SET transaction_timeout='5s'; "
        self.assertEqual(len(calls),3)
        for binary,args,kwargs in calls:
            self.assertEqual(binary,'docker');self.assertIn('psql',args)
            self.assertTrue(kwargs['stdin'].startswith(SQL_SESSION_PREFIX.encode()+extra))
            self.assertEqual(kwargs['stdin'].count(extra),1)
        self.assertEqual(len(j.values),3)

    def test_fixed_backend_keeps_sql_caps_and_original_phase_deadline(self):
        p=prepared();calls=[]
        class Backend:
            def _run(inner,op,binary,args,**kwargs):calls.append(kwargs);return b'{}'
        p.modules['linux_backend']=types.SimpleNamespace(LinuxBackend=Backend,SQL_SESSION_PREFIX='FIXED ')
        supervisor=s.Supervisor(p,types.SimpleNamespace())
        supervisor.deadline=time.monotonic()+1
        supervisor.backend._run(None,'docker',['exec','-i','target','psql'],stdin=b'FIXED SELECT synthetic_read;\n',timeout=30)
        self.assertIn(b"statement_timeout='2s'",calls[0]['stdin'])
        self.assertIn(b"lock_timeout='1s'",calls[0]['stdin'])
        self.assertIn(b"transaction_timeout='5s'",calls[0]['stdin'])
        self.assertLessEqual(calls[0]['timeout'],1)
        extra=b"SET statement_timeout='2s'; SET lock_timeout='1s'; SET transaction_timeout='5s'; "
        supervisor.backend._run(None,'docker',['exec','target','psql'],stdin=b'FIXED '+extra+b'SELECT synthetic_read;\n')
        self.assertEqual(calls[-1]['stdin'].count(extra),1)
        with self.assertRaisesRegex(s.Refused,'duplicate_sql_prefix'):
            supervisor.backend._run(None,'docker',['exec','target','psql'],stdin=b'FIXED '+extra+extra+b'SELECT synthetic_read;\n')
        with self.assertRaises(s.Refused):supervisor.backend._run(None,'docker',['exec','target','psql'],stdin=b'UNKNOWN SQL')


if __name__=='__main__':unittest.main()
