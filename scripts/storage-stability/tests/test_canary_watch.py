"""Offline synthetic timelines only: never a real six-hour or canary run."""
import copy
import datetime as dt
import importlib.util
import os
import sys
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

PATH=Path(__file__).resolve().parents[1]/'canary_watch.py'
spec=importlib.util.spec_from_file_location('canary_watch',PATH)
w=importlib.util.module_from_spec(spec);spec.loader.exec_module(w)


def identity():
    return {'schema_version':1,'kind':'openlegal_canary_watch_inventory','operation_id':str(uuid.uuid4()),
        'canary_id':str(uuid.uuid4()),'recovery_operation_id':str(uuid.uuid4()),'expected_pod_uid':str(uuid.uuid4()),
        'storage_inventory':{'path':'/tmp/synthetic-inventory.json','sha256':'a'*64},
        'preservation_baseline':{'path':'/tmp/synthetic-baseline.json','sha256':'b'*64},
        'supervisor_source':{'path':str(w.SELF.parent/'canary_supervisor.py'),'sha256':'c'*64},
        'canary_policy':{'allowed_active_guard_keys':['serve'],
            'workload_pins':[{'kind':'Deployment','name':'openlegal-serving','uid':str(uuid.uuid4()),'role':'serve','runtime_image_id':'sha256:'+'d'*64}],
            'collection_job_pins':[]}}


def iso(stamp):return dt.datetime.fromtimestamp(stamp,dt.timezone.utc).isoformat()
START=dt.datetime(2026,11,2,tzinfo=dt.timezone.utc).timestamp()


def sample(inv,mono=1,wall=START,*,active=False,consumed=0):
    row=None
    if active:
        row={'id':inv['canary_id'],'operation_id':inv['recovery_operation_id'],'consumed':consumed,
             'started_at':iso(START+.5),'deadline':iso(START+.5+w.DURATION),
             'last_observed_at':iso(min(wall,START+.5+w.DURATION)),'ended_at':None}
    return {'monotonic':mono,'wall_time':wall,'elapsed_seconds':.1,
        'canary':{'schema_version':1,'kind':'openlegal_canary_observation','observed_at':iso(wall),
                  'required_id':inv['canary_id'] if active else None,'canary':row},
        'native':{'pods':[{'uid':inv['expected_pod_uid'],'ready':True,'restarts':0,
            'configured_image':'synthetic@sha256:'+'d'*64,'runtime_image_id':'sha256:'+'d'*64,
            'epoch':'f'*36,'container_id':'e'*64,'started_at':'pinned','process_start_ticks':1}]},
        'provider':{'state':{'control':{'policy':'unchanged'},'budget':{'unresolved_response':False,'admission_owner':None}},
                    'admission_ownership':{'legacy_locked':False,'slots':[]}},'replica_barrier':{'already_checked_by_fixed_reader':True}}


class SchemaTests(unittest.TestCase):
    def test_import_inert(self):
        with patch('os.open',side_effect=AssertionError('no IO import')):
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

    def test_exact_policy_no_wildcards_unknown_sources_or_unmapped_jobs(self):
        inv=identity();w.validate_inventory(inv)
        for change in ('wildcard','unknown','unmapped','wrong_supervisor','runtime'):
            bad=copy.deepcopy(inv)
            if change=='wildcard':bad['canary_policy']['allowed_active_guard_keys'].append('collection.*')
            elif change=='unknown':bad['new_action']='admit'
            elif change=='wrong_supervisor':bad['supervisor_source']['path']='/tmp/exec.py'
            elif change=='runtime':bad['canary_policy']['workload_pins'][0]['runtime_image_id']='mutable:tag'
            else:bad['canary_policy']['allowed_active_guard_keys'].append('collection.'+str(uuid.uuid4())+'.0')
            with self.subTest(change=change),self.assertRaises(w.Refused):w.validate_inventory(bad)

    def test_finite_exact_collection_key_map(self):
        inv=identity();request=str(uuid.uuid4());key='collection.'+request+'.0'
        inv['canary_policy']['allowed_active_guard_keys'].append(key)
        inv['canary_policy']['collection_job_pins']=[{'name':'canary-worker','uid':str(uuid.uuid4()),'request_id':request,'epoch':0,'spec_sha256':'e'*64,'runtime_image_id':'sha256:'+'d'*64}]
        w.validate_inventory(inv)
        inv['canary_policy']['collection_job_pins'][0]['epoch']=True
        with self.assertRaises(w.Refused):w.validate_inventory(inv)

    def test_readonly_explicit_authority_not_mutation_or_accepted(self):
        inv=identity();auth={'schema_version':1,'kind':'openlegal_canary_watch_authority','approved':True,
            'operation_id':inv['operation_id'],'canary_id':inv['canary_id'],'recovery_operation_id':inv['recovery_operation_id'],
            'inventory_sha256':'a'*64,'watch_source_sha256':'b'*64,'supervisor_source_sha256':'c'*64,
            'read_only':True,'duration_secs':21600,'max_global_requests':100,'sampling_interval_secs':55,
            'mutations_permitted':False,'final_acceptance_permitted':False,'valid_until':START+22000}
        w.validate_authority(auth,inv,'a'*64,'b'*64,START)
        for key,value in (('duration_secs',21599),('max_global_requests',101),('approved',1),('mutations_permitted',True),('final_acceptance_permitted',True)):
            with self.subTest(key=key),self.assertRaises(w.Refused):w.validate_authority(dict(auth,**{key:value}),inv,'a'*64,'b'*64,START)
        with self.assertRaises(w.Refused):w.validate_authority(auth,inv,'a'*64,'b'*64,START+13*3600)

    def test_strict_json_duplicates_nonfinite_depth(self):
        for raw in (b'{"a":1,"a":2}',b'{"n":NaN}',b'['*34+b'0'+b']'*34):
            with self.assertRaises(w.Refused):w.decode(raw)

    def test_bundle_duplicate_bound_and_raw_hash(self):
        buf=w.SampleBuffer();value={'actual_exit_code':0,'owned_processes_closed':True}
        pin=buf.save('command.json',value);self.assertEqual(pin,w.digest(w.canonical(value)))
        with self.assertRaises(w.Refused):buf.save('command.json',value)
        with self.assertRaises(w.Refused):buf.save_bytes('large.json',b'x'*(w.MAX_BUNDLE+1))


class TimelineTests(unittest.TestCase):
    def setUp(self):
        self.inv=identity();self.t=w.Timeline(self.inv);self.t.accept(sample(self.inv),'before')
        self.t.accept(sample(self.inv,2,START+1,active=True),'after')

    def test_actual_21600_not_shortened_and_no_count_only_publication_acceptance(self):
        # Synthetic arithmetic proof only; no Session.observe or real six-hour claim.
        elapsed=0
        while elapsed<w.DURATION:
            elapsed=min(elapsed+w.INTERVAL,w.DURATION)
            done=self.t.accept(sample(self.inv,2+elapsed,START+1+elapsed,active=True,consumed=min(100,elapsed//55)),'observe')
            self.assertEqual(done,elapsed>=w.DURATION)
        self.assertLessEqual(self.t.count,w.MAX_SAMPLES)
        self.assertGreaterEqual(self.t.last['monotonic']-self.t.anchor,21600)

    def test_six_hour_must_have_live_before_after_bracket(self):
        t=w.Timeline(self.inv)
        with self.assertRaises(w.Refused):t.accept(sample(self.inv,2,START+1,active=True),'after')
        bad=sample(self.inv,62.01,START+61.01,active=True)
        with self.assertRaises(w.Refused):self.t.accept(bad,'observe')

    def test_gap_wall_jump_and_cumulative_jump_refused(self):
        for mono,wall in ((63,START+62),(57,START+59),(1,START+2)):
            with self.subTest(mono=mono,wall=wall),self.assertRaises(w.Refused):self.t.accept(sample(self.inv,mono,wall,active=True),'observe')

    def test_image_epoch_pod_uid_restart_or_replaced_process_refused(self):
        for field,value in (('uid',str(uuid.uuid4())),('restarts',1),('configured_image','new'),('epoch','new'),('container_id','a'*64),('process_start_ticks',2),('ready',False)):
            bad=sample(self.inv,57,START+56,active=True);bad['native']['pods'][0][field]=value
            with self.subTest(field=field),self.assertRaises(w.Refused):self.t.accept(bad,'observe')

    def test_db_window_cap_boolean_closed_and_reset_refused(self):
        for field,value in (('consumed',101),('consumed',True),('deadline',iso(START+21602)),('operation_id',str(uuid.uuid4())),('ended_at',iso(START+2))):
            bad=sample(self.inv,57,START+56,active=True);bad['canary']['canary'][field]=value
            with self.subTest(field=field),self.assertRaises(w.Refused):self.t.accept(bad,'observe')
        self.t.accept(sample(self.inv,57,START+56,active=True,consumed=3),'observe')
        with self.assertRaises(w.Refused):self.t.accept(sample(self.inv,112,START+111,active=True,consumed=2),'observe')

    def test_unknown_unlocked_owners_policy_drift_and_db_clock_failure(self):
        for change in ('owner','unknown','policy','clock'):
            bad=sample(self.inv,57,START+56,active=True)
            if change=='owner':bad['provider']['admission_ownership']['slots']=[{'locked':False}]
            elif change=='unknown':bad['provider']['state']['budget']['unresolved_response']=True
            elif change=='policy':bad['provider']['state']['control']['policy']='changed'
            else:bad['canary']['observed_at']=iso(START+70)
            with self.subTest(change=change),self.assertRaises(w.Refused):self.t.accept(bad,'observe')

    def test_cross_utc_midnight_never_resets_global_canary_cap(self):
        shift=20*3600;inv=identity();t=w.Timeline(inv)
        def shifted(mono,wall,active=False,consumed=0):
            r=sample(inv,mono,wall-shift,active=active,consumed=consumed)
            r['wall_time']=wall;r['canary']['observed_at']=iso(wall)
            if active:
                for k in ('started_at','deadline','last_observed_at'):r['canary']['canary'][k]=iso(w.timestamp(r['canary']['canary'][k])+shift)
            return r
        t.accept(shifted(1,START+shift),'before');t.accept(shifted(2,START+shift+1,True),'after')
        for elapsed in range(55,4*3600+56,55):t.accept(shifted(2+elapsed,START+shift+1+elapsed,True,100),'observe')
        with self.assertRaises(w.Refused):t.accept(shifted(2+elapsed+55,START+shift+1+elapsed+55,True,0),'observe')

    def test_19utc_timer_overlap_rejected_before_activation(self):
        inv=identity();t=w.Timeline(inv);r=sample(inv);r['wall_time']=START+13*3600;r['canary']['observed_at']=iso(r['wall_time']);t.accept(r,'before')
        r=sample(inv,2,START+13*3600+1,active=True)
        r['canary']['canary']['started_at']=iso(START+13*3600+.5);r['canary']['canary']['deadline']=iso(START+19*3600+.5)
        with self.assertRaises(w.Refused):t.accept(r,'after')

    def test_full_monotonic_window_cannot_replace_actual_db_deadline_observation(self):
        # Reproduces reviewer88fe: actual DB time may lag sample wall time by
        # 4.9s within permitted read skew, yet still be BEFORE its own deadline.
        for elapsed in range(55,21600,55):
            self.t.accept(sample(self.inv,2+elapsed,START+1+elapsed,active=True,consumed=100),'observe')
        final=sample(self.inv,21602,START+21601,active=True,consumed=100)
        final['canary']['observed_at']=iso(START+21596.1)
        final['canary']['canary']['last_observed_at']=iso(START+21596.1)
        self.assertFalse(self.t.accept(final,'observe'))
        actual_db_after=sample(self.inv,21607,START+21606,active=True,consumed=100)
        actual_db_after['canary']['observed_at']=iso(START+21601.1)
        self.assertTrue(self.t.accept(actual_db_after,'observe'))
        self.assertEqual(actual_db_after['canary']['canary']['deadline'],final['canary']['canary']['deadline'])

    def test_sample_deadline_no_late_healthy_acceptance(self):
        bad=sample(self.inv,57,START+56,active=True);bad['elapsed_seconds']=5.01
        with self.assertRaises(w.Refused):self.t.accept(bad,'observe')


class PortTests(unittest.TestCase):
    def backend(self):
        obj=w.Session();obj.value=identity();obj.sample_deadline=w.time.monotonic()+5;obj.cleanup_deadline=None
        self.calls=[]
        class Base:
            def _sql(inner,op,sql,**kw):self.calls.append(('sql',sql,kw));return {}
            def _run(inner,op,binary,args,**kw):self.calls.append(('run',binary,args,kw));return b''
        obj.modules={'linux_backend':types.SimpleNamespace(LinuxBackend=Base,SQL_SESSION_PREFIX='FIXED ')}
        return obj,obj._backend()

    def test_fixed_named_reads_only_and_resolved_original_deadline(self):
        obj,backend=self.backend();query="SELECT openlegal_checkpoint.canary_observation('"+obj.value['canary_id']+"'::uuid)"
        backend._sql(None,query)
        self.assertEqual(self.calls[-1][2]['deadline'],obj.sample_deadline)
        for sql in ('SELECT openlegal_admin.canary_init(NULL,NULL)','COMMIT','DELETE FROM x'):
            with self.assertRaises(w.Refused):backend._sql(None,sql)
        with self.assertRaises(w.Refused):backend._sql_mutation(None,'anything')
        self.assertEqual(len(self.calls),1)

    def test_no_kube_or_docker_effect_and_no_arbitrary_psql(self):
        obj,backend=self.backend();op=types.SimpleNamespace(inventory={'linux':{'cri_endpoint':'unix:///fixed'}})
        for binary,args,kw in (('kubectl',['patch','deployment'],{}),('docker',['start','cid'],{}),('docker',['exec','cid','psql'],{'stdin':b'FIXED DELETE FROM x;\n'}),('shell',['get'],{})):
            with self.subTest(args=args),self.assertRaises(w.Refused):backend._run(op,binary,args,**kw)
        self.assertEqual(self.calls,[])
        backend._run(op,'crictl',['--runtime-endpoint','unix:///fixed','ps','-a','-o','json'])
        self.assertEqual(len(self.calls),1)

    def test_fixed_cri_inspect_only_private_hash_recording_and_bound_endpoint(self):
        obj,backend=self.backend();op=types.SimpleNamespace(inventory={'linux':{'cri_endpoint':'unix:///fixed'}})
        exact=['--runtime-endpoint','unix:///fixed','inspect','-o','json','a'*64]
        backend._run(op,'crictl',exact,record=False)
        self.assertEqual(self.calls[-1][2],exact);self.assertIs(self.calls[-1][3]['record'],False)
        for args,kw in ((exact,{}),(exact,{'record':True}),
                (['--runtime-endpoint','unix:///other',*exact[2:]],{'record':False}),
                (exact[:-1]+['bad-id'],{'record':False}),
                (exact+['--output','yaml'],{'record':False}),
                (exact[:2]+['exec','a'*64,'sh'],{'record':False})):
            with self.subTest(args=args,kw=kw),self.assertRaises(w.Refused):backend._run(op,'crictl',args,**kw)
        self.assertEqual(len(self.calls),1)

    def test_readonly_query_server_caps_exact_and_no_silent_clock_reset(self):
        obj,backend=self.backend();query="SELECT openlegal_checkpoint.canary_observation('"+obj.value['canary_id']+"'::uuid)"
        backend._run(None,'docker',['exec','cid','psql'],stdin=('FIXED '+query+';\n').encode())
        raw=self.calls[0][3]['stdin'];self.assertIn(b"statement_timeout='2s'",raw);self.assertIn(b"transaction_timeout='5s'",raw)
        self.assertEqual(raw.count(query.encode()),1)

    def test_session_refuses_fork_or_serialized_anchor_without_any_files(self):
        obj=w.Session();obj.failed=False;obj.pid=os.getpid()+1;obj.ticks=1
        with self.assertRaises(w.Refused):obj._seal()
        obj.timeline=types.SimpleNamespace(after=None);obj.completed=False
        with self.assertRaises(w.Refused):obj.observe()


class PostWindowLoopTests(unittest.TestCase):
    def test_expired_monotonic_remaining_still_takes_bounded_fresh_read_samples(self):
        # Pure loop fixture only; no real time, Session._sample, DB or canary.
        obj=w.Session();obj.failed=False;obj.completed=False;obj.binding={}
        obj.timeline=types.SimpleNamespace(after={},anchor=0,count=2,last=None)
        now=[21600.0];sleeps=[];calls=[];outputs={}
        def sleep(seconds):sleeps.append(seconds);now[0]+=seconds
        def fresh(stage):
            calls.append(stage);obj.timeline.count+=1
            obj.timeline.last={'monotonic':now[0],'canary':{'canary':{'consumed':100}}}
            return len(calls)==2
        obj._seal=lambda:None;obj._sample=fresh
        obj.journal=types.SimpleNamespace(save=lambda name,value:outputs.update({name:value}))
        with patch.object(w,'time',types.SimpleNamespace(monotonic=lambda:now[0],sleep=sleep)):
            result=obj.observe()
        self.assertEqual(sleeps,[1,1]);self.assertEqual(calls,['observe','observe'])
        self.assertEqual(result['status'],'OBSERVATION_COMPLETE_PENDING_FINAL_GATES')
        self.assertFalse(result['final_acceptance']);self.assertTrue(obj.completed)

    def test_post_window_read_grace_never_unbounded_or_reopens_budget(self):
        obj=w.Session();obj.failed=False;obj.completed=False
        obj.timeline=types.SimpleNamespace(after={},anchor=0)
        obj._seal=lambda:None;obj._sample=lambda _:self.fail('no sample after bounded grace')
        with patch.object(w,'time',types.SimpleNamespace(monotonic=lambda:21661)),self.assertRaises(w.Refused):obj.observe()


class BundleAcceptanceTests(unittest.TestCase):
    def fixture(self, *, fail_reader=False, fail_journal=False):
        obj=w.Session();obj.value=identity();obj.failed=False;obj.binding={'authority_sha256':'a'*64};obj.storage={'source_graph':{}}
        obj.sample_deadline=None;obj.cleanup_deadline=None;self.events=[];self.writes={}
        class Timeline:
            count=0
            def accept(inner,*args):inner.count+=1;self.events.append('validated');return False
        class Journal:
            def save(inner,name,value):
                if fail_journal and name.startswith('sample-'):raise OSError('synthetic mirror unavailable')
                self.writes[name]=copy.deepcopy(value);self.events.append('durable-bundle');return 'e'*64
        class Backend:
            _artifacts={}
            def _canary_native(inner,op):
                self.events.append('fixed-native-read')
                op.journal.save('synthetic-command.json',{'fixture_only':True,'actual_execution_claimed':False})
                if fail_reader:raise w.Refused('synthetic_reader_failure')
                return {'synthetic':True}
            def _read_replica_barrier(inner,*args,**kwargs):return {'synthetic':True}
            def _sql(inner,*args,**kwargs):return {'synthetic':True}
        obj.timeline=Timeline();obj.journal=Journal();obj.backend=Backend();obj.modules={'provider_backend':types.SimpleNamespace(_project=lambda _:None)}
        obj._seal=lambda:w.need(not obj.failed,'synthetic_failed_session')
        return obj

    def test_read_evidence_never_accepted_before_dual_bundle_write(self):
        obj=self.fixture();obj._sample('before')
        self.assertEqual(self.events,['fixed-native-read','validated','durable-bundle'])
        bundle=self.writes['sample-0001.json'];self.assertEqual(bundle['actual_command_evidence']['synthetic-command.json']['json']['actual_execution_claimed'],False)

    def test_failed_native_read_preserves_unknown_bundle_no_completed_sample(self):
        obj=self.fixture(fail_reader=True)
        with self.assertRaises(w.Refused):obj._sample('before')
        self.assertTrue(obj.failed);self.assertIn('failed.json',self.writes);self.assertNotIn('sample-0001.json',self.writes)
        with self.assertRaises(w.Refused):obj._sample('before')
        self.assertEqual(self.events.count('fixed-native-read'),1)

    def test_mirror_publication_error_poisoned_no_success_return_or_retry(self):
        obj=self.fixture(fail_journal=True)
        with self.assertRaises(OSError):obj._sample('before')
        self.assertTrue(obj.failed);self.assertEqual(self.writes['failed.json']['status'],'FAILED_OR_UNKNOWN')
        self.assertFalse(self.writes['failed.json']['final_acceptance'])


class BackendLifetimeTests(unittest.TestCase):
    def context(self,close_failure=False):
        events=[];obj=w.Session();obj.failed=False;obj.binding={}
        class Lock:
            held=True
            def close(inner):events.append('lock-close');inner.held=False
        lock=Lock()
        class Backend:
            def close(inner):
                self.assertTrue(lock.held);events.append('backend-close')
                if close_failure:raise OSError('synthetic FD failure')
        obj.lock=lock;obj.backend=Backend();obj.journal=types.SimpleNamespace(save=lambda *args:None)
        return obj,events
    def test_arm_final_validation_failure_closes_new_backend_before_lock(self):
        events=[];value=identity()
        class Lock:
            held=True
            def acquire(inner):return inner
            def close(inner):events.append('lock-close');inner.held=False
        lock=Lock()
        class Backend:
            def close(inner):self.assertTrue(lock.held);events.append('backend-close');raise OSError('secondary cleanup failure')
        class Journal:
            def begin(inner,*args):pass
        storage={'host':{'boot_id':'synthetic-boot'},'shared_lock':{},'journal_roots':[]}
        modules={'journal':types.SimpleNamespace(SharedLock=lambda _:lock,DualJournal=lambda *args:Journal())}
        with patch.object(w,'_prepared',return_value=(value,{},storage,modules,{})), \
             patch.object(w.Session,'_backend',return_value=Backend()), \
             patch.object(w.Session,'_seal',side_effect=w.Refused('original final arm check')), \
             patch.object(sys.modules['__main__'],'__file__',value['supervisor_source']['path']):
            with self.assertRaisesRegex(w.Refused,'original final arm check'):
                w.Session.arm(inventory_path='x',inventory_sha256='a'*64,authority_path='y',authority_sha256='b'*64,source_sha256='c'*64)
        self.assertEqual(events,['backend-close','lock-close'])

    def test_normal_watch_backend_close_precedes_lock_release(self):
        obj,events=self.context()
        with obj:pass
        self.assertEqual(events,['backend-close','lock-close'])
    def test_original_body_failure_survives_close_failure_and_lock_still_releases(self):
        obj,events=self.context(True)
        with self.assertRaisesRegex(w.Refused,'synthetic original'):
            with obj:raise w.Refused('synthetic original')
        self.assertEqual(events,['backend-close','lock-close'])
    def test_close_error_without_original_failure_refuses_clean_context_exit(self):
        obj,events=self.context(True)
        with self.assertRaises(OSError):
            with obj:pass
        self.assertEqual(events,['backend-close','lock-close'])


if __name__=='__main__':unittest.main()
