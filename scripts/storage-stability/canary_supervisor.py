#!/usr/bin/env python3
"""Explicit one-use canary lifecycle; final acceptance is a separate gate.

No effects on import, credential/role creation, arbitrary SQL/command hooks,
provider HTTP, retry, historical-phase adoption, or automatic failover. The live
watch Session holds the same operation lock across activation, 21600 seconds of
sampled observation, actual writer drainage, owned final hold, full original
preservation and M31 finish. The final hold stays installed. Publication,
checkpoint, restore, public transport and actual OS evidence remain pending.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import json
import math
import os
import re
import stat
import sys
import time
from pathlib import Path
from types import SimpleNamespace

SELF=Path('/usr/local/lib/openlegal-storage-stability/canary_supervisor.py')
PRESERVATION_BUDGET_SECS=1200
PENDING=('fresh_full_old_preservation_subset','eligible_ack_publication_source_relationship',
         'verified_coordinated_checkpoint','verified_isolated_restore','fresh_public_16_transports',
         'actual_supervisor_os_exit_zero_and_closed_dual_journal')
LIFECYCLE_FUNCTIONS=(
 'openlegal_checkpoint.activate_canary(uuid,uuid,text,uuid)',
 'openlegal_checkpoint.finish_canary(uuid,text,uuid)',
 'openlegal_checkpoint.canary_readback(uuid)',
 'openlegal_checkpoint.hold(uuid,text)',
 'openlegal_checkpoint.readback(uuid)',
 'openlegal_checkpoint.observe(uuid)',
 'openlegal_checkpoint.canary_observation(uuid)',
 'openlegal_checkpoint.preservation_page(text,jsonb,integer)',
 'openlegal_checkpoint.retained_references()',
 'openlegal_checkpoint.database_position()',
 'openlegal_checkpoint.client_sessions()',
 'openlegal_replication.capture_prefix()',
 'openlegal_replication.observe_flush()')
# This is a fixed source-owned list, never an inventory/authority SQL hook.
IDENTITY_SQL=("SELECT jsonb_build_object('session_user',session_user,'current_user',current_user,"
 "'superuser',rolsuper,'create_role',rolcreaterole,'create_db',rolcreatedb,"
 "'replication',rolreplication,'bypass_rls',rolbypassrls,'login',rolcanlogin,"
 "'lifecycle_functions_installed',"+
 ' AND '.join("to_regprocedure('"+name+"') IS NOT NULL" for name in LIFECYCLE_FUNCTIONS)+
 ",'lifecycle_execute_permitted',COALESCE("+
 ' AND '.join("has_function_privilege(current_user,to_regprocedure('"+name+"')::oid,'EXECUTE')" for name in LIFECYCLE_FUNCTIONS)+
 " AND has_schema_privilege(current_user,'openlegal_checkpoint','USAGE')"
 " AND has_schema_privilege(current_user,'openlegal_replication','USAGE'),false)) "
 "FROM pg_catalog.pg_roles WHERE rolname=current_user")


class Refused(RuntimeError):
    """Static sanitized error codes; never include credentials or SQL output."""


def need(value,code):
    if not value:raise Refused(code)


def canonical(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)+'\n').encode()


def digest(raw):return hashlib.sha256(raw).hexdigest()


def pin(value):return type(value)is str and re.fullmatch('[0-9a-f]{64}',value)is not None


def _raw(path,expected):
    """Bootstrap before importing any non-stdlib source."""
    need(pin(expected),'bootstrap_pin')
    path=Path(path);need(path.is_absolute()and path.resolve()==path and '..'not in path.parts,'bootstrap_path')
    for parent in path.parents:
        s=parent.lstat();need(stat.S_ISDIR(s.st_mode)and s.st_uid==0 and
            (not s.st_mode&0o022 or parent==Path('/tmp')and s.st_mode&stat.S_ISVTX),'bootstrap_ancestor')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
    with os.fdopen(fd,'rb')as f:
        a=os.fstat(f.fileno());need(stat.S_ISREG(a.st_mode)and a.st_uid==a.st_gid==0 and a.st_nlink==1 and
            stat.S_IMODE(a.st_mode)==0o600 and a.st_size<=4*1024*1024,'bootstrap_metadata')
        raw=f.read(4*1024*1024+1);z=os.fstat(f.fileno());now=path.lstat()
    need(len(raw)==a.st_size and all(getattr(a,k)==getattr(z,k)==getattr(now,k)for k in
        ('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns'))and digest(raw)==expected,'bootstrap_hash_or_rebind')
    return raw


def _watch(source_sha):
    path=SELF.parent/'canary_watch.py';raw=_raw(path,source_sha)
    spec=importlib.util.spec_from_file_location('canary_watch',path);module=importlib.util.module_from_spec(spec)
    # Source is authenticated BEFORE importing its code. It is inert on import.
    sys.modules['canary_watch']=module;exec(compile(raw,str(path),'exec'),module.__dict__)
    return module


def semantic_intents(inventory_sha,source_sha,watch_inventory_sha,watch_authority_sha):
    base={'schema_version':1,'kind':'openlegal_canary_supervisor_semantic_intent',
          'inventory_sha256':inventory_sha,'supervisor_source_sha256':source_sha,
          'watch_inventory_sha256':watch_inventory_sha,'watch_authority_sha256':watch_authority_sha}
    return {stage:dict(base,stage=stage)for stage in('activate','final_hold_and_finish')}


def validate_inputs(value,authority,watch,storage,*,inventory_sha,source_sha,now):
    need(type(value)is dict and set(value)=={'schema_version','kind','operation_id','watch_inventory',
        'watch_authority','watch_source_sha256','expected_held_generation','operator_login'}and
        type(value['schema_version'])is int and value['schema_version']==1 and
        value['kind']=='openlegal_canary_supervisor_inventory','supervisor_inventory')
    need(value['operation_id']==watch['operation_id']and pin(value['watch_source_sha256'])and
         type(value['operator_login'])is str and re.fullmatch('[a-z][a-z0-9_]{0,62}',value['operator_login'])and
         value['operator_login']not in('postgres','root')and value['operator_login']==storage['linux']['postgres']['admin_user'],
         'supervisor_operation_or_login')
    for key in('watch_inventory','watch_authority'):
        row=value[key];need(type(row)is dict and set(row)=={'path','sha256'}and pin(row['sha256'])and
            type(row['path'])is str and Path(row['path']).is_absolute(),'supervisor_input_pin')
    # UUID grammar is delegated to the already source-pinned watch reader.
    import uuid
    try:need(str(uuid.UUID(value['expected_held_generation']))==value['expected_held_generation']and
             int(uuid.UUID(value['expected_held_generation']))!=0,'supervisor_hold_generation')
    except (ValueError,TypeError,AttributeError):raise Refused('supervisor_hold_generation')from None
    intents=semantic_intents(inventory_sha,source_sha,value['watch_inventory']['sha256'],value['watch_authority']['sha256'])
    expected={'schema_version':1,'kind':'openlegal_canary_supervisor_authority','approved':True,
        'operation_id':value['operation_id'],'canary_id':watch['canary_id'],'recovery_operation_id':watch['recovery_operation_id'],
        'inventory_sha256':inventory_sha,'supervisor_source_sha256':source_sha,'watch_source_sha256':value['watch_source_sha256'],
        'managed_epoch':storage['managed_epoch'],'expected_held_generation':value['expected_held_generation'],
        'activation_intent_sha256':digest(canonical(intents['activate'])),
        'final_hold_intent_sha256':digest(canonical(intents['final_hold_and_finish'])),
        'canary_activate_permitted':True,'canary_collection_jobs_activate_permitted':True,
        'canary_final_stop_permitted':True,'canary_final_hold_permitted':True,'canary_final_preservation_permitted':True,'canary_finish_permitted':True,'canary_held_lookup_resume_permitted':True,
        'duration_secs':21600,'max_global_requests':100,'automatic_retry':False,
        'credential_provisioning_permitted':False,'final_acceptance_permitted':False}
    need(type(authority)is dict and set(authority)==set(expected)|{'valid_until'}and
         all(type(authority[k])is type(v)and authority[k]==v for k,v in expected.items()),'supervisor_authority')
    need(type(authority['valid_until'])in(int,float)and math.isfinite(authority['valid_until'])and
         now+21660<authority['valid_until'],'supervisor_authority_window')
    need(1<=len(watch['canary_policy']['collection_job_pins'])<=16 and
         watch['canary_policy']['allowed_active_guard_keys'][0]=='serve','supervisor_finite_canary_jobs')
    return intents


def _load(inventory_path,inventory_sha,authority_path,authority_sha,source_sha):
    need(os.geteuid()==0 and sys.dont_write_bytecode and Path(__file__).resolve()==SELF,'installed_root_no_bytecode')
    _raw(SELF,source_sha)
    # Decode only after the tiny trusted watcher bootstrap authenticates its
    # source. Inventory itself is bounded UTF8, duplicate/nonfinite-safe there.
    first=_raw(inventory_path,inventory_sha)
    def pairs(rows):
        out={}
        for k,v in rows:need(k not in out,'bootstrap_json_duplicate');out[k]=v
        return out
    value=json.loads(first,object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in()).throw(Refused('bootstrap_json_nonfinite')))
    watch_module=_watch(value['watch_source_sha256'])
    value=watch_module.decode(first);authority=watch_module.decode(_raw(authority_path,authority_sha))
    wi=value['watch_inventory'];wa=value['watch_authority']
    watch,_,storage,modules,files=watch_module._prepared(wi['path'],wi['sha256'],wa['path'],wa['sha256'],value['watch_source_sha256'])
    intents=validate_inputs(value,authority,watch,storage,inventory_sha=inventory_sha,source_sha=source_sha,now=time.time())
    files.update({str(SELF):source_sha,str(inventory_path):inventory_sha,str(authority_path):authority_sha})
    return SimpleNamespace(value=value,authority=authority,watch=watch,storage=storage,modules=modules,
        files=files,intents=intents,watch_module=watch_module,source_sha=source_sha,inventory_sha=inventory_sha,authority_sha=authority_sha)


def _login(value,wanted):
    fields={'session_user','current_user','superuser','create_role','create_db','replication','bypass_rls','login','lifecycle_functions_installed','lifecycle_execute_permitted'}
    need(type(value)is dict and set(value)==fields and value['session_user']==value['current_user']==wanted and
         all(value[k]is True for k in('login','lifecycle_functions_installed','lifecycle_execute_permitted'))and
         all(value[k]is False for k in('superuser','create_role','create_db','replication','bypass_rls')),
         'canary_operator_login_not_least_privileged')


def _activation(value,prepared):
    w=prepared.watch;storage=prepared.storage;expected={'canary_id':w['canary_id'],'recovery_operation_id':w['recovery_operation_id'],
        'intent_sha256':prepared.authority['activation_intent_sha256'],'managed_epoch':storage['managed_epoch'],
        'blob_binding_sha256':storage['linux']['blob_binding_sha256'],
        'original_hold_generation':prepared.value['expected_held_generation'],'recovery_hold':False}
    need(type(value)is dict and set(value)==set(expected)|{'released_generation','started_at','deadline','consumed'}and
         all(type(value[k])is type(v)and value[k]==v for k,v in expected.items()),'canary_activation_result')
    need(prepared.watch_module.uid(value['released_generation'])and value['released_generation']!=expected['original_hold_generation']and
         type(value['consumed'])is int and 0<=value['consumed']<=100 and
         prepared.watch_module.timestamp(value['deadline'])-prepared.watch_module.timestamp(value['started_at'])==21600,
         'canary_activation_window_or_generation')


def _finished(value,prepared,activation,hold):
    expected={'canary_id':prepared.watch['canary_id'],'recovery_operation_id':prepared.watch['recovery_operation_id'],
        'final_checkpoint_operation':prepared.value['operation_id'],'intent_sha256':prepared.authority['final_hold_intent_sha256'],
        'managed_epoch':prepared.storage['managed_epoch'],'blob_binding_sha256':prepared.storage['linux']['blob_binding_sha256'],
        'hold_generation':hold['installed_generation'],'started_at':activation['started_at'],'deadline':activation['deadline'],
        'recovery_hold':True,'physical_closure_and_publication_acceptance':'separate_required_evidence'}
    need(type(value)is dict and set(value)==set(expected)|{'closed_at','consumed'}and
         all(type(value[k])is type(v)and value[k]==v for k,v in expected.items())and type(value['consumed'])is int and
         0<=value['consumed']<=100 and prepared.watch_module.timestamp(value['closed_at'])>=prepared.watch_module.timestamp(value['deadline']),
         'canary_finish_result')


def _jobs(backend,op,policy):
    """Exactly one UID/RV/spec-fenced suspend-true to false attempt per Job."""
    config=op.inventory['linux'];need(op.authority['canary_collection_jobs_activate_permitted']is True,'canary_job_activation_permission')
    for pin in policy['collection_job_pins']:
        item=backend._kube(op,['-n',config['namespace'],'get','job',pin['name'],'-o','json'])
        spec=copy.deepcopy(item['spec']);suspend=spec.pop('suspend',False)
        need(item['metadata']['uid']==pin['uid']and suspend is True and
             digest(canonical(spec))==pin['spec_sha256']and item.get('status',{}).get('active',0)==0,
             'canary_activation_job_changed')
        patch=[{'op':'test','path':'/metadata/uid','value':pin['uid']},
            {'op':'test','path':'/metadata/resourceVersion','value':item['metadata']['resourceVersion']},
            {'op':'test','path':'/spec/suspend','value':True},
            {'op':'replace','path':'/spec/suspend','value':False}]
        patch_raw=json.dumps(patch,separators=(',',':'))
        op.seal()
        backend._artifact(op,'canary-job-activation-intent-'+pin['uid'],{
            'kind':'openlegal_canary_job_activation_intent','operation_id':op.operation_id,
            'phase':op.phase,'authority_sha256':op.authority_sha256,'inventory_sha256':op.inventory_sha256,
            'source_graph_sha256':op.source_graph_sha256,'watch_policy_sha256':digest(canonical(policy)),
            'namespace':config['namespace'],'name':pin['name'],'uid':pin['uid'],
            'original_resource_version':item['metadata']['resourceVersion'],
            'normalized_spec_sha256':pin['spec_sha256'],'patch_sha256':digest(patch_raw.encode('utf8')),
            'patch':patch,'one_attempt':True,'actual_ack':'NOT_YET_OBSERVED'})
        op.seal();result=backend._kube(op,['-n',config['namespace'],'patch','job',pin['name'],'--type=json',
            '-p',patch_raw,'-o','json'])
        normalized=copy.deepcopy(result['spec']);normalized.pop('suspend',None)
        need(result['metadata']['uid']==pin['uid']and result['spec'].get('suspend')is False and
             result['metadata']['resourceVersion']!=item['metadata']['resourceVersion']and
             digest(canonical(normalized))==pin['spec_sha256'],'canary_activation_job_ack_unknown')
        backend._artifact(op,'canary-job-activated-'+pin['uid'],{'uid':pin['uid'],'name':pin['name'],
            'original_resource_version':item['metadata']['resourceVersion'],'resource_version':result['metadata']['resourceVersion'],
            'normalized_spec_sha256':pin['spec_sha256'],'one_attempt':True})


def _closure(value,fields):
    need(type(value)is dict and set(value)==fields and value['sql_sessions']==[] and
         type(value['cri_containers'])is list and type(value['cgroups'])is list and value['cgroups'] and
         type(value['runtime_guards'])is list and value['runtime_guards'],'canary_final_closure_shape')
    need(all(type(r)is dict and r.get('state')=='CONTAINER_EXITED'and
             type(r.get('id'))is str and re.fullmatch('[0-9a-f]{64}',r['id'])for r in value['cri_containers']),
         'canary_live_cri_remains')
    need(all(type(r)is dict and r.get('pids')==[]for r in value['cgroups'])and
         all(type(r)is dict and r.get('state')=='closed_clean'and r.get('locked')is False for r in value['runtime_guards']),
         'canary_writer_or_guard_remains')


def _lookup(value,p,activation,finish,hold):
    fields={'kind','canary_id','finish_readback','provider_before','provider_after','replica_barrier','lookup',
        'collection_resumed','final_acceptance','remaining_gates'}
    need(type(value)is dict and set(value)==fields and value['kind']=='openlegal_canary_closed_lookup_resume'and
         value['canary_id']==p.watch['canary_id']and value['finish_readback']=={'activation':activation,'finish':finish}and
         value['collection_resumed']is False and value['final_acceptance']is False and
         value['remaining_gates']=='external_checkpoint_preservation_public_acceptance','canary_held_lookup_result')
    for key in('provider_before','provider_after'):
        projection=p.modules['provider_backend']._project(value[key])
        need(projection['clean']and projection['recovery_hold']and projection['generation']==hold['installed_generation']and
             projection['owner_operation_id']==p.value['operation_id'],'canary_held_lookup_provider')
    pods=value['lookup'].get('pods');config=p.storage['linux']
    need(type(pods)is list and len(pods)==1 and p.watch_module.uid(pods[0].get('uid'))and
         pods[0].get('ready')is True and type(pods[0].get('restarts'))is int and pods[0]['restarts']==0 and
         pods[0].get('epoch')==p.storage['managed_epoch']and pods[0].get('configured_image')==config['serving_image']and
         pods[0].get('runtime_image_id')==config['serving_runtime_image_id'],'canary_held_lookup_native')


def _preservation(value,op,baseline,modules):
    """Only an actual acknowledged native subset result discharges this gate."""
    need(type(value) is dict and set(value) == {'index','reference','comparison'},'canary_preservation_result')
    capture=modules['preservation_capture'];reader=modules['inventory'].decode
    ref=value['reference'];comparison=value['comparison'];index=value['index']
    need(type(ref) is dict and set(ref) == capture.REF_FIELDS and ref['schema_version'] == 1
         and type(ref['schema_version']) is int and ref['kind'] == 'openlegal_preservation_reference'
         and ref['phase'] == op.phase == 'preservation_final' and ref['operation_id'] == op.operation_id
         and ref['name'] == capture.INDEX_NAME and pin(ref['sha256']),
         'canary_preservation_final_reference')
    identity={k:getattr(op,k) for k in ('operation_id','action','phase','inventory_sha256',
        'source_graph_sha256','authority_sha256')}
    identity.update(managed_epoch=op.inventory['managed_epoch'],
        target_container_id=op.inventory['linux']['postgres']['primary']['container_id'])
    need(type(index) is dict and index.get('kind') == 'openlegal_preservation_capture_index'
         and type(index.get('schema_version')) is int and index['schema_version'] == 1
         and all(index.get(k) == v for k,v in identity.items())
         and index.get('journal_phase_roots') == ref['journal_phase_roots']
         and all(index.get(k) is False for k in ('producer_os_exit_zero_claimed','public_acknowledgement_claimed',
             'checkpoint_claimed','final_acceptance_claimed'))
         and reader(op.journal.read_phase_bytes(op.phase,ref['name'],ref['sha256'])) == index,
         'canary_preservation_index_raw_binding')
    need(type(comparison) is dict and set(comparison) == {'name','sha256'}
         and comparison['name'] == op.phase+'-capture-subset-result.json' and pin(comparison['sha256']),
         'canary_preservation_comparison_reference')
    proof=reader(op.journal.read_phase_bytes(op.phase,comparison['name'],comparison['sha256']))
    fields={'schema_version','kind',*identity,'baseline','final','relations','generations',
        'every_baseline_row_unchanged','all_baseline_generations_preserved',
        'public_acknowledgement_claimed','checkpoint_claimed','final_acceptance_claimed'}
    need(type(proof) is dict and set(proof) == fields and type(proof['schema_version']) is int
         and proof['schema_version'] == 1 and proof['kind'] == 'openlegal_preservation_subset_result'
         and all(proof[k] == v for k,v in identity.items()) and proof['baseline'] == baseline and proof['final'] == ref
         and proof['every_baseline_row_unchanged'] is True and proof['all_baseline_generations_preserved'] is True
         and all(proof[k] is False for k in ('public_acknowledgement_claimed','checkpoint_claimed','final_acceptance_claimed')),
         'canary_preservation_subset_raw_binding')
    kinds=modules['preservation_backend'].KINDS
    need(type(proof['relations']) is list and len(proof['relations']) == len(kinds)
         and [r.get('relation') for r in proof['relations']] == list(kinds),'canary_preservation_all_relations')
    for row in proof['relations']:
        need(set(row) == {'relation','baseline_rows','final_rows','every_baseline_row_unchanged','monotonic_fields_preserved'}
             and all(type(row[k]) is int and row[k] >= 0 for k in ('baseline_rows','final_rows'))
             and row['final_rows'] >= row['baseline_rows'] and row['every_baseline_row_unchanged'] is True
             and row['monotonic_fields_preserved'] is True,'canary_preservation_relation_proof')
    need(type(proof['generations']) is dict and set(proof['generations']) == set(capture.ROLES),
         'canary_preservation_four_roots')
    for row in proof['generations'].values():
        need(type(row) is dict and set(row) == {'baseline_generations','final_generations','all_baseline_generations_preserved'}
             and all(type(row[k]) is int and row[k] >= 0 for k in ('baseline_generations','final_generations'))
             and row['final_generations'] >= row['baseline_generations'] and row['all_baseline_generations_preserved'] is True,
             'canary_preservation_generation_proof')
    return {'baseline':copy.deepcopy(baseline),'final':copy.deepcopy(ref),'comparison':copy.deepcopy(comparison)}


class Supervisor:
    """Fixed lifecycle only. Constructed from authenticated files, never hooks."""
    def __init__(self,prepared,session):
        self.p=prepared;self.session=session;self.backend=self._backend()
        self.phase=None;self.deadline=None;self.cleanup_deadline=None;self.failed=False
        self.activation_may_dispatched=False;self.stop_may_dispatched=False;self.lookup_may_dispatched=False;self.failure_stop_attempted=False;self.started=False
        self.original_error=None;self.baseline_reference=None
        self.stop_available=callable(getattr(self.backend,'_canary_stop_and_drain',None))
        self.lookup_available=callable(getattr(self.backend,'_canary_restart_lookup_held',None))
        self.preservation_available=all(callable(getattr(self.backend,name,None)) for name in (
            '_preservation_load_baseline','_preservation_final'))

    def _backend(self):
        base=self.p.modules['linux_backend'].LinuxBackend;supervisor=self
        class Fixed(base):
            def _run(inner,op,binary,args,**kwargs):
                if binary=='docker'and args and args[0]=='exec'and 'psql'in args:
                    prefix=supervisor.p.modules['linux_backend'].SQL_SESSION_PREFIX.encode()
                    raw=kwargs.get('stdin');need(type(raw)is bytes and raw.startswith(prefix),'supervisor_sql_prefix')
                    extra=b"SET statement_timeout='2s'; SET lock_timeout='1s'; SET transaction_timeout='5s'; "
                    suffix=raw[len(prefix):]
                    need(not suffix.startswith(extra+extra),'supervisor_duplicate_sql_prefix')
                    # Native observe/readback already install these exact caps.
                    # Preserve a single source-owned prefix for witness binding.
                    kwargs['stdin']=raw if suffix.startswith(extra) else prefix+extra+suffix
                end=supervisor.cleanup_deadline or supervisor.deadline
                if end is not None:kwargs['timeout']=min(kwargs.get('timeout',30),max(.001,end-time.monotonic()))
                return super()._run(op,binary,args,**kwargs)
            def _close_owned_sql(inner,op,target,application):
                need(target==op.inventory['linux']['postgres']['primary']['container_id']and
                     re.fullmatch('openlegal_storage_'+op.operation_id.replace('-','')+'_[0-9]{1,8}',application),'supervisor_sql_cleanup_owner')
                supervisor.cleanup_deadline=time.monotonic()+15
                try:return super()._close_owned_sql(op,target,application)
                finally:supervisor.cleanup_deadline=None
        return Fixed()

    def seal(self):
        need(not self.failed,'supervisor_terminal_failure')
        self.session._seal()
        need(time.time()<self.p.authority['valid_until']and time.time()<self.p.watch_module.next_timer(self.session.timeline.before['wall_time']if self.session.timeline.before else time.time()),
             'supervisor_authority_or_daily_boundary')
        end=self.cleanup_deadline or self.deadline
        if end is not None:need(time.monotonic()<end,'supervisor_phase_deadline')
        for path,pin in self.p.files.items():_raw(path,pin)

    def op(self,phase):
        self.seal()
        self.session.journal.begin(phase);self.phase=phase
        self.session.journal.save('supervisor-phase-authority.json',{'kind':'openlegal_canary_supervisor_phase_authority',
            'phase':phase,'operation_id':self.p.value['operation_id'],'authority_sha256':self.p.authority_sha,
            'inventory_sha256':self.p.inventory_sha,'source_sha256':self.p.source_sha,
            'watch_session_authority_is_not_mutation_authority':True})
        return SimpleNamespace(operation_id=self.p.value['operation_id'],action='canary',phase=phase,
            inventory=self.p.storage,inventory_sha256=self.p.watch['storage_inventory']['sha256'],
            source_graph_sha256=digest(canonical(self.p.storage['source_graph'])),authority_sha256=self.p.authority_sha,
            journal=self.session.journal,authority=self.p.modules['interface'].freeze_authority(copy.deepcopy(self.p.authority)),
            previous={},seal=self.seal,canary_policy=copy.deepcopy(self.p.watch['canary_policy']))

    def mutation(self,op):
        stage=op.phase;self.seal()
        watch=self.p.watch;intent_sha=self.p.authority['final_hold_intent_sha256']
        if stage=='activate':
            need(op.authority['canary_activate_permitted']is True,'activate_permission')
            intent=self.p.intents['activate']
            sql="SELECT openlegal_checkpoint.activate_canary('%s'::uuid,'%s'::uuid,'%s','%s'::uuid)"%(
                watch['recovery_operation_id'],watch['canary_id'],self.p.authority['activation_intent_sha256'],self.p.value['expected_held_generation'])
        elif stage=='final_hold':
            need(op.authority['canary_final_hold_permitted']is True,'final_hold_permission')
            intent=self.p.intents['final_hold_and_finish']
            sql="SELECT openlegal_checkpoint.hold('%s'::uuid,'%s')"%(self.p.value['operation_id'],intent_sha)
        elif stage=='finish':
            need(op.authority['canary_finish_permitted']is True,'finish_permission')
            intent=self.p.intents['final_hold_and_finish']
            sql="SELECT openlegal_checkpoint.finish_canary('%s'::uuid,'%s','%s'::uuid)"%(self.p.value['operation_id'],intent_sha,watch['canary_id'])
        else:raise Refused('supervisor_unknown_mutation_stage')
        self.backend._artifact(op,'semantic-intent',intent)
        old=self.session.journal.output_hashes()
        if stage=='activate':self.activation_may_dispatched=True
        result=self.backend._sql_mutation(op,sql)
        ack=self.p.modules['provider_backend']._ack(self.backend,op,old)
        self.backend._artifact(op,'actual-mutation-result',{'result':result,'wal_ack':ack})
        return result

    def containment_seal(self):
        # A failed observation poisons acceptance, not the separately approved
        # stop-only capability. Revalidate live/source/authority/lock evidence
        # directly, without clearing or overriding Session.failed.
        p=self.p;s=self.session
        need(os.getpid()==s.pid and s._ticks()==s.ticks and
             Path('/proc/sys/kernel/random/boot_id').read_text().strip()==s.boot,'containment_live_owner')
        need(p.authority['canary_final_stop_permitted']is True and time.time()<p.authority['valid_until'],
             'containment_authority')
        for path,pin in p.files.items():_raw(path,pin)
        s.lock.check();s.journal.check()

    def contain_once(self):
        if not self.activation_may_dispatched:return {'attempted':False,'reason':'activation_not_dispatched'}
        if self.failure_stop_attempted or self.stop_may_dispatched and not self.lookup_may_dispatched:return {'attempted':False,'reason':'stop_already_attempted_no_retry'}
        try:self.containment_seal()
        except BaseException:return {'attempted':False,'reason':'seal_invalid','fenced':False}
        op=SimpleNamespace(operation_id=self.p.value['operation_id'],action='canary',phase='failure_stop',
            inventory=self.p.storage,inventory_sha256=self.p.watch['storage_inventory']['sha256'],
            source_graph_sha256=digest(canonical(self.p.storage['source_graph'])),authority_sha256=self.p.authority_sha,
            journal=self.session.journal,authority=self.p.modules['interface'].freeze_authority(copy.deepcopy(self.p.authority)),
            previous={},seal=self.containment_seal,canary_policy=copy.deepcopy(self.p.watch['canary_policy']))
        try:
            self.session.journal.begin('failure_stop')
            self.session.journal.save('supervisor-phase-authority.json',{'kind':'canary_failure_stop_only_authority',
                'authority_sha256':self.p.authority_sha,'source_sha256':self.p.source_sha,'operation_id':self.p.value['operation_id'],
                'prior_failure_preserved':True,'no_mutation_authority_from_watch':True})
            self.failure_stop_attempted=True
            closure=self.backend._canary_stop_and_drain(op);_closure(closure,self.p.modules['provider_backend'].CLOSURE_FIELDS)
            return {'attempted':True,'actual_closure':closure,'fenced':True,'no_sql_hold_or_finish_attempted':True}
        except BaseException:return {'attempted':True,'fenced':False,'status':'FAILED_OR_UNKNOWN','automatic_retry':False}

    def run(self):
        need(not self.started,'supervisor_one_use');self.started=True
        try:
            need(self.stop_available and self.lookup_available and self.preservation_available,'canary_final_executor_missing')
            self.backend._validate_linux(self.p.storage['linux'])
            self.deadline=time.monotonic()+PRESERVATION_BUDGET_SECS
            op=self.op('preflight')
            _login(self.backend._sql(op,IDENTITY_SQL,timeout=2),self.p.value['operator_login'])
            baseline=self.p.watch['preservation_baseline']
            reference=self.p.watch_module.decode(_raw(baseline['path'],baseline['sha256']))
            loaded=self.backend._preservation_load_baseline(op,reference,deadline=self.deadline)
            need(type(loaded) is dict and type(loaded.get('index')) is dict
                 and loaded['index'].get('phase') == 'preservation_baseline'
                 and loaded['index'].get('operation_id') == reference.get('operation_id'),
                 'canary_original_baseline_not_authenticated')
            self.backend._artifact(op,'authenticated-preservation-baseline',{'kind':'openlegal_canary_authenticated_baseline',
                'reference':reference,'external_pointer_sha256':baseline['sha256'],
                'actual_producer_os_exit_or_install_qualification_claimed':False})
            self.baseline_reference=copy.deepcopy(reference)
            self.session.before_activation()
            self.deadline=time.monotonic()+55
            op=self.op('activate')
            before=self.backend._sql(op,"SELECT openlegal_checkpoint.observe('"+self.p.value['operation_id']+"'::uuid)",timeout=2)
            projection=self.p.modules['provider_backend']._project(before)
            need(projection['clean']and projection['recovery_hold']and projection['generation']==self.p.value['expected_held_generation']and
                 not before['state']['admissions'],'canary_activation_exact_clean_hold')
            activation=self.mutation(op);_activation(activation,self.p)
            readback=self.backend._sql(op,"SELECT openlegal_checkpoint.canary_readback('"+self.p.watch['canary_id']+"'::uuid)",timeout=2)
            need(readback=={'activation':activation,'finish':None},'canary_activation_readback_unknown')
            self.backend._artifact(op,'lifecycle-readback',readback)
            provider=self.backend._sql(op,"SELECT openlegal_checkpoint.observe('"+self.p.value['operation_id']+"'::uuid)",timeout=2)
            after=self.p.modules['provider_backend']._project(provider)
            need(not after['recovery_hold']and after['generation']==activation['released_generation']and
                 provider['state']['control']==before['state']['control'],'canary_release_readback_unknown')
            _jobs(self.backend,op,self.p.watch['canary_policy'])
            self.deadline=None
            self.session.journal.begin('observation')
            self.session.journal.save('supervisor-phase-authority.json',{'kind':'canary_readonly_observation_phase',
                'authority_sha256':self.session.binding['authority_sha256'],'source_sha256':self.p.value['watch_source_sha256']})
            self.session.after_activation()
            observation=self.session.observe()
            need(observation.get('status')=='OBSERVATION_COMPLETE_PENDING_FINAL_GATES'and
                 observation.get('elapsed_monotonic_seconds',0)>=21600,'canary_watch_not_complete')
            self.deadline=time.monotonic()+PRESERVATION_BUDGET_SECS
            op=self.op('final_stop');self.stop_may_dispatched=True;closure=self.backend._canary_stop_and_drain(op)
            _closure(closure,self.p.modules['provider_backend'].CLOSURE_FIELDS)
            self.backend._artifact(op,'returned-physical-closure',closure)
            op=self.op('final_hold')
            before=self.backend._sql(op,"SELECT openlegal_checkpoint.observe('"+self.p.value['operation_id']+"'::uuid)",timeout=2)
            projection=self.p.modules['provider_backend']._project(before)
            need(projection['clean']and not projection['recovery_hold']and projection['owner_operation_id']is None and
                 not before['state']['admissions'],'canary_final_hold_not_clean_or_owned')
            intent=self.p.authority['final_hold_intent_sha256']
            hold=self.mutation(op)
            readback=self.p.modules['provider_backend']._readback(self.backend._sql(op,
                "SELECT openlegal_checkpoint.readback('"+self.p.value['operation_id']+"'::uuid)",timeout=2),op)
            need(readback['hold']==hold and hold['owned']is True and hold['intent_sha256']==intent and
                 hold['original_generation']==projection['generation']and hold['installed_generation']!=projection['generation']and
                 readback['release']is None,'canary_final_hold_readback_unknown')
            self.backend._artifact(op,'owned-final-hold-readback',readback)
            op=self.op('preservation_final')
            preservation=_preservation(self.backend._preservation_final(op,self.baseline_reference,
                deadline=self.deadline),op,self.baseline_reference,self.p.modules)
            self.backend._artifact(op,'validated-original-preservation',preservation)
            op=self.op('finish')
            finish=self.mutation(op)
            _finished(finish,self.p,activation,hold)
            need(type(observation.get('consumed'))is int and activation['consumed']<=observation['consumed']<=finish['consumed']<=100,'canary_final_count_reset')
            lifecycle=self.backend._sql(op,"SELECT openlegal_checkpoint.canary_readback('"+self.p.watch['canary_id']+"'::uuid)",timeout=2)
            need(lifecycle=={'activation':activation,'finish':finish},'canary_finish_readback_unknown')
            self.backend._artifact(op,'lifecycle-readback',lifecycle)
            closed=self.backend._sql(op,"SELECT openlegal_checkpoint.canary_observation('"+self.p.watch['canary_id']+"'::uuid)",timeout=2)
            need(type(closed)is dict and closed.get('required_id')==self.p.watch['canary_id']and type(closed.get('canary'))is dict and
                 closed['canary']['id']==self.p.watch['canary_id']and closed['canary']['operation_id']==self.p.watch['recovery_operation_id']and
                 self.p.watch_module.timestamp(closed['canary']['ended_at'])==self.p.watch_module.timestamp(finish['closed_at'])and
                 closed['canary']['consumed']==finish['consumed'],'canary_closed_current_state_unknown')
            final_provider=self.backend._sql(op,"SELECT openlegal_checkpoint.observe('"+self.p.value['operation_id']+"'::uuid)",timeout=2)
            final_projection=self.p.modules['provider_backend']._project(final_provider)
            need(final_projection['clean']and final_projection['recovery_hold']and final_projection['owner_operation_id']==self.p.value['operation_id']and
                 final_projection['generation']==hold['installed_generation']and final_provider['state']['budget']==before['state']['budget']and
                 final_provider['state']['control']==before['state']['control']and not final_provider['state']['admissions'],
                 'canary_final_hold_current_state_unknown')
            self.backend._artifact(op,'final-current-readback',{'canary':closed,'provider':final_provider})
            op=self.op('final_lookup');self.lookup_may_dispatched=True
            held_lookup=self.backend._canary_restart_lookup_held(op,canary_id=self.p.watch['canary_id'])
            _lookup(held_lookup,self.p,activation,finish,hold)
            self.backend._artifact(op,'validated-held-lookup',held_lookup)
            result={'schema_version':1,'kind':'openlegal_canary_supervisor_lifecycle_result',
                'operation_id':self.p.value['operation_id'],'canary_id':self.p.watch['canary_id'],
                'supervisor_source_sha256':self.p.source_sha,'authority_sha256':self.p.authority_sha,
                'session_nonce':self.session.nonce,'status':'PENDING_FINAL_GATES','observation':observation,
                'final_hold_generation':hold['installed_generation'],'canary_closed_at':finish['closed_at'],
                'required_canary_kept':True,'collection_resumed_after_finish':False,'held_lookup':held_lookup,
                'preservation':preservation,
                'missing_final_executors':[gate for gate in PENDING if gate != 'fresh_full_old_preservation_subset'],
                'final_acceptance':False,'actual_supervisor_exit':'NOT_YET_OBSERVABLE'}
            self.seal();self.session.journal.save('supervisor-lifecycle-pending.json',result)
            return result
        except BaseException as error:
            self.original_error=error
            self.deadline=None
            try:containment=self.contain_once()
            except BaseException:containment={'attempted':'UNKNOWN','fenced':False,'status':'FAILED_OR_UNKNOWN'}
            self.failed=True
            try:self.session.journal.save('supervisor-failure-containment.json',containment)
            except BaseException:pass
            try:self.session.journal.save('supervisor-failed.json',{'kind':'openlegal_canary_supervisor_failure',
                'operation_id':self.p.value['operation_id'],'phase':self.phase,'status':'FAILED_OR_UNKNOWN',
                'automatic_retry':False,'automatic_rollback':False,'final_acceptance':False,
                'provider_hold_or_cleanup_not_inferred':True})
            except BaseException:pass
            raise
        finally:
            try:self.backend.close()
            except BaseException:
                try:self.session.journal.save('supervisor-backend-close-failed.json',{
                    'kind':'supervisor_backend_close_failed','operation_id':self.p.value['operation_id'],
                    'status':'FAILED_OR_UNKNOWN','metadata_fd_cleanup_failed':True,'final_acceptance':False})
                except BaseException:pass
                if self.original_error is None:raise


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('command',choices=('qualify','run-lifecycle'))
    for name in('inventory','authority'):parser.add_argument('--'+name,type=Path,required=True);parser.add_argument('--'+name+'-sha256',required=True)
    parser.add_argument('--source-sha256',required=True);args=parser.parse_args()
    p=_load(args.inventory,args.inventory_sha256,args.authority,args.authority_sha256,args.source_sha256)
    if args.command=='qualify':
        print(canonical({'kind':'openlegal_canary_supervisor_qualification','status':'SOURCE_AND_INPUTS_ONLY',
            'operation_id':p.value['operation_id'],'commands_executed':False,'session_armed':False,
            'production_acceptance_ready':False,'missing_final_executors':list(PENDING),'final_acceptance':False}).decode(),end='');return
    wi=p.value['watch_inventory'];wa=p.value['watch_authority']
    with p.watch_module.Session.arm(inventory_path=wi['path'],inventory_sha256=wi['sha256'],
            authority_path=wa['path'],authority_sha256=wa['sha256'],source_sha256=p.value['watch_source_sha256'])as session:
        result=Supervisor(p,session).run();print(canonical(result).decode(),end='')


if __name__=='__main__':
    try:main()
    except BaseException:
        print('{"status":"REFUSED_OR_UNKNOWN","final_acceptance":false}',file=sys.stderr);raise SystemExit(1)
