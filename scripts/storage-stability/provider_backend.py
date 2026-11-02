"""Fixed schema29 provider observations and checked owned-hold transitions.

Inert on import. LinuxBackend owns authenticated SQL targets, actual waited
commands/SQL closure, and its single-deadline strict WAL mutation primitive.
These helpers return bound candidate facts; checkpoint.check_hold remains the
independent final protocol gate before a phase receipt is accepted.
"""
from __future__ import annotations

import copy
import re
from interface import need, RECEIPT_KIND
from inventory import canonical, decode, digest, pin, valid_uuid

OBSERVATION_FIELDS = {'schema_version','kind','operation_id','phase','stage','projection',
                      'state','admission_ownership','hold_control'}
READBACK_FIELDS = {'hold','release','created_at','released_at'}
HOLD_RESULT_FIELDS = {'operation_id','intent_sha256','owned','original_generation',
                      'installed_generation','provider_recovery_hold','released'}
CLOSURE_FIELDS = {'cri_containers','cgroups','sql_sessions','runtime_guard_directory','runtime_guards'}
ACK_FIELDS = {'managed_epoch','blob_binding_sha256','primary_system_identifier','timeline',
              'postmaster_started_at','standby_name','standby_role','standby_addr',
              'standby_client_dn','standby_issuer_dn','standby_client_serial','sender_pid',
              'sender_started_at','prefix_lsn','flush_lsn'}


def _artifact(backend,op,name,value):
    backend._artifact(op,name,value)
    filename=op.phase+'-'+name+'.json'
    raw=canonical(value);wanted=digest(raw)
    need(op.journal.output_hashes().get(filename)==wanted,'provider_artifact_not_acknowledged')
    need(op.journal.read_phase_bytes(op.phase,filename,wanted)==raw,'provider_artifact_changed')
    return {'artifact':filename,'sha256':wanted}


def _target(op,container):
    expected=op.inventory['linux']['postgres']['primary']['container_id']
    if op.action=='manual_failover'and op.phase=='strict_resume':
        receipt=op.previous.get('reseed')
        need(type(receipt)is dict and receipt.get('kind')==RECEIPT_KIND and type(receipt.get('schema_version'))is int and receipt['schema_version']==1 and receipt.get('operation_id')==op.operation_id and receipt.get('action')=='manual_failover'and receipt.get('phase')=='reseed'and receipt.get('status')=='passed'and type(receipt.get('actual_exit_code'))is int and receipt['actual_exit_code']==0 and receipt.get('owned_processes_closed')is True and all(receipt.get(k)==v for k,v in(('inventory_sha256',op.inventory_sha256),('source_graph_sha256',op.source_graph_sha256),('authority_sha256',op.authority_sha256))),'provider_verified_reseed_target_required')
        raw=canonical(receipt)
        need(op.journal.read_phase_bytes('reseed','receipt.json',digest(raw))==raw,'provider_reseed_receipt_raw_binding')
        facts=receipt.get('facts',{});pair=facts.get('pair',{});promoted=facts.get('promoted',{})
        need(type(pair)is dict and type(pair.get('primary'))is dict and type(promoted)is dict,'provider_reseed_target_shape')
        expected=pair['primary'].get('container_id')
        need(expected==promoted.get('promoted_container_id')and facts.get('new_epoch_bound')is True,'provider_reseed_promoted_target')
    value=expected if container is None else container
    need(type(value)is str and re.fullmatch('[0-9a-f]{64}',value)and value==expected,'provider_target_identity')
    return value


def _query(op,observation,stage):
    raw=op.journal.read_phase_bytes(op.phase,observation['query_artifact'],observation['query_sha256'])
    value=decode(raw)
    fields={'schema_version','kind','operation_id','phase','stage','target_container','result'}
    need(type(value)is dict and set(value)==fields and type(value['schema_version'])is int and value['schema_version']==1 and value['kind']=='openlegal_checkpoint_provider_query_observation'and value['operation_id']==op.operation_id and value['phase']==op.phase and value['target_container']==_target(op,observation['target_container']),'provider_query_target_raw_binding')
    need(value['stage']==stage or op.phase=='qualify'and stage=='after'and value['stage']=='before','provider_query_stage_binding')
    return value


def _project(value):
    need(type(value)is dict and set(value)=={'state','hold_control','admission_ownership','checkpoint_owner','checkpoint_readback'},'provider_query_shape')
    state=value['state'];control=value['hold_control'];ownership=value['admission_ownership']
    need(type(state)is dict and set(state)=={'budget','control','admissions','canary_control','canaries'}and type(state['budget'])is dict and type(state['control'])is dict and type(control)is dict and {'generation','recovery_hold','hold_started_at'}<=set(control),'provider_state_shape')
    need(valid_uuid(control['generation'])and type(control['recovery_hold'])is bool and state['control']=={k:v for k,v in control.items()if k not in('generation','recovery_hold','hold_started_at')},'provider_control_projection')
    rows=state['admissions'];need(type(rows)is list and len(rows)<=16 and type(ownership)is dict and set(ownership)=={'legacy_locked','slots'}and type(ownership['legacy_locked'])is bool and type(ownership['slots'])is list and len(ownership['slots'])==len(rows),'provider_lock_shape')
    indexed={};owners=set()
    for row in rows:
        need(type(row)is dict and {'owner','slot'}<=set(row)and valid_uuid(row['owner'])and type(row['slot'])is int and 1<=row['slot']<=16 and row['slot']not in indexed and row['owner']not in owners,'provider_ledger_identity')
        indexed[row['slot']]=row['owner'];owners.add(row['owner'])
    seen=set()
    for row in ownership['slots']:
        need(type(row)is dict and set(row)=={'owner','slot','locked'}and type(row['slot'])is int and row['slot']not in seen and indexed.get(row['slot'])==row['owner']and type(row['locked'])is bool,'provider_ledger_lock_binding');seen.add(row['slot'])
    budget=state['budget'];need(type(budget.get('unresolved_response'))is bool and 'admission_owner'in budget and(budget['admission_owner']is None or valid_uuid(budget['admission_owner'])),'provider_uncertainty_shape')
    owner=value['checkpoint_owner'];need(owner is None or valid_uuid(owner),'provider_checkpoint_owner')
    need(control['recovery_hold']or owner is None,'provider_owner_without_hold')
    return {'generation':control['generation'],'owner_operation_id':owner,
            'clean':not budget['unresolved_response']and budget['admission_owner']is None and not ownership['legacy_locked']and all(r['locked']for r in ownership['slots']),
            'recovery_hold':control['recovery_hold'],'accounting_sha256':digest(canonical(state))}


def _loaded(op,observation,stage=None):
    fields={'wrapper','artifact','sha256','projection','query_artifact','query_sha256','checkpoint_readback','target_container'}
    need(type(observation)is dict and set(observation)==fields and pin(observation['sha256'])and pin(observation['query_sha256']),'provider_observation_port')
    wrapper=observation['wrapper'];need(type(wrapper)is dict and set(wrapper)==OBSERVATION_FIELDS and type(wrapper['schema_version'])is int and wrapper['schema_version']==1 and wrapper['kind']=='openlegal_checkpoint_provider_observation'and wrapper['operation_id']==op.operation_id and wrapper['phase']==op.phase and wrapper['stage']in('before','after')and(stage is None or wrapper['stage']==stage),'provider_observation_identity')
    need(op.journal.read_phase_bytes(op.phase,observation['artifact'],observation['sha256'])==canonical(wrapper),'provider_observation_raw_binding')
    source=_query(op,observation,wrapper['stage'])['result']
    need(wrapper['projection']==observation['projection']==_project(source)and wrapper['state']==source['state']and wrapper['hold_control']==source['hold_control']and wrapper['admission_ownership']==source['admission_ownership']and observation['checkpoint_readback']==source['checkpoint_readback'],'provider_observation_semantics')
    return observation


def observe(backend,op,stage,*,container=None,baseline=None):
    """One protected live SQL snapshot; qualify may truthfully reuse it once."""
    need(stage in('before','after')and valid_uuid(op.operation_id),'provider_observation_stage')
    target=_target(op,container)
    if baseline is not None:
        need(op.phase=='qualify'and stage=='after','provider_baseline_reuse_scope')
        old=_loaded(op,baseline,'before');need(old['target_container']==target,'provider_baseline_target')
        source=_query(op,old,'before')['result']
        query={'artifact':old['query_artifact'],'sha256':old['query_sha256']}
    else:
        # The schema29 SECURITY DEFINER function owns the fixed lock order and
        # fresh state/slot snapshot. The login needs named EXECUTE only, never
        # direct table/slot-function privileges or a superuser fallback.
        sql=f"SELECT openlegal_checkpoint.observe('{op.operation_id}'::uuid)"
        source=backend._sql(op,sql,container=target,timeout=2)
        _project(source)
        query=_artifact(backend,op,'provider-query-'+stage,{'schema_version':1,'kind':'openlegal_checkpoint_provider_query_observation','operation_id':op.operation_id,'phase':op.phase,'stage':stage,'target_container':target,'result':source})
    projection=_project(source)
    wrapper={'schema_version':1,'kind':'openlegal_checkpoint_provider_observation',
             'operation_id':op.operation_id,'phase':op.phase,'stage':stage,'projection':projection,
             'state':source['state'],'admission_ownership':source['admission_ownership'],'hold_control':source['hold_control']}
    ref=_artifact(backend,op,'provider-state-'+stage,wrapper)
    result=dict(ref,wrapper=copy.deepcopy(wrapper),projection=copy.deepcopy(projection),
                query_artifact=query['artifact'],query_sha256=query['sha256'],
                checkpoint_readback=copy.deepcopy(source['checkpoint_readback']),target_container=target)
    if baseline is not None:
        _artifact(backend,op,'provider-single-snapshot',{'schema_version':1,
            'kind':'openlegal_checkpoint_qualification_single_actual_snapshot','operation_id':op.operation_id,
            'before_sha256':baseline['sha256'],'after_sha256':result['sha256'],
            'actual_query_sha256':query['sha256'],'independent_second_query_claimed':False})
    return result


def _readback(value,op):
    need(type(value)is dict and set(value)==READBACK_FIELDS,'provider_checkpoint_readback_shape')
    hold=value['hold'];need(type(hold)is dict and set(hold)==HOLD_RESULT_FIELDS and hold['operation_id']==op.operation_id and pin(hold['intent_sha256'])and type(hold['owned'])is bool and valid_uuid(hold['original_generation'])and valid_uuid(hold['installed_generation'])and hold['provider_recovery_hold']is True and hold['released']is False and value['created_at']is not None,'provider_checkpoint_hold_identity')
    if value['release']is None:need(value['released_at']is None,'provider_checkpoint_release_time')
    else:
        released=value['release'];need(type(released)is dict and set(released)==HOLD_RESULT_FIELDS|{'released_generation'}and valid_uuid(released['released_generation'])and released==dict(hold,released=True,provider_recovery_hold=False,released_generation=released['released_generation'])and value['released_at']is not None,'provider_checkpoint_release_identity')
    return value


def _owned_history(op):
    found=[]
    for receipt in op.previous.values():
        facts=receipt.get('facts',{})
        if facts.get('hold_created_by_operation')is not True:continue
        value=_readback(facts.get('provider_checkpoint_readback'),op)
        name=facts.get('provider_checkpoint_readback_artifact');need(type(name)is str and name in receipt['artifacts'],'provider_history_artifact')
        need(op.journal.read_phase_bytes(receipt['phase'],name,receipt['artifacts'][name])==canonical(value),'provider_history_raw_binding')
        need(value['hold']['owned']is True,'provider_history_unowned')
        found.append(value)
    if found:need(all(v['hold']==found[0]['hold']for v in found),'provider_history_hold_changed')
    return found


def _authority(op,release=False):
    need(op.authority.get('approved',False)is True and op.action in op.authority.get('actions',()),'provider_operation_not_authorized')
    if release:need(op.authority.get('release_owned_clean_hold_permitted',False)is True,'provider_release_not_authorized')


def _ack(backend,op,old_outputs):
    outputs=op.journal.output_hashes()
    names=[n for n in outputs if n not in old_outputs and n.startswith(op.phase+'-operator-mutation-ack-')and n.endswith('.json')]
    need(len(names)==1,'provider_actual_wal_ack_missing')
    name=names[0];value=decode(op.journal.read_phase_bytes(op.phase,name,outputs[name]))
    need(type(value)is dict and set(value)=={'initial','before','after'},'provider_wal_ack_shape')
    def lsn(v):
        need(type(v)is str and re.fullmatch('[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}',v),'provider_wal_lsn');a,b=v.split('/');return int(a,16)<<32|int(b,16)
    for row in value.values():
        need(type(row)is dict and set(row)==ACK_FIELDS and valid_uuid(row['managed_epoch'])and pin(row['blob_binding_sha256'])and type(row['sender_pid'])is int and 0<row['sender_pid']<=2147483647 and type(row['timeline'])is int and 0<row['timeline']<=4294967295 and all(type(row[k])is str and 0<len(row[k])<=1024 for k in ACK_FIELDS-{'timeline','sender_pid'}),'provider_wal_receipt_identity')
        need(re.fullmatch('[0-9]{1,20}',row['primary_system_identifier'])and re.fullmatch('[0-9]{1,60}',row['standby_client_serial'])and int(row['standby_client_serial'])>0,'provider_wal_tls_serial');lsn(row['prefix_lsn']);lsn(row['flush_lsn'])
    initial,before,after=(value[k]for k in('initial','before','after'))
    static=ACK_FIELDS-{'prefix_lsn','flush_lsn'}
    need(all(initial[k]==before[k]==after[k]for k in static)and lsn(before['prefix_lsn'])>=lsn(initial['prefix_lsn'])and lsn(after['flush_lsn'])>=lsn(before['prefix_lsn']),'provider_wal_ack_unproved')
    return {'artifact':name,'sha256':outputs[name]}


def hold(backend,op,before,*,container=None):
    _authority(op);old=_loaded(op,before,'before');need(old['target_container']==_target(op,container),'provider_mutation_target')
    need(op.phase=='drain_writers'or op.phase=='fence_old'and op.action=='manual_failover','provider_hold_phase')
    projection=old['projection'];history=_owned_history(op)
    if projection['recovery_hold']:
        need(projection['owner_operation_id']!=op.operation_id or history,'provider_failed_operation_not_adopted')
        ref=_artifact(backend,op,'provider-preserved-hold',{'kind':'preserved_existing_hold','before_sha256':old['sha256'],'no_mutation':True})
        return dict(ref,kind='preserved_existing_hold',created=bool(history),before_sha256=old['sha256'],readback=old['checkpoint_readback'],wal_ack=None)
    need(projection['clean']and projection['owner_operation_id']is None and not old['wrapper']['state']['admissions'],'provider_hold_requires_clean_drain')
    intent={'kind':'openlegal_checkpoint_provider_hold_intent','operation_id':op.operation_id,'phase':op.phase,
            'inventory_sha256':op.inventory_sha256,'source_graph_sha256':op.source_graph_sha256,
            'authority_sha256':op.authority_sha256,'before_sha256':old['sha256'],'target_container':old['target_container']}
    ref=_artifact(backend,op,'provider-mutation-intent',intent);outputs=op.journal.output_hashes()
    result=backend._sql_mutation(op,f"SELECT openlegal_checkpoint.hold('{op.operation_id}'::uuid,'{ref['sha256']}')",container=old['target_container'])
    ack=_ack(backend,op,outputs)
    value=_readback(backend._sql(op,f"SELECT openlegal_checkpoint.readback('{op.operation_id}'::uuid)",container=old['target_container'],timeout=2),op)
    need(type(result)is dict and set(result)==HOLD_RESULT_FIELDS and value['hold']==result and result['owned']is True and result['intent_sha256']==ref['sha256']and result['original_generation']==projection['generation']and result['installed_generation']!=projection['generation']and value['release']is None,'provider_hold_actual_readback')
    recorded=_artifact(backend,op,'provider-checkpoint-readback',value)
    return dict(recorded,kind='checked_owned_hold',created=True,before_sha256=old['sha256'],readback=value,wal_ack=ack)


def release(backend,op,before,*,container=None):
    _authority(op,release=True);old=_loaded(op,before,'before');need(old['target_container']==_target(op,container),'provider_mutation_target')
    need(op.phase in('resume_owned_hold','strict_resume'),'provider_release_phase')
    history=_owned_history(op);need(history and all(v['release']is None for v in history),'provider_release_requires_owned_history')
    original=history[0]['hold'];projection=old['projection'];current=_readback(old['checkpoint_readback'],op)
    need(current['hold']==original and current['release']is None and projection['clean']and projection['recovery_hold']and projection['owner_operation_id']==op.operation_id and projection['generation']==original['installed_generation']and not old['wrapper']['state']['admissions'],'provider_release_current_owner')
    origin=next(r for r in op.previous.values()if r.get('facts',{}).get('hold_created_by_operation')is True)
    need(projection['accounting_sha256']==origin['facts']['provider_hold_after']['accounting_sha256'],'provider_release_accounting_changed')
    _artifact(backend,op,'provider-mutation-intent',{'kind':'openlegal_checkpoint_provider_release_intent',
        'operation_id':op.operation_id,'phase':op.phase,'authority_sha256':op.authority_sha256,
        'before_sha256':old['sha256'],'original_hold_intent_sha256':original['intent_sha256']})
    outputs=op.journal.output_hashes()
    result=backend._sql_mutation(op,f"SELECT openlegal_checkpoint.release('{op.operation_id}'::uuid,'{original['intent_sha256']}')",container=old['target_container'])
    ack=_ack(backend,op,outputs)
    value=_readback(backend._sql(op,f"SELECT openlegal_checkpoint.readback('{op.operation_id}'::uuid)",container=old['target_container'],timeout=2),op)
    need(type(result)is dict and set(result)==HOLD_RESULT_FIELDS|{'released_generation'}and value['hold']==original and value['release']==result and result['released_generation']not in(original['original_generation'],original['installed_generation']),'provider_release_actual_readback')
    recorded=_artifact(backend,op,'provider-checkpoint-readback',value)
    return dict(recorded,kind='checked_owned_release',created=True,before_sha256=old['sha256'],readback=value,wal_ack=ack)


def phase_facts(backend,op,before,after,*,closure=None,mutation=None,collectors_resumed=False):
    a=_loaded(op,before,'before');b=_loaded(op,after,'after')
    need(a['target_container']==b['target_container']and type(collectors_resumed)is bool,'provider_phase_target')
    history=_owned_history(op);created=bool(history)
    facts={'provider_hold_before':a['projection'],'provider_hold_after':b['projection'],
           'provider_state_before_artifact':a['artifact'],'provider_state_after_artifact':b['artifact'],
           'hold_created_by_operation':created,'collectors_resumed':collectors_resumed,
           'provider_accounting_preserved':True}
    if mutation is not None:
        need(type(mutation)is dict and mutation.get('before_sha256')==a['sha256']and type(mutation.get('created'))is bool and pin(mutation.get('sha256')),'provider_mutation_port')
        raw=op.journal.read_phase_bytes(op.phase,mutation['artifact'],mutation['sha256'])
        need(mutation.get('kind')in('checked_owned_hold','checked_owned_release','preserved_existing_hold'),'provider_mutation_kind')
        if mutation['kind']!='preserved_existing_hold':
            value=_readback(decode(raw),op);need(value==mutation['readback']and value==b['checkpoint_readback']and mutation['wal_ack']is not None,'provider_mutation_after_binding')
            facts.update(provider_checkpoint_readback=value,provider_checkpoint_readback_artifact=mutation['artifact'],provider_mutation_wal_ack_artifact=mutation['wal_ack']['artifact'])
        elif mutation['created']:
            need(history,'provider_preserved_ownership_history');value=_readback(b['checkpoint_readback'],op);need(value['hold']==history[0]['hold'],'provider_preserved_owned_hold_changed')
            ref=_artifact(backend,op,'provider-checkpoint-readback',value);facts.update(provider_checkpoint_readback=value,provider_checkpoint_readback_artifact=ref['artifact'])
        facts['hold_created_by_operation']=mutation['created']
    elif created:
        value=_readback(b['checkpoint_readback'],op);ref=_artifact(backend,op,'provider-checkpoint-readback',value)
        facts.update(provider_checkpoint_readback=value,provider_checkpoint_readback_artifact=ref['artifact'])
    if closure is not None:
        need(type(closure)is dict and set(closure)==CLOSURE_FIELDS and op.phase in('stop','drain_writers','fence_old'),'provider_closure_port')
        envelope=dict(copy.deepcopy(closure),schema_version=1,kind='openlegal_checkpoint_provider_settlement_closure',operation_id=op.operation_id,phase=op.phase,provider_state_after_sha256=b['sha256'])
        need(envelope['sql_sessions']==[]and type(envelope['cri_containers'])is list and all(r.get('state')=='CONTAINER_EXITED'for r in envelope['cri_containers']),'provider_closure_processes')
        need(type(envelope['cgroups'])is list and envelope['cgroups']and all(r.get('pids')==[]for r in envelope['cgroups']),'provider_closure_cgroups')
        need(type(envelope['runtime_guards'])is list and all(r.get('state')=='closed_clean'and r.get('locked')is False for r in envelope['runtime_guards']),'provider_closure_unclean_guard')
        ref=_artifact(backend,op,'provider-settlement-closure',envelope);facts['provider_settlement_closure_artifact']=ref['artifact']
    if collectors_resumed:
        need(op.phase in('resume_owned_hold','strict_resume')and op.authority.get('collection_resume_permitted',False)is True and b['projection']['clean']and not b['projection']['recovery_hold']and b['projection']['owner_operation_id']is None,'provider_collection_not_authorized')
    return facts
