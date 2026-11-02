#!/usr/bin/env python3
"""Explicit, source-pinned checkpoints and manual failover; never run on import.

All host effects belong to the reviewed fixed-action LinuxBackend. A checkpoint
never invokes manual failover. Failed or interrupted operations are one-use and
cannot be reopened, retried, relabelled or used as success authority.
"""
import argparse
import copy
import datetime
import hashlib
import importlib.util
import json
import os
import re
import stat
import sys
import time
import uuid
from pathlib import Path

INSTALL_ROOT=Path('/usr/local/lib/openlegal-storage-stability')
FILES={'interface.py','inventory.py','journal.py','copy_tree.py','retention.py','checkpoint.py','linux_backend.py','restore_backend.py','manual_backend.py','provider_backend.py','bootstrap_backend.py','command_process.py','canary_native.py','preservation_backend.py','preservation_capture.py'}

# Bootstrap stays stdlib-only until every sibling's actual source bytes match.
def bootstrap_file(path,pin,limit=4*1024*1024):
    path=Path(path)
    if not path.is_absolute()or path.resolve()!=path:raise RuntimeError('bootstrap_path')
    for parent in path.parents:
        s=parent.lstat()
        if not stat.S_ISDIR(s.st_mode)or s.st_uid!=0 or s.st_mode&0o022 and not(parent==Path('/tmp')and s.st_mode&stat.S_ISVTX):raise RuntimeError('bootstrap_ancestor')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
    with os.fdopen(fd,'rb')as stream:
        a=os.fstat(stream.fileno())
        if not stat.S_ISREG(a.st_mode)or a.st_uid!=0 or a.st_nlink!=1 or stat.S_IMODE(a.st_mode)!=0o600 or a.st_size>limit:raise RuntimeError('bootstrap_file')
        raw=stream.read(limit+1);z=os.fstat(stream.fileno());current=path.lstat()
    if len(raw)!=a.st_size or not all(getattr(a,k)==getattr(z,k)==getattr(current,k)for k in('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns'))or hashlib.sha256(raw).hexdigest()!=pin:raise RuntimeError('bootstrap_changed')
    return raw

def modules(raw_inventory,base=INSTALL_ROOT):
    def pairs(rows):
        out={}
        for k,v in rows:
            if k in out:raise RuntimeError('bootstrap_duplicate')
            out[k]=v
        return out
    value=json.loads(raw_inventory,object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in()).throw(RuntimeError('bootstrap_nonfinite')))
    graph=value['source_graph']
    if type(graph)is not dict or set(graph)!=FILES:raise RuntimeError('bootstrap_graph')
    sources={name:bootstrap_file(base/name,pin,1024*1024)for name,pin in graph.items()}
    result={}
    for name in ('interface','inventory','journal','copy_tree','retention','linux_backend','restore_backend','manual_backend','provider_backend','bootstrap_backend','command_process','canary_native','preservation_backend','preservation_capture'):
        path=base/(name+'.py');spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);sys.modules[name]=module
        exec(compile(sources[name+'.py'],str(path),'exec'),module.__dict__);result[name]=module
    return result

def validate_receipt(module,op,receipt):
    i=module['interface'];need=i.need;p=module['inventory']
    expected={'schema_version':1,'kind':i.RECEIPT_KIND,'operation_id':op.operation_id,'action':op.action,'phase':op.phase,'inventory_sha256':op.inventory_sha256,'source_graph_sha256':op.source_graph_sha256,'authority_sha256':op.authority_sha256,'status':'passed','actual_exit_code':0,'owned_processes_closed':True}
    need(type(receipt)is dict and set(receipt)==i.RECEIPT_FIELDS and all(receipt[k]==v for k,v in expected.items())and type(receipt['schema_version'])is int and type(receipt['actual_exit_code'])is int and receipt['owned_processes_closed']is True,'backend_receipt_identity')
    need(type(receipt['facts'])is dict and all(receipt['facts'].get(k)is True for k in i.REQUIRED_TRUE[op.phase]),'backend_receipt_facts')
    outputs=op.journal.output_hashes();artifacts=receipt['artifacts']
    need(type(artifacts)is dict and artifacts and len(artifacts)<=4096 and all(type(n)is str and n in outputs and p.pin(pin)and outputs[n]==pin for n,pin in artifacts.items()),'backend_artifacts_unbound')
    if op.phase in('export_snapshots','verify_isolated_restore'):
        need(receipt['facts'].get('parent_chains')==[],'incremental_export_not_supported')
    if op.phase=='verify_isolated_restore':
        need(receipt['facts'].get('export_receipt_sha256')==p.digest(p.canonical(op.previous['export_snapshots'])),'restore_full_export_receipt_binding')
    if op.phase=='snapshot_same_pool':
        expected_datasets={d['name']:d['guid']for d in p.all_datasets(op.inventory)}
        snapshots=receipt['facts'].get('snapshots');need(type(snapshots)is list and len(snapshots)==len(expected_datasets),'snapshot_all_original_and_active')
        need(len({s['dataset']for s in snapshots})==len(expected_datasets),'snapshot_unique')
        groups=set()
        for s in snapshots:
            need(type(s)is dict and set(s)=={'dataset','guid','snapshot','snapshot_guid'}and expected_datasets.get(s['dataset'])==s['guid']and re.fullmatch('[0-9]{1,20}',s['snapshot_guid'])is not None,'snapshot_original_guid')
            need(s['snapshot'].startswith(s['dataset']+'@openlegal-checkpoint-')and op.operation_id.replace('-','')[:8]in s['snapshot'],'snapshot_new_namespace')
            groups.add(s['snapshot'].split('@',1)[1])
        need(len({s['snapshot_guid']for s in snapshots})==len(snapshots),'snapshot_guid_unique')
        need(len(groups)==1 and p.pin(receipt['facts'].get('index_sha256'))and re.fullmatch('[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}',receipt['facts'].get('latest_wal_lsn',''))is not None,'coherent_snapshot_group')
    return receipt

def provider_observation(module,op,receipt,which):
    p=module['inventory'];need=module['interface'].need;name=receipt['facts'].get('provider_state_'+which+'_artifact')
    need(type(name)is str and name in receipt['artifacts'],'provider_state_artifact_binding')
    raw=op.journal.read_phase_bytes(receipt['phase'],name,receipt['artifacts'][name]);v=p.decode(raw)
    fields={'schema_version','kind','operation_id','phase','stage','projection','state','admission_ownership','hold_control'}
    need(type(v)is dict and set(v)==fields and type(v['schema_version'])is int and v['schema_version']==1 and v['kind']=='openlegal_checkpoint_provider_observation'and v['operation_id']==op.operation_id and v['phase']==receipt['phase']and v['stage']==which and v['projection']==receipt['facts']['provider_hold_'+which],'provider_observation_identity')
    state=v['state'];control=v['hold_control'];owners=v['admission_ownership']
    need(type(state)is dict and set(state)=={'budget','control','admissions','canary_control','canaries'}and type(state['budget'])is dict and type(state['control'])is dict and type(control)is dict and {'generation','recovery_hold','hold_started_at'}<=set(control),'provider_state_shape')
    need(state['control']=={k:x for k,x in control.items()if k not in('generation','recovery_hold','hold_started_at')}and v['projection']['generation']==control['generation']and type(control['recovery_hold'])is bool and v['projection']['recovery_hold']==control['recovery_hold']and v['projection']['accounting_sha256']==p.digest(p.canonical(state)),'provider_state_projection')
    rows=state['admissions'];need(type(rows)is list and len(rows)<=16 and type(owners)is dict and set(owners)=={'legacy_locked','slots'}and type(owners['legacy_locked'])is bool and type(owners['slots'])is list and len(owners['slots'])==len(rows),'provider_ownership_shape')
    indexed={}
    for row in rows:
        need(type(row)is dict and {'owner','slot'}<=set(row)and p.valid_uuid(row['owner'])and type(row['slot'])is int and 1<=row['slot']<=16 and row['slot']not in indexed,'provider_admission_identity');indexed[row['slot']]=row['owner']
    seen=set()
    for row in owners['slots']:
        need(type(row)is dict and set(row)=={'owner','slot','locked'}and type(row['slot'])is int and row['slot']not in seen and indexed.get(row['slot'])==row['owner']and type(row['locked'])is bool,'provider_slot_identity');seen.add(row['slot'])
    budget=state['budget'];need(type(budget.get('unresolved_response'))is bool and 'admission_owner'in budget,'provider_budget_uncertainty')
    clean=not budget['unresolved_response']and budget['admission_owner']is None and not owners['legacy_locked']and all(r['locked']for r in owners['slots'])
    need(v['projection']['clean']==clean,'provider_clean_projection')
    return v

def provider_closure(module,op,receipt,after):
    p=module['inventory'];need=module['interface'].need;name=receipt['facts'].get('provider_settlement_closure_artifact')
    need(type(name)is str and name in receipt['artifacts'],'provider_settlement_closure_missing')
    value=p.decode(op.journal.read_phase_bytes(op.phase,name,receipt['artifacts'][name]))
    fields={'schema_version','kind','operation_id','phase','provider_state_after_sha256','cri_containers','cgroups','sql_sessions','runtime_guard_directory','runtime_guards'}
    need(type(value)is dict and set(value)==fields and type(value['schema_version'])is int and value['schema_version']==1 and value['kind']=='openlegal_checkpoint_provider_settlement_closure'and value['operation_id']==op.operation_id and value['phase']==op.phase and (op.phase in('stop','drain_writers')or op.phase=='fence_old'and op.action=='manual_failover')and value['provider_state_after_sha256']==receipt['artifacts'][receipt['facts']['provider_state_after_artifact']],'provider_settlement_closure_identity')
    need(value['sql_sessions']==[]and type(value['cri_containers'])is list and len(value['cri_containers'])<=4096 and all(type(r)is dict and r.get('state')=='CONTAINER_EXITED'and type(r.get('id'))is str and r['id']for r in value['cri_containers']),'provider_settlement_process_or_sql')
    need(type(value['cgroups'])is list and 0<len(value['cgroups'])<=4096,'provider_settlement_cgroups')
    for row in value['cgroups']:
        need(type(row)is dict and set(row)=={'path','dev','ino','pids'}and type(row['path'])is str and row['path'].startswith('/')and all(type(row[k])is int and row[k]>=0 for k in('dev','ino'))and row['pids']==[],'provider_settlement_cgroup_populated')
    directory=value['runtime_guard_directory'];need(type(directory)is dict and set(directory)=={'path','dev','ino'}and type(directory['path'])is str and directory['path'].startswith('/')and all(type(directory[k])is int and directory[k]>=0 for k in('dev','ino')),'provider_settlement_guard_directory')
    records=value['runtime_guards'];need(type(records)is list and len(records)<=512,'provider_settlement_guard_bound');names=set()
    for row in records:
        need(type(row)is dict and set(row)=={'name','sha256','state','locked'}and type(row['name'])is str and row['name']not in names and '/'not in row['name']and row['name'].endswith('.json')and p.pin(row['sha256'])and row['state']=='closed_clean'and row['locked']is False,'provider_settlement_guard_unclean');names.add(row['name'])

def provider_progress(module,op,receipt,before,after,terminal=None):
    """Only actual charged settlement after source-reviewed physical closure."""
    need=module['interface'].need
    same_hold=all(before['projection'][k]==after['projection'][k]for k in('generation','owner_operation_id','recovery_hold'))
    if same_hold:need(before['hold_control']==after['hold_control'],'provider_hold_control_metadata_changed')
    if before['state']==after['state']and before['admission_ownership']==after['admission_ownership']:return
    need((op.phase in('stop','drain_writers')or op.phase=='fence_old'and op.action=='manual_failover'),'provider_progress_phase')
    need(before['projection']['clean']and not before['projection']['recovery_hold']and before['projection']['owner_operation_id']is None,'provider_unknown_or_foreign_progress')
    need(after['projection']['clean'],'provider_progress_became_uncertain')
    terminal=after if terminal is None else terminal
    provider_closure(module,op,receipt,terminal)
    a=before['state'];b=after['state'];need(a['control']==b['control']and a['canary_control']==b['canary_control'],'provider_policy_changed')
    old=a['budget'];new=b['budget'];need(set(old)==set(new)and old.get('utc_day')==new.get('utc_day')and type(old.get('utc_day'))is int,'provider_day_rollover')
    monotonic={'daily_used','on_demand_used','pilot_used','next_request_at_ms','next_allowed_at','collection_scheduler_seen_at'}
    need({'daily_used','on_demand_used','pilot_used','next_request_at_ms','next_allowed_at'}<=set(old),'provider_counter_shape')
    for key in old:
        if key in monotonic:
            need(type(old[key])is int and type(new[key])is int and 0<=old[key]<=new[key]<=9223372036854775807,'provider_counter_underflow')
        elif key=='last_admission_mode':
            need(new[key]in(None,'continuous','on_demand')and(old[key]==new[key]or any(new[k]>old[k]for k in('daily_used','on_demand_used','pilot_used'))),'provider_admission_mode')
        else:need(old[key]==new[key],'provider_budget_policy_or_unknown_changed')
    need(type(a['canaries'])is list and type(b['canaries'])is list and len(a['canaries'])==len(b['canaries'])<=1,'provider_canary_shape')
    def stamp(value):
        try:
            parsed=datetime.datetime.fromisoformat(value.replace('Z','+00:00'));need(parsed.tzinfo is not None,'provider_canary_timezone');return parsed
        except (TypeError,ValueError,AttributeError):raise module['interface'].Refused('provider_canary_clock')from None
    for x,y in zip(a['canaries'],b['canaries']):
        need(type(x)is dict and type(y)is dict and set(x)==set(y)and {'consumed','last_observed_at','deadline'}<=set(x),'provider_canary_fields')
        need(type(x['consumed'])is int and type(y['consumed'])is int and 0<=x['consumed']<=y['consumed']<=100 and stamp(x['last_observed_at'])<=stamp(y['last_observed_at'])<=stamp(x['deadline']),'provider_canary_counter_or_deadline')
        need(all(x[k]==y[k]for k in x if k not in('consumed','last_observed_at')),'provider_canary_policy_changed')
    # Physical closure is essential: only then may formerly LOCKED owners settle.
    need(not terminal['state']['admissions']and not terminal['admission_ownership']['slots']and not terminal['admission_ownership']['legacy_locked'],'provider_settlement_admissions_remain')

def check_hold(module,op,receipt,authority):
    """Chain fresh live projections; no initial-count reuse or unknown cleanup."""
    i=module['interface'];p=module['inventory'];need=i.need;facts=receipt['facts']
    need(op.phase!='fence_old'or op.action=='manual_failover','manual_fence_hold_action')
    before=facts.get('provider_hold_before');after=facts.get('provider_hold_after')
    def shape(value):
        return type(value)is dict and set(value)==i.HOLD_FIELDS and p.valid_uuid(value['generation']) and(value['owner_operation_id']is None or p.valid_uuid(value['owner_operation_id']))and type(value['clean'])is bool and type(value['recovery_hold'])is bool and p.pin(value['accounting_sha256'])
    need(shape(before)and shape(after),'provider_hold_projection')
    for key in ('provider_state_before_artifact','provider_state_after_artifact'):
        name=facts.get(key);need(type(name)is str and name in receipt['artifacts'],'provider_state_artifact_binding')
    raw_before=provider_observation(module,op,receipt,'before');raw_after=provider_observation(module,op,receipt,'after')
    if (op.phase in('stop','drain_writers')or op.phase=='fence_old'and op.action=='manual_failover'):provider_closure(module,op,receipt,raw_after)
    previous=[r for r in op.previous.values()if r.get('phase')in i.HOLD_PHASES]
    if op.phase=='qualify':
        need(not previous and before==after and raw_before['state']==raw_after['state']and raw_before['admission_ownership']==raw_after['admission_ownership'],'provider_fresh_qualification')
    else:
        need(previous,'provider_hold_before_binding')
        previous_after=previous[-1]['facts']['provider_hold_after'];prior_raw=provider_observation(module,op,previous[-1],'after')
        need(all(before[k]==previous_after[k]for k in i.HOLD_FIELDS if k!='accounting_sha256'),'provider_hold_before_binding')
        provider_progress(module,op,receipt,prior_raw,raw_before,raw_after)
    created=facts.get('hold_created_by_operation');resumed=facts.get('collectors_resumed')
    need(type(created)is bool and type(resumed)is bool,'provider_hold_boolean')
    owned=[r for r in previous if r['facts'].get('hold_created_by_operation')is True]
    need(not resumed or op.phase in('resume_owned_hold','strict_resume')and after['clean']and not after['recovery_hold']and after['owner_operation_id']is None and authority['collection_resume_permitted'],'provider_existing_hold_resume')
    provider_progress(module,op,receipt,raw_before,raw_after)
    hold_changed=any(before[k]!=after[k]for k in i.HOLD_FIELDS if k!='accounting_sha256')
    if not hold_changed:
        need(created==bool(owned),'provider_hold_ownership_chain')
        return
    need(before['accounting_sha256']==after['accounting_sha256'],'provider_accounting_changed_during_hold_change')
    readback=facts.get('provider_checkpoint_readback');name=facts.get('provider_checkpoint_readback_artifact')
    need(type(readback)is dict and set(readback)=={'hold','release','created_at','released_at'}and type(name)is str and receipt['artifacts'].get(name)==p.digest(p.canonical(readback)),'provider_checkpoint_readback_binding')
    hold=readback['hold'];hold_fields={'operation_id','intent_sha256','owned','original_generation','installed_generation','provider_recovery_hold','released'}
    need(type(hold)is dict and set(hold)==hold_fields and hold['operation_id']==op.operation_id and p.pin(hold['intent_sha256'])and hold['owned']is True and hold['provider_recovery_hold']is True and hold['released']is False,'provider_checkpoint_owned_hold')
    need(readback['created_at']is not None,'provider_checkpoint_creation_time')
    if (op.phase=='drain_writers'or op.phase=='fence_old'and op.action=='manual_failover')and not owned:
        need(created and before['clean']and not before['recovery_hold']and before['owner_operation_id']is None and after['clean']and after['recovery_hold']and after['owner_operation_id']==op.operation_id,'provider_foreign_or_uncertain_hold_changed')
        need(hold['original_generation']==before['generation']and hold['installed_generation']==after['generation']and before['generation']!=after['generation']and readback['release']is None and readback['released_at']is None,'provider_hold_install_generation')
        return
    need(op.phase in('resume_owned_hold','strict_resume')and created and owned,'provider_unowned_hold_release')
    need(authority['release_owned_clean_hold_permitted'],'provider_hold_release_scope')
    need(before['clean']and before['recovery_hold']and before['owner_operation_id']==op.operation_id and after['clean']and not after['recovery_hold']and after['owner_operation_id']is None,'provider_hold_release_shape')
    origin=owned[0]['facts'];need(hold==origin['provider_checkpoint_readback']['hold']and hold['installed_generation']==before['generation'],'provider_hold_original_operation')
    release=readback['release'];need(type(release)is dict and set(release)==hold_fields|{'released_generation'}and release==dict(hold,released=True,provider_recovery_hold=False,released_generation=after['generation'])and after['generation']not in(before['generation'],hold['original_generation'])and readback['released_at']is not None,'provider_hold_release_generation')
    if op.action=='checkpoint':need('drain_writers'in op.previous,'provider_release_requires_drain')
    else:need('fence_old'in op.previous,'provider_release_requires_manual_fence')

def backup_namespace(module,inventory):
    j=module['journal'];p=Path(inventory['backup_root']['path']);j.directory(p,inventory['backup_root']['identity']);root=p/'storage-stability-chains'
    if not root.exists():
        j.new_directory(root,inventory['backup_root']['identity']);j.new_file(root/'managed.json',module['inventory'].canonical({'schema_version':1,'kind':'openlegal_storage_stability_owned_backup_namespace','backup_root_identity':inventory['backup_root']['identity']}))
    marker=module['inventory'].decode(module['inventory'].private_bytes(root/'managed.json'))
    module['interface'].need(marker=={'schema_version':1,'kind':'openlegal_storage_stability_owned_backup_namespace','backup_root_identity':inventory['backup_root']['identity']},'backup_namespace_identity')
    j.directory(root);return root

def execute(module,inventory,authority,inventory_path,inventory_pin,authority_path,authority_pin,operation,action,base=INSTALL_ROOT):
    i=module['interface'];p=module['inventory'];j=module['journal'];need=i.need
    source_pin=p.digest(p.canonical(inventory['source_graph']));previous={};protected_directories={};protected_files={}
    operation_deadline=time.monotonic()+6*3600
    binding={'schema_version':1,'kind':'openlegal_storage_stability_operation','operation_id':operation,'action':action,'inventory_sha256':inventory_pin,'source_graph_sha256':source_pin,'authority_sha256':authority_pin,'managed_epoch':inventory['managed_epoch'],'revision':inventory['revision'],'automatic_retry':False,'automatic_failover':False}
    def source_seal():
        need(time.monotonic()<operation_deadline,'operation_deadline')
        p.private_bytes(inventory_path,inventory_pin);p.private_bytes(authority_path,authority_pin)
        for name,pin in inventory['source_graph'].items():p.private_bytes(base/name,pin,1024*1024)
        need(Path('/proc/sys/kernel/random/boot_id').read_text().strip()==inventory['host']['boot_id']and Path('/etc/machine-id').read_text().strip()==inventory['host']['machine_id'],'host_rebooted_or_changed')
        p.validate_authority(authority,inventory,inventory_pin,action,operation)
        j.directory(Path(inventory['backup_root']['path']),inventory['backup_root']['identity'])
        for path,identity in protected_directories.items():j.directory(path,identity)
        for path,pin in protected_files.items():p.private_bytes(path,pin)
        lock.check()
    with j.SharedLock(inventory['shared_lock'])as lock:
        source_seal();journal=j.DualJournal(inventory['journal_roots'],operation,binding)
        def seal():source_seal();journal.check()
        # The fixed backend is authenticated before import; it accepts no hooks.
        backend=module['linux_backend'].LinuxBackend()
        original_error=None
        try:
            namespace=Path(inventory['backup_root']['path'])/'storage-stability-chains';chain=namespace/operation
            phases=i.CHECKPOINT_PHASES if action=='checkpoint'else('qualify',)+i.FAILOVER_PHASES
            for phase in phases:
                seal();journal.begin(phase)
                op=i.Operation(operation,action,phase,inventory,inventory_pin,source_pin,authority_pin,journal,chain,dict(previous),seal,i.freeze_authority(copy.deepcopy(authority)))
                try:
                    if phase=='copy_ssd':
                        need(previous['stop_postgres']['facts']['postgres_processes_absent']is True and previous['snapshot_same_pool']['facts']['all_writers_still_stopped']is True,'copy_requires_stopped_snapshot')
                        target=chain/'ssd';j.new_directory(target,protected_directories[chain]);manifests={};copy_deadline=time.monotonic()+3600
                        for record in inventory['ssd_trees']:
                            seal();manifest=module['copy_tree'].copy_tree(record,target/record['name'],copy_deadline)
                            name='copy-'+record['name']+'.json';manifests[record['name']]=journal.save(name,manifest)
                        receipt={'schema_version':1,'kind':'openlegal_storage_stability_local_copy_receipt','operation_id':operation,'phase':phase,'inventory_sha256':inventory_pin,'source_graph_sha256':source_pin,'authority_sha256':authority_pin,'status':'passed','manifests':manifests,'source_writers_quiescent_claimed':False}
                    elif phase=='retention':
                        need('verify_isolated_restore'in previous,'retention_requires_actual_restore')
                        paths=module['retention'].plan(namespace,time.time());journal.save('retention-plan.json',{'chains':[x.name for x in paths],'minimum_days':14,'original_snapshots_deleted':False})
                        prune_deadline=time.monotonic()+600
                        for path in paths:seal();module['retention'].prune(path,namespace,time.time(),prune_deadline)
                        receipt={'schema_version':1,'kind':'openlegal_storage_stability_retention_receipt','operation_id':operation,'phase':phase,'status':'passed','removed_owned_chains':[x.name for x in paths],'original_snapshots_deleted':False}
                    else:
                        seal();receipt=validate_receipt(module,op,getattr(backend,phase)(op))
                        if phase in i.HOLD_PHASES:check_hold(module,op,receipt,authority)
                    seal();receipt_pin=journal.save('receipt.json',receipt)
                    journal.save('completed.json',dict(binding,phase=phase,status='passed',receipt_sha256=receipt_pin,outputs=journal.output_hashes(),at=time.time()))
                    previous[phase]=receipt
                    if phase=='qualify':
                        seal();namespace=backup_namespace(module,inventory);protected_directories[namespace]=j.directory(namespace);protected_files[namespace/'managed.json']=p.digest(p.private_bytes(namespace/'managed.json'));chain=namespace/operation;protected_directories[chain]=j.new_directory(chain,protected_directories[namespace])
                        j.new_file(chain/'chain.json',p.canonical(dict(binding,kind='openlegal_storage_stability_chain',created_at=time.time(),chain_format='full_self_contained_v1',parent_chains=[])));protected_files[chain/'chain.json']=p.digest(p.private_bytes(chain/'chain.json'))
                    if phase=='verify_isolated_restore':
                        export_raw=p.canonical(previous['export_snapshots']);restore_raw=p.canonical(receipt)
                        j.new_file(chain/'export-receipt.json',export_raw);j.new_file(chain/'restore-receipt.json',restore_raw)
                        j.new_file(chain/'verified.json',p.canonical({'schema_version':1,'kind':'openlegal_storage_stability_verified_chain','operation_id':operation,'inventory_sha256':inventory_pin,'source_graph_sha256':source_pin,'status':'passed','actual_exit_code':0,'owned_processes_closed':True,'restore_receipt_sha256':p.digest(restore_raw),'export_receipt_sha256':p.digest(export_raw),'chain_format':'full_self_contained_v1','parent_chains':[],'verified_at':time.time()}))
                        for filename in('export-receipt.json','restore-receipt.json','verified.json'):protected_files[chain/filename]=p.digest(p.private_bytes(chain/filename))
                except BaseException:
                    # Failure of either journal medium is terminal; never continue on
                    # the other medium or attempt automatic resume/rollback/retry.
                    try:journal.save('failed.json',dict(binding,phase=phase,status='FAILED_OR_UNKNOWN',at=time.time(),automatic_resume=False))
                    except BaseException:pass
                    raise
            seal();return {'operation_id':operation,'action':action,'status':'completed_pending_actual_operator_exit','phase_count':len(previous),'collection_resume_authorized':authority['collection_resume_permitted'],'originals_preserved':True,'automatic_retry':False}
        except BaseException as error:
            original_error=error
            raise
        finally:
            try:backend.close()
            except BaseException:
                try:journal.save('backend-close-failed.json',dict(binding,status='FAILED_OR_UNKNOWN',metadata_fd_cleanup_failed=True))
                except BaseException:pass
                if original_error is None:raise

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=('qualify','checkpoint','manual-failover'))
    parser.add_argument('--inventory',type=Path,required=True);parser.add_argument('--inventory-sha256',required=True)
    parser.add_argument('--authority',type=Path);parser.add_argument('--authority-sha256');parser.add_argument('--operation')
    args=parser.parse_args()
    if os.geteuid()!=0 or not sys.dont_write_bytecode or Path(__file__).resolve()!=INSTALL_ROOT/'checkpoint.py':raise RuntimeError('fixed_root_source_required')
    os.umask(0o077);raw=bootstrap_file(args.inventory,args.inventory_sha256);module=modules(raw);p=module['inventory'];inventory=p.validate_inventory(p.decode(raw))
    if args.action=='qualify':
        print(json.dumps({'status':'source_and_schema_qualified_only','inventory_sha256':args.inventory_sha256,'host_or_storage_not_qualified':True,'effects_permitted':False}));return
    module['interface'].need(args.authority is not None and args.authority_sha256 is not None,'actual_authority_required')
    authority=p.decode(p.private_bytes(args.authority,args.authority_sha256));action=args.action.replace('-','_');operation=args.operation or str(uuid.uuid4())
    module['interface'].need(p.valid_uuid(operation),'operation_uuid');p.validate_authority(authority,inventory,args.inventory_sha256,action,operation)
    print(json.dumps(execute(module,inventory,authority,args.inventory,args.inventory_sha256,args.authority,args.authority_sha256,operation,action),sort_keys=True))
if __name__=='__main__':
    try:main()
    except BaseException:
        print('{"status":"FAILED_OR_UNKNOWN","automatic_retry":false,"automatic_resume":false}',file=sys.stderr);raise SystemExit(1)
