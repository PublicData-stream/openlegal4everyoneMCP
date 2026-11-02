"""Read-only canary Pod/controller/CRI/guard identity adapter. Inert on import.

All command execution belongs to the pinned LinuxBackend read ports. This module
has no action, retry, provider API, cleanup, role or SQL mutation. Job hashes here
are canonical+LF(spec WITHOUT top-level suspend); daily inventory Job pins keep
its separate unchanged FULL-spec domain. Terminal actors are lifecycle evidence,
never physical process/SQL closure or final canary acceptance.
"""
from __future__ import annotations

import copy
import json
import os
import re
import stat
from pathlib import Path

from interface import need
from inventory import canonical, digest, valid_uuid

HEX = re.compile('[0-9a-f]{64}\\Z')
DEFAULT_TOLERATIONS = [
    {'key':'node.kubernetes.io/not-ready','operator':'Exists','effect':'NoExecute','tolerationSeconds':300},
    {'key':'node.kubernetes.io/unreachable','operator':'Exists','effect':'NoExecute','tolerationSeconds':300}]


def _owner(item):
    rows = [r for r in item.get('metadata',{}).get('ownerReferences',[]) if r.get('controller') is True]
    need(len(rows)==1 and rows[0].get('kind') in ('ReplicaSet','Deployment','Job') and valid_uuid(rows[0].get('uid')),
         'canary_controller_owner')
    return rows[0]['kind'],rows[0]['uid']


def _items(value):
    rows=value.get('items') if type(value)is dict else None
    need(type(rows)is list and len(rows)<=4096 and all(type(r)is dict for r in rows),'canary_kube_list_bound')
    return rows


def _pod_spec(actual, expected, node):
    """Exact source fields; only typed Kubernetes scheduling defaults differ."""
    need(type(actual)is dict and type(expected)is dict and expected.get('automountServiceAccountToken')is False,
         'canary_unsupported_pod_defaults')
    current=copy.deepcopy(actual);wanted=copy.deepcopy(expected)
    existing=wanted.get('tolerations',[]);extra=[]
    for row in current.get('tolerations',[]):
        if row not in existing:extra.append(row)
    need(all(row in DEFAULT_TOLERATIONS for row in extra) and len(extra)==len({digest(canonical(r))for r in extra}),
         'canary_pod_toleration_changed')
    if 'tolerations'in current:
        current['tolerations']=[r for r in current['tolerations'] if r not in extra]
        if 'tolerations'not in wanted and not current['tolerations']:current.pop('tolerations')
    for key in set(current)-set(wanted):
        if key=='nodeName':need(current[key]==node,'canary_pod_node_changed')
        elif key=='serviceAccount':need(current[key]==current.get('serviceAccountName'),'canary_service_account_alias')
        elif key=='priority':need(type(current[key])is int and current[key]==0 and not current.get('priorityClassName'),'canary_priority_changed')
        elif key=='preemptionPolicy':need(current[key]=='PreemptLowerPriority','canary_preemption_changed')
        else:need(False,'canary_extra_pod_spec_field')
        current.pop(key)
    need(current==wanted,'canary_pod_template_changed')


def _proc(pid,cid):
    """Anchor actual host PID across proc reads; no executable or data writes."""
    need(type(pid)is int and pid>0 and HEX.fullmatch(cid),'canary_proc_identity')
    path=Path('/proc')/str(pid)
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:
        before=os.fstat(fd)
        def read(name):
            child=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC,dir_fd=fd)
            try:
                data=os.read(child,65537);need(len(data)<=65536,'canary_proc_read_bound');return data.decode('utf8','strict')
            finally:os.close(child)
        first=read('stat');ticks=int(first.rsplit(')',1)[1].split()[19]);need(ticks>0,'canary_process_start')
        status=read('status');nspid=[line.split()[1:]for line in status.splitlines()if line.startswith('NSpid:')]
        need(len(nspid)==1 and nspid[0] and all(x.isdecimal() and int(x)>0 for x in nspid[0])and int(nspid[0][0])==pid,
             'canary_process_namespace')
        groups=read('cgroup').splitlines()
        need(len(groups)==1 and groups[0].startswith('0::/') and groups[0]!='0::/' and cid in groups[0],
             'canary_process_cgroup')
        group=groups[0].split(':',2)[2];need('..'not in Path(group).parts,'canary_cgroup_path')
        cgroup=Path('/sys/fs/cgroup')/group.lstrip('/');need(cgroup.resolve()==cgroup,'canary_cgroup_alias')
        meta=cgroup.stat();need(stat.S_ISDIR(meta.st_mode),'canary_cgroup_directory')
        need(str(pid)in(cgroup/'cgroup.procs').read_text().split(),'canary_cgroup_membership')
        need(first==read('stat')or int(read('stat').rsplit(')',1)[1].split()[19])==ticks,'canary_proc_reused')
        after=path.stat();need((before.st_dev,before.st_ino)==(after.st_dev,after.st_ino),'canary_proc_rebound')
        return {'pid':pid,'process_start_ticks':ticks,'namespace_pid':int(nspid[0][-1]),
            'cgroup':{'path':str(cgroup),'dev':meta.st_dev,'ino':meta.st_ino,'group':group}}
    finally:os.close(fd)


def _cid(value):
    need(type(value)is str and re.fullmatch('containerd://[0-9a-f]{64}',value),'canary_container_id')
    return value[len('containerd://'):]


def _image(value):
    need(type(value)is str and re.search('sha256:[0-9a-f]{64}$',value),'canary_runtime_image')
    return value.rsplit('sha256:',1)[1]


def _process(backend,op,pod,status,guard):
    config=op.inventory['linux'];cid=_cid(status.get('containerID'))
    args=['--runtime-endpoint',config['cri_endpoint'],'inspect','-o','json',cid]
    raw=json.loads(backend._run(op,'crictl',args,record=False));current=raw.get('status',{})
    need(current.get('id')==cid and current.get('state')=='CONTAINER_RUNNING' and
         current.get('labels',{}).get('io.kubernetes.pod.uid')==pod['metadata']['uid'] and
         current.get('labels',{}).get('io.kubernetes.pod.namespace')==config['namespace'] and
         _image(current.get('imageRef'))==_image(status.get('imageID')),'canary_cri_binding')
    pid=raw.get('info',{}).get('pid');need(type(pid)is int and pid>0,'canary_cri_host_pid')
    process=_proc(pid,cid);op.seal()
    need(guard['pid']==process['namespace_pid'] and guard['process_start_ticks']==process['process_start_ticks'],
         'canary_guard_process_mismatch')
    # A second fixed inspect is a read, not a retry of an action.
    again=json.loads(backend._run(op,'crictl',args,record=False))
    need(again.get('status',{}).get('id')==cid and again['status'].get('state')=='CONTAINER_RUNNING' and
         again.get('info',{}).get('pid')==process['pid'],'canary_cri_process_replaced')
    return {'container_id':status['containerID'],'host_pid':process['pid'],
        'process_start_ticks':process['process_start_ticks'],'started_at':status['state']['running']['startedAt'],
        'namespace_pid':process['namespace_pid'],'cgroup':process['cgroup'],'guard_record_sha256':guard['record_sha256']}


def observe(backend,op):
    """Return sampled native evidence only; no running actor closure claims."""
    op.seal();config=op.inventory['linux'];policy=op.canary_policy
    need(op.watch_stage in('before_activation','after_activation','observe'),'canary_watch_stage')
    keys=tuple(policy['allowed_active_guard_keys']);need(keys and keys[0]=='serve' and len(set(keys))==len(keys),'canary_allowed_keys')
    known={row['uid']:row for row in config['workloads']}
    allowed={row['uid']:row for row in policy['workload_pins']}
    need(len(known)==len(config['workloads']) and set(allowed)<=set(known),'canary_workload_inventory')
    workloads={};roles={};templates={};runtime={}
    get=lambda kind,name:backend._kube(op,['-n',config['namespace'],'get',kind]+([name]if name is not None else[])+['-o','json'])
    for ident,row in known.items():
        item=get('deployment',row['name'])
        need(item['metadata']['uid']==ident and item['metadata']['name']==row['name'] and
             digest(canonical(item['spec']['template']['spec']))==row['pod_spec_sha256'],'canary_workload_source_changed')
        role=allowed[ident]['role'] if ident in allowed else None
        if role is not None:need(allowed[ident]['name']==row['name'] and role in keys,'canary_workload_role')
        replicas=item['spec'].get('replicas')
        need(type(replicas)is int and replicas==(1 if role in('serve','scheduler')else 0),'canary_workload_replica_scope')
        if row['name']==config['serving_workload']:need(role=='serve','canary_serving_role')
        if role is not None:runtime[role]=allowed[ident]['runtime_image_id']
        workloads[ident]=item;roles[ident]=role;templates[ident]=item['spec']['template']['spec']
    need(list(roles.values()).count('serve')==1,'canary_single_serving_workload')
    for cm in config['config_maps']:
        item=get('configmap',cm['name'])
        need(item['metadata']['uid']==cm['uid'] and digest(canonical(item.get('data',{})))==cm['data_sha256'],
             'canary_configmap_changed')
    sets={}
    for rs in _items(get('replicasets',None)):
        if not any(o.get('controller')is True and o.get('kind')=='Deployment'and o.get('uid')in known for o in rs.get('metadata',{}).get('ownerReferences',[])):continue
        kind,owner=_owner(rs);need(kind=='Deployment','canary_replicaset_owner')
        sets[rs['metadata']['uid']]=(owner,rs)
    jobs={};job_roles={};job_rows=[]
    for row in policy['collection_job_pins']:
        item=get('job',row['name']);spec=copy.deepcopy(item['spec']);suspend=spec.pop('suspend',False)
        key='collection.'+row['request_id']+'.'+str(row['epoch'])
        need(type(suspend)is bool and suspend==(op.watch_stage=='before_activation'),'canary_job_suspend_stage')
        need(spec.get('backoffLimit')==0 and type(spec.get('backoffLimit'))is int and type(spec.get('parallelism',1))is int and spec.get('parallelism',1)==1 and type(spec.get('completions',1))is int and spec.get('completions',1)==1 and
             ('ttlSecondsAfterFinished'not in spec or type(spec['ttlSecondsAfterFinished'])is int and spec['ttlSecondsAfterFinished']>=21660),'canary_job_retry_or_expiry')
        need(type(suspend)is bool and item['metadata']['uid']==row['uid'] and key in keys and
             digest(canonical(spec))==row['spec_sha256'],'canary_job_source_changed')
        need(row['name']=='openlegal-request-'+row['request_id'].replace('-','')+'-'+str(row['epoch']), 'canary_job_name_identity')
        status=item.get('status',{});need(type(status.get('active',0))is int and 0<=status.get('active',0)<=1 and
             (op.watch_stage!='before_activation' or status.get('active',0)==0),'canary_job_active_bound')
        need(type(status.get('failed',0))is int and status.get('failed',0)==0 and
             not any(c.get('type')=='Failed'and c.get('status')=='True' for c in status.get('conditions',[])), 'canary_job_failed')
        containers=spec['template']['spec'].get('containers',[])
        need(len(containers)==1 and containers[0].get('args')==['--collection-job',row['request_id']+'@'+str(row['epoch']),'/etc/openlegal/server.toml'],
             'canary_job_request_argv')
        jobs[row['uid']]=item;job_roles[row['uid']]=key;runtime[key]=row['runtime_image_id']
        job_rows.append({'role':key,'job_uid':row['uid'],'job_name':row['name'],'lifecycle':'job_suspended'if suspend else 'job_completed'if status.get('succeeded',0)>0 else 'job_admitted','physical_closure_proven':False})
    oldpins={row['uid']:row for row in config['jobs']}
    alljobs=_items(get('jobs',None));seenjobs={item['metadata']['uid']for item in alljobs}
    need(len(seenjobs)==len(alljobs) and set(oldpins)|set(jobs)<=seenjobs,'canary_job_inventory_missing')
    for item in alljobs:
        ident=item['metadata']['uid'];status=item.get('status',{})
        terminal=any(c.get('type')in('Complete','Failed')and c.get('status')=='True'for c in status.get('conditions',[]))
        if ident in jobs:
            expected=copy.deepcopy(item['spec']);suspended=expected.pop('suspend',False)
            pin=next(r for r in policy['collection_job_pins']if r['uid']==ident)
            need(digest(canonical(expected))==pin['spec_sha256'] and suspended==(op.watch_stage=='before_activation'),'canary_job_list_changed')
        elif ident in oldpins:
            pin=oldpins[ident]
            need(item['metadata']['name']==pin['name'] and digest(canonical(item['spec']))==pin['spec_sha256'] and
                 type(status.get('active',0))is int and status.get('active',0)==0 and
                 (terminal or item['spec'].get('suspend')is True),'canary_quarantined_job_changed')
        else:need(terminal,'canary_unknown_nonterminal_job')
    native=backend._lookup_native(op,allowed_active_guard_keys=keys)
    proof=backend._runtime_guards(op,active=True,allowed_active_keys=keys)
    processes=proof['processes'];active={r['key']:r for r in processes if r['state']=='active_unclean'}
    need('serve'in active and all(r['locked']is True for r in active.values()) and len(active)==sum(r['state']=='active_unclean'for r in processes),
         'canary_active_guard_binding')
    rows=_items(get('pods',None));need(len({p['metadata']['uid']for p in rows})==len(rows),'canary_duplicate_pod_uid')
    running={};actors=job_rows;serving=[];seen_roles=set()
    for pod in rows:
        terminal=pod.get('status',{}).get('phase')in('Succeeded','Failed')
        if terminal:
            owners=pod.get('metadata',{}).get('ownerReferences',[])
            if any(r.get('controller')is True and r.get('kind')=='Job'and r.get('uid')in jobs for r in owners):
                need(pod['status']['phase']=='Succeeded','canary_selected_job_pod_failed')
                statuses=pod['status'].get('containerStatuses',[])
                need(len(statuses)==1 and statuses[0].get('state',{}).get('terminated',{}).get('exitCode')==0 and
                     type(statuses[0]['state']['terminated']['exitCode'])is int,'canary_selected_job_terminal_status')
            continue
        kind,owner=_owner(pod)
        if kind=='ReplicaSet'and owner in sets:
            deployment,replicaset=sets[owner];role=roles[deployment];template=templates[deployment]
            need(digest(canonical(replicaset['spec']['template']['spec']))==digest(canonical(template)),'canary_replicaset_template_changed')
            need(role is not None,'canary_unapproved_active_workload')
        elif kind=='Job'and owner in jobs:
            role=job_roles[owner];template=jobs[owner]['spec']['template']['spec']
        else:need(False,'canary_unapproved_active_pod')
        need(role not in seen_roles,'canary_multiple_live_pods_per_role');seen_roles.add(role)
        need(not role.startswith('collection.')or op.watch_stage!='before_activation','canary_job_pod_before_activation')
        _pod_spec(pod['spec'],template,op.inventory['host']['node_name'])
        containers=pod['spec'].get('containers',[]);statuses=pod.get('status',{}).get('containerStatuses',[])
        need(len(containers)==1 and len(statuses)<=1 and not pod.get('spec',{}).get('initContainers') and
             not pod.get('spec',{}).get('ephemeralContainers'),'canary_container_shape')
        base={'role':role,'pod_uid':pod['metadata']['uid'],'phase':pod['status']['phase'],'physical_closure_proven':False}
        if not statuses:
            need(role!='serve'and role not in active,'canary_active_guard_without_container')
            actors.append(dict(base,lifecycle='pending_no_runtime_observed'));continue
        current=statuses[0]
        need(type(current.get('restartCount'))is int and current['restartCount']==0 and current.get('name')==containers[0]['name'],
             'canary_actor_restart_or_name')
        if role=='serve':need(current.get('ready')is True and any(c.get('type')=='Ready'and c.get('status')=='True'for c in pod['status'].get('conditions',[])),'canary_serving_became_unready')
        state=current.get('state',{})
        if 'terminated'in state:
            need(role!='serve'and state['terminated'].get('exitCode')==0 and type(state['terminated'].get('exitCode'))is int and
                 role not in active,'canary_actor_failed_or_unclean_terminal')
            actors.append(dict(base,lifecycle='terminal_exit_zero_not_closure'));continue
        if 'waiting'in state:
            need(role!='serve'and role not in active,'canary_waiting_guard_mismatch')
            actors.append(dict(base,lifecycle='waiting_no_runtime_observed'));continue
        need('running'in state and type(state['running'].get('startedAt'))is str,'canary_actor_state')
        need(type(runtime[role])is str and current.get('imageID')==runtime[role] and
             (role!='serve' or runtime[role]==config['serving_runtime_image_id']),
             'canary_actor_runtime_image')
        guards=[r for r in processes if r['key']==role and r['pid']>0]
        live=[r for r in guards if r['state']=='active_unclean']
        if not live:
            # Exact closed-clean process identity may briefly precede container
            # exit. It is a lifecycle observation, never an absence proof.
            need(role!='serve','canary_serving_guard_closed');live=[r for r in guards if r['state']=='closed_clean'and r['locked']is False]
        need(len(live)==1,'canary_current_actor_guard')
        process=_process(backend,op,pod,current,live[0]);cid=_cid(current['containerID'])
        need(cid not in running,'canary_duplicate_running_container');running[cid]=pod['metadata']['uid']
        row=dict(base,**process,lifecycle='active_guarded'if live[0]['state']=='active_unclean'else'closed_clean_draining')
        if role=='serve':serving.append(row)
        else:actors.append(row)
    need(len(serving)==1 and set(active)=={'serve'}|{r['role']for r in actors if r['lifecycle']=='active_guarded'},
         'canary_guard_actor_bijection')
    raw=json.loads(backend._run(op,'crictl',['--runtime-endpoint',config['cri_endpoint'],'ps','-a','-o','json'],record=False))
    cri=raw.get('containers');need(type(cri)is list and len(cri)<=16384,'canary_cri_list_bound')
    actual={r.get('id'):r.get('labels',{}).get('io.kubernetes.pod.uid')for r in cri
            if r.get('state')=='CONTAINER_RUNNING'and r.get('labels',{}).get('io.kubernetes.pod.namespace')==config['namespace']}
    need(actual==running,'canary_unapproved_running_cri')
    need(len(native['pods'])==1 and native['pods'][0]['uid']==serving[0]['pod_uid'],'canary_native_serving_changed')
    # Lookup's existing six fields remain, with actual stable process identity.
    native['pods'][0].update({key:serving[0][key]for key in ('container_id','process_start_ticks','started_at')})
    result={key:native[key]for key in ('pods','endpoints','health','runtime_guard')}
    result.update(actors=actors,guard_proof=proof)
    op.seal();backend._artifact(op,'canary-native-'+str(backend._sequence),result)
    return result
