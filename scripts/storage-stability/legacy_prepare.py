"""Legacy schema27 fencing and exact seven-dataset snapshot producer.

Only a sealed ROOT parent may call this after a fork with its retained writer
cgroup descriptors. The parent observes the producer's real OS exit and wraps
the returned raw payloads; this module never claims its own future exit zero.
No strict replica/guard, role, ledger cleanup, seed or serving-start port exists.
"""
from __future__ import annotations

import hashlib
import fcntl
import os
from pathlib import Path
import platform
import re
import stat
import time

import bootstrap_backend as blobs
from copy_tree import root_fd
from interface import Refused, need
from inventory import canonical, decode, digest, pin, private_bytes, valid_uuid
import initial_migration as migration
import linux_backend as linux
from manual_backend import _process_absent

PHASE='legacy_fence_snapshot'
ROLES={'cache_hdd','corpus_hdd','corpus_ssd_lookup'}
IMAGE_FORMAT='{"Id":{{json .Id}},"Os":{{json .Os}},"Architecture":{{json .Architecture}}}'


def _save(backend,op,name,value):
    return migration._saved(backend,op,name,value)


def _scope(backend,op):
    need(isinstance(backend,linux.LinuxBackend) and os.geteuid()==0 and
         op.action=='legacy_prepare' and op.phase==PHASE and valid_uuid(op.operation_id) and
         op.authority.get('legacy_fence_snapshot_permitted') is True,'legacy_prepare_scope')
    op.seal()
    c=op.inventory['legacy_prepare']
    need(type(c) is dict and set(c)=={'deadline_secs','pgdata','image','expected_system_identifier',
         'expected_timeline','migration_manifest','blob_roots'},'legacy_prepare_policy')
    need(type(c['deadline_secs']) is int and 1<=c['deadline_secs']<=1800 and
         type(c['expected_system_identifier']) is str and re.fullmatch('[1-9][0-9]{0,19}',c['expected_system_identifier']) and
         type(c['expected_timeline']) is int and 1<=c['expected_timeline']<=4294967295,'legacy_prepare_identity')
    need(set(c['blob_roots'])==ROLES and len(op.inventory['original_datasets'])==7 and
         {r['name'] for r in op.inventory['original_datasets']}==migration.SNAPSHOT_DATASETS,'legacy_prepare_exact_topology')
    need(type(op.inventory['linux']['workloads']) is list and len(op.inventory['linux']['workloads'])==2 and
         len({r['name'] for r in op.inventory['linux']['workloads']})==2 and
         op.inventory['linux']['serving_workload'] in {r['name'] for r in op.inventory['linux']['workloads']} and
         all(r['kind']=='deployment' and valid_uuid(r['uid']) for r in op.inventory['linux']['workloads']),
         'legacy_prepare_workloads')
    jobs=op.inventory['linux']['jobs']
    need(type(jobs) is list and len(jobs)<=256 and len({r['uid'] for r in jobs})==len(jobs) and
         len({r['name'] for r in jobs})==len(jobs) and all(valid_uuid(r['uid']) and pin(r['spec_sha256']) for r in jobs),
         'legacy_prepare_jobs')
    for row in op.inventory['original_datasets']:
        need(set(row)=={'name','guid','pool','mountpoint'} and row['pool']=='cachepool' and
             type(row['guid']) is str and re.fullmatch('[0-9]{1,20}',row['guid']) and
             Path(row['mountpoint']).is_absolute(),'legacy_prepare_dataset')
    image=c['image']
    need(type(image) is dict and set(image)=={'reference','inspect_id','postgres_sha256','pg_controldata_sha256'} and
         re.fullmatch(r'(?:postgres|docker.io/library/postgres)@sha256:[0-9a-f]{64}',image['reference']) and
         re.fullmatch(r'sha256:[0-9a-f]{64}',image['inspect_id']) and
         all(pin(image[k]) for k in ('postgres_sha256','pg_controldata_sha256')),'legacy_prepare_image')
    ref=c['migration_manifest'];need(type(ref) is dict and set(ref)=={'path','sha256'} and pin(ref['sha256']),'legacy_prepare_manifest_ref')
    rows=decode(private_bytes(ref['path'],ref['sha256']))
    need(type(rows) is list and len(rows)==27 and [r.get('version') for r in rows]==list(range(1,28)) and
         all(set(r)=={'version','checksum'} and type(r['version']) is int and
             re.fullmatch('[0-9a-f]{96}',r['checksum']) for r in rows),'legacy_prepare_manifest27')
    for name in ('legacy_prepare.py','initial_migration.py','linux_backend.py','command_process.py',
                 'interface.py','inventory.py','manual_backend.py','bootstrap_backend.py','copy_tree.py'):
        need(name in op.inventory['source_graph'],'legacy_prepare_source_graph')
        private_bytes(Path(__file__).resolve().parent/name,op.inventory['source_graph'][name])
    return c,rows


def _guard(backend,op):
    """One attempt per original primary, retained after every outcome."""
    cid=op.inventory['linux']['postgres']['primary']['container_id']
    need(re.fullmatch('[0-9a-f]{64}',cid),'legacy_primary_id')
    fd=os.open('/tmp/openlegal-legacy-prepare-primary-'+cid+'.lock',
               os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
    backend._legacy_prepare_guard_fd=fd;fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
    raw=canonical({'operation_id':op.operation_id,'unknown_requires_manual_audit':True})
    view=memoryview(raw)
    while view:
        n=os.write(fd,view);need(n>0,'legacy_guard_write');view=view[n:]
    os.fsync(fd);os.lseek(fd,0,os.SEEK_SET)
    need(os.read(fd,len(raw)+1)==raw,'legacy_guard_readback')
    parent=root_fd('/tmp')
    try:os.fsync(parent)
    finally:os.close(parent)
    _save(backend,op,'one-attempt-guard',{'primary_id':cid,'operation_id':op.operation_id})


def _parent_anchor(backend,op):
    """Verify actual inherited FDs are also retained by the live ROOT parent."""
    parent=os.getppid();need(parent>1 and backend._cgroup_fds,'legacy_parent_prefork_capture_required')
    statpath=Path('/proc')/str(parent)/'stat'
    ticks=statpath.read_text().rsplit(')',1)[1].split()[19]
    need(Path('/proc/'+str(parent)).stat().st_uid==0,'legacy_parent_owner')
    rows=[]
    for fd,expected in backend._cgroup_fds.values():
        own=os.fstat(fd);other=Path('/proc/'+str(parent)+'/fd/'+str(fd)).stat()
        need(stat.S_ISDIR(own.st_mode) and (own.st_dev,own.st_ino)==(other.st_dev,other.st_ino)==
             (expected['dev'],expected['ino']),'legacy_parent_did_not_retain_cgroup')
        rows.append({'fd':fd,'dev':own.st_dev,'ino':own.st_ino,'path':expected['path']})
    need(statpath.read_text().rsplit(')',1)[1].split()[19]==ticks,'legacy_parent_reused')
    return {'pid':parent,'start_ticks':ticks,'boot_id':op.inventory['host']['boot_id'],'cgroups':rows}


def _resources(backend,op,expected):
    config=op.inventory['linux'];rows=[]
    for pinrow in config['workloads']:
        item=backend._kube(op,['-n',config['namespace'],'get','deployment',pinrow['name'],'-o','json'])
        meta=item['metadata'];spec=item['spec']
        need(_same_fenced_spec(item,expected['deployment',pinrow['name']]),'legacy_deployment_fence_changed')
        need(meta['uid']==pinrow['uid'] and spec.get('replicas')==0 and
             digest(canonical(spec['template']['spec']))==pinrow['pod_spec_sha256'],'legacy_deployment_reopened')
        rows.append({'kind':'deployment','name':pinrow['name'],'uid':pinrow['uid'],
            'resource_version':meta['resourceVersion'],'spec_sha256':digest(canonical(spec)),
            'fence_nonce':meta.get('annotations',{}).get('openlegal4everyone.stream/operation-fence')})
    actual=backend._kube(op,['-n',config['namespace'],'get','jobs','-o','json'])['items']
    pins={r['uid']:r for r in config['jobs']};seen=set()
    for item in actual:
        meta=item['metadata'];finished=_terminal(item)
        if meta['uid'] not in pins:
            need(finished,'legacy_unknown_nonterminal_job');continue
        p=pins[meta['uid']];seen.add(meta['uid']);need(meta['name']==p['name'],'legacy_job_name')
        need(_same_fenced_spec(item,expected['job',p['name']]),'legacy_job_fence_changed')
        need(finished or item['spec'].get('suspend') is True,'legacy_job_reopened')
        rows.append({'kind':'job','name':p['name'],'uid':p['uid'],'resource_version':meta['resourceVersion'],
            'spec_sha256':digest(canonical(item['spec'])),
            'fence_nonce':None if finished else meta.get('annotations',{}).get('openlegal4everyone.stream/operation-fence')})
    need(seen==set(pins),'legacy_job_missing')
    for row in rows:
        if row['fence_nonce'] is not None:
            need(type(row['fence_nonce']) is str and row['fence_nonce'].startswith(op.operation_id+'.'),'legacy_fence_nonce')
        else:need(row['kind']=='job','legacy_deployment_fence_missing')
    return rows


def _same_fenced_spec(actual,expected):
    key='openlegal4everyone.stream/operation-fence'
    return actual['spec']==expected['spec'] and all(actual['metadata'][k]==expected['metadata'][k]
        for k in ('uid','name')) and actual['metadata'].get('annotations',{}).get(key)==\
        expected['metadata'].get('annotations',{}).get(key)


def _terminal(item):
    return any(r.get('type') in ('Complete','Failed') and r.get('status')=='True'
               for r in item.get('status',{}).get('conditions',[]))


def _fence(backend,op,deadline):
    anchor=_parent_anchor(backend,op)
    initial_ids={r['id'] for r in backend._cri_rows(op)}
    backend._capture_writer_cgroups(op)
    need(_parent_anchor(backend,op)==anchor,'legacy_new_unanchored_writer')
    config=op.inventory['linux'];expected={}
    jobs=backend._kube(op,['-n',config['namespace'],'get','jobs','-o','json'])['items']
    pins={r['uid']:r for r in config['jobs']}
    need({r['metadata']['uid'] for r in jobs if r['metadata']['uid'] in pins}==set(pins),'legacy_known_job_missing')
    need(all(r['metadata']['uid'] in pins or _terminal(r) for r in jobs),'legacy_unknown_nonterminal_job')
    for row in config['workloads']:
        item=backend._kube(op,['-n',config['namespace'],'get','deployment',row['name'],'-o','json'])
        expected['deployment',row['name']]=backend._cas_replicas(op,row['name'],row['uid'],replicas=0,
            current=item['spec'].get('replicas',1),template_sha256=row['pod_spec_sha256'])
    for item in jobs:
        uid=item['metadata']['uid']
        if uid not in pins:need(_terminal(item),'legacy_unknown_nonterminal_job');continue
        if _terminal(item):
            need(item['metadata']['name']==pins[uid]['name'] and digest(canonical(item['spec']))==pins[uid]['spec_sha256'],
                 'legacy_terminal_history_changed')
            expected['job',pins[uid]['name']]=item
        else:
            changed=backend._cas_suspend_job(op,pins[uid],normalize_suspend=False)
            # A Job may become terminal between the list and CAS read. Preserve
            # it untouched; qualify its actual full spec again below.
            if changed.get('terminal_untouched') is True:
                changed=backend._kube(op,['-n',config['namespace'],'get','job',pins[uid]['name'],'-o','json'])
                need(_terminal(changed) and changed['metadata']['uid']==uid and
                     digest(canonical(changed['spec']))==pins[uid]['spec_sha256'],'legacy_terminal_history_changed')
            expected['job',pins[uid]['name']]=changed
    end=min(deadline,time.monotonic()+180)
    while True:
        op.seal();need(time.monotonic()<end,'legacy_drain_deadline')
        try:closure=backend._physical_writers_closed(op);break
        except Refused as error:
            need(str(error) in ('mcp_pods_not_drained','mcp_cri_not_drained','mcp_cgroup_not_drained',
                               'mcp_original_process_still_alive'),'legacy_drain_unknown')
            time.sleep(.1)
    need({r['id'] for r in closure['cri_containers']}<=initial_ids,'legacy_new_cri_after_prefork_capture')
    resources=_resources(backend,op,expected)
    migration._fence(backend,op,{'resources':resources,'closure':closure})
    _save(backend,op,'parent-fd-anchor',anchor)
    return {'resources':resources,'closure':closure}


def _hash_file(fd,expected):
    before=os.fstat(fd);need(stat.S_ISREG(before.st_mode) and before.st_size<=128*1024**2,'legacy_native_binary')
    hashed=hashlib.sha256()
    while block:=os.read(fd,1024*1024):hashed.update(block)
    need(linux.same_stamp(before,os.fstat(fd)) and hashed.hexdigest()==expected,'legacy_native_binary_changed')


def _kernel_mountinfo(pid=None):
    return Path('/proc/self/mountinfo' if pid is None else '/proc/'+str(pid)+'/mountinfo').read_text()


def _fd_mount_id(fd):
    rows=Path('/proc/self/fdinfo/'+str(fd)).read_text().splitlines()
    ids=[r.split(':',1)[1].strip() for r in rows if r.startswith('mnt_id:')]
    need(len(ids)==1 and ids[0].isdecimal(),'legacy_mount_id')
    return int(ids[0])


def _mounts(pid=None):
    raw=_kernel_mountinfo(pid);lines=raw.splitlines()
    need(len(raw)<=16*1024**2 and len(lines)<=65536,'legacy_mountinfo_bound')
    def text(value):
        return re.sub(r'\\(040|011|012|134)',lambda m:chr(int(m[1],8)),value)
    result={}
    for line in lines:
        row=line.split();need('-' in row,'legacy_mountinfo_shape');split=row.index('-')
        need(split>=6 and len(row)==split+4 and row[0].isdecimal() and row[0] not in result and
             re.fullmatch('[0-9]+:[0-9]+',row[2]),'legacy_mountinfo_shape')
        result[row[0]]={'mount_id':int(row[0]),'device':row[2],'root':text(row[3]),
            'mountpoint':text(row[4]),'filesystem':row[split+1],'source':text(row[split+2])}
    return result


def _path_mount(fd,path,table):
    """Fresh pathname and held FD must still refer to the same kernel mount."""
    meta=os.fstat(fd);mid=_fd_mount_id(fd);row=table.get(str(mid))
    need(row is not None and row['device']==str(os.major(meta.st_dev))+':'+str(os.minor(meta.st_dev)) and
         Path(path).is_relative_to(row['mountpoint']),'legacy_mount_missing_or_device')
    fresh=root_fd(path)
    try:
        now=os.fstat(fresh)
        need((meta.st_dev,meta.st_ino)==(now.st_dev,now.st_ino) and _fd_mount_id(fresh)==mid,
             'legacy_mount_path_rebound')
    finally:os.close(fresh)
    return row


def _no_nested_mounts(path,table):
    path=Path(path)
    need(not any(Path(r['mountpoint'])!=path and Path(r['mountpoint']).is_relative_to(path)
                 for r in table.values()),'legacy_nested_mount_unsupported')


def _native_mounts(pid,pgdata,data_fd,wal_fd):
    # These are the live postmaster's namespace IDs, not the host dataset IDs.
    table=_mounts(pid);mid=_fd_mount_id(data_fd);row=table.get(str(mid))
    info=os.fstat(data_fd)
    need(row is not None and Path(pgdata).is_relative_to(row['mountpoint']) and
         row['device']==str(os.major(info.st_dev))+':'+str(os.minor(info.st_dev)) and
         _fd_mount_id(wal_fd)==mid,'legacy_native_mount_changed')
    _no_nested_mounts(pgdata,table)


def _recheck_native_mounts(backend,op,c,native,deadline):
    pid=native['process']['pid'];ticks=native['process']['start_ticks']
    def alive():
        state=backend._inspect(op,'primary',deadline=deadline)['state']
        need(state['Running'] is True and state['Pid']==pid and
             Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()[19]==ticks,
             'legacy_native_mount_process_changed')
    alive();data=wal=None
    try:
        data=backend._container_directory(pid,c['pgdata']['container_path'])
        wal=backend._container_directory(pid,c['pgdata']['container_path']+'/pg_wal')
        for actual,host in ((data,native['data_fd']),(wal,native['wal_fd'])):
            a=os.fstat(actual);b=os.fstat(host)
            need((a.st_dev,a.st_ino)==(b.st_dev,b.st_ino),'legacy_native_mount_inode_changed')
        _native_mounts(pid,c['pgdata']['container_path'],data,wal);alive()
    finally:
        for fd in (data,wal):
            if fd is not None:os.close(fd)


def _native_primary(backend,op,c,deadline=None):
    current=backend._inspect(op,'primary',deadline=deadline);state=current['state'];pid=state['Pid']
    need(state['Running'] is True and not any(state.get(k,False) for k in ('Paused','Restarting','Dead','OOMKilled'))
         and type(pid) is int and pid>0,'legacy_primary_not_running')
    process={'pid':pid,'start_ticks':Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()[19],
             'boot_id':op.inventory['host']['boot_id']}
    need(Path('/proc/sys/kernel/random/boot_id').read_text().strip()==process['boot_id'],'legacy_primary_boot')
    pgdata=c['pgdata'];need(type(pgdata) is dict and set(pgdata)=={'host_path','container_path','identity'} and
        pgdata['container_path']=='/var/lib/postgresql/18/docker','legacy_pgdata_policy')
    candidates=[r for r in current['mounts'] if Path(pgdata['container_path']).is_relative_to(r['destination'])]
    need(candidates,'legacy_pgdata_mount_missing');mount=max(candidates,key=lambda r:len(Path(r['destination']).parts))
    need(mount['type']=='bind' and mount['rw'] is True and
         Path(mount['source'])/Path(pgdata['container_path']).relative_to(mount['destination'])==Path(pgdata['host_path']),
         'legacy_pgdata_mount_changed')
    host=root_fd(pgdata['host_path']);native=wal=cwd=exe=tools=group=None
    try:
        native=backend._container_directory(pid,pgdata['container_path'])
        a=os.fstat(host);b=os.fstat(native)
        need(a.st_uid==a.st_gid==70 and stat.S_IMODE(a.st_mode)==0o700 and
             (a.st_dev,a.st_ino)==(b.st_dev,b.st_ino)==(pgdata['identity']['dev'],pgdata['identity']['ino']),
             'legacy_pgdata_native_identity')
        cwd=os.open('/proc/'+str(pid)+'/cwd',os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
        need((os.fstat(cwd).st_dev,os.fstat(cwd).st_ino)==(a.st_dev,a.st_ino),'legacy_effective_pgdata_cwd')
        w=os.stat('pg_wal',dir_fd=host,follow_symlinks=False)
        need(stat.S_ISDIR(w.st_mode) and w.st_dev==a.st_dev,'legacy_external_wal_unsupported')
        wal=os.open('pg_wal',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=host)
        nativewal=backend._container_directory(pid,pgdata['container_path']+'/pg_wal')
        try:
            need((os.fstat(nativewal).st_dev,os.fstat(nativewal).st_ino)==(w.st_dev,w.st_ino),'legacy_native_wal_changed')
            _native_mounts(pid,pgdata['container_path'],native,nativewal)
        finally:os.close(nativewal)
        spaces=os.open('pg_tblspc',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=host)
        try:need(os.listdir(spaces)==[],'legacy_external_tablespace_unsupported')
        finally:os.close(spaces)
        exe=os.open('/proc/'+str(pid)+'/exe',os.O_RDONLY|os.O_CLOEXEC);_hash_file(exe,c['image']['postgres_sha256'])
        tools=backend._container_directory(pid,'/usr/local/bin')
        tool=os.open('pg_controldata',os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=tools)
        try:_hash_file(tool,c['image']['pg_controldata_sha256'])
        finally:os.close(tool)
        need(Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()[19]==process['start_ticks'],'legacy_primary_reused')
        result={'process':process,'data_fd':host,'wal_fd':wal,'data_identity':{'dev':a.st_dev,'ino':a.st_ino},
                'groups':linux.process_cgroup(pid),'restart_policy':current['restart_policy']}
        cid=op.inventory['linux']['postgres']['primary']['container_id']
        need(len(result['groups'])==1 and result['groups'][0].startswith('0::/') and
             any(part in (cid,'docker-'+cid+'.scope') for part in Path(result['groups'][0][3:]).parts),
             'legacy_primary_cgroup')
        group=root_fd('/sys/fs/cgroup'+result['groups'][0][3:])
        gi=os.fstat(group);result.update(cgroup_fd=group,cgroup_identity={'dev':gi.st_dev,'ino':gi.st_ino},
            data_path=pgdata['host_path'],wal_identity={'dev':w.st_dev,'ino':w.st_ino})
        need(linux.process_cgroup(pid)==result['groups'] and
             backend._inspect(op,'primary',deadline=deadline)['state']['Pid']==pid and
             Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()[19]==process['start_ticks'],
             'legacy_primary_changed_during_capture')
        host=wal=group=None
        return result
    finally:
        for fd in (host,native,wal,cwd,exe,tools,group):
            if fd is not None:os.close(fd)


def _control(backend,op,c,deadline):
    cid=op.inventory['linux']['postgres']['primary']['container_id']
    raw=backend._run(op,'docker',['exec','-u','70','-e','LC_ALL=C','-e','LANG=C',cid,
        '/usr/local/bin/pg_controldata',c['pgdata']['container_path']],deadline=deadline)
    values={}
    for line in raw.decode('ascii','strict').splitlines():
        if ':' not in line:continue
        name,value=line.split(':',1)
        if name in ('Database system identifier',"Latest checkpoint's TimeLineID"):
            need(name not in values,'legacy_control_duplicate');values[name]=value.strip()
    need(set(values)=={'Database system identifier',"Latest checkpoint's TimeLineID"} and
         values['Database system identifier']==c['expected_system_identifier'] and
         values["Latest checkpoint's TimeLineID"]==str(c['expected_timeline']),'legacy_control_identity')
    return values


def _images(backend,op,c,deadline):
    pg=op.inventory['linux']['postgres']['primary'];out=[]
    for ref in (c['image']['reference'],pg['image_id']):
        value=decode(backend._run(op,'docker',['image','inspect','--format',IMAGE_FORMAT,ref],deadline=deadline))
        need(value=={'Id':c['image']['inspect_id'],'Os':'linux',
            'Architecture':{'x86_64':'amd64','aarch64':'arm64'}.get(platform.machine())},'legacy_native_image_domain')
        out.append(value)
    return {'container_image_id':pg['image_id'],'official_reference':c['image']['reference'],'actual_inspect':out}


def _blob_state(backend,op,c,deadline,label):
    result={};held=[]
    try:
        for role in sorted(ROLES):
            row=c['blob_roots'][role];need(type(row) is dict and set(row)=={'path','identity'},'legacy_blob_root_policy')
            fd=root_fd(row['path'],noatime=True);held.append(fd);meta=os.fstat(fd)
            need({'dev':meta.st_dev,'ino':meta.st_ino}==row['identity'],'legacy_blob_root_rebound')
            table=_mounts();mount=_path_mount(fd,row['path'],table);_no_nested_mounts(row['path'],table)
            value=blobs.blob_inventory(row['path'],deadline=deadline,controls=True,online=False)
            need(value['root_identity']==row['identity'],'legacy_blob_scan_root_changed')
            table=_mounts();need(_path_mount(fd,row['path'],table)==mount,'legacy_blob_mount_changed')
            _no_nested_mounts(row['path'],table)
            result[role]=value
        _save(backend,op,'blob-manifests-'+label,result)
        retained=getattr(backend,'_legacy_prepare_blob_fds',[])
        retained.extend(held);backend._legacy_prepare_blob_fds=retained;held=[]
        return result
    finally:
        for fd in held:os.close(fd)


def _stopped(backend,op,native,deadline=None):
    current=backend._inspect(op,'primary',deadline=deadline);state=current['state']
    need(state['Running'] is False and state['Pid']==0 and type(state['ExitCode']) is int and state['ExitCode']==0,
         'legacy_primary_exit_unknown')
    need(current['restart_policy']=={'Name':'no','MaximumRetryCount':0},'legacy_stopped_restart_policy')
    _process_absent(native['process']);need(not linux.members(native['groups']),'legacy_primary_cgroup_not_closed')
    for key in ('data','wal'):
        info=os.fstat(native[key+'_fd']);path=Path(native['data_path'])/('pg_wal' if key=='wal' else '')
        now=path.lstat();need(stat.S_ISDIR(info.st_mode) and stat.S_ISDIR(now.st_mode) and
            (info.st_dev,info.st_ino)==(now.st_dev,now.st_ino)==
            (native[key+'_identity']['dev'],native[key+'_identity']['ino']),'legacy_primary_descriptor')
    info=os.fstat(native['cgroup_fd'])
    need((info.st_dev,info.st_ino)==(native['cgroup_identity']['dev'],native['cgroup_identity']['ino']),
         'legacy_primary_cgroup_rebound')


def _datasets(backend,op,c,deadline,native):
    """Actual ZFS mountpoints cover the selected native PG and HDD Blob roots."""
    roots={};held=getattr(backend,'_legacy_prepare_dataset_fds',{})
    backend._legacy_prepare_dataset_fds=held
    for row in op.inventory['original_datasets']:
        raw=backend._run(op,'zfs',['get','-Hp','-o','property,value','guid,mountpoint,mounted',row['name']],deadline=deadline)
        pairs=[line.split('\t') for line in raw.decode('ascii','strict').splitlines()]
        need(len(pairs)==3 and all(len(r)==2 for r in pairs),'legacy_zfs_properties')
        values=dict(pairs)
        need(values=={'guid':row['guid'],'mountpoint':row['mountpoint'],'mounted':'yes'},'legacy_dataset_binding')
        if row['name'] not in held:
            held[row['name']]=root_fd(row['mountpoint'])
    table=_mounts()
    for row in op.inventory['original_datasets']:
        fd=held[row['name']];mount=_path_mount(fd,row['mountpoint'],table)
        need(mount['filesystem']=='zfs' and mount['source']==row['name'] and mount['root']=='/' and
             mount['mountpoint']==row['mountpoint'],'legacy_dataset_kernel_mount')
        _no_nested_mounts(row['mountpoint'],table);roots[row['name']]=mount
    sources=getattr(backend,'_legacy_prepare_source_fds',{});backend._legacy_prepare_source_fds=sources
    selected=[('openlegal-postgres',c['pgdata']['host_path'],native['data_fd']),
              ('openlegal-postgres',str(Path(c['pgdata']['host_path'])/'pg_wal'),native['wal_fd'])]
    for role,name in (('cache_hdd','openlegal-cache-blobs'),('corpus_hdd','openlegal-corpus-blobs')):
        path=c['blob_roots'][role]['path']
        if path not in sources:sources[path]=root_fd(path)
        info=os.fstat(sources[path]);need({'dev':info.st_dev,'ino':info.st_ino}==c['blob_roots'][role]['identity'],
                                        'legacy_blob_root_rebound')
        selected.append((name,path,sources[path]))
    for name,path,fd in selected:
        mount=roots['cachepool/'+name];actual=_path_mount(fd,path,table)
        need(Path(path).is_relative_to(mount['mountpoint']) and actual['mount_id']==mount['mount_id'],
             'legacy_source_not_in_snapshot')
        _no_nested_mounts(path,table)
    count=getattr(backend,'_legacy_prepare_mount_sequence',0)+1;backend._legacy_prepare_mount_sequence=count
    _save(backend,op,'dataset-mounts-'+str(count),roots)


def _contain(backend,op,c,native,deadline):
    """One stop-only attempt after a possibly dispatched restart; never retry."""
    current=backend._inspect(op,'primary',deadline=deadline)
    need(current['restart_policy']=={'Name':'no','MaximumRetryCount':0},'legacy_containment_restart_policy')
    captured=None
    if current['state']['Running'] is True:
        # Capture exact CID/image, root binding, actual PID/start and cgroup
        # before the sole containment stop. Ambiguity refuses dispatch.
        captured=_native_primary(backend,op,c,deadline)
        backend._legacy_prepare_containment_native=captured
    else:
        _stopped(backend,op,native,deadline)
        return {'status':'already_closed','stop_dispatched':False}
    cid=op.inventory['linux']['postgres']['primary']['container_id']
    _save(backend,op,'containment-stop-intent',{'id':cid,'process':captured['process'],
        'groups':captured['groups'],'pgdata_identity':captured['data_identity'],'one_stop_only':True})
    need(backend._run(op,'docker',['stop','--time','90',cid],timeout=100,deadline=deadline).decode().strip()==cid,
         'legacy_containment_stop_ack_unknown')
    need(backend._run(op,'docker',['wait',cid],deadline=deadline)==b'0\n','legacy_containment_wait_unknown')
    _stopped(backend,op,captured,deadline)
    return {'status':'closed','stop_dispatched':True,'process':captured['process']}


def run(backend,op):
    c,expected=_scope(backend,op);deadline=time.monotonic()+c['deadline_secs'];native=fresh=None
    stop_may_dispatched=restart_may_dispatched=False
    cid=op.inventory['linux']['postgres']['primary']['container_id']
    try:
        _parent_anchor(backend,op);_guard(backend,op)
        drain=_fence(backend,op,deadline)
        before=migration._sessions(backend,op,deadline)
        need(migration._sql(backend,op,migration.MIGRATIONS_SQL,deadline)==[dict(r,success=True) for r in expected],
             'legacy_actual_schema27')
        old=migration._old_state(backend,op,deadline)
        images=_images(backend,op,c,deadline);native=_native_primary(backend,op,c,deadline)
        _datasets(backend,op,c,deadline,native)
        control=_control(backend,op,c,deadline);physical=_blob_state(backend,op,c,deadline,'before')
        _datasets(backend,op,c,deadline,native)
        need(native['restart_policy'] in ({'Name':'unless-stopped','MaximumRetryCount':0},
                                        {'Name':'no','MaximumRetryCount':0}),'legacy_restart_policy_scope')
        _save(backend,op,'restart-policy-intent',{'id':cid,'images':images,'original_restart_policy':native['restart_policy'],
            'new_restart_policy':{'Name':'no','MaximumRetryCount':0},'automatic_restore':False})
        backend._run(op,'docker',['update','--restart=no',cid],deadline=deadline)
        need(backend._inspect(op,'primary')['restart_policy']=={'Name':'no','MaximumRetryCount':0},'legacy_restart_fence_unknown')
        migration._fence(backend,op,drain);migration._sessions(backend,op,deadline,before)
        _recheck_native_mounts(backend,op,c,native,deadline)
        _save(backend,op,'primary-stop-intent',{'id':cid,'process':native['process'],'groups':native['groups'],
            'pgdata_identity':native['data_identity'],'wal_identity':native['wal_identity'],
            'cgroup_identity':native['cgroup_identity'],'control_identity':control,
            'old_state_sha256':digest(canonical(old))})
        stop_may_dispatched=True
        need(backend._run(op,'docker',['stop','--time','90',cid],timeout=100,deadline=deadline).decode().strip()==cid,
             'legacy_stop_ack_unknown')
        need(backend._run(op,'docker',['wait',cid],deadline=deadline)==b'0\n','legacy_primary_actual_wait')
        _stopped(backend,op,native,deadline);migration._fence(backend,op,drain)
        _datasets(backend,op,c,deadline,native)
        snapshots=[];tag='openlegal-initial-'+op.operation_id.replace('-','')
        for row in sorted(op.inventory['original_datasets'],key=lambda r:r['name']):
            actual=backend._run(op,'zfs',['get','-Hp','-o','value','guid',row['name']],deadline=deadline).decode().strip()
            need(actual==row['guid'],'legacy_dataset_guid_changed')
            snapshots.append({'dataset':row['name'],'dataset_guid':row['guid'],'snapshot':row['name']+'@'+tag})
        argv=['snapshot',*[r['snapshot'] for r in snapshots]]
        _save(backend,op,'atomic-snapshot-intent',{'argv':argv,'old_state_sha256':digest(canonical(old)),
            'blob_manifests_sha256':digest(canonical(physical)),'primary_process':native['process']})
        _stopped(backend,op,native,deadline)
        _datasets(backend,op,c,deadline,native)
        previous=set(op.journal.output_hashes());backend._run(op,'zfs',argv,timeout=120,deadline=deadline)
        names=[n for n in op.journal.output_hashes() if n not in previous and re.fullmatch(op.phase+'-command-[0-9]+.json',n)]
        need(len(names)==1,'legacy_snapshot_actual_command_missing')
        hashes=op.journal.output_hashes();command=decode(op.journal.read_phase_bytes(op.phase,names[0],hashes[names[0]]))
        need(command['binary']=='zfs' and command['arguments_sha256']==digest(canonical(argv)) and
             type(command['actual_exit_code']) is int and command['actual_exit_code']==0 and
             all(command[k] is True for k in ('started','owned_processes_closed')) and
             all(command[k] is False for k in ('expired','interrupted','unexpected_descendants','output_limit_exceeded')),
             'legacy_snapshot_command_unclosed')
        actual_command={k:command[k] for k in ('binary','arguments_sha256','actual_exit_code','started','owned_processes_closed',
            'expired','interrupted','unexpected_descendants','output_limit_exceeded')}
        actual_command['binary_sha256']=op.inventory['linux']['binaries']['zfs']['sha256']
        for row in snapshots:
            guid=backend._run(op,'zfs',['get','-Hp','-o','value','guid',row['snapshot']],deadline=deadline).decode().strip()
            need(re.fullmatch('[0-9]{1,20}',guid),'legacy_snapshot_guid');row['snapshot_guid']=guid
        _stopped(backend,op,native,deadline);migration._fence(backend,op,drain)
        need(_blob_state(backend,op,c,deadline,'stopped')==physical,'legacy_blob_generations_changed')
        _datasets(backend,op,c,deadline,native)
        _save(backend,op,'primary-restart-intent',{'id':cid,'original_process':native['process'],
            'restart_policy_remains':'no','snapshots':snapshots,'applications_remain_fenced':True})
        op.seal();need(time.monotonic()<deadline,'legacy_restart_deadline');restart_may_dispatched=True
        need(backend._run(op,'docker',['start',cid],deadline=deadline).decode().strip()==cid,'legacy_restart_ack_unknown')
        # One source-fixed readiness command polls only local native UDS. No
        # start/migration/SQL mutation is replayed on slow startup or unknown ACK.
        backend._run(op,'docker',['exec','-u','70',cid,'/bin/sh','-ec',
            'n=0; until /usr/local/bin/pg_isready -q -h /var/run/postgresql -p 5432; do '
            'n=$((n+1)); [ "$n" -lt 120 ] || exit 124; sleep 0.25; done'],timeout=40,deadline=deadline)
        after=migration._sessions(backend,op,deadline)
        fresh=_native_primary(backend,op,c,deadline);need(fresh['process']!=native['process'],'legacy_postmaster_not_fresh')
        need(fresh['restart_policy']=={'Name':'no','MaximumRetryCount':0},'legacy_restart_policy_changed')
        need(after['postmaster_started_at']!=before['postmaster_started_at'] and
             {k:v for k,v in after.items() if k!='postmaster_started_at'}==
             {k:v for k,v in before.items() if k!='postmaster_started_at'},'legacy_restart_sql_identity')
        need(_control(backend,op,c,deadline)==control,'legacy_restart_control_changed')
        need(migration._old_state(backend,op,deadline,list(old['tables']))==old and
             migration._sql(backend,op,migration.MIGRATIONS_SQL,deadline)==[dict(r,success=True) for r in expected],
             'legacy_old_rows_or_references_changed')
        need(_blob_state(backend,op,c,deadline,'after')==physical,'legacy_blob_generations_changed')
        _datasets(backend,op,c,deadline,fresh)
        _recheck_native_mounts(backend,op,c,fresh,deadline)
        migration._fence(backend,op,drain);migration._sessions(backend,op,deadline,after)
        final=backend._inspect(op,'primary',deadline=deadline)
        need(final['state']['Running'] is True and final['state']['Pid']==fresh['process']['pid'] and
             final['restart_policy']=={'Name':'no','MaximumRetryCount':0} and
             Path('/proc/'+str(fresh['process']['pid'])+'/stat').read_text().rsplit(')',1)[1].split()[19]==
             fresh['process']['start_ticks'],'legacy_final_primary_changed')
        socketfd=backend._container_directory(fresh['process']['pid'],'/var/run/postgresql')
        try:
            meta=os.fstat(socketfd);node=os.stat('.s.PGSQL.5432',dir_fd=socketfd,follow_symlinks=False)
            need(stat.S_ISSOCK(node.st_mode) and node.st_uid==node.st_gid==70,'legacy_fresh_socket')
            primary={'primary_process':fresh['process'],'socket_directory':{'dev':meta.st_dev,'ino':meta.st_ino},
                     'socket':{'dev':node.st_dev,'ino':node.st_ino}}
            need(Path('/proc/'+str(fresh['process']['pid'])+'/stat').read_text().rsplit(')',1)[1].split()[19]==
                 fresh['process']['start_ticks'],'legacy_socket_primary_reused')
        finally:os.close(socketfd)
        result={'kind':'openlegal_legacy_prepare_payloads','status':'PENDING_PARENT_PRODUCER_OS_EXIT',
            'operation_id':op.operation_id,'payloads':{'drain':drain,'snapshot':{'snapshots':snapshots,
            'actual_command':actual_command},'preservation':old},'fresh_primary':primary,
            'original_restart_policy':native['restart_policy'],'current_restart_policy':'no',
            'blob_manifests_sha256':digest(canonical(physical)),'producer_os_exit':'NOT_YET_OBSERVABLE',
            'ready':False,'provider_state_modified':False,'strict_seed_created':False}
        _save(backend,op,'returned-payloads',result);need(time.monotonic()<deadline,'legacy_completion_deadline')
        return result
    except BaseException:
        containment={'status':'not_attempted','stop_dispatched':False}
        if restart_may_dispatched:
            try:
                # Bounded safety-only work has its own deadline. A failed or
                # blocked stop remains UNKNOWN; ownership/guard are retained.
                containment=_contain(backend,op,c,fresh or native,time.monotonic()+120)
            except BaseException:containment={'status':'UNKNOWN','stop_dispatched':'MAYBE','retry':False}
        try:_save(backend,op,'failed',{'status':'FAILED_OR_UNKNOWN','stop_may_dispatched':stop_may_dispatched,
            'restart_may_dispatched':restart_may_dispatched,'automatic_retry':False,'automatic_restore':False,
            'application_resume':False,'containment':containment,'producer_os_exit':'NOT_YET_OBSERVABLE'})
        except BaseException:pass
        raise
    finally:
        # Keep native source descriptors available to parent-side reconciliation
        # while this producer process remains alive; no claimed exit from here.
        held=set(getattr(backend,'_legacy_prepare_primary_fds',[]))
        held.update(r for row in (native,fresh,
            getattr(backend,'_legacy_prepare_containment_native',None)) if row is not None
            for r in (row['data_fd'],row['wal_fd'],row['cgroup_fd']))
        backend._legacy_prepare_primary_fds=sorted(held)
