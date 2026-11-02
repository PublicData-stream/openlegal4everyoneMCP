"""Shared certificate-free epoch observations and strict configuration hashing.

No emergency validator, certificate input, producer dispatch or Ready side effect.
Wrappers must validate their own live authority before using private observations.
Logical roles name the real roots; no suffix-derived physical-media selection.
"""
from __future__ import annotations

import math
import os
import platform
from pathlib import Path
import re
import stat
import time

import linux_backend as linux
import manual_backend as manual
from interface import need
from inventory import canonical, decode, digest, pin, valid_uuid

DOMAIN = b'openlegal-strict-config-v1\n'
TOPOLOGY = 'cache_hdd_primary_corpus_ssd_primary_v1'
SSD_SURVIVORS = ('cache_replica', 'corpus_primary')
HDD_LOST = ('cache_primary', 'corpus_replica')

def _root(row):
    manual._shape(row, ('path', 'dev', 'ino', 'uid', 'gid', 'mode', 'kernel_filesystem_id'),
           'survivor_root_descriptor')
    manual._path(row['path'])
    need(all(type(row[k]) is int and row[k] >= 0 for k in (
        'dev', 'ino', 'uid', 'gid', 'mode', 'kernel_filesystem_id')) and row['mode'] == 0o700,
        'survivor_root_identity')


def _blob_root(row):
    manual._shape(row, ('path', 'dev', 'ino', 'uid', 'gid', 'mode', 'kernel_filesystem_id',
                 'container_path', 'filesystem_id', 'marker_sha256'), 'survivor_blob_descriptor')
    _root({k: row[k] for k in ('path', 'dev', 'ino', 'uid', 'gid', 'mode', 'kernel_filesystem_id')})
    manual._path(row['container_path'])
    need(type(row['filesystem_id']) is str and 0 < len(row['filesystem_id']) <= 256
         and pin(row['marker_sha256']), 'survivor_blob_marker_pin')


PROFILE_FIELDS = {'schema_version','kind','namespace','runtime','deployments','jobs','configmaps',
                  'persistent_volumes','persistent_volume_claims','service','survivor','blob_survivors','topology'}


def _plain(value):
    if hasattr(value, 'items'): return {k:_plain(v) for k,v in value.items()}
    if isinstance(value, (tuple,list)): return [_plain(v) for v in value]
    return value


def _shape(value, fields, code):
    need(type(value) is dict and set(value) == set(fields), code)


def configuration_hash_v1(projection):
    """Pure shared schema2 projection; retain the v1 hash domain and exact bytes."""
    _shape(projection, PROFILE_FIELDS, 'emergency_config_projection')
    need(type(projection['schema_version']) is int and projection['schema_version'] == 2
         and projection['kind'] == 'openlegal_strict_config', 'emergency_config_version')
    need(projection['topology'] == TOPOLOGY, 'epoch_config_topology')
    linux.identifier(projection['namespace'],linux.KUBE_NAME)
    for key in ('deployments','jobs','configmaps','persistent_volumes','persistent_volume_claims'):
        need(type(projection[key]) is list and len(projection[key]) <= 256, 'emergency_config_bound')
        order=[(r['kind'],r.get('namespace',''),r['name']) for r in projection[key]]
        need(order == sorted(set(order)), 'emergency_config_order')
    rows = {
        'deployments':{'kind','name','namespace','uid','spec_sha256','selector_sha256','template_sha256'},
        'jobs':{'kind','name','namespace','uid','spec_sha256'},
        'configmaps':{'kind','name','namespace','uid','data_sha256','binary_data_sha256','immutable'},
        'persistent_volumes':{'kind','name','uid','spec_sha256'},
        'persistent_volume_claims':{'kind','name','namespace','uid','spec_sha256','volume_name'}}
    for key,fields in rows.items():
        for row in projection[key]:
            _shape(row,fields,'emergency_config_resource_shape')
            expected={'deployments':'deployment','jobs':'job','configmaps':'configmap',
                'persistent_volumes':'persistentvolume','persistent_volume_claims':'persistentvolumeclaim'}[key]
            need(row['kind']==expected,'emergency_config_resource_kind')
            linux.identifier(row['name'],linux.KUBE_NAME)
            if 'namespace' in row:need(row['namespace']==projection['namespace'],'emergency_config_namespace')
            need(valid_uuid(row['uid']) and all(pin(v) for k,v in row.items() if k.endswith('_sha256')),
                 'emergency_config_resource_pin')
            if key=='configmaps':need(row['immutable'] is None or type(row['immutable']) is bool,
                                     'emergency_config_configmap_immutable')
            if key=='persistent_volume_claims':linux.identifier(row['volume_name'],linux.KUBE_NAME)
    _shape(projection['runtime'], {'reference','docker_image_id','revision','os','architecture'},
           'emergency_config_runtime')
    _shape(projection['service'], {'kind','name','namespace','uid','spec_sha256','selector_sha256'},
           'emergency_config_service')
    _shape(projection['survivor'], {'container_id','image_id','data_directory','data_root','wal_root'},
           'emergency_config_survivor')
    _shape(projection['blob_survivors'], set(SSD_SURVIVORS), 'emergency_config_blob_roles')
    runtime=projection['runtime'];survivor=projection['survivor'];service=projection['service']
    need(type(runtime['reference']) is str and
         re.fullmatch(r'ghcr.io/publicdata-stream/openlegal-server@sha256:[0-9a-f]{64}',runtime['reference'])
         and type(runtime['docker_image_id']) is str and re.fullmatch('sha256:[0-9a-f]{64}',runtime['docker_image_id'])
         and type(runtime['revision']) is str and re.fullmatch('[0-9a-f]{40}',runtime['revision'])
         and runtime['os']=='linux' and runtime['architecture'] in ('amd64','arm64'), 'emergency_config_runtime_pin')
    need(service['kind']=='service' and service['namespace']==projection['namespace']
         and valid_uuid(service['uid']) and pin(service['spec_sha256']) and pin(service['selector_sha256']),
         'emergency_config_service_pin');linux.identifier(service['name'],linux.KUBE_NAME)
    need(pin(survivor['container_id']) and type(survivor['image_id']) is str
         and re.fullmatch('sha256:[0-9a-f]{64}',survivor['image_id']), 'emergency_config_survivor_pin')
    manual._path(survivor['data_directory'])
    for key in ('data_root','wal_root'):_root(survivor[key])
    for root in projection['blob_survivors'].values():_blob_root(root)
    return digest(DOMAIN + canonical(projection))


def _check(deadline):
    need(type(deadline) in (float,int) and math.isfinite(deadline)
         and time.monotonic() < deadline, 'emergency_native_original_deadline')


def _descriptor(path,fd):
    s=os.fstat(fd)
    return {'path':str(path),'dev':s.st_dev,'ino':s.st_ino,'uid':s.st_uid,'gid':s.st_gid,
            'mode':stat.S_IMODE(s.st_mode),'kernel_filesystem_id':os.fstatvfs(fd).f_fsid}


def _directory(path):
    path=Path(path)
    need(path.is_absolute() and path.resolve() == path, 'emergency_native_path')
    return os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NOATIME)


def _fd_mount_id(fd):
    with open('/proc/self/fdinfo/'+str(fd),'rb') as stream:raw=stream.read(16385)
    need(len(raw)<=16384,'emergency_fdinfo_bound')
    ids=[row.split(b':',1)[1].strip() for row in raw.splitlines() if row.startswith(b'mnt_id:')]
    need(len(ids)==1 and ids[0].isdigit(),'emergency_fd_mount_id')
    return int(ids[0])


def _fd_identity(fd):
    info=os.fstat(fd)
    return (info.st_dev,info.st_ino,os.fstatvfs(fd).f_fsid,_fd_mount_id(fd))


def _kernel_mountinfo(pid,deadline):
    _check(deadline)
    name='/proc/self/mountinfo' if pid is None else '/proc/'+str(pid)+'/mountinfo'
    with open(name,'rb') as stream:raw=stream.read(16*1024**2+1)
    _check(deadline);need(len(raw)<=16*1024**2,'emergency_mountinfo_bound')
    return raw.decode('utf8')


def _mounts(pid,deadline):
    raw=_kernel_mountinfo(pid,deadline);lines=raw.splitlines()
    need(len(lines)<=65536,'emergency_mountinfo_bound');result={}
    def text(value):return re.sub(r'\\(040|011|012|134)',lambda m:chr(int(m[1],8)),value)
    for line in lines:
        _check(deadline);row=line.split();need('-' in row,'emergency_mountinfo_shape');split=row.index('-')
        need(split>=6 and len(row)==split+4 and row[0].isdecimal() and row[0] not in result
             and re.fullmatch('[0-9]+:[0-9]+',row[2]),'emergency_mountinfo_shape')
        result[row[0]]={'mount_id':int(row[0]),'device':row[2],'root':text(row[3]),
            'mountpoint':text(row[4]),'filesystem':row[split+1],'source':text(row[split+2])}
    return result


def _mount_pair(data,wal,path,table):
    # IDs are looked up ONLY in the namespace from which these FDs were opened.
    # A host ID is never compared to a foreign namespace's numeric ID.
    mid=_fd_mount_id(data);row=table.get(str(mid));info=os.fstat(data)
    need(row is not None and Path(path).is_relative_to(row['mountpoint'])
         and row['device']==str(os.major(info.st_dev))+':'+str(os.minor(info.st_dev))
         and _fd_mount_id(wal)==mid and os.fstat(wal).st_dev==info.st_dev,
         'emergency_inline_wal_mount_changed')
    need(not any(Path(r['mountpoint'])!=Path(path) and Path(r['mountpoint']).is_relative_to(path)
                 for r in table.values()),'emergency_nested_data_mount_unsupported')
    return row


def _inline_layout(pid,path,host,data,wal,native,nativewal,deadline):
    _check(deadline)
    host_mount=_mount_pair(data,wal,host,_mounts(None,deadline))
    native_mount=_mount_pair(native,nativewal,path,_mounts(pid,deadline))
    fresh=[]
    try:
        for name in (host,Path(host)/'pg_wal'):fresh.append(_directory(name))
        for name in (path,path+'/pg_wal'):
            fresh.append(linux.LinuxBackend._container_directory(pid,name))
        need(all(_fd_identity(a)==_fd_identity(b) for a,b in zip((data,wal,native,nativewal),fresh)),
             'emergency_native_mount_path_rebound')
        tables=os.stat('pg_tblspc',dir_fd=data,follow_symlinks=False)
        need(stat.S_ISDIR(tables.st_mode),'emergency_external_tablespace_unsupported')
        fd=os.open('pg_tblspc',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=data)
        try:
            need((os.fstat(fd).st_dev,os.fstat(fd).st_ino)==(tables.st_dev,tables.st_ino)
                 and os.listdir(fd)==[],'emergency_external_tablespace_unsupported')
        finally:os.close(fd)
        _check(deadline);return {'host':host_mount,'native':native_mount}
    finally:
        for fd in fresh:os.close(fd)




def _verify_root(row):
    path = manual._path(row['path'])
    need(path.resolve() == path, 'survivor_root_symlink')
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NOATIME)
    try:
        s = os.fstat(fd)
        need((s.st_dev, s.st_ino, s.st_uid, s.st_gid, stat.S_IMODE(s.st_mode))
             == tuple(row[k] for k in ('dev', 'ino', 'uid', 'gid', 'mode'))
             and os.fstatvfs(fd).f_fsid == row['kernel_filesystem_id'], 'survivor_actual_root_changed')
        now = path.lstat()
        need((now.st_dev, now.st_ino) == (s.st_dev, s.st_ino), 'survivor_root_rebound')
    finally: os.close(fd)


def _verify_marker(row, role, epoch):
    root = os.open(row['path'], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    fd = None
    try:
        s = os.fstat(root)
        need((s.st_dev, s.st_ino) == (row['dev'], row['ino']), 'survivor_marker_root_replaced')
        fd = os.open('.openlegal-seed-v1.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                     | os.O_CLOEXEC | os.O_NOATIME, dir_fd=root)
        before = os.fstat(fd)
        need(stat.S_ISREG(before.st_mode) and before.st_uid == 0 and before.st_nlink == 1
             and before.st_dev == s.st_dev and stat.S_IMODE(before.st_mode) in (0o640, 0o644)
             and 0 < before.st_size <= 16384, 'survivor_original_marker_metadata')
        raw = os.read(fd, 16385)
        after = os.fstat(fd); named = os.stat('.openlegal-seed-v1.json', dir_fd=root, follow_symlinks=False)
        need(len(raw) == before.st_size and digest(raw) == row['marker_sha256']
             and all(getattr(before, k) == getattr(after, k) == getattr(named, k) for k in (
                 'st_dev', 'st_ino', 'st_size', 'st_ctime_ns', 'st_mtime_ns')), 'survivor_original_marker_changed')
        value = manual._metadata_decode(raw)
        _shape(value, ('schema_version', 'kind', 'managed_epoch', 'seed_operation', 'role',
            'manifest_sha256', 'correspondence_sha256', 'filesystem_id'), 'survivor_original_marker_shape')
        need(value['schema_version'] == 1 and type(value['schema_version']) is int
             and value['kind'] == 'openlegal_blob_seed' and value['managed_epoch'] == epoch
             and value['role'] == role and value['filesystem_id'] == row['filesystem_id']
             and valid_uuid(value['seed_operation']) and pin(value['manifest_sha256'])
             and pin(value['correspondence_sha256']), 'survivor_original_marker_identity')
    finally:
        if fd is not None: os.close(fd)
        os.close(root)


# Never request Config.Env, health-check output or daemon error text.
INSPECT = ('{"Id":{{json .Id}},"Image":{{json .Image}},"State":{'
           '"Running":{{json .State.Running}},"Paused":{{json .State.Paused}},'
           '"Restarting":{{json .State.Restarting}},"Dead":{{json .State.Dead}},'
           '"OOMKilled":{{json .State.OOMKilled}},"Pid":{{json .State.Pid}},'
           '"StartedAt":{{json .State.StartedAt}}},"Mounts":{{json .Mounts}}}')
IMAGE = ('{"Id":{{json .Id}},"Os":{{json .Os}},"Architecture":{{json .Architecture}},'
         '"Labels":{{json .Config.Labels}}}')
UNSET = ('PGSERVICE','PGSERVICEFILE','PGPASSWORD','PGHOST','PGHOSTADDR','PGPORT','PGDATABASE',
         'PGUSER','PGOPTIONS','PGSSLCERT','PGSSLKEY','PGSSLROOTCERT','PGSSLCRL','PGSSLCRLDIR',
         'PGTARGETSESSIONATTRS','PGCONNECT_TIMEOUT','PGREQUIREPEER','PGCHANNELBINDING',
         'PGSSLMODE','PGREQUIRESSL','PGGSSENCMODE','PGCLIENTENCODING')
IDENTITY = """jsonb_build_object('database',current_database(),'session_user',session_user,
 'current_user',current_user,'role',(SELECT jsonb_build_object('login',rolcanlogin,
 'superuser',rolsuper,'create_role',rolcreaterole,'create_db',rolcreatedb,
 'replication',rolreplication,'bypass_rls',rolbypassrls,'inherit',rolinherit)
 FROM pg_catalog.pg_roles WHERE rolname=current_user),
 'broad_roles',pg_has_role(current_user,'pg_read_all_stats','MEMBER') OR
 pg_has_role(current_user,'pg_read_all_settings','MEMBER'),
 'owner_membership',EXISTS(SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n
 ON n.oid=p.pronamespace WHERE (n.nspname,p.proname) IN
 (('openlegal_replication','survivor_position'),('openlegal_checkpoint','survivor_snapshot'),
 ('openlegal_checkpoint','client_sessions')) AND pg_has_role(current_user,p.proowner,'MEMBER')),
 'function_owners_qualified',(SELECT count(*)=3 AND bool_and(p.prosecdef AND NOT r.rolcanlogin
 AND NOT r.rolsuper AND NOT r.rolcreaterole AND NOT r.rolcreatedb AND NOT r.rolreplication
 AND NOT r.rolbypassrls AND r.rolname<>current_user) FROM pg_catalog.pg_proc p
 JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace JOIN pg_catalog.pg_roles r ON r.oid=p.proowner
 WHERE p.pronargs=0 AND p.prokind='f' AND (n.nspname,p.proname) IN
 (('openlegal_replication','survivor_position'),('openlegal_checkpoint','survivor_snapshot'),
 ('openlegal_checkpoint','client_sessions'))))"""
POSITION_SQL = 'SELECT openlegal_replication.survivor_position()'
SNAPSHOT_SQL = 'SELECT openlegal_checkpoint.survivor_snapshot()'
CLOSURE_SQL = 'SELECT openlegal_checkpoint.client_sessions()'
QUERIES = {'position': POSITION_SQL, 'snapshot': SNAPSHOT_SQL, 'sql-closure': CLOSURE_SQL}


CONTROL_FIELDS = {'managed_epoch', 'primary_system_identifier', 'timeline', 'standby_name',
    'standby_role', 'standby_addr', 'standby_client_dn', 'standby_issuer_dn',
    'standby_client_serial', 'blob_binding_sha256'}

def _subject_blob_binding(subject):
    rows = []
    for role, row in sorted(subject['blob_roots'].items()):
        rows.append({'role': role, 'container_path': row['container_path'],
            'filesystem_id': row['filesystem_id'], 'kernel_filesystem_id': row['kernel_filesystem_id'],
            'marker_sha256': row['marker_sha256'], 'root_dev': row['dev'], 'root_ino': row['ino'],
            'root_uid': row['uid'], 'root_gid': row['gid'], 'root_mode': row['mode']})
    return digest(b'openlegal-blob-seed-binding-v1\n' + canonical({
        'schema_version': 1, 'kind': 'openlegal_blob_seed_binding', 'managed_epoch': subject['managed_epoch'],
        'boot_id': subject['host_boot_id'], 'roots': rows}))


def _control(value):
    manual._shape(value, CONTROL_FIELDS, 'survivor_control_shape')
    need(valid_uuid(value['managed_epoch']) and pin(value['blob_binding_sha256'])
         and type(value['primary_system_identifier']) is str
         and re.fullmatch('[1-9][0-9]{0,19}', value['primary_system_identifier'])
         and type(value['timeline']) is int and 1 <= value['timeline'] <= 4294967295,
         'survivor_control_identity')
    for key in ('standby_name', 'standby_role'):
        need(type(value[key]) is str and manual.IDENTIFIER.fullmatch(value[key]), 'survivor_peer_name')
    manual._peer_address(value['standby_addr'], value['standby_addr'])
    need(type(value['standby_client_serial']) is str
         and re.fullmatch('[1-9][0-9]{0,59}', value['standby_client_serial'])
         and all(type(value[k]) is str and 0 < len(value[k].encode()) <= 1024
                 for k in ('standby_client_dn', 'standby_issuer_dn')), 'survivor_peer_certificate')



class NativeObservation:
    """Private source-owned read ports, independent of certificate authority.

    The emergency/healthy wrapper owns its separate authority and live-phase
    check. Only exact known backends are accepted; no inventory callbacks or
    SQL fragments are consumed. Driver commands retain their original end.
    """
    def __init__(self,backend):
        self.backend=backend;self.anchors={};self._backend_type()

    def _backend_type(self):
        if type(self.backend) is linux.LinuxBackend:
            selected=linux.LinuxBackend
        else:
            import initial_driver
            need(type(self.backend) is initial_driver.Backend,'epoch_fixed_backend')
            selected=initial_driver.Backend
        need(not any(callable(v) for v in vars(self.backend).values()),'epoch_fixed_backend')
        return selected

    def _deadline(self,end):
        self._backend_type();_check(end)
        if type(self.backend) is not linux.LinuxBackend:
            original=self.backend._driver_deadline
            need(type(original) is float and math.isfinite(original),'epoch_original_driver_deadline')
            end=min(end,original)
        _check(end);return end

    def _run(self,op,binary,args,deadline,**kw):
        end=self._deadline(deadline)
        value=self._backend_type()._run(self.backend,op,binary,args,deadline=end,**kw)
        self._deadline(end);return value

    def close(self):
        """Explicit operator teardown of read-only descriptor evidence only."""
        for row in self.anchors.values():
            for fd in row['fds']:os.close(fd)
        self.anchors.clear()

    def _inspect(self,op,subject,deadline,*,which='standby'):
        need(which in ('primary','standby'),'epoch_fixed_pg_selection')
        deadline=self._deadline(deadline)
        expected=subject[which];cid=expected['container_id']
        need(cid == op.inventory['linux']['postgres'][which]['container_id'], 'emergency_standby_selection')
        value=decode(self._run(op,'docker',['inspect','--format',INSPECT,cid],deadline))
        _shape(value, {'Id','Image','State','Mounts'}, 'emergency_native_inspect_shape')
        state=value['State']
        need(value['Id'] == cid and value['Image'] == expected['image_id']
             and state.get('Running') is True and type(state.get('Pid')) is int and state['Pid'] > 0
             and not any(state.get(k,False) for k in ('Paused','Restarting','Dead','OOMKilled')),
             'emergency_standby_native_identity')
        mounts=sorted([{'source':m['Source'],'destination':m['Destination'],'rw':m['RW'],'type':m['Type']}
                       for m in value['Mounts']],key=lambda m:m['destination'])
        need(mounts == op.inventory['linux']['postgres'][which]['mounts'], 'emergency_standby_mounts')
        value['Mounts']=mounts;return value

    def _native(self,op,subject,deadline,*,which='standby'):
        need(which in ('primary','standby'),'epoch_fixed_pg_selection')
        deadline=self._deadline(deadline)
        _check(deadline);expected=subject[which];value=self._inspect(op,subject,deadline,which=which)
        pid=value['State']['Pid'];process=expected['process']
        def alive():
            _check(deadline)
            need(pid == process['pid'] and Path('/proc/sys/kernel/random/boot_id').read_text().strip()
                 == process['boot_id'] == subject['host_boot_id'] and
                 Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()[19] == process['start_ticks'],
                 'emergency_standby_lifetime')
        alive();data=expected['data_directory'];mounts=[m for m in value['Mounts']
            if Path(data).is_relative_to(m['destination'])]
        need(mounts,'emergency_standby_data_unmounted');mount=max(mounts,key=lambda m:len(Path(m['destination']).parts))
        host=Path(mount['source'])/Path(data).relative_to(mount['destination'])
        need(mount['type']=='bind' and mount['rw'] is True and str(host)==expected['data_root']['path'],
             'emergency_standby_selected_root')
        fds=[]
        try:
            a=_directory(host);fds.append(a)
            need(_descriptor(host,a)==expected['data_root'],'emergency_standby_root_identity')
            need(expected['data_root']['uid']==op.inventory['linux']['postgres']['uid'], 'emergency_standby_uid')
            wal=os.stat('pg_wal',dir_fd=a,follow_symlinks=False)
            need(stat.S_ISDIR(wal.st_mode),'emergency_external_wal_unsupported')
            b=_directory(host/'pg_wal');fds.append(b)
            native=linux.LinuxBackend._container_directory(pid,data);fds.append(native)
            nativewal=linux.LinuxBackend._container_directory(pid,data+'/pg_wal');fds.append(nativewal)
            cwd=os.open('/proc/'+str(pid)+'/cwd',os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC);fds.append(cwd)
            need((os.fstat(a).st_dev,os.fstat(a).st_ino)==(os.fstat(native).st_dev,os.fstat(native).st_ino)
                 ==(os.fstat(cwd).st_dev,os.fstat(cwd).st_ino) and
                 (os.fstat(b).st_dev,os.fstat(b).st_ino)==(os.fstat(nativewal).st_dev,os.fstat(nativewal).st_ino),
                 'emergency_standby_native_fd_correspondence')
            layout=_inline_layout(pid,data,host,a,b,native,nativewal,deadline);alive()
            record={'container_id':value['Id'],'image_id':value['Image'],'data_directory':data,
                    'data_root':_descriptor(host,a),'wal_root':_descriptor(host/'pg_wal',b)}
            key=(op.operation_id,expected['container_id'])
            if key in self.anchors:
                previous=self.anchors[key]
                need(record == previous['record'] and
                     [_fd_identity(fd) for fd in fds[:4]]==previous['identities'] and
                     [_fd_identity(fd) for fd in previous['fds']]==previous['identities'],
                     'emergency_native_anchor_changed')
            else:
                held=[]
                try:
                    for fd in fds[:4]:held.append(os.dup(fd))
                except BaseException:
                    for fd in held:os.close(fd)
                    raise
                self.anchors[key]={'record':record,'fds':held,'identities':[_fd_identity(fd) for fd in held]}
            alive();after=self._inspect(op,subject,deadline,which=which)
            need(after['State']['Pid']==pid and after['State'].get('StartedAt')==value['State'].get('StartedAt'),
                 'emergency_standby_changed_during_read')
            alive();need(_inline_layout(pid,data,host,a,b,native,nativewal,deadline)==layout,
                         'emergency_inline_layout_changed_during_read')
            alive();return record
        finally:
            for fd in fds:os.close(fd)

    def _query_metadata(self,op,subject,kind,deadline,*,which='standby'):
        need(which in ('primary','standby'),'epoch_fixed_pg_selection')
        deadline=self._deadline(deadline)
        need(kind in ('position','snapshot','sql-closure'),'emergency_fixed_query')
        # M33 position/snapshot explicitly require recovery. A healthy primary
        # needs its separate fixed prefix/flush proof; never imply it here.
        need(which=='standby' or kind=='sql-closure','epoch_survivor_query_requires_standby')
        sql=QUERIES[kind]
        pg=op.inventory['linux']['postgres'];user=linux.identifier(pg['admin_user']);database=linux.identifier(pg['database'])
        before=self._native(op,subject,deadline,which=which)
        settings=("SET LOCAL statement_timeout='30000ms'; SET LOCAL lock_timeout='1000ms'; "
                  "SET LOCAL transaction_timeout='35000ms'; SET LOCAL idle_in_transaction_session_timeout='5000ms'; "
                  if kind=='snapshot' else
                  "SET LOCAL statement_timeout='2000ms'; SET LOCAL lock_timeout='1000ms'; "
                  "SET LOCAL transaction_timeout='5000ms'; SET LOCAL idle_in_transaction_session_timeout='5000ms'; ")
        script=(('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; ' if kind=='snapshot' else 'BEGIN READ ONLY; ')+settings+
            "SET LOCAL TimeZone='UTC'; SET LOCAL DateStyle='ISO,YMD'; SET LOCAL client_encoding='UTF8'; SET LOCAL search_path='pg_catalog'; "+
            "SELECT jsonb_build_object('identity',"+IDENTITY+",'result',("+sql[len('SELECT '):]+")); ROLLBACK;\n").encode()
        args=['exec','-i','-u',str(pg['uid']),'-e','PGPASSFILE=/dev/null',
              '-e','PGAPPNAME=openlegal_survivor_observer',subject[which]['container_id'],'/usr/bin/env']
        for name in UNSET:args.extend(['-u',name])
        args+=['psql','-X','-w','-h','/var/run/postgresql','-p','5432','-qAt','-P','pager=off','-v','ON_ERROR_STOP=1',
               '-U',user,'-d',database]
        raw=self._run(op,'docker',args,deadline,stdin=script,timeout=36 if kind=='snapshot' else 3)
        need(type(raw) is bytes and len(raw)<=linux.MAX_OUTPUT,'emergency_native_sql_output_bound')
        # Fixed bulk snapshot metadata and its hex-bearing OS receipt exceed
        # the small configuration parser. Reuse its existing bounded parser
        # only for this source-owned query; global command limits stay intact.
        parse=manual._metadata_decode if kind=='snapshot' else decode
        # Actual _run publishes this only after OS wait/owned closure and both
        # journal ACKs. A fabricated return value without its receipt is refused.
        name=op.phase+'-command-'+str(self.backend._sequence).zfill(4)+'.json'
        hashes=op.journal.output_hashes();need(name in hashes,'emergency_actual_command_missing')
        receipt=parse(op.journal.read_phase_bytes(op.phase,name,hashes[name]))
        config=self.backend._docker_config
        need(config is not None,'emergency_private_docker_context')
        dispatch=['--config',str(config[0]),'--host','unix:///var/run/docker.sock']
        need(receipt.get('binary')=='docker' and receipt.get('arguments_sha256')==digest(canonical(dispatch+args))
             and receipt.get('stdin_sha256')==digest(script) and receipt.get('stdout_sha256')==digest(raw)
             and receipt.get('stdout_bytes')==len(raw) and receipt.get('stdout_hex')==raw.hex()
             and type(receipt.get('actual_exit_code')) is int and receipt['actual_exit_code']==0
             and receipt.get('started') is True and receipt.get('owned_processes_closed') is True
             and all(receipt.get(k) is False for k in ('expired','interrupted','output_limit_exceeded','unexpected_descendants')),
             'emergency_actual_command_not_witnessed')
        value=parse(raw);_shape(value,{'identity','result'},'emergency_sql_result')
        identity=value['identity']
        need(identity == {'database':database,'session_user':user,'current_user':user,
             'role':{'login':True,'superuser':False,'create_role':False,'create_db':False,
                     'replication':False,'bypass_rls':False,'inherit':False},
             'broad_roles':False,'owner_membership':False,'function_owners_qualified':True}, 'emergency_observer_role')
        need(self._native(op,subject,deadline,which=which)==before,'emergency_native_changed_after_sql')
        _check(deadline)
        result={'container_id':subject[which]['container_id'],'sql':sql,'result':value['result'],
            'actual_command':{'phase':op.phase,'artifact':name,'sha256':hashes[name]}}
        self._deadline(deadline);return result

    def _kube(self,op,kind,name,deadline,namespace=True):
        need(kind in ('deployment','job','configmap','persistentvolume','persistentvolumeclaim','service'),
             'emergency_fixed_resource_kind');linux.identifier(name,linux.KUBE_NAME)
        config=op.inventory['linux'];linux.secure_path(config['kubeconfig']['path'])
        need(digest(Path(config['kubeconfig']['path']).read_bytes())==config['kubeconfig']['sha256'],
             'emergency_kubeconfig_changed')
        args=['--kubeconfig',config['kubeconfig']['path'],'--context',config['context'],'--request-timeout=15s']
        if namespace:args+=['-n',config['namespace']]
        value=decode(self._run(op,'kubectl',args+['get',kind,name,'-o','json'],deadline,record=False,timeout=15))
        meta=value['metadata'];need(meta['name']==name and valid_uuid(meta['uid']) and
            meta.get('namespace','')==(config['namespace'] if namespace else ''),'emergency_resource_identity')
        return value

    def _configuration_projection(self,op,subject,deadline):
        end=self._deadline(deadline);config=op.inventory['linux']
        native=self._native(op,subject,end);namespace=config['namespace'];pods=[];serving_pod=None
        projection={'schema_version':2,'kind':'openlegal_strict_config','topology':TOPOLOGY,'namespace':namespace,
            'deployments':[],'jobs':[],'configmaps':[],'persistent_volumes':[],
            'persistent_volume_claims':[],'survivor':native,'blob_survivors':{}}
        def row(kind,item):
            m=item['metadata'];return {'kind':kind,'name':m['name'],'namespace':namespace,'uid':m['uid']}
        for wanted in config['workloads']:
            item=self._kube(op,'deployment',wanted['name'],end);spec=item['spec']
            need(item['metadata']['uid']==wanted['uid'] and digest(canonical(spec['template']['spec']))==wanted['pod_spec_sha256'],
                 'emergency_deployment_changed')
            normalized=dict(spec);normalized.pop('replicas',None)
            projection['deployments'].append(dict(row('deployment',item),spec_sha256=digest(canonical(normalized)),
                selector_sha256=digest(canonical(spec['selector'])),template_sha256=digest(canonical(spec['template']))))
            pods.append(spec['template']['spec'])
            if wanted['name']==config['serving_workload']:
                need(serving_pod is None,'emergency_serving_template_duplicate')
                serving_pod=spec['template']['spec']
        for wanted in config['jobs']:
            item=self._kube(op,'job',wanted['name'],end);need(item['metadata']['uid']==wanted['uid'],'emergency_job_changed')
            spec=dict(item['spec']);spec.pop('suspend',None)
            projection['jobs'].append(dict(row('job',item),spec_sha256=digest(canonical(spec))))
            pods.append(spec['template']['spec'])
        for wanted in config['config_maps']:
            item=self._kube(op,'configmap',wanted['name'],end)
            need(item['metadata']['uid']==wanted['uid'] and digest(canonical(item.get('data',{})))==wanted['data_sha256'],
                 'emergency_configmap_changed')
            projection['configmaps'].append(dict(row('configmap',item),data_sha256=digest(canonical(item.get('data',{}))),
                binary_data_sha256=digest(canonical(item.get('binaryData',{}))),immutable=item.get('immutable')))
        pv={}
        for wanted in config['persistent_volumes']:
            item=self._kube(op,'persistentvolume',wanted['name'],end,False)
            need(item['metadata']['uid']==wanted['uid'] and item['spec'].get('local',{}).get('path')==wanted['path'],
                 'emergency_pv_changed');pv[wanted['name']]=item
            projection['persistent_volumes'].append({'kind':'persistentvolume','name':wanted['name'],
                'uid':item['metadata']['uid'],'spec_sha256':digest(canonical(item['spec']))})
        claims={}
        for pod in pods:
            for volume in pod.get('volumes',[]):
                if 'persistentVolumeClaim' not in volume:continue
                name=volume['persistentVolumeClaim']['claimName']
                if name in claims:continue
                item=self._kube(op,'persistentvolumeclaim',name,end)
                need(item['spec'].get('volumeName') in pv,'emergency_unbound_pvc');claims[name]=item
                projection['persistent_volume_claims'].append(dict(row('persistentvolumeclaim',item),
                    spec_sha256=digest(canonical(item['spec'])),volume_name=item['spec']['volumeName']))
        service=self._kube(op,'service',config['serving_service']['name'],end)
        need(service['metadata']['uid']==config['serving_service']['uid'] and
             digest(canonical(service['spec']))==config['serving_service']['spec_sha256'],'emergency_service_changed')
        projection['service']=dict(row('service',service),spec_sha256=digest(canonical(service['spec'])),
                                   selector_sha256=digest(canonical(service['spec'].get('selector',{}))))
        ref=config['serving_image'];image_id=config['serving_runtime_image_id']
        need(re.fullmatch(r'ghcr.io/publicdata-stream/openlegal-server@sha256:[0-9a-f]{64}',ref) and
             re.fullmatch('sha256:[0-9a-f]{64}',image_id),'emergency_runtime_typed_image')
        images=[decode(self._run(op,'docker',['image','inspect','--format',IMAGE,target],end,record=False))
                for target in (ref,image_id)]
        need(images[0]==images[1] and images[0]['Id']==image_id and images[0]['Os']=='linux' and
             images[0]['Architecture']=={'x86_64':'amd64','aarch64':'arm64'}.get(platform.machine()),
             'emergency_runtime_image_binding')
        revision=images[0]['Labels'].get('org.opencontainers.image.revision')
        need(type(revision) is str and re.fullmatch('[0-9a-f]{40}',revision),'emergency_runtime_revision')
        need(type(serving_pod) is dict and serving_pod.get('containers')
             and all(c['image']==ref for c in serving_pod['containers']), 'emergency_serving_template_image')
        # Serving and ingestion are distinct project artifacts. The actual
        # local ID/revision observation above qualifies the selected serving
        # image only. Every other immutable reference remains fully bound by
        # the unchanged spec/template hashes in the pre-ACK profile.
        need(all(type(c.get('image')) is str and re.fullmatch(
                 r'ghcr.io/publicdata-stream/openlegal-server(?:-ingestion)?@sha256:[0-9a-f]{64}',c['image'])
                 for pod in pods for kind in ('containers','initContainers','ephemeralContainers')
                 for c in pod.get(kind,[])), 'emergency_project_template_image')
        projection['runtime']={'reference':ref,'docker_image_id':image_id,'revision':revision,
            'os':images[0]['Os'],'architecture':images[0]['Architecture']}
        configured={r['role']:r for r in config['bound_roots']}
        for role in SSD_SURVIVORS:
            wanted=subject['blob_roots'][role];selected=configured[role]
            need(all(selected[k]==wanted[k] for k in ('path','uid','dev','ino')),'emergency_selected_blob_root')
            _verify_root(wanted);_verify_marker(wanted,role,subject['managed_epoch'])
            fd=_directory(selected['path'])
            try:actual=_descriptor(selected['path'],fd)
            finally:os.close(fd)
            marker=Path(selected['path'])/'.openlegal-seed-v1.json'
            flags=os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC|os.O_NOATIME
            fd=os.open(marker,flags)
            try:raw=os.read(fd,16385)
            finally:os.close(fd)
            mark=decode(raw);matches=[]
            for pod in pods:
                volumes={v['name']:v for v in pod.get('volumes',[])}
                for container in pod.get('containers',[]):
                    for mount in container.get('volumeMounts',[]):
                        if mount['mountPath']!=wanted['container_path']:continue
                        need(not mount.get('subPathExpr'),'emergency_dynamic_subpath')
                        volume=volumes[mount['name']]
                        if 'persistentVolumeClaim' in volume:
                            claim=claims[volume['persistentVolumeClaim']['claimName']]
                            host=pv[claim['spec']['volumeName']]['spec']['local']['path']
                        else:
                            need('hostPath' in volume,'emergency_blob_volume_kind');host=volume['hostPath']['path']
                        sub=mount.get('subPath','');need(not Path(sub).is_absolute() and '..' not in Path(sub).parts,
                                                       'emergency_blob_subpath')
                        need(str(Path(host)/sub)==selected['path'],'emergency_blob_configured_mapping');matches.append(mount['mountPath'])
            need(matches,'emergency_blob_configured_mount_missing')
            projection['blob_survivors'][role]=dict(actual,container_path=matches[0],
                filesystem_id=mark['filesystem_id'],marker_sha256=digest(raw))
        for key in ('deployments','jobs','configmaps','persistent_volumes','persistent_volume_claims'):
            projection[key].sort(key=lambda r:(r['kind'],r.get('namespace',''),r['name']))
        sha=configuration_hash_v1(projection)
        need(self._native(op,subject,end)==native,'emergency_configuration_native_changed')
        op.seal();self._deadline(end)
        return {'projection':projection,'configuration_sha256':sha,'native':native}
