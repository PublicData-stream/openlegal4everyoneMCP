"""One explicit schema27 -> 33 native migration, before any strict seed.

The sealed caller owns the global operation lock and retained writer descriptors.
This phase creates no role/credential/seed/workload and never selectively cancels
SQL. Failed/unknown attempts retain a per-primary guard and all native evidence.
There is deliberately no standalone bootstrap CLI or arbitrary command port.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import stat
import time

import linux_backend as linux
from interface import need
from inventory import canonical, decode, digest, pin, private_bytes, valid_uuid

PHASE = 'initial_migration'
MIGRATION_ENV = ('OPENLEGAL_MIGRATION_DATABASE_URL=postgresql://openlegal_migration@localhost/'
                 'openlegal?host=%2Fvar%2Frun%2Fpostgresql&port=5432')
PROFILE = b'''[source]
url="https://example.invalid/migration-only-not-served"
[http]
bind="127.0.0.1:0"
allowed_hosts=[]
allowed_origins=[]
[webtransport]
bind="127.0.0.1:0"
certificate="/unopened/migration-only.crt"
private_key="/unopened/migration-only.key"
allowed_hosts=[]
allowed_origins=[]
[health]
bind="127.0.0.1:0"
[cache]
mode="persistent"
[cache.postgres]
url_env="OPENLEGAL_DATABASE_URL"
migration_url_env="OPENLEGAL_MIGRATION_DATABASE_URL"
provider_admin_url_env="OPENLEGAL_PROVIDER_ADMIN_DATABASE_URL"
max_connections=2
tls_mode="plaintext"
replication_mode="local"
[cache.blob]
kind="filesystem"
path="/unopened/migration-only-blobs"
'''
SESSIONS_SQL = ("SELECT jsonb_build_object('database',current_database(),'session_user',session_user,"
 "'current_user',current_user,'postmaster_started_at',pg_postmaster_start_time()::text,"
 "'role_flags',(SELECT jsonb_build_object('superuser',rolsuper,'create_role',rolcreaterole,"
 "'create_db',rolcreatedb,'replication',rolreplication,'bypass_rls',rolbypassrls) FROM pg_roles WHERE rolname=current_user),'sessions',"
 "COALESCE((SELECT jsonb_agg(jsonb_build_object('pid',pid,'backend_start',backend_start::text,"
 "'user',usename,'database',datname) ORDER BY pid) FROM pg_stat_activity WHERE "
 "datname=current_database() AND pid<>pg_backend_pid()),'[]'::jsonb))")
MIGRATIONS_SQL = ("SELECT COALESCE(jsonb_agg(jsonb_build_object('version',version,'checksum',"
 "encode(checksum,'hex'),'success',success) ORDER BY version),'[]'::jsonb) FROM public._sqlx_migrations")
OLD_MIGRATIONS_SQL = ("SELECT jsonb_build_object('rows',count(*),'sha256',encode(sha256(convert_to("
 "COALESCE(string_agg(h,'' ORDER BY h COLLATE \"C\"),''),'UTF8')),'hex')) FROM "
 "(SELECT encode(sha256(convert_to(to_jsonb(t)::text,'UTF8')),'hex') h FROM public._sqlx_migrations t "
 "WHERE version BETWEEN 1 AND 27) hashes")
OLD_TABLE_PREFIX = ("SELECT jsonb_build_object('rows',count(*),'sha256',encode(sha256(convert_to("
 "COALESCE(string_agg(h,'' ORDER BY h COLLATE \"C\"),''),'UTF8')),'hex')) FROM "
 "(SELECT encode(sha256(convert_to(to_jsonb(t)::text,'UTF8')),'hex') h FROM ")
OLD_TABLE_SUFFIX = ' t) q'
TABLES_SQL = ("SELECT COALESCE(jsonb_agg(schemaname||'.'||tablename ORDER BY (schemaname||'.'||tablename) COLLATE \"C\"),"
 "'[]'::jsonb) FROM pg_tables WHERE schemaname IN ('openlegal','openlegal_admin')")
REFERENCES_SQL = ("SELECT jsonb_build_object('rows',count(*),'sha256',encode(sha256(convert_to("
 "COALESCE(string_agg(h,'' ORDER BY h COLLATE \"C\"),''),'UTF8')),'hex')) FROM ("
 "SELECT encode(sha256(convert_to(to_jsonb(r)::text,'UTF8')),'hex') h FROM ("
 "SELECT * FROM (SELECT 'cache'::text role,storage_key,encode(sha256,'hex') sha256,size_bytes size "
 "FROM openlegal.blob_object WHERE ready UNION ALL SELECT 'corpus',storage_key,encode(raw_sha256,'hex'),"
 "raw_size FROM openlegal.corpus_capture UNION ALL SELECT 'corpus',storage_key,encode(raw_sha256,'hex'),"
 "raw_size FROM openlegal.corpus_capture_blob UNION ALL SELECT 'corpus',storage_key,encode(raw_sha256,'hex'),"
 "raw_size FROM openlegal.corpus_source_observation WHERE storage_key IS NOT NULL) refs "
 "ORDER BY role,storage_key LIMIT 200001) r) hashes")
SNAPSHOT_DATASETS = {'cachepool/' + s for s in ('openlegal-postgres','openlegal-cache-blobs',
    'openlegal-corpus-blobs','openlegal-corpus-index','openlegal-mecab-ko','openlegal4everyone-conf','http')}
IMAGE_FORMAT = '{"Id":{{json .Id}},"Os":{{json .Os}},"Architecture":{{json .Architecture}},"Volumes":{{json .Config.Volumes}}}'
GATE = ('n=0; while [ ! -f /initial-control/go ]; do n=$((n+1)); '
        '[ "$n" -lt 600 ] || exit 125; sleep 0.1; done; '
        'exec /usr/local/bin/openlegal-server --migrate /initial-control/migration.toml')


def _saved(backend, op, name, value):
    backend._artifact(op, name, value)
    filename = op.phase + '-' + name + '.json'; raw = canonical(value); sha = digest(raw)
    need(op.journal.output_hashes().get(filename) == sha and
         op.journal.read_phase_bytes(op.phase, filename, sha) == raw, 'initial_artifact_not_acknowledged')
    return {'name': filename, 'sha256': sha}


def _config(op):
    need(op.action == 'strict_prepare' and op.phase == PHASE and
         op.authority.get('initial_migration_permitted') is True and valid_uuid(op.operation_id), 'initial_scope')
    op.seal()
    c = op.inventory['initial_migration']
    need(type(c) is dict and set(c) == {'primary_process','socket_directory','socket','candidate',
        'compiled_manifest','prerequisites','producer_source','legacy_binding','deadline_secs'}, 'initial_policy_shape')
    need(type(c['legacy_binding']) is dict and set(c['legacy_binding']) ==
         {'inventory_sha256','source_graph_sha256','authority_sha256'} and
         all(pin(v) for v in c['legacy_binding'].values()),'initial_legacy_binding')
    workloads=op.inventory['linux']['workloads']
    need(type(workloads) is list and len(workloads)==2 and
         all(r.get('kind')=='deployment' and valid_uuid(r.get('uid')) for r in workloads) and
         len({r['name'] for r in workloads})==2 and
         op.inventory['linux']['serving_workload'] in {r['name'] for r in workloads},'initial_workload_scope')
    need(type(c['deadline_secs']) is int and 1 <= c['deadline_secs'] <= 300, 'initial_deadline')
    for name in ('compiled_manifest','producer_source'):
        row = c[name]; need(type(row) is dict and set(row) == {'path','sha256'} and pin(row['sha256']), 'initial_private_ref')
        private_bytes(row['path'], row['sha256'])
    candidate = c['candidate']
    need(type(candidate) is dict and set(candidate) == {'reference','image_id','executable_sha256'} and
         re.fullmatch(r'ghcr.io/publicdata-stream/openlegal-server@sha256:[0-9a-f]{64}', candidate['reference'])
         and re.fullmatch(r'sha256:[0-9a-f]{64}', candidate['image_id']) and pin(candidate['executable_sha256']), 'initial_candidate')
    for name in ('initial_migration.py','linux_backend.py','command_process.py','interface.py','inventory.py','manual_backend.py'):
        source = Path(__file__).resolve().parent/name
        need(name in op.inventory['source_graph'], 'initial_source_graph')
        private_bytes(source, op.inventory['source_graph'][name])
    return c


def _prior(op, c, key):
    row = c['prerequisites'][key]
    need(type(row) is dict and set(row) == {'phase','name','sha256'} and pin(row['sha256']), 'initial_prior_ref')
    raw = op.journal.read_phase_bytes(row['phase'], row['name'], row['sha256'])
    need(digest(raw) == row['sha256'], 'initial_prior_raw_hash')
    value = decode(raw)
    need(type(value) is dict and set(value) == {'schema_version','kind','operation_id','inventory_sha256',
        'source_graph_sha256','authority_sha256','producer_source_sha256','producer_exit','payload'} and
        type(value['schema_version']) is int and value['schema_version'] == 1 and
        value['kind'] == 'openlegal_initial_' + key + '_proof' and value['operation_id'] == op.operation_id and
        all(value[k] == v for k,v in c['legacy_binding'].items()) and
        value['producer_source_sha256'] == c['producer_source']['sha256'], 'initial_prior_binding')
    exitrow = value['producer_exit']
    need(type(exitrow) is dict and set(exitrow) == {'pid','start_ticks','boot_id','actual_exit_code','remaining_children'}
         and type(exitrow['pid']) is int and exitrow['pid'] > 0 and type(exitrow['start_ticks']) is str
         and exitrow['start_ticks'].isdecimal() and exitrow['boot_id'] == op.inventory['host']['boot_id']
         and type(exitrow['actual_exit_code']) is int and exitrow['actual_exit_code'] == 0
         and exitrow['remaining_children'] == [], 'initial_prior_actual_exit')
    from manual_backend import _process_absent
    _process_absent({k: exitrow[k] for k in ('pid','start_ticks','boot_id')})
    return value['payload']


def _fence(backend, op, drain):
    need(type(drain) is dict and set(drain) == {'resources','closure'}, 'initial_drain_shape')
    expected = {(r['kind'].lower(),r['name'],r['uid']) for r in op.inventory['linux']['workloads']}
    expected |= {('job',r['name'],r['uid']) for r in op.inventory['linux']['jobs']}
    rows = drain['resources']; need(type(rows) is list and len(rows) == len(expected), 'initial_fence_count')
    seen = set()
    for row in rows:
        need(type(row) is dict and set(row) == {'kind','name','uid','resource_version','spec_sha256','fence_nonce'}, 'initial_fence_row')
        key = (row['kind'],row['name'],row['uid']); need(key in expected and key not in seen, 'initial_fence_identity'); seen.add(key)
        actual = backend._kube(op, ['-n',op.inventory['linux']['namespace'],'get',row['kind'],row['name'],'-o','json'])
        meta = actual['metadata']; spec = actual['spec']
        terminal = row['kind'] == 'job' and any(x.get('type') in ('Complete','Failed') and x.get('status') == 'True'
            for x in actual.get('status',{}).get('conditions',[]))
        need(meta['uid'] == row['uid'] and meta['resourceVersion'] == row['resource_version'] and
             digest(canonical(spec)) == row['spec_sha256'], 'initial_live_fence_changed')
        if terminal:
            need(row['fence_nonce'] is None,'initial_terminal_job_must_remain_untouched')
        else:
            need(type(row['fence_nonce']) is str and row['fence_nonce'].startswith(op.operation_id + '.') and
                 meta.get('annotations',{}).get('openlegal4everyone.stream/operation-fence') == row['fence_nonce'],
                 'initial_revision_fence_missing')
            need(spec.get('replicas') == 0 if row['kind'] == 'deployment' else spec.get('suspend') is True,
                 'initial_writer_reopened')
    need(seen == expected, 'initial_fence_missing')
    # This source-owned port reads actual CRI/cgroups/old process identities and
    # requires the same retained descriptors from the live legacy fence phase.
    closure = backend._physical_writers_closed(op)
    need(closure == drain['closure'], 'initial_physical_fence_changed')


def _manifest(c):
    ref = c['compiled_manifest']; value = decode(private_bytes(ref['path'], ref['sha256']))
    need(type(value) is dict and set(value) == {'kind','candidate','native_architecture','migrations'} and
         value['kind'] == 'openlegal_compiled_migration_manifest' and value['candidate'] == c['candidate'] and
         value['native_architecture'] == {'x86_64':'amd64','aarch64':'arm64'}.get(platform.machine()), 'initial_compiled_identity')
    rows = value['migrations']
    need(type(rows) is list and len(rows) == 33 and [r.get('version') for r in rows] == list(range(1,34)), 'initial_compiled_versions')
    need(all(set(r) == {'version','checksum'} and type(r['version']) is int and type(r['checksum']) is str
             and re.fullmatch('[0-9a-f]{96}', r['checksum']) for r in rows), 'initial_compiled_checksums')
    return rows


def _sql(backend, op, sql, deadline, *, audit=False):
    """Fixed metadata reads only; deliberately bypasses _sql's SQL cancellation."""
    need(type(audit) is bool, 'initial_sql_profile')
    if audit:
        # Only complete preservation digests get the maintenance budget. No
        # inventory/authority field or caller-provided SQL chooses a profile.
        table = sql[len(OLD_TABLE_PREFIX):-len(OLD_TABLE_SUFFIX)] if (
            sql.startswith(OLD_TABLE_PREFIX) and sql.endswith(OLD_TABLE_SUFFIX)) else ''
        need(sql in (REFERENCES_SQL, OLD_MIGRATIONS_SQL) or
             re.fullmatch(r'openlegal(?:_admin)?\.[a-z_][a-z0-9_]{0,62}',table), 'initial_audit_sql_domain')
    need(time.monotonic() < deadline, 'initial_sql_original_deadline')
    primary = op.inventory['linux']['postgres']['primary']['container_id']
    prefix = (("SET statement_timeout='30s'; SET lock_timeout='1s'; SET transaction_timeout='35s'; " if audit else
               "SET statement_timeout='2s'; SET lock_timeout='1s'; SET transaction_timeout='5s'; ") +
              "SET TIME ZONE 'UTC'; SET DateStyle='ISO,YMD'; SET bytea_output='hex'; ")
    raw = backend._run(op, 'docker', ['exec','-i','-u','70','-e','PGPASSFILE=/dev/null',
        '-e','PGPASSWORD=','-e','PGSERVICE=','-e','PGSERVICEFILE=/dev/null','-e','PGHOSTADDR=',
        '-e','PGOPTIONS=','-e','PGSSLCERT=','-e','PGSSLKEY=','-e','PGSSLROOTCERT=',
        '-e','PGAPPNAME=openlegal_initial_observer', primary,
        '/usr/bin/env','-u','PGSERVICE','psql','-X','-w','-h','/var/run/postgresql','-p','5432','-qAt','-v','ON_ERROR_STOP=1',
        '-U','openlegal_migration','-d','openlegal'], stdin=(prefix+sql+';\n').encode(),
        timeout=31 if audit else 3, deadline=deadline)
    return decode(raw)


def _sessions(backend, op, deadline, original=None):
    value = _sql(backend,op,SESSIONS_SQL,deadline)
    need(type(value) is dict and set(value) == {'database','session_user','current_user','postmaster_started_at','role_flags','sessions'}
         and value['database'] == 'openlegal' and value['session_user'] == value['current_user'] == 'openlegal_migration'
         and value['role_flags'] == dict.fromkeys(('superuser','create_role','create_db','replication','bypass_rls'),False)
         and all(v is False for v in value['role_flags'].values()) and value['sessions'] == [] and
         type(value['postmaster_started_at']) is str, 'initial_global_sql_not_closed')
    need(original is None or value == original, 'initial_postmaster_or_identity_changed')
    return value


def _old_state(backend, op, deadline, tables=None):
    if tables is None: tables = _sql(backend,op,TABLES_SQL,deadline)
    need(type(tables) is list and 1 <= len(tables) <= 256 and tables == sorted(set(tables)) and
         all(type(t) is str and re.fullmatch(r'openlegal(?:_admin)?\.[a-z_][a-z0-9_]{0,62}', t) for t in tables), 'initial_table_domain')
    rows = {}
    for table in tables:
        sql = OLD_TABLE_PREFIX + table + OLD_TABLE_SUFFIX
        result = _sql(backend,op,sql,deadline,audit=True)
        need(type(result) is dict and set(result) == {'rows','sha256'} and type(result['rows']) is int
             and result['rows'] >= 0 and pin(result['sha256']), 'initial_old_rows')
        rows[table] = result
    refs = _sql(backend,op,REFERENCES_SQL,deadline,audit=True)
    need(type(refs) is dict and set(refs)=={'rows','sha256'} and type(refs['rows']) is int and
         0<=refs['rows']<=200000 and pin(refs['sha256']), 'initial_references_bound')
    history=_sql(backend,op,OLD_MIGRATIONS_SQL,deadline,audit=True)
    need(type(history) is dict and set(history)=={'rows','sha256'} and type(history['rows']) is int
         and history['rows']==27 and pin(history['sha256']),'initial_old_migration_history')
    return {'tables': rows, 'references': refs, 'migration_history':history}


def _primary(backend, op, c):
    actual = backend._inspect(op,'primary'); w = c['primary_process']
    need(type(w) is dict and set(w) == {'pid','start_ticks','boot_id'} and w['boot_id'] == op.inventory['host']['boot_id']
         and type(w['pid']) is int and w['pid'] > 0 and type(w['start_ticks']) is str
         and w['start_ticks'].isdecimal() and actual['state']['Running'] is True and
         actual['state']['Pid'] == w['pid'], 'initial_primary_native')
    ticks = Path('/proc/'+str(w['pid'])+'/stat').read_text().rsplit(')',1)[1].split()[19]
    need(ticks == w['start_ticks'] and Path('/proc/sys/kernel/random/boot_id').read_text().strip() == w['boot_id'], 'initial_primary_reused')
    fd = backend._container_directory(w['pid'],'/var/run/postgresql')
    try:
        directory = os.fstat(fd); node = os.stat('.s.PGSQL.5432',dir_fd=fd,follow_symlinks=False)
        need(stat.S_ISSOCK(node.st_mode) and node.st_uid == node.st_gid == 70 and
             {'dev':directory.st_dev,'ino':directory.st_ino} == c['socket_directory'] and
             {'dev':node.st_dev,'ino':node.st_ino} == c['socket'], 'initial_primary_socket_changed')
        return fd
    except BaseException: os.close(fd); raise


def _write(parent, name, raw, mode=0o640, gid=10004):
    fd = os.open(parent/name,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,mode)
    try:
        os.fchown(fd,0,gid);os.fchmod(fd,mode)
        view=memoryview(raw)
        while view:
            n=os.write(fd,view);need(n>0,'initial_write');view=view[n:]
        os.fsync(fd);os.lseek(fd,0,os.SEEK_SET);need(os.read(fd,len(raw)+1)==raw,'initial_write_readback')
    finally:os.close(fd)
    fd=os.open(parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(fd)
    finally:os.close(fd)


def run(backend, op):
    """Actually dispatch the official native CLI once, using the reviewed runner.

    Caller retains SharedLock and this backend object on failed/unknown result.
    The per-primary exclusive guard FD is intentionally retained on backend,
    including failures; its persistent marker prevents fresh-UUID replay.
    """
    need(isinstance(backend,linux.LinuxBackend),'initial_fixed_linux_backend')
    c = _config(op); need(set(c['prerequisites']) == {'drain','snapshot','preservation'},'initial_prerequisites')
    expected = _manifest(c); drain = _prior(op,c,'drain'); snapshot = _prior(op,c,'snapshot')
    preserved = _prior(op,c,'preservation'); deadline = time.monotonic()+c['deadline_secs']
    need(type(snapshot) is dict and set(snapshot) == {'snapshots','actual_command'} and len(snapshot['snapshots']) == 7 and
         {r['dataset'] for r in snapshot['snapshots']} == SNAPSHOT_DATASETS,'initial_snapshot_set')
    refs=[r['snapshot'] for r in snapshot['snapshots']]
    need(len({s.split('@',1)[1] for s in refs if '@' in s})==1,'initial_snapshot_group')
    command=snapshot['actual_command']
    need(type(command) is dict and set(command)=={'binary','binary_sha256','arguments_sha256','actual_exit_code',
         'started','owned_processes_closed','expired','interrupted','unexpected_descendants','output_limit_exceeded'} and
         command['binary']=='zfs' and command['binary_sha256']==op.inventory['linux']['binaries']['zfs']['sha256'] and
         command['arguments_sha256']==digest(canonical(['snapshot',*refs])) and
         type(command['actual_exit_code']) is int and command['actual_exit_code']==0 and
         all(command[k] is True for k in ('started','owned_processes_closed')) and
         all(command[k] is False for k in ('expired','interrupted','unexpected_descendants','output_limit_exceeded')),
         'initial_atomic_snapshot_command_unproven')
    for row in snapshot['snapshots']:
        need(set(row) == {'dataset','snapshot','dataset_guid','snapshot_guid'} and
             row['snapshot'].startswith(row['dataset']+'@') and
             all(type(row[k]) is str and re.fullmatch('[0-9]{1,20}',row[k]) for k in ('dataset_guid','snapshot_guid')), 'initial_snapshot_row')
        guid = backend._run(op,'zfs',['get','-Hp','-o','value','guid',row['snapshot']],deadline=deadline).decode().strip()
        parent_guid = backend._run(op,'zfs',['get','-Hp','-o','value','guid',row['dataset']],deadline=deadline).decode().strip()
        pinned={d['name']:d['guid'] for d in op.inventory['original_datasets']}
        need(guid == row['snapshot_guid'] and parent_guid == row['dataset_guid'] == pinned.get(row['dataset']),
             'initial_snapshot_replaced')
    _fence(backend,op,drain); socket_fd = _primary(backend,op,c)
    cid = None; gate_released = False; cgroup_fd = None
    primary_id = op.inventory['linux']['postgres']['primary']['container_id']
    try:
        guard_path = Path('/tmp/openlegal-initial-migration-primary-'+primary_id+'.lock')
        guard = os.open(guard_path,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
        backend._initial_migration_guard_fd = guard
        fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB)
        raw=canonical({'operation_id':op.operation_id,'unknown_requires_manual_audit':True})
        view=memoryview(raw)
        while view:
            n=os.write(guard,view);need(n>0,'initial_guard_write');view=view[n:]
        os.fsync(guard);os.lseek(guard,0,os.SEEK_SET);need(os.read(guard,len(raw)+1)==raw,'initial_guard_readback')
        parent=os.open('/tmp',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(parent)
        finally:os.close(parent)
        before = _sessions(backend,op,deadline)
        old_migrations = _sql(backend,op,MIGRATIONS_SQL,deadline)
        need(old_migrations == [dict(r,success=True) for r in expected[:27]],'initial_schema27_checksums')
        old = _old_state(backend,op,deadline)
        need(old == preserved,'initial_old_proof_not_current')
        root = Path('/tmp/openlegal-initial-migration-'+op.operation_id);os.mkdir(root,0o700)
        control=root/'control';os.mkdir(control,0o755);os.chmod(control,0o755)
        _write(control,'migration.toml',PROFILE)
        raw_image = backend._run(op,'docker',['image','inspect','--format',IMAGE_FORMAT,c['candidate']['reference']],deadline=deadline)
        image=decode(raw_image)
        need(image == {'Id':c['candidate']['image_id'],'Os':'linux',
            'Architecture':{'x86_64':'amd64','aarch64':'arm64'}.get(platform.machine()),'Volumes':None},'initial_candidate_native_image')
        source='/proc/'+str(c['primary_process']['pid'])+'/root/var/run/postgresql'
        args=['create','--pull=never','--name','openlegal-initial-migration-'+op.operation_id,
            '--label','openlegal.operation='+op.operation_id,'--restart=no','--network=none','--read-only',
            '--user=10004:10004','--cap-drop=ALL','--security-opt=no-new-privileges','--pids-limit=64',
            '--memory=512m','--memory-swap=512m','--cpus=1','--log-driver=json-file','--log-opt=max-size=1m',
            '--log-opt=max-file=1','--tmpfs','/tmp:rw,noexec,nosuid,nodev,size=16m',
            '--mount','type=bind,src='+source+',dst=/var/run/postgresql,readonly',
            '--mount','type=bind,src='+str(control)+',dst=/initial-control,readonly',
            '-e',MIGRATION_ENV,'-e','PGPASSFILE=/dev/null','-e','PGPASSWORD=','-e','PGSERVICE=',
            '-e','PGSERVICEFILE=/dev/null','-e','PGHOSTADDR=','-e','PGOPTIONS=',
            '-e','PGSSLCERT=','-e','PGSSLKEY=','-e','PGSSLROOTCERT=',
            '--entrypoint=/bin/sh',c['candidate']['image_id'],'-ec',GATE]
        _saved(backend,op,'native-create-intent',{'argv':args,'compiled_manifest':c['compiled_manifest'],
            'prerequisites':c['prerequisites'],'primary_process':c['primary_process'],'socket':c['socket']})
        cid=backend._run(op,'docker',args,deadline=deadline).decode().strip()
        need(re.fullmatch('[0-9a-f]{64}',cid),'initial_create_ack_unknown')
        _saved(backend,op,'native-created',{'id':cid})
        backend._run(op,'docker',['start',cid],deadline=deadline)
        raw=backend._run(op,'docker',['inspect','--format',
            '{"Id":{{json .Id}},"Image":{{json .Image}},"Pid":{{json .State.Pid}},"Running":{{json .State.Running}},"Mounts":{{json .Mounts}},"User":{{json .Config.User}},"Entrypoint":{{json .Config.Entrypoint}},"Cmd":{{json .Config.Cmd}},"Readonly":{{json .HostConfig.ReadonlyRootfs}},"Network":{{json .HostConfig.NetworkMode}}}',cid],deadline=deadline)
        native=decode(raw);pid=native['Pid']
        need(native['Id']==cid and native['Image']==c['candidate']['image_id'] and native['Running'] is True
             and type(pid) is int and pid>0,'initial_gated_native_actor')
        need(native['User']=='10004:10004' and native['Entrypoint']==['/bin/sh'] and native['Cmd']==['-ec',GATE]
             and native['Readonly'] is True and native['Network']=='none','initial_fixed_native_command')
        mounts=sorted((m['Source'],m['Destination'],m['RW']) for m in native['Mounts'] if m['Type']!='tmpfs')
        need(mounts==sorted([(source,'/var/run/postgresql',False),(str(control),'/initial-control',False)])
             and all((m['Type']=='bind' and m['RW'] is False) or
                     (m['Type']=='tmpfs' and m['Destination']=='/tmp' and m['RW'] is True)
                     for m in native['Mounts']),'initial_native_mounts')
        mapped=backend._container_directory(pid,'/var/run/postgresql')
        try: need((os.fstat(mapped).st_dev,os.fstat(mapped).st_ino)==(os.fstat(socket_fd).st_dev,os.fstat(socket_fd).st_ino),'initial_socket_mount_correspondence')
        finally:os.close(mapped)
        native_bin=backend._container_directory(pid,'/usr/local/bin')
        try:
            fd=os.open('openlegal-server',os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=native_bin)
            try:
                meta=os.fstat(fd);need(stat.S_ISREG(meta.st_mode) and meta.st_size<=128*1024**2,'initial_native_executable')
                h=hashlib.sha256()
                while block:=os.read(fd,1024*1024):h.update(block)
                need(h.hexdigest()==c['candidate']['executable_sha256'],'initial_native_compiled_bytes')
            finally:os.close(fd)
        finally:os.close(native_bin)
        witness={'pid':pid,'start_ticks':Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()[19],
                 'boot_id':op.inventory['host']['boot_id'],'groups':linux.process_cgroup(pid)}
        need(len(witness['groups'])==1 and cid in witness['groups'][0] and witness['groups'][0].startswith('0::/'),
             'initial_native_cgroup')
        cgroup_path=Path('/sys/fs/cgroup')/witness['groups'][0].split(':',2)[2].lstrip('/')
        cgroup_fd=os.open(cgroup_path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
        cg=os.fstat(cgroup_fd);witness['cgroup_identity']={'dev':cg.st_dev,'ino':cg.st_ino}
        _saved(backend,op,'native-gated-owner',{'id':cid,'witness':witness,'socket_mount':c['socket_directory']})
        _fence(backend,op,drain);_sessions(backend,op,deadline,before)
        need(_manifest(_config(op))==expected,'initial_compiled_input_changed')
        fresh=_primary(backend,op,c);os.close(fresh)
        need(time.monotonic()<deadline,'initial_dispatch_deadline');op.seal()
        _saved(backend,op,'migration-dispatch-intent',{'id':cid,'witness':witness,'profile_sha256':digest(PROFILE),
            'fixed_arguments':['/usr/local/bin/openlegal-server','--migrate','/initial-control/migration.toml'],
            'old_state_sha256':digest(canonical(old)),'expected_migrations':expected,'automatic_retry':False})
        need(time.monotonic()<deadline,'initial_dispatch_deadline')
        # Creation of the gate can already release the native payload. An
        # unknown write/fsync ACK must be recorded as possibly dispatched.
        gate_released=True
        _write(control,'go',b'go\n',0o644,0)
        need(time.monotonic()<deadline,'initial_dispatch_deadline')
        need(backend._run(op,'docker',['wait',cid],timeout=c['deadline_secs'],deadline=deadline)==b'0\n','initial_native_actual_exit')
        exited=decode(backend._run(op,'docker',['inspect','--format',
            '{"Id":{{json .Id}},"Image":{{json .Image}},"Pid":{{json .State.Pid}},"Running":{{json .State.Running}},"ExitCode":{{json .State.ExitCode}}}',cid],deadline=deadline))
        need(exited=={'Id':cid,'Image':c['candidate']['image_id'],'Pid':0,'Running':False,'ExitCode':0}
             and type(exited['ExitCode']) is int and type(exited['Pid']) is int
             and exited['Running'] is False,'initial_actual_container_exit_unknown')
        from manual_backend import _process_absent
        _process_absent({k:witness[k] for k in ('pid','start_ticks','boot_id')})
        need(not linux.members(witness['groups']),'initial_native_descendants_remain')
        need({'dev':os.fstat(cgroup_fd).st_dev,'ino':os.fstat(cgroup_fd).st_ino}==witness['cgroup_identity'],
             'initial_native_cgroup_rebound')
        _sessions(backend,op,deadline,before);_fence(backend,op,drain)
        need(_sql(backend,op,MIGRATIONS_SQL,deadline)==[dict(r,success=True) for r in expected],'initial_schema33_actual_checksums')
        need(_old_state(backend,op,deadline,list(old['tables']))==old,'initial_old_rows_or_references_changed')
        _sessions(backend,op,deadline,before)
        result={'kind':'openlegal_initial_migration_result','candidate_id':cid,'schema_version':33,
            'migration_native_exit_code':0,'same_database_sessions':[],'old_rows_and_references_sha256':digest(canonical(old)),
            'seed_created':False,'roles_created':False,'ready':False,'outer_actual_os_exit':'NOT_YET_OBSERVABLE'}
        _saved(backend,op,'migration-complete',result)
        need(time.monotonic()<deadline,'initial_completion_deadline')
        return result
    except BaseException:
        try:_saved(backend,op,'migration-failed',{'status':'FAILED_OR_UNKNOWN','candidate_id':cid,
            'payload_may_dispatched':gate_released,'automatic_retry':False,'automatic_cleanup':False,
            'selective_sql_cancellation':False,'primary_guard_retained':hasattr(backend,'_initial_migration_guard_fd')})
        except BaseException:pass
        raise
    finally:
        os.close(socket_fd)
        if cgroup_fd is not None:
            # Retain this actual ownership descriptor on failures as well.
            if not hasattr(backend,'_initial_migration_cgroup_fd'):
                backend._initial_migration_cgroup_fd=cgroup_fd
