"""One live post-seed role transaction; no CLI, trust provisioning or Ready.

Only the initial driver's retained ROOT backend/lock/journal may call run.
New role names and expected old owners are DATA, never SQL/ACL fragments.
Unknown COMMIT retains its once guard; no adoption, replay, drop or fallback.
This in-process producer claims native command closure, not a child OS exit.
"""
from __future__ import annotations

import copy
import fcntl
import os
from pathlib import Path
import re
import stat
import threading
import time

import initial_blob_seed as seed
import initial_driver as driver
import initial_migration as migration
import journal as journals
import legacy_prepare as legacy
import manual_backend as manual
import command_process as processes
from interface import Operation, freeze_authority, need
from inventory import canonical, decode, digest, pin, private_bytes, valid_uuid

PHASE = 'initial_role_prepare'
EXECUTION = 'IN_PROCESS_WITH_NATIVE_COMMAND_CLOSURE'
ROLE_KEYS = ('replication_owner','checkpoint_owner','binding_owner','canary_budget_owner',
             'canary_lifecycle_owner','storage_login','recovery_login','canary_login','peer_login')
SCHEMAS = ('openlegal','openlegal_admin','openlegal_checkpoint','openlegal_replication')
# prosrc is precisely UTF8 bytes between the migration's dollar-quote delimiters.
# PostgreSQL dollar-quoted text performs no SQL-string escape conversion.
FUNCTIONS = {'openlegal.replication_required()': ('replication_owner',
                                      '2d87d8b598fd645899cbeba4bde4b7baa2c5cd43df7be503d3a1df223508daba',
                                      28),
 'openlegal_admin.canary_close(uuid,uuid)': ('canary_budget_owner',
                                             '64707c92101fe5bde2eceb0abc48a8e633d213117b2b50dbbb6da0ce51a8ec2a',
                                             28),
 'openlegal_admin.canary_consume(uuid)': ('canary_budget_owner',
                                          '6cc9f5874dbdb47fb348554df4e4433446957e936272c76db9b9ac3ef693bca6',
                                          28),
 'openlegal_admin.canary_init(uuid,uuid)': ('canary_budget_owner',
                                            '701b90f328c0db51c3416bc0e9ed1c3dec550325d6f30311138624541d0a3f07',
                                            28),
 'openlegal_checkpoint.activate_canary(uuid,uuid,text,uuid)': ('canary_lifecycle_owner',
                                                               'cfc8485456a4ee84ecf5ae92f33b84925df1410fbb19b033b122433f0678d7a4',
                                                               31),
 'openlegal_checkpoint.bind_replica(uuid,text,jsonb,jsonb)': ('binding_owner',
                                                              '3312bddedfff0c20af000c2f1ad8f31cdb771243ea466a1fcb22591208233cfc',
                                                              30),
 'openlegal_checkpoint.canary_observation(uuid)': ('checkpoint_owner',
                                                   '9151c6829bbb171269124d2660432fbd097e667fb94e71414b529c34b0984579',
                                                   29),
 'openlegal_checkpoint.canary_readback(uuid)': ('canary_lifecycle_owner',
                                                'ce05dd53ec18ab11ce4c3345fe6ae96ff717ac22d9b6eb73e3e9771f79b2c977',
                                                31),
 'openlegal_checkpoint.client_sessions()': ('checkpoint_owner',
                                            '530eeead189f8b3d59be4ec1a63d250fd0bbbe07a7edf38aa11387c59fcaede9',
                                            29),
 'openlegal_checkpoint.database_layout()': ('checkpoint_owner',
                                            'c4bee0a6b384c58e431751db70c4c8dba950bb9c739fce76219ff97a1d282799',
                                            29),
 'openlegal_checkpoint.database_position()': ('checkpoint_owner',
                                              '38e8c6bc30b230b2290ace0f56b374353d6aef1fffeb926dcd0a831c60d6a16a',
                                              29),
 'openlegal_checkpoint.finish_canary(uuid,text,uuid)': ('canary_lifecycle_owner',
                                                        'cc7a414342aa622c0f7af7f7aea329637d5357da378275b95c3fcc867b283756',
                                                        31),
 'openlegal_checkpoint.hold(uuid,text)': ('checkpoint_owner',
                                          '3ddb245011eaee4110248ab47018d2d9fc199047f7e7264b0fd16b193f41fbad',
                                          29),
 'openlegal_checkpoint.observe(uuid)': ('checkpoint_owner',
                                        '98583bc00c778ef8686dcdfbfd3293c38cac5853252d486e98ba1b7f4b70227c',
                                        29),
 'openlegal_checkpoint.preservation_page(text,jsonb,integer)': ('checkpoint_owner',
                                                                'fbb015ac44a2d92c16ca2ab5a08740659bb11c8a0d4c71ed9d4d9986c3acdd1b',
                                                                32),
 'openlegal_checkpoint.provider_survivor_state()': ('checkpoint_owner',
                                                    'e20c49b25049fb356b73f9f8ff40dac527187d5d52823c342e4e18f02c1dc693',
                                                    33),
 'openlegal_checkpoint.publication_page(bigint,text,integer)': ('checkpoint_owner',
                                                                'f0566a5436c05494b6a8a83ef9def6083448293032d9b717cf34213eb8fe1c25',
                                                                32),
 'openlegal_checkpoint.readback(uuid)': ('checkpoint_owner',
                                         '75d522314e32bad46a0f8b96dbd261e104c5d9df45e0c60cea199b4e97201dde',
                                         29),
 'openlegal_checkpoint.release(uuid,text)': ('checkpoint_owner',
                                             '3ba3344bcadd2bc6c0825e3b88187da0c6c589a3d77664a1a5014d564cf69ce5',
                                             29),
 'openlegal_checkpoint.retained_references()': ('checkpoint_owner',
                                                'fb5b8d42ecec23b1f7b73ad1e658ef3a6f5e437a094256cdc55f24614f511727',
                                                29),
 'openlegal_checkpoint.retained_tables()': ('checkpoint_owner',
                                            '0279ec7bee992343a87edd885a33a9af27d023c00ff6947f2254666bcb781443',
                                            29),
 'openlegal_checkpoint.state()': ('checkpoint_owner',
                                  'c37d875aada75c55824e547a6fc555c860e895d2ceccd025cb676bec94714a60',
                                  29),
 'openlegal_checkpoint.storage_metadata()': ('checkpoint_owner',
                                             'f54981fed3b40c79744444f252b57198a310cd1af8e8cead5de50da515bb2532',
                                             29),
 'openlegal_checkpoint.survivor_snapshot()': ('checkpoint_owner',
                                              'b75261de09a3919397f45223e3585c24f2e78ff7d107e3b4142ea0a484281a30',
                                              33),
 'openlegal_replication.capture_prefix()': ('replication_owner',
                                            '2998c0232dab3edc2acabc6f2cda0b95205719017d9b16a7f50314ac9cd4bd3b',
                                            28),
 'openlegal_replication.observe_candidate(jsonb)': ('replication_owner',
                                                    '2758294d61d9946d429e0de1ba41a19bcfcfef5890a5b6773709dc9bc3a406c4',
                                                    30),
 'openlegal_replication.observe_flush()': ('replication_owner',
                                           '46a1e92d0c2c34ca7684291c99a6accfd5e2859e16b74f2abdd0bd384ef9a71c',
                                           28),
 'openlegal_replication.survivor_position()': ('replication_owner',
                                               'd3607138c71fed71c2067f312dc04d036b756ab7dc16fb3b23afe5ada71b2621',
                                               33)}
TABLES = ('openlegal.blob_deletion',
 'openlegal.blob_object',
 'openlegal.cache_head',
 'openlegal.cache_query',
 'openlegal.cache_snapshot',
 'openlegal.cache_storage',
 'openlegal.collection_request',
 'openlegal.corpus_blob_deletion',
 'openlegal.corpus_capture',
 'openlegal.corpus_capture_blob',
 'openlegal.corpus_capture_catalog',
 'openlegal.corpus_citation_lease',
 'openlegal.corpus_control',
 'openlegal.corpus_job',
 'openlegal.corpus_object',
 'openlegal.corpus_outbox',
 'openlegal.corpus_pin',
 'openlegal.corpus_retirement',
 'openlegal.corpus_revision',
 'openlegal.corpus_session',
 'openlegal.corpus_source_observation',
 'openlegal.corpus_staging',
 'openlegal.current_inventory_audit',
 'openlegal.current_inventory_candidate',
 'openlegal.provider_clone_member',
 'openlegal.provider_clone_view',
 'openlegal.provider_collection_gap',
 'openlegal.provider_demand_ticket',
 'openlegal.provider_inventory_cursor',
 'openlegal.provider_request_admission',
 'openlegal.provider_request_budget',
 'openlegal.provider_supplement_control',
 'openlegal.provider_supplement_job',
 'openlegal.upstream_daily_budget',
 'openlegal_admin.provider_canary',
 'openlegal_admin.provider_canary_audit',
 'openlegal_admin.provider_canary_control',
 'openlegal_admin.provider_control',
 'openlegal_admin.provider_recovery_audit',
 'openlegal_admin.uncertainty_observation',
 'openlegal_checkpoint.canary_lifecycle',
 'openlegal_checkpoint.operation',
 'openlegal_replication.control',
 'public._sqlx_migrations')
MIGRATIONS = {'crates/adapters/migrations/0028_storage_replication.sql': 'd273c56e49bff9708b54245ac42d19beb87c894b16cf488b9cdc19c509b5f274',
 'crates/adapters/migrations/0029_checkpoint_hold.sql': '61bae8d52794bdff877fc6ead8607cd43a14d4ab44c2160da0b61670e4a8687b',
 'crates/adapters/migrations/0030_replication_binding.sql': '04fd89a0eb81f20378f68225096470099232cf53f84001fa2eb11275a6347296',
 'crates/adapters/migrations/0031_canary_lifecycle.sql': 'faa87559897b8275a5c100cb87cf425aeab3efb9d2774505209c770fa7e111f3',
 'crates/adapters/migrations/0032_preservation_observers.sql': '22419315db431eeccfb8ff20b829f3852ddbf54d7be38139d648fd3ce6d9d3bb',
 'crates/adapters/migrations/0033_survivor_observers.sql': 'ab3ac18d2702a1d4281c0ee13744cd4b1fd6fa867b88c84fba0c9f884e61634e'}
CATALOG_MEMBERSHIPS = {
    'replication_owner': ('pg_read_all_stats','pg_read_all_settings'),
    'checkpoint_owner': ('pg_read_all_settings',),
}
OLD_RECOVERY = ('openlegal_admin.provider_inspect()', 'openlegal_admin.provider_inspect_selected(jsonb)',
                'openlegal_admin.provider_apply(jsonb)', 'openlegal_admin.provider_readback(uuid)')
RUNTIME_EXECUTE = ('openlegal_replication.capture_prefix()', 'openlegal_replication.observe_flush()',
                   'openlegal_admin.canary_consume(uuid)')
UNSET = ('PGSERVICE','PGSERVICEFILE','PGPASSWORD','PGHOST','PGHOSTADDR','PGPORT','PGDATABASE','PGUSER',
         'PGOPTIONS','PGSSLCERT','PGSSLKEY','PGSSLROOTCERT','PGSSLCRL','PGSSLCRLDIR','PGTARGETSESSIONATTRS',
         'PGCONNECT_TIMEOUT','PGREQUIREPEER','PGCHANNELBINDING','PGSSLMODE','PGREQUIRESSL','PGGSSENCMODE',
         'PGCLIENTENCODING','PGAPPNAME','PGPASSFILE')


def _shape(value, fields, code):
    need(type(value) is dict and set(value) == set(fields), code)


def _name(value):
    need(type(value) is str and re.fullmatch('[a-z_][a-z0-9_]{0,62}', value)
         and not value.startswith('pg_'), 'role_named_identifier')
    return value


def _quote(value):
    need(type(value) is str and '\x00' not in value, 'role_sql_literal')
    return "'" + value.replace("'", "''") + "'"


def _parent():
    return {'pid':os.getpid(), 'start_ticks':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],
            'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def _check(backend):
    driver._check_end(backend)
    driver._writer_fds(backend)
    seed._bound(backend)


def _load(inventory_ref, authority_ref):
    raw=driver._reference(inventory_ref); c=decode(raw)
    _shape(c, ('schema_version','kind','operation_id','source_sha256','seed_inventory','seed_authority',
               'roles','runtime_role','expected_function_owners'), 'role_inventory')
    need(type(c['schema_version']) is int and c['schema_version']==1
         and c['kind']=='openlegal_initial_role_prepare_inventory' and valid_uuid(c['operation_id'])
         and pin(c['source_sha256']), 'role_inventory_identity')
    private_bytes(Path(__file__).resolve(), c['source_sha256'])
    sc=seed._load(c['seed_inventory'],c['seed_authority'])
    need(sc[0]['operation_id']==c['operation_id'], 'role_seed_operation')
    _shape(c['roles'],ROLE_KEYS,'role_exact_names')
    names=[_name(c['roles'][k]) for k in ROLE_KEYS]; _name(c['runtime_role'])
    need(len(set(names))==len(names) and c['runtime_role'] not in names, 'role_names_overlap')
    _shape(c['expected_function_owners'],FUNCTIONS,'role_exact_function_owners')
    need(all(_name(v) not in names for v in c['expected_function_owners'].values()), 'role_old_owner_overlap')
    authraw=driver._reference(authority_ref); a=decode(authraw)
    _shape(a, ('schema_version','kind','approved','operation_id','inventory_sha256','source_sha256',
        'seed_inventory_sha256','seed_authority_sha256','driver_inventory_sha256','driver_authority_sha256',
        'valid_until','role_prepare_permitted','dba_uds_ddl_permitted','peer_role_permitted',
        'runtime_new_execute_and_schema_usage_permitted','preserve_originals','automatic_retry',
        'credentials_permitted','trust_permitted','ready_permitted','provider_permitted',
        'workload_permitted','drop_permitted'), 'role_authority')
    need(type(a['schema_version']) is int and a['schema_version']==1
         and a['kind']=='openlegal_initial_role_prepare_authority' and a['approved'] is True
         and a['operation_id']==c['operation_id'] and a['inventory_sha256']==digest(raw)
         and a['source_sha256']==c['source_sha256']
         and a['seed_inventory_sha256']==c['seed_inventory']['sha256']
         and a['seed_authority_sha256']==c['seed_authority']['sha256']
         and a['driver_inventory_sha256']==digest(sc[7]) and a['driver_authority_sha256']==digest(sc[9])
         and type(a['valid_until']) is int and time.time()<a['valid_until']
         and all(a[k] is True for k in ('role_prepare_permitted','dba_uds_ddl_permitted','peer_role_permitted',
                 'runtime_new_execute_and_schema_usage_permitted','preserve_originals'))
         and all(a[k] is False for k in ('automatic_retry','credentials_permitted','trust_permitted',
                 'ready_permitted','provider_permitted','workload_permitted','drop_permitted')), 'role_authority_scope')
    return c,a,raw,authraw,sc


def _grant_map(c):
    """Closed ACL data shared with independent consumers; no caller ACL fields."""
    g={k:set() for k in ROLE_KEYS}; g['runtime']=set()
    def grant(key,kind,obj,*privileges):
        g[key].update((kind,obj,p) for p in privileges)
    def schemas(key,*values):
        for s in values:grant(key,'schema',s,'USAGE')
    def funcs(key,*values):
        for f in values:grant(key,'function',f,'EXECUTE')
    def select(key,*values):
        for t in values:grant(key,'table',t,'SELECT')
    def update(key,table,*columns):
        for col in columns:grant(key,'column',table+'.'+col,'UPDATE')
    for signature,(owner,_,_) in FUNCTIONS.items():funcs(owner,signature)
    schemas('replication_owner','openlegal','openlegal_replication')
    schemas('checkpoint_owner',*SCHEMAS,'public')
    schemas('binding_owner',*SCHEMAS)
    schemas('canary_budget_owner','openlegal','openlegal_admin')
    schemas('canary_lifecycle_owner',*SCHEMAS)
    select('replication_owner','openlegal_replication.control')
    funcs('replication_owner','pg_catalog.pg_control_system()','pg_catalog.pg_control_checkpoint()')
    select('checkpoint_owner',*TABLES)
    update('checkpoint_owner','openlegal.corpus_control','singleton')
    update('checkpoint_owner','openlegal.provider_request_budget','singleton')
    update('checkpoint_owner','openlegal_admin.provider_control','recovery_hold','generation','hold_started_at')
    grant('checkpoint_owner','table','openlegal_checkpoint.operation','INSERT','UPDATE')
    funcs('checkpoint_owner','pg_catalog.pg_control_system()','pg_catalog.pg_control_checkpoint()',
          'openlegal_admin.slot_locked(integer)','openlegal_replication.survivor_position()')
    for table in ('openlegal.corpus_control','openlegal.provider_request_budget','openlegal_admin.provider_control'):
        select('binding_owner',table);update('binding_owner',table,'singleton')
    grant('binding_owner','table','openlegal_replication.control','SELECT','INSERT','UPDATE')
    funcs('binding_owner','openlegal_replication.capture_prefix()')
    select('canary_budget_owner','openlegal.provider_request_budget','openlegal.provider_request_admission',
           'openlegal_admin.provider_control','openlegal_admin.provider_recovery_audit',
           'openlegal_admin.provider_canary','openlegal_admin.provider_canary_control')
    update('canary_budget_owner','openlegal.provider_request_budget','singleton')
    update('canary_budget_owner','openlegal_admin.provider_control','singleton')
    for table in ('openlegal_admin.provider_canary','openlegal_admin.provider_canary_control'):
        grant('canary_budget_owner','table',table,'INSERT','UPDATE')
    grant('canary_budget_owner','table','openlegal_admin.provider_canary_audit','INSERT')
    select('canary_lifecycle_owner','openlegal.corpus_control','openlegal.provider_request_budget',
           'openlegal.provider_request_admission','openlegal_admin.provider_control',
           'openlegal_admin.provider_recovery_audit','openlegal_admin.provider_canary',
           'openlegal_admin.provider_canary_control','openlegal_replication.control',
           'openlegal_checkpoint.operation','openlegal_checkpoint.canary_lifecycle')
    for table in ('openlegal.corpus_control','openlegal.provider_request_budget','openlegal_replication.control'):
        update('canary_lifecycle_owner',table,'singleton')
    update('canary_lifecycle_owner','openlegal_admin.provider_control','recovery_hold','generation','hold_started_at')
    update('canary_lifecycle_owner','openlegal_admin.provider_canary','ended_at')
    update('canary_lifecycle_owner','openlegal_admin.provider_canary_control','required_id')
    update('canary_lifecycle_owner','openlegal_checkpoint.operation','operation_id')
    grant('canary_lifecycle_owner','table','openlegal_admin.provider_canary_audit','INSERT')
    grant('canary_lifecycle_owner','table','openlegal_checkpoint.canary_lifecycle','INSERT','UPDATE')
    funcs('canary_lifecycle_owner','openlegal_admin.canary_init(uuid,uuid)',
          'openlegal_replication.capture_prefix()','openlegal_checkpoint.state()')
    observations=tuple(s for s,(k,_,_) in FUNCTIONS.items() if k in ('checkpoint_owner','replication_owner'))
    schemas('storage_login','openlegal','openlegal_checkpoint','openlegal_replication')
    funcs('storage_login',*(s for s in observations if s not in (
        'openlegal_checkpoint.preservation_page(text,jsonb,integer)',
        'openlegal_checkpoint.publication_page(bigint,text,integer)',
        'openlegal_checkpoint.survivor_snapshot()','openlegal_checkpoint.provider_survivor_state()',
        'openlegal_replication.survivor_position()','openlegal_replication.observe_candidate(jsonb)')))
    schemas('recovery_login',*SCHEMAS,'public')
    funcs('recovery_login',*observations,*OLD_RECOVERY,'openlegal_checkpoint.bind_replica(uuid,text,jsonb,jsonb)')
    # Recovery CLI validates installed migration version without protected DML.
    select('recovery_login','public._sqlx_migrations')
    schemas('canary_login','openlegal','openlegal_checkpoint','openlegal_replication')
    funcs('canary_login',*(s for s,(k,_,_) in FUNCTIONS.items() if k=='canary_lifecycle_owner'),
          'openlegal_checkpoint.canary_observation(uuid)','openlegal_checkpoint.observe(uuid)',
          'openlegal_replication.capture_prefix()','openlegal_replication.observe_flush()')
    # openlegal_admin USAGE already belongs to legacy runtime, and must preexist.
    schemas('runtime','openlegal_replication')
    funcs('runtime',*RUNTIME_EXECUTE)
    return {key:sorted(value) for key,value in g.items()}


def manifest(c):
    """Pure source-fixed grant/signature map for pair/maintenance catalog review."""
    return {'functions':{s:{'owner':c['roles'][v[0]],'owner_key':v[0],'body_sha256':v[1],
                'expected_old_owner':c['expected_function_owners'][s]} for s,v in FUNCTIONS.items()},
            'roles':dict(c['roles']), 'runtime_role':c['runtime_role'],
            'grants':{c['roles'].get(k,c['runtime_role']):[list(r) for r in rows]
                      for k,rows in _grant_map(c).items()},
            'memberships':{c['roles'][k]:list(v) for k,v in CATALOG_MEMBERSHIPS.items()},
            'maintenance_only':True,'strict_ack':False}


def expected_catalog(role_names, runtime_role):
    """Public pure DATA contract used by the independently reviewed pair gate."""
    _shape(role_names,ROLE_KEYS,'role_expected_names'); _name(runtime_role)
    need(len(set(role_names.values()))==len(ROLE_KEYS) and runtime_role not in role_names.values(),
         'role_expected_overlap')
    for name in role_names.values():_name(name)
    c={'roles':role_names,'runtime_role':runtime_role}
    return {'flags':{name:{'login':key.endswith('_login'),'inherit':False,'super':False,
                'createdb':False,'createrole':False,'replication':key=='peer_login','bypass':False,
                'connection_limit':-1,'config':None,'valid_until':None}
                for key,name in role_names.items()},
            'grants':{role_names[k]:[list(r) for r in rows] for k,rows in _grant_map(c).items() if k!='runtime'},
            'memberships':sorted([[role_names[k],group,False,True,False]
                for k,groups in CATALOG_MEMBERSHIPS.items() for group in groups]),
            'functions':{sig:{'owner':role_names[row[0]],'body_sha256':row[1],
                'config':_function_config(sig),'public_execute':sig=='openlegal.replication_required()'}
                for sig,row in FUNCTIONS.items()}}


def _function_config(signature):
    value=['search_path=pg_catalog']
    if FUNCTIONS[signature][2] in (32,33):value+=['TimeZone=UTC','DateStyle=ISO,YMD']
    if FUNCTIONS[signature][2]==32 or signature=='openlegal_checkpoint.survivor_snapshot()':
        value+=['IntervalStyle=iso_8601','bytea_output=hex','extra_float_digits=3']
    return value


def catalog_sql(role_names, runtime_role):
    """Only fixed catalog relations and validated identifiers; never caller SQL."""
    expected_catalog(role_names,runtime_role)
    names=','.join(_quote(v) for v in sorted(role_names.values()))
    sigs=','.join(_quote(s) for s in sorted(FUNCTIONS))
    signature="n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')'"
    grant_rows="""SELECT a.grantee,'schema' kind,n.nspname object,a.privilege_type privilege,a.is_grantable
      FROM pg_namespace n CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a
      UNION ALL SELECT a.grantee,'function',%s,a.privilege_type,a.is_grantable
      FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
       CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a
      UNION ALL SELECT a.grantee,'table',n.nspname||'.'||c.relname,a.privilege_type,a.is_grantable
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 'S'::"char" ELSE 'r'::"char" END,c.relowner))) a
      UNION ALL SELECT a.grantee,'column',n.nspname||'.'||c.relname||'.'||t.attname,a.privilege_type,a.is_grantable
      FROM pg_attribute t JOIN pg_class c ON c.oid=t.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
       CROSS JOIN LATERAL aclexplode(t.attacl) a
      UNION ALL SELECT a.grantee,'database',d.datname,a.privilege_type,a.is_grantable
      FROM pg_database d CROSS JOIN LATERAL aclexplode(COALESCE(d.datacl,acldefault('d',d.datdba))) a""" % signature
    query = """SELECT jsonb_build_object('session_user',session_user,'current_user',current_user,
      'roles',COALESCE((SELECT jsonb_object_agg(r.rolname,jsonb_build_object(
       'login',r.rolcanlogin,'inherit',r.rolinherit,'super',r.rolsuper,'createdb',r.rolcreatedb,
       'createrole',r.rolcreaterole,'replication',r.rolreplication,'bypass',r.rolbypassrls,
       'connection_limit',r.rolconnlimit,'config',r.rolconfig,'valid_until',r.rolvaliduntil::text))
       FROM pg_roles r WHERE r.rolname IN (%s)),'{}'::jsonb),
      'grants',COALESCE((SELECT jsonb_agg(jsonb_build_array(r.rolname,g.kind,g.object,g.privilege,g.is_grantable)
       ORDER BY r.rolname COLLATE "C",g.kind COLLATE "C",g.object COLLATE "C",g.privilege COLLATE "C") FROM (%s) g JOIN pg_roles r ON r.oid=g.grantee
       WHERE r.rolname IN (%s)),'[]'::jsonb),
      'memberships',COALESCE((SELECT jsonb_agg(jsonb_build_array(member.rolname,owner.rolname,
       m.admin_option,m.inherit_option,m.set_option) ORDER BY member.rolname COLLATE "C",owner.rolname COLLATE "C")
       FROM pg_auth_members m JOIN pg_roles member ON member.oid=m.member JOIN pg_roles owner ON owner.oid=m.roleid
       WHERE member.rolname IN (%s) OR owner.rolname IN (%s)),'[]'::jsonb),
      'functions',COALESCE((SELECT jsonb_object_agg(v,jsonb_build_object('owner',r.rolname,
       'body_sha256',encode(sha256(convert_to(p.prosrc,'UTF8')),'hex'),'config',p.proconfig,
       'definer',p.prosecdef,'language',l.lanname,'acl',(SELECT jsonb_agg(jsonb_build_array(CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE (SELECT rolname FROM pg_roles WHERE oid=a.grantee) END,a.privilege_type,a.is_grantable) ORDER BY (CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE (SELECT rolname FROM pg_roles WHERE oid=a.grantee) END) COLLATE "C",a.privilege_type COLLATE "C") FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a),'public_execute',EXISTS(SELECT 1 FROM
        aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee=0 AND a.privilege_type='EXECUTE')))
       FROM unnest(ARRAY[%s]::text[])v LEFT JOIN pg_proc p ON p.oid=to_regprocedure(v)
       LEFT JOIN pg_roles r ON r.oid=p.proowner LEFT JOIN pg_language l ON l.oid=p.prolang),'{}'::jsonb),
      'other_owned',EXISTS(SELECT 1 FROM pg_shdepend d JOIN pg_roles r ON r.oid=d.refobjid
       WHERE d.refclassid='pg_authid'::regclass AND d.deptype='o' AND r.rolname IN (%s)
       AND NOT(d.dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
        AND d.classid='pg_proc'::regclass AND d.objid IN (SELECT to_regprocedure(v) FROM unnest(ARRAY[%s]::text[])v))),
      'effective',('ROLE_EFFECTIVE_PLACEHOLDER'),
      'runtime',jsonb_build_object('exists',EXISTS(SELECT 1 FROM pg_roles WHERE rolname=%s),
       'admin_usage',has_schema_privilege(%s,'openlegal_admin','USAGE'),
       'new_usage',has_schema_privilege(%s,'openlegal_replication','USAGE'),
       'executes',(SELECT jsonb_agg(has_function_privilege(%s,v,'EXECUTE') ORDER BY v)
         FROM unnest(ARRAY[%s]::text[])v)))""" % (
        names,grant_rows,names,names,names,sigs,names,sigs,_quote(runtime_role),_quote(runtime_role),
        _quote(runtime_role),_quote(runtime_role),','.join(_quote(s) for s in RUNTIME_EXECUTE))
    return query.replace("'ROLE_EFFECTIVE_PLACEHOLDER'", _effective_sql(role_names))


def check_catalog(value, role_names, runtime_role):
    """Fresh exact role/membership/direct-ACL/function closure; no effect."""
    e=expected_catalog(role_names,runtime_role)
    _shape(value,('session_user','current_user','roles','grants','memberships','functions','other_owned','effective','runtime'),
           'role_catalog_shape')
    need(value['session_user']==value['current_user'] and value['roles']==e['flags'], 'role_catalog_flags')
    wanted=sorted([name,*row,False] for name,rows in e['grants'].items() for row in rows)
    need(value['grants']==wanted and value['memberships']==e['memberships']
         and value['other_owned'] is False,'role_catalog_acl_or_membership')
    need(set(value['functions'])==set(FUNCTIONS), 'role_catalog_functions')
    for sig,row in value['functions'].items():
        acl=sorted([[name,'EXECUTE',False] for name,rows in e['grants'].items() if ['function',sig,'EXECUTE'] in rows]+([[runtime_role,'EXECUTE',False]] if sig in RUNTIME_EXECUTE else [])+([['PUBLIC','EXECUTE',False]] if sig=='openlegal.replication_required()' else []))
        need(row==dict(e['functions'][sig],acl=acl,definer=True,
                      language='sql' if sig in SQL_FUNCTIONS else 'plpgsql'), 'role_catalog_function_identity')
    _check_effective(value['effective'],e)
    need(value['runtime']=={'exists':True,'admin_usage':True,'new_usage':True,'executes':[True]*3},
         'role_catalog_runtime_chain')
    return e


SQL_FUNCTIONS = frozenset(('openlegal.replication_required()','openlegal_replication.observe_flush()',
    'openlegal_checkpoint.state()','openlegal_checkpoint.readback(uuid)',
    'openlegal_checkpoint.retained_references()','openlegal_checkpoint.database_layout()',
    'openlegal_checkpoint.client_sessions()','openlegal_checkpoint.storage_metadata()',
    'openlegal_checkpoint.canary_readback(uuid)'))


def _effective_sql(role_names):
    names=','.join(_quote(v) for v in sorted(role_names.values()))
    sig="n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')'"
    return """SELECT jsonb_build_object(
      'functions',COALESCE((SELECT jsonb_agg(jsonb_build_array(r.rolname,%s) ORDER BY r.rolname COLLATE "C",(%s) COLLATE "C")
       FROM pg_roles r CROSS JOIN pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
       WHERE r.rolname IN (%s) AND has_function_privilege(r.oid,p.oid,'EXECUTE')
       AND NOT EXISTS(SELECT 1 FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a
        WHERE a.grantee=0 AND a.privilege_type='EXECUTE')),'[]'::jsonb),
      'tables',COALESCE((SELECT jsonb_agg(jsonb_build_array(r.rolname,n.nspname||'.'||c.relname,v)
        ORDER BY r.rolname COLLATE "C",(n.nspname||'.'||c.relname) COLLATE "C",v COLLATE "C")
       FROM pg_roles r CROSS JOIN pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       CROSS JOIN unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN'])v
       WHERE r.rolname IN (%s) AND (n.nspname IN ('openlegal','openlegal_admin','openlegal_checkpoint','openlegal_replication')
        OR n.nspname='public' AND c.relname='_sqlx_migrations') AND c.relkind IN ('r','p','v','m','f')
       AND has_table_privilege(r.oid,c.oid,v)),'[]'::jsonb),
      'columns',COALESCE((SELECT jsonb_agg(jsonb_build_array(r.rolname,n.nspname||'.'||c.relname||'.'||t.attname,v)
        ORDER BY r.rolname COLLATE "C",(n.nspname||'.'||c.relname||'.'||t.attname) COLLATE "C",v COLLATE "C")
       FROM pg_roles r CROSS JOIN pg_attribute t JOIN pg_class c ON c.oid=t.attrelid
       JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN unnest(ARRAY['SELECT','INSERT','UPDATE','REFERENCES'])v
       WHERE r.rolname IN (%s) AND t.attnum>0 AND NOT t.attisdropped
       AND (n.nspname IN ('openlegal','openlegal_admin','openlegal_checkpoint','openlegal_replication')
        OR n.nspname='public' AND c.relname='_sqlx_migrations') AND c.relkind IN ('r','p','v','m','f')
       AND has_column_privilege(r.oid,c.oid,t.attnum,v) AND NOT has_table_privilege(r.oid,c.oid,v)),'[]'::jsonb),
      'schemas',COALESCE((SELECT jsonb_agg(jsonb_build_array(r.rolname,n.nspname) ORDER BY r.rolname COLLATE "C",n.nspname COLLATE "C")
       FROM pg_roles r CROSS JOIN pg_namespace n WHERE r.rolname IN (%s)
       AND has_schema_privilege(r.oid,n.oid,'USAGE') AND NOT EXISTS(SELECT 1 FROM
        aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a WHERE a.grantee=0 AND a.privilege_type='USAGE')),'[]'::jsonb),
      'create',EXISTS(SELECT 1 FROM pg_roles r CROSS JOIN pg_namespace n WHERE r.rolname IN (%s)
        AND has_schema_privilege(r.oid,n.oid,'CREATE')) OR EXISTS(SELECT 1 FROM pg_roles r CROSS JOIN pg_database d
        WHERE r.rolname IN (%s) AND has_database_privilege(r.oid,d.oid,'CREATE')),
      'membership_paths',COALESCE((SELECT jsonb_agg(jsonb_build_array(r.rolname,t.rolname,
         pg_has_role(r.oid,t.oid,'MEMBER'),pg_has_role(r.oid,t.oid,'USAGE'),pg_has_role(r.oid,t.oid,'SET'))
         ORDER BY r.rolname COLLATE "C",t.rolname COLLATE "C") FROM pg_roles r CROSS JOIN pg_roles t
       WHERE r.rolname IN (%s) AND r.oid<>t.oid AND
        (pg_has_role(r.oid,t.oid,'MEMBER') OR pg_has_role(r.oid,t.oid,'USAGE') OR pg_has_role(r.oid,t.oid,'SET'))),'[]'::jsonb),
      'admin_edges',COALESCE((SELECT jsonb_agg(jsonb_build_array(r.rolname,member.rolname,owner.rolname)
         ORDER BY r.rolname COLLATE "C",member.rolname COLLATE "C",owner.rolname COLLATE "C") FROM pg_roles r CROSS JOIN pg_auth_members m
       JOIN pg_roles member ON member.oid=m.member JOIN pg_roles owner ON owner.oid=m.roleid
       WHERE r.rolname IN (%s) AND m.admin_option AND
         (r.oid=m.member OR pg_has_role(r.oid,m.member,'MEMBER'))),'[]'::jsonb))""" % (sig,sig,names,names,names,names,names,names,names,names)


def _check_effective(value, expected):
    _shape(value,('functions','tables','columns','schemas','create','membership_paths','admin_edges'),'role_effective_shape')
    grants=expected['grants']
    need(value['create'] is False,'role_effective_create')
    paths=sorted([[member,group,True,True,False] for member,group,_,_,_ in expected['memberships']])
    need(value['membership_paths']==paths and value['admin_edges']==[],'role_effective_indirect_membership')
    for field,kind in (('functions','function'),('tables','table'),('columns','column'),('schemas','schema')):
        wanted=[]
        for name,rows in grants.items():
            for objkind,obj,priv in rows:
                if objkind!=kind:continue
                if kind=='function' and obj=='openlegal.replication_required()':continue
                if kind=='schema' and obj=='public':continue  # Default PUBLIC USAGE baseline.
                wanted.append([name,obj] if kind in ('function','schema') else [name,obj,priv])
        need(value[field]==sorted(wanted),'role_effective_'+field)


def _old_catalog_sql(c):
    """Normalize ACL defaults; exclude only the new source-fixed grantees."""
    names=','.join(_quote(v) for v in sorted(c['roles'].values()))
    managed=','.join(_quote(s) for s in sorted(FUNCTIONS))
    signature="n.nspname||'.'||p.proname||'('||replace(oidvectortypes(p.proargtypes),', ', ',')||')'"
    acl=lambda expression,extra='':("(SELECT COALESCE(jsonb_agg(jsonb_build_array(COALESCE(r.rolname,'PUBLIC'),"
        "a.privilege_type,a.is_grantable,owner.rolname) ORDER BY COALESCE(r.rolname,'PUBLIC'),a.privilege_type,owner.rolname),'[]'::jsonb)"
        " FROM aclexplode("+expression+")a LEFT JOIN pg_roles r ON r.oid=a.grantee JOIN pg_roles owner ON owner.oid=a.grantor"
        " WHERE (r.rolname IS NULL OR r.rolname NOT IN ("+names+"))"+extra+")")
    funcacl=acl("COALESCE(p.proacl,acldefault('f',p.proowner))")
    # Fixed grants to NEW recovery are the sole old-function ACL additions.
    # Already excluded by the new-grantee filter; exact new ACLs checked separately.
    schemaacl=acl("COALESCE(n.nspacl,acldefault('n',n.nspowner))",
        " AND NOT(COALESCE(r.rolname="+_quote(c['runtime_role'])+",false) AND n.nspname='openlegal_replication' AND a.privilege_type='USAGE')")
    tableacl=acl("COALESCE(t.relacl,acldefault('r',t.relowner))")
    columnacl=acl('t.attacl')
    return """SELECT jsonb_build_object(
      'roles',(SELECT jsonb_agg(to_jsonb(r)-'rolpassword' ORDER BY r.rolname) FROM pg_roles r WHERE r.rolname NOT IN (%s)),
      'memberships',COALESCE((SELECT jsonb_agg(to_jsonb(m) ORDER BY m.roleid,m.member)
       FROM pg_auth_members m WHERE m.member NOT IN(SELECT oid FROM pg_roles WHERE rolname IN (%s))
       AND m.roleid NOT IN(SELECT oid FROM pg_roles WHERE rolname IN (%s))),'[]'::jsonb),
      'functions',(SELECT jsonb_agg(jsonb_build_object('signature',%s,'definition',to_jsonb(p)-'proacl',
       'acl',%s) ORDER BY %s) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
       WHERE n.nspname IN ('openlegal','openlegal_admin','openlegal_replication','openlegal_checkpoint')
       AND p.oid NOT IN(SELECT to_regprocedure(v) FROM unnest(ARRAY[%s]::text[])v)),
      'schemas',(SELECT jsonb_agg(jsonb_build_object('name',n.nspname,'owner',n.nspowner,'acl',%s) ORDER BY n.nspname)
       FROM pg_namespace n WHERE n.nspname IN ('public','openlegal','openlegal_admin','openlegal_replication','openlegal_checkpoint')),
      'tables',(SELECT jsonb_agg(jsonb_build_object('schema',n.nspname,'table',t.relname,'owner',t.relowner,'kind',t.relkind,
       'acl',%s) ORDER BY n.nspname,t.relname) FROM pg_class t JOIN pg_namespace n ON n.oid=t.relnamespace
       WHERE t.relkind IN ('r','p','v','m','f') AND(n.nspname IN('openlegal','openlegal_admin','openlegal_replication','openlegal_checkpoint')
        OR n.nspname='public' AND t.relname='_sqlx_migrations')),
      'columns',(SELECT jsonb_agg(jsonb_build_object('relation',t.attrelid,'column',t.attnum,'acl',%s)
        ORDER BY t.attrelid,t.attnum) FROM pg_attribute t JOIN pg_class c ON c.oid=t.attrelid
       JOIN pg_namespace n ON n.oid=c.relnamespace WHERE t.attnum>0 AND NOT t.attisdropped AND
        (n.nspname IN('openlegal','openlegal_admin','openlegal_replication','openlegal_checkpoint')
         OR n.nspname='public' AND c.relname='_sqlx_migrations')))""" % (
            names,names,names,signature,funcacl,signature,managed,schemaacl,tableacl,columnacl)


def _preflight(c, value):
    expected=expected_catalog(c['roles'],c['runtime_role'])
    need(value['session_user']==value['current_user']=='postgres' and value['roles']=={}
         and value['grants']==[] and value['memberships']==[] and value['other_owned'] is False,
         'role_new_names_not_absent')
    need(value['runtime']['exists'] is True and value['runtime']['admin_usage'] is True,
         'role_existing_runtime_missing')
    need(set(value['functions'])==set(FUNCTIONS),'role_preflight_function_set')
    for sig,row in value['functions'].items():
        old=c['expected_function_owners'][sig]
        acl=sorted([[old,'EXECUTE',False]]+([['PUBLIC','EXECUTE',False]] if sig=='openlegal.replication_required()' else []))
        need(row==dict(expected['functions'][sig],owner=old,acl=acl,definer=True,
                       language='sql' if sig in SQL_FUNCTIONS else 'plpgsql'), 'role_preflight_function_changed')
    _shape(value['effective'],('functions','tables','columns','schemas','create','membership_paths','admin_edges'),'role_preflight_effective')
    need(value['effective']==dict.fromkeys(('functions','tables','columns','schemas','membership_paths','admin_edges'),[])|{'create':False},
         'role_preflight_names_not_absent')


def _ddl(c, before):
    """Exactly one transaction; repeated invocation fails CREATE ROLE, never adopts."""
    _preflight(c,before)
    sql=['BEGIN;',"SET LOCAL statement_timeout='30s'; SET LOCAL lock_timeout='1s'; SET LOCAL transaction_timeout='35s';",
         'DO $roleprepare$ BEGIN IF ('+catalog_sql(c['roles'],c['runtime_role'])+') IS DISTINCT FROM '+
         _quote(canonical(before).decode())+"::jsonb THEN RAISE EXCEPTION 'role preflight changed'; END IF; END $roleprepare$;"]
    for key in ROLE_KEYS:
        sql.append('CREATE ROLE '+_name(c['roles'][key])+(' LOGIN' if key.endswith('_login') else ' NOLOGIN')+
            ' NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE'+(' REPLICATION' if key=='peer_login' else ' NOREPLICATION')+
            ' NOBYPASSRLS CONNECTION LIMIT -1;')
    # Transfer only the exact closed new function list. No old M1-27 owner loop.
    owner_schemas={key:sorted({s.split('.')[0] for s,v in FUNCTIONS.items() if v[0]==key}) for key in ROLE_KEYS[:5]}
    for key,schemas in owner_schemas.items():
        sql.append('GRANT CREATE ON SCHEMA '+','.join(schemas)+' TO '+c['roles'][key]+';')
    for sig,(key,_,_) in FUNCTIONS.items():
        sql.append('ALTER FUNCTION '+sig+' OWNER TO '+c['roles'][key]+';')
    for key,schemas in owner_schemas.items():
        sql.append('REVOKE CREATE ON SCHEMA '+','.join(schemas)+' FROM '+c['roles'][key]+';')
    for key,rows in _grant_map(c).items():
        name=c['runtime_role'] if key=='runtime' else c['roles'][key]
        for kind,obj,priv in rows:
            # Function owners receive EXECUTE implicitly, not a new grant cycle.
            if kind=='function' and obj in FUNCTIONS and FUNCTIONS[obj][0]==key:continue
            if kind=='column':
                table,col=obj.rsplit('.',1);target='TABLE '+table;priv+='('+col+')'
            else:target={'schema':'SCHEMA','table':'TABLE','function':'FUNCTION'}[kind]+' '+obj
            sql.append('GRANT '+priv+' ON '+target+' TO '+name+';')
    for key,groups in CATALOG_MEMBERSHIPS.items():
        for group in groups:
            sql.append('GRANT '+group+' TO '+c['roles'][key]+' WITH ADMIN FALSE, INHERIT TRUE, SET FALSE;')
    sql+= ["SELECT jsonb_build_object('ddl_transaction','prepared_only_pending_commit');",'COMMIT;']
    return ('\n'.join(sql)+'\n').encode()


def _command(backend,op,sql,*,ddl=False):
    op.seal(); _check(backend)
    need(type(sql) is bytes and len(sql)<=3*1024**2, 'role_sql_bound')
    primary=op.inventory['linux']['postgres']['primary']['container_id']
    env=['/usr/bin/env']
    for name in UNSET:env+=['-u',name]
    env+=['PGPASSFILE=/dev/null','PGAPPNAME=openlegal_initial_role_prepare','PGCLIENTENCODING=UTF8']
    args=['exec','-i','-u','70',primary,*env,'psql','-X','-w','-h','/var/run/postgresql','-p','5432',
          '-qAt','-v','ON_ERROR_STOP=1','-U','postgres','-d','openlegal']
    prefix=(b"SET statement_timeout='30s'; SET lock_timeout='1s'; SET transaction_timeout='35s'; "
            b"SET TIME ZONE 'UTC'; SET DateStyle='ISO,YMD'; SET bytea_output='hex'; SET search_path=pg_catalog; ")
    stdin=prefix+sql
    raw=backend._run(op,'docker',args,stdin=stdin,timeout=31,deadline=backend._driver_deadline)
    name=PHASE+'-command-%04d.json' % backend._sequence
    sha=op.journal.output_hashes().get(name); need(pin(sha),'role_command_receipt_missing')
    receipt=decode(op.journal.read_phase_bytes(PHASE,name,sha))
    dispatch=['--config',str(backend._docker_config[0]),'--host','unix:///var/run/docker.sock']
    need(receipt['binary']=='docker' and receipt['arguments_sha256']==digest(canonical(dispatch+args))
         and receipt['stdin_sha256']==digest(stdin) and receipt['stdout_hex']==raw.hex()
         and receipt['stdout_sha256']==digest(raw) and receipt['stdout_bytes']==len(raw)
         and type(receipt['actual_exit_code']) is int and receipt['actual_exit_code']==0
         and receipt['started'] is True and receipt['owned_processes_closed'] is True
         and all(receipt[k] is False for k in ('expired','interrupted','unexpected_descendants','output_limit_exceeded')),
         'role_native_command_not_closed_zero')
    value=decode(raw)
    if ddl:need(value=={'ddl_transaction':'prepared_only_pending_commit'},'role_ddl_output')
    op.seal(); return value,{'phase':PHASE,'name':name,'sha256':sha}


def _query(backend,op,sql):
    return _command(backend,op,(sql+';\n').encode())[0]


def _saved(backend,op,name,value):
    ref=migration._saved(backend,op,name,value)
    return {'phase':PHASE,**ref}


def _local_ref(j,ref):
    _shape(ref,('phase','name','sha256'),'role_artifact_ref')
    need(pin(ref['sha256']),'role_artifact_sha')
    raw=j.read_phase_bytes(ref['phase'],ref['name'],ref['sha256'])
    need(digest(raw)==ref['sha256'],'role_artifact_raw')
    return raw


def _live(backend,lock,c,sc,allowed):
    seedcfg,_,_,_,dc,inv,template,draw,invraw,daraw=sc
    j=getattr(backend,'_initial_driver_journal',None)
    need(type(j) is journals.DualJournal and j.operation==c['operation_id']
         and j.binding['inventory_sha256']==digest(draw) and j.binding['authority_sha256']==digest(daraw)
         and j.phase_paths and j.phase_paths[0][0].name in allowed,'role_same_live_journal')
    need(getattr(backend,'_initial_blob_seed_started',False) is True
         and type(getattr(backend,'_initial_blob_seed_fds',None)) is dict,'role_live_seed_fds')
    lock.check(); j.check(); _check(backend)
    need(lock.path==Path(inv['shared_lock']['path']) and lock.identity==inv['shared_lock']['identity'], 'role_shared_lock')
    initialraw,_=seed._phase_raw(j,migration.PHASE,'driver-complete.json');initial=decode(initialraw)
    need(initial['kind']=='openlegal_initial_driver_result' and initial['operation_id']==c['operation_id']
         and initial['schema_version']==33 and initial['initial_actual_exit']==backend._initial_driver_last_exit
         and all(initial[k] is False for k in ('ready','roles_created','strict_seed_created','provider_calls','automatic_retry')),
         'role_driver_actual_result')
    for key in ('legacy_actual_exit','initial_actual_exit'):
        row=initial[key]
        _shape(row,('pid','start_ticks','boot_id','actual_exit_code','remaining_children'),'role_driver_exit')
        need(type(row['actual_exit_code']) is int and row['actual_exit_code']==0 and row['remaining_children']==[]
             and row['boot_id']==inv['host']['boot_id'],'role_driver_child_not_zero')
        manual._process_absent({k:row[k] for k in ('pid','start_ticks','boot_id')})
    anchorraw,_=seed._phase_raw(j,legacy.PHASE,legacy.PHASE+'-parent-fd-anchor.json');anchor=decode(anchorraw)
    need(all(anchor[k]==v for k,v in _parent().items()),'role_same_live_parent')
    need({(r['fd'],r['dev'],r['ino'],r['path']) for r in anchor['cgroups']}==
         {(fd,r['dev'],r['ino'],r['path']) for fd,r in backend._cgroup_fds.values()},'role_retained_parent_fds')
    completeraw,sha=seed._phase_raw(j,seed.PHASE,seed.PHASE+'-complete.json');complete=decode(completeraw)
    need(complete['kind']=='openlegal_initial_blob_seed_result' and complete['operation_id']==c['operation_id']
         and complete['managed_epoch']==seedcfg['managed_epoch'] and complete['media_profile']==seedcfg['media_profile']
         and pin(complete['blob_binding_sha256']) and {r['role'] for r in complete['roots']}==set(seed.ROLES)
         and all(complete[k] is False for k in ('postgres_replica_created','roles_created','workload_started',
             'provider_called','ready','automatic_retry')),'role_live_seed_complete')
    oldraw,_=seed._phase_raw(j,legacy.PHASE,legacy.PHASE+'-returned-payloads.json');old=decode(oldraw)
    current=copy.deepcopy(template);current.update(old['fresh_primary']);prereqs={}
    for key in ('drain','snapshot','preservation'):
        raw,p=seed._phase_raw(j,legacy.PHASE,'live-'+key+'-proof.json');value=decode(raw)
        need(value['producer_exit']==initial['legacy_actual_exit'] and value['payload']==old['payloads'][key]
             and value['operation_id']==c['operation_id'] and value['inventory_sha256']==digest(invraw)
             and value['authority_sha256']==digest(daraw),'role_initial_proof')
        prereqs[key]={'phase':legacy.PHASE,'name':'live-'+key+'-proof.json','sha256':p}
    current['prerequisites']=prereqs
    current['legacy_binding']={'inventory_sha256':digest(invraw),'source_graph_sha256':digest(canonical(dc['source_graph'])),
                               'authority_sha256':digest(daraw)}
    work=copy.deepcopy(inv);work['initial_migration']=current
    return j,work,old,{'phase':seed.PHASE,'name':seed.PHASE+'-complete.json','sha256':sha}


def _guard(backend,op):
    primary=op.inventory['linux']['postgres']['primary']['container_id']
    need(type(primary) is str and re.fullmatch('[0-9a-f]{64}',primary),'role_primary_identity')
    path=Path('/tmp/openlegal-initial-role-prepare-primary-'+primary+'.lock')
    fd=os.open(path,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_CLOEXEC|os.O_NOFOLLOW,0o600)
    backend._initial_role_prepare_guard_fd=fd
    fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
    raw=canonical({'operation_id':op.operation_id,'unknown_requires_manual_audit':True})
    view=memoryview(raw)
    while view:
        n=os.write(fd,view);need(n>0,'role_guard_write');view=view[n:]
    os.fsync(fd);os.lseek(fd,0,os.SEEK_SET);need(os.read(fd,len(raw)+1)==raw,'role_guard_readback')
    parent=os.open('/tmp',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:os.fsync(parent)
    finally:os.close(parent)
    _saved(backend,op,'one-attempt-guard',{'primary_id':primary,'operation_id':op.operation_id})


def run(backend, shared_lock, inventory_ref, authority_ref):
    """Called after actual four-root seed, before separate maintenance and pair."""
    need(type(backend) is driver.Backend and type(shared_lock) is journals.SharedLock
         and os.geteuid()==0 and threading.current_thread() is threading.main_thread()
         and len(list(Path('/proc/self/task').iterdir()))==1 and not processes._children(os.getpid())
         and not any(callable(v) for v in vars(backend).values())
         and not getattr(backend,'_initial_role_prepare_started',False),'role_live_native_backend')
    c,a,raw,authraw,sc=_load(inventory_ref,authority_ref)
    j,work,old,seedref=_live(backend,shared_lock,c,sc,(seed.PHASE,))
    need(j.count+64<=4096 and j.bytes+16*1024**2<=journals.MAX_PHASE_BYTES,'role_global_journal_budget')
    backend._initial_role_prepare_started=True
    j.begin(PHASE);frozen=canonical(work);parent=_parent()
    def seal():
        shared_lock.check();j.check();_check(backend)
        need(_parent()==parent and driver._reference(inventory_ref)==raw and driver._reference(authority_ref)==authraw
             and time.time()<a['valid_until'] and canonical(work)==frozen,'role_sealed_input_changed')
        latest=_load(inventory_ref,authority_ref)
        need(latest[:4]==(c,a,raw,authraw),'role_input_reloaded_changed')
        driver._source_seal(sc[4]['source_graph']);driver._docker_binding(backend,c['operation_id'])
    graphsha=digest(canonical({'driver':sc[4]['source_graph'],'initial_blob_seed.py':sc[0]['source_sha256'],
                               'initial_role_prepare.py':c['source_sha256']}))
    op=Operation(c['operation_id'],'strict_prepare',PHASE,work,digest(raw),graphsha,digest(authraw),j,
                 Path(sc[5]['backup_root']['path']),{},seal,freeze_authority(a))
    try:
        _guard(backend,op)
        # Fresh native/socket/schema/old-row checks; no trust in JSON phase names.
        before_native=seed._fresh(backend,op,old)
        sessions=migration._sessions(backend,op,backend._driver_deadline)
        control=_query(backend,op,"SELECT jsonb_build_object('required',openlegal.replication_required(),"
            "'rows',(SELECT count(*) FROM openlegal_replication.control))")
        need(control=={'required':False,'rows':0},'role_strict_control_already_bound')
        before_catalog=_query(backend,op,catalog_sql(c['roles'],c['runtime_role']));_preflight(c,before_catalog)
        before_oldcatalog=_query(backend,op,_old_catalog_sql(c))
        before_old=migration._old_state(backend,op,backend._driver_deadline,list(old['payloads']['preservation']['tables']))
        need(before_old==old['payloads']['preservation'],'role_legacy_before_changed')
        refs={
            'manifest':_saved(backend,op,'manifest',manifest(c)),
            'catalog_before':_saved(backend,op,'catalog-before',before_catalog),
            'legacy_before':_saved(backend,op,'legacy-before',{'rows':before_old,'catalog':before_oldcatalog,'native':before_native})}
        sql=_ddl(c,before_catalog)
        _saved(backend,op,'ddl-intent',{'operation_id':op.operation_id,'sql_sha256':digest(sql),'bytes':len(sql),
            'inventory_sha256':digest(raw),'authority_sha256':digest(authraw),'source_sha256':c['source_sha256'],
            'roles':dict(c['roles']),'seed_complete':seedref,'automatic_retry':False,'transactions':1,
            'authentication':'fixed_uid70_protected_uds_postgres_dba_only','maintenance_only':True,'strict_ack':False})
        # A single dispatch. Any failed/unknown COMMIT exits to sticky failure.
        _,refs['native_command']=_command(backend,op,sql,ddl=True)
        migration._sessions(backend,op,backend._driver_deadline,sessions)
        readback=_query(backend,op,catalog_sql(c['roles'],c['runtime_role']))
        need(readback['session_user']==readback['current_user']=='postgres','role_dba_identity_changed')
        check_catalog(readback,c['roles'],c['runtime_role'])
        after_oldcatalog=_query(backend,op,_old_catalog_sql(c))
        need(after_oldcatalog==before_oldcatalog,'role_existing_catalog_changed')
        after_old=migration._old_state(backend,op,backend._driver_deadline,list(before_old['tables']))
        need(after_old==before_old,'role_legacy_after_changed')
        after_native=seed._fresh(backend,op,old)
        need(after_native==before_native,'role_primary_lifetime_changed')
        need(_query(backend,op,"SELECT jsonb_build_object('required',openlegal.replication_required(),"
             "'rows',(SELECT count(*) FROM openlegal_replication.control))")==control,'role_control_changed')
        closure=migration._sessions(backend,op,backend._driver_deadline,sessions)
        refs['catalog_readback']=_saved(backend,op,'catalog-readback',readback)
        refs['legacy_after']=_saved(backend,op,'legacy-after',{'rows':after_old,'catalog':after_oldcatalog,'native':after_native})
        refs['sql_closure']=_saved(backend,op,'sql-closure',closure)
        result={'schema_version':1,'kind':'openlegal_initial_role_prepare_result','operation_id':op.operation_id,
            'driver_inventory_sha256':digest(sc[7]),'driver_authority_sha256':digest(sc[9]),
            'seed_inventory_sha256':c['seed_inventory']['sha256'],'seed_authority_sha256':c['seed_authority']['sha256'],
            'source_graph_sha256':graphsha,'inventory':copy.deepcopy(inventory_ref),'authority':copy.deepcopy(authority_ref),
            'producer_source':{'path':str(Path(__file__).resolve()),'sha256':c['source_sha256']},
            'parent':parent,'producer_execution':EXECUTION,'seed_complete':seedref,'role_names':dict(c['roles']),**refs,
            'maintenance_only':True,'strict_ack':False,'ready':False,'provider_called':False,'automatic_retry':False}
        reference=_saved(backend,op,'complete',result);op.seal()
        backend._initial_role_prepare_complete={'raw':canonical(result),'reference':reference,'parent':parent}
        return result
    except BaseException:
        try:_saved(backend,op,'failed',{'status':'FAILED_OR_UNKNOWN','automatic_retry':False,'drop_permitted':False,
                                      'maintenance_only':True,'strict_ack':False,'ready':False})
        except BaseException:pass
        raise


def _consume(backend,lock,reference,allowed):
    need(type(backend) is driver.Backend and type(lock) is journals.SharedLock and os.geteuid()==0
         and not any(callable(v) for v in vars(backend).values())
         and getattr(backend,'_initial_role_prepare_started',False) is True,'role_consumer_live_backend')
    live=getattr(backend,'_initial_role_prepare_complete',None)
    _shape(live,('raw','reference','parent'),'role_consumer_live_field')
    need(type(live['raw']) is bytes and live['reference']==reference and live['parent']==_parent(),
         'role_consumer_same_live_completion')
    result=decode(live['raw']);c,a,raw,authraw,sc=_load(result['inventory'],result['authority'])
    j,work,old,seedref=_live(backend,lock,c,sc,allowed)
    need(reference['phase']==PHASE and reference['name']==PHASE+'-complete.json'
         and _local_ref(j,reference)==live['raw'] and result['kind']=='openlegal_initial_role_prepare_result'
         and type(result['schema_version']) is int and result['schema_version']==1
         and result['operation_id']==c['operation_id'] and result['parent']==live['parent']
         and result['producer_execution']==EXECUTION and result['seed_complete']==seedref
         and result['role_names']==c['roles'] and result['driver_inventory_sha256']==digest(sc[7])
         and result['driver_authority_sha256']==digest(sc[9])
         and result['seed_inventory_sha256']==c['seed_inventory']['sha256']
         and result['seed_authority_sha256']==c['seed_authority']['sha256']
         and result['producer_source']=={'path':str(Path(__file__).resolve()),'sha256':c['source_sha256']}
         and result['source_graph_sha256']==digest(canonical({'driver':sc[4]['source_graph'],
             'initial_blob_seed.py':sc[0]['source_sha256'],'initial_role_prepare.py':c['source_sha256']}))
         and result['maintenance_only'] is True and all(result[k] is False for k in
             ('strict_ack','ready','provider_called','automatic_retry')),'role_consumer_chain')
    driver._source_seal(sc[4]['source_graph']);driver._docker_binding(backend,c['operation_id'])
    for key in ('manifest','catalog_before','catalog_readback','legacy_before','legacy_after','native_command','sql_closure'):
        need(result[key]['phase']==PHASE,'role_consumer_phase_ref');_local_ref(j,result[key])
    need(decode(_local_ref(j,result['manifest']))==manifest(c),'role_consumer_manifest')
    check_catalog(decode(_local_ref(j,result['catalog_readback'])),c['roles'],c['runtime_role'])
    need(decode(_local_ref(j,result['legacy_before']))==decode(_local_ref(j,result['legacy_after'])),
         'role_consumer_legacy_changed')
    need(decode(_local_ref(j,result['sql_closure']))['sessions']==[],'role_consumer_sql_not_closed')
    # Native command was checked with exact argv/stdin/output at production time.
    cmd=decode(_local_ref(j,result['native_command']))
    need(type(cmd['actual_exit_code']) is int and cmd['actual_exit_code']==0 and cmd['owned_processes_closed'] is True
         and cmd['started'] is True and all(cmd[k] is False for k in ('expired','interrupted','unexpected_descendants','output_limit_exceeded')),
         'role_consumer_native_not_zero_closed')
    _check(backend);lock.check();j.check();return result


def consume_live(backend,shared_lock,reference):
    """Maintenance may consume only this live current role phase once completed."""
    return _consume(backend,shared_lock,reference,(PHASE,))


def consume_archived_live(backend,shared_lock,reference):
    """Pair verifies same live raw role result after maintenance; no phase spoof."""
    return _consume(backend,shared_lock,reference,('initial_provider_maintenance','initial_pg_pair'))
