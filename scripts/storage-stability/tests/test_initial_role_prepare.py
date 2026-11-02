"""Source/pure catalog tests and owned /tmp guard only; no Docker/SQL/trust.

Synthetic command receipts are deliberately not native PostgreSQL acceptance.
Real production role/catalog/authentication and unknown-COMMIT closure remain
separate required gates.
"""
import copy
import os
from pathlib import Path
import re
import sys
import tempfile
import time
import types
import unittest
import uuid
from unittest.mock import patch

BASE=Path(__file__).resolve().parents[1];sys.path.insert(0,str(BASE))
import initial_role_prepare as r
from interface import Refused
from inventory import canonical,decode,digest


def config():
    return {'roles':{k:'fixture_'+k for k in r.ROLE_KEYS},'runtime_role':'runtime_fixture',
            'expected_function_owners':dict.fromkeys(r.FUNCTIONS,'migration_fixture')}


def catalog(c,*,before=False):
    e=r.expected_catalog(c['roles'],c['runtime_role']); g=e['grants']
    functions={}
    for sig,v in e['functions'].items():
        acl=sorted([[name,'EXECUTE',False] for name,rows in g.items() if ['function',sig,'EXECUTE'] in rows]+
             ([[c['runtime_role'],'EXECUTE',False]] if sig in r.RUNTIME_EXECUTE else [])+
             ([['PUBLIC','EXECUTE',False]] if sig=='openlegal.replication_required()' else []))
        functions[sig]=dict(v,acl=acl,definer=True,language='sql' if sig in r.SQL_FUNCTIONS else 'plpgsql')
        if before:
            functions[sig]['owner']=c['expected_function_owners'][sig]
            functions[sig]['acl']=sorted([[functions[sig]['owner'],'EXECUTE',False]]+
                ([['PUBLIC','EXECUTE',False]] if sig=='openlegal.replication_required()' else []))
    effective={'functions':[],'tables':[],'columns':[],'schemas':[],'create':False,'membership_paths':[],'admin_edges':[]}
    if not before:
        for field,kind in (('functions','function'),('tables','table'),('columns','column'),('schemas','schema')):
            rows=[]
            for name,grants in g.items():
                for k,obj,p in grants:
                    if k!=kind or kind=='function' and obj=='openlegal.replication_required()' or kind=='schema' and obj=='public':continue
                    rows.append([name,obj] if kind in ('function','schema') else [name,obj,p])
            effective[field]=sorted(rows)
        effective['membership_paths']=sorted([[a,b,True,True,False] for a,b,_,_,_ in e['memberships']])
    return {'session_user':'postgres','current_user':'postgres','roles':{} if before else e['flags'],
        'grants':[] if before else sorted([name,*row,False] for name,rows in g.items() for row in rows),
        'memberships':[] if before else e['memberships'],'functions':functions,'effective':effective,'other_owned':False,
        'runtime':{'exists':True,'admin_usage':True,'new_usage':not before,'executes':[not before]*3}}


class SourceContracts(unittest.TestCase):
    def test_every_source_body_language_and_set_value_matches_closed_28_signatures(self):
        root=BASE.parents[1];found={};tables=[]
        for p in sorted((root/'crates/adapters/migrations').glob('*.sql')):
            raw=p.read_bytes();text=raw.decode();version=int(p.name[:4])
            tables+=re.findall(r'CREATE TABLE (openlegal(?:_admin|_checkpoint|_replication)?\.[a-z_]+)',text)
            if version<28:continue
            self.assertEqual(digest(raw),r.MIGRATIONS[str(p.relative_to(root))])
            for m in re.finditer(r'CREATE FUNCTION\s+([a-z_.]+)\s*\(([^;]*?)\)\s*RETURNS\s+\w+\s+(.*?)AS \$\$(.*?)\$\$;',text,re.S):
                args=','.join(x.strip().split()[-1] for x in m[2].split(',') if x.strip())
                sig=m[1]+'('+args+')';self.assertIn(sig,r.FUNCTIONS)
                # Dollar quote delimiters are excluded, initial/final LF retained.
                self.assertTrue(m[4].startswith('\n'));self.assertTrue(m[4].endswith('\n') or m[4].endswith(' '))
                self.assertEqual(digest(m[4].encode()),r.FUNCTIONS[sig][1]);self.assertEqual(version,r.FUNCTIONS[sig][2])
                self.assertEqual('LANGUAGE sql' in m[3],sig in r.SQL_FUNCTIONS)
                settings=[]
                for name,value in re.findall(r'SET (\w+)=(\w+|\'[^\']*\')',m[3]):
                    settings.append(name+'='+value.strip("'"))
                self.assertEqual(settings,r._function_config(sig));found[sig]=True
        self.assertEqual(set(found),set(r.FUNCTIONS));self.assertEqual(len(found),28)
        self.assertEqual(sorted(tables+['public._sqlx_migrations']),list(r.TABLES))

    def test_names_cannot_escape_identifier_or_reuse_pg_roles(self):
        for bad in ('a;CREATE ROLE root','"r"','pg_read_all_stats','A','a-b','x'*64,'',None):
            with self.subTest(bad=bad),self.assertRaises(Refused):r._name(bad)
        c=config();c['roles']['peer_login']=c['roles']['storage_login']
        with self.assertRaisesRegex(Refused,'overlap'):r.expected_catalog(c['roles'],c['runtime_role'])

    def test_only_new_functions_change_owners_and_peer_is_sole_replication_login(self):
        c=config();ddl=r._ddl(c,catalog(c,before=True)).decode()
        self.assertEqual(ddl.count('BEGIN;'),1);self.assertEqual(ddl.count('COMMIT;'),1)
        self.assertEqual(ddl.count('CREATE ROLE '),9);self.assertEqual(ddl.count('ALTER FUNCTION '),28)
        owners=re.findall(r'ALTER FUNCTION (.*?) OWNER TO (.*?);',ddl)
        self.assertEqual({s for s,_ in owners},set(r.FUNCTIONS))
        creates=[line for line in ddl.splitlines() if line.startswith('CREATE ROLE')]
        repl=[line for line in creates if ' REPLICATION ' in line]
        self.assertEqual(len(repl),1);self.assertIn(c['roles']['peer_login'],repl[0])
        for line in creates:
            self.assertIn(' NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE',line);self.assertIn(' NOBYPASSRLS',line)
        for forbidden in ('IF NOT EXISTS','DROP ROLE','PASSWORD','ALTER ROLE','ALTER SYSTEM','pg_reload_conf','pg_cancel_backend'):
            self.assertNotIn(forbidden,ddl)
        self.assertNotIn('ALL FUNCTIONS',ddl);self.assertNotIn('ALL TABLES',ddl)
        for key in r.ROLE_KEYS[:5]:
            name=c['roles'][key]
            self.assertTrue(any(line.startswith('REVOKE CREATE ON SCHEMA ') and line.endswith(' FROM '+name+';') for line in ddl.splitlines()))

    def test_grants_are_source_fixed_and_logins_have_no_owner_memberships(self):
        c=config();e=r.expected_catalog(c['roles'],c['runtime_role']);g=e['grants']
        for key in ('storage_login','recovery_login','canary_login','peer_login'):
            self.assertFalse(any(member==c['roles'][key] for member,*_ in e['memberships']))
            rows=g[c['roles'][key]]
            self.assertFalse(any(row[0] in ('table','column') and row[2]!='SELECT' for row in rows))
        self.assertEqual(g[c['roles']['peer_login']],[])
        bind=['function','openlegal_checkpoint.bind_replica(uuid,text,jsonb,jsonb)','EXECUTE']
        self.assertNotIn(bind,g[c['roles']['storage_login']]);self.assertIn(bind,g[c['roles']['recovery_login']])
        self.assertEqual(r._grant_map(c)['runtime'],[
            ('function','openlegal_admin.canary_consume(uuid)','EXECUTE'),
            ('function','openlegal_replication.capture_prefix()','EXECUTE'),
            ('function','openlegal_replication.observe_flush()','EXECUTE'),('schema','openlegal_replication','USAGE')])
        for name,rows in g.items():
            for kind,obj,priv in rows:
                self.assertNotEqual(priv,'CREATE');self.assertNotIn('ALL',priv)

    def test_catalog_uses_public_baseline_and_all_indirect_privilege_gates(self):
        c=config();sql=r.catalog_sql(c['roles'],c['runtime_role'])
        self.assertIn("aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner)))",sql)
        self.assertIn("pg_has_role(r.oid,t.oid,'MEMBER')",sql);self.assertIn("pg_has_role(r.oid,t.oid,'USAGE')",sql)
        self.assertIn("pg_has_role(r.oid,t.oid,'SET')",sql);self.assertIn('m.admin_option',sql)
        self.assertIn('has_column_privilege',sql);self.assertIn('has_database_privilege',sql)
        self.assertIn("d.deptype='o'",sql);self.assertNotIn('ROLE_EFFECTIVE_PLACEHOLDER',sql)
        self.assertIn("'S'::\"char\"",sql)


class CatalogNegatives(unittest.TestCase):
    def setUp(self):self.c=config();self.value=catalog(self.c)
    def check(self,v):return r.check_catalog(v,self.c['roles'],self.c['runtime_role'])
    def test_exact_closed_catalog_accepts_and_preflight_requires_all_names_absent(self):
        self.check(self.value);r._preflight(self.c,catalog(self.c,before=True))
        v=catalog(self.c,before=True);v['roles']={'fixture_storage_login':self.value['roles']['fixture_storage_login']}
        with self.assertRaisesRegex(Refused,'not_absent'):r._preflight(self.c,v)
    def test_every_owner_body_config_language_public_acl_drift_refuses(self):
        sig='openlegal_checkpoint.bind_replica(uuid,text,jsonb,jsonb)'
        for field,value in (('owner','runtime_fixture'),('body_sha256','f'*64),('definer',False),
             ('language','sql'),('config',['search_path=public']),('public_execute',True),
             ('acl',self.value['functions'][sig]['acl']+[['old_broad_login','EXECUTE',False]])):
            v=copy.deepcopy(self.value);v['functions'][sig][field]=value
            with self.subTest(field=field),self.assertRaises(Refused):self.check(v)
    def test_login_flags_direct_grants_grant_option_and_foreign_ownership_refuse(self):
        for key in ('super','createdb','createrole','bypass','inherit','replication'):
            v=copy.deepcopy(self.value);v['roles']['fixture_recovery_login'][key]=True
            with self.subTest(key=key),self.assertRaises(Refused):self.check(v)
        v=copy.deepcopy(self.value);v['other_owned']=True
        with self.assertRaises(Refused):self.check(v)
        for row in (['fixture_recovery_login','table','openlegal_admin.provider_control','UPDATE',False],
                    ['fixture_recovery_login','function','openlegal_admin.provider_apply(jsonb)','EXECUTE',True]):
            v=copy.deepcopy(self.value);v['grants'].append(row);v['grants'].sort()
            with self.assertRaises(Refused):self.check(v)
    def test_indirect_member_set_and_admin_paths_are_not_hidden_by_direct_acl(self):
        for field,row in (('membership_paths',['fixture_checkpoint_owner','pg_write_server_files',True,False,True]),
                          ('admin_edges',['fixture_checkpoint_owner','pg_read_all_settings','foreign_role'])):
            v=copy.deepcopy(self.value);v['effective'][field].append(row);v['effective'][field].sort()
            with self.assertRaisesRegex(Refused,'indirect_membership'):self.check(v)
        v=copy.deepcopy(self.value);v['memberships'][0][-1]=True
        with self.assertRaises(Refused):self.check(v)
    def test_effective_nonpublic_catalog_functions_column_dml_and_create_refuse(self):
        for field,row in (('functions',['fixture_peer_login','pg_catalog.pg_reload_conf()']),
             ('columns',['fixture_recovery_login','openlegal.provider_request_budget.unresolved_response','UPDATE']),
             ('tables',['fixture_recovery_login','openlegal_admin.provider_control','UPDATE']),
             ('schemas',['fixture_peer_login','openlegal_replication'])):
            v=copy.deepcopy(self.value);v['effective'][field].append(row);v['effective'][field].sort()
            with self.subTest(field=field),self.assertRaises(Refused):self.check(v)
        v=copy.deepcopy(self.value);v['effective']['create']=True
        with self.assertRaisesRegex(Refused,'effective_create'):self.check(v)
    def test_preflight_changed_body_or_preexisting_grantee_cannot_generate_ddl(self):
        v=catalog(self.c,before=True);v['functions']['openlegal.replication_required()']['body_sha256']='f'*64
        with self.assertRaisesRegex(Refused,'preflight_function_changed'):r._ddl(self.c,v)
        v=catalog(self.c,before=True);v['functions']['openlegal.replication_required()']['acl'].append(['runtime_fixture','EXECUTE',False])
        with self.assertRaises(Refused):r._ddl(self.c,v)


class Journal:
    def __init__(self):self.rows={}
    def save(self,name,value):
        if name in self.rows:raise FileExistsError(name)
        self.rows[name]=canonical(value);return digest(self.rows[name])
    def output_hashes(self):return {k:digest(v) for k,v in self.rows.items()}
    def read_phase_bytes(self,phase,name,sha):
        raw=self.rows[name];assert digest(raw)==sha;return raw


class CommandBoundary(unittest.TestCase):
    def setUp(self):
        self.j=Journal();self.op=types.SimpleNamespace(seal=lambda:None,journal=self.j,
            inventory={'linux':{'postgres':{'primary':{'container_id':'a'*64}}}})
        self.calls=[];self.bad=None;test=self
        class Backend:
            _sequence=0;_docker_config=(Path('/tmp/explicit-synthetic-client'),(1,2));_driver_deadline=time.monotonic()+30
            def _run(inner,op,binary,args,**kw):
                test.calls.append((binary,args,kw));inner._sequence+=1;raw=canonical({'ddl_transaction':'prepared_only_pending_commit'})
                value={'binary':'docker','arguments_sha256':digest(canonical(['--config',str(inner._docker_config[0]),'--host','unix:///var/run/docker.sock',*args])),
                    'stdin_sha256':digest(kw['stdin']),'stdout_hex':raw.hex(),'stdout_sha256':digest(raw),'stdout_bytes':len(raw),
                    'actual_exit_code':0,'started':True,'owned_processes_closed':True,'expired':False,'interrupted':False,
                    'unexpected_descendants':False,'output_limit_exceeded':False}
                if test.bad:value[test.bad[0]]=test.bad[1]
                test.j.save(r.PHASE+'-command-%04d.json'%inner._sequence,value);return raw
        self.b=Backend();self.check=patch.object(r,'_check',lambda backend:None);self.check.start();self.addCleanup(self.check.stop)
    def test_uid70_only_dba_uds_sanitizes_all_libpq_inputs_and_receipt_is_native_zero(self):
        value,ref=r._command(self.b,self.op,b'BEGIN; COMMIT;',ddl=True)
        binary,args,kw=self.calls[0];self.assertEqual(binary,'docker');self.assertEqual(args[:4],['exec','-i','-u','70'])
        self.assertIn('/usr/bin/env',args);self.assertIn('/var/run/postgresql',args)
        self.assertEqual(args[args.index('-U')+1],'postgres');self.assertEqual(args[args.index('-d')+1],'openlegal')
        for key in r.UNSET:self.assertIn(['-u',key],[args[i:i+2] for i in range(len(args)-1)])
        self.assertIn('-w',args);self.assertIn('-X',args);self.assertEqual(kw['deadline'],self.b._driver_deadline)
        self.assertEqual(len(self.calls),1);self.assertEqual(ref['phase'],r.PHASE)
    def test_unknown_commit_nonzero_or_open_native_processes_never_ack(self):
        for key,value in (('actual_exit_code',1),('actual_exit_code',False),('owned_processes_closed',False),
             ('started',False),('expired',True),('interrupted',True),('unexpected_descendants',True),
             ('output_limit_exceeded',True),('stdin_sha256','f'*64)):
            self.bad=(key,value)
            with self.subTest(key=key,value=value),self.assertRaisesRegex(Refused,'native_command_not_closed_zero'):
                r._command(self.b,self.op,b'BEGIN; COMMIT;',ddl=True)
        self.assertEqual(len(self.calls),9)
    def test_saved_artifact_requires_exact_dual_acknowledgement(self):
        self.b._artifact=lambda op,name,value:self.j.save(op.phase+'-'+name+'.json',value)
        self.op.phase=r.PHASE
        ref=r._saved(self.b,self.op,'test',{'x':1});self.assertEqual(decode(r._local_ref(self.j,ref)),{'x':1})
        self.j.rows[ref['name']]=canonical({'x':2})
        with self.assertRaises(AssertionError):r._local_ref(self.j,ref)


@unittest.skipUnless(os.geteuid()==0,'root ownership gate')
class RootGuard(unittest.TestCase):
    def test_actual_exclusive_guard_is_durable_and_never_adopts_existing_attempt(self):
        primary=uuid.uuid4().hex*2;path=Path('/tmp/openlegal-initial-role-prepare-primary-'+primary+'.lock')
        self.addCleanup(lambda:path.unlink(missing_ok=True));b=types.SimpleNamespace();op=types.SimpleNamespace(
            operation_id=str(uuid.uuid4()),inventory={'linux':{'postgres':{'primary':{'container_id':primary}}}})
        with patch.object(r,'_saved'):
            r._guard(b,op);self.addCleanup(os.close,b._initial_role_prepare_guard_fd)
            self.assertEqual(path.stat().st_uid,0);self.assertEqual(path.stat().st_mode&0o777,0o600)
            self.assertEqual(decode(path.read_bytes()),{'operation_id':op.operation_id,'unknown_requires_manual_audit':True})
            with self.assertRaises(FileExistsError):r._guard(types.SimpleNamespace(),op)


class LiveRefusal(unittest.TestCase):
    def test_historical_receipt_or_fake_backend_is_not_live_authority(self):
        for method in (r.run,r.consume_live,r.consume_archived_live):
            with self.subTest(method=method.__name__),self.assertRaisesRegex(Refused,'live_native_backend|consumer_live_backend'):
                method(types.SimpleNamespace(),types.SimpleNamespace(),{}) if method!=r.run else method(types.SimpleNamespace(),types.SimpleNamespace(),{},{})


class SyntheticLiveFlow(CommandBoundary):
    """Exact source flow with explicitly synthetic private/native ports."""
    def setUp(self):
        super().setUp();self.c=config();self.c.update(operation_id=str(uuid.uuid4()),source_sha256='a'*64,
            seed_inventory={'path':'/synthetic/seed','sha256':'b'*64},
            seed_authority={'path':'/synthetic/seed-authority','sha256':'c'*64})
        self.a={'valid_until':int(time.time())+60};self.invref={'path':'/synthetic/role','sha256':'d'*64}
        self.authref={'path':'/synthetic/role-authority','sha256':'e'*64}
        self.raw=b'role-inventory';self.authraw=b'role-authority'
        self.sc=({'source_sha256':'f'*64},None,None,None,{'source_graph':{}},
            {'backup_root':{'path':'/tmp'}},None,b'driver-inventory',None,b'driver-authority')
        self.oldstate={'tables':{'openlegal.corpus_object':{'rows':1,'sha256':'2'*64}},'references':{},'migration_history':{}}
        self.old={'payloads':{'preservation':copy.deepcopy(self.oldstate)}}
        self.work={'linux':{'postgres':{'primary':{'container_id':'a'*64}}}}
        self.seedref={'phase':'initial_blob_seed','name':'initial_blob_seed-complete.json','sha256':'9'*64}
        self.j.count=0;self.j.bytes=0;self.j.check=lambda:None;self.j.begin=lambda phase:None
        artifact=lambda backend,op,name,value:self.j.save(op.phase+'-'+name+'.json',value)
        item=patch.object(type(self.b),'_artifact',artifact,create=True);item.start();self.addCleanup(item.stop)
        self.lock=types.SimpleNamespace(check=lambda:None)
        self.port_calls=[]
        before=catalog(self.c,before=True);after=catalog(self.c)
        self.queries=[{'required':False,'rows':0},before,{'catalog':'unchanged'},after,{'catalog':'unchanged'},
                      {'required':False,'rows':0}]
        def query(backend,op,sql):self.port_calls.append('query');return self.queries.pop(0)
        patches=[patch.object(r.driver,'Backend',type(self.b)),patch.object(r.journals,'SharedLock',type(self.lock)),
            patch.object(r,'_load',return_value=(self.c,self.a,self.raw,self.authraw,self.sc)),
            patch.object(r,'_live',return_value=(self.j,self.work,self.old,self.seedref)),
            patch.object(r,'_guard',side_effect=lambda *args:self.port_calls.append('guard')),
            patch.object(r,'_query',side_effect=query),patch.object(r.seed,'_fresh',return_value={'schema_version':33}),
            patch.object(r.migration,'_sessions',return_value={'sessions':[]}),
            patch.object(r.migration,'_old_state',return_value=self.oldstate),
            patch.object(r.driver,'_reference',side_effect=lambda ref:self.raw if ref==self.invref else self.authraw),
            patch.object(r.driver,'_source_seal'),patch.object(r.driver,'_docker_binding')]
        for item in patches:item.start();self.addCleanup(item.stop)
    def test_flow_records_intent_before_exact_one_native_dispatch_then_live_result(self):
        original=self.b._run
        def checked(op,binary,args,**kwargs):
            self.assertIn(r.PHASE+'-ddl-intent.json',self.j.rows)
            self.assertNotIn(r.PHASE+'-complete.json',self.j.rows)
            return original(op,binary,args,**kwargs)
        self.b._run=checked
        # Authorized source forbids instance callbacks. Preserve that boundary by
        # installing this explicit synthetic hook on its test class only.
        del self.b._run
        with patch.object(type(self.b),'_run',side_effect=checked):
            result=r.run(self.b,self.lock,self.invref,self.authref)
        self.assertEqual(len(self.calls),1);self.assertEqual(self.port_calls[0],'guard')
        self.assertEqual(result['producer_execution'],r.EXECUTION)
        self.assertTrue(result['maintenance_only']);self.assertFalse(result['strict_ack'])
        self.assertNotIn('producer_exit',result)
        live=self.b._initial_role_prepare_complete
        self.assertEqual(live['raw'],canonical(result));self.assertEqual(decode(r._local_ref(self.j,live['reference'])),result)
        self.assertEqual(r.consume_live(self.b,self.lock,live['reference']),result)
        self.assertEqual(r.consume_archived_live(self.b,self.lock,live['reference']),result)
        live['parent']=dict(live['parent'],pid=live['parent']['pid']+1)
        with self.assertRaisesRegex(Refused,'same_live_completion'):r.consume_live(self.b,self.lock,live['reference'])
        live['parent']=result['parent']
        with self.assertRaisesRegex(Refused,'live_native_backend'):r.run(self.b,self.lock,self.invref,self.authref)
    def test_unknown_commit_leaves_sticky_started_without_complete_and_never_retries(self):
        self.bad=('actual_exit_code',1)
        with self.assertRaisesRegex(Refused,'native_command_not_closed_zero'):
            r.run(self.b,self.lock,self.invref,self.authref)
        self.assertTrue(self.b._initial_role_prepare_started)
        self.assertFalse(hasattr(self.b,'_initial_role_prepare_complete'))
        self.assertEqual(len(self.calls),1);self.assertIn(r.PHASE+'-failed.json',self.j.rows)
        with self.assertRaisesRegex(Refused,'live_native_backend'):r.run(self.b,self.lock,self.invref,self.authref)
        self.assertEqual(len(self.calls),1)


if __name__=='__main__':unittest.main(verbosity=2)
