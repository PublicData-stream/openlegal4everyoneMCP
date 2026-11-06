"""Harmless /tmp/real-fork tests; NO Docker/PostgreSQL/native acceptance.

The paired /tmp directories below exercise real dual raw-file reconciliation,
not independent media durability. Provider state/audits are explicit synthetic
records. Child status, waitpid/pidfd ownership and source phase handoff are real.
"""
import copy
from contextlib import ExitStack
import errno
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import types
import unittest
import uuid
from unittest.mock import patch

BASE=Path(__file__).resolve().parents[1]; sys.path.insert(0,str(BASE))
import initial_provider_maintenance as m
from interface import Refused
from inventory import canonical,digest


def request(owners=None):
    return {'actor':'fixture operator','reason':'hold-preserving maintenance',
        'quiescence':{'writers_stopped_at':int(time.time())-10,'deployment_revision':'fictional-revision',
            'stopped_writer_ids':['fictional-serving','fictional-scheduler'],
            'evidence_reference':'fictional retained fence'},
        'resolve_legacy':False,'owners':owners or [],'resume':False,'waits':[]}


def audit_fixture():
    owner=str(uuid.uuid4()); before={'all_slots_free':True,'uncertainty':[],
        'state':{'storage_id':'old-storage','corpus_control':{'epoch':7,'stop':True},'schema':[1,2,3],
            'budget':{'unresolved_response':False,'admission_owner':None,'charged':12,
                'next_request_at_ms':9999999999999,'operator_suspended':True},
            'control':{'generation':str(uuid.uuid4()),'recovery_hold':False,'hold_started_at':None,
                'legacy_identity':str(uuid.uuid4())},
            'legacy_lock_held':False,'ledger':[{'owner':owner,'slot':3,'lock_held':False,'started_at_ms':1000}]}}
    now=int(time.time()); nested=str(uuid.uuid4()); req=request([owner])
    plan={'version':1,'operation_id':nested,'created_at':now,'expires_at':now+900,'request':req,
        'snapshot':{'version':1,'observed_at':now,'state':copy.deepcopy(before['state']),
            'waiting':[],'waiting_truncated':False}}
    result={'version':1,'operation_id':nested,'applied_at':now,'recovery_hold':True,
        'hold_generation':str(uuid.uuid4()),'resolved_legacy':False,'resolved_owners':[owner],
        'advanced_waits':[],'unchanged_waits':[]}
    audit={'operation_id':nested,'plan':copy.deepcopy(plan),'actor':req['actor'],
        'database_actor':'provider_operator','actual_writers_stopped_at':req['quiescence']['writers_stopped_at'],
        'applied_at':now,'before_state':copy.deepcopy(before['state']),'result':copy.deepcopy(result)}
    after=copy.deepcopy(before); after['state']['ledger']=[]
    after['state']['control'].update(generation=result['hold_generation'],recovery_hold=True,hold_started_at=now)
    after['uncertainty']=[{'identity':owner,'kind':'slot','owner':owner,'slot':3,'first_observed_at':None}]
    return plan,result,copy.deepcopy(result),audit,before,after,'provider_operator'


class ContractTests(unittest.TestCase):
    def test_missing_live_context_never_loads_or_dispatches(self):
        with patch.object(m,'_load',side_effect=AssertionError('no input load')):
            with self.assertRaisesRegex(Refused,'live_dedicated_root_backend'):
                m.run(object(),object(),{}, {})

    def test_expired_original_deadline_never_loads_inputs(self):
        b=m.driver.Backend(); b._driver_deadline=time.monotonic()-1
        lock=m.journals.SharedLock({'path':'/tmp/unopened-maintenance-lock','identity':{'dev':1,'ino':2}})
        with patch.object(m,'_load',side_effect=AssertionError('no native inputs')):
            with self.assertRaisesRegex(Refused,'driver_original_deadline'):m.run(b,lock,{}, {})

    def test_request_cannot_resume_resolve_legacy_or_change_waits(self):
        for key,value in [('resume',True),('resolve_legacy',True),('waits',[{'id':str(uuid.uuid4())}])]:
            r=request(); r[key]=value
            with self.subTest(key=key),self.assertRaisesRegex(Refused,'no_release_or_legacy_resolution'):m._request(r)
        r=request(); r['command']='invented'
        with self.assertRaisesRegex(Refused,'maintenance_request'):m._request(r)

    def test_duplicate_owner_bad_uuid_future_time_and_control_text_refuse(self):
        cases=[]; r=request(); r['owners']=[str(uuid.uuid4())]*2; cases.append(r)
        r=request(); r['owners']=['not-a-uuid']; cases.append(r)
        r=request(); r['quiescence']['writers_stopped_at']=int(time.time())+10; cases.append(r)
        r=request(); r['actor']='bad\ntext'; cases.append(r)
        for r in cases:
            with self.subTest(request=r),self.assertRaises(Refused):m._request(r)

    def test_full_unlocked_ledger_and_all_seventeen_locks_required(self):
        _,_,_,_,before,_,_=audit_fixture(); owner=before['state']['ledger'][0]['owner']
        self.assertEqual(m._state_before(before,[owner]),before['state'])
        for mutate in (lambda b:b.update(all_slots_free=False),
            lambda b:b['state']['ledger'][0].update(lock_held=True),
            lambda b:b['state']['budget'].update(unresolved_response=True),
            lambda b:b['state']['budget'].update(admission_owner=owner)):
            changed=copy.deepcopy(before); mutate(changed)
            with self.subTest(changed=changed),self.assertRaises(Refused):m._state_before(changed,[owner])
        with self.assertRaisesRegex(Refused,'full_unlocked_ledger_review'):m._state_before(before,[])

    def test_cli_plan_mints_nested_uuid_without_rewriting_outer_operation(self):
        plan,_,_,_,before,_,_=audit_fixture(); outer=str(uuid.uuid4())
        self.assertIs(m._plan(plan,plan['request'],before['state'],outer),plan)
        with self.assertRaisesRegex(Refused,'nested_plan_binding'):
            m._plan(plan,plan['request'],before['state'],plan['operation_id'])
        changed=copy.deepcopy(plan); changed['request']['resume']=True
        with self.assertRaisesRegex(Refused,'nested_plan_binding'):
            m._plan(changed,plan['request'],before['state'],outer)

    def test_actual_zero_is_not_python_false_or_docker_client_only(self):
        receipt={'actual_exit_code':0,'started':True,'owned_processes_closed':True,'expired':False,
            'interrupted':False,'unexpected_descendants':False,'output_limit_exceeded':False}
        m._command_ok(receipt)
        for field,value in [('actual_exit_code',False),('actual_exit_code',1),('owned_processes_closed',False),
            ('started',False),('expired',True),('unexpected_descendants',True)]:
            changed=copy.deepcopy(receipt); changed[field]=value
            with self.subTest(field=field,value=value),self.assertRaises(Refused):m._command_ok(changed)

    def test_gate_is_fixed_env_and_has_one_source_owned_apply(self):
        for action in ('plan','apply','readback'):
            script=m._gate(action,'provider_operator')
            self.assertIn('exec /usr/bin/env -i ',script)
            self.assertIn('OPENLEGAL_PROVIDER_ADMIN_DATABASE_URL=postgresql://provider_operator@localhost/',script)
            self.assertIn('openlegal-server --provider-admin '+action+' ',script)
            self.assertNotIn('--resume',script); self.assertNotIn('PGPASSWORD',script)
            self.assertNotIn('PGSERVICE=',script); self.assertEqual(script.count('openlegal-server'),1)
        for action in ('inspect','resume','../apply','apply;true'):
            with self.assertRaises(Refused):m._gate(action,'provider_operator')
        with self.assertRaisesRegex(Refused,'named_role'):m._gate('apply','role;touch /tmp/unwanted')

    def test_auth_profile_rejects_password_or_tls_shape_without_qualified_profile(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as root:
            root=Path(root); root.chmod(0o700)
            def ref(name):
                p=root/name; p.write_bytes(b'fictional retained auth\n'); p.chmod(0o600)
                return {'path':str(p),'sha256':digest(p.read_bytes())}
            candidate={'reference':'ghcr.io/publicdata-stream/openlegal-server@sha256:'+'1'*64,
                'image_id':'sha256:'+'2'*64,'executable_sha256':'3'*64}
            p={'schema_version':1,'kind':'openlegal_initial_provider_maintenance_preprovision',
                'candidate':candidate,'operator_role':'provider_operator','operator_uid':10004,
                'authentication':{'kind':'protected_uds_peer_v1','system_user':'openlegal',
                    'map_name':'provider_peer','hba_file':ref('hba'),'ident_file':ref('ident'),
                    'local_rules_sha256':'5'*64,'ident_rules_sha256':'6'*64},
                'function_owners':{s:{'name':'provider_owner','body_sha256':'4'*64} for s in m.FUNCTIONS}}
            self.assertIs(m._provision(p,candidate),p)
            for mutate in (lambda v:v['authentication'].update(kind='tls'),
                lambda v:v['authentication'].update(password='not allowed'),
                lambda v:v.update(operator_uid=0),lambda v:v.update(shell='/bin/sh')):
                bad=copy.deepcopy(p); mutate(bad)
                with self.assertRaises(Refused):m._provision(bad,candidate)

    def test_explicit_local_trust_uses_first_matching_literal_rule_and_exact_order(self):
        rules=[{'rule_number':1,'type':'local','database':['openlegal'],'user_name':['other_role'],
            'auth_method':'peer','options':None,'error':None},
            {'rule_number':2,'type':'local','database':['all'],'user_name':['all'],
            'auth_method':'trust','options':None,'error':None}]
        maps=[]; value={'hba':rules,'ident':maps}
        auth={'kind':'protected_uds_trust_v1','local_rules_sha256':digest(canonical(rules)),
            'ident_rules_sha256':digest(canonical(maps))}
        m._auth_catalog(value,auth,'provider_operator')
        changed=copy.deepcopy(value); changed['hba'].reverse()
        with self.assertRaisesRegex(Refused,'auth_catalog_changed'):m._auth_catalog(changed,auth,'provider_operator')
        blocked=copy.deepcopy(value); blocked['hba'][0]['user_name']=['provider_operator']
        auth['local_rules_sha256']=digest(canonical(blocked['hba']))
        with self.assertRaisesRegex(Refused,'explicit_local_trust_required'):
            m._auth_catalog(blocked,auth,'provider_operator')
        tcp=copy.deepcopy(value); tcp['hba'][0]['type']='host'
        auth['local_rules_sha256']=digest(canonical(tcp['hba']))
        with self.assertRaisesRegex(Refused,'hba_local_only'):m._auth_catalog(tcp,auth,'provider_operator')

    def test_peer_profile_never_falls_back_to_preexisting_trust(self):
        rules=[{'rule_number':1,'type':'local','database':['openlegal'],'user_name':['provider_operator'],
            'auth_method':'trust','options':None,'error':None}]
        auth={'kind':'protected_uds_peer_v1','map_name':'operator_peer','system_user':'openlegal',
            'local_rules_sha256':digest(canonical(rules)),'ident_rules_sha256':digest(canonical([]))}
        with self.assertRaisesRegex(Refused,'exact_peer_hba'):
            m._auth_catalog({'hba':rules,'ident':[]},auth,'provider_operator')

    def test_named_operator_rejects_restricted_builtin_execute_even_without_membership(self):
        value={'role':{'rolname':'provider_operator','rolcanlogin':True,'rolinherit':True,
            **dict.fromkeys(('rolsuper','rolcreaterole','rolcreatedb','rolreplication','rolbypassrls'),False)},
            'memberships':[],'broad_privileges':False,'migration_read':True,'column_privileges':False,
            'other_definer_execute':False,'nonpublic_extra_execute':False,
            'schema_create':False,'database_create':False}
        m._operator_privileges(value,'provider_operator')
        # Models an actual has_function_privilege(...pg_read_binary_file...) true
        # observation; SQL/native grant discovery itself is a separate gate.
        changed=copy.deepcopy(value); changed['nonpublic_extra_execute']=True
        with self.assertRaisesRegex(Refused,'operator_broad_authority'):
            m._operator_privileges(changed,'provider_operator')
        changed=copy.deepcopy(value); changed['migration_read']=False
        with self.assertRaises(Refused):m._operator_privileges(changed,'provider_operator')


class AuditTests(unittest.TestCase):
    def test_full_sql_audit_and_apply_readback_correspond_and_preserve_charges(self):
        data=audit_fixture(); owner=data[4]['state']['ledger'][0]['owner']
        self.assertEqual(m._audit(*data),[owner])
        self.assertTrue(data[5]['state']['budget']['operator_suspended'])
        self.assertEqual(data[5]['state']['budget']['charged'],12)

    def test_readback_result_alone_is_never_claimed_full_audit(self):
        data=list(audit_fixture()); data[3]=data[2]
        with self.assertRaisesRegex(Refused,'full_audit'):m._audit(*data)

    def test_any_changed_charge_pacing_suspension_corpus_schema_or_history_refuses(self):
        mutations=[lambda d:d[5]['state']['budget'].update(charged=11),
            lambda d:d[5]['state']['budget'].update(next_request_at_ms=0),
            lambda d:d[5]['state']['budget'].update(operator_suspended=False),
            lambda d:d[5]['state']['corpus_control'].update(epoch=8),
            lambda d:d[5]['state'].update(schema=[]),
            lambda d:d[5]['state']['control'].update(legacy_identity=str(uuid.uuid4()))]
        for mutate in mutations:
            data=list(audit_fixture()); mutate(data)
            with self.subTest(mutation=mutate),self.assertRaises(Refused):m._audit(*data)

    def test_wrong_nested_uuid_database_actor_audit_before_or_cli_readback_refuses(self):
        mutations=[lambda d:d[3].update(operation_id=str(uuid.uuid4())),
            lambda d:d[3].update(database_actor='runtime'),
            lambda d:d[3]['before_state']['budget'].update(charged=0),
            lambda d:d[2].update(recovery_hold=False)]
        for mutate in mutations:
            data=list(audit_fixture()); mutate(data)
            with self.subTest(mutation=mutate),self.assertRaises(Refused):m._audit(*data)

    def test_new_uncertainty_must_be_exact_slot_identity_with_null_time(self):
        mutations=[lambda d:d[5]['uncertainty'][0].update(first_observed_at=int(time.time())),
            lambda d:d[5]['uncertainty'][0].update(identity=str(uuid.uuid4())),
            lambda d:d[5]['uncertainty'][0].update(slot=4),
            lambda d:d[5]['uncertainty'][0].update(kind='legacy')]
        for mutate in mutations:
            data=list(audit_fixture()); mutate(data)
            with self.assertRaises(Refused):m._audit(*data)

    def test_existing_uncertainty_timestamp_and_entire_rows_are_permanent(self):
        data=list(audit_fixture()); old={'identity':str(uuid.uuid4()),'kind':'legacy',
            'owner':None,'slot':None,'first_observed_at':None}
        data[4]['uncertainty']=[copy.deepcopy(old)]; data[5]['uncertainty'].append(copy.deepcopy(old))
        self.assertEqual(len(m._audit(*data)),1)
        data[5]['uncertainty'][-1]['first_observed_at']=123
        with self.assertRaisesRegex(Refused,'old_uncertainty_changed'):m._audit(*data)


class ReadonlyAuthenticationMetadata(unittest.TestCase):
    """Synthetic SQL output with real held /tmp directory FD correspondence."""
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/tmp',prefix='maintenance-auth-metadata-')
        self.root=Path(self.tmp.name); self.fd=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY)
        self.addCleanup(self.tmp.cleanup); self.addCleanup(os.close,self.fd)
        self.calls=[]; self.records=[]; self.closed=[]; self.primary_checks=[]
        self.value={'session_user':'postgres','current_user':'postgres','superuser':True,
            'transaction_read_only':'on','hba':[{'file_name':'/fictional/hba','type':'local'}],
            'ident':[{'file_name':'/fictional/ident'}],'hba_path':'/fictional/hba','ident_path':'/fictional/ident'}
        self.context=types.SimpleNamespace(primary='a'*64,socketfd=self.fd,b=types.SimpleNamespace(),
            op=object(),work={'initial_migration':{'synthetic':True}},seal=lambda:None,
            sql_uncertain=False,
            save=lambda label,value:self.records.append((label,value)))
        def sessions():self.closed.append(True); self.context.sql_uncertain=False
        self.context.sessions=sessions
        def command(args,**kw):
            self.calls.append((args,kw)); return canonical(self.value),{'phase':m.PHASE,'name':'synthetic-command.json','sha256':'1'*64}
        self.context.command=command
        def primary(*args):self.primary_checks.append(args); return os.dup(self.fd)
        item=patch.object(m.migration,'_primary',side_effect=primary); item.start(); self.addCleanup(item.stop)

    def test_dba_exception_is_fixed_uid70_uds_readonly_and_dual_raw_bound(self):
        result=m._auth_observation(self.context)
        self.assertEqual(set(result),{'hba','ident','hba_path','ident_path'})
        self.assertEqual(len(self.calls),1); args,kw=self.calls[0]
        self.assertEqual(args[:5],['exec','-i','-u','70','a'*64])
        self.assertEqual(args[5:7],['/usr/bin/env','-i'])
        self.assertEqual(args[args.index('-U')+1],'postgres'); self.assertEqual(args[args.index('-h')+1],'/var/run/postgresql')
        self.assertIn('-w',args); self.assertIn('-X',args); self.assertNotIn('PGPASSWORD',repr(args))
        self.assertIs(kw['stdin'],m.AUTH_SQL); self.assertEqual(kw['timeout'],3)
        sql=kw['stdin'].decode(); self.assertTrue(sql.startswith('BEGIN READ ONLY;'))
        self.assertTrue(sql.endswith('COMMIT;\n')); self.assertEqual(sql.count('BEGIN '),1)
        for limit in ("statement_timeout='2s'","lock_timeout='1s'","transaction_timeout='5s'"):
            self.assertIn('SET LOCAL '+limit,sql)
        self.assertIn("FROM pg_hba_file_rules WHERE type='local'",sql)
        self.assertIn('UNSUPPORTED_REDACTED',sql); self.assertNotIn('to_jsonb(h)',sql)
        self.assertIn("ELSE 'invalid_auth_rule'",sql); self.assertIn("ELSE 'invalid_ident_rule'",sql)
        self.assertEqual(len(self.primary_checks),2); self.assertEqual(self.closed,[True])
        label,record=self.records[0]; self.assertEqual(label,'auth-readonly-raw')
        self.assertEqual(m.decode(record['raw_utf8'].encode()),self.value)
        self.assertEqual(record['actual_command']['name'],'synthetic-command.json')

    def test_actual_identity_superuser_and_readonly_are_required_not_copied(self):
        for field,value in [('session_user','openlegal_migration'),('current_user','provider_operator'),
            ('superuser',False),('superuser',1),('transaction_read_only','off')]:
            with self.subTest(field=field,value=value):
                original=self.value[field]; self.value[field]=value
                with self.assertRaisesRegex(Refused,'auth_readonly_dba_identity'):
                    m._auth_observation(self.context)
                self.assertEqual(m.decode(self.records[-1][1]['raw_utf8'].encode())[field],value)
                self.assertTrue(self.context.sql_uncertain)
                self.value[field]=original

    def test_failed_auth_command_is_not_replayed_and_only_closure_may_clear_uncertainty(self):
        attempts=[]
        def failed(args,**kwargs):
            attempts.append((args,kwargs)); raise Refused('synthetic_auth_command_unknown')
        self.context.command=failed
        with self.assertRaisesRegex(Refused,'auth_command_unknown'):m._auth_observation(self.context)
        self.assertTrue(self.context.sql_uncertain)
        m._retain_sql_closed(self.context)
        self.assertFalse(self.context.sql_uncertain); self.assertEqual(self.closed,[True])
        self.assertEqual(len(attempts),1); self.assertIs(attempts[0][1]['stdin'],m.AUTH_SQL)

    def test_included_auth_files_and_rebound_native_socket_refuse(self):
        self.value['hba'][0]['file_name']='/unapproved/include'
        with self.assertRaisesRegex(Refused,'auth_unapproved_include'):m._auth_observation(self.context)
        self.value['hba'][0]['file_name']='/fictional/hba'
        other=self.root/'other'; other.mkdir()
        with patch.object(m.migration,'_primary',side_effect=lambda *a:os.open(other,os.O_RDONLY|os.O_DIRECTORY)):
            with self.assertRaisesRegex(Refused,'auth_socket_changed'):m._auth_observation(self.context)
        self.assertEqual(len(self.calls),1)


def phase_fixture(root):
    """Real files, synthetic same-filesystem dual journal, no constructor claim."""
    operation=str(uuid.uuid4()); j=m.journals.DualJournal.__new__(m.journals.DualJournal)
    j.operation=operation; j.binding={'operation_id':operation}; j.base=[]; j.paths=[]; j.phase_paths=[]
    j.bytes=0; j.count=0; j.artifacts={}; j.history=[]; j.phase_names={m.PHASE}
    for name in ('synthetic-a','synthetic-b'):
        base=root/name; base.mkdir(mode=0o700); j.base.append((base,m.journals.directory(base)))
        op=base/operation; op.mkdir(mode=0o700); j.paths.append((op,m.journals.directory(op)))
        phase=op/m.PHASE; phase.mkdir(mode=0o700); j.phase_paths.append((phase,m.journals.directory(phase)))
    j.save('intent.json',{'synthetic':True,'operation_id':operation})
    b=m.driver.Backend(); b._driver_deadline=time.monotonic()+10
    obj=types.SimpleNamespace(b=b,j=j,cfg={'operation_id':operation},serial=0,
        work={'host':{'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip()}},
        helpers=[],socketfd=None,rootfd=None,cancelled=False,sql_uncertain=False)
    def seal():
        m.driver._check_end(b); j.check()
    def save(label,value):
        seal(); obj.serial+=1; name=m.PHASE+'-%04d-'%obj.serial+label+'.json'
        return {'phase':m.PHASE,'name':name,'sha256':j.save(name,value)}
    obj.seal=seal; obj.save=save
    def sessions():
        save('synthetic-global-sql-closure',{'sessions':[]}); obj.sql_uncertain=False
    obj.sessions=sessions
    return obj


class RealForkTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/tmp',prefix='maintenance-harmless-')
        self.root=Path(self.tmp.name); self.root.chmod(0o700)
    def tearDown(self):self.tmp.cleanup()

    def _capture_owned_test_fork(self,captured):
        real=os.fork;rootpid=os.getpid()
        def fork():
            pid=real()
            if pid and os.getpid()==rootpid:
                # The TEST owns the raw PID before any injected production
                # failure, including a failure allocating its own pidfd.
                captured['pid']=pid
                captured['pidfd']=os.pidfd_open(pid)
            return pid
        return fork

    def _reap_test_child(self,captured):
        pid=captured.get('pid')
        if pid is None:return
        end=time.monotonic()+3
        while time.monotonic()<end:
            try:found,status=os.waitpid(pid,os.WNOHANG)
            except ChildProcessError:return  # Production already reaped it.
            if found==pid:
                captured['outer_wait_status']=status
                return
            time.sleep(.01)
        self.fail('test-owned harmless child did not close after its GO gate opened')

    def _parent_postfork_fault(self,mode):
        root=self.root/mode;root.mkdir(mode=0o700);obj=phase_fixture(root)
        rootpid=os.getpid();captured={};read,write=os.pipe()
        oldsub=m.processes._subreaper()
        previous={number:signal.getsignal(number) for number in
                  (signal.SIGCHLD,signal.SIGINT,signal.SIGTERM,signal.SIGALRM)}
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL),(0.,0.))
        real_pair=m.socket.socketpair;real_identity=m.processes._identity
        def gated_child(*_):
            try:
                # This test-only producer has no actors and exits only after
                # its external test owner's gate, even if production sends TERM.
                signal.signal(signal.SIGTERM,lambda *_:None)
                os.read(read,1)
            finally:os._exit(1)
        class ParentCloseError:
            def __init__(self,sock):self.sock=sock;self.raised=False
            def close(self):
                self.sock.close()
                if os.getpid()==rootpid and not self.raised:
                    self.raised=True;raise OSError(errno.EIO,'synthetic_parent_postfork_close')
            def __getattr__(self,name):return getattr(self.sock,name)
        def sockets():
            a,b=real_pair();return a,ParentCloseError(b)
        def identity(pid):
            if os.getpid()==rootpid and pid!=rootpid:
                raise OSError(errno.EIO,'synthetic_persistent_identity')
            return real_identity(pid)
        returned_with_live_child=False
        try:
            # An outer gate release bounds the regression even if production
            # correctly waits with no /proc identity or Ownership constructor.
            signal.signal(signal.SIGCHLD,signal.SIG_DFL)
            signal.signal(signal.SIGALRM,lambda *_:os.write(write,b'g'))
            signal.setitimer(signal.ITIMER_REAL,.15)
            with ExitStack() as stack:
                stack.enter_context(patch.object(m.os,'fork',side_effect=self._capture_owned_test_fork(captured)))
                stack.enter_context(patch.object(m,'_child',side_effect=gated_child))
                if mode=='parent-close':
                    stack.enter_context(patch.object(m.socket,'socketpair',side_effect=sockets))
                elif mode=='leader-init':
                    stack.enter_context(patch.object(m.driver,'_Leader',side_effect=OSError(errno.EMFILE,'synthetic_leader_init')))
                elif mode=='ownership-init':
                    stack.enter_context(patch.object(m.processes,'_Ownership',side_effect=OSError(errno.EMFILE,'synthetic_ownership_init')))
                else:stack.enter_context(patch.object(m.processes,'_identity',side_effect=identity))
                with self.assertRaisesRegex(OSError,'synthetic_'):m._fork(obj)
            if captured:
                returned_with_live_child=not bool(m.select.select([captured['pidfd']],[],[],0)[0])
        finally:
            if os.getpid()!=rootpid:os._exit(91)
            signal.setitimer(signal.ITIMER_REAL,0)
            # Restore ownership state only AFTER the outer owner's real wait.
            signal.signal(signal.SIGCHLD,signal.SIG_DFL)
            try:
                os.write(write,b'g');self._reap_test_child(captured)
            finally:
                for fd in (read,write,captured.get('pidfd')):
                    if fd is not None:os.close(fd)
                for number,handler in previous.items():signal.signal(number,handler)
                m.processes._subreaper(oldsub)
        self.assertFalse(returned_with_live_child,'production returned before test-owned child actually closed')
        self.assertEqual(m.processes._children(rootpid),set())
        self.assertFalse(m._SERIAL.locked())

    def test_parent_postfork_failures_keep_raw_wait_owner_through_actual_closure(self):
        for mode in ('parent-close','leader-init','ownership-init','identity'):
            with self.subTest(mode=mode):self._parent_postfork_fault(mode)

    def _child_exit_fault(self,mode):
        root=self.root/mode;root.mkdir(mode=0o700);obj=phase_fixture(root)
        rootpid=os.getpid();captured={};parentfds=[]
        real_pair=m.socket.socketpair;real_close=os.close;real_pidfd=os.pidfd_open
        oldsub=m.processes._subreaper()
        previous={number:signal.getsignal(number) for number in
                  (signal.SIGCHLD,signal.SIGINT,signal.SIGTERM)}
        class ChildCloseError:
            def __init__(self,sock):self.sock=sock;self.raised=False
            def close(self):
                self.sock.close()
                if os.getpid()!=rootpid and not self.raised:
                    self.raised=True;raise OSError(errno.EIO,'synthetic_child_prologue_close')
            def __getattr__(self,name):return getattr(self.sock,name)
        def sockets():
            a,b=real_pair();return ChildCloseError(a),b
        def pidfd(pid,*args):
            fd=real_pidfd(pid,*args)
            if pid==rootpid:parentfds.append(fd)
            return fd
        def close(fd):
            real_close(fd)
            if os.getpid()!=rootpid and fd in parentfds:
                raise OSError(errno.EIO,'synthetic_child_cleanup_close')
        try:
            signal.signal(signal.SIGCHLD,signal.SIG_DFL)
            with ExitStack() as stack:
                stack.enter_context(patch.object(m.os,'fork',side_effect=self._capture_owned_test_fork(captured)))
                stack.enter_context(patch.object(m,'_produce',side_effect=lambda _:None))
                if mode=='prologue-close':
                    stack.enter_context(patch.object(m.socket,'socketpair',side_effect=sockets))
                else:
                    stack.enter_context(patch.object(m.os,'pidfd_open',side_effect=pidfd))
                    stack.enter_context(patch.object(m.os,'close',side_effect=close))
                with self.assertRaises((Refused,OSError,ValueError)):m._fork(obj)
        finally:
            # A regressed child must NEVER duplicate this unittest runner. Its
            # externally observed 91 demonstrates escape into ROOT's caller.
            if os.getpid()!=rootpid:os._exit(91)
            signal.signal(signal.SIGCHLD,signal.SIG_DFL)
            try:self._reap_test_child(captured)
            finally:
                if captured.get('pidfd') is not None:real_close(captured['pidfd'])
                for number,handler in previous.items():signal.signal(number,handler)
                m.processes._subreaper(oldsub)
        row=obj.b._initial_provider_maintenance_last_exit
        self.assertEqual(row['pid'],captured['pid'])
        self.assertEqual(row['actual_exit_code'],1,'fork child escaped into the ROOT caller')
        with self.assertRaises(ChildProcessError):os.waitpid(row['pid'],os.WNOHANG)
        self.assertFalse(m._SERIAL.locked())

    def test_child_prologue_and_cleanup_errors_always_exit_in_the_fork_child(self):
        for mode in ('prologue-close','cleanup-close'):
            with self.subTest(mode=mode):self._child_exit_fault(mode)

    def test_parent_waitpid_zero_and_both_raw_phase_copies_are_real(self):
        obj=phase_fixture(self.root); parent=os.getpid()
        def harmless(child):
            child.save('actual-fork',{'pid':os.getpid(),'parent':os.getppid(),'synthetic':True})
        with patch.object(m,'_produce',side_effect=harmless):row=m._fork(obj)
        self.assertEqual(row,obj.b._initial_provider_maintenance_last_exit)
        self.assertEqual(type(row['actual_exit_code']),int); self.assertEqual(row['actual_exit_code'],0)
        self.assertNotEqual(row['pid'],parent); self.assertEqual(row['remaining_children'],[])
        with self.assertRaises(ChildProcessError):os.waitpid(row['pid'],os.WNOHANG)
        self.assertIsNone(m.processes._identity(row['pid']))
        artifacts=[v for name,v in obj.j.artifacts.items() if name.endswith('-actual-fork.json')]
        self.assertEqual(len(artifacts),1); value=m.decode(artifacts[0]['raw'])
        self.assertEqual(value['pid'],row['pid']); self.assertEqual(value['parent'],parent)
        obj.j.check()

    def test_child_failure_cannot_be_replaced_by_historical_json_zero(self):
        obj=phase_fixture(self.root)
        obj.b._initial_provider_maintenance_last_exit={'actual_exit_code':0,'pid':111,'historical':True}
        def failed(child):
            child.save('claimed-zero',{'actual_exit_code':0,'invented':True})
            raise Refused('synthetic_harmless_failure')
        with patch.object(m,'_produce',side_effect=failed):
            with self.assertRaisesRegex(Refused,'producer_not_successfully_closed'):m._fork(obj)
        actual=obj.b._initial_provider_maintenance_last_exit
        self.assertEqual(actual['actual_exit_code'],1); self.assertNotEqual(actual['pid'],111)
        self.assertIsNone(m.processes._identity(actual['pid']))

    def test_partial_raw_phase_is_rejected_after_real_child_exit(self):
        obj=phase_fixture(self.root)
        def partial(child):
            path=child.j.phase_paths[0][0]/(m.PHASE+'-unacknowledged.json')
            m.journals.new_file(path,canonical({'partial':True}))
            raise Refused('synthetic_partial')
        with patch.object(m,'_produce',side_effect=partial):
            with self.assertRaisesRegex(Refused,'partial_or_extra_artifact'):m._fork(obj)
        self.assertEqual(obj.b._initial_provider_maintenance_last_exit['actual_exit_code'],1)

    def test_modified_dual_raw_ack_fails_reconciliation(self):
        obj=phase_fixture(self.root); index=m._index(obj)
        path=obj.j.phase_paths[1][0]/'intent.json'; path.write_bytes(canonical({'tampered':True}))
        with self.assertRaises(Refused):m._reconcile(obj,index)

    def test_parent_discovery_error_still_waits_for_actual_producer_closure(self):
        obj=phase_fixture(self.root); parent=os.getpid(); original=m.processes._Ownership.capture; calls=[]
        def raced_capture(owned):
            if os.getpid()==parent:
                calls.append(1)
                if len(calls)==1:raise OSError(errno.EIO,'synthetic ownership discovery error')
            return original(owned)
        def harmless(child):time.sleep(.1)
        with patch.object(m,'_produce',side_effect=harmless),\
             patch.object(m.processes._Ownership,'capture',side_effect=None,autospec=True) as capture:
            capture.side_effect=raced_capture
            with self.assertRaisesRegex(OSError,'synthetic ownership discovery error'):m._fork(obj)
        actual=obj.b._initial_provider_maintenance_last_exit
        self.assertEqual(type(actual['actual_exit_code']),int)
        self.assertEqual(actual['remaining_children'],[]); self.assertIsNone(m.processes._identity(actual['pid']))
        with self.assertRaises(ChildProcessError):os.waitpid(actual['pid'],os.WNOHANG)
        self.assertGreater(len(calls),1)

    def test_failed_sql_child_exits_only_after_fresh_global_closure_and_failed_probe_retry(self):
        obj=phase_fixture(self.root)
        def failed(child):
            child.sql_uncertain=True; child.probes=0
            def sessions():
                child.probes+=1
                child.save('synthetic-closure-attempt',{'attempt':child.probes,'closure_only':True})
                if child.probes==1:raise Refused('synthetic_closure_observer_failure')
                child.save('synthetic-global-sql-closed',{'sessions':[]}); child.sql_uncertain=False
            child.sessions=sessions
            raise Refused('synthetic_transmitted_sql_unknown')
        with patch.object(m,'_produce',side_effect=failed):
            with self.assertRaisesRegex(Refused,'producer_not_successfully_closed'):m._fork(obj)
        actual=obj.b._initial_provider_maintenance_last_exit
        self.assertEqual(actual['actual_exit_code'],1); self.assertIsNone(m.processes._identity(actual['pid']))
        attempts=[v for k,v in obj.j.artifacts.items() if k.endswith('-synthetic-closure-attempt.json')]
        closure=[v for k,v in obj.j.artifacts.items() if k.endswith('-synthetic-global-sql-closed.json')]
        self.assertEqual(len(attempts),2); self.assertEqual(len(closure),1)
        self.assertEqual(m.decode(closure[0]['raw']),{'sessions':[]}); obj.j.check()

    def test_expired_sql_uncertainty_retains_owned_child_without_new_probe(self):
        read,write=os.pipe(); pid=os.fork()
        if pid==0:
            os.close(read); signal.signal(signal.SIGTERM,signal.SIG_DFL)
            context=types.SimpleNamespace(sql_uncertain=True,
                seal=lambda:(_ for _ in ()).throw(Refused('synthetic_original_deadline_expired')),
                sessions=lambda:os.write(write,b'UNAUTHORIZED_PROBE'))
            os.write(write,b'r'); m._retain_sql_closed(context); os._exit(3)
        os.close(write); fd=os.pidfd_open(pid)
        try:
            self.assertEqual(os.read(read,1),b'r'); time.sleep(.2)
            self.assertFalse(m.select.select([fd],[],[],0)[0])
            self.assertFalse(m.select.select([read],[],[],0)[0])
            signal.pidfd_send_signal(fd,signal.SIGTERM,None,0)
            found,status=os.waitpid(pid,0); self.assertEqual(found,pid)
            self.assertEqual(os.waitstatus_to_exitcode(status),-signal.SIGTERM)
        finally:
            try:signal.pidfd_send_signal(fd,signal.SIGTERM,None,0)
            except ProcessLookupError:pass
            try:os.waitpid(pid,0)
            except ChildProcessError:pass
            os.close(fd); os.close(read)

    def test_unknown_native_actor_cannot_be_proved_closed_by_client_zero(self):
        helper={'known':False,'pidfd':None,'groupfd':None,'closed':False}
        with self.assertRaisesRegex(Refused,'ownership_unknown'):m._helper_closed(helper)

    def test_cleanup_retains_real_children_through_identity_signal_and_descriptor_errors(self):
        # Real pidfds/TERM/waitpid/absence; ONLY cgroup membership is synthetic.
        # No Docker/runtime/database actor or external signal target exists.
        helpers=[]; readyread,readywrite=os.pipe(); boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        real_signal=signal.pidfd_send_signal; real_close=os.close; outcomes={}
        for number in range(2):
            pid=os.fork()
            if pid==0:
                os.close(readyread); signal.signal(signal.SIGTERM,lambda *_:os._exit(0))
                os.write(readywrite,b'r')
                while True:signal.pause()
            group=os.open(self.root,os.O_RDONLY|os.O_DIRECTORY); st=os.fstat(group)
            helpers.append({'started':True,'known':True,'closed':False,'pidfd':os.pidfd_open(pid),
                'groupfd':group,'group_identity':(st.st_dev,st.st_ino),'groups':['0::/synthetic-'+str(pid)],
                'process':{'pid':pid,'start_ticks':m.processes._identity(pid),'boot_id':boot}})
        os.close(readywrite); self.assertEqual(os.read(readyread,1)+os.read(readyread,1),b'rr'); os.close(readyread)
        def members(groups):
            pid=int(groups[0].rsplit('-',1)[1])
            if pid not in outcomes:
                found,status=os.waitpid(pid,os.WNOHANG)
                if found:outcomes[pid]=os.waitstatus_to_exitcode(status)
            return [] if pid in outcomes else [pid]
        identity=m.processes._identity; reads=[]; signals=[]; closes=[]
        def raced_identity(pid):
            reads.append(pid)
            if len(reads)==1:raise OSError(errno.EIO,'synthetic procfs read failure')
            return identity(pid)
        def uncertain_signal(fd,number,*args):
            signals.append(fd); real_signal(fd,number,*args)
            if len(signals)==1:raise ProcessLookupError(errno.ESRCH,'synthetic raced signal ACK')
        def failed_close(fd):
            closes.append(fd)
            self.assertEqual(len(outcomes),2)  # ALL actors close before any FD.
            real_close(fd)
            if fd==helpers[0]['groupfd']:raise OSError(errno.EIO,'synthetic close ACK failure')
        try:
            with patch.object(m.linux,'members',side_effect=members),\
                 patch.object(m.processes,'_identity',side_effect=raced_identity),\
                 patch.object(m.signal,'pidfd_send_signal',side_effect=uncertain_signal),\
                 patch.object(m.os,'close',side_effect=failed_close):
                m._retain_until_helpers_closed(types.SimpleNamespace(helpers=helpers))
            self.assertEqual(list(outcomes.values()),[0,0]); self.assertTrue(all(h['closed'] for h in helpers))
            self.assertEqual(signals,[h['pidfd'] for h in helpers])
            self.assertEqual(len(closes),4); self.assertGreaterEqual(len(reads),3)
        finally:
            for h in helpers:
                pid=h['process']['pid']
                if pid not in outcomes:
                    if identity(pid)==h['process']['start_ticks']:
                        try:real_signal(h['pidfd'],signal.SIGTERM,None,0)
                        except ProcessLookupError:pass
                    try:os.waitpid(pid,0)
                    except ChildProcessError:pass
                for fd in (h['pidfd'],h['groupfd']):
                    try:real_close(fd)
                    except OSError:pass


if __name__=='__main__':unittest.main()
