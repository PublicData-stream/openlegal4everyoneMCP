"""Synthetic fixed Docker/SQL ports and actual harmless /tmp gate process.

No Docker, PG, ZFS, migration, credentials, provider or network traffic occurs.
"""
import copy
import ctypes.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import types
import unittest
import uuid
from unittest.mock import patch

BASE=Path(__file__).resolve().parents[1];sys.path.insert(0,str(BASE))
import initial_migration as m
import linux_backend as linux
from interface import Refused
from inventory import canonical, digest


class Journal:
    def __init__(self):self.rows={}
    def save(self,name,value):
        if name in self.rows:raise FileExistsError(name)
        self.rows[name]=canonical(value);return digest(self.rows[name])
    def output_hashes(self):return {k:digest(v) for k,v in self.rows.items()}
    def read_phase_bytes(self,phase,name,sha):
        raw=self.rows[name];assert digest(raw)==sha;return raw


class PureTests(unittest.TestCase):
    def test_profile_is_migration_only_explicit_local_no_seed_or_credentials(self):
        import tomllib
        c=tomllib.loads(m.PROFILE.decode());p=c['cache']['postgres']
        self.assertEqual((p['replication_mode'],p['tls_mode']),('local','plaintext'))
        self.assertNotIn('replication_epoch',p);self.assertNotIn('ca_file',p)
        self.assertEqual(len({p['url_env'],p['migration_url_env'],p['provider_admin_url_env']}),3)
        self.assertIn('openlegal_migration@localhost/openlegal?',m.MIGRATION_ENV)
        self.assertNotIn('password',m.MIGRATION_ENV.lower());self.assertIn('--migrate',m.GATE)
        self.assertNotIn('--storage-binding',m.GATE);self.assertNotIn('pg_cancel_backend',m.SESSIONS_SQL)

    def test_global_sql_closure_counts_every_other_same_database_session(self):
        self.assertIn('datname=current_database() AND pid<>pg_backend_pid()',m.SESSIONS_SQL)
        self.assertNotIn('application_name=',m.SESSIONS_SQL)
        self.assertNotIn('usename=',m.SESSIONS_SQL)


@unittest.skipUnless(os.geteuid()==0,'root ownership gate')
class PhaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='initial-migration-test-',dir='/tmp');self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700);self.calls=[];self.opid=str(uuid.uuid4())
        self.primary=uuid.uuid4().hex*2;self.candidate=uuid.uuid4().hex*2;self.boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.guard=Path('/tmp/openlegal-initial-migration-primary-'+self.primary+'.lock')
        self.controlroot=Path('/tmp/openlegal-initial-migration-'+self.opid)
        self.addCleanup(lambda:self.guard.unlink(missing_ok=True))
        self.addCleanup(lambda:__import__('shutil').rmtree(self.controlroot,ignore_errors=True))
        self.sock=self.root/'socket';self.sock.mkdir(mode=0o700)
        self.socket=socket.socket(socket.AF_UNIX);self.socket.bind(str(self.sock/'.s.PGSQL.5432'));self.addCleanup(self.socket.close)
        os.chown(self.sock/'.s.PGSQL.5432',70,70)
        self.bin=self.root/'bin';self.bin.mkdir(mode=0o700);(self.bin/'openlegal-server').write_bytes(b'synthetic compiled bytes')
        self.cg=self.root/'cgroup';self.cg.mkdir(mode=0o700);(self.cg/'cgroup.procs').write_text('')
        self.native=None;self.mode=None
        self.rows=[{'version':i,'checksum':format(i,'096x')} for i in range(1,34)]
        self.journal=Journal();self.state={'tables':{'openlegal.corpus_capture':{'rows':2,'sha256':'a'*64}},
            'references':{'rows':0,'sha256':digest(b'')},'migration_history':{'rows':27,'sha256':'b'*64}}
        self.sessions={'database':'openlegal','session_user':'openlegal_migration','current_user':'openlegal_migration',
            'postmaster_started_at':'2026-11-02T00:00:00+00',
            'role_flags':dict.fromkeys(('superuser','create_role','create_db','replication','bypass_rls'),False),'sessions':[]}
        datasets=[{'name':name,'guid':str(i+100)} for i,name in enumerate(sorted(m.SNAPSHOT_DATASETS))]
        s=self.sock.stat();node=(self.sock/'.s.PGSQL.5432').stat()
        c={'primary_process':{'pid':os.getpid(),'start_ticks':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],'boot_id':self.boot},
           'socket_directory':{'dev':s.st_dev,'ino':s.st_ino},'socket':{'dev':node.st_dev,'ino':node.st_ino},
           'candidate':{'reference':'ghcr.io/publicdata-stream/openlegal-server@sha256:'+'b'*64,
                        'image_id':'sha256:'+'c'*64,'executable_sha256':digest((self.bin/'openlegal-server').read_bytes())},
           'compiled_manifest':{'path':str(self.root/'manifest.json'),'sha256':'x'},
           'producer_source':{'path':str(self.root/'producer.py'),'sha256':'d'*64},'deadline_secs':30,'prerequisites':{},
           'legacy_binding':{'inventory_sha256':'1'*64,'source_graph_sha256':'2'*64,'authority_sha256':'3'*64}}
        self.c=c
        manifest={'kind':'openlegal_compiled_migration_manifest','candidate':c['candidate'],
            'native_architecture':{'x86_64':'amd64','aarch64':'arm64'}.get(m.platform.machine()),'migrations':self.rows}
        raw=canonical(manifest);Path(c['compiled_manifest']['path']).write_bytes(raw);Path(c['compiled_manifest']['path']).chmod(0o600)
        c['compiled_manifest']['sha256']=digest(raw)
        self.op=types.SimpleNamespace(operation_id=self.opid,action='strict_prepare',phase=m.PHASE,
            authority={'initial_migration_permitted':True},inventory_sha256='1'*64,source_graph_sha256='2'*64,
            authority_sha256='3'*64,inventory={'initial_migration':c,'host':{'boot_id':self.boot},'original_datasets':datasets,
                'linux':{'namespace':'synthetic','workloads':[],'jobs':[], 'binaries':{'zfs':{'sha256':'e'*64}},
                         'postgres':{'primary':{'container_id':self.primary}}}},
            journal=self.journal,seal=lambda:None)
        self.closure={'cri_containers':[],'cgroups':[{'path':str(self.cg),'dev':1,'ino':2,'pids':[]}]}
        self.drain={'resources':[],'closure':self.closure}
        snapshots={'snapshots':[{'dataset':r['name'],'dataset_guid':r['guid'],'snapshot':r['name']+'@same-operation',
                                'snapshot_guid':str(i+1000)} for i,r in enumerate(datasets)]}
        snapshots['actual_command']={'binary':'zfs','binary_sha256':'e'*64,
            'arguments_sha256':digest(canonical(['snapshot',*[r['snapshot'] for r in snapshots['snapshots']]])),
            'actual_exit_code':0,'started':True,'owned_processes_closed':True,'expired':False,'interrupted':False,
            'unexpected_descendants':False,'output_limit_exceeded':False}
        self.snapshots=snapshots
        for key,payload in [('drain',self.drain),('snapshot',snapshots),('preservation',self.state)]:
            value={'schema_version':1,'kind':'openlegal_initial_'+key+'_proof','operation_id':self.opid,
                'inventory_sha256':'1'*64,'source_graph_sha256':'2'*64,'authority_sha256':'3'*64,
                'producer_source_sha256':'d'*64,'producer_exit':{'pid':9999999,'start_ticks':'1','boot_id':self.boot,
                    'actual_exit_code':0,'remaining_children':[]},'payload':copy.deepcopy(payload)}
            name=key+'.json';pin=self.journal.save(name,value);c['prerequisites'][key]={'phase':'legacy-'+key,'name':name,'sha256':pin}
        test=self
        class Backend(linux.LinuxBackend):
            def _artifact(inner,op,name,value):op.journal.save(op.phase+'-'+name+'.json',value)
            def _inspect(inner,op,which):return {'state':{'Running':True,'Pid':os.getpid()}}
            def _physical_writers_closed(inner,op):return copy.deepcopy(test.closure)
            def _container_directory(inner,pid,path):return os.open(test.bin if path=='/usr/local/bin' else test.sock,os.O_RDONLY|os.O_DIRECTORY)
            def _run(inner,op,binary,args,**kwargs):return test.command(binary,args,kwargs)
        self.backend=Backend()
        self.addCleanup(self.close)
        self.configpatch=patch.object(m,'_config',lambda op:c);self.configpatch.start();self.addCleanup(self.configpatch.stop)
        self.procpatch=patch.object(m.linux,'process_cgroup',return_value=['0::/synthetic/'+self.candidate]);self.procpatch.start();self.addCleanup(self.procpatch.stop)
        self.members=patch.object(m.linux,'members',side_effect=lambda g:[]);self.members.start();self.addCleanup(self.members.stop)
        original=m.os.open
        def opened(path,*args,**kwargs):
            if str(path)=='/sys/fs/cgroup/synthetic/'+self.candidate:path=self.cg
            return original(path,*args,**kwargs)
        self.openpatch=patch.object(m.os,'open',side_effect=opened);self.openpatch.start();self.addCleanup(self.openpatch.stop)

    def close(self):
        if self.native is not None:
            if self.native.poll() is None:self.native.terminate()
            self.native.wait(timeout=3)
        for name in ('_initial_migration_guard_fd','_initial_migration_cgroup_fd'):
            if hasattr(self.backend,name):os.close(getattr(self.backend,name));delattr(self.backend,name)

    def command(self,binary,args,kw):
        self.calls.append((binary,args,kw))
        if binary=='zfs':
            for i,row in enumerate(self.op.inventory['original_datasets']):
                if args[-1]==row['name']:return (row['guid']+'\n').encode()
                if args[-1]==row['name']+'@same-operation':return (str(i+1000)+'\n').encode()
        if args[0]=='exec':
            sql=kw['stdin'].decode()
            if m.SESSIONS_SQL in sql:
                value=copy.deepcopy(self.sessions)
                if self.mode=='sessions':value['sessions']=[{'pid':1,'backend_start':'synthetic','user':'other','database':'openlegal'}]
                return canonical(value)
            if m.MIGRATIONS_SQL in sql:return canonical([dict(r,success=True) for r in self.rows[:33 if self.native else 27]])
            if m.OLD_MIGRATIONS_SQL in sql:return canonical(self.state['migration_history'])
            if m.TABLES_SQL in sql:return canonical(sorted(self.state['tables']))
            if m.REFERENCES_SQL in sql:return canonical(self.state['references'])
            return canonical(self.state['tables']['openlegal.corpus_capture'])
        if args[:2]==['image','inspect']:
            return canonical({'Id':self.c['candidate']['image_id'],'Os':'linux',
                'Architecture':{'x86_64':'amd64','aarch64':'arm64'}.get(m.platform.machine()),'Volumes':None})
        if args[0]=='create':return (self.candidate+'\n').encode()
        if args[0]=='start':
            control=self.controlroot/'control'
            code='import pathlib,time; p=pathlib.Path('+repr(str(control/'go'))+');\nwhile not p.exists(): time.sleep(.01)\n'
            self.native=subprocess.Popen([sys.executable,'-I','-B','-c',code],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            return (self.candidate+'\n').encode()
        if args[0]=='inspect':
            if 'ExitCode' in args[2]:return canonical({'Id':self.candidate,'Image':self.c['candidate']['image_id'],'Pid':0,'Running':False,'ExitCode':0})
            return canonical({'Id':self.candidate,'Image':self.c['candidate']['image_id'],'Pid':self.native.pid,'Running':True,
                'User':'10004:10004','Entrypoint':['/bin/sh'],'Cmd':['-ec',m.GATE],'Readonly':True,'Network':'none',
                'Mounts':[{'Type':'bind','Source':'/proc/'+str(os.getpid())+'/root/var/run/postgresql','Destination':'/var/run/postgresql','RW':False},
                    {'Type':'bind','Source':str(self.controlroot/'control'),'Destination':'/initial-control','RW':False}]})
        if args[0]=='wait':
            if self.mode=='unknown':raise RuntimeError('synthetic native ACK unknown')
            return (str(self.native.wait(timeout=3))+'\n').encode()
        raise AssertionError((binary,args))

    def test_fixed_native_phase_requires_real_gate_process_exit_and_records_no_ready(self):
        result=m.run(self.backend,self.op)
        self.assertEqual(result['migration_native_exit_code'],0);self.assertEqual(result['schema_version'],33)
        self.assertFalse(result['ready']);self.assertFalse(result['seed_created']);self.assertTrue(self.guard.exists())
        intent=self.journal.rows[m.PHASE+'-migration-dispatch-intent.json'];self.assertIn(b'--migrate',intent)
        self.assertFalse(Path('/proc/'+str(self.native.pid)).exists())
        create=next(args for binary,args,kw in self.calls if args[0]=='create')
        self.assertIn('--network=none',create);self.assertIn('--read-only',create)
        self.assertEqual(create.count(m.MIGRATION_ENV),1);self.assertFalse(any(args[0] in ('rm','stop','kill') for _,args,_ in self.calls))

    def test_any_other_same_database_session_refuses_before_candidate_creation(self):
        self.mode='sessions'
        with self.assertRaisesRegex(Refused,'global_sql_not_closed'):m.run(self.backend,self.op)
        self.assertFalse(any(args[0]=='create' for _,args,_ in self.calls));self.assertTrue(self.guard.exists())

    def test_unknown_native_ack_preserves_guard_payload_intent_and_does_not_cancel_sql(self):
        self.mode='unknown'
        with self.assertRaisesRegex(RuntimeError,'ACK unknown'):m.run(self.backend,self.op)
        self.assertTrue(self.guard.exists());self.assertFalse(any(args[0] in ('rm','stop','kill') for _,args,_ in self.calls))
        value=json.loads(self.journal.rows[m.PHASE+'-migration-failed.json'])
        self.assertTrue(value['payload_may_dispatched']);self.assertFalse(value['selective_sql_cancellation'])
        self.assertTrue(hasattr(self.backend,'_initial_migration_guard_fd'))

    def test_unknown_prior_exit_or_wrong_raw_hash_refuses_before_any_effect(self):
        ref=self.c['prerequisites']['drain'];value=json.loads(self.journal.rows[ref['name']]);value['producer_exit']['actual_exit_code']=None
        self.journal.rows[ref['name']]=canonical(value);ref['sha256']=digest(self.journal.rows[ref['name']])
        with self.assertRaisesRegex(Refused,'prior_actual_exit'):m.run(self.backend,self.op)
        self.assertEqual(self.calls,[]);self.assertFalse(self.guard.exists())

    def test_wrong_compiled_manifest33_or_native_executable_never_releases_payload(self):
        self.c['candidate']['executable_sha256']='f'*64
        # Source/image-bound manifest must change with the proposed executable;
        # the actual opened candidate bytes then independently reject it.
        path=Path(self.c['compiled_manifest']['path']);value=json.loads(path.read_bytes());value['candidate']=self.c['candidate']
        path.write_bytes(canonical(value));path.chmod(0o600);self.c['compiled_manifest']['sha256']=digest(path.read_bytes())
        with self.assertRaisesRegex(Refused,'compiled_bytes'):m.run(self.backend,self.op)
        self.assertFalse((self.controlroot/'control/go').exists())

    def test_socket_inode_and_primary_start_tick_are_actual_not_boolean(self):
        self.c['socket']['ino']^=1
        with self.assertRaisesRegex(Refused,'socket_changed'):m._primary(self.backend,self.op,self.c)
        self.c['socket']['ino']^=1;self.c['primary_process']['start_ticks']='0'
        with self.assertRaisesRegex(Refused,'primary_reused'):m._primary(self.backend,self.op,self.c)

    def test_old_reference_or_row_mismatch_refuses_before_candidate_creation(self):
        self.state['tables']['openlegal.corpus_capture']['rows']=3
        with self.assertRaisesRegex(Refused,'old_proof_not_current'):m.run(self.backend,self.op)
        self.assertFalse(any(args[0]=='create' for _,args,_ in self.calls))

    def test_completed_guard_prevents_fresh_uuid_replay(self):
        m.run(self.backend,self.op)
        self.op.operation_id=str(uuid.uuid4())
        with patch.object(m,'_prior',side_effect=lambda op,c,key:{'drain':self.drain,'snapshot':self.snapshots,
                'preservation':self.state}[key]):
            with self.assertRaises(FileExistsError):m.run(self.backend,self.op)

    def test_atomic_snapshot_unknown_exit_or_wrong_binary_refuses_before_reads(self):
        ref=self.c['prerequisites']['snapshot'];value=json.loads(self.journal.rows[ref['name']])
        value['payload']['actual_command']['actual_exit_code']=None
        self.journal.rows[ref['name']]=canonical(value);ref['sha256']=digest(self.journal.rows[ref['name']])
        with self.assertRaisesRegex(Refused,'atomic_snapshot_command_unproven'):m.run(self.backend,self.op)
        self.assertEqual(self.calls,[])

    def test_fast_native_success_still_refuses_any_post_database_session(self):
        original=self.command
        def command(binary,args,kw):
            if args[0]=='wait':
                value=original(binary,args,kw);self.mode='sessions';return value
            return original(binary,args,kw)
        self.backend._run=lambda op,binary,args,**kw:command(binary,args,kw)
        with self.assertRaisesRegex(Refused,'global_sql_not_closed'):m.run(self.backend,self.op)
        self.assertFalse(m.PHASE+'-migration-complete.json' in self.journal.rows)

    def test_postmigration_old_row_change_cannot_be_waived_as_schema_change(self):
        original=self.command
        def command(binary,args,kw):
            if args[0]=='wait':
                value=original(binary,args,kw);self.state['tables']['openlegal.corpus_capture']['rows']+=1;return value
            return original(binary,args,kw)
        self.backend._run=lambda op,binary,args,**kw:command(binary,args,kw)
        with self.assertRaisesRegex(Refused,'old_rows_or_references_changed'):m.run(self.backend,self.op)
        self.assertTrue(self.guard.exists())

    def test_terminal_job_remains_unmodified_but_live_suspended_job_requires_fresh_fence(self):
        uid=str(uuid.uuid4());self.op.inventory['linux']['jobs']=[{'name':'history','uid':uid}]
        spec={'suspend':False};actual={'metadata':{'uid':uid,'resourceVersion':'10'},'spec':spec,
            'status':{'conditions':[{'type':'Complete','status':'True'}]}}
        row={'kind':'job','name':'history','uid':uid,'resource_version':'10','spec_sha256':digest(canonical(spec)),'fence_nonce':None}
        drain={'resources':[row],'closure':self.closure}
        with patch.object(self.backend,'_kube',return_value=actual):m._fence(self.backend,self.op,drain)
        actual['status']={};spec['suspend']=True;row['spec_sha256']=digest(canonical(spec))
        with patch.object(self.backend,'_kube',return_value=actual),self.assertRaisesRegex(Refused,'revision_fence_missing'):
            m._fence(self.backend,self.op,drain)

    def test_legacy_raw_binding_is_non_circular_and_wrong_authority_refuses(self):
        self.op.inventory_sha256='f'*64
        self.assertEqual(m._prior(self.op,self.c,'preservation'),self.state)
        self.c['legacy_binding']['authority_sha256']='e'*64
        with self.assertRaisesRegex(Refused,'prior_binding'):m._prior(self.op,self.c,'preservation')

    def test_unknown_gate_fsync_records_possible_dispatch_and_never_replays(self):
        original=m._write
        def write(parent,name,raw,*args,**kwargs):
            original(parent,name,raw,*args,**kwargs)
            if name=='go':raise OSError('synthetic gate fsync ACK unknown')
        with patch.object(m,'_write',side_effect=write),self.assertRaisesRegex(OSError,'ACK unknown'):
            m.run(self.backend,self.op)
        value=json.loads(self.journal.rows[m.PHASE+'-migration-failed.json'])
        self.assertTrue(value['payload_may_dispatched'])
        self.assertEqual(sum(args[0]=='start' for _,args,_ in self.calls),1)
        self.assertFalse(any(args[0] in ('rm','stop','kill') for _,args,_ in self.calls))

    def test_reference_multiset_proof_refuses_truncation_sentinel_before_dispatch(self):
        self.state['references']['rows']=200001
        with self.assertRaisesRegex(Refused,'references_bound'):m.run(self.backend,self.op)
        self.assertFalse(any(args[0]=='create' for _,args,_ in self.calls))

    def test_only_full_preservation_digests_use_maintenance_sql_budget(self):
        end=time.monotonic()+60
        self.assertEqual(m._old_state(self.backend,self.op,end),self.state)
        m._sessions(self.backend,self.op,end)
        m._sql(self.backend,self.op,m.MIGRATIONS_SQL,end)
        audit=quick=0
        for binary,args,kw in self.calls:
            self.assertEqual(kw['deadline'],end)
            sql=kw['stdin'].decode()
            if kw['timeout']==31:
                audit+=1;self.assertIn("statement_timeout='30s'",sql)
                self.assertIn("transaction_timeout='35s'",sql)
                self.assertNotIn(m.SESSIONS_SQL,sql);self.assertNotIn(m.TABLES_SQL,sql)
                self.assertNotIn(m.MIGRATIONS_SQL,sql)
            else:
                quick+=1;self.assertEqual(kw['timeout'],3)
                self.assertIn("statement_timeout='2s'",sql);self.assertIn("transaction_timeout='5s'",sql)
            self.assertIn("lock_timeout='1s'",sql)
        self.assertEqual((audit,quick),(3,3))

    def test_blank_libpq_service_is_not_unset_and_native_launcher_removes_it(self):
        # Real libpq, no server: BOTH probes target an empty private Unix socket
        # directory. The explicit library override is test-only, useful where
        # a local PostgreSQL client package is not on the default loader path.
        library=os.environ.get('OPENLEGAL_TEST_LIBPQ') or ctypes.util.find_library('pq')
        if not library:self.skipTest('local libpq unavailable; set OPENLEGAL_TEST_LIBPQ for this probe')
        probe = '''import ctypes,json,sys
q=ctypes.CDLL(sys.argv[1])
q.PQconnectdb.argtypes=[ctypes.c_char_p];q.PQconnectdb.restype=ctypes.c_void_p
q.PQerrorMessage.argtypes=[ctypes.c_void_p];q.PQerrorMessage.restype=ctypes.c_char_p
q.PQstatus.argtypes=[ctypes.c_void_p];q.PQstatus.restype=ctypes.c_int
q.PQfinish.argtypes=[ctypes.c_void_p]
connection=q.PQconnectdb(("host='"+sys.argv[2]+"' port=65433 dbname=fixture user=fixture connect_timeout=1 sslmode=disable").encode())
try:print(json.dumps({'status':q.PQstatus(connection),'error':q.PQerrorMessage(connection).decode()}))
finally:q.PQfinish(connection)
'''
        sockets=self.root/'absent-pg-socket';sockets.mkdir(mode=0o700)
        env={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','PGSERVICE':'',
             'PGSERVICEFILE':'/dev/null','PGPASSFILE':'/dev/null','PGPASSWORD':''}
        command=[sys.executable,'-I','-B','-c',probe,library,str(sockets)]
        bad=subprocess.run(command,env=env,capture_output=True,timeout=5,check=True)
        before=json.loads(bad.stdout)
        self.assertNotEqual(before['status'],0)
        self.assertIn('definition of service "" not found',before['error'])
        m._sql(self.backend,self.op,m.MIGRATIONS_SQL,time.monotonic()+30)
        args=self.calls[-1][1]
        # Execute the actual source-owned env prefix from the captured fixed
        # native argv, replacing only the psql payload by the real libpq probe.
        prefix=args[args.index('/usr/bin/env'):args.index('psql')]
        good=subprocess.run(prefix+command,env=env,capture_output=True,timeout=5,check=True)
        after=json.loads(good.stdout)
        self.assertNotEqual(after['status'],0)
        self.assertNotIn('definition of service',after['error'])
        self.assertIn(str(sockets),after['error'])
        self.assertIn('No such file or directory',after['error'])
        self.assertEqual(list(sockets.iterdir()),[])

    def test_audit_profile_cannot_widen_session_or_arbitrary_sql_budget(self):
        for sql in (m.SESSIONS_SQL,m.TABLES_SQL,m.MIGRATIONS_SQL,'SELECT 1',
                    m.OLD_TABLE_PREFIX+'public.other'+m.OLD_TABLE_SUFFIX):
            with self.assertRaisesRegex(Refused,'audit_sql_domain'):
                m._sql(self.backend,self.op,sql,time.monotonic()+60,audit=True)
        self.assertEqual(self.calls,[])

    def test_audit_sequence_never_refreshes_the_original_phase_deadline(self):
        # Enumeration and table digest consume the same original budget. The
        # next digest cannot obtain a fresh 31-second window after expiry.
        with patch.object(m.time,'monotonic',side_effect=[5,6,11]):
            with self.assertRaisesRegex(Refused,'sql_original_deadline'):
                m._old_state(self.backend,self.op,10)
        self.assertEqual(len(self.calls),2)
        self.assertEqual([kw['deadline'] for _,_,kw in self.calls],[10,10])
        self.assertEqual([kw['timeout'] for _,_,kw in self.calls],[3,31])
        self.assertFalse(any(m.REFERENCES_SQL in kw['stdin'].decode() for _,_,kw in self.calls))

    def test_actual_command_runner_caps_31_second_budget_at_original_deadline(self):
        # The real source-pinned process runner executes only a harmless Python
        # sleep. It must close that owned process at the original deadline.
        binary=Path(sys.executable).resolve()
        self.op.inventory['linux']['binaries']['audit_fixture']={'path':str(binary),'sha256':digest(binary.read_bytes())}
        end=time.monotonic()+.3
        with self.assertRaisesRegex(Refused,'command_failed_or_unclosed'):
            linux.LinuxBackend._run(self.backend,self.op,'audit_fixture',
                ['-I','-B','-c','import time; time.sleep(10)'],timeout=31,deadline=end)
        command=json.loads(self.journal.rows[m.PHASE+'-command-0001.json'])
        self.assertTrue(command['started']);self.assertTrue(command['expired'])
        self.assertTrue(command['owned_processes_closed']);self.assertLess(command['elapsed_seconds'],3)


if __name__=='__main__':unittest.main(verbosity=2)
