"""Pure and harmless /tmp/real-process tests. NO PostgreSQL/Docker/media gate.

Native catalogs, TLS, cgroups and the future maintenance producer are synthetic
where patched. Real Linux command receipts, actual child exit/pidfds, file
metadata/rebinding and parsing/budget/deadline guards are exercised separately.
"""
import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import types
import unittest
import uuid
from unittest.mock import patch

BASE=Path(__file__).resolve().parents[1];sys.path.insert(0,str(BASE))
import initial_pg_pair as p
from interface import Refused
from inventory import canonical,digest


def binding():
    return {'managed_epoch':str(uuid.uuid4()),'primary_system_identifier':'12345678901234567890','timeline':1,
        'standby_name':'strict_peer','standby_role':'replication_login','standby_addr':'172.31.64.3',
        'standby_client_dn':'CN=fixture-client','standby_issuer_dn':'CN=fixture-ca',
        'standby_client_serial':'123456789012345678901234567890123456789012345678901234567890',
        'blob_binding_sha256':'a'*64}


def peer(expected,*,prefix='0/100',flush='0/100',candidate=False):
    d=copy.deepcopy(expected)
    if candidate:
        for k in ('managed_epoch','blob_binding_sha256'):d.pop(k)
    d.update(postmaster_started_at='2026-11-02 00:00:00+00',sender_pid=1234,
        sender_started_at='2026-11-02 00:00:01+00',prefix_lsn=prefix,flush_lsn=flush)
    return d


class Contracts(unittest.TestCase):
    def test_exact_m30_keys_and_sixty_digit_serial_are_preserved(self):
        e=binding();self.assertIs(p._binding(e),e)
        bad=copy.deepcopy(e);bad['client_dn']=bad.pop('standby_client_dn')
        with self.assertRaisesRegex(Refused,'binding_fields'):p._binding(bad)
        changed=copy.deepcopy(e);changed['standby_client_serial']+='0'
        with self.assertRaisesRegex(Refused,'certificate'):p._binding(changed)
        actual=copy.deepcopy(e);actual['standby_client_serial']=int(e['standby_client_serial']);actual['standby_addr']+='/32'
        self.assertEqual(p._normalized_binding(actual),e)
        subnet=copy.deepcopy(actual);subnet['standby_addr']='172.31.64.3/24'
        with self.assertRaisesRegex(Refused,'host_prefix'):p._normalized_binding(subnet)
        actual['standby_client_serial']=float(e['standby_client_serial'])
        with self.assertRaisesRegex(Refused,'serial'):p._normalized_binding(actual)

    def test_flush_can_advance_without_insertion_or_equal_cut_assumptions(self):
        e=binding();a=peer(e);b=peer(e,prefix='0/900',flush='0/100')
        self.assertEqual(p._proof(a,b,e)['fixed_target'],'0/100')
        b['flush_lsn']='0/FF'
        with self.assertRaisesRegex(Refused,'not_durable'):p._proof(a,b,e)

    def test_wrong_epoch_blob_sender_or_postmaster_is_not_ack(self):
        e=binding();a=peer(e)
        for k,v in [('managed_epoch',str(uuid.uuid4())),('blob_binding_sha256','b'*64),
                    ('sender_pid',999),('postmaster_started_at','later')]:
            b=peer(e);b[k]=v
            with self.subTest(k=k),self.assertRaises(Refused):p._proof(a,b,e)
        a.pop('managed_epoch')
        with self.assertRaisesRegex(Refused,'control_missing'):p._proof(a,peer(e),e)

    def test_global_journal_budget_is_not_a_fresh_phase_budget(self):
        j=types.SimpleNamespace(count=10,bytes=100)
        r=p._reserve(j,20,1024,40)
        self.assertEqual(r['start_count'],10);self.assertEqual(r['records'],1024+320+120)
        j.count=4090
        with self.assertRaisesRegex(Refused,'whole_operation'):p._reserve(j,1,1)
        j.count=10;j.bytes=p.journals.MAX_PHASE_BYTES-1024
        with self.assertRaisesRegex(Refused,'whole_operation'):p._reserve(j,1,1)
        self.assertEqual(j.bytes,p.journals.MAX_PHASE_BYTES-1024)

    def test_free_floor_is_twenty_percent_TOTAL_not_free_and_no_reservation_claim(self):
        c={'filesystem_total_bytes':1000,'reserve_bytes':200,'copy_max_bytes':500}
        space=types.SimpleNamespace(f_blocks=1000,f_frsize=1,f_bavail=700)
        v=p._space_budget(c,space,0);self.assertFalse(v['hard_reservation']);self.assertFalse(v['peak_guarantee'])
        space.f_bavail=699
        with self.assertRaisesRegex(Refused,'live_space'):p._space_budget(c,space,0)
        space.f_bavail=200;self.assertEqual(p._space_budget(c,space,500)['remaining_copy_bound'],0)
        c['reserve_bytes']=199
        with self.assertRaisesRegex(Refused,'floor_changed'):p._space_budget(c,space,500)

    def test_total_byte_transport_is_lossless_above_js_number_precision(self):
        total=2**56+3;c={'filesystem_total_bytes':total,'reserve_bytes':(total+4)//5,'copy_max_bytes':10}
        space=types.SimpleNamespace(f_blocks=total,f_frsize=1,f_bavail=total)
        self.assertEqual(p._space_budget(c,space,0)['total_bytes'],total)
        space.f_blocks-=1
        with self.assertRaisesRegex(Refused,'total_or_floor'):p._space_budget(c,space,0)

    def test_plain_ssd_manifest_accepts_observed_budget_without_a_quota(self):
        # Native physical-media proof is deliberately NOT mocked as success here:
        # this exercises only the closed DATA grammar and actual private files.
        with tempfile.TemporaryDirectory(dir='/tmp') as root:
            root=Path(root);os.chmod(root,0o700)
            def ref(name,raw):
                path=root/name;path.write_bytes(raw);path.chmod(0o600)
                return {'path':str(path),'sha256':digest(raw)}
            q={'application_name':'peer','role':'replication_login','server_name':'primary.fixture',
                'server_certificate_sha256':'1'*64,'client_certificate_sha256':'2'*64,
                'client_dn':'CN=x','issuer_dn':'CN=ca','client_serial':'123'}
            for k in ('ca','certificate','private_key'):
                q[k]=ref('client-'+k,b'fictional '+k.encode());os.chown(q[k]['path'],70,70)
            trust={k:ref('server-'+k,b'fictional '+k.encode()) for k in ('ca','certificate','private_key','hba')}
            for row in trust.values():os.chown(row['path'],70,70)
            parent=root/'target';parent.mkdir(mode=0o700);st=parent.stat()
            d={'schema_version':1,'kind':'openlegal_initial_pair_preprovision','image_id':'sha256:'+'3'*64,
               'tools':{k:'4'*64 for k in p.TOOLS},'network':{'id':'5'*64,'name':'internal-fixture','subnet':'172.31.64.0/24',
                   'primary_address':'172.31.64.2','backup_address':'172.31.64.4','standby_address':'172.31.64.3'},
               'peer':q,'recovery_role':'pair_recovery','function_owners':{k:{'name':'no_login_owner','body_sha256':'6'*64} for k in p.FUNCTIONS},
               'ordinary_roles':['runtime_fixture'],'target':{'parent':str(parent),'identity':{'dev':st.st_dev,'ino':st.st_ino},
                   'kernel_filesystem_id':os.statvfs(parent).f_fsid,'capacity':{'kind':'plain_ssd_bounded_copy_v1',
                       'copy_max_bytes':500,'reserve_bytes':200,'filesystem_total_bytes':1000,'max_files':1000,
                       'physical_leaves':['/dev/sda'],'max_rate_kib':65536}},
               'primary_slot':{'path':str(root/'auto.conf'),'identity':{'dev':1,'ino':2},
                   'before':ref('before',b"synchronous_standby_names=''\n"),
                   'after':ref('after',b"synchronous_standby_names='FIRST 1 (peer)'\n")},
               'standby_trust':trust,'source_receipts':[ref('receipt',b'fixed expected native metadata')]}
            p._provision(d,str(uuid.uuid4()))
            d['target']['capacity']['reserve_bytes']=20
            with self.assertRaisesRegex(Refused,'copy_budget_policy'):p._provision(d,str(uuid.uuid4()))
            d['target']['capacity']['reserve_bytes']=200;d['password']='forbidden'
            with self.assertRaisesRegex(Refused,'pair_provision'):p._provision(d,str(uuid.uuid4()))

    def test_fixed_create_uses_no_pull_no_restart_no_cleanup_and_max_rate(self):
        e=binding();pair=types.SimpleNamespace(cfg={'operation_id':str(uuid.uuid4())},sc=[{'managed_epoch':e['managed_epoch']}],
            workspace=Path('/tmp/fictional-fixed-control'),p={'image_id':'sha256:'+'3'*64,
                'target':{'parent':'/fictional/ssd','capacity':{'max_rate_kib':65536}},
                'peer':{'role':e['standby_role'],'application_name':e['standby_name'],'server_name':'primary.fixture',
                    **{k:{'path':'/fictional/'+k} for k in ('ca','certificate','private_key')}},
                'network':{'id':'5'*64,'primary_address':'172.31.64.2','backup_address':'172.31.64.4','standby_address':'172.31.64.3'}})
        args=p._spec(pair,'backup')['args']
        self.assertIn('--pull=never',args);self.assertIn('--restart=no',args);self.assertIn('--max-rate=65536',args)
        self.assertNotIn('--rm',args);self.assertNotIn('PGPASSWORD',repr(args));self.assertIn('passfile=/dev/null',args[-1])

    def test_missing_live_backend_cannot_reach_any_native_action(self):
        with patch.object(p,'_load',side_effect=AssertionError('must not load')):
            with self.assertRaisesRegex(Refused,'dedicated_root_context'):p.run(object(),object(),{}, {})


class StrictTests(unittest.TestCase):
    def setUp(self):
        self.expected=binding();self.operation=str(uuid.uuid4());self.calls=[];self.intent={'sha256':'c'*64}
        self.backend=types.SimpleNamespace(_driver_deadline=time.monotonic()+100,_initial_pg_pair_bind_state='UNATTEMPTED')
        self.pair=types.SimpleNamespace(b=self.backend,cfg={'operation_id':self.operation},
            save=lambda *a:self.intent,sessions=lambda:None,seal=lambda:None)
        self.candidate=peer(self.expected,candidate=True,prefix='0/80')
        executable=Path(sys.executable).resolve()
        self.pair.primary='1'*64;self.pair.p={'recovery_role':'fixture_pair_role'}
        self.pair.work={'linux':{'binaries':{'docker':{'path':str(executable),'sha256':digest(executable.read_bytes())}}}}
        self.backend._docker_config=(Path('/tmp/explicitly-synthetic-no-docker'),None)
        self.addCleanup(patch.stopall)
        patch.object(p,'_critical_light',return_value=None).start()
        def execute(command,**kwargs):
            # Synthetic SQL payload; source builds the REAL fixed native argv
            # and stdin. Only the process outcome is replaced in these tests.
            value,_,_=self.pair.sql(kwargs['stdin'].decode(),end=kwargs['deadline'],multi=True)
            if not isinstance(value,list):value=[value]
            stdout=b''.join(canonical(v) for v in value)
            return p.driver.processes.CommandResult(stdout,b'',0,False,False,True,False,.001,True,False)
        patch.object(p.driver.processes,'execute',side_effect=execute).start()
    def batch(self,sql,**kwargs):
        self.calls.append((sql,kwargs))
        result={'operation_id':self.operation,'intent_sha256':self.intent['sha256'],'previous_binding':None,
            'new_binding':self.expected,'peer_before_commit':peer(self.expected,prefix='0/90')}
        return [result,peer(self.expected),peer(self.expected,prefix='0/900')],{},0
    def test_initial_NULL_bind_and_fresh_cut_share_ONE_original_two_second_deadline(self):
        self.pair.sql=self.batch
        proof=p._strict(self.pair,self.expected,candidate=self.candidate,bind=True)
        sql,args=self.calls[0]
        self.assertIn(',NULL::jsonb,',sql);self.assertEqual(sql.count('COMMIT'),1)
        self.assertLess(sql.index('COMMIT'),sql.index('capture_prefix'))
        self.assertIn('standby_client_dn',sql);self.assertNotIn('provider_apply',sql)
        self.assertLessEqual(args['end'],self.backend._driver_deadline)
        self.assertLessEqual(args['end'],time.monotonic()+2)
        self.assertEqual(proof['fixed_target'],'0/100');self.assertEqual(self.backend._initial_pg_pair_bind_state,'ACKNOWLEDGED')
    def test_unknown_transmitted_commit_never_retries(self):
        def lost(sql,**kwargs):self.calls.append(sql);raise Refused('synthetic_lost_commit')
        self.pair.sql=lost
        with self.assertRaisesRegex(Refused,'lost_commit'):p._strict(self.pair,self.expected,candidate=self.candidate,bind=True)
        self.assertEqual(self.backend._initial_pg_pair_bind_state,'TRANSMITTED_UNKNOWN')
        with self.assertRaisesRegex(Refused,'bind_once'):p._strict(self.pair,self.expected,candidate=self.candidate,bind=True)
        self.assertEqual(len(self.calls),1)
    def test_lag_poll_reuses_frozen_target_AND_original_deadline_without_recommit(self):
        def lag(sql,**kw):
            self.calls.append((sql,kw))
            if len(self.calls)==1:return [peer(self.expected),peer(self.expected,flush='0/FF')],{},0
            return peer(self.expected,prefix='0/999',flush='0/100'),{},0
        self.pair.sql=lag
        proof=p._strict(self.pair,self.expected,candidate=self.candidate)
        self.assertEqual(proof['fixed_target'],'0/100');self.assertEqual(len(self.calls),2)
        self.assertEqual(self.calls[0][1]['end'],self.calls[1][1]['end'])
        self.assertNotIn('COMMIT',self.calls[1][0]);self.assertNotIn('capture_prefix',self.calls[1][0])
    def test_sessions_or_late_ack_cannot_be_promoted_to_success(self):
        self.pair.sql=self.batch
        def unclosed():raise Refused('synthetic_sql_still_live')
        self.pair.sessions=unclosed
        with self.assertRaisesRegex(Refused,'sql_still_live'):p._strict(self.pair,self.expected,candidate=self.candidate,bind=True)
        self.assertEqual(self.backend._initial_pg_pair_bind_state,'TRANSMITTED_UNKNOWN')
    def test_original_caller_end_is_not_renewed(self):
        self.backend._driver_deadline=time.monotonic()-.1;self.pair.sql=self.batch
        with self.assertRaises(Refused):p._strict(self.pair,self.expected,candidate=self.candidate,bind=True)
        self.assertEqual(self.calls,[]);self.assertEqual(self.backend._initial_pg_pair_bind_state,'UNATTEMPTED')


@unittest.skipUnless(os.geteuid()==0,'actual UID70/private metadata')
class FilesAndProcesses(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='initial-pair-review-',dir='/tmp');self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700)
    def test_trust_hash_and_readability_refuse_symlinks_or_late_replacement(self):
        path=self.root/'key';path.write_bytes(b'fictional private material');os.chown(path,70,70);path.chmod(0o600)
        ref={'path':str(path),'sha256':digest(path.read_bytes())}
        self.assertEqual(p._trust_file(ref),b'fictional private material')
        path.unlink();path.symlink_to('/dev/null')
        with self.assertRaises(OSError):p._trust_file(ref)
        path.unlink();path.write_bytes(b'changed');os.chown(path,70,70);path.chmod(0o600)
        with self.assertRaisesRegex(Refused,'file_changed'):p._trust_file(ref)
    def test_real_native_helper_pidfd_stop_closes_ONLY_actual_owned_harmless_child(self):
        child=subprocess.Popen([sys.executable,'-I','-B','-c','import time;time.sleep(10)'])
        fd=os.pidfd_open(child.pid);group=os.open(self.root,os.O_DIRECTORY|os.O_RDONLY)
        backend=types.SimpleNamespace(_initial_pg_pair_retained=[])
        native={'pidfd':fd,'cgroup_fd':group,'groups':['0::/explicitly-synthetic-kernel-view']}
        def members(_):return [child.pid] if child.poll() is None else []
        try:
            with patch.object(p.linux,'members',side_effect=members),patch.object(p,'root_fd',side_effect=lambda _:os.dup(group)):p._stop_owned_helper(types.SimpleNamespace(b=backend),native)
            self.assertEqual(child.wait(),-signal.SIGTERM)
        finally:
            if child.poll() is None:child.kill();child.wait()
            for held in (fd,group,*backend._initial_pg_pair_retained):os.close(held)
    def test_copy_monitor_failure_stops_owned_helper_without_wait_or_ack(self):
        child=subprocess.Popen([sys.executable,'-I','-B','-c','import time;time.sleep(10)']);fd=os.pidfd_open(child.pid)
        native={'pidfd':fd,'spec':{'command':['/usr/local/bin/pg_basebackup']}}
        pair=types.SimpleNamespace(b=types.SimpleNamespace(_driver_deadline=time.monotonic()+100),
            command=lambda *a,**k:self.fail('Docker wait must not run after monitor failure'))
        events=[]
        try:
            with patch.object(p,'_copy_watch',side_effect=OSError('synthetic metadata EIO')),\
                 patch.object(p,'_stop_owned_helper',side_effect=lambda a,b:events.append(b)):
                with self.assertRaisesRegex(OSError,'metadata EIO'):p._wait(pair,'1'*64,native,900)
            self.assertEqual(events,[native])
        finally:child.kill();child.wait();os.close(fd)

    def test_missing_cgroup_unknown_keeps_owner_until_real_pidfd_exit(self):
        # The cgroup view is explicitly synthetic /tmp metadata. The leader
        # and its pidfd/exit are real; unknown must keep the owner in the loop.
        child=subprocess.Popen([sys.executable,'-I','-B','-c','import time;time.sleep(10)'])
        pidfd=os.pidfd_open(child.pid);group=os.open(self.root,os.O_DIRECTORY|os.O_RDONLY)
        events=self.root/'cgroup.events';events.write_bytes(b'populated 1\nfrozen 0\n')
        native={'pidfd':pidfd,'cgroup_fd':group,'groups':['0::/synthetic-missing-group']}
        observed=[]
        def wait_unknown(_):
            observed.append(child.poll());self.assertIsNone(child.poll())
            child.terminate();child.wait();events.write_bytes(b'populated 0\nfrozen 0\n')
        try:
            with patch.object(p,'root_fd',side_effect=FileNotFoundError('synthetic missing cgroup')),\
                 patch.object(p.time,'sleep',side_effect=wait_unknown):
                p._stop_owned_helper(types.SimpleNamespace(b=types.SimpleNamespace(_initial_pg_pair_retained=[])),native)
            self.assertEqual(observed,[None]);self.assertEqual(child.returncode,-signal.SIGTERM)
        finally:
            if child.poll() is None:child.kill();child.wait()
            os.close(pidfd);os.close(group)

    def test_historical_closed_marker_never_waives_current_pidfd_and_cgroup_absence(self):
        child=subprocess.Popen([sys.executable,'-I','-B','-c','import time;time.sleep(10)'])
        pidfd=os.pidfd_open(child.pid);group=os.open(self.root,os.O_DIRECTORY|os.O_RDONLY)
        operation=str(uuid.uuid4());cid='1'*64
        process={'pid':child.pid,'start_ticks':Path('/proc/'+str(child.pid)+'/stat').read_text().rsplit(')',1)[1].split()[19],
            'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
        native={'container_id':cid,'process':process,'pidfd':pidfd,'cgroup_fd':group,
            'groups':['0::/explicitly-synthetic-cgroup'],'closure':{'name':'synthetic-closed'}}
        backend=types.SimpleNamespace(_initial_pg_pair_retained=[])
        dispatch=['--config','/tmp/openlegal-storage-docker-client-'+operation,
            '--host','unix:///var/run/docker.sock','wait',cid]
        command={'actual_exit_code':0,'started':True,'owned_processes_closed':True,'binary':'docker',
            'arguments_sha256':digest(canonical(dispatch)),'stdin_sha256':None,
            'stdout_sha256':digest(b'0\n'),'stdout_bytes':2,
            **dict.fromkeys(('expired','interrupted','unexpected_descendants','output_limit_exceeded'),False)}
        closed={'container_id':cid,'process':process,'actual_exit_code':0,'owned_members':[],
            'wait_command':{'name':'synthetic-wait'}}
        def raw(_,ref):return closed if ref['name']=='synthetic-closed' else command
        try:
            with patch.object(p,'_raw_reference',side_effect=raw),\
                 patch.object(p,'root_fd',side_effect=lambda _:os.dup(group)),\
                 patch.object(p.linux,'members',side_effect=lambda _:([child.pid] if child.poll() is None else [])):
                p._stop_owned_helper(types.SimpleNamespace(b=backend,j=object(),cfg={'operation_id':operation}),native)
            self.assertEqual(child.wait(),-signal.SIGTERM)
        finally:
            if child.poll() is None:child.kill();child.wait()
            for fd in (pidfd,group,*backend._initial_pg_pair_retained):os.close(fd)

    def test_started_helper_failures_remain_registered_and_drain_actual_child(self):
        for failure in ('start_unknown','inspect','pidfd','native_receipt'):
            with self.subTest(failure=failure):self._started_failure(failure)

    def _started_failure(self,failure):
        cid='1'*64;group=os.open(self.root,os.O_DIRECTORY|os.O_RDONLY);child=None
        group_name='0::/synthetic/docker-'+cid+'.scope';spec={'name':'synthetic-only','args':['create'],
            'command':['/usr/local/bin/pg_basebackup']}
        backend=types.SimpleNamespace(_driver_deadline=time.monotonic()+30,_initial_pg_pair_retained=[],
            _initial_pg_pair_pending_ownership={})
        pair=types.SimpleNamespace(b=backend,created={},helpers=[],primary='2'*64,
            cfg={'operation_id':str(uuid.uuid4())},parent=os.getpid(),ticks='synthetic-parent',
            j=types.SimpleNamespace(operation='synthetic-journal'),raw=b'synthetic-inventory',authraw=b'synthetic-authority',
            work={'host':{'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip()}})
        pair.p={'tools':{'pg_basebackup':digest(Path(sys.executable).resolve().read_bytes())}}
        def command(args,**kwargs):
            nonlocal child
            if args[0]=='create':return (cid+'\n').encode(),{}
            self.assertEqual(args,['start',cid])
            self.assertIs(pair.helpers[0],backend._initial_pg_pair_pending_ownership[cid])
            self.assertTrue(pair.helpers[0]['start_transmitted'])
            child=subprocess.Popen([sys.executable,'-I','-B','-c','import time;time.sleep(10)'])
            if failure=='start_unknown':raise Refused('synthetic start response lost')
            return b'',{}
        def inspect(*args,**kwargs):
            if failure=='inspect':raise Refused('synthetic post-start inspect refusal')
            return {'State':{'Pid':child.pid}}
        def save(name,value):
            if name=='backup-native-start' and failure=='native_receipt':raise Refused('synthetic journal ACK failure')
            return {'phase':p.PHASE,'name':'synthetic-'+name,'sha256':'a'*64}
        pair.command=command;pair.save=save
        actual_pidfd=os.pidfd_open;failed=[]
        def pidfd(pid,*args):
            if failure=='pidfd' and not failed:
                failed.append(True);raise OSError('synthetic first anchor failure')
            return actual_pidfd(pid,*args)
        try:
            with patch.object(p,'_spec',return_value=spec),patch.object(p,'_inspect',side_effect=inspect),\
                 patch.object(p.linux,'process_cgroup',side_effect=lambda pid:[group_name] if child and pid==child.pid and child.poll() is None else []),\
                 patch.object(p.linux,'members',side_effect=lambda _:([child.pid] if child and child.poll() is None else [])),\
                 patch.object(p,'root_fd',side_effect=lambda _:os.dup(group)),patch.object(p.os,'pidfd_open',side_effect=pidfd):
                with self.assertRaises((Refused,OSError)):p._start(pair,'backup')
                self.assertEqual(len(pair.helpers),1);self.assertIn(cid,backend._initial_pg_pair_pending_ownership)
                # Same cleanup call as _run's failure path; no synthetic success
                # callback can replace the actual pidfd signal/wait/absence.
                p._stop_owned_helper(pair,pair.helpers[0])
            self.assertEqual(child.wait(),-signal.SIGTERM)
        finally:
            if child and child.poll() is None:child.kill();child.wait()
            os.close(group)
            for fd in backend._initial_pg_pair_retained:os.close(fd)


class MoreTests(unittest.TestCase):
    def _role_catalog(self):
        roles={k:'fixture_'+k for k in p.role_prepare.ROLE_KEYS};runtime='fixture_runtime'
        expected=p.role_prepare.expected_catalog(roles,runtime)
        functions={}
        for sig,row in expected['functions'].items():
            acl=[[name,'EXECUTE',False] for name,grants in expected['grants'].items() if ['function',sig,'EXECUTE'] in grants]
            if sig in p.role_prepare.RUNTIME_EXECUTE:acl.append([runtime,'EXECUTE',False])
            if sig=='openlegal.replication_required()':acl.append(['PUBLIC','EXECUTE',False])
            functions[sig]=dict(row,acl=sorted(acl),definer=True,
                language='sql' if sig in p.role_prepare.SQL_FUNCTIONS else 'plpgsql')
        effective={'functions':[],'tables':[],'columns':[],'schemas':[],'create':False,
            'membership_paths':sorted([[m,g,True,True,False] for m,g,_,_,_ in expected['memberships']]),'admin_edges':[]}
        for name,grants in expected['grants'].items():
            for kind,obj,priv in grants:
                if kind=='function' and obj!='openlegal.replication_required()':effective['functions'].append([name,obj])
                elif kind in ('table','column'):effective['tables' if kind=='table' else 'columns'].append([name,obj,priv])
                elif kind=='schema' and obj!='public':effective['schemas'].append([name,obj])
        for key in ('functions','tables','columns','schemas'):effective[key].sort()
        value={'session_user':roles['recovery_login'],'current_user':roles['recovery_login'],
            'roles':copy.deepcopy(expected['flags']),
            'grants':sorted([name,*r,False] for name,rows in expected['grants'].items() for r in rows),
            'memberships':copy.deepcopy(expected['memberships']),'functions':functions,'other_owned':False,
            'effective':effective,'runtime':{'exists':True,'admin_usage':True,'new_usage':True,'executes':[True]*3}}
        return roles,runtime,value

    def test_pair_fresh_source_catalog_denies_peer_owner_column_and_catalog_escalation(self):
        roles,runtime,value=self._role_catalog();queries=[];saves=[]
        def sql(query,**kwargs):
            queries.append(query)
            return (copy.deepcopy(value) if query.startswith('SELECT jsonb_build_object') else True),{},0
        pair=types.SimpleNamespace(role_context={'names':roles,'runtime':runtime},
            p={'recovery_role':roles['recovery_login'],'ordinary_roles':sorted([runtime,roles['storage_login'],roles['canary_login']])},
            sql=sql,save=lambda *args:saves.append(args))
        p._catalog(pair);self.assertEqual(len(saves),1)
        self.assertIn('has_column_privilege',queries[0]);self.assertIn("'SET'",queries[0])
        self.assertIn('m.admin_option',queries[0]);self.assertIn("a.grantee=0 AND a.privilege_type='EXECUTE'",queries[0])
        self.assertTrue(all("'MEMBER'" in q for q in queries[1:]))
        mutations=[
            lambda v:v['memberships'].append([roles['peer_login'],roles['binding_owner'],False,False,True]),
            lambda v:v['memberships'].append([roles['checkpoint_owner'],'pg_write_server_files',False,True,False]),
            lambda v:v['effective']['membership_paths'].append([roles['replication_owner'],'pg_write_server_files',True,True,False]),
            lambda v:v['effective']['admin_edges'].append([roles['replication_owner'],'pg_read_all_settings','pg_write_server_files']),
            lambda v:v['effective']['columns'].append([roles['recovery_login'],'openlegal_admin.provider_control.recovery_hold','UPDATE']),
            lambda v:v['grants'].append([roles['recovery_login'],'column','openlegal_admin.provider_control.recovery_hold','UPDATE',False]),
            lambda v:v['effective']['functions'].append([roles['peer_login'],'pg_catalog.pg_read_binary_file(text)']),
            lambda v:v['effective'].__setitem__('create',True),
            lambda v:v.__setitem__('other_owned',True),
            lambda v:v['roles'][roles['recovery_login']].__setitem__('super',True),
        ]
        original=copy.deepcopy(value)
        for mutate in mutations:
            value.clear();value.update(copy.deepcopy(original));mutate(value)
            with self.assertRaises(Refused):p._catalog(pair)
        self.assertEqual(len(saves),1)

    def test_m32_bundles_reuse_raw_receipts_and_preserve_typed_cursors(self):
        def make():
            calls=[]
            def sql(query,**kw):
                calls.append(query);result=[]
                for kind,cursor in p.re.findall(r"preservation_page\('([^']+)',(NULL::jsonb|'[^']+'::jsonb),128\)",query):
                    if kind=='corpus_capture':
                        start=0 if cursor=='NULL::jsonb' else 128
                        rows=[{'key':['%064x'%i],'sha256':'d'*64,'monotonic':None} for i in range(start,min(start+128,129))]
                    else:rows=[]
                    more=kind=='corpus_capture' and cursor=='NULL::jsonb'
                    result.append({'schema_version':1,'kind':'openlegal_preservation_page','relation':kind,'rows':rows,
                                   'next_key':rows[-1]['key'] if more else None,'has_more':more})
                return result,{'name':'ack'+str(len(calls))},len(canonical(result))
            pair=types.SimpleNamespace(seal=lambda:None,sql=sql)
            return pair,calls
        a,calls=make();b,_=make()
        first=p._m32(a);second=p._m32(b)
        self.assertEqual(first['page_hashes'],second['page_hashes']);self.assertEqual(first['bundles'],2)
        self.assertEqual(len(first['page_hashes']['corpus_capture']),2)
        self.assertEqual(len(calls),2);self.assertIn('%064x'%127,calls[1])
        self.assertTrue(all('preservation_page' in sql and '128' in sql for sql in calls))

    def test_global_session_check_never_filters_by_application_name(self):
        pair=object.__new__(p._Pair)
        with patch.object(p._Pair,'sql',return_value=({'sessions':[{'pid':123}]},{},0)) as sql:
            with self.assertRaisesRegex(Refused,'global_sql_not_closed'):pair.sessions()
        self.assertEqual(sql.call_args.args[0],'SELECT openlegal_checkpoint.client_sessions()')

    def test_actual_copy_walker_refuses_symlinks_hardlinks_and_original_expiry(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as path:
            root=Path(path);a=root/'a';a.write_bytes(b'fictional')
            pair=types.SimpleNamespace(b=types.SimpleNamespace(_driver_deadline=time.monotonic()+10),
                p={'target':{'capacity':{'max_files':10,'copy_max_bytes':100}}})
            self.assertEqual(p._copy_size(pair,root),{'entries':1,'logical_bytes':9})
            os.link(a,root/'b')
            with self.assertRaisesRegex(Refused,'external_file'):p._copy_size(pair,root)
            (root/'b').unlink();(root/'b').symlink_to(a)
            with self.assertRaisesRegex(Refused,'external_file'):p._copy_size(pair,root)
            (root/'b').unlink();pair.b._driver_deadline=time.monotonic()-1
            with self.assertRaises(Refused):p._copy_size(pair,root)

    def test_same_device_nested_mount_view_refuses_in_copy_walk(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as path:
            root=Path(path);(root/'child').mkdir();ino=(root/'child').stat().st_ino
            pair=types.SimpleNamespace(b=types.SimpleNamespace(_driver_deadline=time.monotonic()+10),
                p={'target':{'capacity':{'max_files':10,'copy_max_bytes':100}}})
            original=p.blobs._fd_mount_id
            with patch.object(p.blobs,'_fd_mount_id',side_effect=lambda fd:original(fd)+(1 if os.fstat(fd).st_ino==ino else 0)):
                with self.assertRaisesRegex(Refused,'nested_mount'):p._copy_size(pair,root)

    def test_recovery_transition_reconstructs_legacy_domain_and_rejects_charge_mutation(self):
        op=str(uuid.uuid4());owner=str(uuid.uuid4());generation=str(uuid.uuid4());new=str(uuid.uuid4())
        before={'storage_id':'fictional','schema':[],'corpus_control':{},'legacy_lock_held':False,
            'ledger':[{'owner':owner,'slot':1,'lock_held':False}],
            'budget':{'unresolved_response':False,'admission_owner':None,'daily_used':27},
            'control':{'generation':generation,'recovery_hold':True,'hold_started_at':4,'legacy_identity':None}}
        request={'owners':[owner],'resume':False,'resolve_legacy':False,'waits':[],
            'actor':'fixture','quiescence':{'writers_stopped_at':5}}
        plan={'operation_id':op,'request':request,'snapshot':{'state':before},'created_at':5,'expires_at':20}
        result={'hold_generation':new,'recovery_hold':True,'resolved_legacy':False,'advanced_waits':[],
            'unchanged_waits':[],'resolved_owners':[owner],'applied_at':10}
        audit={'operation_id':op,'plan':plan,'before_state':before,'result':result,'applied_at':10,
            'actor':'fixture','database_actor':'named_fixture','actual_writers_stopped_at':5}
        state=copy.deepcopy(before);state['ledger']=[];state['control']['generation']=new
        value={'audit':audit,'state':state,'all_slots_free':True,
            'new_observations':[{'identity':owner,'owner':owner,'kind':'slot','slot':1,'first_observed_at':None}]}
        oldproof={'rows':1,'sha256':'f'*64};sqls=[]
        old={'tables':{t:oldproof for t in ('openlegal_admin.provider_control','openlegal.provider_request_admission',
            'openlegal_admin.provider_recovery_audit','openlegal_admin.uncertainty_observation')},
            'references':oldproof,'migration_history':oldproof}
        def sql(query,**kw):
            sqls.append(query)
            if "'all_slots_free'" in query:return copy.deepcopy(value),{},0
            if query==p.migration.MIGRATIONS_SQL:return [],{},0
            return oldproof,{},0
        pair=types.SimpleNamespace(cfg={'operation_id':op},old={'payloads':{'preservation':old}},sql=sql,
            work={'initial_migration':{}},sessions=lambda cid:None)
        with patch.object(p.migration,'_manifest',return_value=[]):
            p._audit_legacy(pair,{'new_uncertainty_ids':[owner],'provider_recovery_operation':op},audit)
            self.assertTrue(any('jsonb_populate_recordset' in s for s in sqls))
            self.assertTrue(any("value" not in s and 'before_state' in s for s in sqls))
            value['state']['budget']['daily_used']+=1
            with self.assertRaisesRegex(Refused,'charges_or_data'):p._audit_legacy(pair,{'new_uncertainty_ids':[owner],'provider_recovery_operation':op},audit)
            value['state']['budget']['daily_used']-=1;value['new_observations'][0]['first_observed_at']=10
            with self.assertRaisesRegex(Refused,'timestamp_not_fabricated'):p._audit_legacy(pair,{'new_uncertainty_ids':[owner],'provider_recovery_operation':op},audit)


class RawJournal:
    """Two actual /tmp raw-file ACKs; same filesystem is EXPLICITLY synthetic media.
    This tests the command port, not run()'s exact DualJournal/live-driver gate.
    """
    def __init__(self,root):
        self.roots=[root/'a',root/'b']
        for r in self.roots:r.mkdir(mode=0o700)
        self.artifacts={};self.count=0;self.bytes=0
    def check(self):
        for n,row in self.artifacts.items():
            for r in self.roots:
                if (r/n).read_bytes()!=row['raw']:raise Refused('fixture_both_journal_changed')
    def save(self,n,value):return self.save_bytes(n,canonical(value))
    def save_bytes(self,n,raw):
        self.check()
        for r in self.roots:
            fd=os.open(r/n,os.O_WRONLY|os.O_EXCL|os.O_CREAT,0o600)
            try:os.write(fd,raw);os.fsync(fd)
            finally:os.close(fd)
            parent=os.open(r,os.O_DIRECTORY|os.O_RDONLY)
            try:os.fsync(parent)
            finally:os.close(parent)
        self.artifacts[n]={'raw':raw,'sha256':digest(raw)};self.count+=1;self.bytes+=len(raw);self.check();return digest(raw)
    def output_hashes(self):self.check();return {n:r['sha256'] for n,r in self.artifacts.items()}
    def read_phase_bytes(self,phase,n,sha):
        self.check();assert phase==p.PHASE and self.artifacts[n]['sha256']==sha;return self.artifacts[n]['raw']


@unittest.skipUnless(os.geteuid()==0,'actual root executable/metadata proof')
class RealCommandPort(unittest.TestCase):
    def test_critical_port_records_real_child_zero_and_rejects_exit_seven(self):
        # SQL and Docker are synthetic. The dedicated critical port hashes and
        # executes a held real fixture inode, observes actual OS exit/descendant
        # closure and records its exact native window and raw output.
        for code in (0,7):
            with self.subTest(code=code),tempfile.TemporaryDirectory(prefix='pair-critical-port-',dir='/tmp') as path:
                root=Path(path);root.chmod(0o700);e=binding()
                output=canonical(peer(e))+canonical(peer(e,prefix='0/200'))
                tool=root/'explicitly-synthetic-docker'
                tool.write_text('#!/usr/bin/python3 -I\nimport sys\nsys.stdin.buffer.read()\nsys.stdout.buffer.write('+repr(output)+')\nsys.exit('+str(code)+')\n')
                tool.chmod(0o755);native_end=time.monotonic()+2
                backend=types.SimpleNamespace(_driver_deadline=native_end+5,
                    _docker_config=(root/'synthetic-client-config',None))
                pair=types.SimpleNamespace(b=backend,primary='1'*64,p={'recovery_role':'fixture_pair_role'},
                    work={'linux':{'binaries':{'docker':{'path':str(tool),'sha256':digest(tool.read_bytes())}}}})
                with patch.object(p,'_critical_light',return_value=None),patch.object(p.linux,'secure_path',return_value=tool.stat()):
                    values,receipt=p._critical_execute(pair,'proof',e,None,native_end)
                self.assertEqual(receipt['actual_exit_code'],code);self.assertTrue(receipt['owned_processes_closed'])
                self.assertTrue(receipt['started']);self.assertFalse(receipt['expired'])
                self.assertEqual(receipt['stdout_hex'],output.hex());self.assertEqual(receipt['stdout_sha256'],digest(output))
                self.assertLess(receipt['native_finished_at'],native_end)
                self.assertEqual(receipt['native_absolute_deadline'],native_end)
                self.assertIs(backend._initial_pg_pair_critical_result,receipt)
                if code:self.assertIsInstance(values,p._CriticalFailure)
                else:self.assertEqual(values,[peer(e),peer(e,prefix='0/200')])

    def test_actual_linux_os_zero_raw_hashes_and_both_ACK_before_SQL_result(self):
        self._actual(0,b'{"fictional_metadata":true}\n')
    def test_success_JSON_cannot_hide_actual_exit_seven(self):
        self._actual(7,b'{"owned_processes_closed":true}\n')
    def test_nonfinite_actual_output_is_refused_after_real_child_zero(self):
        self._actual(0,b'{"fictional_metadata":NaN}\n',invalid=True)
    def _actual(self,code,output,invalid=False):
        with tempfile.TemporaryDirectory(prefix='pair-command-port-',dir='/tmp') as path:
            root=Path(path);root.chmod(0o700);tool=root/'fixed-harmless-fixture'
            tool.write_text('#!/usr/bin/python3 -I\nimport sys\nsys.stdin.buffer.read()\nsys.stdout.buffer.write('+repr(output)+')\nsys.exit('+str(code)+')\n');tool.chmod(0o755)
            backend=p.driver.Backend();backend._driver_deadline=time.monotonic()+15
            j=RawJournal(root);operation=str(uuid.uuid4())
            pair=object.__new__(p._Pair);pair.b=backend;pair.j=j;pair.budget=None;pair.serial=0
            pair.primary='1'*64;pair.standby=None;pair.created={};pair.p={'recovery_role':'pair_fixture_role'}
            pair.op=types.SimpleNamespace(operation_id=operation,phase=p.PHASE,journal=j,seal=lambda:None,
                inventory={'linux':{'binaries':{'docker':{'path':str(tool),'sha256':digest(tool.read_bytes())}}}})
            try:
                original_secure=p.linux.secure_path
                def fixture_binary(path,*args,**kw):
                    # Only permit this known root-private /tmp executable in the
                    # unit fixture. Production secure_path is NOT weakened.
                    if path==str(tool):
                        self.assertEqual(tool.resolve(),tool);self.assertEqual(tool.stat().st_uid,0)
                        self.assertEqual(root.stat().st_mode&0o777,0o700)
                        self.assertEqual(tool.stat().st_mode&0o777,0o755)
                        return tool.stat()
                    return original_secure(path,*args,**kw)
                with patch.object(p._Pair,'seal',return_value=None),patch.object(p.linux,'secure_path',side_effect=fixture_binary):
                    if code or invalid:
                        with self.assertRaises(Refused):pair.sql("SELECT jsonb_build_object('fictional_metadata',true)")
                    else:
                        value,ref,_=pair.sql("SELECT jsonb_build_object('fictional_metadata',true)")
                        self.assertEqual(value,{'fictional_metadata':True})
                        saved=json.loads(j.read_phase_bytes(p.PHASE,ref['name'],ref['sha256']))
                        self.assertEqual(saved['raw_utf8'].encode(),output)
                commands=[json.loads(r['raw']) for n,r in j.artifacts.items() if '-command-' in n]
                self.assertEqual(len(commands),1);receipt=commands[0]
                self.assertEqual(receipt['actual_exit_code'],code);self.assertTrue(receipt['owned_processes_closed'])
                self.assertEqual(receipt['stdout_sha256'],digest(output));self.assertNotIn('stdout_hex',receipt)
                self.assertEqual(receipt['stdout_bytes'],len(output))
            finally:
                if backend._docker_config is not None:backend._docker_config[0].rmdir()


@unittest.skipUnless(os.geteuid()==0,'root-owned private input authority fixture')
class AuthorityTests(unittest.TestCase):
    def test_closed_authority_does_not_allow_provision_provider_release_or_ready(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as path:
            root=Path(path);root.chmod(0o700)
            def ref(name,value):
                file=root/name;raw=canonical(value);file.write_bytes(raw);file.chmod(0o600)
                return {'path':str(file),'sha256':digest(raw)}
            operation=str(uuid.uuid4());source=digest(Path(p.__file__).read_bytes())
            cfg={'schema_version':1,'kind':'openlegal_initial_pg_pair_inventory','operation_id':operation,'source_sha256':source,
                'seed_inventory':ref('seed',{}),'seed_authority':ref('seed-auth',{}),
                'maintenance':{'phase':p.MAINTENANCE,'name':'future-complete.json','sha256':'d'*64},'provision':ref('preprovision',{})}
            invref=ref('inventory',cfg)
            auth={'schema_version':1,'kind':'openlegal_initial_pg_pair_authority','approved':True,'operation_id':operation,
                'inventory_sha256':invref['sha256'],'source_sha256':source,'seed_inventory_sha256':cfg['seed_inventory']['sha256'],
                'seed_authority_sha256':cfg['seed_authority']['sha256'],'provision_sha256':cfg['provision']['sha256'],
                'valid_until':int(time.time())+1000,'create_pair_permitted':True,'initial_bind_permitted':True,'preserve_originals':True,
                **{k:False for k in ('automatic_retry','provisioning_permitted','provider_permitted','release_permitted',
                                    'ready_permitted','cleanup_permitted','promotion_permitted')}}
            authref=ref('authority',auth)
            def selfpin(path,sha):
                self.assertEqual(Path(path),Path(p.__file__).resolve());self.assertEqual(sha,source)
            with patch.object(p,'private_bytes',side_effect=selfpin),patch.object(p,'_provision'),\
                 patch.object(p.seed,'_load',return_value=({'operation_id':operation,'media_profile':p.blobs.MEDIA_PROFILE},)):
                self.assertEqual(p._load(invref,authref)[0],cfg)
                for key in ('automatic_retry','provisioning_permitted','provider_permitted','release_permitted',
                            'ready_permitted','cleanup_permitted','promotion_permitted'):
                    bad=copy.deepcopy(auth);bad[key]=True
                    with self.subTest(key=key),self.assertRaisesRegex(Refused,'authority_scope'):
                        p._load(invref,ref('authority',bad))
                bad=copy.deepcopy(auth);bad['approved']=1
                with self.assertRaisesRegex(Refused,'authority_scope'):p._load(invref,ref('authority',bad))
                bad=copy.deepcopy(auth);bad['shell']='forbidden'
                with self.assertRaisesRegex(Refused,'pair_authority'):p._load(invref,ref('authority',bad))

    def test_absent_maintenance_does_not_create_a_pair_or_accept_historical_success(self):
        backend=p.driver.Backend();backend._driver_deadline=time.monotonic()+10
        # _live refuses BEFORE considering any disk success file or native port.
        with self.assertRaisesRegex(Refused,'same_live_driver'):
            p._live(backend,None,{'operation_id':str(uuid.uuid4())},({},None,None,None,{}, {},{},b'',b'',b''))
        self.assertFalse(getattr(backend,'_initial_pg_pair_started',False))


if __name__=='__main__':unittest.main(verbosity=2)
