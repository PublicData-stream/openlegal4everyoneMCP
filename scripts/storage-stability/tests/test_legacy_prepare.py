"""Synthetic Docker/Kubernetes/ZFS plus actual harmless /tmp PID/FD fixtures.

Positive ZFS mount views explicitly mock ONLY kernel mountinfo/fdinfo inputs;
descriptor/path/device/inode checks still use real /tmp FDs. No real mount,
daemon, provider, database, Docker, ZFS or network operation is executed.
"""
import copy
import json
import os
from pathlib import Path
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
import legacy_prepare as m
from interface import Refused
from inventory import canonical,digest


class Journal:
    def __init__(self):self.rows={}
    def save(self,name,value):
        if name in self.rows:raise FileExistsError(name)
        self.rows[name]=canonical(value);return digest(self.rows[name])
    def output_hashes(self):return {k:digest(v) for k,v in self.rows.items()}
    def read_phase_bytes(self,phase,name,sha):
        raw=self.rows[name];assert digest(raw)==sha;return raw


@unittest.skipUnless(os.geteuid()==0,'actual root-owned metadata gates')
class LegacyTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='legacy-prepare-test-',dir='/tmp');self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700);self.calls=[];self.mode=None;self.starts=0;self.serial=0
        self.cid=uuid.uuid4().hex*2;self.opid=str(uuid.uuid4());self.boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.guard=Path('/tmp/openlegal-legacy-prepare-primary-'+self.cid+'.lock');self.addCleanup(lambda:self.guard.unlink(missing_ok=True))
        self.datasets=[]
        for i,name in enumerate(sorted(m.migration.SNAPSHOT_DATASETS)):
            mount=self.root/name.split('/')[1];mount.mkdir(mode=0o700)
            self.datasets.append({'name':name,'guid':str(100+i),'pool':'cachepool','mountpoint':str(mount)})
        self.data=self.root/'openlegal-postgres'/'18'/'docker';self.data.mkdir(parents=True,mode=0o700)
        self.data.chmod(0o700);os.chown(self.data,70,70)
        (self.data/'pg_wal').mkdir();(self.data/'pg_tblspc').mkdir()
        self.bin=self.root/'bin';self.bin.mkdir();(self.bin/'pg_controldata').write_bytes(b'synthetic control binary')
        self.cg=self.root/'cgroup';self.cg.mkdir();(self.cg/'cgroup.procs').write_text('')
        self.sock=self.root/'socket';self.sock.mkdir();self.socket=socket.socket(socket.AF_UNIX)
        self.socket.bind(str(self.sock/'.s.PGSQL.5432'));os.chown(self.sock/'.s.PGSQL.5432',70,70);self.addCleanup(self.socket.close)
        self.c={'deadline_secs':60,'expected_system_identifier':'73737777777777','expected_timeline':1,
            'pgdata':{'host_path':str(self.data),'container_path':'/var/lib/postgresql/18/docker',
                'identity':{'dev':self.data.stat().st_dev,'ino':self.data.stat().st_ino}},
            'image':{'reference':'postgres@sha256:'+'a'*64,'inspect_id':'sha256:'+'b'*64,
                'postgres_sha256':digest(Path(sys.executable).resolve().read_bytes()),
                'pg_controldata_sha256':digest((self.bin/'pg_controldata').read_bytes())},
            'migration_manifest':{'path':str(self.root/'manifest.json'),'sha256':'c'*64},'blob_roots':{}}
        for role,parent in (('cache_hdd',self.root/'openlegal-cache-blobs'),
                            ('corpus_hdd',self.root/'openlegal-corpus-blobs'),('corpus_ssd_lookup',self.root)):
            path=parent/role;path.mkdir(mode=0o700);os.chown(path,10004,10004)
            raw=(role+' permanent bytes').encode();sha=digest(raw);shard=path/sha[:2];shard.mkdir(mode=0o700);os.chown(shard,10004,10004)
            file=shard/(sha+'-'+str(uuid.uuid4()));file.write_bytes(raw);file.chmod(0o600);os.chown(file,10004,10004)
            self.c['blob_roots'][role]={'path':str(path),'identity':{'dev':path.stat().st_dev,'ino':path.stat().st_ino}}
        self.expected=[{'version':i,'checksum':format(i,'096x')} for i in range(1,28)]
        Path(self.c['migration_manifest']['path']).write_bytes(canonical(self.expected));Path(self.c['migration_manifest']['path']).chmod(0o600)
        self.c['migration_manifest']['sha256']=digest(canonical(self.expected))
        self.items={};workloads=[];jobs=[]
        for name,replicas in (('serving',1),('scheduler',0)):
            item={'metadata':{'uid':str(uuid.uuid4()),'name':name,'resourceVersion':'1'},
                  'spec':{'replicas':replicas,'template':{'spec':{'containers':[{'name':name,'image':'immutable'}]}}}}
            self.items['deployment',name]=item;workloads.append({'kind':'deployment','name':name,'uid':item['metadata']['uid'],
                'pod_spec_sha256':digest(canonical(item['spec']['template']['spec']))})
        for name,terminal in (('active',False),('already-suspended',False),('history',True)):
            item={'metadata':{'uid':str(uuid.uuid4()),'name':name,'resourceVersion':'1'},
                'spec':{'suspend':name!='active','template':{'spec':{'containers':[{'name':'job','image':'immutable'}]}}}}
            if terminal:item['status']={'conditions':[{'type':'Complete','status':'True'}]}
            self.items['job',name]=item;jobs.append({'name':name,'uid':item['metadata']['uid'],'spec_sha256':digest(canonical(item['spec']))})
        self.history=copy.deepcopy(self.items['job','history'])
        self.journal=Journal();self.op=types.SimpleNamespace(operation_id=self.opid,action='legacy_prepare',phase=m.PHASE,
            authority={'legacy_fence_snapshot_permitted':True},journal=self.journal,seal=lambda:None,
            inventory={'host':{'boot_id':self.boot},'legacy_prepare':self.c,'original_datasets':self.datasets,
                'linux':{'namespace':'synthetic','serving_workload':'serving','workloads':workloads,'jobs':jobs,
                    'binaries':{'zfs':{'sha256':'e'*64}},'postgres':{'primary':{'container_id':self.cid,
                        'image_id':'sha256:'+'d'*64}}},'source_graph':{}})
        self.closed={'cri_containers':[{'id':'old-cri','state':'CONTAINER_EXITED'}],
            'cgroups':[{'path':'/synthetic/writer','dev':1,'ino':2,'pids':[]}]}
        self.state={'tables':{'openlegal.corpus_control':{'rows':1,'sha256':'f'*64},
                             'openlegal.provider_request_budget':{'rows':1,'sha256':'1'*64}},
                    'references':{'rows':3,'sha256':'2'*64},'migration_history':{'rows':27,'sha256':'3'*64}}
        self.sessions={'database':'openlegal','session_user':'openlegal_migration','current_user':'openlegal_migration',
            'postmaster_started_at':'before','role_flags':dict.fromkeys(('superuser','create_role','create_db','replication','bypass_rls'),False),
            'sessions':[]}
        self.policy={'Name':'unless-stopped','MaximumRetryCount':0};self.primary=None;self.children=[];self.start_primary()
        test=self
        class Backend(m.linux.LinuxBackend):
            def _artifact(inner,op,name,value):return op.journal.save(op.phase+'-'+name+'.json',value)
            def _inspect(inner,op,which,**kw):
                if test.mode=='ambiguous-containment' and test.starts:raise Refused('synthetic_identity_ambiguous')
                running=test.primary is not None and test.primary.poll() is None
                return {'id':test.cid,'image':op.inventory['linux']['postgres']['primary']['image_id'],
                    'state':{'Running':running,'Pid':test.primary.pid if running else 0,'ExitCode':0},
                    'mounts':[{'source':str(test.root/'openlegal-postgres'),'destination':'/var/lib/postgresql','type':'bind','rw':True}],
                    'restart_policy':copy.deepcopy(test.policy)}
            def _container_directory(inner,pid,path):
                selected=test.data if path==test.c['pgdata']['container_path'] else (
                    test.data/'pg_wal' if path.endswith('/pg_wal') else test.bin if path=='/usr/local/bin' else test.sock)
                return os.open(selected,os.O_RDONLY|os.O_DIRECTORY)
            def _capture_writer_cgroups(inner,op):pass
            def _cri_rows(inner,op):return copy.deepcopy(test.closed['cri_containers'])
            def _physical_writers_closed(inner,op):return copy.deepcopy(test.closed)
            def _kube(inner,op,args):return test.kube(args)
            def _run(inner,op,binary,args,**kw):return test.command(binary,args,kw)
        self.backend=Backend();self.addCleanup(self.close)
        self.real_scope=m._scope
        self.real_kernel_mountinfo=m._kernel_mountinfo;self.real_fd_mount_id=m._fd_mount_id
        self.host_mountinfo=m._kernel_mountinfo();maximum=max(int(r.split()[0]) for r in self.host_mountinfo.splitlines())
        self.mount_ids={r['name']:maximum+100+i for i,r in enumerate(self.datasets)}
        self.mount_overrides={};self.host_extra_mounts=[];self.native_extra_mounts=[];self.fd_mount_overrides={}
        self.patchers=[patch.object(m,'_scope',return_value=(self.c,self.expected)),
            patch.object(m,'_parent_anchor',return_value={'pid':123,'start_ticks':'1','boot_id':self.boot,'cgroups':[]}),
            patch.object(m.linux,'process_cgroup',return_value=['0::/synthetic/docker-'+self.cid+'.scope']),
            patch.object(m.linux,'members',return_value=[])]
        original=m.root_fd
        self.patchers.append(patch.object(m,'root_fd',side_effect=lambda path,*a,**kw:
            original(self.cg if str(path).startswith('/sys/fs/cgroup/') else path,*a,**kw)))
        self.patchers.extend([patch.object(m,'_kernel_mountinfo',side_effect=self.synthetic_kernel_mountinfo),
                              patch.object(m,'_fd_mount_id',side_effect=self.synthetic_fd_mount_id)])
        for p in self.patchers:p.start();self.addCleanup(p.stop)

    def close(self):
        for child in self.children:
            if child.poll() is None:child.terminate()
            child.wait(timeout=3)
        fds=set(getattr(self.backend,'_legacy_prepare_primary_fds',[])+getattr(self.backend,'_legacy_prepare_blob_fds',[]))
        fds.update(getattr(self.backend,'_legacy_prepare_dataset_fds',{}).values())
        fds.update(getattr(self.backend,'_legacy_prepare_source_fds',{}).values())
        if hasattr(self.backend,'_legacy_prepare_guard_fd'):fds.add(self.backend._legacy_prepare_guard_fd)
        for fd in fds:
            try:os.close(fd)
            except OSError:pass

    def synthetic_kernel_mountinfo(self,pid=None):
        """Explicit synthetic kernel ZFS view, never a real mount operation."""
        def escape(value):
            return str(value).replace('\\','\\134').replace(' ','\\040').replace('\t','\\011').replace('\n','\\012')
        lines=[] if pid is not None else self.host_mountinfo.splitlines()
        for row in self.datasets:
            if pid is not None and row['name']!='cachepool/openlegal-postgres':continue
            dev=Path(row['mountpoint']).stat().st_dev
            values={'id':self.mount_ids[row['name']],'device':str(os.major(dev))+':'+str(os.minor(dev)),
                'root':'/','mountpoint':'/var/lib/postgresql' if pid is not None else row['mountpoint'],
                'filesystem':'zfs','source':row['name']}
            values.update(self.mount_overrides.get(row['name'],{}))
            lines.append(str(values['id'])+' 1 '+values['device']+' '+escape(values['root'])+' '+
                escape(values['mountpoint'])+' rw - '+values['filesystem']+' '+escape(values['source'])+' rw')
        lines.extend(self.native_extra_mounts if pid is not None else self.host_extra_mounts)
        return '\n'.join(lines)+'\n'

    def synthetic_fd_mount_id(self,fd):
        path=os.readlink('/proc/self/fd/'+str(fd))
        if path in self.fd_mount_overrides:return self.fd_mount_overrides[path]
        for row in self.datasets:
            if Path(path).is_relative_to(row['mountpoint']):return self.mount_ids[row['name']]
        return self.real_fd_mount_id(fd)

    def start_primary(self):
        self.primary=subprocess.Popen([sys.executable,'-I','-B','-c','import time; time.sleep(60)'],cwd=self.data,
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);self.children.append(self.primary)

    def kube(self,args):
        if args[2:4]==['get','jobs']:return {'items':copy.deepcopy([v for (k,n),v in self.items.items() if k=='job'])}
        action,kind,name=args[2:5];item=self.items[kind,name]
        if action=='get':return copy.deepcopy(item)
        self.assertEqual(action,'patch');old=copy.deepcopy(item)
        for step in json.loads(args[args.index('-p')+1]):
            parts=[v.replace('~1','/').replace('~0','~') for v in step['path'].split('/')[1:]];node=item
            for part in parts[:-1]:node=node[part]
            if step['op']=='test':self.assertEqual(node[parts[-1]],step['value'])
            else:node[parts[-1]]=step['value']
        item['metadata']['resourceVersion']=str(int(old['metadata']['resourceVersion'])+1)
        return copy.deepcopy(item)

    def command(self,binary,args,kw):
        self.calls.append((binary,copy.deepcopy(args),kw));self.serial+=1
        if binary=='zfs':
            if args[0]=='snapshot':
                if self.mode=='snapshot-unknown':raise RuntimeError('synthetic snapshot ACK unknown')
                evidence={'binary':'zfs','arguments_sha256':digest(canonical(args)),'actual_exit_code':None if self.mode=='snapshot-open' else 0,
                    'started':True,'owned_processes_closed':True,'expired':False,'interrupted':False,
                    'unexpected_descendants':False,'output_limit_exceeded':False}
                self.journal.save(m.PHASE+'-command-'+str(self.serial)+'.json',evidence);return b''
            row=next(r for r in self.datasets if args[-1].split('@')[0]==r['name'])
            if args[3]=='property,value':return ('guid\t'+row['guid']+'\nmountpoint\t'+row['mountpoint']+'\nmounted\tyes\n').encode()
            return ((str(int(row['guid'])+1000) if '@' in args[-1] else row['guid'])+'\n').encode()
        self.assertEqual(binary,'docker')
        if args[:2]==['image','inspect']:
            return canonical({'Id':self.c['image']['inspect_id'],'Os':'linux',
                'Architecture':{'x86_64':'amd64','aarch64':'arm64'}.get(m.platform.machine())})
        if args[0]=='update':self.policy={'Name':'no','MaximumRetryCount':0};return (self.cid+'\n').encode()
        if args[0]=='stop':
            self.primary.terminate();self.primary.wait(timeout=3)
            return (self.cid+'\n').encode()
        if args[0]=='wait':return b'0\n'
        if args[0]=='start':
            self.starts+=1;self.start_primary()
            if self.mode in ('start-unknown','ambiguous-containment'):raise RuntimeError('synthetic start ACK unknown')
            return (self.cid+'\n').encode()
        if '/usr/local/bin/pg_controldata' in args:
            return ('Database system identifier: '+self.c['expected_system_identifier']+"\nLatest checkpoint's TimeLineID: 1\n").encode()
        if '/bin/sh' in args:return b''
        sql=kw['stdin'].decode()
        if m.migration.SESSIONS_SQL in sql:
            value=copy.deepcopy(self.sessions);value['postmaster_started_at']='after' if self.starts else 'before'
            if self.mode=='sessions' or self.mode=='post-sessions' and self.starts:
                value['sessions']=[{'pid':2,'backend_start':'x','user':'other','database':'openlegal'}]
            return canonical(value)
        if m.migration.MIGRATIONS_SQL in sql:return canonical([dict(r,success=True) for r in self.expected])
        if m.migration.OLD_MIGRATIONS_SQL in sql:return canonical(self.state['migration_history'])
        if m.migration.TABLES_SQL in sql:return canonical(sorted(self.state['tables']))
        if m.migration.REFERENCES_SQL in sql:
            value=copy.deepcopy(self.state['references'])
            if self.mode=='changed-reference' and self.starts:value['rows']+=1
            return canonical(value)
        return canonical(next(v for k,v in self.state['tables'].items() if k+' t)' in sql))

    def test_complete_payload_waits_for_parent_exit_and_keeps_applications_fenced(self):
        result=m.run(self.backend,self.op)
        self.assertEqual(result['status'],'PENDING_PARENT_PRODUCER_OS_EXIT');self.assertFalse(result['ready'])
        self.assertEqual(result['producer_os_exit'],'NOT_YET_OBSERVABLE');self.assertFalse(result['provider_state_modified'])
        self.assertEqual(result['payloads']['preservation'],self.state);self.assertTrue(self.guard.exists())
        self.assertEqual(self.policy,{'Name':'no','MaximumRetryCount':0});self.assertEqual(self.starts,1)
        self.assertEqual(self.items['job','history'],self.history)
        for kind,name in (('deployment','scheduler'),('job','already-suspended')):
            self.assertEqual(self.items[kind,name]['metadata']['resourceVersion'],'2')
            self.assertTrue(self.items[kind,name]['metadata']['annotations']['openlegal4everyone.stream/operation-fence'].startswith(self.opid+'.'))
        snapshots=[args for binary,args,kw in self.calls if binary=='zfs' and args[0]=='snapshot']
        self.assertEqual(len(snapshots),1);self.assertEqual(len(snapshots[0]),8)
        self.assertEqual({v.split('@')[0] for v in snapshots[0][1:]},m.migration.SNAPSHOT_DATASETS)
        self.assertEqual(len(self.backend._legacy_prepare_blob_fds),9)
        self.assertEqual(len(self.backend._legacy_prepare_primary_fds),6)
        self.assertNotEqual(result['fresh_primary']['primary_process']['pid'],self.children[0].pid)
        commands=[args[0] for _,args,_ in self.calls];self.assertLess(commands.index('update'),commands.index('stop'))
        self.assertEqual(commands.count('stop'),1);self.assertNotIn('rm',commands)

    def test_existing_global_sql_session_refuses_before_postgres_policy_or_stop(self):
        self.mode='sessions'
        with self.assertRaisesRegex(Refused,'global_sql_not_closed'):m.run(self.backend,self.op)
        self.assertFalse(any(args[0] in ('update','stop','start','snapshot') for _,args,_ in self.calls));self.assertTrue(self.guard.exists())

    def test_unknown_snapshot_preserves_guard_and_primary_stopped_without_restart(self):
        self.mode='snapshot-unknown'
        with self.assertRaisesRegex(RuntimeError,'snapshot ACK unknown'):m.run(self.backend,self.op)
        self.assertEqual(self.starts,0);self.assertIsNotNone(self.primary.poll());self.assertTrue(self.guard.exists())
        self.assertEqual(sum(args[0]=='snapshot' for _,args,_ in self.calls),1)

    def test_unproven_snapshot_process_exit_refuses_before_restart(self):
        self.mode='snapshot-open'
        with self.assertRaisesRegex(Refused,'snapshot_command_unclosed'):m.run(self.backend,self.op)
        self.assertEqual(self.starts,0)

    def test_unknown_start_ack_gets_one_qualified_stop_only_containment(self):
        self.mode='start-unknown'
        with self.assertRaisesRegex(RuntimeError,'start ACK unknown'):m.run(self.backend,self.op)
        self.assertEqual(self.starts,1);self.assertIsNotNone(self.primary.poll())
        self.assertEqual(sum(args[0]=='stop' for _,args,_ in self.calls),2)
        failure=json.loads(self.journal.rows[m.PHASE+'-failed.json']);self.assertEqual(failure['containment']['status'],'closed')
        self.assertIn(m.PHASE+'-containment-stop-intent.json',self.journal.rows)

    def test_ambiguous_restart_identity_does_not_dispatch_containment_stop(self):
        self.mode='ambiguous-containment'
        with self.assertRaisesRegex(RuntimeError,'start ACK unknown'):m.run(self.backend,self.op)
        self.assertEqual(sum(args[0]=='stop' for _,args,_ in self.calls),1)
        failure=json.loads(self.journal.rows[m.PHASE+'-failed.json']);self.assertEqual(failure['containment']['status'],'UNKNOWN')

    def test_any_postrestart_session_or_reference_change_stops_same_primary_once(self):
        self.mode='changed-reference'
        with self.assertRaisesRegex(Refused,'old_rows_or_references_changed'):m.run(self.backend,self.op)
        self.assertIsNotNone(self.primary.poll());self.assertEqual(self.starts,1)
        self.assertEqual(sum(args[0]=='stop' for _,args,_ in self.calls),2)

    def test_postrestart_sql_session_never_uses_selective_sql_cleanup(self):
        self.mode='post-sessions'
        with self.assertRaisesRegex(Refused,'global_sql_not_closed'):m.run(self.backend,self.op)
        raw=b''.join(kw.get('stdin',b'') for _,_,kw in self.calls)
        self.assertNotIn(b'pg_terminate_backend',raw);self.assertNotIn(b'pg_cancel_backend',raw)
        self.assertEqual(self.starts,1);self.assertIsNotNone(self.primary.poll())

    def test_guard_blocks_fresh_uuid_attempt_before_any_cas_or_command(self):
        m.run(self.backend,self.op);calls=len(self.calls);fds=list(self.backend._legacy_prepare_primary_fds)
        self.op.operation_id=str(uuid.uuid4())
        with self.assertRaises(FileExistsError):m.run(self.backend,self.op)
        self.assertEqual(len(self.calls),calls)
        self.assertEqual(self.backend._legacy_prepare_primary_fds,fds)
        for fd in fds:self.assertTrue(os.fstat(fd))

    def test_external_wal_and_tablespace_are_refused_before_stop(self):
        (self.data/'pg_wal').rmdir();(self.data/'pg_wal').symlink_to(self.root,target_is_directory=True)
        with self.assertRaisesRegex(Refused,'external_wal'):m.run(self.backend,self.op)
        self.assertFalse(any(args[0]=='stop' for _,args,_ in self.calls))

    def test_external_tablespace_link_is_retained_and_refused(self):
        link=self.data/'pg_tblspc'/'123';link.symlink_to(self.root,target_is_directory=True)
        with self.assertRaisesRegex(Refused,'external_tablespace'):m.run(self.backend,self.op)
        self.assertTrue(link.is_symlink());self.assertFalse(any(args[0]=='stop' for _,args,_ in self.calls))

    def test_remaining_native_cgroup_member_prevents_snapshot_and_restart(self):
        with patch.object(m.linux,'members',return_value=[424242]):
            with self.assertRaisesRegex(Refused,'primary_cgroup_not_closed'):m.run(self.backend,self.op)
        self.assertEqual(self.starts,0);self.assertFalse(any(args[0]=='snapshot' for _,args,_ in self.calls))

    def test_restart_policy_reopening_before_snapshot_is_not_accepted(self):
        original=self.command
        def command(binary,args,kw):
            value=original(binary,args,kw)
            if args[0]=='wait':self.policy={'Name':'unless-stopped','MaximumRetryCount':0}
            return value
        self.command=command
        with self.assertRaisesRegex(Refused,'stopped_restart_policy'):m.run(self.backend,self.op)
        self.assertEqual(self.starts,0);self.assertFalse(any(args[0]=='snapshot' for _,args,_ in self.calls))

    def test_native_cwd_and_binary_and_image_domain_are_real_checks(self):
        self.primary.terminate();self.primary.wait(timeout=3)
        self.primary=subprocess.Popen([sys.executable,'-I','-B','-c','import time; time.sleep(60)'],cwd=self.root)
        self.children.append(self.primary)
        with self.assertRaisesRegex(Refused,'effective_pgdata_cwd'):m._native_primary(self.backend,self.op,self.c)
        self.primary.terminate();self.primary.wait(timeout=3);self.start_primary()
        self.c['image']['postgres_sha256']='0'*64
        with self.assertRaisesRegex(Refused,'native_binary_changed'):m._native_primary(self.backend,self.op,self.c)

    def test_actual_blob_bytes_change_detected_without_repair_or_delete(self):
        before=m._blob_state(self.backend,self.op,self.c,time.monotonic()+10,'first')
        root=Path(self.c['blob_roots']['corpus_ssd_lookup']['path']);file=next(p for p in root.rglob('*') if p.is_file())
        file.write_bytes(b'changed')
        with self.assertRaisesRegex(Refused,'blob_digest'):m._blob_state(self.backend,self.op,self.c,time.monotonic()+10,'second')
        self.assertEqual(file.read_bytes(),b'changed');self.assertTrue(before['corpus_ssd_lookup']['permanent'])

    def test_unknown_nonterminal_job_refuses_before_deployment_cas(self):
        self.items['job','foreign']={'metadata':{'uid':str(uuid.uuid4()),'name':'foreign','resourceVersion':'1'},'spec':{}}
        with self.assertRaisesRegex(Refused,'unknown_nonterminal_job'):m.run(self.backend,self.op)
        self.assertFalse(any(args[0]=='stop' for _,args,_ in self.calls))
        self.assertEqual(self.items['deployment','serving']['metadata']['resourceVersion'],'1')

    def test_changed_job_spec_after_cas_is_not_adopted_into_fence(self):
        original=self.backend._physical_writers_closed
        def closed(op):
            self.items['job','active']['spec']['backoffLimit']=999
            return original(op)
        self.backend._physical_writers_closed=closed
        with self.assertRaisesRegex(Refused,'job_fence_changed'):m.run(self.backend,self.op)
        self.assertFalse(any(args[0]=='update' for _,args,_ in self.calls))

    def test_status_only_resource_revision_is_allowed_with_same_spec_and_nonce(self):
        original=self.backend._physical_writers_closed;done=False
        def closed(op):
            nonlocal done
            if not done:
                item=self.items['deployment','serving'];item['metadata']['resourceVersion']='3'
                item['status']={'readyReplicas':0};done=True
            return original(op)
        self.backend._physical_writers_closed=closed
        result=m.run(self.backend,self.op)
        serving=next(r for r in result['payloads']['drain']['resources'] if r['name']=='serving')
        self.assertEqual(serving['resource_version'],'3')

    def test_all_three_blob_roots_and_manifest_checksums_are_required(self):
        # A protected temporary installation exercises real source/hash checks.
        install=self.root/'installed';install.mkdir(mode=0o700)
        names=('legacy_prepare.py','initial_migration.py','linux_backend.py','command_process.py',
            'interface.py','inventory.py','manual_backend.py','bootstrap_backend.py','copy_tree.py')
        for name in names:
            raw=(BASE/name).read_bytes();path=install/name;path.write_bytes(raw);path.chmod(0o600)
            self.op.inventory['source_graph'][name]=digest(raw)
        with patch.object(m,'__file__',str(install/'legacy_prepare.py')):
            config,rows=self.real_scope(self.backend,self.op)
            self.assertEqual(rows,self.expected);self.assertIs(config,self.c)
            self.op.inventory['original_datasets'].append(copy.deepcopy(self.datasets[0]))
            with self.assertRaisesRegex(Refused,'exact_topology'):self.real_scope(self.backend,self.op)
            self.op.inventory['original_datasets'].pop()
            self.c['blob_roots'].pop('corpus_ssd_lookup')
            with self.assertRaisesRegex(Refused,'exact_topology'):self.real_scope(self.backend,self.op)

    def test_real_source_or_migration_manifest_pin_change_refuses_scope(self):
        # Manifest mutation is rejected by the native private-file hash reader,
        # before source installation paths or any command are consulted.
        Path(self.c['migration_manifest']['path']).write_bytes(b'[]\n')
        with self.assertRaisesRegex(Refused,'hash|changed'):self.real_scope(self.backend,self.op)
        self.assertEqual(self.calls,[])

    def test_unrecognized_native_image_identity_domain_is_refused(self):
        self.c['image']['inspect_id']='sha256:'+'0'*64
        original=self.command
        def command(binary,args,kw):
            if args[:2]==['image','inspect']:
                return canonical({'Id':'sha256:'+'b'*64,'Os':'linux','Architecture':'amd64'})
            return original(binary,args,kw)
        self.command=command
        with self.assertRaisesRegex(Refused,'native_image_domain'):m.run(self.backend,self.op)
        self.assertFalse(any(args[0]=='stop' for _,args,_ in self.calls))

    def dataset_native(self):
        native=m._native_primary(self.backend,self.op,self.c)
        for key in ('data_fd','wal_fd','cgroup_fd'):self.addCleanup(os.close,native[key])
        return native

    def nested_mount_line(self,path,*,source='cachepool/openlegal-postgres',root='/bound-subtree'):
        dev=self.root.stat().st_dev
        return str(max(self.mount_ids.values())+1000)+' 1 '+str(os.major(dev))+':'+str(os.minor(dev))+\
            ' '+root+' '+str(path)+' rw - zfs '+source+' rw'

    def test_actual_non_zfs_kernel_mount_refused_despite_matching_logical_zfs_get(self):
        native=self.dataset_native()
        with patch.object(m,'_kernel_mountinfo',side_effect=self.real_kernel_mountinfo),\
             patch.object(m,'_fd_mount_id',side_effect=self.real_fd_mount_id):
            with self.assertRaisesRegex(Refused,'dataset_kernel_mount'):
                m._datasets(self.backend,self.op,self.c,time.monotonic()+10,native)
        self.assertFalse(any(args[0]=='snapshot' for _,args,_ in self.calls))

    def test_same_device_wrong_filesystem_source_root_or_mountpoint_refused(self):
        native=self.dataset_native();first=self.datasets[0]
        for override in ({'filesystem':'ext4'},{'source':'cachepool/unselected'},
                         {'root':'/same-device-subtree'},{'mountpoint':str(self.root)}):
            with self.subTest(override=override):
                self.mount_overrides[first['name']]=override
                with self.assertRaisesRegex(Refused,'dataset_kernel_mount'):
                    m._datasets(self.backend,self.op,self.c,time.monotonic()+10,native)
        self.mount_overrides.clear()

    def test_host_pgdata_and_wal_require_selected_dataset_mount_id(self):
        native=self.dataset_native()
        for path in (self.data,self.data/'pg_wal'):
            with self.subTest(path=path):
                self.fd_mount_overrides[str(path)]=self.real_fd_mount_id(native['data_fd'])
                with self.assertRaisesRegex(Refused,'source_not_in_snapshot'):
                    m._datasets(self.backend,self.op,self.c,time.monotonic()+10,native)
                self.fd_mount_overrides.clear()

    def test_host_blob_root_requires_selected_dataset_mount_id(self):
        native=self.dataset_native()
        for role in ('cache_hdd','corpus_hdd'):
            with self.subTest(role=role):
                path=self.c['blob_roots'][role]['path'];fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
                try:self.fd_mount_overrides[path]=self.real_fd_mount_id(fd)
                finally:os.close(fd)
                with self.assertRaisesRegex(Refused,'source_not_in_snapshot'):
                    m._datasets(self.backend,self.op,self.c,time.monotonic()+10,native)
                self.fd_mount_overrides.clear()

    def test_nested_same_device_mount_in_any_original_dataset_refuses(self):
        native=self.dataset_native()
        for row in self.datasets:
            with self.subTest(dataset=row['name']):
                self.host_extra_mounts=[self.nested_mount_line(Path(row['mountpoint'])/'hidden',source=row['name'])]
                with self.assertRaisesRegex(Refused,'nested_mount_unsupported'):
                    m._datasets(self.backend,self.op,self.c,time.monotonic()+10,native)
        self.host_extra_mounts=[]

    def test_nested_same_device_blob_shard_mount_refuses_byte_scan(self):
        for role in sorted(m.ROLES):
            with self.subTest(role=role):
                root=Path(self.c['blob_roots'][role]['path']);shard=next(root.iterdir())
                self.host_extra_mounts=[self.nested_mount_line(shard)]
                with self.assertRaisesRegex(Refused,'nested_mount_unsupported'):
                    m._blob_state(self.backend,self.op,self.c,time.monotonic()+10,'nested-'+role)
        self.host_extra_mounts=[]

    def test_native_namespace_nested_pgdata_or_wal_bind_mount_refuses(self):
        for tail in ('base','pg_wal/hidden'):
            with self.subTest(path=tail):
                self.native_extra_mounts=[self.nested_mount_line(Path(self.c['pgdata']['container_path'])/tail)]
                with self.assertRaisesRegex(Refused,'nested_mount_unsupported'):
                    m._native_primary(self.backend,self.op,self.c)
        self.native_extra_mounts=[]

    def test_native_namespace_mount_ids_may_differ_from_host_dataset_ids(self):
        # Same real inode correspondence remains required by _native_primary;
        # this namespace-local checker never compares a native ID to a host ID.
        data=os.open(self.data,os.O_RDONLY|os.O_DIRECTORY);wal=os.open(self.data/'pg_wal',os.O_RDONLY|os.O_DIRECTORY)
        self.addCleanup(os.close,data);self.addCleanup(os.close,wal)
        foreign=max(self.mount_ids.values())+5000
        dev=os.fstat(data).st_dev
        raw=str(foreign)+' 1 '+str(os.major(dev))+':'+str(os.minor(dev))+\
            ' / /var/lib/postgresql rw - zfs cachepool/openlegal-postgres rw\n'
        with patch.object(m,'_kernel_mountinfo',return_value=raw),patch.object(m,'_fd_mount_id',return_value=foreign):
            m._native_mounts(self.primary.pid,self.c['pgdata']['container_path'],data,wal)

    def test_mount_binding_revalidated_after_long_blob_scan(self):
        original=m.blobs.blob_inventory
        def inventory(path,**kwargs):
            result=original(path,**kwargs)
            self.mount_overrides['cachepool/openlegal-cache-blobs']={'source':'cachepool/unselected'}
            return result
        with patch.object(m.blobs,'blob_inventory',side_effect=inventory):
            with self.assertRaisesRegex(Refused,'blob_mount_changed'):
                m._blob_state(self.backend,self.op,self.c,time.monotonic()+10,'changed-mount')
        self.assertFalse(any(args[0]=='stop' for _,args,_ in self.calls))

    def test_kernel_mount_rechecked_immediately_before_atomic_snapshot(self):
        original=self.backend._artifact
        def artifact(op,name,value):
            result=original(op,name,value)
            if name=='atomic-snapshot-intent':
                self.mount_overrides['cachepool/openlegal-postgres']={'source':'cachepool/unselected'}
            return result
        self.backend._artifact=artifact
        with self.assertRaisesRegex(Refused,'dataset_kernel_mount'):m.run(self.backend,self.op)
        self.assertEqual(self.starts,0);self.assertIsNotNone(self.primary.poll())
        self.assertFalse(any(args[0]=='snapshot' for _,args,_ in self.calls))

    def test_held_root_path_replacement_is_not_reopened_and_adopted(self):
        native=self.dataset_native();m._datasets(self.backend,self.op,self.c,time.monotonic()+10,native)
        path=Path(self.datasets[0]['mountpoint']);path.rename(path.with_name(path.name+'-old'));path.mkdir()
        with self.assertRaisesRegex(Refused,'mount_path_rebound'):
            m._datasets(self.backend,self.op,self.c,time.monotonic()+10,native)

    def test_same_inode_fresh_path_with_different_mount_id_is_refused(self):
        native=self.dataset_native();m._datasets(self.backend,self.op,self.c,time.monotonic()+10,native)
        row=self.datasets[0];held=self.backend._legacy_prepare_dataset_fds[row['name']]
        original=self.synthetic_fd_mount_id
        def changed(fd):
            if fd!=held and os.readlink('/proc/self/fd/'+str(fd))==row['mountpoint']:
                return self.real_fd_mount_id(fd)
            return original(fd)
        with patch.object(m,'_fd_mount_id',side_effect=changed):
            with self.assertRaisesRegex(Refused,'mount_path_rebound'):
                m._datasets(self.backend,self.op,self.c,time.monotonic()+10,native)

    def test_native_namespace_rechecked_after_long_scan_before_stop(self):
        original=self.backend._artifact
        def artifact(op,name,value):
            result=original(op,name,value)
            if name=='restart-policy-intent':
                self.native_extra_mounts=[self.nested_mount_line(Path(self.c['pgdata']['container_path'])/'late-bind')]
            return result
        self.backend._artifact=artifact
        with self.assertRaisesRegex(Refused,'nested_mount_unsupported'):m.run(self.backend,self.op)
        self.assertFalse(any(args[0]=='stop' for _,args,_ in self.calls));self.assertEqual(self.starts,0)

    def test_native_namespace_read_requires_same_postmaster_lifetime_after_read(self):
        native=self.dataset_native();original=self.synthetic_kernel_mountinfo
        def mounted(pid=None):
            result=original(pid)
            if pid is not None:
                self.primary.terminate();self.primary.wait(timeout=3)
            return result
        with patch.object(m,'_kernel_mountinfo',side_effect=mounted):
            with self.assertRaisesRegex(Refused,'native_mount_process_changed'):
                m._recheck_native_mounts(self.backend,self.op,self.c,native,time.monotonic()+10)

    def test_parent_fd_anchor_requires_actual_inherited_descriptor(self):
        # A harmless child checks that the ROOT parent still has the exact FD.
        fd=os.open(self.cg,os.O_RDONLY|os.O_DIRECTORY);self.addCleanup(os.close,fd)
        info=os.fstat(fd);source=('import os,sys,types; sys.path.insert(0,'+repr(str(BASE))+'); '
            'import legacy_prepare as m; b=types.SimpleNamespace(_cgroup_fds={"x":('+str(fd)+','+
            repr({'path':str(self.cg),'dev':info.st_dev,'ino':info.st_ino})+')}); '
            'op=types.SimpleNamespace(inventory={"host":{"boot_id":'+repr(self.boot)+'}}); '
            'assert m._parent_anchor(b,op)["pid"]==os.getppid()')
        result=subprocess.run([sys.executable,'-I','-B','-c',source],pass_fds=(fd,),capture_output=True,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr.decode())
        # Child's independent directory FD number is absent from the parent.
        source=source.replace('b=types.SimpleNamespace(', 'fd=os.open('+repr(str(self.cg))+',os.O_RDONLY|os.O_DIRECTORY); b=types.SimpleNamespace(')
        source=source.replace('"x":('+str(fd)+',','"x":(fd,')
        result=subprocess.run([sys.executable,'-I','-B','-c',source],capture_output=True,timeout=5)
        self.assertNotEqual(result.returncode,0)


if __name__=='__main__':unittest.main()
