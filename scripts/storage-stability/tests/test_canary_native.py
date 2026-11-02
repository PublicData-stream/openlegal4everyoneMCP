"""Synthetic metadata/proc fixtures ONLY. No Kubernetes/CRI/Docker/HTTP calls."""
import copy
import json
import sys
import tempfile
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import canary_native as n
from interface import Refused
from inventory import canonical,digest


def uid():return str(uuid.uuid4())
CID='a'*64
IMAGE='registry.invalid/server@sha256:'+'e'*64
RUNTIME='sha256:'+'e'*64


def fixture():
    server,scheduler,rs,pod,cm=uid(),uid(),uid(),uid(),uid()
    spec={'automountServiceAccountToken':False,'containers':[{'name':'server','image':IMAGE,'args':['/etc/openlegal/server.toml']}],
          'restartPolicy':'Always','serviceAccountName':'openlegal-serving'}
    sched=copy.deepcopy(spec);sched['containers'][0]['args']=['--collection-scheduler','/etc/openlegal/server.toml']
    def deployment(name,ident,template,replicas):return {'metadata':{'name':name,'uid':ident},'spec':{'replicas':replicas,'template':{'spec':template}}}
    owner=lambda kind,ident:{'kind':kind,'uid':ident,'controller':True}
    serving={'metadata':{'uid':pod,'ownerReferences':[owner('ReplicaSet',rs)]},'spec':copy.deepcopy(spec),
        'status':{'phase':'Running','conditions':[{'type':'Ready','status':'True'}],'containerStatuses':[{
            'name':'server','imageID':RUNTIME,'containerID':'containerd://'+CID,'restartCount':0,'ready':True,
            'state':{'running':{'startedAt':'2026-11-02T00:00:00Z'}}}]}}
    deployments={'server':deployment('server',server,spec,1),'scheduler':deployment('scheduler',scheduler,sched,0)}
    rsobj={'metadata':{'uid':rs,'ownerReferences':[owner('Deployment',server)]},'spec':{'template':{'spec':copy.deepcopy(spec)}}}
    config={'namespace':'openlegal-serving','serving_workload':'server','cri_endpoint':'unix:///synthetic',
        'serving_runtime_image_id':RUNTIME,'workloads':[{'name':name,'uid':d['metadata']['uid'],'kind':'deployment',
        'pod_spec_sha256':digest(canonical(d['spec']['template']['spec']))}for name,d in deployments.items()],
        'jobs':[],'config_maps':[{'name':'config','uid':cm,'data_sha256':digest(canonical({'setting':'metadata-only'}))}]}
    policy={'allowed_active_guard_keys':['serve'],'workload_pins':[{'kind':'Deployment','name':'server','uid':server,'role':'serve','runtime_image_id':RUNTIME}],
        'collection_job_pins':[]}
    op=types.SimpleNamespace(canary_policy=policy,watch_stage='before_activation',inventory={'linux':config,'host':{'node_name':'synthetic-node'}},seal=lambda:None)
    process={'key':'serve','operation_id':uid(),'pid':1,'process_start_ticks':100,'state':'active_unclean','record_sha256':'c'*64,'locked':True}
    class Backend:
        _sequence=1
        def __init__(self):
            self.deployments=deployments;self.rs=[rsobj];self.pods=[serving];self.jobs={};self.guards=[process];self.commands=[];self.artifacts={}
            self.cri={CID:{'status':{'id':CID,'state':'CONTAINER_RUNNING','imageRef':RUNTIME,
                'labels':{'io.kubernetes.pod.uid':pod,'io.kubernetes.pod.namespace':config['namespace']}},
                'info':{'pid':10001,'config':{'envs':[{'key':'SECRET','value':'never-export-secret'}]}}}}
            self.running=[{'id':CID,'state':'CONTAINER_RUNNING','labels':{'io.kubernetes.pod.uid':pod,'io.kubernetes.pod.namespace':config['namespace']}}]
        def _kube(self,op,args):
            kind=args[3];name=args[4]if args[4]!='-o'else None
            if kind=='deployment':return copy.deepcopy(self.deployments[name])
            if kind=='configmap':return {'metadata':{'uid':cm},'data':{'setting':'metadata-only'}}
            if kind=='replicasets':return {'items':copy.deepcopy(self.rs)}
            if kind=='pods':return {'items':copy.deepcopy(self.pods)}
            if kind=='job':return copy.deepcopy(self.jobs[name])
            if kind=='jobs':return {'items':copy.deepcopy(list(self.jobs.values()))}
            raise AssertionError('no other API')
        def _lookup_native(self,op,**kw):
            self.allowed=kw['allowed_active_guard_keys']
            return {'pods':[{'uid':pod,'configured_image':IMAGE,'runtime_image_id':RUNTIME,'ready':True,'restarts':0,'epoch':'pinned-epoch'}],
                    'endpoints':[pod],'health':{'status':200},'runtime_guard':{'state':'active'}}
        def _runtime_guards(self,*args,**kw):return {'processes':copy.deepcopy(self.guards),'records':[],'guard':{'state':'active'}}
        def _run(self,op,binary,args,**kw):
            self.commands.append((binary,args,kw));assert binary=='crictl'and kw['record']is False
            if args[2]=='inspect':return json.dumps(self.cri[args[-1]]).encode()
            assert args[2:]==['ps','-a','-o','json'];return json.dumps({'containers':self.running}).encode()
        def _artifact(self,op,name,value):self.artifacts[name]=copy.deepcopy(value)
    backend=Backend()
    return op,backend


def proc(pid,cid):return {'pid':pid,'namespace_pid':1,'process_start_ticks':100,
    'cgroup':{'path':'/sys/fs/cgroup/synthetic/'+cid,'dev':1,'ino':2,'group':'/synthetic/'+cid}}


class NativeTests(unittest.TestCase):
    def setUp(self):self.op,self.b=fixture()
    def observe(self):
        with patch.object(n,'_proc',side_effect=proc):return n.observe(self.b,self.op)

    def test_serving_stable_identity_and_no_env_or_running_closure_claim(self):
        value=self.observe();row=value['pods'][0]
        self.assertEqual(row['container_id'],'containerd://'+CID);self.assertEqual(row['process_start_ticks'],100)
        self.assertEqual(row['started_at'],'2026-11-02T00:00:00Z');self.assertEqual(self.b.allowed,('serve',))
        self.assertNotIn('never-export-secret',repr(value));self.assertEqual(set(value),{'pods','endpoints','health','runtime_guard','actors','guard_proof'})
        self.assertTrue(all(kw['record']is False for _,_,kw in self.b.commands))

    def test_inactive_historical_replicaset_and_terminal_pod_preserved_ignored(self):
        old=copy.deepcopy(self.b.rs[0]);old['metadata']['uid']=uid();old['spec']['template']['spec']['containers'][0]['image']='old'
        self.b.rs.append(old);self.b.pods.append({'metadata':{'uid':uid()},'status':{'phase':'Succeeded'}})
        self.assertEqual(len(self.observe()['pods']),1)

    def test_actual_active_replicaset_template_mutation_refuses(self):
        self.b.rs[0]['spec']['template']['spec']['containers'][0]['args']=['different']
        with self.assertRaisesRegex(Refused,'replicaset_template'):self.observe()

    def test_unapproved_pod_unknown_owner_and_second_live_role_refuse(self):
        for change in ('unknown','duplicate','scheduler'):
            op,b=fixture();self.op,self.b=op,b;pod=copy.deepcopy(b.pods[0]);pod['metadata']['uid']=uid()
            if change=='unknown':pod['metadata']['ownerReferences'][0]['uid']=uid()
            elif change=='scheduler':
                scheduler=b.deployments['scheduler']['metadata']['uid'];b.rs[0]['metadata']['ownerReferences'][0]['uid']=scheduler
                b.rs[0]['spec']['template']['spec']=copy.deepcopy(b.deployments['scheduler']['spec']['template']['spec'])
            b.pods.append(pod)
            with self.subTest(change=change),self.assertRaises(Refused):self.observe()

    def test_workload_uid_replica_template_config_image_restart_known_failures(self):
        for change in ('uid','replica','template','image','restart','unready'):
            self.op,self.b=fixture()
            if change=='uid':self.b.deployments['server']['metadata']['uid']=uid()
            elif change=='replica':self.b.deployments['scheduler']['spec']['replicas']=1
            elif change=='template':self.b.deployments['server']['spec']['template']['spec']['containers'][0]['args']=['other']
            elif change=='image':self.b.pods[0]['spec']['containers'][0]['image']='wrong'
            elif change=='restart':self.b.pods[0]['status']['containerStatuses'][0]['restartCount']=1
            else:self.b.pods[0]['status']['containerStatuses'][0]['ready']=False
            with self.subTest(change=change),self.assertRaises(Refused):self.observe()

    def test_cri_pod_namespace_image_pid_or_guard_mismatch_refuse(self):
        for change in ('pod','namespace','image','pid','guard'):
            self.op,self.b=fixture()
            if change=='pod':self.b.cri[CID]['status']['labels']['io.kubernetes.pod.uid']=uid()
            elif change=='namespace':self.b.cri[CID]['status']['labels']['io.kubernetes.pod.namespace']='other'
            elif change=='image':self.b.cri[CID]['status']['imageRef']='sha256:'+'f'*64
            elif change=='pid':self.b.cri[CID]['info']['pid']=0
            else:self.b.guards[0]['pid']=2
            with self.subTest(change=change),self.assertRaises(Refused):self.observe()

    def test_unknown_global_running_cri_even_with_terminal_pod_refuses(self):
        self.b.running.append({'id':'d'*64,'state':'CONTAINER_RUNNING','labels':{'io.kubernetes.pod.uid':uid(),'io.kubernetes.pod.namespace':'openlegal-serving'}})
        with self.assertRaisesRegex(Refused,'unapproved_running_cri'):self.observe()

    def test_guard_actor_bijection_rejects_unmatched_or_missing_serving(self):
        for change in ('extra','missing'):
            self.op,self.b=fixture()
            if change=='extra':self.b.guards.append(dict(self.b.guards[0],key='scheduler'))
            else:self.b.guards=[]
            with self.subTest(change=change),self.assertRaises(Refused):self.observe()

    def add_job(self, *, phase=None, running=False):
        request,jobid,jobpod=uid(),uid(),uid();epoch=1793589000;key='collection.'+request+'.'+str(epoch)
        template=copy.deepcopy(self.b.deployments['server']['spec']['template']['spec'])
        template['containers'][0]['name']='collector';template['containers'][0]['args']=['--collection-job',request+'@'+str(epoch),'/etc/openlegal/server.toml']
        template['restartPolicy']='Never'
        job={'metadata':{'name':'openlegal-request-'+request.replace('-','')+'-'+str(epoch),'uid':jobid},
             'spec':{'suspend':True,'backoffLimit':0,'template':{'spec':template}},'status':{}}
        spec=copy.deepcopy(job['spec']);spec.pop('suspend')
        pin={'name':job['metadata']['name'],'uid':jobid,'request_id':request,'epoch':epoch,'spec_sha256':digest(canonical(spec)),'runtime_image_id':RUNTIME}
        self.op.canary_policy['allowed_active_guard_keys'].append(key);self.op.canary_policy['collection_job_pins'].append(pin)
        self.b.jobs[pin['name']]=job
        if phase is not None:
            self.op.watch_stage='observe';job['spec']['suspend']=False
            pod={'metadata':{'uid':jobpod,'ownerReferences':[{'kind':'Job','uid':jobid,'controller':True}]},'spec':copy.deepcopy(template),'status':{'phase':phase}}
            self.b.pods.append(pod)
            if running:
                cid='b'*64;pod['status']['containerStatuses']=[{'name':'collector','imageID':RUNTIME,'containerID':'containerd://'+cid,'restartCount':0,'ready':True,'state':{'running':{'startedAt':'2026-11-02T00:00:01Z'}}}]
                self.b.cri[cid]=copy.deepcopy(self.b.cri[CID]);self.b.cri[cid]['status']['id']=cid;self.b.cri[cid]['status']['labels']['io.kubernetes.pod.uid']=jobpod
                self.b.running.append({'id':cid,'state':'CONTAINER_RUNNING','labels':{'io.kubernetes.pod.uid':jobpod,'io.kubernetes.pod.namespace':'openlegal-serving'}})
                self.b.guards.append(dict(self.b.guards[0],key=key))
        return pin,job,key

    def test_finite_prepared_pending_and_completed_jobs_are_lifecycle_not_closure(self):
        pin,job,key=self.add_job(phase='Pending');value=self.observe()
        podrows=[r for r in value['actors']if'pod_uid'in r]
        self.assertEqual(podrows[0]['lifecycle'],'pending_no_runtime_observed')
        self.assertFalse(podrows[0]['physical_closure_proven'])
        self.b.pods[-1]['status'].update(phase='Succeeded',containerStatuses=[{'state':{'terminated':{'exitCode':0}}}]);job['status']={'succeeded':1}
        self.assertEqual(self.observe()['actors'][0]['lifecycle'],'job_completed')

    def test_guarded_finite_worker_normal_and_closed_clean_draining(self):
        pin,job,key=self.add_job(phase='Running',running=True)
        value=self.observe();rows=[r for r in value['actors']if'pod_uid'in r];self.assertEqual(rows[0]['role'],key)
        self.assertEqual(rows[0]['lifecycle'],'active_guarded')
        self.b.guards[-1].update(state='closed_clean',locked=False)
        value=self.observe();rows=[r for r in value['actors']if'pod_uid'in r];self.assertEqual(rows[0]['lifecycle'],'closed_clean_draining')
        self.assertFalse(rows[0]['physical_closure_proven'])

    def test_stage_ttl_runtime_pin_and_quarantine_cannot_drift(self):
        for change in ('suspend','ttl','runtime','unknown_job','quarantine'):
            self.op,self.b=fixture();pin,job,key=self.add_job(phase='Running',running=True)
            if change=='suspend':job['spec']['suspend']=True
            elif change=='ttl':
                job['spec']['ttlSecondsAfterFinished']=3600
                normalized=copy.deepcopy(job['spec']);normalized.pop('suspend');pin['spec_sha256']=digest(canonical(normalized))
            elif change=='runtime':pin['runtime_image_id']='sha256:'+'f'*64
            else:
                old={'metadata':{'name':'quarantined','uid':uid()},'spec':{'suspend':False},'status':{}}
                self.b.jobs['quarantined']=old
                if change=='quarantine':self.op.inventory['linux']['jobs']=[{'name':'quarantined','uid':old['metadata']['uid'],'spec_sha256':digest(canonical(old['spec']))}]
            with self.subTest(change=change),self.assertRaises(Refused):self.observe()

    def test_real_readonly_facade_accepts_exact_native_cri_inspect_without_raw_record(self):
        import canary_watch as w
        session=w.Session();session.value={'canary_id':uid(),'recovery_operation_id':uid()}
        session.sample_deadline=w.time.monotonic()+5;session.cleanup_deadline=None
        session.modules={'linux_backend':types.SimpleNamespace(LinuxBackend=type(self.b),SQL_SESSION_PREFIX='fixed')}
        readonly=session._backend()
        with patch.object(n,'_proc',side_effect=proc):result=n.observe(readonly,self.op)
        self.assertEqual(result['pods'][0]['container_id'],'containerd://'+CID)
        self.assertEqual(len(readonly.commands),3)
        self.assertTrue(all(kw['record']is False for _,_,kw in readonly.commands))

    def test_job_hash_request_argv_and_failed_status_refuse(self):
        for change in ('hash','argv','failed'):
            self.op,self.b=fixture();pin,job,key=self.add_job()
            if change=='hash':job['spec']['backoffLimit']=1
            elif change=='argv':
                job['spec']['template']['spec']['containers'][0]['args'][1]=uid()+'@0'
                normalized=copy.deepcopy(job['spec']);normalized.pop('suspend');pin['spec_sha256']=digest(canonical(normalized))
            else:job['status']['failed']=1
            with self.subTest(change=change),self.assertRaises(Refused):self.observe()

    def test_proc_replacement_between_cri_inspects_refuses(self):
        actual=self.b._run;count=[0]
        def run(*args,**kwargs):
            if args[2][2]=='inspect':
                count[0]+=1
                if count[0]==2:self.b.cri[CID]['info']['pid']=10002
            return actual(*args,**kwargs)
        self.b._run=run
        with self.assertRaisesRegex(Refused,'process_replaced'):self.observe()


class PodDefaultsTests(unittest.TestCase):
    def test_only_typed_scheduling_defaults_and_exact_template_allowed(self):
        source={'automountServiceAccountToken':False,'containers':[{'name':'server','image':IMAGE}],'serviceAccountName':'fixed'}
        actual=dict(copy.deepcopy(source),nodeName='node',priority=0,preemptionPolicy='PreemptLowerPriority',serviceAccount='fixed',tolerations=copy.deepcopy(n.DEFAULT_TOLERATIONS))
        n._pod_spec(actual,source,'node')
        for key,value in (('nodeName','other'),('priority',1),('hostPID',True),('volumes',[{'name':'token'}])):
            bad=copy.deepcopy(actual);bad[key]=value
            with self.subTest(key=key),self.assertRaises(Refused):n._pod_spec(bad,source,'node')
        actual['tolerations'].append({'operator':'Exists'})
        with self.assertRaises(Refused):n._pod_spec(actual,source,'node')


class ProcFileTests(unittest.TestCase):
    def fixture(self,root):
        proc=root/'proc'/'321';proc.mkdir(parents=True)
        (proc/'stat').write_text('321 (synthetic) '+' '.join(['S']+['0']*18+['123'])+'\n')
        (proc/'status').write_text('NSpid:\t321\t1\n');(proc/'cgroup').write_text('0::/docker/'+CID+'\n')
        group=root/'cgroup'/'docker'/CID;group.mkdir(parents=True);(group/'cgroup.procs').write_text('321\n')
        def path(value):
            if value=='/proc':return root/'proc'
            if value=='/sys/fs/cgroup':return root/'cgroup'
            return Path(value)
        return proc,group,path
    def test_real_disposable_proc_files_namespace_start_and_cgroup(self):
        with tempfile.TemporaryDirectory(prefix='canary-native-proc-',dir='/tmp')as tmp:
            root=Path(tmp);proc,group,path=self.fixture(root)
            with patch.object(n,'Path',side_effect=path):value=n._proc(321,CID)
            self.assertEqual(value['namespace_pid'],1);self.assertEqual(value['process_start_ticks'],123)
            self.assertEqual(value['cgroup']['ino'],group.stat().st_ino)
    def test_proc_namespace_or_cgroup_membership_or_rebind_refuse(self):
        for change in ('namespace','membership','rebind'):
            with self.subTest(change=change),tempfile.TemporaryDirectory(prefix='canary-native-proc-',dir='/tmp')as tmp:
                root=Path(tmp);proc,group,path=self.fixture(root)
                if change=='namespace':(proc/'status').write_text('NSpid:\t999\t1\n')
                elif change=='membership':(group/'cgroup.procs').write_text('999\n')
                actual=n.os.read;swapped=[False]
                def read(fd,size):
                    raw=actual(fd,size)
                    if change=='rebind'and not swapped[0]:
                        swapped[0]=True;proc.rename(root/'old-proc');proc.mkdir()
                    return raw
                with patch.object(n,'Path',side_effect=path),patch.object(n.os,'read',side_effect=read),self.assertRaises(Refused):n._proc(321,CID)


if __name__=='__main__':unittest.main()
