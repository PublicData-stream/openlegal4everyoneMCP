"""Synthetic historical records and real private /tmp FDs; no native acceptance.

Fixed SQL-producing source helpers run against a command recorder, never Docker,
PostgreSQL, providers or a cluster. Only temporary certificate installations and
file/FD syscalls are real. The historical HDD device descriptor is synthetic.
"""
import copy
import os
from pathlib import Path
import stat
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch

BASE=Path(__file__).resolve().parents[1]; sys.path.insert(0,str(BASE))
import epoch_completion as read
import epoch_certificate as certificate_codec
import epoch_current
import epoch_primary_fence as fence
import initial_epoch_certificate as producer
import initial_blob_seed as seed
import initial_pg_pair as pair_source
from interface import Refused
from inventory import canonical,decode,digest
from test_epoch_certificate import certificate,uid
from test_epoch_observation import projection
from test_initial_epoch_certificate import snapshot,images,strict


class Fixture:
    def __init__(self, root, snapshot_mutation=None, control_mutation=None):
        self.root=root; self.fds=[]; self.serial=0; self.command_serial=0
        self.operation=uid(); self.original_end=time.monotonic()+100
        sha=digest(b'fictional retained source')
        self.references=[{'role':'corpus','sha256':sha,'storage_key':sha[:2]+'/'+sha+'-'+uid(),'size':25}]
        self.archive=root/'archive'; self.source=root/'source'; self.control=root/'control'; self.hdd=root/'synthetic-hdd'
        for path in (self.archive,self.source,self.control,self.hdd): path.mkdir(mode=0o700)
        (self.archive/self.operation).mkdir(mode=0o700)
        self.archive_fd=self.open_dir(self.archive); self.source_fd=self.open_dir(self.source); self.control_fd=self.open_dir(self.control)
        self.desc=certificate_codec.root_descriptor(self.archive_fd)
        self.source_desc=certificate_codec.root_descriptor(self.source_fd); self.control_desc=certificate_codec.root_descriptor(self.control_fd)
        self.hdd_fd=self.open_dir(self.hdd); actual_hdd=certificate_codec.root_descriptor(self.hdd_fd)
        self.hdd_desc=dict(actual_hdd,dev=actual_hdd['dev']+1)
        self.controls=[{'label':'ssd','path':str(self.archive),'descriptor':self.desc,
                'media':{'kind':'ssd','physical_leaves':['/dev/fixture-ssd']}},
            {'label':'hdd','path':str(self.hdd),'descriptor':self.hdd_desc,
                'media':{'kind':'hdd','dataset':'cachepool/fixture','guid':'12345','mountpoint':str(self.hdd)}}]
        graph={}
        for name in sorted(read.PRODUCER_SOURCES):
            raw=(BASE/name).read_bytes(); self.private(self.source/name,raw); graph[name]=digest(raw)
        self.graph=graph; self.graphsha=digest(canonical(graph)); self.producer_sha=graph['initial_epoch_certificate.py']
        self.c=certificate(); self.subject=self.c['subject']; self.user='recovery_login'; self.boot=self.subject['host_boot_id']
        self.parent={'pid':201,'start_ticks':'123','boot_id':self.boot}
        self.pg={'primary':self.subject['primary'],'standby':self.subject['standby'],'admin_user':self.user,'database':'openlegal','uid':70}
        binary=Path('/usr/bin/true').resolve(); binary_sha=digest(binary.read_bytes())
        self.cfg={key:{} for key in read.INVENTORY_FIELDS}
        self.cfg.update(schema_version=1,kind='openlegal_initial_epoch_certificate_inventory',operation_id=self.operation,
            revision=self.subject['runtime']['revision'],managed_epoch=self.subject['managed_epoch'],host={'boot_id':self.boot},
            source_graph=graph,linux={'postgres':self.pg,'binaries':{'docker':{'path':str(binary),'sha256':binary_sha}}})
        self.rows={}; self.backend=types.SimpleNamespace(_sequence=0,_driver_deadline=self.original_end,
            _docker_config=(Path('/tmp/openlegal-storage-docker-client-'+self.operation),None))
        self.backend._run=self.run; self.backend._artifact=self.artifact
        self.journal=types.SimpleNamespace(output_hashes=self.hashes,read_phase_bytes=self.read_phase)
        self.op=types.SimpleNamespace(phase=read.SEED,operation_id=self.operation,inventory=self.cfg,journal=self.journal,seal=lambda:None)
        refs=seed._references(self.backend,self.op,'after')
        roots=[]
        for role, row in self.subject['blob_roots'].items():
            roots.append({'role':role,'host_path':row['path'],**{'root_'+key:row[key] for key in ('dev','ino','uid','gid','mode')},
                **{key:row[key] for key in ('container_path','filesystem_id','kernel_filesystem_id','marker_sha256')},
                'manifest_sha256':'b'*64})
        seeded={'kind':'openlegal_initial_blob_seed_result','operation_id':self.operation,'managed_epoch':self.subject['managed_epoch'],
            'blob_binding_sha256':self.subject['control']['blob_binding_sha256'],'roots':roots,'reference_pages':refs['pages'],
            'union_pages':[],'media_profile':'cache_hdd_corpus_ssd_primary_v1','all_original_generations_preserved':True,
            **{k:False for k in ('postgres_replica_created','roles_created','workload_started','provider_called','ready','automatic_retry')}}
        self.seed_ref=self.save(read.SEED,'initial_blob_seed-complete.json',seeded)
        original_ref=self.external('original.json',{'managed_epoch':self.subject['managed_epoch']})
        driver_ref=self.external('driver.json',{'operation_id':self.operation,'legacy_inventory':original_ref})
        driver_auth=self.external('driver-authority.json',self.authority(driver_ref))
        seed_ref=self.external('seed.json',{'operation_id':self.operation,'driver_inventory':driver_ref,
            'driver_authority':driver_auth,'source_sha256':graph['initial_blob_seed.py']})
        seed_auth=self.external('seed-authority.json',self.authority(seed_ref))
        pair_ref=self.external('pair.json',{'operation_id':self.operation,'seed_inventory':seed_ref,
            'seed_authority':seed_auth,'source_sha256':graph['initial_pg_pair.py']})
        pair_auth=self.external('pair-authority.json',self.authority(pair_ref))
        recovery_op=uid(); previous=[]
        for basename,kind in (('initial_role_prepare','openlegal_initial_role_prepare_result'),
                ('initial_provider_maintenance','openlegal_initial_provider_maintenance_result')):
            invref=self.external(basename+'.json',{'operation_id':self.operation,'source_sha256':graph[basename+'.py']})
            auth=self.authority(invref); auth['source_sha256']=graph[basename+'.py']; authref=self.external(basename+'-auth.json',auth)
            value={'kind':kind,'operation_id':self.operation,'producer_source':{'path':str(self.source/(basename+'.py')),
                'sha256':graph[basename+'.py']},'inventory':invref,'authority':authref,'authority_sha256':authref['sha256'],
                'seed_complete':self.seed_ref,'role_names':{'recovery_login':self.user},'provider_recovery_operation':recovery_op}
            if basename=='initial_role_prepare': value['parent']=copy.deepcopy(self.parent)
            previous.append(self.save(basename,'complete.json',value))
        role, maintenance=previous
        self.cfg['epoch_certificate']={'certificate_id':self.c['certificate_id'],'reason':'initial_bind','generation':1,
            'predecessor_certificate_sha256':None,'original_inventory':original_ref,'driver_inventory':driver_ref,
            'driver_authority':driver_auth,'seed_inventory':seed_ref,'seed_authority':seed_auth,'pair_inventory':pair_ref,
            'pair_authority':pair_auth,'role_prepare':role,'maintenance':maintenance,'epoch_roots':self.controls}
        q=self.cfg['epoch_certificate']; self.subject['original_inventory']=original_ref
        inp=self.save(read.PAIR,'input.json',{'inventory':pair_ref,'authority':pair_auth,'role_prepare':role,
            'maintenance':maintenance,'seed_inventory':seed_ref,'seed_authority':seed_auth,'original_absolute_deadline':self.original_end})
        first=self.ack(first=True); final=self.ack(first=False)
        paired={'kind':'openlegal_initial_pg_pair_result','operation_id':self.operation,'inventory_sha256':pair_ref['sha256'],
            'authority_sha256':pair_auth['sha256'],'source_sha256':graph['initial_pg_pair.py'],'managed_epoch':self.subject['managed_epoch'],
            'binding':self.subject['control'],'blob_roots':roots,'input_binding':inp,'first_bind_ack':first,'final_ack':final,
            'provider_recovery_operation':recovery_op,**{side+'_container_id':self.subject[side]['container_id'] for side in ('primary','standby')},
            **{side+'_process':self.subject[side]['process'] for side in ('primary','standby')},
            **{key:False for key in ('ready','provider_called','roles_created','released','automatic_retry')}}
        q['pair_complete']=self.save(read.PAIR,'complete.json',paired)
        raw_toml=strict(self.subject); toml=self.external('server.toml',raw_toml)
        ingest_toml=self.external('ingestion.toml',strict(self.subject,ingestion=True))
        template=self.external('collection.json',{'kind':'Job'})
        profile={'schema_version':1,'kind':'openlegal_initial_epoch_strict_configmap_pvc_v1','images':images(),
            'serving':{'deployment_name':'server','configmap_name':'serving','configmap_uid':uid(),'toml_key':'server.toml','toml_source':toml},
            'ingestion':{'deployment_name':'scheduler','configmap_name':'scheduler','configmap_uid':uid(),'toml_key':'server.toml',
                'toml_source':ingest_toml,'collection_template_key':'collection-job.json','collection_template_source':template}}
        q['strict_preprovision']=self.external('profile.json',profile)
        proj=projection(); proj['runtime'].update(revision=self.subject['runtime']['revision'],docker_image_id=self.subject['runtime']['image_id'])
        proj['survivor'].update({k:self.subject['standby'][k] for k in ('container_id','image_id','data_directory','data_root')})
        proj['blob_survivors']={key:self.subject['blob_roots'][key] for key in certificate_codec.observation.SSD_SURVIVORS}
        self.subject['runtime']['configuration_sha256']=certificate_codec.observation.configuration_hash_v1(proj)
        mapping=[{'role':role,'pvc_uid':uid(),'pv_uid':uid(),'pv_spec_sha256':'c'*64,'host_root':row['path'],
            'container_root':row['container_path']} for role,row in self.subject['blob_roots'].items()]
        self.configproof={'source_profile':q['strict_preprovision'],
            'profiles':[{'profile':key,'toml_sha256':profile[key]['toml_source']['sha256'],'mapping':mapping}
                for key in ('serving','ingestion')],'collection_template_sha256':template['sha256'],
            'collection_mapping':mapping,'projection':proj,'configuration_sha256':self.subject['runtime']['configuration_sha256'],
            'runtime_authenticated_or_ready':False}
        manifest={'kind':'openlegal_compiled_migration_manifest','candidate':{},'native_architecture':'amd64',
            'migrations':[{'version':n,'checksum':format(n,'096x')} for n in range(1,34)]}
        manifest_ref=self.external('manifest.json',manifest)
        self.cfg['initial_migration']={'compiled_manifest':manifest_ref,'candidate':{}}
        if control_mutation is not None: control_mutation(self.controls[1])
        self.invref=self.external('inventory.json',self.cfg)
        auth={key:False for key in read.AUTHORITY_FIELDS}
        auth.update(schema_version=1,kind='openlegal_initial_epoch_certificate_authority',operation_id=self.operation,
            inventory_sha256=self.invref['sha256'],source_graph_sha256=self.graphsha,source_sha256=self.producer_sha,valid_until=1,
            **{key:True for key in ('approved','epoch_certificate_observation_permitted','epoch_certificate_install_permitted',
                'initial_bind_only','preserve_originals')})
        self.authref=self.external('authority.json',auth)
        self.obs_config=self.save(read.OBSERVATION,'config.json',self.configproof)
        initial_fence=self.primary_fence(read.OBSERVATION,'initial')
        snapshot_value=snapshot(self.subject); snapshot_value['retained_references']=copy.deepcopy(self.references)
        if snapshot_mutation is not None: snapshot_mutation(snapshot_value)
        snap=self.metadata(read.OBSERVATION,'snapshot','standby',snapshot_value)
        snap.update(fixed_primary_target='0/100',initial_primary_fence=initial_fence)
        snapshot_ref=self.save(read.OBSERVATION,'snapshot.json',snap)
        self.op.phase=read.OBSERVATION
        m=types.SimpleNamespace(cfg=self.cfg,role_names={'recovery_login':self.user},seal=lambda:None,b=self.backend,op=self.op,
            j=self.journal,phase=read.OBSERVATION,original_end=self.original_end,
            p=types.SimpleNamespace(work={'initial_migration':self.cfg['initial_migration']}),
            save=lambda label,value:self.save(read.OBSERVATION,label+'.json',value))
        self.next_stdout=canonical([dict(v,success=True) for v in manifest['migrations']])
        migrations=producer._Producer.sql(m,'migrations')
        audit=self.save(read.OBSERVATION,'roots.json',{'roots':self.subject['blob_roots'],'references_sha256':digest(canonical(self.references)),
            'audit':[{'role':row['role'],'permanent_sha256':row['manifest_sha256'],'generations':1,
                'references':sum(v['role']==row['role'].split('_')[0] for v in self.references)} for row in roots]})
        physical=self.save(read.OBSERVATION,'physical.json',{'cri_containers':[],'cgroups':[{'path':'/fixture','pids':[]}]})
        obs={'schema_version':1,'kind':'openlegal_initial_epoch_observation_pending_parent_exit','operation_id':self.operation,
            'inventory_sha256':self.invref['sha256'],'authority_sha256':self.authref['sha256'],'source_graph_sha256':self.graphsha,
            'subject':self.subject,'configuration':self.obs_config,'standby_snapshot':snapshot_ref,'migrations':migrations,
            'four_root_audit':audit,'physical_fence':physical,'primary_fence':self.primary_fence(read.OBSERVATION,'final'),
            'sql_closure':self.both_sql(read.OBSERVATION)}
        obsref=self.save(read.OBSERVATION,'observation.json',obs)
        child=lambda pid:{'pid':pid,'start_ticks':'124','boot_id':self.boot,'actual_exit_code':0,'remaining_children':[]}
        obsexit=self.save(read.OBSERVATION,'child-exit.json',child(202))
        self.install_exit=self.save(read.INSTALL,'child-exit.json',child(203))
        graphref=self.save(read.OBSERVATION,'source-graph.json',graph)
        self.c['provenance']={'operation_id':self.operation,'reason':'initial_bind','producer_source':{
            'path':str(self.source/'initial_epoch_certificate.py'),'sha256':self.producer_sha},
            'source_graph':{'path':str(self.path(graphref)),'sha256':graphref['sha256']},'inventory':self.invref,
            'authority':self.authref,'seed':self.seed_ref,'first_pair':q['pair_complete'],'observation':obsref,'observation_child_exit':obsexit}
        self.raw=canonical(self.c); self.head=certificate_codec.make_head(self.raw)
        certref=self.save(read.OBSERVATION,'certificate.json',self.c)
        intent={'schema_version':1,'kind':'openlegal_initial_epoch_dual_install_intent','operation_id':self.operation,
            'certificate':certref,'certificate_sha256':digest(self.raw),'head_sha256':digest(self.head),
            'roots':[{'path':v['path'],'descriptor':v['descriptor'],'expected_head':None} for v in self.controls],
            'observation':obsref,'observation_child_exit':obsexit,'automatic_retry':False}
        intentref=self.save(read.INSTALL,'dual-intent.json',intent)
        facts=[]; observations=[]
        for index, (fd,desc) in enumerate(((self.archive_fd,self.desc),(self.hdd_fd,actual_hdd))):
            fact=certificate_codec.install_one(fd,desc,self.raw,None,time.monotonic()+20)
            observed=epoch_current.read_current(fd,desc,time.monotonic()+20)
            observed={k:v for k,v in observed.items() if k not in ('head_raw','certificate_raw')}
            if index:
                fact['root_identity']=self.hdd_desc; observed['root_identity']=self.hdd_desc
                for kind in ('certificate','head'):
                    fact[kind+'_dev']=self.hdd_desc['dev']; observed[kind+'_identity']['dev']=self.hdd_desc['dev']
                one={'schema_version':2,'kind':'openlegal_epoch_install_intent','certificate_id':self.c['certificate_id'],
                    'certificate_sha256':digest(self.raw),'generation':1,'predecessor_certificate_sha256':None,
                    'expected_head_sha256':None,'intended_head_sha256':digest(self.head),'root_identity':self.hdd_desc}
                fact['intent_sha256']=digest(canonical(one))
            facts.append(fact); observations.append(observed)
        result={'schema_version':1,'kind':'openlegal_initial_epoch_install_pending_parent_exit','operation_id':self.operation,
            'certificate_sha256':digest(self.raw),'head_sha256':digest(self.head),'intent':intentref,'installations':facts}
        self.installed_ref=self.save(read.INSTALL,'installations.json',result)
        back=self.save(read.INSTALL,'readback.json',{'observations':observations,'certificate_sha256':digest(self.raw),'head_sha256':digest(self.head)})
        self.proof={'schema_version':2,'kind':'openlegal_pre_ack_epoch_install','operation_id':self.operation,'inventory':self.invref,
            'authority':self.authref,'source_graph_sha256':self.graphsha,'certificate_id':self.c['certificate_id'],
            'certificate_sha256':digest(self.raw),'subject_sha256':digest(canonical(self.subject)),'generation':1,
            'predecessor_certificate_sha256':None,'observation':obsref,'observation_child_exit':obsexit,
            'install_intent':intentref,'installations':self.installed_ref,'install_child_exit':self.install_exit,'independent_readback':back,
            'final_configuration':self.save(read.INSTALL,'config.json',self.configproof),
            'final_primary_fence':self.primary_fence(read.INSTALL,'final'),'final_sql_closure':self.both_sql(read.INSTALL),
            'parent':self.parent,'ready':False,'public_ack':False,'automatic_retry':False,'outer_actual_os_exit':'NOT_OBSERVABLE_IN_PROCESS'}
        self.completion_ref=self.save(read.INSTALL,'complete.json',self.proof)

    def open_dir(self,path):
        fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC); self.fds.append(fd); return fd

    def close(self):
        for fd in reversed(self.fds): os.close(fd)

    @staticmethod
    def private(path,raw):
        path.parent.mkdir(parents=True,mode=0o700,exist_ok=True); path.write_bytes(raw); path.chmod(0o600)

    def external(self,name,value):
        raw=value if type(value) is bytes else canonical(value); path=self.control/name; self.private(path,raw)
        return {'path':str(path),'sha256':digest(raw)}

    def authority(self,ref): return {'approved':True,'operation_id':self.operation,'inventory_sha256':ref['sha256'],'valid_until':1}

    def path(self,ref): return self.archive/self.operation/ref['phase']/ref['name']

    def save(self,phase,name,value):
        raw=value if type(value) is bytes else canonical(value); ref={'phase':phase,'name':name,'sha256':digest(raw)}
        self.private(self.path(ref),raw); self.rows[(phase,name)]=raw; return ref

    def hashes(self): return {name:digest(raw) for (phase,name),raw in self.rows.items() if phase==self.op.phase}

    def read_phase(self,phase,name,sha):
        raw=self.rows[(phase,name)]; assert digest(raw)==sha; return raw

    def artifact(self,op,label,value): return self.save(op.phase,op.phase+'-'+label+'.json',value)

    def receipt(self,stdout,stdin,args,**extra):
        return dict(binary='docker',arguments_sha256=digest(canonical(read._dispatch(self.operation)+args)),
            stdin_sha256=digest(stdin),actual_exit_code=0,started=True,owned_processes_closed=True,
            expired=False,interrupted=False,unexpected_descendants=False,output_limit_exceeded=False,elapsed_seconds=0,
            stdout_hex=stdout.hex(),stdout_bytes=len(stdout),stdout_sha256=digest(stdout),
            stderr_hex='',stderr_bytes=0,stderr_sha256=digest(b''),**extra)

    def run(self,op,binary,args,deadline_arg=None,*,stdin=None,**unused):
        self.backend._sequence+=1
        if hasattr(self,'next_stdout'): stdout=self.next_stdout; del self.next_stdout
        else: stdout=canonical({'source_rows':len(self.references),'rows':self.references})
        self.save(op.phase,op.phase+'-command-%04d.json'%self.backend._sequence,self.receipt(stdout,stdin,args))
        return stdout

    def execute(self,stdout):
        return types.SimpleNamespace(actual_exit_code=0,started=True,owned_processes_closed=True,expired=False,
            interrupted=False,unexpected_descendants=False,output_limit_exceeded=False,elapsed_seconds=0,stdout=stdout,stderr=b'')

    def peer(self):
        return dict(self.subject['control'],postmaster_started_at=self.subject['primary']['postmaster_started_at'],
            sender_pid=301,sender_started_at='2026-11-02 14:00:00+00',prefix_lsn='0/100',flush_lsn='0/200')

    def ack(self,*,first):
        before=self.peer(); after=self.peer(); started=time.monotonic(); end=started+1
        p=types.SimpleNamespace(work={'linux':self.cfg['linux']},primary=self.subject['primary']['container_id'],
            cfg={'operation_id':self.operation},b=self.backend,p={'recovery_role':self.user})
        intent=None; lines=[before,after]
        if first:
            intent=self.save(read.PAIR,'initial_pg_pair-0001-first-bind-intent.json',{'operation_id':self.operation,
                'expected_old':None,'proposed_binding':self.subject['control'],'candidate':before,
                'ack_budget_seconds':2,'caller_original_absolute_deadline':self.original_end})
            lines.insert(0,{'operation_id':self.operation,'intent_sha256':intent['sha256'],'previous_binding':None,
                'new_binding':self.subject['control'],'peer_before_commit':before})
        stdout=b''.join(canonical(v) for v in lines)
        with patch.object(pair_source,'_critical_light'),patch.object(pair_source.driver.processes,'execute',return_value=self.execute(stdout)):
            _,row=pair_source._critical_execute(p,'bind' if first else 'proof',self.subject['control'],intent,end)
        ref=self.save(read.PAIR,'native-bind.json' if first else 'native-final.json',row)
        return {'before':before,'after':after,'fixed_target':'0/100','native_window':{'started':started,'finished':time.monotonic(),'deadline':end},
            'native_command_refs':[ref],'observation_status':'historical_native_proof'}

    def metadata(self,phase,kind,side,result):
        self.op.phase=phase
        common=certificate_codec.observation.NativeObservation.__new__(certificate_codec.observation.NativeObservation)
        common.backend=self.backend
        identity={'database':'openlegal','session_user':self.user,'current_user':self.user,'role':{'login':True,'superuser':False,
            'create_role':False,'create_db':False,'replication':False,'bypass_rls':False,'inherit':False},
            'broad_roles':False,'owner_membership':False,'function_owners_qualified':True}
        self.next_stdout=canonical({'identity':identity,'result':result})
        with patch.object(type(common),'_deadline',side_effect=lambda end:end),patch.object(type(common),'_native',return_value={}),\
             patch.object(type(common),'_run',side_effect=self.run):
            return common._query_metadata(self.op,self.subject,kind,self.original_end,which=side)

    def both_sql(self,phase):
        refs=[self.save(phase,side+'-sql.json',self.metadata(phase,'sql-closure',side,{'sessions':[]})) for side in ('primary','standby')]
        return self.save(phase,'both-sql.json',{'observations':refs,'sessions':[]})

    def primary_fence(self,phase,label):
        before=self.peer(); after=self.peer(); started=time.monotonic(); end=started+1
        p=types.SimpleNamespace(subject=self.subject,expected=self.subject['control'],role_names={'recovery_login':self.user},
            binary_hash=self.cfg['linux']['binaries']['docker']['sha256'],config=self.backend._docker_config[0],records=[],
            original_end=self.original_end,binary_path='/usr/bin/true',binary_fd=None,op=self.op)
        stdout=b''.join(canonical({'session_user':self.user,'current_user':self.user,'read_only':'on','result':v}) for v in (before,after))
        with patch.object(fence,'_light'),patch.object(fence,'_hash_binary'),patch.object(fence.processes,'execute',return_value=self.execute(stdout)):
            fence._execute(p,'proof',end)
        native=self.save(phase,label+'-native.json',p.records[0]); finished=time.monotonic()
        raw=canonical({'session_user':self.user,'current_user':self.user,'result':{'sessions':[]}})
        sql=("BEGIN READ ONLY; SET LOCAL statement_timeout='2s'; SET LOCAL lock_timeout='1s'; "
            "SET LOCAL transaction_timeout='5s'; SET LOCAL search_path=pg_catalog; "
            "SELECT jsonb_build_object('session_user',session_user,'current_user',current_user,'result',openlegal_checkpoint.client_sessions()); ROLLBACK;\n").encode()
        cmd=self.save(phase,label+'-closure-command.json',self.receipt(raw,sql,fence._args(p)))
        closed=self.save(phase,label+'-closure.json',{'actual_command':cmd,'raw_utf8':raw.decode()})
        value={'schema_version':1,'kind':'openlegal_epoch_primary_fence','operation_id':self.operation,'native_phase':phase,
            'archive_phase':phase,'inventory_sha256':self.invref['sha256'],'authority_sha256':self.authref['sha256'],
            'source_graph_sha256':self.graphsha,'native_started_at':started,'native_finished_at':finished,'native_absolute_deadline':end,
            'original_absolute_deadline':self.original_end,'native_command_refs':[native],'native_proof_valid':True,'failure_type':None,
            'proof':{'before':before,'after':after,'fixed_target':'0/100'},'global_sql_closure':closed,
            'observation_status':'historical_native_window_not_current_after_archive','public_ack':False,'ready':False}
        return self.save(phase,label+'-fence.json',value)

    def read(self,**kwargs):
        args={'expected_operation_id':self.operation,'expected_producer_sha256':self.producer_sha,
            'expected_source_graph_sha256':self.graphsha,'max_records':4096,'max_bytes':512*1024**2}; args.update(kwargs)
        return read.read_completed_install(self.archive_fd,self.desc,(self.source_fd,self.source_desc,str(self.source)),
            (self.control_fd,self.control_desc,str(self.control)),self.raw,self.completion_ref,time.monotonic()+30,**args)

    def proof_update(self): self.completion_ref=self.save(read.INSTALL,'complete.json',self.proof)


@unittest.skipUnless(os.geteuid()==0,'root-private /tmp completion FD gate')
class Completion(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/tmp',prefix='epoch-completion-'); self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name); self.root.chmod(0o700); self.f=Fixture(self.root); self.addCleanup(self.f.close)

    def test_complete_source_produced_sql_wrappers_and_real_fd_bundle_are_historical_only(self):
        with patch('fcntl.flock',side_effect=AssertionError('reader changed lock')):
            result=self.f.read()
        self.assertIs(result['historical_only'],True); self.assertEqual(result['certificate_raw'],self.f.raw)
        self.assertEqual(result['operation_id'],self.f.operation)
        self.assertEqual(result['unique_records'],len(result['records']))
        self.assertEqual(result['unique_bytes'],sum(len(row['raw']) for row in result['records']))
        self.assertNotIn('ready',result); self.assertNotIn('outer_actual_exit_code',result)
        self.assertEqual(certificate_codec.root_descriptor(self.f.archive_fd),self.f.desc)
        self.assertEqual(result['legacy_typed_listings'][0]['kind'],'legacy_pair_first_bind_intent')

    def test_exact_expected_operation_producer_and_graph_pins_are_required(self):
        for args in ({'expected_operation_id':uid()},{'expected_producer_sha256':'1'*64},{'expected_source_graph_sha256':'2'*64}):
            with self.subTest(args=args),self.assertRaises(Refused): self.f.read(**args)

    def test_original_call_limits_and_global_caps_are_not_expanded(self):
        result=self.f.read()
        for args in ({'max_records':result['unique_records']-1},{'max_bytes':result['unique_bytes']-1},
            {'max_records':4097},{'max_bytes':512*1024**2+1},{'max_records':True}):
            with self.subTest(args=args),self.assertRaises(Refused): self.f.read(**args)

    def test_missing_final_sql_closure_or_success_booleans_refuse(self):
        original=copy.deepcopy(self.f.proof)
        for key,value in (('ready',True),('public_ack',True),('automatic_retry',True),('outer_actual_os_exit',0)):
            self.f.proof=copy.deepcopy(original); self.f.proof[key]=value; self.f.proof_update()
            with self.subTest(key=key),self.assertRaisesRegex(Refused,'proof_binding'): self.f.read()
        self.f.proof=copy.deepcopy(original); del self.f.proof['final_sql_closure']; self.f.proof_update()
        with self.assertRaisesRegex(Refused,'proof_shape'): self.f.read()

    def test_bool_child_exit_is_not_actual_zero(self):
        row=decode(self.f.path(self.f.install_exit).read_bytes()); row['actual_exit_code']=False
        self.f.proof['install_child_exit']=self.f.save(read.INSTALL,self.f.install_exit['name'],row); self.f.proof_update()
        with self.assertRaisesRegex(Refused,'child_not_closed_zero'): self.f.read()

    def test_install_readback_must_match_actual_installed_inode_history(self):
        result=decode(self.f.path(self.f.installed_ref).read_bytes()); result['installations'][1]['head_ino']+=1
        self.f.proof['installations']=self.f.save(read.INSTALL,self.f.installed_ref['name'],result); self.f.proof_update()
        with self.assertRaisesRegex(Refused,'readback_install_inode'): self.f.read()

    def test_symlink_hardlink_owner_and_mode_do_not_become_private_history(self):
        target=self.f.path(self.f.completion_ref); saved=target.read_bytes()
        backup=self.root/'saved'; backup.write_bytes(saved); backup.chmod(0o600)
        target.unlink(); target.symlink_to(backup)
        with self.assertRaises(OSError): self.f.read()
        target.unlink(); self.f.private(target,saved); hard=self.root/'hard'; os.link(target,hard)
        with self.assertRaisesRegex(Refused,'private_file'): self.f.read()
        hard.unlink(); target.chmod(0o640)
        with self.assertRaisesRegex(Refused,'private_file'): self.f.read()
        target.chmod(0o600); os.chown(target,10004,10004)
        with self.assertRaisesRegex(Refused,'private_file'): self.f.read()

    def test_root_path_label_and_same_mount_are_checked_from_real_fds(self):
        with self.assertRaisesRegex(Refused,'root_path_rebound'):
            read.read_completed_install(self.f.archive_fd,self.f.desc,(self.f.source_fd,self.f.source_desc,str(self.root/'wrong')),
                (self.f.control_fd,self.f.control_desc,str(self.f.control)),self.f.raw,self.f.completion_ref,time.monotonic()+5,
                expected_operation_id=self.f.operation,expected_producer_sha256=self.f.producer_sha,
                expected_source_graph_sha256=self.f.graphsha,max_records=4096,max_bytes=512*1024**2)
        original=read.certificate.root_descriptor
        def descriptor(fd):
            value=original(fd)
            if fd==self.f.source_fd: value['mount_id']+=1
            return value
        changed=dict(self.f.source_desc,mount_id=self.f.source_desc['mount_id']+1)
        with patch.object(read.certificate,'root_descriptor',side_effect=descriptor):
            with self.assertRaisesRegex(Refused,'same_mount_only'):
                read._Reader(self.f.archive_fd,self.f.desc,(self.f.source_fd,changed,str(self.f.source)),
                    (self.f.control_fd,self.f.control_desc,str(self.f.control)),time.monotonic()+5,4096,512*1024**2)

    def test_final_file_inode_and_parent_directory_swaps_are_detected(self):
        original=read._Reader.finish
        for kind in ('file','directory'):
            swapped=[]
            def finish(reader):
                if not swapped:
                    swapped.append(True)
                    if kind=='file':
                        target=self.f.path(self.f.completion_ref); new=target.with_name('replacement.json')
                        self.f.private(new,target.read_bytes()); os.replace(new,target)
                    else:
                        phase=self.f.archive/self.f.operation/read.INSTALL; phase.rename(phase.with_name('moved-install'))
                        phase.mkdir(mode=0o700)
                return original(reader)
            with patch.object(read._Reader,'finish',finish):
                with self.subTest(kind=kind),self.assertRaises(Refused): self.f.read()
            if kind=='directory':
                phase=self.f.archive/self.f.operation/read.INSTALL; phase.rmdir(); phase.with_name('moved-install').rename(phase)

    def test_multiple_legacy_first_bind_intents_refuse_before_candidate_read(self):
        path=self.f.archive/self.f.operation/read.PAIR/'initial_pg_pair-9999-first-bind-intent.json'
        self.f.private(path,canonical({'untrusted':True}))
        with self.assertRaisesRegex(Refused,'multiple_bind_intents'): self.f.read()

    def test_native_receipt_raw_hash_and_actual_closure_are_not_booleans(self):
        row=self.f.receipt(b'{}\n',b'sql',[])
        for key,value in (('actual_exit_code',False),('owned_processes_closed',False),('expired',True),
                ('stdout_hex','00'*3),('stdout_bytes',True)):
            changed=copy.deepcopy(row); changed[key]=value
            with self.subTest(key=key),self.assertRaises(Refused): read._command(changed)

    def test_expired_read_deadline_and_noncanonical_parser_refuse(self):
        with self.assertRaisesRegex(Refused,'deadline'):
            read._Reader(self.f.archive_fd,self.f.desc,(self.f.source_fd,self.f.source_desc,str(self.f.source)),
                (self.f.control_fd,self.f.control_desc,str(self.f.control)),time.monotonic()-1,4096,512*1024**2)
        for raw in (b' {}\n',b'{"x":1,"x":2}\n',b'{"x":NaN}\n',b'['*34+b'0'+b']'*34):
            with self.subTest(raw=raw[:10]),self.assertRaises(Refused): read._json(raw)

    def test_peer_addresses_require_full_host_prefix_like_actual_pair_source(self):
        for host,accepted,rejected in (('192.0.2.2',('192.0.2.2','192.0.2.2/32'),'192.0.2.2/24'),
                ('2001:db8::2',('2001:db8::2','2001:db8::2/128'),'2001:db8::2/64')):
            binding=dict(self.f.subject['control'],standby_addr=host)
            for address in accepted:
                peer=dict(self.f.peer(),standby_addr=address)
                with self.subTest(address=address):
                    self.assertEqual(read._host_address(address),pair_source._host_address(address))
                    self.assertEqual(read._peer(peer,binding),peer)
            with self.subTest(rejected=rejected),self.assertRaisesRegex(Refused,'host_prefix'):
                read._peer(dict(self.f.peer(),standby_addr=rejected),binding)

    def test_storage_snapshot_semantics_refuse_even_with_matching_actual_stdout(self):
        changes=(('bool-total',lambda s:s['storage_metadata']['cache_totals'].__setitem__('queries',True)),
            ('negative-total',lambda s:s['storage_metadata']['cache_totals'].__setitem__('referenced_bytes',-1)),
            ('missing-total',lambda s:s['storage_metadata']['cache_totals'].pop('snapshots')),
            ('bad-cache-id',lambda s:s['storage_metadata']['cache_storage'].__setitem__('id','not-uuid')),
            ('ack-ahead',lambda s:s['storage_metadata']['corpus_control'].__setitem__('index_ack',2)),
            ('extra-storage',lambda s:s['storage_metadata'].__setitem__('unexpected',{})))
        for name,change in changes:
            directory=self.root/name; directory.mkdir(mode=0o700); fixture=Fixture(directory,snapshot_mutation=change)
            try:
                with self.subTest(name=name),self.assertRaisesRegex(Refused,'storage|cache_totals'): fixture.read()
            finally: fixture.close()

    def reader(self):
        r=read._Reader(self.f.archive_fd,self.f.desc,(self.f.source_fd,self.f.source_desc,str(self.f.source)),
            (self.f.control_fd,self.f.control_desc,str(self.f.control)),time.monotonic()+30,4096,512*1024**2)
        r.operation=self.f.operation; r.inventory_sha=self.f.invref['sha256']; r.authority_sha=self.f.authref['sha256']; r.graph_sha=self.f.graphsha
        self.addCleanup(r.close); return r

    def test_native_specific_65536_output_bound_does_not_expand_to_metadata_cap(self):
        ref=self.f.proof['final_primary_fence']; value=decode(self.f.path(ref).read_bytes())
        native=value['native_command_refs'][0]; row=decode(self.f.path(native).read_bytes())
        lines=[decode(line) for line in bytes.fromhex(row['stdout_hex']).splitlines()]
        for line in lines: line['result']['sender_started_at']='x'*40000
        raw=b''.join(canonical(line) for line in lines)
        row.update(stdout_hex=raw.hex(),stdout_sha256=digest(raw),stdout_bytes=len(raw))
        self.assertGreater(len(raw),65536); self.assertEqual(read._command(row),raw)
        value['native_command_refs'][0]=self.f.save(read.INSTALL,native['name'],row)
        changed=self.f.save(read.INSTALL,ref['name'],value)
        with self.assertRaisesRegex(Refused,'native_output_bound'):
            read._primary_fence(self.reader(),changed,self.f.cfg,self.f.subject)
        pair_ref=self.f.cfg['epoch_certificate']['pair_complete']; paired=decode(self.f.path(pair_ref).read_bytes())
        ack=paired['final_ack']; native=ack['native_command_refs'][0]; row=decode(self.f.path(native).read_bytes())
        row['stderr_bytes']=65537; row['stderr_sha256']=digest(b'x'*65537)
        ack['native_command_refs'][0]=self.f.save(read.PAIR,native['name'],row)
        with self.assertRaisesRegex(Refused,'native_output_bound'):
            read._pair_ack(self.reader(),ack,self.f.subject['control'],self.f.cfg,first=False,original_end=self.f.original_end)

    def test_original_root_parent_and_original_driver_deadline_are_cross_bound(self):
        self.f.proof['parent']=dict(self.f.parent,pid=999999); self.f.proof_update()
        with self.assertRaisesRegex(Refused,'maintenance_role_binding'): self.f.read()
        ref=self.f.proof['final_primary_fence']; value=decode(self.f.path(ref).read_bytes())
        value['original_absolute_deadline']+=1; changed=self.f.save(read.INSTALL,ref['name'],value)
        reader=self.reader(); reader.original_end=self.f.original_end
        with self.assertRaisesRegex(Refused,'original_deadline_changed'):
            read._primary_fence(reader,changed,self.f.cfg,self.f.subject)

    def test_unavailable_hdd_descriptor_is_purely_validated_without_opening_hdd(self):
        changes=(('bool-inode',lambda row:row['descriptor'].__setitem__('ino',True)),
            ('foreign-owner',lambda row:row['descriptor'].__setitem__('uid',10004)),
            ('public-mode',lambda row:row['descriptor'].__setitem__('mode',0o750)),
            ('extra-field',lambda row:row['descriptor'].__setitem__('untrusted',True)),
            ('path-alias',lambda row:row.__setitem__('path',str(Path(row['path']).parent/'archive'))))
        for name,change in changes:
            directory=self.root/name; directory.mkdir(mode=0o700); fixture=Fixture(directory,control_mutation=change)
            try:
                with self.subTest(name=name),self.assertRaisesRegex(Refused,'control_descriptor|original_archive|control_path'):
                    fixture.read()
            finally: fixture.close()
