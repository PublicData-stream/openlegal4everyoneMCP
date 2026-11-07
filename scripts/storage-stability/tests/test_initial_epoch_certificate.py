"""Pure and real /tmp/fork tests. NO Docker, SQL, remote, media or Ready proof.

Dual journals in tests deliberately bypass only the physical constructor gate:
all directory/file/dual-byte/child wait checks are real, on the SAME /tmp device.
Fixed child work is replaced only inside these tests; production takes no callback.
"""
import copy
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

BASE=Path(__file__).resolve().parents[1];sys.path.insert(0,str(BASE))
import initial_epoch_certificate as e
from interface import Refused,need
from inventory import canonical,decode,digest
from test_epoch_certificate import certificate
from test_initial_pg_pair import binding,peer


def private(root,name,value):
    raw=value if type(value) is bytes else canonical(value)
    p=root/name;p.write_bytes(raw);p.chmod(0o600)
    return {'path':str(p),'sha256':digest(raw)}


def subject():return certificate()['subject']


def strict(s,*,ingestion=False):
    r=s['blob_roots'];raw=("[cache]\nmode='persistent'\n[cache.postgres]\n"
        "replication_mode='required'\nreplication_epoch='%s'\ntls_mode='verify-full'\n"
        "ca_file='/run/secrets/postgres-ca/ca.crt'\nurl_env='OPENLEGAL_DATABASE_URL'\n"
        "[cache.blob]\nkind='filesystem_mirror'\npath='%s'\nreplica_path='%s'\n"
        "[database]\nblob_path='%s'\nblob_replica_path='%s'\nstorage_failure_policy='recover'\n")%(
        s['managed_epoch'],r['cache_primary']['container_path'],r['cache_replica']['container_path'],
        r['corpus_primary']['container_path'],r['corpus_replica']['container_path'])
    if ingestion:raw+="[database.ingestion]\ncollection_job_template_path='/etc/openlegal/collection-job.json'\nworker_image='%s'\n"%images()['document_worker']
    return raw.encode()


def images():return {key:'ghcr.io/publicdata-stream/openlegal-'+name+'@sha256:'+'1'*64
    for key,name in (('serving','server'),('ingestion','server-ingestion'),('document_worker','document-worker'))}


def pod_fixture(s,kind='serving'):
    cm={'metadata':{'name':'config','uid':str(uuid.uuid4())}}
    pod={'containers':[{'name':'app','image':images()['serving' if kind=='serving' else 'ingestion'],
        'args':['/etc/openlegal/server.toml'] if kind=='serving' else ['--collection-scheduler','/etc/openlegal/server.toml'],
        'env':[{'name':'OPENLEGAL_DATABASE_URL','valueFrom':{'secretKeyRef':{'name':'db','key':'OPENLEGAL_DATABASE_URL'}}}],
        'volumeMounts':[{'name':'config','mountPath':'/etc/openlegal','readOnly':True},
            {'name':'ca','mountPath':'/run/secrets/postgres-ca','readOnly':True}]}],
        'volumes':[{'name':'config','configMap':{'name':'config','defaultMode':0o444}},
            {'name':'ca','secret':{'secretName':'ca'}}]}
    resources={};pvs=[]
    for role,r in s['blob_roots'].items():
        pod['containers'][0]['volumeMounts'].append({'name':role,'mountPath':r['container_path']})
        pod['volumes'].append({'name':role,'persistentVolumeClaim':{'claimName':role}})
        cuid=str(uuid.uuid4());puid=str(uuid.uuid4())
        resources[('persistentvolumeclaim',role)]={'metadata':{'uid':cuid},'spec':{'volumeName':role}}
        resources[('persistentvolume',role)]={'metadata':{'name':role,'uid':puid},
            'spec':{'local':{'path':r['path']},'claimRef':{'uid':cuid}}}
        pvs.append({'name':role,'uid':puid,'path':r['path']})
    m=types.SimpleNamespace(profile={'images':images()},cfg={'linux':{'persistent_volumes':pvs}},op=object(),original_end=time.monotonic()+5)
    m.common=types.SimpleNamespace(_kube=lambda op,k,n,end,*a:copy.deepcopy(resources[(k,n)]))
    return m,pod,cm,resources


class Configuration(unittest.TestCase):
    def test_real_toml_requires_epoch_two_mirrors_verify_full_and_recover(self):
        s=subject();raw=strict(s,ingestion=True)
        value=e._strict_toml(raw,s,images(),ingestion=True)
        self.assertEqual(value['cache']['postgres']['replication_epoch'],s['managed_epoch'])
        self.assertEqual(value['database']['blob_path'],s['blob_roots']['corpus_primary']['container_path'])
        for before,after in ((b"replication_mode='required'",b"replication_mode='local'"),
            (s['managed_epoch'].encode(),str(uuid.uuid4()).encode()),(b"tls_mode='verify-full'",b"tls_mode='plaintext'"),
            (b"kind='filesystem_mirror'",b"kind='filesystem'"),(b"storage_failure_policy='recover'",b"storage_failure_policy='shutdown'"),
            (b"/blobs/corpus_primary",b"/stale/corpus"),(b"collection-job.json",b"dynamic.json"),
            (images()['document_worker'].encode(),b'worker:latest')):
            with self.subTest(before=before),self.assertRaises(Refused):
                e._strict_toml(raw.replace(before,after),s,images(),ingestion=True)

    def test_incomplete_committed_style_profile_and_invalid_toml_refuse(self):
        s=subject()
        for raw in (b'[cache]\nmode="memory"',b'[cache]\nmode="persistent"\n[database]\nblob_path="/one"',
                    b'\xff',b'[cache]\nmode="persistent"\nmode="memory"',b'x'*(e.PROFILE_LIMIT+1)):
            with self.assertRaises(Refused):e._strict_toml(raw,s,images(),ingestion=False)

    def test_actual_pvc_to_pv_exact_four_roots_and_no_secret_values(self):
        s=subject();m,pod,cm,_=pod_fixture(s)
        rows=e._pod_bindings(m,pod,s,cm,kind='serving',image_data={})
        self.assertEqual({r['role'] for r in rows},set(e.certificates.ROLES))
        self.assertEqual({r['host_root'] for r in rows},{r['path'] for r in s['blob_roots'].values()})
        self.assertNotIn('password',canonical(rows).decode())

    def test_parent_mount_hostpath_subpath_readonly_and_old_pv_refuse(self):
        for change in ('parent','hostpath','subpath','subpath-expr','readonly','old-pv','wrong-claim','overlap','init'):
            s=subject();m,pod,cm,res=pod_fixture(s);mount=pod['containers'][0]['volumeMounts'][2]
            if change=='parent':mount['mountPath']='/blobs'
            if change=='hostpath':pod['volumes'][2]={'name':'cache_primary','hostPath':{'path':s['blob_roots']['cache_primary']['path']}}
            if change=='subpath':mount['subPath']='data'
            if change=='subpath-expr':mount['subPathExpr']='$(ROOT)'
            if change=='readonly':mount['readOnly']=True
            if change=='old-pv':res[('persistentvolume','cache_primary')]['spec']['local']['path']='/old'
            if change=='wrong-claim':res[('persistentvolume','cache_primary')]['spec']['claimRef']['uid']=str(uuid.uuid4())
            if change=='overlap':pod['containers'][0]['volumeMounts'].append({'name':'ca','mountPath':'/blobs'})
            if change=='init':pod['initContainers']=[{'name':'extra'}]
            with self.subTest(change=change),self.assertRaises(Refused):e._pod_bindings(m,pod,s,cm,kind='serving',image_data={})

    def test_serving_omitted_args_require_actual_pinned_image_default_not_env(self):
        s=subject();m,pod,cm,_=pod_fixture(s);pod['containers'][0].pop('args')
        good={'Entrypoint':['/usr/local/bin/openlegal-server'],'Cmd':['/etc/openlegal/server.toml']}
        e._pod_bindings(m,pod,s,cm,kind='serving',image_data=good)
        for bad in ({'Entrypoint':good['Entrypoint'],'Cmd':['/other.toml']},
                    {'Entrypoint':['/bin/sh'],'Cmd':good['Cmd']}):
            with self.assertRaises(Refused):e._pod_bindings(m,pod,s,cm,kind='serving',image_data=bad)

    def test_configmap_items_cannot_shadow_verified_toml(self):
        s=subject();m,pod,cm,_=pod_fixture(s);vol=pod['volumes'][0]['configMap']
        vol['items']=[{'key':'server.toml','path':'server.toml'}]
        e._pod_bindings(m,pod,s,cm,kind='serving',image_data={})
        vol['items'].append({'key':'evil','path':'server.toml'})
        with self.assertRaisesRegex(Refused,'configmap_items'):e._pod_bindings(m,pod,s,cm,kind='serving',image_data={})

    def test_verified_config_and_ca_mounts_cannot_be_shadowed(self):
        for path in ('/etc/openlegal/server.toml', '/etc/openlegal/collection-job.json',
                     '/run/secrets/postgres-ca/ca.crt', '/etc', '/run/secrets',
                     '/etc/openlegal', '/run/secrets/postgres-ca',
                     '/etc/./openlegal/server.toml', '/etc/openlegal/../openlegal/server.toml',
                     '/etc/openlegal//server.toml', 'relative/file', '/', '/etc/openlegal/'):
            s=subject();m,pod,cm,_=pod_fixture(s)
            pod['volumes'].append({'name':'shadow','configMap':{'name':'unreviewed'}})
            pod['containers'][0]['volumeMounts'].append({'name':'shadow','mountPath':path,
                'subPath':'unreviewed','readOnly':True})
            with self.subTest(path=path),self.assertRaises(Refused):
                e._pod_bindings(m,pod,s,cm,kind='serving',image_data={})

    def test_mount_paths_are_unique_canonical_and_segment_sibling_prefixes_are_allowed(self):
        s=subject();m,pod,cm,_=pod_fixture(s)
        for path in ('/etc/openlegal-sibling', '/run/secrets/postgres-ca-sibling'):
            pod['containers'][0]['volumeMounts'].append({'name':'ca','mountPath':path,'readOnly':True})
        self.assertEqual(len(e._pod_bindings(m,pod,s,cm,kind='serving',image_data={})),4)
        for path in ('/unrelated/path', '/unrelated/./path', '/unrelated/../path',
                     '/unrelated//path', '/unrelated/path/', '/unrelated/path\x00'):
            changed=copy.deepcopy(pod)
            changed['containers'][0]['volumeMounts'].extend([
                {'name':'ca','mountPath':'/unrelated/path','readOnly':True},
                {'name':'ca','mountPath':path,'readOnly':True}])
            with self.subTest(path=path),self.assertRaises(Refused):
                e._pod_bindings(m,changed,s,cm,kind='serving',image_data={})

    def test_url_inline_or_extra_command_cannot_replace_approved_template(self):
        for change in ('inline','env-extra','command','config-rw','config-subpath','ca-missing'):
            s=subject();m,pod,cm,_=pod_fixture(s);app=pod['containers'][0]
            if change=='inline':app['env'][0]={'name':'OPENLEGAL_DATABASE_URL','value':'postgres://forbidden'}
            if change=='env-extra':app['env'].append(copy.deepcopy(app['env'][0]))
            if change=='command':app['command']=['/bin/sh','-c']
            if change=='config-rw':app['volumeMounts'][0]['readOnly']=False
            if change=='config-subpath':app['volumeMounts'][0]['subPath']='dynamic'
            if change=='ca-missing':app['volumeMounts'].pop(1)
            with self.subTest(change=change),self.assertRaises(Refused):e._pod_bindings(m,pod,s,cm,kind='serving',image_data={})


def position(s):
    return {'schema_version':1,'kind':'openlegal_survivor_position','in_recovery':True,
        'system_identifier':s['control']['primary_system_identifier'],'timeline':s['control']['timeline'],
        'postmaster_started_at':s['standby']['postmaster_started_at'],'data_directory':s['standby']['data_directory'],
        'control':copy.deepcopy(s['control']),'receive_lsn':'0/200','replay_lsn':'0/180',
        'replay_paused':False,'wal_receiver_count':1}


def snapshot(s):
    control={'generation':str(uuid.uuid4()),'recovery_hold':True,'hold_started_at':123,'singleton':True}
    return {'position_before':position(s),'position_after':position(s),
        'transaction':{'isolation':'repeatable read','read_only':True,'statement_timeout_ms':30000,
            'lock_timeout_ms':1000,'transaction_timeout_ms':35000,'idle_in_transaction_session_timeout_ms':5000},
        'retained_tables':{'public._sqlx_migrations':{'rows':33,'sha256':'f'*64}},'retained_references':[],
        'storage_metadata':{'corpus_control':{'next_event':2,'index_ack':1},'cache_storage':{'id':str(uuid.uuid4())},
            'cache_totals':{'snapshots':0,'queries':0,'referenced_bytes':0}},
        'provider':{'state':{'budget':{'operator_suspended':True,'unresolved_response':False,'admission_owner':None},
                'control':{'singleton':True},'admissions':[],'canary_control':None,'canaries':[]},
            'hold_control':control,'lock_provenance':'primary_advisory_locks_not_observable_on_standby'}}


class HealthyStandby(unittest.TestCase):
    def test_streaming_receiver_is_healthy_and_does_not_require_emergency_disconnect(self):
        s=subject();v=snapshot(s)
        self.assertEqual(e._snapshot(v,s,'0/100',[]),'f'*64)
        self.assertEqual(v['position_after']['wal_receiver_count'],1)
        v['position_after']['receive_lsn']='0/300';v['position_after']['replay_lsn']='0/280'
        self.assertEqual(e._snapshot(v,s,'0/100',[]),'f'*64)

    def test_wrong_lineage_restarted_standby_paused_or_replay_before_fixed_F_refuse(self):
        for key,value in (('in_recovery',False),('system_identifier','999'),('timeline',2),
            ('postmaster_started_at','changed'),('data_directory','/other'),('replay_paused',True),
            ('wal_receiver_count',True),('replay_lsn','0/FF')):
            s=subject();v=snapshot(s);v['position_after'][key]=value
            with self.subTest(key=key),self.assertRaises(Refused):e._snapshot(v,s,'0/100',[])

    def test_m33_is_actual_repeatable_read_metadata_and_preserves_provider_hold(self):
        for kind in ('transaction','table','references','provider-resume','control-release','cache-bool','lock-inference'):
            s=subject();v=snapshot(s)
            if kind=='transaction':v['transaction']['read_only']=False
            if kind=='table':v['retained_tables']['public._sqlx_migrations']['sha256']='source-hash-not-a-digest'
            if kind=='references':v['retained_references']=[{'role':'cache','size':0}]
            if kind=='provider-resume':v['provider']['state']['budget']['operator_suspended']=False
            if kind=='control-release':v['provider']['hold_control']['recovery_hold']=False
            if kind=='cache-bool':v['storage_metadata']['cache_totals']['queries']=True
            if kind=='lock-inference':v['provider']['lock_provenance']='primary_clean'
            with self.subTest(kind=kind),self.assertRaises(Refused):e._snapshot(v,s,'0/100',[])



def active_fixture():
    s=subject();primary={'container_id':s['primary']['container_id'],'image_id':s['primary']['image_id'],'mounts':[]}
    mount={'source':'/new-pg/data','destination':'/var/lib/postgresql','rw':True,'type':'bind'}
    oldlinux={'postgres':{'database':'old','admin_user':'old','uid':70,'primary':primary,'standby':{}},
        'bound_roots':[],'blob_binding_sha256':'a'*64,'workloads':[{'name':'serving','uid':'old-serving'}],
        'jobs':[{'name':'historical-job','uid':'old-job'}],'config_maps':[],'persistent_volumes':[],
        'serving_image':'old-image','serving_runtime_image_id':'old-id','serving_service':{},'failure_domains':{'fixed':True}}
    work={'kind':'original','linux':oldlinux,'original_datasets':[{'guid':'fixed-original'}],
        'host':{'boot_id':s['host_boot_id']}}
    p=types.SimpleNamespace(work=work,role_context={'names':{'recovery_login':'recovery_user'}},
        standby=s['standby']['container_id'],p={'image_id':s['standby']['image_id']},created={})
    p.created[p.standby]={'mounts':[mount]};cfg=copy.deepcopy(work)
    cfg['linux']['postgres']={'database':'openlegal','uid':70,'admin_user':'recovery_user','primary':copy.deepcopy(primary),
        'standby':{'container_id':p.standby,'image_id':p.p['image_id'],'mounts':[copy.deepcopy(mount)]},'binding':s['control']}
    cfg['linux']['blob_binding_sha256']=s['control']['blob_binding_sha256']
    roots=[{'role':role,'host_path':row['path'],'root_uid':row['uid'],'root_dev':row['dev'],'root_ino':row['ino']}
           for role,row in s['blob_roots'].items()]
    cfg['linux']['bound_roots']=[{'role':r['role'],'path':r['host_path'],'uid':r['root_uid'],'dev':r['root_dev'],'ino':r['root_ino']} for r in roots]
    return cfg,p,{'binding':s['control'],'blob_roots':roots}


class ActiveDerivation(unittest.TestCase):
    def test_new_standby_and_four_roots_derive_from_actual_pair_not_old_inventory(self):
        cfg,p,value=active_fixture();rows=e._active(cfg,p,value)
        self.assertEqual(len(rows),4);self.assertEqual(cfg['linux']['postgres']['standby']['container_id'],p.standby)
        self.assertEqual(p.work['linux']['postgres']['standby'],{})

    def test_stale_native_root_original_guid_writer_uid_or_extra_scope_changes_refuse(self):
        for kind in ('cid','root','original','writer','uid-bool','role','mount','immutable-linux'):
            cfg,p,value=active_fixture()
            if kind=='cid':cfg['linux']['postgres']['standby']['container_id']='f'*64
            if kind=='root':cfg['linux']['bound_roots'][0]['ino']+=1
            if kind=='original':cfg['original_datasets'][0]['guid']='different'
            if kind=='writer':cfg['linux']['workloads'][0]['uid']='replacement'
            if kind=='uid-bool':cfg['linux']['postgres']['uid']=True
            if kind=='role':cfg['linux']['postgres']['admin_user']='postgres'
            if kind=='mount':cfg['linux']['postgres']['standby']['mounts'][0]['source']='/stale/data'
            if kind=='immutable-linux':cfg['linux']['failure_domains']={'replaced':True}
            with self.subTest(kind=kind),self.assertRaises(Refused):e._active(cfg,p,value)


def journal_fixture(root,phase=e.OBSERVATION):
    # The constructor's independent-device gate is intentionally NOT exercised.
    j=e.journals.DualJournal.__new__(e.journals.DualJournal)
    j.operation=str(uuid.uuid4());j.binding={'operation_id':j.operation};j.base=[];j.paths=[];j.phase_paths=[]
    j.bytes=0;j.count=0;j.artifacts={};j.history=[];j.phase_names={phase}
    for name in ('same-device-a','same-device-b'):
        base=root/name;base.mkdir(mode=0o700);j.base.append((base,e.journals.directory(base)))
        op=base/j.operation;op.mkdir(mode=0o700);j.paths.append((op,e.journals.directory(op)))
        path=op/phase;path.mkdir(mode=0o700);j.phase_paths.append((path,e.journals.directory(path)))
    j.save('intent.json',{'operation_id':j.operation,'synthetic':True});return j


def producer_fixture(root,phase=e.OBSERVATION):
    m=e._Producer.__new__(e._Producer);m.j=journal_fixture(root,phase);m.b=e.driver.Backend()
    m.b._driver_deadline=time.monotonic()+10;m.original_end=m.b._driver_deadline;m.parent=e._parent()
    m.cfg={'operation_id':m.j.operation};m.serial=0;m.phase=phase;m.cancelled=False;m.child=False;m.parentfd=None
    m.count_end=384;m.bytes_end=e.BYTES;m.control=[];return m


def fixture_seal(m):
    need(time.monotonic()<m.original_end and not m.cancelled,'synthetic_original_deadline')
    if m.child:
        e.driver._child_parent(m.parent['pid'],m.parentfd)
        need(e.processes._identity(m.parent['pid'])==m.parent['start_ticks'],'synthetic_parent_lifetime')
    else:need(e._parent()==m.parent,'synthetic_root_lifetime')
    m.j.check()


@unittest.skipUnless(os.geteuid()==0,'private root-owned /tmp file tests')
class ContractsAndActualFiles(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/tmp',prefix='initial-epoch-pure-');self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700)

    def test_run_refuses_nonlive_context_before_loading_or_any_dispatch(self):
        with patch.object(e,'_load',side_effect=AssertionError('unexpected load')):
            with self.assertRaisesRegex(Refused,'live_dedicated_root'):e.run(object(),object(),{}, {})

    def test_expired_original_end_refuses_before_private_input_load(self):
        b=e.driver.Backend();b._driver_deadline=time.monotonic()-1
        lock=e.journals.SharedLock({'path':str(self.root/'unused'),'identity':{'dev':1,'ino':2}})
        with patch.object(e,'_load',side_effect=AssertionError('unexpected load')):
            with self.assertRaisesRegex(Refused,'driver_original_deadline'):e.run(b,lock,{}, {})

    def inputs(self):
        graph={n:'a'*64 for n in e.SOURCES};ref=private(self.root,'inert.json',{})
        cfg={k:None for k in e.BASE_FIELDS};cfg.update(schema_version=1,kind='openlegal_initial_epoch_certificate_inventory',
            operation_id=str(uuid.uuid4()),managed_epoch=str(uuid.uuid4()),revision='a'*40,source_graph=graph)
        q={k:copy.deepcopy(ref) for k in e.INPUT_FIELDS};q.update(certificate_id=str(uuid.uuid4()),reason='initial_bind',generation=1,
            predecessor_certificate_sha256=None,epoch_roots=[],pair_complete={},role_prepare={},maintenance={})
        cfg['epoch_certificate']=q;inv=private(self.root,'inventory.json',cfg)
        auth={'schema_version':1,'kind':'openlegal_initial_epoch_certificate_authority','operation_id':cfg['operation_id'],
            'inventory_sha256':inv['sha256'],'source_graph_sha256':digest(canonical(graph)),
            'source_sha256':graph['initial_epoch_certificate.py'],'valid_until':int(time.time())+60}
        auth.update({k:True for k in ('approved','epoch_certificate_observation_permitted','epoch_certificate_install_permitted','initial_bind_only','preserve_originals')})
        auth.update({k:False for k in ('automatic_retry','roles_permitted','credentials_permitted','trust_permitted','provider_permitted',
            'kubernetes_mutation_permitted','pg_mutation_permitted','ready_permitted','release_permitted','promotion_permitted','cleanup_permitted')})
        return cfg,auth,inv

    def test_actual_private_authority_binds_whole_graph_inventory_and_strict_bools(self):
        cfg,auth,inv=self.inputs();a=private(self.root,'authority.json',auth)
        # Only source staging is synthetic; actual private input files/hashes are read.
        with patch.object(e,'_source_seal'):
            self.assertEqual(e._load(inv,a)[0],cfg)
            for key,value in (('approved',1),('ready_permitted',True),('source_graph_sha256','f'*64),
                ('inventory_sha256','f'*64),('epoch_certificate_install_permitted',False),('valid_until',0)):
                bad=copy.deepcopy(auth);bad[key]=value;a=private(self.root,'authority.json',bad)
                with self.subTest(key=key),self.assertRaises(Refused):e._load(inv,a)

    def test_initial_reason_generation_and_extra_authority_fields_refuse(self):
        cfg,auth,inv=self.inputs()
        with patch.object(e,'_source_seal'):
            for key,value in (('reason','controlled_restart'),('generation',2),('generation',True),('predecessor_certificate_sha256','f'*64)):
                bad=copy.deepcopy(cfg);bad['epoch_certificate'][key]=value
                inv=private(self.root,'inventory.json',bad);a=private(self.root,'authority.json',auth)
                with self.subTest(key=key),self.assertRaisesRegex(Refused,'first_only'):e._load(inv,a)
            inv=private(self.root,'inventory.json',cfg);auth['arbitrary_shell']='forbidden'
            with self.assertRaisesRegex(Refused,'authority'):e._load(inv,private(self.root,'authority.json',auth))

    def test_source_graph_is_closed_and_loaded_sibling_path_is_checked(self):
        with self.assertRaisesRegex(Refused,'closed_source_graph'):e._source_seal({})
        graph={n:'a'*64 for n in e.SOURCES};name=next(n for n in e.SOURCES if n[:-3] in sys.modules)
        with patch.object(e,'private_bytes',return_value=b''),patch.object(sys.modules[name[:-3]],'__file__','/tmp/wrong-loaded-source.py'):
            with self.assertRaisesRegex(Refused,'loaded_source'):e._source_seal(graph)

    def test_both_real_raw_copies_and_extra_partial_files_fail_reconciliation(self):
        for change in ('raw','extra','symlink','counter'):
            parent=self.root/change;parent.mkdir(mode=0o700);m=producer_fixture(parent);idx=e._index(m,None)
            if change=='raw':(m.j.phase_paths[1][0]/'intent.json').write_bytes(b'{}')
            if change=='extra':e.journals.new_file(m.j.phase_paths[0][0]/(m.phase+'-partial.json'),b'{}')
            if change=='symlink':
                p=m.j.phase_paths[1][0]/'intent.json';p.unlink();p.symlink_to(m.j.phase_paths[0][0]/'intent.json')
            if change=='counter':idx['sequence']=True
            before=(m.j.count,m.j.bytes)
            with self.subTest(change=change),self.assertRaises((Refused,OSError)):e._reconcile(m,idx)
            self.assertEqual((m.j.count,m.j.bytes),before)

    def test_reconcile_cannot_extend_global_count_or_bytes_reservation(self):
        m=producer_fixture(self.root);idx=e._index(m,None);m.count_end=0
        with self.assertRaisesRegex(Refused,'global_budget'):e._reconcile(m,idx)
        m.count_end=384;m.bytes_end=e.COMMAND_HEADROOM
        with self.assertRaisesRegex(Refused,'global_budget'):e._reconcile(m,idx)

    def test_first_bind_native_raw_receipt_not_boolean_and_operation_bound(self):
        j=journal_fixture(self.root,e.pair_module.PHASE);b=binding();a=peer(b);z=peer(b,flush='0/110')
        name=e.pair_module.PHASE+'-0001-first-bind-intent.json'
        raw=canonical({'operation_id':j.operation,'expected_old':None,'proposed_binding':b,
            'candidate':e.pair_module._peer(peer(b,prefix='0/80',candidate=True),b)})
        intent=j.save_bytes(name,raw)
        bound={'operation_id':j.operation,'intent_sha256':intent,'previous_binding':None,'new_binding':b,'peer_before_commit':peer(b,prefix='0/90')}
        native={'kind':'openlegal_initial_pair_native_sql','query_kind':'bind','actual_exit_code':0,'started':True,'owned_processes_closed':True,
            **dict.fromkeys(('expired','interrupted','unexpected_descendants','output_limit_exceeded'),False),
            'stdout_hex':b''.join(canonical(r) for r in (bound,a,z)).hex()}
        out=bytes.fromhex(native['stdout_hex']);native.update(stdout_bytes=len(out),stdout_sha256=digest(out),
            native_started_at=10.,native_finished_at=11.,native_absolute_deadline=12.,original_operation_deadline=100.)
        n=e.pair_module.PHASE+'-0002-native.json';sha=j.save(n,native)
        v=dict(e.pair_module._proof(a,z,b),observation_status='historical_native_proof',
            native_command_refs=[{'phase':e.pair_module.PHASE,'name':n,'sha256':sha}],native_window={'started':10.,'finished':11.,'deadline':12.})
        e._ack(j,v,b,first=True)
        for change in ('exit','previous','operation','window'):
            nv=copy.deepcopy(native);vb=copy.deepcopy(v);nb=copy.deepcopy(bound)
            if change=='exit':nv['actual_exit_code']=True
            if change=='previous':nb['previous_binding']=b
            if change=='operation':nb['operation_id']=str(uuid.uuid4())
            if change=='window':vb['native_window']['deadline']=13.
            if change in ('previous','operation'):nv['stdout_hex']=b''.join(canonical(r) for r in (nb,a,z)).hex()
            out=bytes.fromhex(nv['stdout_hex']);nv.update(stdout_bytes=len(out),stdout_sha256=digest(out))
            nn=e.pair_module.PHASE+'-negative-'+change+'.json';ss=j.save(nn,nv)
            vb['native_command_refs']=[{'phase':e.pair_module.PHASE,'name':nn,'sha256':ss}]
            with self.subTest(change=change),self.assertRaises(Refused):e._ack(j,vb,b,first=True)


@unittest.skipUnless(os.geteuid()==0 and hasattr(os,'pidfd_open'),'Linux root-private harmless fork')
class ActualFixedChildren(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/tmp',prefix='initial-epoch-fork-');self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700)
        for name in ('seal','local'):
            p=patch.object(e._Producer,name,fixture_seal);p.start();self.addCleanup(p.stop)

    def test_real_waitpid_zero_and_raw_dual_child_pid_are_parent_observed(self):
        m=producer_fixture(self.root);rootpid=os.getpid()
        def harmless(child):return child.save('actual-child',{'pid':os.getpid(),'parent':os.getppid(),'synthetic':True})
        with patch.object(e,'_observe',side_effect=harmless):ref,row=e._fork(m)
        self.assertEqual(type(row['actual_exit_code']),int);self.assertEqual(row['actual_exit_code'],0)
        self.assertEqual(row,m.b._initial_epoch_certificate_last_exit);self.assertNotEqual(row['pid'],rootpid)
        value=decode(e._ref(m.j,ref));self.assertEqual(value['pid'],row['pid']);self.assertEqual(value['parent'],rootpid)
        self.assertIsNone(e.processes._identity(row['pid']))
        with self.assertRaises(ChildProcessError):os.waitpid(row['pid'],os.WNOHANG)
        m.j.check()

    def test_failure_cannot_use_json_zero_or_stale_parent_zero(self):
        m=producer_fixture(self.root);m.b._initial_epoch_certificate_last_exit={'pid':123,'actual_exit_code':0}
        def failed(child):
            child.save('fabricated-zero',{'actual_exit_code':0});raise Refused('synthetic_failed')
        with patch.object(e,'_observe',side_effect=failed):
            with self.assertRaisesRegex(Refused,'not_successfully_closed'):e._fork(m)
        self.assertEqual(m.b._initial_epoch_certificate_last_exit['actual_exit_code'],1)
        self.assertIsNone(e.processes._identity(m.b._initial_epoch_certificate_last_exit['pid']))

    def test_actual_exit_seven_with_ignored_sigchld_is_never_fabricated_zero(self):
        m=producer_fixture(self.root);old=signal.getsignal(signal.SIGCHLD);signal.signal(signal.SIGCHLD,signal.SIG_IGN)
        self.addCleanup(signal.signal,signal.SIGCHLD,old)
        with patch.object(e,'_observe',side_effect=lambda _:os._exit(7)):
            with self.assertRaises(Refused):e._fork(m)
        row=m.b._initial_epoch_certificate_last_exit;self.assertEqual(row['actual_exit_code'],7)
        self.assertIs(signal.getsignal(signal.SIGCHLD),signal.SIG_IGN)
        self.assertIsNone(e.processes._identity(row['pid']))

    def test_partial_artifact_refuses_and_actual_failed_child_is_closed(self):
        m=producer_fixture(self.root)
        def partial(child):
            e.journals.new_file(child.j.phase_paths[0][0]/(child.phase+'-partial.json'),b'{}')
            raise Refused('synthetic_partial')
        with patch.object(e,'_observe',side_effect=partial):
            with self.assertRaisesRegex(Refused,'partial_artifact'):e._fork(m)
        row=m.b._initial_epoch_certificate_last_exit;self.assertEqual(row['actual_exit_code'],1)
        self.assertIsNone(e.processes._identity(row['pid']))

    def test_parent_discovery_error_still_physically_closes_only_owned_child(self):
        m=producer_fixture(self.root);rootpid=os.getpid();real=e.processes._Ownership.capture;calls=[]
        def capture(owned):
            if os.getpid()==rootpid:
                calls.append(True)
                raise OSError(errno.EIO,'synthetic_discovery_error')
            return real(owned)
        def harmless(child):time.sleep(.05);return child.save('synthetic',{})
        with patch.object(e,'_observe',side_effect=harmless),patch.object(e.processes._Ownership,'capture',capture):
            with self.assertRaisesRegex(OSError,'synthetic_discovery_error'):e._fork(m)
        row=m.b._initial_epoch_certificate_last_exit;self.assertEqual(type(row['actual_exit_code']),int)
        self.assertEqual(row['remaining_children'],[]);self.assertIsNone(e.processes._identity(row['pid']))

    def assert_reaped(self,m):
        row=m.b._initial_epoch_certificate_last_exit
        self.assertIs(type(row['actual_exit_code']),int)
        self.assertEqual(row['remaining_children'],[])
        self.assertIsNone(e.processes._identity(row['pid']))
        with self.assertRaises(ChildProcessError):os.waitpid(row['pid'],os.WNOHANG)
        self.assertEqual(e.processes._children(os.getpid()),set())
        self.assertFalse(e._SERIAL.locked())
        return row

    def test_real_postfork_socket_close_error_keeps_raw_wait_owner(self):
        m=producer_fixture(self.root);rootpid=os.getpid();real=e.socket.socketpair
        class CloseError:
            def __init__(self,sock):self.sock=sock;self.raised=False
            def close(self):
                self.sock.close()
                if os.getpid()==rootpid and not self.raised:
                    self.raised=True;raise OSError(errno.EIO,'synthetic_postfork_close')
            def __getattr__(self,name):return getattr(self.sock,name)
        def sockets():
            a,b=real();return a,CloseError(b)
        with patch.object(e.socket,'socketpair',side_effect=sockets):
            with self.assertRaisesRegex(OSError,'synthetic_postfork_close'):e._fork(m)
        self.assert_reaped(m)

    def test_real_child_socket_close_failure_cannot_return_into_root_caller(self):
        m=producer_fixture(self.root);rootpid=os.getpid();real=e.socket.socketpair
        class ChildCloseError:
            def __init__(self,sock):self.sock=sock
            def close(self):
                self.sock.close()
                if os.getpid()!=rootpid:raise OSError(errno.EIO,'synthetic_child_close')
            def __getattr__(self,name):return getattr(self.sock,name)
        def sockets():
            a,b=real();return ChildCloseError(a),b
        try:
            with patch.object(e.socket,'socketpair',side_effect=sockets):
                with self.assertRaises((Refused,OSError)):e._fork(m)
        finally:
            # A regressed producer must not duplicate the unittest runner.
            # Parent observes 91 if a fork child escaped into this caller.
            if os.getpid()!=rootpid:os._exit(91)
        row=self.assert_reaped(m);self.assertEqual(row['actual_exit_code'],1)

    def test_real_ownership_constructor_failure_keeps_raw_wait_owner(self):
        m=producer_fixture(self.root)
        with patch.object(e.processes,'_Ownership',side_effect=OSError(errno.EMFILE,'synthetic_ownership_init')):
            with self.assertRaisesRegex(OSError,'synthetic_ownership_init'):e._fork(m)
        self.assert_reaped(m)

    def test_real_persistent_identity_failure_still_waits_for_child(self):
        m=producer_fixture(self.root);rootpid=os.getpid();real=e.processes._identity
        def identity(pid):
            if os.getpid()==rootpid and pid!=rootpid:
                raise OSError(errno.EIO,'synthetic_child_identity')
            return real(pid)
        with patch.object(e.processes,'_identity',side_effect=identity):
            with self.assertRaisesRegex(OSError,'synthetic_child_identity'):e._fork(m)
        self.assert_reaped(m)

    def test_real_witness_failure_after_pidfd_registration_still_reaps(self):
        m=producer_fixture(self.root);rootpid=os.getpid();real=e.processes._identity;calls=[]
        def identity(pid):
            if os.getpid()==rootpid and pid!=rootpid:
                calls.append(pid)
                if len(calls)==3:raise OSError(errno.EIO,'synthetic_witness_identity')
            return real(pid)
        with patch.object(e.processes,'_identity',side_effect=identity):
            with self.assertRaisesRegex(OSError,'synthetic_witness_identity'):e._fork(m)
        self.assert_reaped(m)

    def test_wait_owner_constructor_failure_precedes_any_real_fork(self):
        m=producer_fixture(self.root)
        with patch.object(e.driver,'_Leader',side_effect=OSError(errno.ENOMEM,'synthetic_wait_owner')),\
             patch.object(e.os,'fork',side_effect=AssertionError('fork must not run')) as fork:
            with self.assertRaisesRegex(OSError,'synthetic_wait_owner'):e._fork(m)
        fork.assert_not_called();self.assertEqual(e.processes._children(os.getpid()),set())
        self.assertFalse(e._SERIAL.locked())

    def test_actual_setsid_descendant_ignoring_term_is_killed_and_reaped(self):
        m=producer_fixture(self.root)
        def escaped(child):
            reader,writer=os.pipe();pid=os.fork()
            if pid==0:
                os.close(reader);os.setsid();signal.signal(signal.SIGTERM,signal.SIG_IGN)
                os.write(writer,b'ready');os.close(writer)
                while True:signal.pause()
            os.close(writer)
            try:need(os.read(reader,5)==b'ready','synthetic_descendant_ready')
            finally:os.close(reader)
            return child.save('escaped-descendant',{'pid':pid,'synthetic':True})
        with patch.object(e,'_observe',side_effect=escaped),patch.object(e.processes,'TERM_GRACE',.1):
            with self.assertRaises(Refused):e._fork(m)
        # TERM can interrupt the failed leader before its handoff; neither a
        # missing handoff nor a descendant can be accepted as success. The
        # dedicated subreaper must retain all children through actual closure.
        self.assert_reaped(m)

    def test_second_fixed_install_branch_is_distinct_and_keeps_same_original_end(self):
        m=producer_fixture(self.root,e.INSTALL);end=m.original_end
        with patch.object(e,'_observe',side_effect=AssertionError('wrong source branch')),\
             patch.object(e,'_install',side_effect=lambda child:child.save('fixed-install',{'original_end':child.original_end})):
            ref,row=e._fork(m)
        self.assertEqual(row['actual_exit_code'],0);self.assertEqual(decode(e._ref(m.j,ref))['original_end'],end)
        self.assertEqual(m.original_end,end)


@unittest.skipUnless(os.geteuid()==0,'actual root-private /tmp install primitives')
class InstallerIntegration(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir='/tmp',prefix='initial-epoch-install-');self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700);self.m=producer_fixture(self.root,e.INSTALL)
        m=self.m;m.certificate_raw=canonical(certificate());m.certificate_ref={'phase':m.phase,
            'name':m.phase+'-certificate.json','sha256':m.j.save_bytes(m.phase+'-certificate.json',m.certificate_raw)}
        for name in ('control-a','control-b'):
            path=self.root/name;path.mkdir(mode=0o700);fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
            self.addCleanup(os.close,fd);m.control.append({'path':str(path),'fd':fd,'descriptor':e.certificates.root_descriptor(fd)})
        intent={'schema_version':1,'kind':'openlegal_initial_epoch_dual_install_intent','operation_id':m.cfg['operation_id'],
            'certificate':m.certificate_ref,'certificate_sha256':digest(m.certificate_raw),
            'head_sha256':digest(e.certificates.make_head(m.certificate_raw)),
            'roots':[{'path':r['path'],'descriptor':r['descriptor'],'expected_head':None} for r in m.control],
            'automatic_retry':False}
        m.install_intent={'phase':m.phase,'name':m.phase+'-dual-intent.json',
            'sha256':m.j.save(m.phase+'-dual-intent.json',intent)}
        m.install_scope=canonical({'synthetic':True})
        for name in ('seal','local'):
            p=patch.object(e._Producer,name,fixture_seal);p.start();self.addCleanup(p.stop)

    def test_real_two_root_installer_has_no_exit_ready_or_public_proof(self):
        m=self.m;ref=e._install(m);v=decode(e._ref(m.j,ref))
        self.assertEqual(v['certificate_sha256'],digest(m.certificate_raw));self.assertEqual(len(v['installations']),2)
        self.assertNotIn('actual_exit_code',v);self.assertNotIn('ready',v);self.assertEqual(v['intent'],m.install_intent)
        for r in m.control:
            self.assertEqual((Path(r['path'])/'current-head.json').read_bytes(),e.certificates.make_head(m.certificate_raw))
            self.assertEqual(e.certificates.root_descriptor(r['fd']),r['descriptor'])
        # ONLY the independent-device predicate is replaced; raw current-file
        # and inode checks are still performed, without physical acceptance.
        with patch.object(e.current,'_different_devices'):
            out=e._readback(m)
        self.assertEqual(decode(e._ref(m.j,out))['certificate_sha256'],digest(m.certificate_raw))

    def test_missing_intent_or_wrong_cert_binding_refuses_before_any_control_effect(self):
        m=self.m;del m.install_scope
        with self.assertRaisesRegex(Refused,'intent_required'):e._install(m)
        m.install_scope=b'synthetic';m.certificate_raw=canonical(certificate())
        with self.assertRaisesRegex(Refused,'intent_binding'):e._install(m)
        self.assertTrue(all(not list(Path(r['path']).iterdir()) for r in m.control))

    def test_first_root_fsync_unknown_preserves_marker_and_never_touches_second_or_retries(self):
        m=self.m;real=os.fsync;calls=[]
        def failed(fd):
            calls.append(fd)
            if len(calls)==1:raise OSError(errno.EIO,'synthetic_fsync_unknown')
            return real(fd)
        with patch.object(e.certificates.os,'fsync',side_effect=failed):
            with self.assertRaisesRegex(OSError,'synthetic_fsync_unknown'):e._install(m)
        names={r.name for r in Path(m.control[0]['path']).iterdir()}
        self.assertTrue(any(n.startswith('.install-intent-') for n in names))
        self.assertFalse(list(Path(m.control[1]['path']).iterdir()))
        with self.assertRaises((Refused,FileExistsError)):e._install(m)
        self.assertFalse(list(Path(m.control[1]['path']).iterdir()))

    def test_independent_dual_readback_detects_split_heads(self):
        m=self.m;e._install(m)
        p=Path(m.control[1]['path'])/'current-head.json';p.write_bytes(e.certificates.make_head(canonical(certificate())))
        with patch.object(e.current,'_different_devices'):
            with self.assertRaises((Refused,FileNotFoundError)):e._readback(m)

    def test_constructor_uses_actual_pair_sql_reserve_and_global_budget(self):
        m=self.m;b=m.b;b._initial_pg_pair_retained=[]
        seedfd=m.control[1]['fd'];b._initial_blob_seed_fds={'root-cache_primary':(seedfd,m.control[1]['path'],('actual','four','tuple'),[])}
        cfg={'operation_id':m.j.operation,'epoch_certificate':{},'backup_root':{'path':str(self.root)},'source_graph':{}}
        p=types.SimpleNamespace(j=m.j,budget={'baseline_raw_bytes':100,'baseline_bundles':1},
            baseline={'this_is':'a_blob_manifest_not_SQL_metadata'},role_context={'names':{},'runtime':'runtime'},
            raw=b'pair',authraw=b'auth',work={})
        lock=types.SimpleNamespace(fd=m.control[0]['fd'])
        x=e._Producer(b,lock,{}, {},(cfg,{},canonical(cfg),canonical({})),p,{})
        self.assertEqual(x.count_end,m.j.count+384);self.assertEqual(x.bytes_end,m.j.bytes+128*1024**2)
        self.assertIn(seedfd,x.inherited)
        self.assertGreater(e.COMMAND_HEADROOM,4*e.linux.MAX_OUTPUT)
        p.budget['baseline_raw_bytes']=100*1024**2
        with self.assertRaisesRegex(Refused,'metadata_estimated_bound'):e._Producer(b,lock,{}, {},(cfg,{},canonical(cfg),canonical({})),p,{})
        p.budget['baseline_raw_bytes']=100;m.j.count=4096-383
        with self.assertRaisesRegex(Refused,'global_budget'):e._Producer(b,lock,{}, {},(cfg,{},canonical(cfg),canonical({})),p,{})




@unittest.skipUnless(os.geteuid()==0,'actual root private /tmp inputs')
class FinalSealDeadline(unittest.TestCase):
    def test_expiry_during_final_journal_read_cannot_pass_seal(self):
        # Isolate the final-read boundary. Entry/FD/native qualification is a
        # separate port here; all pinned input reads and the original end are real.
        with tempfile.TemporaryDirectory(dir='/tmp') as td:
            root=Path(td);ref=private(root,'dependency.json',{})
            q={k:ref for k in ('original_inventory','driver_inventory','driver_authority',
                'seed_inventory','seed_authority','strict_preprovision','pair_inventory','pair_authority')}
            cfg={'operation_id':str(uuid.uuid4()),'source_graph':{},'epoch_certificate':q};auth={'valid_until':int(time.time())+300}
            inv=private(root,'inventory.json',cfg);approval=private(root,'authority.json',auth)
            backend=e.driver.Backend();backend._driver_deadline=time.monotonic()+5
            end=backend._driver_deadline;clock_patch=None
            def late_read():
                nonlocal clock_patch
                clock_patch=patch.object(e.time,'monotonic',return_value=end+1)
                clock_patch.start()
            journal=types.SimpleNamespace(check=late_read,count=0,bytes=0,
                phase_paths=[(root/e.OBSERVATION,{})])
            work={'fixture_only':'native context separately qualified'};value={}
            m=types.SimpleNamespace(b=backend,j=journal,inventory_ref=inv,authority_ref=approval,
                raw=canonical(cfg),authraw=canonical(auth),cfg=cfg,auth=auth,input_raw=canonical(cfg),
                q=q,p=types.SimpleNamespace(work=work),pair_value=value,
                pair_raw=(canonical({}),canonical({}),canonical(work),canonical(value)),
                phase=e.OBSERVATION,count_end=e.RECORDS,bytes_end=e.BYTES,
                local=lambda:e.driver._check_end(backend))
            try:
                with patch.object(e,'_source_seal',return_value=None), \
                     patch.object(e.driver,'_docker_binding',return_value=None), \
                     self.assertRaisesRegex(Refused,'driver_original_deadline'):
                    e._Producer.seal(m)
            finally:
                if clock_patch is not None:clock_patch.stop()


if __name__=='__main__':unittest.main()
