"""Pure schema and real private /tmp FD/fsync/CAS tests, no native runtime.

The one independent child tests only an owned temporary-directory flock.
Nothing creates authority, observations, Docker/SQL/provider actors or Ready.
Temporary filesystem durability is not independent physical-media acceptance.
"""
import copy
import errno
import fcntl
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import time
import unittest
import uuid
from unittest.mock import patch

BASE=Path(__file__).resolve().parents[1];sys.path.insert(0,str(BASE))
import epoch_certificate as c
import epoch_observation as o
from interface import Refused
from inventory import canonical,digest


def uid():return str(uuid.uuid4())

def private_ref(name):return {'path':'/unopened/'+name,'sha256':'a'*64}

def journal_ref(name):return {'phase':'initial_epoch_observation','name':name+'.json','sha256':'b'*64}

def certificate(*,previous=None,epoch=None):
    managed=epoch or (c.validate_head(previous)['managed_epoch'] if previous else uid());boot=uid()
    root={'path':'/unopened/data','dev':11,'ino':12,'uid':70,'gid':70,'mode':0o700,'kernel_filesystem_id':13}
    def native(side,token):
        return {'container_id':token*64,'image_id':'sha256:'+'3'*64,'postmaster_started_at':'2026-11-02 14:00:00+00',
            'data_directory':'/var/lib/postgresql','data_root':dict(root,path='/unopened/pg/'+side),
            'process':{'pid':101 if side=='primary' else 102,'start_ticks':'1000','boot_id':boot}}
    subject={'managed_epoch':managed,'host_boot_id':boot,'runtime':{'revision':'c'*40,
        'image_id':'sha256:'+'d'*64,'configuration_sha256':'e'*64,'migration_table_sha256':'f'*64},
        'primary':native('primary','1'),'standby':native('standby','2'),'control':{
            'managed_epoch':managed,'primary_system_identifier':'12345','timeline':1,'standby_name':'strict_peer',
            'standby_role':'strict_peer_role','standby_addr':'192.0.2.2','standby_client_dn':'CN=peer',
            'standby_issuer_dn':'CN=ca','standby_client_serial':'123','blob_binding_sha256':'0'*64},
        'blob_roots':{},'original_inventory':private_ref('original.json'),'topology':o.TOPOLOGY,
        'ssd_survivors':list(o.SSD_SURVIVORS),'hdd_lost':list(o.HDD_LOST)}
    for n,role in enumerate(c.ROLES):
        subject['blob_roots'][role]=dict(root,path='/unopened/blobs/'+role,ino=100+n,uid=10004,gid=10004,
            container_path='/blobs/'+role,filesystem_id='fixture-'+role,marker_sha256=digest(role.encode()))
    subject['control']['blob_binding_sha256']=o._subject_blob_binding(subject)
    prov={'operation_id':uid(),'reason':'controlled_restart' if previous else 'initial_bind'}
    prov.update({k:private_ref(k) for k in c.PRIVATE_REFERENCES})
    prov.update({k:journal_ref(k) for k in c.JOURNAL_REFERENCES})
    old=c.validate_head(previous) if previous else None
    return {'schema_version':2,'kind':'openlegal_strict_epoch_certificate','certificate_id':uid(),
        'generation':old['generation']+1 if old else 1,
        'predecessor_certificate_sha256':old['certificate_sha256'] if old else None,
        'subject':subject,'provenance':prov}


class PureSchemas(unittest.TestCase):
    def test_closed_subject_and_provenance_are_pure_without_emergency_or_unavailable_reads(self):
        value=certificate();raw=canonical(value)
        with patch('builtins.open',side_effect=AssertionError('pure validator opened a path')),\
             patch.object(os,'open',side_effect=AssertionError('pure validator opened a path')):
            self.assertEqual(c.validate_certificate(raw),value)
            head=c.make_head(raw);self.assertEqual(c.validate_heads([head,head]),c.validate_head(head))
        self.assertIs(c.observation._subject_blob_binding,o._subject_blob_binding)
        self.assertEqual(o._subject_blob_binding(value['subject']),value['subject']['control']['blob_binding_sha256'])
        self.assertNotIn('emergency_backend',c.__dict__);self.assertNotIn('emergency_native',c.__dict__)
    def test_extra_fields_never_become_execution_or_actual_exit_proof(self):
        for path,key in (((),'approved'),(('subject',),'ready'),(('provenance',),'actual_exit_code'),
                         (('provenance','observation_child_exit'),'actual_exit_code')):
            v=certificate();row=v
            for part in path:row=row[part]
            row[key]=0
            with self.subTest(path=path),self.assertRaises(Refused):c.validate_certificate(canonical(v))
        for field in ('producer_source','source_graph','inventory','authority'):
            v=certificate();v['provenance'][field]['path']='relative/source'
            with self.assertRaises(Refused):c.validate_certificate(canonical(v))
    def test_canonical_duplicate_nonfinite_depth_and_byte_caps_refuse(self):
        raw=canonical(certificate())
        for bad in (b' '+raw,raw[:-1],raw.replace(b'"schema_version":2',b'"schema_version":2,"schema_version":2'),
                    b'{"a":NaN}',b'{"a":"\\ud800"}',b'['*34+b'0'+b']'*34,b'x'*(c.CERTIFICATE_LIMIT+1)):
            with self.subTest(prefix=bad[:12]),self.assertRaises(Refused):c.validate_certificate(bad)
        with self.assertRaises(Refused):c.validate_head(b'{}'*(c.HEAD_LIMIT//2+1))
    def test_wrong_topology_role_alias_binding_pid_and_native_shapes_refuse(self):
        for kind in ('topology','roles','binding','pid','root-extra','native-alias'):
            v=certificate();s=v['subject']
            if kind=='topology':s['topology']='all_replicas_ssd'
            if kind=='roles':s['ssd_survivors']=['cache_replica','corpus_replica']
            if kind=='binding':s['blob_roots']['corpus_primary']['marker_sha256']='8'*64
            if kind=='pid':s['standby']['process']['pid']=True
            if kind=='root-extra':s['blob_roots']['corpus_primary']['invented']=True
            if kind=='native-alias':s['standby']['container_id']=s['primary']['container_id']
            with self.subTest(kind=kind),self.assertRaises(Refused):c.validate_certificate(canonical(v))
    def test_generation_predecessor_and_kind_are_closed(self):
        for key,value in (('generation',True),('generation',0),('generation',2**63),('generation',1.0),
              ('predecessor_certificate_sha256','1'*64),('schema_version',1),('certificate_id','../escape')):
            v=certificate();v[key]=value
            with self.subTest(key=key,value=value),self.assertRaises(Refused):c.validate_certificate(canonical(v))
    def test_first_and_renewal_lineage_bind_exact_previous_certificate_hash(self):
        one=canonical(certificate());h1=c.make_head(one);two=canonical(certificate(previous=h1))
        self.assertEqual(c.validate_transition(one,None),h1)
        self.assertEqual(c.validate_transition(two,h1),c.make_head(two))
        v=certificate(previous=h1,epoch=uid());v['provenance']['reason']='healthy_reseed'
        self.assertEqual(c.validate_head(c.validate_transition(canonical(v),h1))['generation'],2)
        for bad in (None,c.make_head(canonical(certificate()))):
            with self.assertRaises(Refused):c.validate_transition(two,bad)
        v=certificate(previous=h1);v['certificate_id']=c.validate_head(h1)['certificate_id']
        with self.assertRaises(Refused):c.validate_transition(canonical(v),h1)
        with self.assertRaises(Refused):c.validate_transition(one,h1)
    def test_reason_generation_and_epoch_combinations_cannot_relabel_lineage(self):
        first=canonical(certificate());head=c.make_head(first)
        for reason in ('controlled_restart','healthy_reseed'):
            value=certificate();value['provenance']['reason']=reason
            with self.subTest(first_reason=reason),self.assertRaisesRegex(Refused,'reason_generation'):
                c.validate_certificate(canonical(value))
        later=certificate(previous=head);later['provenance']['reason']='initial_bind'
        with self.assertRaisesRegex(Refused,'reason_generation'):c.validate_certificate(canonical(later))
        for reason,same,allowed in (('controlled_restart',True,True),('controlled_restart',False,False),
                                  ('healthy_reseed',True,False),('healthy_reseed',False,True)):
            value=certificate(previous=head,epoch=c.validate_head(head)['managed_epoch'] if same else uid())
            value['provenance']['reason']=reason;raw=canonical(value)
            with self.subTest(reason=reason,same_epoch=same):
                if allowed:self.assertEqual(c.validate_transition(raw,head),c.make_head(raw))
                else:
                    with self.assertRaisesRegex(Refused,'reason_epoch_changed'):c.validate_transition(raw,head)

    def test_split_head_and_absent_later_lineage_never_fallback(self):
        h1=c.make_head(canonical(certificate()));h2=c.make_head(canonical(certificate(previous=h1)))
        self.assertIsNone(c.validate_heads([None,None]))
        for heads in ([h1,None],[h1,h2],[h1],[h1,b' '+h1]):
            with self.assertRaises(Refused):c.validate_heads(heads)
        with self.assertRaises(Refused):c.validate_transition(canonical(certificate(previous=h1)),None)


@unittest.skipUnless(os.geteuid()==0,'root-private /tmp installation gate')
class DescriptorInstall(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='epoch-install-',dir='/tmp');self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name);self.path.chmod(0o700)
        self.fd=os.open(self.path,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
        self.addCleanup(os.close,self.fd);self.desc=c.root_descriptor(self.fd)
        self.raw=canonical(certificate());self.end=time.monotonic()+30
    def install(self,raw=None,old=None):
        return c.install_one(self.fd,self.desc,self.raw if raw is None else raw,old,self.end)
    def names(self):return {p.name for p in self.path.iterdir()}
    def test_real_first_install_and_same_epoch_renewal_preserve_immutable_history(self):
        first=self.install();h1=(self.path/c.HEAD_NAME).read_bytes();self.assertEqual(h1,c.make_head(self.raw))
        two=canonical(certificate(previous=h1));second=self.install(two,h1)
        self.assertEqual((self.path/c.HEAD_NAME).read_bytes(),c.make_head(two))
        self.assertEqual((self.path/first['certificate_basename']).read_bytes(),self.raw)
        for facts in (first,second):
            self.assertEqual(digest((self.path/facts['intent_basename']).read_bytes()),facts['intent_sha256'])
            for key in ('intent_basename','certificate_basename','head_basename'):
                st=(self.path/facts[key]).stat();self.assertEqual((st.st_uid,st.st_gid,stat.S_IMODE(st.st_mode)),(0,0,0o600))
            for forbidden in ('status','passed','actual_exit_code','producer_exit','owned_processes_closed','ready','authority'):
                self.assertNotIn(forbidden,facts)
        self.assertEqual(len([n for n in self.names() if n.startswith('.install-intent-')]),2)
        self.assertFalse(any(n.startswith('.pending-head-') for n in self.names()))
        self.assertEqual(c.root_descriptor(self.fd),self.desc)
    def test_missing_expected_head_and_wrong_root_inode_refuse_before_intent(self):
        wrong=dict(self.desc,ino=self.desc['ino']+1)
        with self.assertRaisesRegex(Refused,'root_changed'):c.install_one(self.fd,wrong,self.raw,None,self.end)
        self.assertEqual(self.names(),set())
        self.install();old=(self.path/c.HEAD_NAME).read_bytes();before=self.names()
        with self.assertRaisesRegex(Refused,'expected_head_changed'):self.install(canonical(certificate()),None)
        self.assertEqual(self.names(),before)
        other=self.path/'another';other.mkdir(mode=0o700);fd=os.open(other,os.O_RDONLY|os.O_DIRECTORY)
        try:
            with self.assertRaisesRegex(Refused,'root_changed'):c.install_one(fd,self.desc,self.raw,None,self.end)
        finally:os.close(fd)
    def test_failure_before_certificate_write_keeps_durable_once_intent_and_blocks_retry(self):
        original=c._write_new;calls=[]
        def fail(root,name,*args):
            calls.append(name)
            if name.startswith('certificate-'):raise OSError(errno.EIO,'synthetic before certificate creation')
            return original(root,name,*args)
        with patch.object(c,'_write_new',side_effect=fail):
            with self.assertRaises(OSError):self.install()
        self.assertEqual(len(self.names()),1);intent=next(iter(self.names()));self.assertTrue(intent.startswith('.install-intent-'))
        saved=(self.path/intent).read_bytes()
        with patch.object(c,'_write_new',wraps=original) as again:
            with self.assertRaises(FileExistsError):self.install()
        self.assertEqual(again.call_count,1);self.assertEqual((self.path/intent).read_bytes(),saved)
        self.assertFalse(any(n.startswith('certificate-') for n in self.names()))
    def test_intent_directory_fsync_failure_retains_marker_and_never_creates_certificate(self):
        original=os.fsync
        def fail(fd):
            if fd==self.fd:raise OSError(errno.EIO,'synthetic intent directory fsync')
            return original(fd)
        with patch.object(os,'fsync',side_effect=fail):
            with self.assertRaises(OSError):self.install()
        self.assertEqual(len(self.names()),1);self.assertTrue(next(iter(self.names())).startswith('.install-intent-'))
        with self.assertRaises(FileExistsError):self.install()
    def test_late_directory_fsync_failure_retains_new_head_without_success_or_rollback(self):
        original=os.fsync;count=0
        def fail(fd):
            nonlocal count
            if fd==self.fd:
                count+=1
                if count==4:raise OSError(errno.EIO,'synthetic after head rename')
            return original(fd)
        with patch.object(os,'fsync',side_effect=fail):
            with self.assertRaises(OSError):self.install()
        self.assertEqual((self.path/c.HEAD_NAME).read_bytes(),c.make_head(self.raw))
        self.assertTrue(any(n.startswith('.install-intent-') for n in self.names()))
        with self.assertRaises(Refused):self.install()
    def test_readback_hash_corruption_cannot_return_durable_facts(self):
        original=c._open_file;corrupted=False
        def corrupt(root,name,*args,**kwargs):
            nonlocal corrupted
            if name==c.HEAD_NAME and not kwargs.get('missing_ok') and not corrupted:
                corrupted=True;(self.path/c.HEAD_NAME).write_bytes(c.make_head(canonical(certificate())))
            return original(root,name,*args,**kwargs)
        with patch.object(c,'_open_file',side_effect=corrupt):
            with self.assertRaisesRegex(Refused,'final_head_changed'):self.install()
        self.assertTrue(corrupted);self.assertTrue(any(n.startswith('certificate-') for n in self.names()))
    def test_same_raw_head_replacement_inode_is_detected_before_cas(self):
        self.install();old=(self.path/c.HEAD_NAME).read_bytes();raw=canonical(certificate(previous=old))
        original=c._directory_sync;count=0
        def change(*args):
            nonlocal count
            original(*args);count+=1
            if count==3:
                replacement=self.path/'replacement';replacement.write_bytes(old);replacement.chmod(0o600)
                os.replace(replacement,self.path/c.HEAD_NAME)
        with patch.object(c,'_directory_sync',side_effect=change):
            # Replacing the name unlinks the held old inode: nlink0 is refused
            # before the later explicit path/inode comparison.
            with self.assertRaisesRegex(Refused,'private_file'):self.install(raw,old)
        self.assertEqual((self.path/c.HEAD_NAME).read_bytes(),old)
        self.assertTrue(any(n.startswith('.pending-head-') for n in self.names()))
    def test_head_certificate_and_pending_symlinks_are_never_followed(self):
        outside=self.path/'outside';outside.write_bytes(b'untouched');outside.chmod(0o600)
        (self.path/c.HEAD_NAME).symlink_to(outside)
        with self.assertRaises(OSError):self.install()
        self.assertFalse(any(n.startswith('.install-intent-') for n in self.names()))
        (self.path/c.HEAD_NAME).unlink();cid=c.validate_certificate(self.raw)['certificate_id']
        cert=self.path/('certificate-'+cid+'.json');cert.symlink_to(outside)
        with self.assertRaises(FileExistsError):self.install()
        self.assertEqual(outside.read_bytes(),b'untouched')
        # A distinct fresh certificate owns a distinct intent, and cannot follow
        # a malicious pending symlink either. This is not retrying the old ID.
        raw=canonical(certificate());cid=c.validate_certificate(raw)['certificate_id']
        (self.path/('.pending-head-'+cid+'.json')).symlink_to(outside)
        with self.assertRaises(FileExistsError):self.install(raw)
        self.assertEqual(outside.read_bytes(),b'untouched')
    def test_head_hardlink_and_nonprivate_mode_refuse_without_new_intent(self):
        self.install();old=(self.path/c.HEAD_NAME).read_bytes();new=canonical(certificate(previous=old));before=self.names()
        (self.path/c.HEAD_NAME).chmod(0o640)
        with self.assertRaisesRegex(Refused,'private_file'):self.install(new,old)
        (self.path/c.HEAD_NAME).chmod(0o600)
        os.link(self.path/c.HEAD_NAME,self.path/'linked')
        with self.assertRaisesRegex(Refused,'private_file'):self.install(new,old)
        self.assertEqual(self.names(),before|{'linked'})
    def test_expired_deadline_refuses_before_effect_and_late_expiry_keeps_intent(self):
        with self.assertRaisesRegex(Refused,'deadline'):c.install_one(self.fd,self.desc,self.raw,None,time.monotonic()-1)
        self.assertEqual(self.names(),set());original=c._write_new;clock=[time.monotonic()]
        def expire(*args):
            result=original(*args);clock[0]=self.end+1;return result
        with patch.object(c.time,'monotonic',side_effect=lambda:clock[0]),patch.object(c,'_write_new',side_effect=expire):
            with self.assertRaisesRegex(Refused,'deadline'):self.install()
        self.assertEqual(len(self.names()),1)
    def test_split_media_partial_install_is_not_dual_acceptance(self):
        self.install();head=(self.path/c.HEAD_NAME).read_bytes()
        with tempfile.TemporaryDirectory(dir='/tmp',prefix='epoch-second-') as name:
            path=Path(name);path.chmod(0o700);fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
            try:
                with patch.object(c,'_directory_sync',side_effect=OSError(errno.EIO,'synthetic second-media fsync')):
                    with self.assertRaises(OSError):c.install_one(fd,c.root_descriptor(fd),self.raw,None,self.end)
                self.assertFalse((path/c.HEAD_NAME).exists())
                with self.assertRaisesRegex(Refused,'split_heads'):c.validate_heads([head,None])
            finally:os.close(fd)
    def test_same_root_independent_open_description_and_process_cannot_take_retained_lock(self):
        self.install();other=os.open(self.path,os.O_RDONLY|os.O_DIRECTORY)
        try:
            with self.assertRaises(BlockingIOError):fcntl.flock(other,fcntl.LOCK_EX|fcntl.LOCK_NB)
        finally:os.close(other)
        read,write=os.pipe();pid=os.fork()
        if pid==0:
            os.close(read);fd=os.open(self.path,os.O_RDONLY|os.O_DIRECTORY)
            try:
                try:c.install_one(fd,c.root_descriptor(fd),canonical(certificate()),None,time.monotonic()+2)
                except Refused as error:
                    os.write(write,b'1' if str(error)=='epoch_install_lock_unavailable' else b'0')
                else:os.write(write,b'0')
            finally:os.close(fd);os.close(write)
            os._exit(0)
        os.close(write)
        try:self.assertEqual(os.read(read,1),b'1')
        finally:os.close(read);_,status=os.waitpid(pid,0)
        self.assertEqual(os.waitstatus_to_exitcode(status),0)


if __name__=='__main__':unittest.main(verbosity=2)
