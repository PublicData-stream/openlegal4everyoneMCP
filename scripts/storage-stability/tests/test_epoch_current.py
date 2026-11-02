"""Real private /tmp current-head reads; no native/runtime/authority proof.

Pair-positive tests replace ONLY the distinct-device predicate. Every root/file
FD, raw byte, mode, inode, rename and recheck stays real. This explicitly does
not establish independent physical filesystems or hardware/WWID qualification.
"""
import fcntl
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

BASE=Path(__file__).resolve().parents[1];sys.path.insert(0,str(BASE))
import epoch_certificate as c
import epoch_current as current
from interface import Refused
from inventory import canonical,digest
from test_epoch_certificate import certificate,uid


@unittest.skipUnless(os.geteuid()==0,'root-private /tmp current-head gate')
class CurrentReads(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='epoch-current-',dir='/tmp');self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name);self.path.chmod(0o700)
        self.fd=os.open(self.path,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
        self.addCleanup(os.close,self.fd);self.desc=c.root_descriptor(self.fd)
        self.end=time.monotonic()+20;self.raw=canonical(certificate())
        self.facts=c.install_one(self.fd,self.desc,self.raw,None,self.end)
        self.head=c.make_head(self.raw);self.certpath=self.path/self.facts['certificate_basename']

    def read(self,**kwargs):return current.read_current(self.fd,self.desc,self.end,**kwargs)

    def assert_root_held(self):self.assertEqual(c.root_descriptor(self.fd),self.desc)

    def test_real_installed_current_raw_hash_and_inode_snapshot(self):
        before={p.name:p.read_bytes() for p in self.path.iterdir()}
        with patch.object(fcntl,'flock',side_effect=AssertionError('reader changed caller flock')):
            snapshot=self.read(expected_certificate_sha256=digest(self.raw))
        self.assertEqual(set(snapshot),{'root_identity','head_raw','head_sha256','head_identity',
            'certificate_basename','certificate_raw','certificate_sha256','certificate_identity'})
        self.assertEqual(snapshot['head_raw'],self.head);self.assertEqual(snapshot['certificate_raw'],self.raw)
        self.assertEqual(snapshot['head_sha256'],digest(self.head));self.assertEqual(snapshot['certificate_sha256'],digest(self.raw))
        self.assertEqual(snapshot['certificate_basename'],self.facts['certificate_basename'])
        for key,path in (('head',self.path/c.HEAD_NAME),('certificate',self.certpath)):
            st=path.stat();row=snapshot[key+'_identity']
            self.assertEqual((row['dev'],row['ino'],row['uid'],row['gid'],row['mode'],row['nlink'],row['size']),
                (st.st_dev,st.st_ino,0,0,0o600,1,len(snapshot[key+'_raw'])))
        self.assertEqual(before,{p.name:p.read_bytes() for p in self.path.iterdir()});self.assert_root_held()

    def test_same_epoch_restart_and_new_epoch_renewal_follow_actual_head(self):
        old=self.raw;oldhead=self.head
        for reason,new_epoch in (('controlled_restart',None),('healthy_reseed',uid())):
            value=certificate(previous=oldhead,epoch=new_epoch);value['provenance']['reason']=reason
            raw=canonical(value);c.install_one(self.fd,self.desc,raw,oldhead,self.end)
            result=self.read();self.assertEqual(result['certificate_raw'],raw)
            self.assertEqual(result['head_raw'],c.make_head(raw));self.assertTrue(self.certpath.exists())
            self.assertEqual(self.certpath.read_bytes(),old)
            oldhead=c.make_head(raw)
        self.assert_root_held()

    def test_obsolete_pin_refuses_before_opening_any_old_or_new_certificate(self):
        fresh=canonical(certificate(previous=self.head));c.install_one(self.fd,self.desc,fresh,self.head,self.end)
        opened=[];original=c._open_file
        def record(root,name,*args,**kwargs):opened.append(name);return original(root,name,*args,**kwargs)
        with patch.object(c,'_open_file',side_effect=record):
            with self.assertRaisesRegex(Refused,'expected_certificate_changed'):
                self.read(expected_certificate_sha256=digest(self.raw))
        self.assertEqual(opened,[c.HEAD_NAME]);self.assertEqual(self.certpath.read_bytes(),self.raw)
        self.assert_root_held()

    def test_missing_current_head_or_selected_certificate_has_no_historical_fallback(self):
        self.certpath.unlink()
        with self.assertRaises(FileNotFoundError):self.read()
        (self.path/c.HEAD_NAME).unlink()
        with self.assertRaises(FileNotFoundError):self.read()
        self.assert_root_held()

    def test_invalid_head_cannot_select_a_certificate_path(self):
        original=c._open_file
        for raw in (b' '+self.head,self.head.replace(b'"certificate_id":"',b'"certificate_id":"../'),b'{}\n'):
            (self.path/c.HEAD_NAME).write_bytes(raw);opened=[]
            def record(root,name,*args,**kwargs):opened.append(name);return original(root,name,*args,**kwargs)
            with patch.object(c,'_open_file',side_effect=record):
                with self.assertRaises(Refused):self.read()
            self.assertEqual(opened,[c.HEAD_NAME]);self.assert_root_held()

    def test_wrong_hash_and_head_certificate_identity_mismatch_refuse(self):
        value=c.validate_head(self.head);value['certificate_sha256']='1'*64
        (self.path/c.HEAD_NAME).write_bytes(canonical(value))
        with self.assertRaisesRegex(Refused,'head_certificate_mismatch'):self.read()
        value=c.validate_head(self.head);value['managed_epoch']=uid()
        (self.path/c.HEAD_NAME).write_bytes(canonical(value))
        with self.assertRaisesRegex(Refused,'head_certificate_mismatch'):self.read()
        self.assert_root_held()

    def test_head_or_certificate_symlink_never_follows_target(self):
        for name in (c.HEAD_NAME,self.facts['certificate_basename']):
            target=self.path/name;saved=target.read_bytes();outside=self.path/('outside-'+name)
            target.rename(outside);target.symlink_to(outside)
            with self.subTest(name=name),self.assertRaises(OSError):self.read()
            self.assertEqual(outside.read_bytes(),saved);target.unlink();outside.rename(target)
            self.assert_root_held()

    def test_hardlink_owner_and_mode_are_not_private_files(self):
        for name in (c.HEAD_NAME,self.facts['certificate_basename']):
            target=self.path/name;link=self.path/('linked-'+name);os.link(target,link)
            with self.assertRaisesRegex(Refused,'private_file'):self.read()
            link.unlink();target.chmod(0o640)
            with self.assertRaisesRegex(Refused,'private_file'):self.read()
            target.chmod(0o600);os.chown(target,10004,10004)
            with self.assertRaisesRegex(Refused,'private_file'):self.read()
            os.chown(target,0,0);self.assert_root_held()

    def test_nonprivate_root_and_wrong_descriptor_preserve_supplied_fd(self):
        with self.assertRaisesRegex(Refused,'root_changed'):
            current.read_current(self.fd,dict(self.desc,ino=self.desc['ino']+1),self.end)
        self.path.chmod(0o750)
        with self.assertRaisesRegex(Refused,'root_changed'):self.read()
        self.path.chmod(0o700);os.chown(self.path,10004,10004)
        with self.assertRaisesRegex(Refused,'root_changed'):self.read()
        os.chown(self.path,0,0);self.assert_root_held()

    def test_same_bytes_head_inode_swap_during_certificate_open_refuses(self):
        original=c._open_file;changed=[]
        def swap(root,name,*args,**kwargs):
            result=original(root,name,*args,**kwargs)
            if name==self.facts['certificate_basename'] and not changed:
                changed.append(True);replacement=self.path/'new-head';replacement.write_bytes(self.head)
                replacement.chmod(0o600);os.replace(replacement,self.path/c.HEAD_NAME)
            return result
        with patch.object(c,'_open_file',side_effect=swap):
            with self.assertRaises(Refused):self.read()
        self.assertEqual((self.path/c.HEAD_NAME).read_bytes(),self.head);self.assert_root_held()

    def test_same_inode_head_bytes_change_during_certificate_open_refuses(self):
        original=c._open_file;changed=[];other=c.make_head(canonical(certificate()))
        def modify(root,name,*args,**kwargs):
            result=original(root,name,*args,**kwargs)
            if name==self.facts['certificate_basename'] and not changed:
                changed.append(True);(self.path/c.HEAD_NAME).write_bytes(other)
            return result
        with patch.object(c,'_open_file',side_effect=modify):
            with self.assertRaises(Refused):self.read()
        self.assert_root_held()

    def test_certificate_inode_swap_at_independent_readback_refuses(self):
        original=c._open_file;count=[]
        def swap(root,name,*args,**kwargs):
            if name==self.facts['certificate_basename']:
                count.append(name)
                if len(count)==2:
                    replacement=self.path/'new-cert';replacement.write_bytes(self.raw);replacement.chmod(0o600)
                    os.replace(replacement,self.certpath)
            return original(root,name,*args,**kwargs)
        with patch.object(c,'_open_file',side_effect=swap):
            with self.assertRaises(Refused):self.read()
        self.assertEqual(self.certpath.read_bytes(),self.raw);self.assert_root_held()

    def test_head_swap_during_independent_certificate_readback_is_rechecked(self):
        original=c._open_file;count=[]
        def swap(root,name,*args,**kwargs):
            result=original(root,name,*args,**kwargs)
            if name==self.facts['certificate_basename']:
                count.append(name)
                if len(count)==2:
                    replacement=self.path/'late-head';replacement.write_bytes(self.head);replacement.chmod(0o600)
                    os.replace(replacement,self.path/c.HEAD_NAME)
            return result
        with patch.object(c,'_open_file',side_effect=swap):
            with self.assertRaises(Refused):self.read()
        self.assert_root_held()

    def test_late_physical_certificate_read_cannot_return_snapshot(self):
        original=os.read;inode=self.certpath.stat().st_ino;delayed=[]
        def late(fd,size):
            raw=original(fd,size)
            if os.fstat(fd).st_ino==inode and not delayed:
                delayed.append(True);time.sleep(.06)
            return raw
        with patch.object(os,'read',side_effect=late):
            with self.assertRaisesRegex(Refused,'deadline'):
                current.read_current(self.fd,self.desc,time.monotonic()+.03)
        self.assertTrue(delayed);self.assert_root_held()

    def test_reader_never_opens_an_install_intent_completion_or_supplied_path(self):
        original=os.open;opened=[]
        def record(path,*args,**kwargs):
            if kwargs.get('dir_fd')==self.fd:opened.append(path)
            return original(path,*args,**kwargs)
        with patch.object(os,'open',side_effect=record):self.read()
        self.assertTrue(opened);self.assertEqual(set(opened),{c.HEAD_NAME,self.facts['certificate_basename']})
        self.assert_root_held()

    def test_caller_flock_survives_reader_success_and_failure(self):
        def contender():
            read,write=os.pipe();pid=os.fork()
            if pid==0:
                os.close(read);fd=os.open(self.path,os.O_RDONLY|os.O_DIRECTORY)
                try:
                    try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);answer=b'unlocked'
                    except BlockingIOError:answer=b'held'
                    os.write(write,answer);os._exit(0)
                finally:os._exit(1)
            os.close(write)
            try:answer=os.read(read,32);found,status=os.waitpid(pid,0)
            finally:os.close(read)
            self.assertEqual(found,pid);self.assertEqual(os.waitstatus_to_exitcode(status),0)
            self.assertEqual(answer,b'held')
        contender();self.read();contender()
        with self.assertRaises(Refused):self.read(expected_certificate_sha256='0'*64)
        contender();self.assert_root_held()


@unittest.skipUnless(os.geteuid()==0,'root-private /tmp paired-current gate')
class PairReads(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='epoch-current-pair-',dir='/tmp');self.addCleanup(self.tmp.cleanup)
        self.paths=[Path(self.tmp.name)/name for name in ('one','two')];self.fds=[];self.desc=[]
        self.raw=canonical(certificate());self.end=time.monotonic()+20
        for path in self.paths:
            path.mkdir(mode=0o700);fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
            self.fds.append(fd);self.addCleanup(os.close,fd);self.desc.append(c.root_descriptor(fd))
            c.install_one(fd,self.desc[-1],self.raw,None,self.end)

    def synthetic_external_device_qualification(self):
        # Only this hardware/filesystem gate is substituted. Actual metadata,
        # held root/file FDs, bytes and all rechecks remain untouched.
        return patch.object(current,'_different_devices',return_value=None)

    def test_actual_same_filesystem_and_alias_roots_refuse_without_any_file_open(self):
        with patch.object(c,'_open_file',side_effect=AssertionError('geometry must precede data open')):
            with self.assertRaisesRegex(Refused,'same_filesystem'):current.read_pair(self.fds,self.desc,self.end)
            with self.assertRaisesRegex(Refused,'distinct_root_fds'):
                current.read_pair([self.fds[0]]*2,[self.desc[0]]*2,self.end)
            duplicate=os.dup(self.fds[0])
            try:
                with self.assertRaisesRegex(Refused,'aliased_root'):
                    current.read_pair([self.fds[0],duplicate],[self.desc[0]]*2,self.end)
            finally:os.close(duplicate)

    def test_identical_current_pair_with_only_device_qualification_synthetic(self):
        with self.synthetic_external_device_qualification():result=current.read_pair(self.fds,self.desc,self.end)
        self.assertEqual(type(result),tuple);self.assertEqual(len(result),2)
        self.assertEqual(result[0]['head_raw'],result[1]['head_raw'])
        self.assertEqual(result[0]['certificate_raw'],result[1]['certificate_raw'])
        self.assertNotEqual(result[0]['root_identity']['ino'],result[1]['root_identity']['ino'])
        for fd,expected in zip(self.fds,self.desc):self.assertEqual(c.root_descriptor(fd),expected)

    def test_split_renewal_and_missing_head_never_fallback(self):
        old=c.make_head(self.raw);raw=canonical(certificate(previous=old))
        c.install_one(self.fds[1],self.desc[1],raw,old,self.end)
        with self.synthetic_external_device_qualification():
            with self.assertRaisesRegex(Refused,'split_heads'):current.read_pair(self.fds,self.desc,self.end)
        (self.paths[1]/c.HEAD_NAME).unlink()
        with self.synthetic_external_device_qualification():
            with self.assertRaises(FileNotFoundError):current.read_pair(self.fds,self.desc,self.end)
        for fd,expected in zip(self.fds,self.desc):self.assertEqual(c.root_descriptor(fd),expected)

    def test_first_root_change_while_second_root_is_read_refuses(self):
        original=c._open_file;changed=[]
        def swap(root,name,*args,**kwargs):
            result=original(root,name,*args,**kwargs)
            if root==self.fds[1] and name==c.HEAD_NAME and not changed:
                changed.append(True);replacement=self.paths[0]/'swap-head'
                replacement.write_bytes(c.make_head(self.raw));replacement.chmod(0o600)
                os.replace(replacement,self.paths[0]/c.HEAD_NAME)
            return result
        with self.synthetic_external_device_qualification(),patch.object(c,'_open_file',side_effect=swap):
            with self.assertRaises(Refused):current.read_pair(self.fds,self.desc,self.end)
        for fd,expected in zip(self.fds,self.desc):self.assertEqual(c.root_descriptor(fd),expected)


if __name__=='__main__':unittest.main()
