"""Closed epoch DATA and one-root descriptor-only durable installation.

No authority/observation producer, native actor, CLI, admission or Ready exists
here. The caller validates every provenance artifact and actual observed child
exit, qualifies independent media/paths, owns the dual-durable intent, and keeps
writers held. Installation facts are not a completed pre-ACK proof.

The caller retains the SAME open file description for the root directory and
its flock through child exit and independent dual-head readback. We never unlock
or close that supplied FD. Qualified writers must honor that lock; this is not
an atomic compare-and-swap against an uncooperative root process. Every attempted
install leaves a permanent O_EXCL intent, even on errors before certificate
creation. No rollback, adoption, cleanup or retry entrypoint is provided.
"""
from __future__ import annotations

import fcntl
import math
import os
from pathlib import Path
import re
import stat
import time

import epoch_observation as observation
from interface import Refused, need
from inventory import canonical, decode, digest, pin, valid_uuid

CERTIFICATE_LIMIT = 256 * 1024
HEAD_LIMIT = 16 * 1024
MAX_GENERATION = 2**63 - 1
HEAD_NAME = 'current-head.json'
ROLES = ('cache_primary','cache_replica','corpus_primary','corpus_replica')
CERTIFICATE_FIELDS = ('schema_version','kind','certificate_id','generation',
                      'predecessor_certificate_sha256','subject','provenance')
HEAD_FIELDS = ('schema_version','kind','managed_epoch','certificate_id','certificate_sha256',
               'generation','predecessor_certificate_sha256')
ROOT_FIELDS = ('dev','ino','uid','gid','mode','kernel_filesystem_id','mount_id')
SUBJECT_FIELDS = ('managed_epoch','host_boot_id','runtime','control','primary','standby',
                  'blob_roots','original_inventory','topology','ssd_survivors','hdd_lost')
PROVENANCE_FIELDS = ('operation_id','reason','producer_source','source_graph','inventory',
                     'authority','seed','first_pair','observation','observation_child_exit')
PRIVATE_REFERENCES = ('producer_source','source_graph','inventory','authority')
JOURNAL_REFERENCES = ('seed','first_pair','observation','observation_child_exit')
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z')


def _shape(value, fields, code):
    need(type(value) is dict and set(value)==set(fields),code)


def _raw(value, limit, code):
    need(type(value) is bytes and 0<len(value)<=limit,code)
    result=decode(value)
    try:encoded=canonical(result)
    except (UnicodeError,ValueError,RecursionError):raise Refused(code+'_canonical') from None
    need(encoded==value,code+'_canonical')
    return result


def _reference(value, *, journal=False):
    if journal:
        _shape(value,('phase','name','sha256'),'epoch_journal_reference')
        need(all(type(value[k]) is str and NAME.fullmatch(value[k]) for k in ('phase','name')),
             'epoch_journal_reference_name')
    else:
        _shape(value,('path','sha256'),'epoch_private_reference')
        observation.manual._path(value['path'])
    need(pin(value['sha256']),'epoch_reference_hash')


def _lineage(value):
    generation=value['generation'];predecessor=value['predecessor_certificate_sha256']
    need(type(generation) is int and 1<=generation<=MAX_GENERATION,'epoch_generation')
    need(predecessor is None if generation==1 else pin(predecessor),'epoch_predecessor')
    need(valid_uuid(value['certificate_id']),'epoch_certificate_id')


def _subject(value):
    _shape(value,SUBJECT_FIELDS,'epoch_subject_shape')
    need(valid_uuid(value['managed_epoch']) and valid_uuid(value['host_boot_id']), 'epoch_subject_identity')
    need(value['topology']==observation.TOPOLOGY
         and value['ssd_survivors']==list(observation.SSD_SURVIVORS)
         and value['hdd_lost']==list(observation.HDD_LOST),'epoch_subject_topology')
    observation._control(value['control'])
    need(value['control']['managed_epoch']==value['managed_epoch'],'epoch_subject_control')
    runtime=value['runtime']
    _shape(runtime,('revision','image_id','configuration_sha256','migration_table_sha256'),'epoch_subject_runtime')
    need(type(runtime['revision']) is str and re.fullmatch('[0-9a-f]{40}',runtime['revision'])
         and type(runtime['image_id']) is str and re.fullmatch('sha256:[0-9a-f]{64}',runtime['image_id'])
         and pin(runtime['configuration_sha256']) and pin(runtime['migration_table_sha256']), 'epoch_runtime_pin')
    for side in ('primary','standby'):
        row=value[side]
        _shape(row,('container_id','image_id','postmaster_started_at','data_directory','data_root','process'),
               'epoch_subject_native')
        need(pin(row['container_id']) and type(row['image_id']) is str
             and re.fullmatch('sha256:[0-9a-f]{64}',row['image_id'])
             and type(row['postmaster_started_at']) is str and 0<len(row['postmaster_started_at'].encode())<=128,
             'epoch_native_pin')
        observation.manual._path(row['data_directory']);observation._root(row['data_root'])
        p=row['process'];_shape(p,('pid','start_ticks','boot_id'),'epoch_native_process')
        need(type(p['pid']) is int and p['pid']>0 and type(p['start_ticks']) is str
             and re.fullmatch('[0-9]+',p['start_ticks']) and p['boot_id']==value['host_boot_id'],
             'epoch_native_process_identity')
    need(value['primary']['container_id']!=value['standby']['container_id'],'epoch_native_alias')
    _shape(value['blob_roots'],ROLES,'epoch_four_blob_roots')
    for root in value['blob_roots'].values():
        observation._blob_root(root);need(root['uid']==10004,'epoch_blob_owner')
    need(observation._subject_blob_binding(value)==value['control']['blob_binding_sha256'],
         'epoch_blob_binding')
    _reference(value['original_inventory'])


def validate_certificate(raw):
    """Pure canonical shape/codec checks; external provenance is caller-owned."""
    value=_raw(raw,CERTIFICATE_LIMIT,'epoch_certificate_bytes')
    _shape(value,CERTIFICATE_FIELDS,'epoch_certificate_shape')
    need(type(value['schema_version']) is int and value['schema_version']==2
         and value['kind']=='openlegal_strict_epoch_certificate','epoch_certificate_version')
    _lineage(value);_subject(value['subject'])
    p=value['provenance'];_shape(p,PROVENANCE_FIELDS,'epoch_provenance_shape')
    need(valid_uuid(p['operation_id']) and p['reason'] in
         ('initial_bind','controlled_restart','healthy_reseed'),'epoch_provenance_identity')
    need((p['reason']=='initial_bind')==(value['generation']==1),'epoch_reason_generation')
    for key in PRIVATE_REFERENCES:_reference(p[key])
    for key in JOURNAL_REFERENCES:_reference(p[key],journal=True)
    return value


def validate_head(raw):
    """Pure head lineage DATA; matching immutable certificate is a separate read."""
    value=_raw(raw,HEAD_LIMIT,'epoch_head_bytes');_shape(value,HEAD_FIELDS,'epoch_head_shape')
    need(type(value['schema_version']) is int and value['schema_version']==2
         and value['kind']=='openlegal_strict_epoch_head','epoch_head_version')
    _lineage(value)
    need(valid_uuid(value['managed_epoch']) and pin(value['certificate_sha256']),'epoch_head_identity')
    return value


def make_head(certificate_raw):
    c=validate_certificate(certificate_raw)
    return canonical({'schema_version':2,'kind':'openlegal_strict_epoch_head',
        'managed_epoch':c['subject']['managed_epoch'],'certificate_id':c['certificate_id'],
        'certificate_sha256':digest(certificate_raw),'generation':c['generation'],
        'predecessor_certificate_sha256':c['predecessor_certificate_sha256']})


def validate_heads(raw_heads):
    """Compare exactly two supplied raw heads; no media/installation proof."""
    need(type(raw_heads) in (list,tuple) and len(raw_heads)==2,'epoch_two_heads')
    if all(v is None for v in raw_heads):return None
    need(all(type(v) is bytes for v in raw_heads),'epoch_split_heads')
    values=[validate_head(raw) for raw in raw_heads]
    need(raw_heads[0]==raw_heads[1],'epoch_split_heads')
    return values[0]


def validate_transition(certificate_raw, expected_head_raw):
    c=validate_certificate(certificate_raw);reason=c['provenance']['reason']
    if expected_head_raw is None:
        need(c['generation']==1 and c['predecessor_certificate_sha256'] is None
             and reason=='initial_bind','epoch_first_install_only')
    else:
        old=validate_head(expected_head_raw)
        need(old['generation']<MAX_GENERATION and c['generation']==old['generation']+1
             and c['predecessor_certificate_sha256']==old['certificate_sha256']
             and c['certificate_id']!=old['certificate_id'],'epoch_head_lineage_changed')
        need(reason in ('controlled_restart','healthy_reseed'),'epoch_renewal_reason')
        same_epoch=c['subject']['managed_epoch']==old['managed_epoch']
        need(same_epoch if reason=='controlled_restart' else not same_epoch,'epoch_reason_epoch_changed')
    return make_head(certificate_raw)


def _end(deadline):
    need(type(deadline) in (int,float),'epoch_install_deadline')
    try:finite=math.isfinite(deadline)
    except OverflowError:finite=False
    need(finite and time.monotonic()<deadline,'epoch_install_deadline')


def root_descriptor(root_fd):
    """FD metadata only; caller must already qualify path/media and retain it."""
    need(type(root_fd) is int and root_fd>=0,'epoch_install_root_fd')
    s=os.fstat(root_fd)
    need(stat.S_ISDIR(s.st_mode) and s.st_nlink>0,'epoch_install_root_directory')
    return {'dev':s.st_dev,'ino':s.st_ino,'uid':s.st_uid,'gid':s.st_gid,
            'mode':stat.S_IMODE(s.st_mode),'kernel_filesystem_id':os.fstatvfs(root_fd).f_fsid,
            'mount_id':observation._fd_mount_id(root_fd)}


def _root(root_fd, expected, deadline):
    _end(deadline);_shape(expected,ROOT_FIELDS,'epoch_install_root_descriptor')
    need(all(type(expected[k]) is int and expected[k]>=0 for k in ROOT_FIELDS)
         and expected['uid']==expected['gid']==0 and expected['mode']==0o700,
         'epoch_install_private_root')
    need(root_descriptor(root_fd)==expected,'epoch_install_root_changed');_end(deadline)


def _signature(s):
    return tuple(getattr(s,k) for k in ('st_dev','st_ino','st_uid','st_gid','st_mode','st_nlink',
                                       'st_size','st_mtime_ns','st_ctime_ns'))


def _private(s, expected, limit):
    need(stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_uid==s.st_gid==0
         and stat.S_IMODE(s.st_mode)==0o600 and s.st_dev==expected['dev']
         and 0<s.st_size<=limit,'epoch_install_private_file')


def _read_held(root_fd, name, fd, expected, limit, deadline):
    _root(root_fd,expected,deadline);before=os.fstat(fd);_private(before,expected,limit)
    os.lseek(fd,0,os.SEEK_SET);raw=bytearray()
    while len(raw)<=limit:
        _end(deadline);part=os.read(fd,min(65536,limit+1-len(raw)))
        if not part:break
        raw.extend(part)
    now=os.stat(name,dir_fd=root_fd,follow_symlinks=False)
    need(len(raw)==before.st_size and _signature(before)==_signature(os.fstat(fd))==_signature(now),
         'epoch_install_file_rebound')
    _root(root_fd,expected,deadline)
    return bytes(raw),before


def _open_file(root_fd,name,expected,limit,deadline,*,missing_ok=False):
    _root(root_fd,expected,deadline)
    try:fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC,dir_fd=root_fd)
    except FileNotFoundError:
        if missing_ok:return None
        raise
    try:
        raw,st=_read_held(root_fd,name,fd,expected,limit,deadline)
        return fd,raw,st
    except BaseException:
        os.close(fd);raise


def _write_new(root_fd,name,raw,expected,limit,deadline):
    _root(root_fd,expected,deadline)
    need(type(raw) is bytes and 0<len(raw)<=limit,'epoch_install_write_bound')
    fd=os.open(name,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=root_fd)
    try:
        os.fchown(fd,0,0);os.fchmod(fd,0o600);view=memoryview(raw)
        while view:
            _end(deadline);n=os.write(fd,view);need(n>0,'epoch_install_short_write');view=view[n:]
        _end(deadline);os.fsync(fd);actual,st=_read_held(root_fd,name,fd,expected,limit,deadline)
        need(actual==raw,'epoch_install_write_readback')
        other,readback,otherst=_open_file(root_fd,name,expected,limit,deadline)
        try:
            need(readback==raw and _signature(otherst)==_signature(st),'epoch_install_independent_readback')
        finally:os.close(other)
        return fd,st
    except BaseException:
        os.close(fd);raise


def _directory_sync(root_fd,expected,deadline):
    _root(root_fd,expected,deadline);os.fsync(root_fd);_root(root_fd,expected,deadline)


def install_one(root_fd, expected_descriptor, certificate_raw, expected_head_raw, deadline):
    """One source-owned FD-only install; errors retain intent and all partials.

    The provided root FD/flock stay caller-owned on every path. Returned facts
    certify only these completed syscalls/readbacks. They do not certify an
    actor's actual OS exit, a dual-media installation, authority or admission.
    """
    need(os.geteuid()==0,'epoch_install_root_required')
    head_raw=validate_transition(certificate_raw,expected_head_raw);c=validate_certificate(certificate_raw)
    _root(root_fd,expected_descriptor,deadline)
    try:fcntl.flock(root_fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:raise Refused('epoch_install_lock_unavailable') from None
    _root(root_fd,expected_descriptor,deadline)
    certificate_name='certificate-'+c['certificate_id']+'.json'
    pending_name='.pending-head-'+c['certificate_id']+'.json'
    intent_name='.install-intent-'+c['certificate_id']+'.json'
    held=[]
    try:
        old=_open_file(root_fd,HEAD_NAME,expected_descriptor,HEAD_LIMIT,deadline,missing_ok=True)
        if old is None:oldfd=oldraw=oldstat=None
        else:
            oldfd,oldraw,oldstat=old;held.append(oldfd)
        need(oldraw==expected_head_raw,'epoch_install_expected_head_changed')
        if oldraw is not None:validate_head(oldraw)
        intent_raw=canonical({'schema_version':2,'kind':'openlegal_epoch_install_intent',
            'certificate_id':c['certificate_id'],'certificate_sha256':digest(certificate_raw),
            'generation':c['generation'],'predecessor_certificate_sha256':c['predecessor_certificate_sha256'],
            'expected_head_sha256':None if oldraw is None else digest(oldraw),
            'intended_head_sha256':digest(head_raw),'root_identity':dict(expected_descriptor)})
        intentfd,intentstat=_write_new(root_fd,intent_name,intent_raw,expected_descriptor,HEAD_LIMIT,deadline);held.append(intentfd)
        _directory_sync(root_fd,expected_descriptor,deadline)
        # No certificate/head mutation occurs before durable one-shot intent.
        certfd,certstat=_write_new(root_fd,certificate_name,certificate_raw,expected_descriptor,CERTIFICATE_LIMIT,deadline);held.append(certfd)
        _directory_sync(root_fd,expected_descriptor,deadline)
        pendingfd,pendingstat=_write_new(root_fd,pending_name,head_raw,expected_descriptor,HEAD_LIMIT,deadline);held.append(pendingfd)
        _directory_sync(root_fd,expected_descriptor,deadline)
        if oldfd is None:
            try:os.stat(HEAD_NAME,dir_fd=root_fd,follow_symlinks=False)
            except FileNotFoundError:pass
            else:raise Refused('epoch_install_head_appeared')
        else:
            current,st=_read_held(root_fd,HEAD_NAME,oldfd,expected_descriptor,HEAD_LIMIT,deadline)
            need(current==expected_head_raw and _signature(st)==_signature(oldstat),'epoch_install_old_head_changed')
        current,pst=_read_held(root_fd,pending_name,pendingfd,expected_descriptor,HEAD_LIMIT,deadline)
        need(current==head_raw and _signature(pst)==_signature(pendingstat),'epoch_install_pending_changed')
        _root(root_fd,expected_descriptor,deadline)
        os.rename(pending_name,HEAD_NAME,src_dir_fd=root_fd,dst_dir_fd=root_fd)
        _end(deadline);os.fsync(pendingfd);_directory_sync(root_fd,expected_descriptor,deadline)
        readfd,readback,headstat=_open_file(root_fd,HEAD_NAME,expected_descriptor,HEAD_LIMIT,deadline);held.append(readfd)
        need(readback==head_raw and (headstat.st_dev,headstat.st_ino)==(pendingstat.st_dev,pendingstat.st_ino),
             'epoch_install_final_head_changed')
        certback,finalcert=_read_held(root_fd,certificate_name,certfd,expected_descriptor,CERTIFICATE_LIMIT,deadline)
        intentback,finalintent=_read_held(root_fd,intent_name,intentfd,expected_descriptor,HEAD_LIMIT,deadline)
        need(certback==certificate_raw and intentback==intent_raw and _signature(finalcert)==_signature(certstat)
             and _signature(finalintent)==_signature(intentstat),'epoch_install_final_immutable_changed')
        _root(root_fd,expected_descriptor,deadline)
        return {'root_identity':dict(expected_descriptor),'intent_basename':intent_name,'intent_sha256':digest(intent_raw),
            'certificate_basename':certificate_name,'certificate_sha256':digest(certificate_raw),
            'certificate_dev':certstat.st_dev,'certificate_ino':certstat.st_ino,'head_basename':HEAD_NAME,
            'head_sha256':digest(head_raw),'head_dev':headstat.st_dev,'head_ino':headstat.st_ino,
            'prior_head_sha256':None if expected_head_raw is None else digest(expected_head_raw),
            'certificate_id':c['certificate_id'],'generation':c['generation'],
            'predecessor_certificate_sha256':c['predecessor_certificate_sha256'],
            'file_fsyncs_completed':True,'directory_fsync_completed':True}
    finally:
        # Keep all evidence, supplied directory FD and caller-owned flock.
        for fd in held:os.close(fd)
