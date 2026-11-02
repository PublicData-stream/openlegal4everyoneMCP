"""FD-only current HEAD and its selected immutable certificate.

The caller qualifies paths/media, owns shared-lock serialization and retains all
supplied root FDs and their flock state. This reader never locks, unlocks, closes
or reopens those root FDs by path. Only current-head.json and the certificate
named by that validated current HEAD are opened below them. There is no old
certificate fallback, authority, completion proof, installer or admission here.

Returned raw bytes and stat identities describe these reads under the caller's
serialization. They are not trusted capabilities, native observations, a writer
exit or proof of independent physical devices. The caller separately validates
certificate provenance and every authority/Ready requirement.
"""
from __future__ import annotations

import os
import stat

import epoch_certificate as certificate
from interface import need
from inventory import digest, pin


def _identity(value):
    return {'dev':value.st_dev,'ino':value.st_ino,'uid':value.st_uid,'gid':value.st_gid,
        'mode':stat.S_IMODE(value.st_mode),'nlink':value.st_nlink,'size':value.st_size,
        'mtime_ns':value.st_mtime_ns,'ctime_ns':value.st_ctime_ns}


def _different_devices(left, right):
    """Basic filesystem gate only; the caller qualifies hardware/whole WWIDs."""
    need(left['dev']!=right['dev'],'epoch_current_same_filesystem')


def _pair_roots(root_fds, descriptors, deadline):
    need(type(root_fds) in (tuple,list) and len(root_fds)==2
         and type(descriptors) in (tuple,list) and len(descriptors)==2,
         'epoch_current_two_roots')
    need(all(type(fd) is int and fd>=0 for fd in root_fds) and root_fds[0]!=root_fds[1],
         'epoch_current_distinct_root_fds')
    for fd,expected in zip(root_fds,descriptors):certificate._root(fd,expected,deadline)
    actual=[certificate.root_descriptor(fd) for fd in root_fds]
    need((actual[0]['dev'],actual[0]['ino'])!=(actual[1]['dev'],actual[1]['ino']),
         'epoch_current_aliased_root')
    _different_devices(*actual)
    certificate._end(deadline)


class _Current:
    """Retain both selected file descriptions until all final reads complete."""
    def __init__(self, root_fd, expected, deadline, expected_certificate_sha256):
        self.root_fd=root_fd; self.head_fd=None; self.certificate_fd=None
        if expected_certificate_sha256 is not None:
            need(pin(expected_certificate_sha256),'epoch_current_expected_certificate_pin')
        certificate._root(root_fd,expected,deadline)
        self.expected=dict(expected)
        try:
            self.head_fd,self.head_raw,self.head_stat=certificate._open_file(
                root_fd,certificate.HEAD_NAME,self.expected,certificate.HEAD_LIMIT,deadline)
            head=certificate.validate_head(self.head_raw)
            # Compare authority's optional pin with the ACTUAL current HEAD
            # before selecting any certificate basename. Never open an old ID.
            need(expected_certificate_sha256 is None
                 or head['certificate_sha256']==expected_certificate_sha256,
                 'epoch_current_expected_certificate_changed')
            self.certificate_name='certificate-'+head['certificate_id']+'.json'
            self.certificate_fd,self.certificate_raw,self.certificate_stat=certificate._open_file(
                root_fd,self.certificate_name,self.expected,certificate.CERTIFICATE_LIMIT,deadline)
            certificate.validate_certificate(self.certificate_raw)
            need(digest(self.certificate_raw)==head['certificate_sha256']
                 and certificate.make_head(self.certificate_raw)==self.head_raw,
                 'epoch_current_head_certificate_mismatch')
            self.recheck(deadline)
        except BaseException:
            self.close(); raise

    def _held(self, name, fd, expected_raw, expected_stat, limit, deadline):
        raw,now=certificate._read_held(self.root_fd,name,fd,self.expected,limit,deadline)
        need(raw==expected_raw and certificate._signature(now)==certificate._signature(expected_stat),
             'epoch_current_observation_changed')

    def _named(self, name, expected_raw, expected_stat, limit, deadline):
        fd,raw,now=certificate._open_file(self.root_fd,name,self.expected,limit,deadline)
        try:
            need(raw==expected_raw and certificate._signature(now)==certificate._signature(expected_stat),
                 'epoch_current_named_readback_changed')
        finally:os.close(fd)

    def recheck(self, deadline):
        certificate._root(self.root_fd,self.expected,deadline)
        self._held(certificate.HEAD_NAME,self.head_fd,self.head_raw,self.head_stat,
            certificate.HEAD_LIMIT,deadline)
        self._held(self.certificate_name,self.certificate_fd,self.certificate_raw,self.certificate_stat,
            certificate.CERTIFICATE_LIMIT,deadline)
        self._named(certificate.HEAD_NAME,self.head_raw,self.head_stat,certificate.HEAD_LIMIT,deadline)
        self._named(self.certificate_name,self.certificate_raw,self.certificate_stat,
            certificate.CERTIFICATE_LIMIT,deadline)
        # A HEAD replacement during the independent certificate readback must
        # still refuse even when its original bytes are unchanged.
        self._held(certificate.HEAD_NAME,self.head_fd,self.head_raw,self.head_stat,
            certificate.HEAD_LIMIT,deadline)
        self._held(self.certificate_name,self.certificate_fd,self.certificate_raw,self.certificate_stat,
            certificate.CERTIFICATE_LIMIT,deadline)
        certificate._root(self.root_fd,self.expected,deadline)

    def snapshot(self, deadline):
        self.recheck(deadline)
        value={'root_identity':dict(self.expected),'head_raw':self.head_raw,
            'head_sha256':digest(self.head_raw),'head_identity':_identity(self.head_stat),
            'certificate_basename':self.certificate_name,'certificate_raw':self.certificate_raw,
            'certificate_sha256':digest(self.certificate_raw),
            'certificate_identity':_identity(self.certificate_stat)}
        certificate._root(self.root_fd,self.expected,deadline)
        return value

    def close(self):
        # Only our own file descriptions. The supplied root FD/flock are owned
        # exclusively by the caller, including every failure path.
        error=None
        for field in ('certificate_fd','head_fd'):
            fd=getattr(self,field)
            if fd is not None:
                setattr(self,field,None)
                try:os.close(fd)
                except OSError as exc:
                    if error is None:error=exc
        if error is not None:raise error


def read_current(root_fd, expected_descriptor, deadline, *, expected_certificate_sha256=None):
    """Read the actual current HEAD and its matching canonical certificate.

    A stale expected certificate pin refuses before any certificate open. The
    optional pin supplies no authority by itself; emergency callers must enforce
    their separately validated authority and expected current generation.
    """
    current=_Current(root_fd,expected_descriptor,deadline,expected_certificate_sha256)
    try:result=current.snapshot(deadline)
    finally:current.close()
    certificate._root(root_fd,current.expected,deadline)
    return result


def read_pair(root_fds, expected_descriptors, deadline):
    """Read two matching current snapshots; caller qualifies physical media.

    Both file descriptions stay held through cross-root comparison and final
    raw/inode/root rechecks. This is a point-in-time read under caller-owned
    serialization, not a certificate installation, writer exit or Ready proof.
    """
    _pair_roots(root_fds,expected_descriptors,deadline); held=[]
    try:
        for fd,expected in zip(root_fds,expected_descriptors):
            held.append(_Current(fd,expected,deadline,None))
        certificate.validate_heads([row.head_raw for row in held])
        need(held[0].certificate_raw==held[1].certificate_raw,'epoch_current_split_certificates')
        result=tuple(row.snapshot(deadline) for row in held)
        for row in held:row.recheck(deadline)
        _pair_roots(root_fds,expected_descriptors,deadline)
    finally:
        error=None
        for row in held:
            try:row.close()
            except OSError as exc:
                if error is None:error=exc
        if error is not None:raise error
    _pair_roots(root_fds,expected_descriptors,deadline)
    return result
