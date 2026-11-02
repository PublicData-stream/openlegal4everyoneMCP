"""Stopped-tree copies with descriptor-relative I/O and complete byte readback.

No source writes, link following, overwrites, implicit repair or partial cleanup.
The caller must separately establish all physical writers stopped throughout.
"""
import hashlib
import os
import stat
import time
from pathlib import Path
from interface import Refused, need
from inventory import digest

CHUNK = 1024 * 1024

def signature(s):
    return (s.st_dev,s.st_ino,s.st_mode,s.st_uid,s.st_gid,s.st_size,s.st_mtime_ns,s.st_ctime_ns,s.st_nlink)

def attributes(fd):
    try:
        names=sorted(os.listxattr(fd)); need(len(names)<=32,'xattr_count')
        values={name:os.getxattr(fd,name) for name in names}
    except OSError:
        raise Refused('xattrs_unsupported_or_unavailable') from None
    need(sum(len(k.encode())+len(v) for k,v in values.items())<=65536,'xattr_bound')
    return values

def metadata(s,attrs):
    return {'uid':s.st_uid,'gid':s.st_gid,'mode':stat.S_IMODE(s.st_mode),'mtime_ns':s.st_mtime_ns,
            'xattrs':{k:{'bytes':len(v),'sha256':digest(v)} for k,v in attrs.items()}}

def apply_metadata(fd,s,attrs):
    os.fchown(fd,s.st_uid,s.st_gid); os.fchmod(fd,stat.S_IMODE(s.st_mode))
    for name in os.listxattr(fd):
        if name not in attrs: os.removexattr(fd,name)
    for name,value in attrs.items(): os.setxattr(fd,name,value)
    os.utime(fd,ns=(s.st_atime_ns,s.st_mtime_ns)); os.fsync(fd)
    need(metadata(os.fstat(fd),attributes(fd))==metadata(s,attrs),'copy_metadata_mismatch')

def root_fd(path,record=None,*,noatime=False):
    path=Path(path); need(path.is_absolute() and path.resolve()==path,'copy_root_path')
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC|(os.O_NOATIME if noatime else 0))
    try:
        s=os.fstat(fd);now=path.lstat()
        need(stat.S_ISDIR(s.st_mode)and(s.st_dev,s.st_ino)==(now.st_dev,now.st_ino),'copy_root_rebound')
        if record is not None:
            need((s.st_dev,s.st_ino)==(record['identity']['dev'],record['identity']['ino'])and(s.st_uid,s.st_gid,stat.S_IMODE(s.st_mode))==(record['uid'],record['gid'],record['mode']),'copy_root_identity')
    except BaseException:os.close(fd);raise
    return fd

def copy_tree(record,destination,deadline):
    """Return a full verified manifest; destination must not already exist.

    Limits and exact allowed symlinks come from the reviewed inventory. Every
    regular source is single-link; unsupported hardlinks/specials fail closed.
    Explicit links are copied as links, NEVER followed, and remain unsafe for a
    restore until its isolated backend separately validates/rebinds all targets.
    """
    source=Path(record['path']);destination=Path(destination)
    need(not destination.is_relative_to(source) and not source.is_relative_to(destination),'copy_source_destination_overlap')
    need(not destination.exists() and not destination.is_symlink() and destination.is_absolute()and destination.resolve()==destination,'copy_destination_must_be_new')
    source_fd=root_fd(source,record,noatime=True)
    parent_fd=None
    try:
        parent_fd=root_fd(destination.parent)
        parent_id=os.fstat(parent_fd); root_id=os.fstat(source_fd)
        for ancestor in destination.parent.parents:
            s=ancestor.stat();need((s.st_dev,s.st_ino)!=(root_id.st_dev,root_id.st_ino),'copy_source_destination_alias')
        need((parent_id.st_dev,parent_id.st_ino)!=(root_id.st_dev,root_id.st_ino),'copy_source_destination_alias')
        entries=[];witness={};destination_witness={};destination_names={};total=0;count=0
        def budget():need(time.monotonic()<deadline,'copy_deadline')
        def names(fd):
            budget(); value=sorted(os.listdir(fd)); need(len(value)<=record['max_files'],'directory_entry_bound');return value
        def visit(src,dst,prefix,depth):
            nonlocal count,total
            budget();need(depth<=64,'tree_depth');before=os.fstat(src);attrs=attributes(src);old_names=names(src)
            for name in old_names:
                budget();count+=1;need(count<=record['max_files'],'copy_file_bound')
                rel=prefix+'/'+name if prefix else name;s=os.stat(name,dir_fd=src,follow_symlinks=False);witness[rel]=signature(s)
                if stat.S_ISDIR(s.st_mode):
                    need(s.st_dev==root_id.st_dev,'nested_source_mount_unsupported')
                    child=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NOATIME,dir_fd=src)
                    try:
                        need(signature(os.fstat(child))==signature(s),'source_directory_rebound')
                        os.mkdir(name,0o700,dir_fd=dst);out=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=dst)
                        try:
                            visit(child,out,rel,depth+1)
                            entries.append(dict(path=rel,kind='directory',**metadata(s,attributes(child))))
                        finally:os.close(out)
                    finally:os.close(child)
                elif stat.S_ISREG(s.st_mode):
                    need(s.st_nlink==1 and s.st_dev==root_id.st_dev,'hardlink_or_nested_file_mount_unsupported')
                    total+=s.st_size;need(total<=record['max_bytes'],'copy_byte_bound')
                    inp=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC|os.O_NOATIME,dir_fd=src)
                    out=None
                    try:
                        need(signature(os.fstat(inp))==signature(s),'source_file_rebound');x=attributes(inp)
                        out=os.open(name,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=dst)
                        h=hashlib.sha256();size=0
                        while True:
                            budget();chunk=os.read(inp,CHUNK)
                            if not chunk:break
                            size+=len(chunk);need(size<=s.st_size,'source_file_grew');h.update(chunk)
                            offset=0
                            while offset<len(chunk):
                                budget();written=os.write(out,chunk[offset:]);need(written>0,'copy_zero_write');offset+=written
                        need(size==s.st_size and signature(os.fstat(inp))==signature(s)and attributes(inp)==x,'source_file_changed')
                        apply_metadata(out,s,x);os.lseek(out,0,os.SEEK_SET);read_hash=hashlib.sha256()
                        while True:
                            budget();chunk=os.read(out,CHUNK)
                            if not chunk:break
                            read_hash.update(chunk)
                        need(read_hash.digest()==h.digest(),'copy_bytes_mismatch')
                        need(signature(os.stat(name,dir_fd=src,follow_symlinks=False))==signature(s),'late_source_file_change')
                        seed_metadata = not prefix and name == '.openlegal-seed-v1.json' and bool(set(record['roles']) & {'cache','corpus'})
                        if seed_metadata:
                            need(s.st_uid==0 and stat.S_IMODE(s.st_mode)in(0o640,0o644) and 0<s.st_size<=16384,'seed_metadata_shape')
                        entries.append(dict(path=rel,kind='metadata_file' if seed_metadata else 'file',size=s.st_size,sha256=h.hexdigest(),**metadata(s,x)))
                    finally:
                        if out is not None:os.close(out)
                        os.close(inp)
                elif stat.S_ISLNK(s.st_mode):
                    target=os.readlink(name,dir_fd=src);need(record['symlinks'].get(rel)==target,'symlink_unsupported')
                    need(not os.listxattr('/proc/self/fd/'+str(src)+'/'+name,follow_symlinks=False),'symlink_xattrs_unsupported')
                    os.symlink(target,name,dir_fd=dst);os.chown(name,s.st_uid,s.st_gid,dir_fd=dst,follow_symlinks=False)
                    os.utime(name,ns=(s.st_atime_ns,s.st_mtime_ns),dir_fd=dst,follow_symlinks=False)
                    copied=os.stat(name,dir_fd=dst,follow_symlinks=False)
                    need((copied.st_uid,copied.st_gid,stat.S_IMODE(copied.st_mode),copied.st_mtime_ns)==(s.st_uid,s.st_gid,stat.S_IMODE(s.st_mode),s.st_mtime_ns),'symlink_metadata_unsupported')
                    need(os.readlink(name,dir_fd=dst)==target and signature(os.stat(name,dir_fd=src,follow_symlinks=False))==signature(s),'symlink_changed')
                    entries.append({'path':rel,'kind':'symlink','target':target,'uid':s.st_uid,'gid':s.st_gid,'mode':stat.S_IMODE(s.st_mode),'mtime_ns':s.st_mtime_ns})
                else:raise Refused('special_file_unsupported')
                destination_witness[rel]=signature(os.stat(name,dir_fd=dst,follow_symlinks=False))
            need(names(src)==old_names and signature(os.fstat(src))==signature(before),'source_directory_changed')
            apply_metadata(dst,before,attrs);os.fsync(dst)
            destination_names[prefix]=old_names
            destination_witness[prefix]=signature(os.fstat(dst))
        os.mkdir(destination.name,0o700,dir_fd=parent_fd);out=root_fd(destination)
        try:
            visit(source_fd,out,'',0);os.fsync(parent_fd)
            # Reopen every path component without following a replaced parent.
            for rel,sig in witness.items():
                fd=os.dup(source_fd)
                try:
                    parts=Path(rel).parts
                    for part in parts[:-1]:
                        child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd);os.close(fd);fd=child
                    need(signature(os.stat(parts[-1],dir_fd=fd,follow_symlinks=False))==sig,'late_source_tree_change')
                finally:os.close(fd)
            expected_entries={row['path']:row for row in entries}
            def verify_destination(fd,prefix,depth):
                budget();need(depth<=64,'destination_depth')
                need(signature(os.fstat(fd))==destination_witness[prefix]and names(fd)==destination_names[prefix],'late_destination_directory_change')
                for name in destination_names[prefix]:
                    rel=prefix+'/'+name if prefix else name;row=expected_entries[rel]
                    s=os.stat(name,dir_fd=fd,follow_symlinks=False)
                    need(signature(s)==destination_witness[rel],'late_destination_tree_change')
                    if row['kind']=='directory':
                        child=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NOATIME,dir_fd=fd)
                        try:verify_destination(child,rel,depth+1)
                        finally:os.close(child)
                    elif row['kind']in('file','metadata_file'):
                        inp=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC|os.O_NOATIME,dir_fd=fd)
                        try:
                            need(signature(os.fstat(inp))==destination_witness[rel],'late_destination_file_rebound')
                            h=hashlib.sha256();size=0
                            while True:
                                budget();chunk=os.read(inp,CHUNK)
                                if not chunk:break
                                size+=len(chunk);need(size<=row['size'],'late_destination_file_grew');h.update(chunk)
                            need(size==row['size']and h.hexdigest()==row['sha256']and metadata(os.fstat(inp),attributes(inp))=={k:row[k]for k in('uid','gid','mode','mtime_ns','xattrs')},'late_destination_bytes_or_metadata')
                            need(signature(os.fstat(inp))==destination_witness[rel]and signature(os.stat(name,dir_fd=fd,follow_symlinks=False))==destination_witness[rel],'late_destination_file_change')
                        finally:os.close(inp)
                    else:need(os.readlink(name,dir_fd=fd)==row['target'],'late_destination_symlink_change')
                need(names(fd)==destination_names[prefix]and signature(os.fstat(fd))==destination_witness[prefix],'late_destination_directory_change')
            verify_destination(out,'',0)
            # Recheck every destination inode after the full byte pass as well.
            for rel,sig in destination_witness.items():
                fd=os.dup(out)
                try:
                    parts=Path(rel).parts
                    for part in parts[:-1]:
                        child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NOATIME,dir_fd=fd);os.close(fd);fd=child
                    actual=os.fstat(fd)if not parts else os.stat(parts[-1],dir_fd=fd,follow_symlinks=False)
                    need(signature(actual)==sig,'final_destination_tree_change')
                finally:os.close(fd)
            for path,fd in ((source,source_fd),(destination.parent,parent_fd),(destination,out)):
                now=path.lstat();held=os.fstat(fd);need((now.st_dev,now.st_ino)==(held.st_dev,held.st_ino),'copy_path_rebound')
            need((parent_id.st_dev,parent_id.st_ino)==(os.fstat(parent_fd).st_dev,os.fstat(parent_fd).st_ino),'copy_parent_changed')
            target=os.fstat(out);entries.sort(key=lambda r:r['path'])
            return {'kind':'verified_stopped_tree_copy','source_identity':{'dev':root_id.st_dev,'ino':root_id.st_ino},'destination_identity':{'dev':target.st_dev,'ino':target.st_ino},'root_metadata':metadata(root_id,attributes(source_fd)),'entries':entries,'files_and_directories':count,'regular_bytes':total,'all_bytes_read_back':True,'source_writers_quiescent_claimed':False,'restore_link_targets_accepted':False,'copied_seed_marker_accepted_for_new_root':False,'legal_permanent_file_count_claimed':False}
        finally:os.close(out)
    finally:
        if parent_fd is not None:os.close(parent_fd)
        os.close(source_fd)
