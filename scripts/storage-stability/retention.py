"""Prune only this tool's verified export/copy chains; never ZFS or live sources."""
import os
import re
import stat
import time
from pathlib import Path
from interface import need, RECEIPT_FIELDS
from inventory import decode, private_bytes, valid_uuid, digest, pin
from journal import directory, sync_directory

DAYS = 14
CHAIN_FIELDS={'schema_version','kind','operation_id','action','inventory_sha256','source_graph_sha256','authority_sha256','managed_epoch','revision','automatic_retry','automatic_failover','created_at','chain_format','parent_chains'}
VERIFIED_FIELDS={'schema_version','kind','operation_id','inventory_sha256','source_graph_sha256','status','actual_exit_code','owned_processes_closed','restore_receipt_sha256','export_receipt_sha256','chain_format','parent_chains','verified_at'}

def full_chain_proof(path,chain,verified):
    # No incremental/dependency framework exists. Unknown format protects EVERY
    # chain by refusing planning; merely skipping it could delete its ancestor.
    need(type(chain)is dict and set(chain)==CHAIN_FIELDS and type(verified)is dict and set(verified)==VERIFIED_FIELDS and chain['schema_version']==verified['schema_version']==1 and type(chain['schema_version'])is int and type(verified['schema_version'])is int and chain['chain_format']==verified['chain_format']=='full_self_contained_v1'and chain['parent_chains']==verified['parent_chains']==[],'backup_chain_not_full_self_contained')
    need(chain['action']=='checkpoint'and chain['automatic_retry']is False and chain['automatic_failover']is False and valid_uuid(chain['managed_epoch'])and type(chain['revision'])is str and re.fullmatch('[0-9a-f]{40}',chain['revision'])is not None and all(pin(chain[k])for k in('inventory_sha256','source_graph_sha256','authority_sha256')),'backup_chain_scope')
    receipts={}
    for phase,name,key in(('export_snapshots','export-receipt.json','export_receipt_sha256'),('verify_isolated_restore','restore-receipt.json','restore_receipt_sha256')):
        raw=private_bytes(path/name);need(pin(verified[key])and digest(raw)==verified[key],'backup_chain_receipt_hash');r=decode(raw)
        need(type(r)is dict and set(r)==RECEIPT_FIELDS and type(r.get('schema_version'))is int and r.get('kind')=='openlegal_storage_stability_backend_receipt'and r.get('schema_version')==1 and r.get('operation_id')==path.name and r.get('action')=='checkpoint'and r.get('phase')==phase and r.get('status')=='passed'and type(r.get('actual_exit_code'))is int and r['actual_exit_code']==0 and r.get('owned_processes_closed')is True and all(r.get(k)==chain[k]for k in('inventory_sha256','source_graph_sha256','authority_sha256')),'backup_chain_receipt_identity')
        need(type(r.get('facts'))is dict and r['facts'].get('parent_chains')==[],'backup_chain_receipt_dependencies');receipts[phase]=r
    export=receipts['export_snapshots']['facts'];restore=receipts['verify_isolated_restore']['facts']
    need(all(export.get(k)is True for k in('full_self_contained_export','chain_inputs_complete','export_bytes_verified','all_snapshot_guids_match'))and all(restore.get(k)is True for k in('full_export_restore_verified','restore_processes_closed','restore_resources_removed','all_retained_rows_equal','all_blob_hashes_equal','wal_and_index_verified','isolated_no_network','original_sources_unmodified'))and restore.get('export_receipt_sha256')==verified['export_receipt_sha256'],'backup_chain_restore_closure')


def owned_chains(root, now):
    root=Path(root);directory(root)
    marker=decode(private_bytes(root/'managed.json'))
    need(marker.get('kind')=='openlegal_storage_stability_owned_backup_namespace'and marker.get('schema_version')==1,'backup_namespace_unowned')
    items=sorted(root.iterdir());need(len(items)<=4097,'backup_chain_count');found=[]
    for path in items:
        if path.name=='managed.json':continue
        need(valid_uuid(path.name),'backup_chain_name');directory(path)
        # Protect partial chains AND any unknown ancestor they might reference.
        # Our producer writes its full/no-parent header before any backup data.
        if not(path/'chain.json').exists():
            need(not list(path.iterdir()),'backup_partial_unknown_dependencies');continue
        chain=decode(private_bytes(path/'chain.json'))
        need(type(chain)is dict and set(chain)==CHAIN_FIELDS and chain.get('schema_version')==1 and type(chain.get('schema_version'))is int and chain.get('chain_format')=='full_self_contained_v1'and chain.get('parent_chains')==[],'backup_partial_unknown_dependencies')
        if not(path/'verified.json').exists():continue
        verified=decode(private_bytes(path/'verified.json'))
        full_chain_proof(path,chain,verified)
        need(chain.get('kind')=='openlegal_storage_stability_chain'and chain.get('operation_id')==path.name and verified.get('kind')=='openlegal_storage_stability_verified_chain'and verified.get('operation_id')==path.name,'backup_chain_identity')
        need(verified.get('status')=='passed'and verified.get('actual_exit_code')==0 and type(verified.get('actual_exit_code'))is int and verified.get('owned_processes_closed')is True,'backup_chain_not_verified')
        need(chain.get('inventory_sha256')==verified.get('inventory_sha256')and chain.get('source_graph_sha256')==verified.get('source_graph_sha256'),'backup_chain_provenance')
        for value in (chain.get('created_at'),verified.get('verified_at')):
            need(type(value)in(int,float)and 0<value<=now,'backup_chain_clock')
        found.append((path,chain,verified))
    return found

def plan(root, now):
    found=owned_chains(root,now)
    if not found:return []
    latest={max(found,key=lambda x:(x[1]['created_at'],x[0].name))[0],max(found,key=lambda x:(x[2]['verified_at'],x[0].name))[0]}
    return [path for path,chain,_ in found if path not in latest and chain['created_at']<now-DAYS*86400]

def mounted_below(path):
    def unescape(s):return re.sub(r'\\([0-7]{3})',lambda m:chr(int(m.group(1),8)),s)
    with open('/proc/self/mountinfo',encoding='utf-8')as stream:
        for line in stream:
            fields=line.split();need(len(fields)>=10,'mountinfo_shape');mount=Path(unescape(fields[4]))
            if mount==path or mount.is_relative_to(path):return True
    return False

def prune(path, root, now, deadline):
    """Private fixed namespace only. Caller must hold authority+shared lock."""
    path=Path(path);root=Path(root);directory(root);directory(path)
    need(path in plan(root,now),'prune_chain_protected')
    need(path.parent==root and valid_uuid(path.name)and not mounted_below(path),'prune_namespace_or_mount')
    root_dev=path.stat().st_dev
    count=0
    def budget():
        nonlocal count
        count+=1;need(count<=4_000_000 and time.monotonic()<deadline,'prune_bound')
    def scan(fd):
        for name in os.listdir(fd):
            budget()
            s=os.stat(name,dir_fd=fd,follow_symlinks=False)
            need(s.st_dev==root_dev,'prune_nested_mount')
            if stat.S_ISDIR(s.st_mode):
                child=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd)
                try:scan(child)
                finally:os.close(child)
            else:need(stat.S_ISREG(s.st_mode)and s.st_nlink==1 or stat.S_ISLNK(s.st_mode),'prune_special_or_hardlink')
    def remove(fd):
        for name in os.listdir(fd):
            budget()
            s=os.stat(name,dir_fd=fd,follow_symlinks=False)
            need(s.st_dev==root_dev,'prune_nested_mount')
            if stat.S_ISDIR(s.st_mode):
                child=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd)
                try:remove(child)
                finally:os.close(child)
                os.rmdir(name,dir_fd=fd)
            else:
                need(stat.S_ISREG(s.st_mode)and s.st_nlink==1 or stat.S_ISLNK(s.st_mode),'prune_special_or_hardlink');os.unlink(name,dir_fd=fd)
        os.fsync(fd)
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:
        before=os.fstat(fd);scan(fd);need(not mounted_below(path),'prune_mount_changed')
        current=path.lstat();need((before.st_dev,before.st_ino)==(current.st_dev,current.st_ino),'prune_root_rebound')
        remove(fd)
        current=path.lstat();need((before.st_dev,before.st_ino)==(current.st_dev,current.st_ino),'prune_root_rebound')
        os.rmdir(path);sync_directory(root)
    finally:os.close(fd)
