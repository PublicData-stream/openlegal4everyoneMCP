"""Strict private input parsing and byte-pinned operator inventory."""
import copy
import hashlib
import json
import math
import os
import re
import stat
import time
import uuid
from pathlib import Path
from interface import SOURCE_FILES, TREE_ROLES, Refused, need

HEX = re.compile(r'[0-9a-f]{64}\Z')
MAX_INPUT = 4 * 1024 * 1024

def digest(value):
    return hashlib.sha256(value).hexdigest()

def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False) + '\n').encode()

def decode(raw):
    need(type(raw) is bytes and len(raw) <= MAX_INPUT, 'json_bound')
    def pairs(rows):
        value = {}
        for key, item in rows:
            need(key not in value, 'json_duplicate')
            value[key] = item
        return value
    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda _: (_ for _ in ()).throw(Refused('json_nonfinite')))
    except (ValueError, RecursionError, UnicodeError):
        raise Refused('json_shape') from None
    def depth(value, level):
        need(level <= 32, 'json_depth')
        if isinstance(value, dict):
            for key, item in value.items():
                need(type(key) is str and len(key) <= 1024, 'json_key')
                depth(item, level + 1)
        elif isinstance(value, list):
            for item in value:
                depth(item, level + 1)
        elif type(value) is float:
            need(math.isfinite(value), 'json_nonfinite')
    depth(value, 0)
    return value

def pin(value):
    return type(value) is str and HEX.fullmatch(value) is not None

def valid_uuid(value):
    try:
        return type(value) is str and str(uuid.UUID(value)) == value and uuid.UUID(value).int != 0
    except ValueError:
        return False

def absolute(value):
    need(type(value) is str and len(value) <= 4096, 'absolute_path_type')
    p = Path(value)
    need(p.is_absolute() and '..' not in p.parts and p != Path('/'), 'absolute_path')
    return p

def identity(value):
    need(type(value) is dict and set(value) == {'dev', 'ino'} and all(type(v) is int and v >= 0 for v in value.values()), 'identity_shape')
    return value

def private_parents(path, owner=0):
    for parent in Path(path).parents:
        s = parent.lstat()
        need(stat.S_ISDIR(s.st_mode) and s.st_uid == owner and parent.resolve() == parent, 'private_ancestor')
        need(not s.st_mode & 0o022 or parent == Path('/tmp') and s.st_mode & stat.S_ISVTX, 'writable_ancestor')

def private_bytes(path, sha=None, limit=MAX_INPUT, owner=0):
    p = absolute(str(path)); private_parents(p, owner)
    fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    with os.fdopen(fd, 'rb') as stream:
        a = os.fstat(stream.fileno())
        need(stat.S_ISREG(a.st_mode) and a.st_uid == owner and a.st_nlink == 1 and stat.S_IMODE(a.st_mode) == 0o600 and a.st_size <= limit, 'private_file')
        raw = stream.read(limit + 1); z = os.fstat(stream.fileno()); current = p.lstat()
    need(len(raw) == a.st_size and all(getattr(a,k) == getattr(z,k) == getattr(current,k) for k in ('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns')), 'private_file_changed')
    need(sha is None or pin(sha) and digest(raw) == sha, 'private_raw_hash')
    return raw

def all_datasets(value):
    """Original archive seven plus current active extras; neither list is rebased."""
    return copy.deepcopy(list(value['original_datasets']) + list(value['active_datasets']))

def validate_inventory(value):
    fields = {'schema_version','kind','revision','managed_epoch','host','shared_lock','journal_roots','backup_root','original_datasets','active_datasets','ssd_trees','coverage','kubernetes','postgres','linux','provider_hold','source_graph'}
    need(type(value) is dict and set(value) == fields and type(value['schema_version']) is int and value['schema_version'] == 1 and value['kind'] == 'openlegal_storage_stability_inventory', 'inventory_shape')
    need(type(value['revision']) is str and re.fullmatch('[0-9a-f]{40}', value['revision']) and valid_uuid(value['managed_epoch']), 'inventory_revision_epoch')
    host = value['host']; need(type(host) is dict and set(host) == {'boot_id','machine_id','node_name'} and valid_uuid(host['boot_id']) and re.fullmatch('[0-9a-f]{32}',host['machine_id']) and re.fullmatch('[a-zA-Z0-9_.-]{1,128}',host['node_name']), 'host_identity')
    lock = value['shared_lock']; need(type(lock) is dict and set(lock) == {'path','identity'}, 'shared_lock_shape'); absolute(lock['path']); identity(lock['identity'])
    roots = value['journal_roots']; need(type(roots) is list and len(roots) == 2, 'two_journal_roots_required')
    for root in roots + [value['backup_root']]:
        need(type(root) is dict and set(root) == {'path','identity'}, 'journal_backup_root_shape'); absolute(root['path']); identity(root['identity'])
    need(roots[0]['identity']['dev'] != roots[1]['identity']['dev'], 'independent_journal_media_required')
    all_roots = [absolute(r['path']) for r in roots + [value['backup_root']]]
    for i, a in enumerate(all_roots):
        need(not any(a.is_relative_to(b) or b.is_relative_to(a) for b in all_roots[i+1:]), 'journal_backup_overlap')
    datasets = value['original_datasets']; need(type(datasets) is list and len(datasets) == 7, 'original_seven_datasets')
    active=value['active_datasets'];need(type(active)is list and len(active)<=16,'active_dataset_bound')
    combined=all_datasets(value)
    for d in combined:
        need(type(d) is dict and set(d) == {'name','guid','pool','mountpoint'} and re.fullmatch('[A-Za-z0-9_.:/-]{1,240}',d['name']) and '@' not in d['name'] and re.fullmatch('[0-9]{1,20}',d['guid']) and re.fullmatch('[A-Za-z0-9_.-]{1,128}',d['pool']) and d['name'].split('/')[0] == d['pool'], 'dataset_identity'); absolute(d['mountpoint'])
    need(len({d['name'] for d in datasets}) == 7 and len({d['guid'] for d in datasets}) == 7 and len({d['pool'] for d in datasets}) == 1, 'one_pool_unique_seven')
    need(len({d['name']for d in combined})==len(combined) and len({d['guid']for d in combined})==len(combined)
         and len({d['pool']for d in combined})==1,'unique_original_and_active_datasets')
    for i,d in enumerate(combined):
        name=Path(d['name']);source=absolute(d['mountpoint'])
        need(all(part not in('','.','..')for part in d['name'].split('/')),'dataset_name_segments')
        for other in combined[i+1:]:
            peer=absolute(other['mountpoint']);peername=Path(other['name'])
            need(not(source.is_relative_to(peer)or peer.is_relative_to(source)or
                     name.is_relative_to(peername)or peername.is_relative_to(name)),'dataset_source_overlap')
        need(not any(source.is_relative_to(r) or r.is_relative_to(source) for r in all_roots), 'original_source_backup_journal_overlap')
    trees = value['ssd_trees']; need(type(trees) is list and 3 <= len(trees) <= 32, 'ssd_tree_count')
    for tree in trees:
        need(type(tree) is dict and set(tree) == {'name','roles','path','identity','uid','gid','mode','max_files','max_bytes','symlinks'}, 'ssd_tree_shape')
        need(re.fullmatch('[a-z][a-z0-9_-]{0,63}',tree['name']) and type(tree['roles']) is list and tree['roles'] and len(set(tree['roles'])) == len(tree['roles']) and set(tree['roles']) <= TREE_ROLES, 'ssd_tree_roles')
        absolute(tree['path']); identity(tree['identity'])
        need(all(type(tree[k]) is int and tree[k] >= 0 for k in ('uid','gid','mode','max_files','max_bytes')) and tree['mode'] <= 0o7777 and 1 <= tree['max_files'] <= 200000 and 1 <= tree['max_bytes'] <= 4*1024**4, 'ssd_tree_bounds')
        need(type(tree['symlinks']) is dict and len(tree['symlinks']) <= 1024, 'symlink_policy')
        for name,target in tree['symlinks'].items():
            need(type(name) is str and not Path(name).is_absolute() and name not in ('','.','..') and '..' not in Path(name).parts and type(target) is str and 0 < len(target) <= 4096 and '\x00' not in target, 'symlink_policy_entry')
    need(len({t['name'] for t in trees}) == len(trees), 'ssd_names_unique')
    paths = [absolute(t['path']) for t in trees]
    for i,a in enumerate(paths):
        need(not any(a.is_relative_to(b) or b.is_relative_to(a) for b in paths[i+1:]), 'ssd_tree_overlap')
        need(not any(a.is_relative_to(b) or b.is_relative_to(a) for b in all_roots), 'source_backup_journal_overlap')
        need(not any(a.is_relative_to(absolute(d['mountpoint'])) or absolute(d['mountpoint']).is_relative_to(a)for d in combined),'dataset_ssd_source_overlap')
    coverage = value['coverage']; required = {'cache','corpus','postgres','config','assets','trust','wal'}
    need(type(coverage) is dict and set(coverage) == required, 'complete_component_coverage')
    known = {d['name'] for d in combined} | {t['name'] for t in trees}
    need(all(type(rows) is list and rows and len(rows) == len(set(rows)) and set(rows) <= known for rows in coverage.values()), 'coverage_references')
    need(all(d['name']in{ref for refs in coverage.values()for ref in refs}for d in active),'unaccounted_active_dataset')
    need(any(ref in {d['name']for d in combined}for ref in coverage['postgres']),'postgres_hdd_coverage_required')
    tree_roles={t['name']:set(t['roles']) for t in trees}
    for role,refs in coverage.items():
        need(all(ref not in tree_roles or role in tree_roles[ref] for ref in refs),'coverage_tree_role_mismatch')
    for name,roles in tree_roles.items():
        need(all(name in coverage[role] for role in roles),'unaccounted_ssd_component')
    for role in ('cache','corpus','postgres'):
        need(any(role in t['roles'] and t['name'] in coverage[role] for t in trees), 'ssd_required_component')
    for key in ('kubernetes','postgres','linux'):
        need(type(value[key]) is dict and value[key] and len(canonical(value[key])) <= 512*1024, 'backend_inventory_shape')
    hold = value['provider_hold']; need(type(hold) is dict and set(hold) == {'generation','owner_operation_id','clean','recovery_hold','accounting_sha256'} and valid_uuid(hold['generation']) and (hold['owner_operation_id'] is None or valid_uuid(hold['owner_operation_id'])) and type(hold['clean']) is bool and type(hold['recovery_hold']) is bool and pin(hold['accounting_sha256']), 'provider_hold_shape')
    graph = value['source_graph']; need(type(graph) is dict and set(graph) == SOURCE_FILES and all(pin(v) for v in graph.values()), 'exact_source_graph')
    return value

def validate_authority(value, inventory, inventory_sha, action, operation, now=None):
    now = time.time() if now is None else now
    fields = {'schema_version','kind','approved','inventory_sha256','source_graph_sha256','managed_epoch','actions','operation_id','issued_at','valid_until','daily_checkpoint_permitted','release_owned_clean_hold_permitted','collection_resume_permitted','preserve_originals','automatic_failover','provider_recovery_permitted','credential_provisioning_permitted'}
    need(type(value) is dict and set(value) == fields and type(value['schema_version']) is int and value['schema_version'] == 1 and value['kind'] == 'openlegal_storage_stability_authority' and value['approved'] is True, 'authority_shape')
    need(value['inventory_sha256'] == inventory_sha and value['source_graph_sha256'] == digest(canonical(inventory['source_graph'])) and value['managed_epoch'] == inventory['managed_epoch'], 'authority_binding')
    need(type(value['actions']) is list and value['actions'] and set(value['actions']) <= {'checkpoint','manual_failover'} and len(value['actions']) == len(set(value['actions'])) and action in value['actions'], 'authority_action')
    need(value['operation_id'] == operation or value['operation_id'] is None and action == 'checkpoint' and value['daily_checkpoint_permitted'] is True, 'authority_operation')
    need(type(value['issued_at']) in (int,float) and type(value['valid_until']) in (int,float) and 0 < value['issued_at'] <= now < value['valid_until'], 'authority_clock')
    for key in ('daily_checkpoint_permitted','release_owned_clean_hold_permitted','collection_resume_permitted'):
        need(type(value[key]) is bool, 'authority_boolean')
    need(value['preserve_originals'] is True and value['automatic_failover'] is False and value['provider_recovery_permitted'] is False and value['credential_provisioning_permitted'] is False, 'authority_scope')
    return value
