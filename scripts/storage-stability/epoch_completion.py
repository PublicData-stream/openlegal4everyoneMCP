"""Read initial historical completion through held same-mount private roots.

This returns archived DATA, never current HEAD/health, admission, a trusted
capability, or the outer producer's actual exit. The source-owned controller
qualifies root paths/physical SSD/WWIDs and holds serialization. All supplied root FDs and
their locks stay caller-owned, including on failure. No live producer loader,
absolute provenance open, DualJournal, HDD open, mutation or execution is used.
"""
from __future__ import annotations

import copy
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import stat
import tomllib

import epoch_certificate as certificate
from interface import Refused, need
from inventory import canonical, digest, pin, valid_uuid

OBSERVATION = 'initial_epoch_observation'
INSTALL = 'initial_epoch_install_verify'
PAIR = 'initial_pg_pair'
SEED = 'initial_blob_seed'
MAX_RECORDS = 4096
MAX_BYTES = 512 * 1024**2
MAX_ARTIFACT = 256 * 1024**2
MAX_OUTPUT = 16 * 1024**2
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z')
# A separate closed historical profile. Never extend interface.SOURCE_FILES.
PRODUCER_SOURCES = frozenset((
    'interface.py','inventory.py','journal.py','copy_tree.py','retention.py',
    'checkpoint.py','linux_backend.py','restore_backend.py','manual_backend.py',
    'provider_backend.py','bootstrap_backend.py','command_process.py','canary_native.py',
    'preservation_backend.py','preservation_capture.py','legacy_prepare.py',
    'initial_migration.py','initial_driver.py','initial_blob_seed.py',
    'initial_role_prepare.py','initial_provider_maintenance.py','initial_pg_pair.py',
    'epoch_observation.py','epoch_primary_fence.py','epoch_certificate.py',
    'epoch_current.py','initial_epoch_certificate.py'))
INVENTORY_FIELDS = frozenset(('schema_version','kind','operation_id','revision','managed_epoch','host','shared_lock',
    'journal_roots','backup_root','original_datasets','active_datasets','ssd_trees','coverage','kubernetes',
    'postgres','linux','provider_hold','source_graph','legacy_prepare','initial_migration','epoch_certificate'))
INPUT_FIELDS = frozenset(('certificate_id','reason','generation','predecessor_certificate_sha256','original_inventory',
    'driver_inventory','driver_authority','seed_inventory','seed_authority','pair_inventory','pair_authority',
    'pair_complete','role_prepare','maintenance','strict_preprovision','epoch_roots'))
AUTHORITY_FIELDS = frozenset(('schema_version','kind','approved','operation_id','inventory_sha256','source_graph_sha256',
    'source_sha256','valid_until','epoch_certificate_observation_permitted','epoch_certificate_install_permitted',
    'initial_bind_only','preserve_originals','automatic_retry','roles_permitted','credentials_permitted',
    'trust_permitted','provider_permitted','kubernetes_mutation_permitted','pg_mutation_permitted','ready_permitted',
    'release_permitted','promotion_permitted','cleanup_permitted'))
PROOF_FIELDS = frozenset(('schema_version','kind','operation_id','inventory','authority','source_graph_sha256',
    'certificate_id','certificate_sha256','subject_sha256','generation','predecessor_certificate_sha256',
    'observation','observation_child_exit','install_intent','installations','install_child_exit',
    'independent_readback','final_configuration','final_primary_fence','final_sql_closure','parent',
    'ready','public_ack','automatic_retry','outer_actual_os_exit'))
ENV = ('PATH=/usr/local/bin:/usr/bin:/bin','HOME=/nonexistent','LANG=C','LC_ALL=C',
       'PGPASSFILE=/dev/null','PGSERVICEFILE=/dev/null')


def _shape(value, fields, code):
    need(type(value) is dict and set(value) == set(fields), code)


def _json(raw, *, canonical_required=True):
    need(type(raw) is bytes and 0 < len(raw) <= MAX_ARTIFACT, 'epoch_completion_json_bound')
    def pairs(rows):
        result = {}
        for key, value in rows:
            need(key not in result, 'epoch_completion_duplicate_key'); result[key] = value
        return result
    try:
        value = json.loads(raw, object_pairs_hook=pairs,
            parse_constant=lambda _: need(False, 'epoch_completion_nonfinite'))
        def depth(item, level):
            need(level <= 32, 'epoch_completion_depth')
            if type(item) is dict:
                for key, child in item.items():
                    need(type(key) is str and len(key) <= 1024, 'epoch_completion_json_key'); depth(child, level+1)
            elif type(item) is list:
                for child in item: depth(child, level+1)
            elif type(item) is float: need(math.isfinite(item), 'epoch_completion_nonfinite')
        depth(value, 0)
        if canonical_required: need(canonical(value) == raw, 'epoch_completion_noncanonical')
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise Refused('epoch_completion_json') from None


def _path(value):
    need(type(value) is str and 0 < len(value) <= 4096 and '\x00' not in value,
         'epoch_completion_path')
    path = Path(value)
    need(path.is_absolute() and path != Path('/') and str(path) == value
         and not any(v in ('','.','..') for v in value.split('/')[1:]), 'epoch_completion_canonical_path')
    return path


def _identity(st):
    return {key: getattr(st, 'st_'+key) for key in
        ('dev','ino','uid','gid','mode','nlink','size','mtime_ns','ctime_ns')}


class _Reader:
    def __init__(self, archive_fd, descriptor, source_root, control_root, end, count, size):
        need(os.geteuid() == 0, 'epoch_completion_root_required')
        need(type(count) is int and 1 <= count <= MAX_RECORDS and type(size) is int
             and 1 <= size <= MAX_BYTES, 'epoch_completion_limits')
        certificate._end(end)
        self.end=end; self.max_records=count; self.max_bytes=size; self.bytes=0
        self.files={}; self.inodes={}; self.directories={}; self.owned=[]; self.listings=[]; self.original_end=None
        need(type(archive_fd) is int and archive_fd >= 0, 'epoch_completion_archive_fd')
        archive_path=os.readlink('/proc/self/fd/'+str(archive_fd))
        self.roots={'archive':(archive_fd,copy.deepcopy(descriptor),str(_path(archive_path)))}
        for key, row in (('source',source_root),('control',control_root)):
            need(type(row) in (tuple,list) and len(row)==3, 'epoch_completion_root_tuple')
            fd, desc, path=row
            self.roots[key]=(fd,copy.deepcopy(desc),str(_path(path)))
        for row in self.roots.values(): self._root(row)
        rows=list(self.roots.values()); mount=self._mount(rows[0][1])
        need(all(self._mount(row[1])==mount for row in rows), 'epoch_completion_same_mount_only')
        for i, row in enumerate(rows):
            a=Path(row[2])
            need(all(not(a.is_relative_to(Path(b[2])) or Path(b[2]).is_relative_to(a))
                     for b in rows[i+1:]), 'epoch_completion_root_overlap')

    @staticmethod
    def _mount(desc): return tuple(desc[k] for k in ('dev','kernel_filesystem_id','mount_id'))

    def _root(self, row):
        fd, desc, path=row; certificate._root(fd,desc,self.end)
        need(os.readlink('/proc/self/fd/'+str(fd))==path, 'epoch_completion_root_path_rebound')

    def roots_check(self):
        for row in self.roots.values(): self._root(row)

    def _directory(self, root, parts):
        self.roots_check(); fd, desc, path=self.roots[root]
        for index, name in enumerate(parts):
            need(type(name) is str and NAME.fullmatch(name), 'epoch_completion_component')
            key=(root,tuple(parts[:index+1])); expected_path=str(Path(path).joinpath(*key[1]))
            if key not in self.directories:
                child=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd)
                self.owned.append(child); st=os.fstat(child)
                need(st.st_uid==st.st_gid==0 and stat.S_IMODE(st.st_mode)==0o700 and st.st_nlink>0
                     and self._mount(certificate.root_descriptor(child))==self._mount(desc),
                     'epoch_completion_private_directory')
                self.directories[key]=(child,fd,name,_identity(st),expected_path)
            child, parent, basename, saved, current_path=self.directories[key]
            need(parent==fd and _identity(os.fstat(child))==saved
                 and _identity(os.stat(basename,dir_fd=parent,follow_symlinks=False))==saved
                 and os.readlink('/proc/self/fd/'+str(child))==current_path,
                 'epoch_completion_directory_rebound')
            fd=child
        return fd

    def read(self, root, parts, sha, *, limit=MAX_ARTIFACT, is_json=True):
        need(type(parts) in (tuple,list) and 1 <= len(parts) <= 32 and all(
            type(v) is str and NAME.fullmatch(v) for v in parts) and pin(sha), 'epoch_completion_reference')
        key=(root,tuple(parts)); parent=self._directory(root,parts[:-1]); name=parts[-1]
        if key in self.files:
            row=self.files[key]; need(row['sha256']==sha, 'epoch_completion_conflicting_reference')
            self._check_file(row)
        else:
            need(len(self.files)<self.max_records, 'epoch_completion_record_budget')
            fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC,dir_fd=parent)
            self.owned.append(fd); st=os.fstat(fd)
            need(stat.S_ISREG(st.st_mode) and st.st_uid==st.st_gid==0 and stat.S_IMODE(st.st_mode)==0o600
                 and st.st_nlink==1 and 0<st.st_size<=limit
                 and self._mount(certificate.root_descriptor(parent))==self._mount(self.roots[root][1])
                 and st.st_dev==self.roots[root][1]['dev']
                 and os.fstatvfs(fd).f_fsid==self.roots[root][1]['kernel_filesystem_id']
                 and certificate.observation._fd_mount_id(fd)==self.roots[root][1]['mount_id'],
                 'epoch_completion_private_file')
            need(self.bytes+st.st_size<=self.max_bytes, 'epoch_completion_byte_budget')
            inode=(st.st_dev,st.st_ino)
            need(inode not in self.inodes, 'epoch_completion_file_alias')
            row={'fd':fd,'parent':parent,'name':name,'identity':_identity(st),'sha256':sha,'raw':None}
            raw=self._read_file(row); need(digest(raw)==sha, 'epoch_completion_raw_hash')
            row['raw']=raw; self.files[key]=row; self.inodes[inode]=key; self.bytes+=len(raw)
        return _json(row['raw']) if is_json else row['raw']

    def _read_file(self, row):
        certificate._end(self.end); os.lseek(row['fd'],0,os.SEEK_SET); raw=bytearray()
        size=row['identity']['size']
        while len(raw)<=size:
            certificate._end(self.end); part=os.read(row['fd'],min(65536,size+1-len(raw)))
            if not part: break
            raw.extend(part)
        need(len(raw)==size and _identity(os.fstat(row['fd']))==row['identity']
             and _identity(os.stat(row['name'],dir_fd=row['parent'],follow_symlinks=False))==row['identity'],
             'epoch_completion_file_rebound')
        return bytes(raw)

    def _check_file(self, row):
        raw=self._read_file(row)
        need(raw==row['raw'] and digest(raw)==row['sha256'], 'epoch_completion_file_changed')

    def journal(self, ref, *, phase=None):
        certificate._reference(ref,journal=True)
        need(phase is None or ref['phase']==phase, 'epoch_completion_phase')
        return self.read('archive',(self.operation,ref['phase'],ref['name']),ref['sha256'])

    def external(self, ref, *, source=False, is_json=True):
        certificate._reference(ref); key='source' if source else 'control'
        base=Path(self.roots[key][2]); path=_path(ref['path'])
        need(path.is_relative_to(base) and path!=base, 'epoch_completion_external_escape')
        return self.read(key,path.relative_to(base).parts,ref['sha256'],limit=4*1024**2,is_json=is_json)

    def pair_intent(self, sha):
        fd=self._directory('archive',(self.operation,PAIR)); count=0; names=[]
        with os.scandir(fd) as entries:
            for entry in entries:
                certificate._end(self.end); count+=1
                need(count<=MAX_RECORDS and NAME.fullmatch(entry.name), 'epoch_completion_pair_listing_bound')
                if entry.name.endswith('-first-bind-intent.json'): names.append(entry.name)
                need(len(names)<=1, 'epoch_completion_multiple_bind_intents')
        need(len(names)==1 and re.fullmatch(r'initial_pg_pair-[0-9]{4,}-first-bind-intent.json',names[0]),
             'epoch_completion_bind_intent_name')
        self.listings.append({'kind':'legacy_pair_first_bind_intent','phase':PAIR,'entries':count,'name':names[0]})
        return self.journal({'phase':PAIR,'name':names[0],'sha256':sha},phase=PAIR)

    def finish(self):
        self.roots_check()
        for key in self.directories: self._directory(key[0],key[1])
        for row in self.files.values(): self._check_file(row)
        self.roots_check(); certificate._end(self.end)

    def close(self):
        error=None
        for fd in reversed(self.owned):
            try: os.close(fd)
            except OSError as exc:
                if error is None: error=exc
        self.owned=[]
        if error is not None: raise error


def _process(row, boot):
    _shape(row,('pid','start_ticks','boot_id'),'epoch_completion_process_shape')
    need(type(row['pid']) is int and row['pid']>0 and type(row['start_ticks']) is str
         and re.fullmatch('[0-9]+',row['start_ticks']) and row['boot_id']==boot, 'epoch_completion_process')


def _child(row, boot, parent):
    _shape(row,('pid','start_ticks','boot_id','actual_exit_code','remaining_children'),'epoch_completion_child_shape')
    _process({k:row[k] for k in ('pid','start_ticks','boot_id')},boot)
    need(type(row['actual_exit_code']) is int and row['actual_exit_code']==0 and row['remaining_children']==[]
         and row['pid']!=parent['pid'], 'epoch_completion_child_not_closed_zero')


def _command(row):
    need(type(row) is dict and type(row.get('actual_exit_code')) is int and row['actual_exit_code']==0
         and row.get('started') is True and row.get('owned_processes_closed') is True
         and all(row.get(k) is False for k in ('expired','interrupted','unexpected_descendants','output_limit_exceeded')),
         'epoch_completion_command_not_closed_zero')
    need(type(row.get('stdout_bytes')) is int and 0<=row['stdout_bytes']<=MAX_OUTPUT
         and type(row.get('stdout_hex')) is str and len(row['stdout_hex'])==2*row['stdout_bytes']
         and re.fullmatch('[0-9a-f]*',row['stdout_hex']) and pin(row.get('stdout_sha256')),
         'epoch_completion_stdout_bound')
    raw=bytes.fromhex(row['stdout_hex']); need(digest(raw)==row['stdout_sha256'], 'epoch_completion_stdout_hash')
    need(type(row.get('stderr_bytes')) is int and 0<=row['stderr_bytes']<=MAX_OUTPUT
         and pin(row.get('stderr_sha256')), 'epoch_completion_stderr_bound')
    if 'stderr_hex' in row:
        value=row['stderr_hex']; need(type(value) is str and len(value)==2*row['stderr_bytes']
            and re.fullmatch('[0-9a-f]*',value) and digest(bytes.fromhex(value))==row['stderr_sha256'],
            'epoch_completion_stderr_hash')
    return raw


def _lsn(value):
    need(type(value) is str and re.fullmatch('[0-9A-F]{1,8}/[0-9A-F]{1,8}',value), 'epoch_completion_lsn')
    a,b=value.split('/'); return (int(a,16)<<32)+int(b,16)


def _peer(value, binding):
    need(type(value) is dict, 'epoch_completion_peer')
    basic=(set(binding)-{'managed_epoch','blob_binding_sha256'})|{
        'postmaster_started_at','sender_pid','sender_started_at','prefix_lsn','flush_lsn'}
    need(set(value) in (basic,basic|{'managed_epoch','blob_binding_sha256'}), 'epoch_completion_peer_fields')
    for key, expected in binding.items():
        if key in ('managed_epoch','blob_binding_sha256'):
            if key in value: need(value[key]==expected, 'epoch_completion_peer_binding')
        elif key=='standby_client_serial':
            actual=value.get(key)
            need(type(actual) in (int,str) and re.fullmatch('[0-9]{1,60}',str(actual))
                 and int(actual)==int(expected), 'epoch_completion_peer_serial')
        elif key=='standby_addr':
            actual=value.get(key)
            actual=_host_address(actual)
            need(actual==str(ipaddress.ip_address(expected)), 'epoch_completion_peer_address')
        else: need(type(value.get(key)) is type(expected) and value[key]==expected, 'epoch_completion_peer_binding')
    need(type(value['sender_pid']) is int and value['sender_pid']>0
         and all(type(value[k]) is str and value[k] for k in ('sender_started_at','postmaster_started_at')),
         'epoch_completion_peer_lifetime')
    _lsn(value['prefix_lsn']); _lsn(value['flush_lsn']); return value


def _host_address(value):
    need(type(value) is str,'epoch_completion_peer_address')
    try: address=ipaddress.ip_interface(value)
    except ValueError: raise Refused('epoch_completion_peer_address') from None
    need(address.network.prefixlen==address.max_prefixlen,'epoch_completion_inet_must_be_host_prefix')
    return str(address.ip)


def _proof(before, after, binding, *, previous=None):
    for row in (before,after):
        _peer(row,binding)
        need(all(row.get(k)==binding[k] for k in ('managed_epoch','blob_binding_sha256')),
             'epoch_completion_proof_control')
    static=set(before)-{'prefix_lsn','flush_lsn'}
    need(static==set(after)-{'prefix_lsn','flush_lsn'} and all(before[k]==after[k] for k in static)
         and _lsn(after['flush_lsn'])>=_lsn(before['prefix_lsn']), 'epoch_completion_proof_not_durable')
    if previous is not None:
        _peer(previous,binding)
        need(all(previous[k]==before[k] for k in static if k not in ('managed_epoch','blob_binding_sha256'))
             and _lsn(before['prefix_lsn'])>=_lsn(previous['prefix_lsn']), 'epoch_completion_precommit_changed')
    return {'before':before,'after':after,'fixed_target':before['prefix_lsn']}


def _window(row, started, finished, end, original):
    values=[row.get(k) for k in (started,finished,end,original)]
    need(all(type(v) in (int,float) and math.isfinite(v) for v in values)
         and values[0]<=values[1]<values[2]<=values[3], 'epoch_completion_historical_clock')


def _dispatch(operation):
    return ['--config','/tmp/openlegal-storage-docker-client-'+operation,'--host','unix:///var/run/docker.sock']


def _psql(subject, user):
    return ['exec','-i','-u','70',subject['primary']['container_id'],'/usr/bin/env','-i',*ENV,
        '/usr/local/bin/psql','-X','-w','-h','/var/run/postgresql','-p','5432','-qAt',
        '-v','ON_ERROR_STOP=1','-U',user,'-d','openlegal']


def _native_sql(kind):
    need(kind in ('proof','flush'), 'epoch_completion_native_kind')
    functions=('capture_prefix','observe_flush') if kind=='proof' else ('observe_flush',)
    prefix=("BEGIN READ ONLY; SET LOCAL statement_timeout='2s'; SET LOCAL lock_timeout='1s'; "
        "SET LOCAL transaction_timeout='5s'; SET LOCAL search_path=pg_catalog; SET LOCAL TIME ZONE 'UTC'; "
        "SET LOCAL DateStyle='ISO,YMD'; SET LOCAL client_encoding='UTF8'; ")
    return (prefix+' '.join("SELECT jsonb_build_object('session_user',session_user,'current_user',current_user,"
        "'read_only',current_setting('transaction_read_only'),'result',openlegal_replication."+v+'());'
        for v in functions)+' ROLLBACK;\n').encode()


def _primary_fence(r, ref, cfg, subject):
    value=r.journal(ref); phase=ref['phase']; user=cfg['linux']['postgres']['admin_user']
    _shape(value,('schema_version','kind','operation_id','native_phase','archive_phase','inventory_sha256',
        'authority_sha256','source_graph_sha256','native_started_at','native_finished_at','native_absolute_deadline',
        'original_absolute_deadline','native_command_refs','native_proof_valid','failure_type','proof',
        'global_sql_closure','observation_status','public_ack','ready'),'epoch_completion_fence_shape')
    need(type(value['schema_version']) is int and value['schema_version']==1
         and value['kind']=='openlegal_epoch_primary_fence' and value['operation_id']==r.operation
         and value['native_phase']==value['archive_phase']==phase
         and value['inventory_sha256']==r.inventory_sha and value['authority_sha256']==r.authority_sha
         and value['source_graph_sha256']==r.graph_sha and value['native_proof_valid'] is True
         and value['failure_type'] is None and value['ready'] is value['public_ack'] is False
         and value['observation_status']=='historical_native_window_not_current_after_archive',
         'epoch_completion_fence_binding')
    _window(value,'native_started_at','native_finished_at','native_absolute_deadline','original_absolute_deadline')
    need(r.original_end is None or value['original_absolute_deadline']==r.original_end,
         'epoch_completion_original_deadline_changed')
    r.original_end=value['original_absolute_deadline']
    need(value['native_absolute_deadline']<=value['native_started_at']+2,
         'epoch_completion_native_two_seconds')
    refs=value['native_command_refs']; need(type(refs) is list and 1<=len(refs)<=33,'epoch_completion_native_count')
    peers=[]
    for index, native_ref in enumerate(refs):
        row=r.journal(native_ref,phase=phase); raw=_command(row); kind='proof' if index==0 else 'flush'
        need(row['stdout_bytes']<=65536 and row['stderr_bytes']<=65536,'epoch_completion_native_output_bound')
        need(row.get('kind')=='openlegal_epoch_primary_native_sql' and row.get('query_kind')==kind
             and row.get('binary_sha256')==cfg['linux']['binaries']['docker']['sha256']
             and row.get('arguments_sha256')==digest(canonical(_dispatch(r.operation)+_psql(subject,user)))
             and row.get('stdin_sha256')==digest(_native_sql(kind)), 'epoch_completion_native_dispatch')
        _window(row,'native_started_at','native_finished_at','native_absolute_deadline','original_absolute_deadline')
        need(row['native_absolute_deadline']==value['native_absolute_deadline']
             and row['original_absolute_deadline']==value['original_absolute_deadline']
             and row['native_started_at']>=value['native_started_at']
             and row['native_finished_at']<=value['native_finished_at'], 'epoch_completion_native_window_binding')
        lines=[_json(line,canonical_required=False) for line in raw.splitlines()]
        need(len(lines)==(2 if index==0 else 1),'epoch_completion_native_output')
        for line in lines:
            _shape(line,('session_user','current_user','read_only','result'),'epoch_completion_native_envelope')
            need(line['session_user']==line['current_user']==user and line['read_only']=='on',
                 'epoch_completion_native_role')
            peer=_peer(line['result'],subject['control'])
            need(peer['postmaster_started_at']==subject['primary']['postmaster_started_at'],
                 'epoch_completion_postmaster_changed'); peers.append(peer)
    proof=_proof(peers[0],peers[-1],subject['control'])
    static=set(peers[0])-{'prefix_lsn','flush_lsn'}
    need(all(set(peer)-{'prefix_lsn','flush_lsn'}==static and all(peer[k]==peers[0][k] for k in static)
             for peer in peers),'epoch_completion_native_peer_changed_during_poll')
    need(value['proof']==proof, 'epoch_completion_native_proof_changed')
    closure=r.journal(value['global_sql_closure'],phase=phase)
    _shape(closure,('actual_command','raw_utf8'),'epoch_completion_fence_closure_shape')
    row=r.journal(closure['actual_command'],phase=phase); raw=_command(row)
    script=("BEGIN READ ONLY; SET LOCAL statement_timeout='2s'; SET LOCAL lock_timeout='1s'; "
        "SET LOCAL transaction_timeout='5s'; SET LOCAL search_path=pg_catalog; "
        "SELECT jsonb_build_object('session_user',session_user,'current_user',current_user,"
        "'result',openlegal_checkpoint.client_sessions()); ROLLBACK;\n").encode()
    need(row.get('binary')=='docker' and row.get('arguments_sha256')==digest(canonical(_dispatch(r.operation)+_psql(subject,user)))
         and row.get('stdin_sha256')==digest(script) and closure['raw_utf8'].encode()==raw,
         'epoch_completion_fence_closure_dispatch')
    need(_json(raw,canonical_required=False)=={'session_user':user,'current_user':user,'result':{'sessions':[]}},
         'epoch_completion_fence_sessions_remain')
    return proof


def _both_sql(r, ref, cfg, subject):
    phase=ref['phase']; closed=r.journal(ref)
    _shape(closed,('observations','sessions'),'epoch_completion_both_sql_shape')
    need(closed['sessions']==[] and type(closed['observations']) is list and len(closed['observations'])==2,
         'epoch_completion_both_sql')
    o=certificate.observation; pg=cfg['linux']['postgres']; user=pg['admin_user']; database=pg['database']
    script=("BEGIN READ ONLY; SET LOCAL statement_timeout='2000ms'; SET LOCAL lock_timeout='1000ms'; "
        "SET LOCAL transaction_timeout='5000ms'; SET LOCAL idle_in_transaction_session_timeout='5000ms'; "
        "SET LOCAL TimeZone='UTC'; SET LOCAL DateStyle='ISO,YMD'; SET LOCAL client_encoding='UTF8'; SET LOCAL search_path='pg_catalog'; "
        "SELECT jsonb_build_object('identity',"+o.IDENTITY+",'result',("+o.CLOSURE_SQL[len('SELECT '):]+")); ROLLBACK;\n").encode()
    for side, ref in zip(('primary','standby'),closed['observations']):
        value=r.journal(ref,phase=phase)
        _shape(value,('container_id','sql','result','actual_command'),'epoch_completion_sql_shape')
        need(value['container_id']==subject[side]['container_id'] and value['sql']==o.CLOSURE_SQL
             and value['result']=={'sessions':[]},'epoch_completion_sql_sessions_remain')
        native=value['actual_command']; _shape(native,('phase','artifact','sha256'),'epoch_completion_sql_command_ref')
        row=r.journal({'phase':native['phase'],'name':native['artifact'],'sha256':native['sha256']},phase=phase)
        raw=_command(row)
        args=['exec','-i','-u',str(pg['uid']),'-e','PGPASSFILE=/dev/null','-e','PGAPPNAME=openlegal_survivor_observer',
            subject[side]['container_id'],'/usr/bin/env']
        for name in o.UNSET: args.extend(['-u',name])
        args+=['psql','-X','-w','-h','/var/run/postgresql','-p','5432','-qAt','-P','pager=off',
               '-v','ON_ERROR_STOP=1','-U',user,'-d',database]
        need(row.get('binary')=='docker' and row.get('arguments_sha256')==digest(canonical(_dispatch(r.operation)+args))
             and row.get('stdin_sha256')==digest(script),'epoch_completion_sql_dispatch')
        expected={'database':database,'session_user':user,'current_user':user,'role':{'login':True,'superuser':False,
            'create_role':False,'create_db':False,'replication':False,'bypass_rls':False,'inherit':False},
            'broad_roles':False,'owner_membership':False,'function_owners_qualified':True}
        need(_json(raw,canonical_required=False)=={'identity':expected,'result':{'sessions':[]}},
             'epoch_completion_sql_identity')


def _configuration(r, ref, subject, profile_ref):
    value=r.journal(ref)
    _shape(value,('source_profile','profiles','collection_template_sha256','collection_mapping','projection',
        'configuration_sha256','runtime_authenticated_or_ready'),'epoch_completion_configuration_shape')
    need(value['source_profile']==profile_ref and value['runtime_authenticated_or_ready'] is False
         and value['configuration_sha256']==subject['runtime']['configuration_sha256']
         ==certificate.observation.configuration_hash_v1(value['projection']), 'epoch_completion_configuration_binding')
    runtime=value['projection']['runtime']
    need(runtime['revision']==subject['runtime']['revision'] and runtime['docker_image_id']==subject['runtime']['image_id'],
         'epoch_completion_configuration_runtime')
    profile=r.external(profile_ref)
    _shape(profile,('schema_version','kind','serving','ingestion','images'),'epoch_completion_strict_profile_shape')
    need(type(profile['schema_version']) is int and profile['schema_version']==1
         and profile['kind']=='openlegal_initial_epoch_strict_configmap_pvc_v1','epoch_completion_strict_profile_kind')
    _shape(profile['images'],('serving','ingestion','document_worker'),'epoch_completion_strict_images')
    for key, image in (('serving','server'),('ingestion','server-ingestion'),('document_worker','document-worker')):
        need(type(profile['images'][key]) is str and re.fullmatch(
            'ghcr.io/publicdata-stream/openlegal-'+image+'@sha256:[0-9a-f]{64}',profile['images'][key]),
            'epoch_completion_strict_image_pin')
    need(type(value['profiles']) is list and len(value['profiles'])==2
         and {row.get('profile') for row in value['profiles']}=={'serving','ingestion'},
         'epoch_completion_configuration_profiles')
    for key in ('serving','ingestion'):
        row=profile[key]
        fields=('deployment_name','configmap_name','configmap_uid','toml_key','toml_source')
        _shape(row,fields if key=='serving' else (*fields,'collection_template_key','collection_template_source'),
            'epoch_completion_config_profile_row')
        need(valid_uuid(row['configmap_uid']) and row['toml_key']=='server.toml','epoch_completion_config_profile_key')
        raw=r.external(row['toml_source'],is_json=False)
        need(len(raw)<=256*1024,'epoch_completion_toml_bound')
        try: config=tomllib.loads(raw.decode('utf8'))
        except (tomllib.TOMLDecodeError,UnicodeError): raise Refused('epoch_completion_toml') from None
        cache=config.get('cache'); db=config.get('database')
        need(type(cache) is dict and cache.get('mode')=='persistent' and type(db) is dict,
             'epoch_completion_persistent_config')
        pg=cache.get('postgres'); roots=subject['blob_roots']
        need(type(pg) is dict and pg.get('replication_mode')=='required'
             and pg.get('replication_epoch')==subject['managed_epoch'] and pg.get('tls_mode')=='verify-full'
             and pg.get('ca_file')=='/run/secrets/postgres-ca/ca.crt' and pg.get('url_env')=='OPENLEGAL_DATABASE_URL'
             and cache.get('blob')=={'kind':'filesystem_mirror','path':roots['cache_primary']['container_path'],
                 'replica_path':roots['cache_replica']['container_path']}
             and db.get('blob_path')==roots['corpus_primary']['container_path']
             and db.get('blob_replica_path')==roots['corpus_replica']['container_path']
             and db.get('storage_failure_policy')=='recover','epoch_completion_strict_toml_binding')
        if key=='ingestion':
            ingest=db.get('ingestion'); need(type(ingest) is dict
                and ingest.get('collection_job_template_path')=='/etc/openlegal/collection-job.json'
                and ingest.get('worker_image')==profile['images']['document_worker'], 'epoch_completion_ingestion_toml_binding')
        saved=next(v for v in value['profiles'] if v['profile']==key)
        _shape(saved,('profile','toml_sha256','mapping'),'epoch_completion_config_saved_profile')
        need(saved['toml_sha256']==row['toml_source']['sha256'],'epoch_completion_config_toml_hash')
        _mapping(saved['mapping'],subject)
    row=profile['ingestion']; need(row['collection_template_key']=='collection-job.json','epoch_completion_template_key')
    template=r.external(row['collection_template_source'])
    need(type(template) is dict and value['collection_template_sha256']==row['collection_template_source']['sha256'],
         'epoch_completion_template_binding')
    _mapping(value['collection_mapping'],subject)
    return value


def _mapping(rows, subject):
    need(type(rows) is list and len(rows)==4 and {row.get('role') for row in rows}==set(certificate.ROLES),
         'epoch_completion_mapping_roles')
    for row in rows:
        _shape(row,('role','pvc_uid','pv_uid','pv_spec_sha256','host_root','container_root'),'epoch_completion_mapping_shape')
        root=subject['blob_roots'][row['role']]
        need(valid_uuid(row['pvc_uid']) and valid_uuid(row['pv_uid']) and pin(row['pv_spec_sha256'])
             and row['host_root']==root['path'] and row['container_root']==root['container_path'],
             'epoch_completion_mapping_root')


def _installation(r, proof, raw, controls):
    c=certificate.validate_certificate(raw); head=certificate.make_head(raw); head_sha=digest(head)
    roots=[{'path':v['path'],'descriptor':v['descriptor'],'expected_head':None} for v in controls]
    intent=r.journal(proof['install_intent'],phase=INSTALL)
    _shape(intent,('schema_version','kind','operation_id','certificate','certificate_sha256','head_sha256',
        'roots','observation','observation_child_exit','automatic_retry'),'epoch_completion_install_intent_shape')
    need(type(intent['schema_version']) is int and intent['schema_version']==1
         and intent['kind']=='openlegal_initial_epoch_dual_install_intent'
         and intent['operation_id']==r.operation and intent['certificate_sha256']==digest(raw)
         and intent['head_sha256']==head_sha and intent['roots']==roots
         and intent['observation']==proof['observation'] and intent['observation_child_exit']==proof['observation_child_exit']
         and intent['automatic_retry'] is False,'epoch_completion_install_intent_binding')
    saved=r.journal(intent['certificate'],phase=OBSERVATION)
    need(canonical(saved)==raw,'epoch_completion_install_certificate_changed')
    result=r.journal(proof['installations'],phase=INSTALL)
    _shape(result,('schema_version','kind','operation_id','certificate_sha256','head_sha256','intent','installations'),
        'epoch_completion_install_result_shape')
    need(type(result['schema_version']) is int and result['schema_version']==1
         and result['kind']=='openlegal_initial_epoch_install_pending_parent_exit' and result['operation_id']==r.operation
         and result['certificate_sha256']==digest(raw) and result['head_sha256']==head_sha
         and result['intent']==proof['install_intent'] and type(result['installations']) is list
         and len(result['installations'])==2,'epoch_completion_install_result_binding')
    back=r.journal(proof['independent_readback'],phase=INSTALL)
    _shape(back,('observations','certificate_sha256','head_sha256'),'epoch_completion_readback_shape')
    need(back['certificate_sha256']==digest(raw) and back['head_sha256']==head_sha
         and type(back['observations']) is list and len(back['observations'])==2,
         'epoch_completion_readback_binding')
    for root, fact, observed in zip(controls,result['installations'],back['observations']):
        desc=root['descriptor']; name='certificate-'+c['certificate_id']+'.json'
        _shape(fact,('root_identity','intent_basename','intent_sha256','certificate_basename','certificate_sha256',
            'certificate_dev','certificate_ino','head_basename','head_sha256','head_dev','head_ino','prior_head_sha256',
            'certificate_id','generation','predecessor_certificate_sha256','file_fsyncs_completed','directory_fsync_completed'),
            'epoch_completion_install_fact_shape')
        one={'schema_version':2,'kind':'openlegal_epoch_install_intent','certificate_id':c['certificate_id'],
            'certificate_sha256':digest(raw),'generation':1,'predecessor_certificate_sha256':None,
            'expected_head_sha256':None,'intended_head_sha256':head_sha,'root_identity':desc}
        need(fact['root_identity']==desc and fact['intent_basename']=='.install-intent-'+c['certificate_id']+'.json'
             and fact['intent_sha256']==digest(canonical(one)) and fact['certificate_basename']==name
             and fact['certificate_sha256']==digest(raw) and fact['head_basename']==certificate.HEAD_NAME
             and fact['head_sha256']==head_sha and fact['prior_head_sha256'] is None
             and fact['certificate_id']==c['certificate_id'] and type(fact['generation']) is int and fact['generation']==1
             and fact['predecessor_certificate_sha256'] is None and fact['file_fsyncs_completed'] is True
             and fact['directory_fsync_completed'] is True,'epoch_completion_install_fact_binding')
        _shape(observed,('root_identity','head_sha256','head_identity','certificate_basename','certificate_sha256',
            'certificate_identity'),'epoch_completion_readback_fact_shape')
        need(observed['root_identity']==desc and observed['certificate_basename']==name
             and observed['certificate_sha256']==digest(raw) and observed['head_sha256']==head_sha,
             'epoch_completion_readback_fact_binding')
        inodes=[]
        for kind, expected_raw in (('certificate',raw),('head',head)):
            row=observed[kind+'_identity']
            _shape(row,('dev','ino','uid','gid','mode','nlink','size','mtime_ns','ctime_ns'),
                'epoch_completion_readback_identity_shape')
            need(all(type(v) is int and v>=0 for v in row.values()) and row['dev']==desc['dev']
                 and row['uid']==row['gid']==0 and row['mode']==0o600 and row['nlink']==1
                 and row['size']==len(expected_raw) and type(fact[kind+'_dev']) is int
                 and type(fact[kind+'_ino']) is int and row['dev']==fact[kind+'_dev']
                 and row['ino']==fact[kind+'_ino'],'epoch_completion_readback_install_inode')
            inodes.append((row['dev'],row['ino']))
        need(inodes[0]!=inodes[1],'epoch_completion_installed_file_alias')


def _pair_ack(r, value, binding, cfg, *, first, original_end):
    _shape(value,('before','after','fixed_target','native_window','native_command_refs','observation_status'),
        'epoch_completion_pair_ack_shape')
    need(value['observation_status']=='historical_native_proof','epoch_completion_pair_ack_kind')
    window=value['native_window']; _shape(window,('started','finished','deadline'),'epoch_completion_pair_window')
    _window(dict(window,original=original_end),'started','finished','deadline','original')
    need(window['deadline']<=window['started']+2,'epoch_completion_pair_two_seconds')
    refs=value['native_command_refs']; need(type(refs) is list and 1<=len(refs)<=33,'epoch_completion_pair_native_count')
    user=cfg['linux']['postgres']['admin_user']; subject=cfg['linux']['postgres']; outputs=[]
    for index, ref in enumerate(refs):
        row=r.journal(ref,phase=PAIR); raw=_command(row); kind=('bind' if first else 'proof') if index==0 else 'flush'
        need(row['stdout_bytes']<=65536 and row['stderr_bytes']<=65536,'epoch_completion_native_output_bound')
        need(row.get('kind')=='openlegal_initial_pair_native_sql' and row.get('query_kind')==kind
             and row.get('binary_sha256')==cfg['linux']['binaries']['docker']['sha256'],
             'epoch_completion_pair_native_source')
        _window(row,'native_started_at','native_finished_at','native_absolute_deadline','original_operation_deadline')
        need(row['native_absolute_deadline']==window['deadline'] and row['original_operation_deadline']==original_end
             and row['native_started_at']>=window['started'] and row['native_finished_at']<=window['finished'],
             'epoch_completion_pair_native_window')
        lines=[_json(line,canonical_required=False) for line in raw.splitlines()]
        need(len(lines)==(3 if kind=='bind' else 2 if kind=='proof' else 1),'epoch_completion_pair_native_output')
        if kind=='bind':
            bound=lines[0]
            _shape(bound,('operation_id','intent_sha256','previous_binding','new_binding','peer_before_commit'),
                'epoch_completion_first_bind_result')
            normalized=copy.deepcopy(bound['new_binding'])
            _shape(normalized,binding,'epoch_completion_first_bind_control')
            serial=normalized['standby_client_serial']
            need(type(serial) in (int,str) and re.fullmatch('[0-9]{1,60}',str(serial)),
                 'epoch_completion_first_bind_serial')
            normalized['standby_client_serial']=str(int(serial))
            normalized['standby_addr']=_host_address(normalized['standby_addr'])
            need(bound['operation_id']==r.operation and bound['previous_binding'] is None and normalized==binding,
                 'epoch_completion_not_initial_bind')
            intent=r.pair_intent(bound['intent_sha256'])
            _shape(intent,('operation_id','expected_old','proposed_binding','candidate','ack_budget_seconds',
                'caller_original_absolute_deadline'),'epoch_completion_pair_intent_shape')
            need(intent['operation_id']==r.operation and intent['expected_old'] is None and intent['proposed_binding']==binding
                 and type(intent['ack_budget_seconds']) is int and intent['ack_budget_seconds']==2
                 and intent['caller_original_absolute_deadline']==original_end,'epoch_completion_pair_intent_binding')
            sql=("BEGIN; SELECT openlegal_checkpoint.bind_replica('"+r.operation+"'::uuid,'"+bound['intent_sha256']+
                "',NULL::jsonb,'"+canonical(binding).decode().strip().replace("'","''")+
                "'::jsonb); COMMIT; SELECT openlegal_replication.capture_prefix(); SELECT openlegal_replication.observe_flush()")
        else: sql=('SELECT openlegal_replication.capture_prefix(); ' if kind=='proof' else '')+'SELECT openlegal_replication.observe_flush()'
        prefix=("SET statement_timeout='2s'; SET lock_timeout='1s'; SET transaction_timeout='5s'; "
            "SET standard_conforming_strings=on; SET TIME ZONE 'UTC'; SET DateStyle='ISO,YMD'; SET bytea_output='hex'; ")
        unset=(*certificate.observation.UNSET,'PGAPPNAME')
        args=['exec','-i','-u','70',subject['primary']['container_id'],'/usr/bin/env',
            *[v for name in unset for v in ('-u',name)],'PGPASSFILE=/dev/null','/usr/local/bin/psql',
            '-X','-w','-h','/var/run/postgresql','-p','5432','-qAt','-v','ON_ERROR_STOP=1','-U',user,'-d','openlegal']
        need(row.get('arguments_sha256')==digest(canonical(_dispatch(r.operation)+args))
             and row.get('stdin_sha256')==digest((prefix+sql+';\n').encode()),'epoch_completion_pair_fixed_dispatch')
        outputs.append(lines)
    before=outputs[0][-2]; after=outputs[-1][-1]
    peers=[peer for index,values in enumerate(outputs) for peer in (values[-2:] if index==0 else values)]
    static=set(before)-{'prefix_lsn','flush_lsn'}
    for peer in peers:
        _peer(peer,binding)
        need(set(peer)-{'prefix_lsn','flush_lsn'}==static and all(peer[k]==before[k] for k in static),
             'epoch_completion_native_peer_changed_during_poll')
    if first:
        committed=outputs[0][0]['peer_before_commit']
        _proof(committed,after,binding,previous=intent['candidate'])
        computed=_proof(before,after,binding,previous=committed)
    else: computed=_proof(before,after,binding)
    need(all(value[k]==computed[k] for k in computed),'epoch_completion_pair_ack_changed')


def _seed_references(r, seeded, cfg):
    refs=seeded['reference_pages']; need(type(refs) is list and 1<=len(refs)<=1564,'epoch_completion_seed_page_count')
    rows=[]; last=None; source_count=None; previous_size=128
    for ref in refs:
        _shape(ref,('name','sha256','offset','rows'),'epoch_completion_seed_page_ref')
        need(type(ref['offset']) is int and ref['offset']==len(rows)
             and type(ref['rows']) is int and 0<=ref['rows']<=128 and previous_size==128,
             'epoch_completion_seed_page_order')
        page=r.journal({'phase':SEED,'name':ref['name'],'sha256':ref['sha256']},phase=SEED)
        _shape(page,('offset','source_rows','rows','actual_command'),'epoch_completion_seed_page_shape')
        need(page['offset']==len(rows) and type(page['source_rows']) is int and 0<=page['source_rows']<=200000
             and type(page['rows']) is list and len(page['rows'])==ref['rows']
             and len(rows)+len(page['rows'])<=page['source_rows']
             and (source_count is None or source_count==page['source_rows']), 'epoch_completion_seed_page_binding')
        source_count=page['source_rows']; previous_size=len(page['rows'])
        cmd=page['actual_command']; _shape(cmd,('name','sha256'),'epoch_completion_seed_command_ref')
        receipt=r.journal(dict(cmd,phase=SEED),phase=SEED); raw=_command(receipt)
        need(_json(raw,canonical_required=False)=={'source_rows':source_count,'rows':page['rows']},
             'epoch_completion_seed_raw_page_mismatch')
        # Pin the exact source-owned query and target; no SQL fragment from DATA.
        offset=len(rows)
        sql=("WITH source AS (SELECT openlegal_checkpoint.retained_references() AS refs), "
            "refs AS (SELECT DISTINCT value FROM source CROSS JOIN LATERAL jsonb_array_elements(source.refs) AS t(value)) "
            "SELECT jsonb_build_object('source_rows',(SELECT jsonb_array_length(refs) FROM source),"
            "'rows',COALESCE(jsonb_agg(value ORDER BY (value->>'role') COLLATE \"C\","
            "(value->>'storage_key') COLLATE \"C\"),'[]'::jsonb)) "
            "FROM (SELECT value FROM refs ORDER BY (value->>'role') COLLATE \"C\","
            "(value->>'storage_key') COLLATE \"C\" OFFSET %d LIMIT 128) q")%offset
        prefix=("SET statement_timeout='30s'; SET lock_timeout='1s'; SET transaction_timeout='35s'; "
            "SET TIME ZONE 'UTC'; SET DateStyle='ISO,YMD'; SET bytea_output='hex'; ")
        args=['exec','-i','-u','70','-e','PGPASSFILE=/dev/null','-e','PGPASSWORD=','-e','PGSERVICE=',
            '-e','PGSERVICEFILE=/dev/null','-e','PGHOSTADDR=','-e','PGOPTIONS=','-e','PGSSLCERT=',
            '-e','PGSSLKEY=','-e','PGSSLROOTCERT=','-e','PGAPPNAME=openlegal_initial_blob_seed',
            cfg['linux']['postgres']['primary']['container_id'],'/usr/bin/env','-u','PGSERVICE','psql','-X','-w',
            '-h','/var/run/postgresql','-p','5432','-qAt','-v','ON_ERROR_STOP=1','-U','openlegal_migration','-d','openlegal']
        need(receipt.get('binary')=='docker' and receipt.get('arguments_sha256')==digest(canonical(_dispatch(r.operation)+args))
             and receipt.get('stdin_sha256')==digest((prefix+sql+';\n').encode()),'epoch_completion_seed_dispatch')
        for row in page['rows']:
            _shape(row,('role','storage_key','sha256','size'),'epoch_completion_reference_shape')
            need(row['role'] in ('cache','corpus') and pin(row['sha256']) and type(row['size']) is int
                 and 0<=row['size']<=100*1024**2 and type(row['storage_key']) is str,
                 'epoch_completion_reference_domain')
            match=re.fullmatch(r'([0-9a-f]{2})/([0-9a-f]{64})-([0-9a-f-]{36})',row['storage_key'])
            need(match and match[1]==row['sha256'][:2] and match[2]==row['sha256'] and valid_uuid(match[3]),
                 'epoch_completion_reference_key')
            key=(row['role'],row['storage_key']); need(last is None or key>last,'epoch_completion_reference_order')
            last=key; rows.append(row)
    need(previous_size<128 and len(rows)<=200000,'epoch_completion_seed_incomplete_pages')
    return rows


def _snapshot(r, ref, cfg, subject, references):
    phase=ref['phase']; value=r.journal(ref,phase=OBSERVATION)
    _shape(value,('container_id','sql','result','actual_command','fixed_primary_target','initial_primary_fence'),
        'epoch_completion_snapshot_shape')
    o=certificate.observation; pg=cfg['linux']['postgres']; user=pg['admin_user']; database=pg['database']
    need(value['container_id']==subject['standby']['container_id'] and value['sql']==o.SNAPSHOT_SQL,
         'epoch_completion_snapshot_target')
    native=value['actual_command']; _shape(native,('phase','artifact','sha256'),'epoch_completion_snapshot_command_ref')
    row=r.journal({'phase':native['phase'],'name':native['artifact'],'sha256':native['sha256']},phase=phase)
    raw=_command(row)
    script=("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL statement_timeout='30000ms'; "
        "SET LOCAL lock_timeout='1000ms'; SET LOCAL transaction_timeout='35000ms'; "
        "SET LOCAL idle_in_transaction_session_timeout='5000ms'; "
        "SET LOCAL TimeZone='UTC'; SET LOCAL DateStyle='ISO,YMD'; SET LOCAL client_encoding='UTF8'; SET LOCAL search_path='pg_catalog'; "
        "SELECT jsonb_build_object('identity',"+o.IDENTITY+",'result',("+o.SNAPSHOT_SQL[len('SELECT '):]+")); ROLLBACK;\n").encode()
    args=['exec','-i','-u',str(pg['uid']),'-e','PGPASSFILE=/dev/null','-e','PGAPPNAME=openlegal_survivor_observer',
        subject['standby']['container_id'],'/usr/bin/env']
    for name in o.UNSET: args.extend(['-u',name])
    args+=['psql','-X','-w','-h','/var/run/postgresql','-p','5432','-qAt','-P','pager=off','-v','ON_ERROR_STOP=1','-U',user,'-d',database]
    need(row.get('binary')=='docker' and row.get('arguments_sha256')==digest(canonical(_dispatch(r.operation)+args))
         and row.get('stdin_sha256')==digest(script),'epoch_completion_snapshot_dispatch')
    result=_json(raw,canonical_required=False)
    _shape(result,('identity','result'),'epoch_completion_snapshot_envelope')
    identity={'database':database,'session_user':user,'current_user':user,'role':{'login':True,'superuser':False,
        'create_role':False,'create_db':False,'replication':False,'bypass_rls':False,'inherit':False},
        'broad_roles':False,'owner_membership':False,'function_owners_qualified':True}
    need(result=={'identity':identity,'result':value['result']},'epoch_completion_snapshot_raw_binding')
    snapshot=value['result']; _shape(snapshot,('position_before','position_after','transaction','retained_tables',
        'retained_references','provider','storage_metadata'),'epoch_completion_snapshot_result_shape')
    need(snapshot['retained_references']==references and snapshot['transaction']=={'isolation':'repeatable read','read_only':True,
        'statement_timeout_ms':30000,'lock_timeout_ms':1000,'transaction_timeout_ms':35000,
        'idle_in_transaction_session_timeout_ms':5000},'epoch_completion_snapshot_transaction_or_references')
    first=_primary_fence(r,value['initial_primary_fence'],cfg,subject)
    need(value['initial_primary_fence']['phase']==OBSERVATION and value['fixed_primary_target']==first['fixed_target'],
         'epoch_completion_snapshot_fixed_target')
    for key in ('position_before','position_after'):
        position=snapshot[key]
        _shape(position,('schema_version','kind','in_recovery','system_identifier','timeline','postmaster_started_at',
            'data_directory','control','receive_lsn','replay_lsn','replay_paused','wal_receiver_count'),
            'epoch_completion_standby_position_shape')
        need(type(position['schema_version']) is int and position['schema_version']==1
             and position['kind']=='openlegal_survivor_position' and position['in_recovery'] is True
             and position['system_identifier']==subject['control']['primary_system_identifier']
             and type(position['timeline']) is int and position['timeline']==subject['control']['timeline']
             and position['postmaster_started_at']==subject['standby']['postmaster_started_at']
             and position['data_directory']==subject['standby']['data_directory'] and position['control']==subject['control']
             and position['replay_paused'] is False and type(position['wal_receiver_count']) is int
             and 0<=position['wal_receiver_count']<=1 and _lsn(position['replay_lsn'])>=_lsn(first['fixed_target']),
             'epoch_completion_standby_position_binding')
        _lsn(position['receive_lsn'])
    tables=snapshot['retained_tables']; need(type(tables) is dict and 1<=len(tables)<=256,'epoch_completion_table_bound')
    for name, row in tables.items():
        _shape(row,('rows','sha256'),'epoch_completion_table_shape')
        need(re.fullmatch(r'[a-z_][a-z0-9_]{0,62}\.[a-z_][a-z0-9_]{0,62}',name)
             and type(row['rows']) is int and row['rows']>=0 and pin(row['sha256']),'epoch_completion_table_domain')
    need(tables['public._sqlx_migrations']['sha256']==subject['runtime']['migration_table_sha256'],
         'epoch_completion_migration_fingerprint')
    provider=snapshot['provider']; _shape(provider,('state','hold_control','lock_provenance'),'epoch_completion_provider_shape')
    state=provider['state']; control=provider['hold_control']
    _shape(state,('budget','control','admissions','canary_control','canaries'),'epoch_completion_provider_state_shape')
    need(provider['lock_provenance']=='primary_advisory_locks_not_observable_on_standby'
         and state['budget'].get('operator_suspended') is True and type(state['budget'].get('unresolved_response')) is bool
         and (state['budget'].get('admission_owner') is None or valid_uuid(state['budget']['admission_owner']))
         and type(control) is dict and valid_uuid(control.get('generation')) and control.get('recovery_hold') is True
         and state['control']=={k:v for k,v in control.items() if k not in ('generation','recovery_hold','hold_started_at')}
         and type(state['admissions']) is list and len(state['admissions'])<=16
         and type(state['canaries']) is list and len(state['canaries'])<=1,'epoch_completion_provider_held')
    storage=snapshot['storage_metadata']
    _shape(storage,('corpus_control','cache_storage','cache_totals'),'epoch_completion_storage_metadata_shape')
    corpus=storage['corpus_control']; cache=storage['cache_storage']; totals=storage['cache_totals']
    need(type(corpus) is dict and type(corpus.get('next_event')) is int and corpus['next_event']>=1
         and type(corpus.get('index_ack')) is int and 0<=corpus['index_ack']<corpus['next_event']
         and type(cache) is dict and valid_uuid(cache.get('id')),'epoch_completion_storage_identity')
    _shape(totals,('snapshots','queries','referenced_bytes'),'epoch_completion_cache_totals_shape')
    need(all(type(v) is int and v>=0 for v in totals.values()),'epoch_completion_cache_totals_domain')


def _predecessors(r, provenance, q, deps, cfg, subject, observation, parent):
    seeded=r.journal(provenance['seed'],phase=SEED)
    need(seeded.get('kind')=='openlegal_initial_blob_seed_result' and seeded.get('operation_id')==r.operation
         and seeded.get('managed_epoch')==subject['managed_epoch']
         and seeded.get('blob_binding_sha256')==subject['control']['blob_binding_sha256']
         and seeded.get('all_original_generations_preserved') is True
         and all(seeded.get(k) is False for k in ('postgres_replica_created','roles_created','workload_started',
             'provider_called','ready','automatic_retry')),'epoch_completion_seed_binding')
    roots=seeded['roots']; need(type(roots) is list and len(roots)==4 and {v['role'] for v in roots}==set(certificate.ROLES),
        'epoch_completion_seed_roots')
    for row in roots:
        actual=subject['blob_roots'][row['role']]
        need(row['host_path']==actual['path'] and all(row['root_'+key]==actual[key]
            for key in ('dev','ino','uid','gid','mode')) and all(row[key]==actual[key]
            for key in ('container_path','filesystem_id','kernel_filesystem_id','marker_sha256')),
            'epoch_completion_seed_root_identity')
    pair=r.journal(provenance['first_pair'],phase=PAIR)
    need(pair.get('kind')=='openlegal_initial_pg_pair_result' and pair.get('operation_id')==r.operation
         and pair.get('inventory_sha256')==q['pair_inventory']['sha256']
         and pair.get('authority_sha256')==q['pair_authority']['sha256']
         and pair.get('source_sha256')==cfg['source_graph']['initial_pg_pair.py']
         and pair.get('managed_epoch')==subject['managed_epoch'] and pair.get('binding')==subject['control']
         and pair.get('blob_roots')==roots and all(pair.get(side+'_container_id')==subject[side]['container_id']
             and pair.get(side+'_process')==subject[side]['process'] for side in ('primary','standby'))
         and all(pair.get(k) is False for k in ('ready','provider_called','roles_created','released','automatic_retry')),
         'epoch_completion_first_pair_binding')
    inputs=r.journal(pair['input_binding'],phase=PAIR)
    need(inputs.get('inventory')==q['pair_inventory'] and inputs.get('authority')==q['pair_authority']
         and inputs.get('role_prepare')==q['role_prepare'] and inputs.get('maintenance')==q['maintenance']
         and inputs.get('seed_inventory')==q['seed_inventory'] and inputs.get('seed_authority')==q['seed_authority'],
         'epoch_completion_pair_input_binding')
    original=inputs['original_absolute_deadline']
    need(r.original_end is None or original==r.original_end,'epoch_completion_original_deadline_changed')
    r.original_end=original
    _pair_ack(r,pair['first_bind_ack'],subject['control'],cfg,first=True,original_end=original)
    _pair_ack(r,pair['final_ack'],subject['control'],cfg,first=False,original_end=original)
    need(pair['final_ack']['after']['postmaster_started_at']==subject['primary']['postmaster_started_at'],
         'epoch_completion_pair_postmaster_changed')
    role=r.journal(q['role_prepare'],phase='initial_role_prepare')
    maintenance=r.journal(q['maintenance'],phase='initial_provider_maintenance')
    for value, kind, basename in ((role,'openlegal_initial_role_prepare_result','initial_role_prepare.py'),
            (maintenance,'openlegal_initial_provider_maintenance_result','initial_provider_maintenance.py')):
        need(value.get('kind')==kind and value.get('operation_id')==r.operation and value.get('producer_source')=={
            'path':str(Path(r.roots['source'][2])/basename),'sha256':cfg['source_graph'][basename]},
            'epoch_completion_fixed_predecessor_source')
        inventory=r.external(value['inventory']); authority=r.external(value['authority'])
        need(inventory.get('operation_id')==r.operation and inventory.get('source_sha256')==cfg['source_graph'][basename]
             and authority.get('approved') is True and authority.get('operation_id')==r.operation
             and authority.get('inventory_sha256')==value['inventory']['sha256']
             and authority.get('source_sha256')==cfg['source_graph'][basename],
             'epoch_completion_predecessor_authority_binding')
    need(role.get('parent')==parent and role.get('seed_complete')==provenance['seed']
         and role.get('role_names',{}).get('recovery_login')==cfg['linux']['postgres']['admin_user']
         and pair.get('provider_recovery_operation')==maintenance.get('provider_recovery_operation')
         and maintenance.get('authority_sha256')==maintenance['authority']['sha256'],
         'epoch_completion_maintenance_role_binding')
    references=_seed_references(r,seeded,cfg)
    _snapshot(r,observation['standby_snapshot'],cfg,subject,references)
    audit=r.journal(observation['four_root_audit'],phase=OBSERVATION)
    _shape(audit,('roots','audit','references_sha256'),'epoch_completion_root_audit_shape')
    need(audit['roots']==subject['blob_roots'] and audit['references_sha256']==digest(canonical(references))
         and type(audit['audit']) is list and len(audit['audit'])==4
         and {row['role'] for row in audit['audit']}==set(certificate.ROLES),'epoch_completion_root_audit_binding')
    for row in audit['audit']:
        _shape(row,('role','permanent_sha256','generations','references'),'epoch_completion_root_audit_row')
        original=next(v for v in roots if v['role']==row['role'])
        need(row['permanent_sha256']==original['manifest_sha256'] and type(row['generations']) is int
             and row['generations']>=0 and type(row['references']) is int
             and row['references']==sum(v['role']==row['role'].split('_')[0] for v in references),
             'epoch_completion_root_audit_counts')
    physical=r.journal(observation['physical_fence'],phase=OBSERVATION)
    _shape(physical,('cri_containers','cgroups'),'epoch_completion_physical_fence_shape')
    need(type(physical['cri_containers']) is list and len(physical['cri_containers'])<=4096
         and all(row.get('state')=='CONTAINER_EXITED' for row in physical['cri_containers'])
         and type(physical['cgroups']) is list and 1<=len(physical['cgroups'])<=4096
         and all(row.get('pids')==[] for row in physical['cgroups']),'epoch_completion_historical_writers_not_closed')
    migrations=r.journal(observation['migrations'],phase=OBSERVATION)
    _shape(migrations,('rows','actual_command'),'epoch_completion_migrations_shape')
    manifest=r.external(cfg['initial_migration']['compiled_manifest'])
    need(manifest.get('kind')=='openlegal_compiled_migration_manifest' and type(manifest.get('migrations')) is list
         and len(manifest['migrations'])==33 and [v.get('version') for v in manifest['migrations']]==list(range(1,34))
         and migrations['rows']==[dict(v,success=True) for v in manifest['migrations']],
         'epoch_completion_compiled_migrations')
    for row in manifest['migrations']:
        _shape(row,('version','checksum'),'epoch_completion_migration_shape')
        need(type(row['version']) is int and type(row['checksum']) is str and re.fullmatch('[0-9a-f]{96}',row['checksum']),
             'epoch_completion_migration_checksum')
    receipt=r.journal(migrations['actual_command'],phase=OBSERVATION); raw=_command(receipt)
    sql=("SELECT COALESCE(jsonb_agg(jsonb_build_object('version',version,'checksum',"
        "encode(checksum,'hex'),'success',success) ORDER BY version),'[]'::jsonb) FROM public._sqlx_migrations")
    script=("BEGIN READ ONLY; SET LOCAL statement_timeout='2s'; SET LOCAL lock_timeout='1s'; "
        "SET LOCAL transaction_timeout='5s'; SET LOCAL search_path=pg_catalog; "+sql+'; ROLLBACK;\n').encode()
    args=_psql(subject,cfg['linux']['postgres']['admin_user']); args.remove('PGSERVICEFILE=/dev/null')
    need(receipt.get('binary')=='docker' and receipt.get('stdin_sha256')==digest(script)
         and receipt.get('arguments_sha256')==digest(canonical(_dispatch(r.operation)+args))
         and _json(raw,canonical_required=False)==migrations['rows'], 'epoch_completion_actual_migrations')


def _read_completed(r, raw, completion_ref, operation, producer_sha, graph_sha):
    c=certificate.validate_certificate(raw); certificate.validate_transition(raw,None); p=c['provenance']; s=c['subject']
    need(valid_uuid(operation) and p['operation_id']==operation and pin(producer_sha) and pin(graph_sha)
         and p['producer_source']['sha256']==producer_sha, 'epoch_completion_expected_identity')
    r.operation=operation; r.graph_sha=graph_sha
    need(p['producer_source']['path']==str(Path(r.roots['source'][2])/'initial_epoch_certificate.py'),
         'epoch_completion_actual_producer_path')
    graphpath=_path(p['source_graph']['path']); base=Path(r.roots['archive'][2])/operation/OBSERVATION
    need(graphpath.parent==base and NAME.fullmatch(graphpath.name), 'epoch_completion_graph_path')
    graph=r.journal({'phase':OBSERVATION,'name':graphpath.name,'sha256':p['source_graph']['sha256']},phase=OBSERVATION)
    need(type(graph) is dict and set(graph)==PRODUCER_SOURCES and all(pin(v) for v in graph.values())
         and digest(canonical(graph))==graph_sha and graph['initial_epoch_certificate.py']==producer_sha,
         'epoch_completion_closed_source_graph')
    for name, sha in sorted(graph.items()): r.read('source',(name,),sha,limit=4*1024**2,is_json=False)
    cfg=r.external(p['inventory']); auth=r.external(p['authority'])
    _shape(cfg,INVENTORY_FIELDS,'epoch_completion_inventory_shape'); _shape(auth,AUTHORITY_FIELDS,'epoch_completion_authority_shape')
    r.inventory_sha=p['inventory']['sha256']; r.authority_sha=p['authority']['sha256']
    need(type(cfg['schema_version']) is int and cfg['schema_version']==1
         and cfg['kind']=='openlegal_initial_epoch_certificate_inventory' and cfg['operation_id']==operation
         and cfg['managed_epoch']==s['managed_epoch'] and cfg['revision']==s['runtime']['revision']
         and cfg['host']['boot_id']==s['host_boot_id'] and cfg['source_graph']==graph,
         'epoch_completion_inventory_binding')
    need(type(auth['schema_version']) is int and auth['schema_version']==1
         and auth['kind']=='openlegal_initial_epoch_certificate_authority' and auth['operation_id']==operation
         and auth['inventory_sha256']==r.inventory_sha and auth['source_graph_sha256']==graph_sha
         and auth['source_sha256']==producer_sha and type(auth['valid_until']) is int
         and all(auth[k] is True for k in ('approved','epoch_certificate_observation_permitted',
             'epoch_certificate_install_permitted','initial_bind_only','preserve_originals'))
         and all(auth[k] is False for k in AUTHORITY_FIELDS if k.endswith('_permitted') and k not in
             ('epoch_certificate_observation_permitted','epoch_certificate_install_permitted'))
         and auth['automatic_retry'] is False,'epoch_completion_historical_authority_scope')
    q=cfg['epoch_certificate']; _shape(q,INPUT_FIELDS,'epoch_completion_inputs_shape')
    need(q['certificate_id']==c['certificate_id'] and q['reason']=='initial_bind' and type(q['generation']) is int
         and q['generation']==1 and q['predecessor_certificate_sha256'] is None
         and q['pair_complete']==p['first_pair'] and q['original_inventory']==s['original_inventory'],
         'epoch_completion_initial_inputs')
    deps={k:r.external(q[k]) for k in ('original_inventory','driver_inventory','driver_authority',
        'seed_inventory','seed_authority','pair_inventory','pair_authority','strict_preprovision')}
    # Fixed dependency bindings; these files are historical DATA, never loaders.
    for key in ('driver_inventory','seed_inventory','pair_inventory'):
        need(deps[key].get('operation_id')==operation, 'epoch_completion_dependency_operation')
    need(deps['driver_inventory'].get('legacy_inventory')==q['original_inventory']
         and deps['seed_inventory'].get('driver_inventory')==q['driver_inventory']
         and deps['seed_inventory'].get('driver_authority')==q['driver_authority']
         and deps['pair_inventory'].get('seed_inventory')==q['seed_inventory']
         and deps['pair_inventory'].get('seed_authority')==q['seed_authority']
         and deps['seed_inventory'].get('source_sha256')==graph['initial_blob_seed.py']
         and deps['pair_inventory'].get('source_sha256')==graph['initial_pg_pair.py'],
         'epoch_completion_dependency_chain')
    for key in ('driver','seed','pair'):
        a=deps[key+'_authority']; invref=q[key+'_inventory']
        need(a.get('approved') is True and a.get('operation_id')==operation
             and a.get('inventory_sha256')==invref['sha256'] and type(a.get('valid_until')) is int,
             'epoch_completion_dependency_authority')
    controls=q['epoch_roots']; need(type(controls) is list and len(controls)==2
        and {v.get('label') for v in controls}=={'ssd','hdd'},'epoch_completion_control_roots')
    for row in controls:
        _shape(row,('label','path','descriptor','media'),'epoch_completion_control_root_shape')
        _path(row['path']); desc=row['descriptor']
        _shape(desc,certificate.ROOT_FIELDS,'epoch_completion_control_descriptor_shape')
        need(all(type(v) is int and v>=0 for v in desc.values()) and desc['uid']==desc['gid']==0
             and desc['mode']==0o700,'epoch_completion_control_descriptor')
        media=row['media']
        _shape(media,('kind','physical_leaves') if row['label']=='ssd' else ('kind','dataset','guid','mountpoint'),
            'epoch_completion_control_media_shape')
        need(media['kind']==row['label'],'epoch_completion_control_media_kind')
    need(controls[0]['path']!=controls[1]['path'],'epoch_completion_control_path_alias')
    ssd=next(v for v in controls if v['label']=='ssd')
    need(ssd['path']==r.roots['archive'][2] and ssd['descriptor']==r.roots['archive'][1],
         'epoch_completion_original_archive_root')
    need(controls[0]['descriptor']['dev']!=controls[1]['descriptor']['dev'], 'epoch_completion_historical_independent_roots')
    proof=r.journal(completion_ref,phase=INSTALL); _shape(proof,PROOF_FIELDS,'epoch_completion_proof_shape')
    need(type(proof['schema_version']) is int and proof['schema_version']==2
         and proof['kind']=='openlegal_pre_ack_epoch_install' and proof['operation_id']==operation
         and proof['inventory']==p['inventory'] and proof['authority']==p['authority']
         and proof['source_graph_sha256']==graph_sha and proof['certificate_id']==c['certificate_id']
         and proof['certificate_sha256']==digest(raw) and proof['subject_sha256']==digest(canonical(s))
         and type(proof['generation']) is int and proof['generation']==1 and proof['predecessor_certificate_sha256'] is None
         and proof['observation']==p['observation'] and proof['observation_child_exit']==p['observation_child_exit']
         and all(proof[k] is False for k in ('ready','public_ack','automatic_retry'))
         and proof['outer_actual_os_exit']=='NOT_OBSERVABLE_IN_PROCESS','epoch_completion_proof_binding')
    _process(proof['parent'],s['host_boot_id'])
    obs_exit=r.journal(proof['observation_child_exit'],phase=OBSERVATION)
    ins_exit=r.journal(proof['install_child_exit'],phase=INSTALL)
    for child in (obs_exit,ins_exit): _child(child,s['host_boot_id'],proof['parent'])
    need((obs_exit['pid'],obs_exit['start_ticks'])!=(ins_exit['pid'],ins_exit['start_ticks']),
         'epoch_completion_child_alias')
    obs=r.journal(proof['observation'],phase=OBSERVATION)
    _shape(obs,('schema_version','kind','operation_id','inventory_sha256','authority_sha256','source_graph_sha256',
        'subject','configuration','standby_snapshot','migrations','four_root_audit','physical_fence','primary_fence','sql_closure'),
        'epoch_completion_observation_shape')
    need(type(obs['schema_version']) is int and obs['schema_version']==1
         and obs['kind']=='openlegal_initial_epoch_observation_pending_parent_exit'
         and obs['operation_id']==operation and obs['inventory_sha256']==r.inventory_sha
         and obs['authority_sha256']==r.authority_sha and obs['source_graph_sha256']==graph_sha
         and obs['subject']==s,'epoch_completion_observation_binding')
    _configuration(r,obs['configuration'],s,q['strict_preprovision'])
    _configuration(r,proof['final_configuration'],s,q['strict_preprovision'])
    for ref, phase in ((obs['primary_fence'],OBSERVATION),(proof['final_primary_fence'],INSTALL)):
        need(ref['phase']==phase,'epoch_completion_fence_phase'); _primary_fence(r,ref,cfg,s)
    for ref, phase in ((obs['sql_closure'],OBSERVATION),(proof['final_sql_closure'],INSTALL)):
        need(ref['phase']==phase,'epoch_completion_closure_phase'); _both_sql(r,ref,cfg,s)
    # Continue only when every predecessor/native evidence branch below is checked.
    _predecessors(r,p,q,deps,cfg,s,obs,proof['parent'])
    _installation(r,proof,raw,controls)
    r.finish()
    return {'kind':'openlegal_historical_initial_epoch_completion','historical_only':True,
        'operation_id':operation,'certificate_sha256':digest(raw),'source_graph_sha256':graph_sha,
        'certificate_raw':raw,'completion_reference':copy.deepcopy(completion_ref),
        'completion_raw':r.files[('archive',(operation,INSTALL,completion_ref['name']))]['raw'],
        'roots':{k:{'descriptor':copy.deepcopy(v[1]),'path':v[2]} for k,v in r.roots.items()},
        'records':tuple({'root':key[0],'relative':list(key[1]),'sha256':row['sha256'],
            'raw':row['raw'],'identity':dict(row['identity'])} for key,row in sorted(r.files.items())),
        'unique_records':len(r.files),'unique_bytes':r.bytes,'legacy_typed_listings':copy.deepcopy(r.listings)}


def read_completed_install(archive_fd, archive_descriptor, source_root, control_root,
                          certificate_raw, completion_reference, deadline, *, expected_operation_id,
                          expected_producer_sha256, expected_source_graph_sha256, max_records, max_bytes):
    """Return bounded historical bytes only; caller retains all roots and locks."""
    r=_Reader(archive_fd,archive_descriptor,source_root,control_root,deadline,max_records,max_bytes)
    try:
        return _read_completed(r,certificate_raw,completion_reference,expected_operation_id,
            expected_producer_sha256,expected_source_graph_sha256)
    finally: r.close()
