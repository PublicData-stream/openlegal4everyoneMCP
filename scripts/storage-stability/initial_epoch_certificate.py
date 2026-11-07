"""Initial-bind-only observation/install producer; never activation or Ready.

One live ROOT consumes its genuine first-pair result, then owns two source-fixed
children. The observation child cannot claim its own OS exit; the certificate
binds ROOT's later actual wait/closure artifact. The installer also cannot claim
its own exit; only ROOT emits a detached non-Ready proof after independent current
head reads and a new native primary fence. No authority, configuration, role,
credential, PG binding, Kubernetes mutation, cleanup or retry is provided here.

The source-owned caller must retain backend/SharedLock/FDs on every failure. A
failed copy-fit gate does not authorize actual enrollment merely because this
source exists. Native/physical acceptance and activation consumers are separate.
"""
from __future__ import annotations

import copy
import fcntl
import hashlib
import os
from pathlib import Path
import re
import select
import signal
import socket
import stat
import sys
import threading
import time
import tomllib
import uuid

import bootstrap_backend as blobs
import command_process as processes
import epoch_certificate as certificates
import epoch_current as current
import epoch_observation as observation
import epoch_primary_fence as fences
import initial_blob_seed as seed
import initial_driver as driver
import initial_migration as migration
import initial_pg_pair as pair_module
import initial_role_prepare as roles
import journal as journals
import legacy_prepare as legacy
import linux_backend as linux
import manual_backend as manual
from copy_tree import root_fd
from interface import Operation, Refused, freeze_authority, need
from inventory import (MAX_INPUT, canonical, decode, digest, pin, private_bytes, valid_uuid,
    private_fd_mount_id, recheck_private_inbox, read_private_file_at)

OBSERVATION = 'initial_epoch_observation'
INSTALL = 'initial_epoch_install_verify'
RECORDS = 384
BYTES = 128 * 1024**2
# Every native backend receipt can contain up to MAX_OUTPUT bytes per stream,
# encoded as hex plus fixed metadata. Keep this conservative headroom before
# EVERY source-owned seal/dispatch, so even shared backend artifacts fit HARD cap.
COMMAND_HEADROOM = 4 * linux.MAX_OUTPUT + 1024**2
MAX_HANDOFF = 4 * 1024**2
PROFILE_LIMIT = 256 * 1024
SOURCES = driver.SOURCES | frozenset(('initial_blob_seed.py','initial_role_prepare.py',
    'initial_provider_maintenance.py','initial_pg_pair.py','epoch_observation.py',
    'epoch_primary_fence.py','epoch_certificate.py','epoch_current.py','initial_epoch_certificate.py'))
BASE_FIELDS = frozenset(('schema_version','kind','operation_id','revision','managed_epoch','host','shared_lock',
    'journal_roots','backup_root','original_datasets','active_datasets','ssd_trees','coverage','kubernetes',
    'postgres','linux','provider_hold','source_graph','legacy_prepare','initial_migration','epoch_certificate'))
INPUT_FIELDS = ('certificate_id','reason','generation','predecessor_certificate_sha256','original_inventory',
    'driver_inventory','driver_authority','seed_inventory','seed_authority','pair_inventory','pair_authority',
    'pair_complete','role_prepare','maintenance','strict_preprovision','epoch_roots')
IMAGE_FORMAT = ('{"Id":{{json .Id}},"Revision":{{json (index .Config.Labels "org.opencontainers.image.revision")}},'
                '"Entrypoint":{{json .Config.Entrypoint}},"Cmd":{{json .Config.Cmd}}}')
_SERIAL = threading.Lock()


def _shape(value, fields, code):need(type(value) is dict and set(value)==set(fields),code)


def _parent():
    return {'pid':os.getpid(),'start_ticks':processes._identity(os.getpid()),
            'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def _ref(j, ref):
    _shape(ref,('phase','name','sha256'),'initial_epoch_artifact_reference')
    need(all(type(ref[k]) is str and journals.NAME.fullmatch(ref[k]) for k in ('phase','name'))
         and pin(ref['sha256']),'initial_epoch_artifact_name')
    raw=j.read_phase_bytes(ref['phase'],ref['name'],ref['sha256'])
    need(digest(raw)==ref['sha256'],'initial_epoch_raw_artifact_changed');return raw


def _source_seal(graph):
    _shape(graph,SOURCES,'initial_epoch_closed_source_graph')
    base=Path(__file__).resolve().parent
    for name,sha in graph.items():
        need(pin(sha),'initial_epoch_source_pin');private_bytes(base/name,sha)
        loaded=sys.modules.get(name[:-3])
        if loaded is not None:
            need(Path(loaded.__file__).resolve()==base/name,'initial_epoch_loaded_source_mismatch')


def _load_inventory(inventory_ref):
    raw=driver._reference(inventory_ref);cfg=decode(raw);_shape(cfg,BASE_FIELDS,'initial_epoch_inventory')
    need(type(cfg['schema_version']) is int and cfg['schema_version']==1
         and cfg['kind']=='openlegal_initial_epoch_certificate_inventory' and valid_uuid(cfg['operation_id'])
         and valid_uuid(cfg['managed_epoch']) and type(cfg['revision']) is str
         and re.fullmatch('[0-9a-f]{40}',cfg['revision']),'initial_epoch_inventory_identity')
    _source_seal(cfg['source_graph']);q=cfg['epoch_certificate'];_shape(q,INPUT_FIELDS,'initial_epoch_input_section')
    need(valid_uuid(q['certificate_id']) and q['reason']=='initial_bind' and type(q['generation']) is int
         and q['generation']==1 and q['predecessor_certificate_sha256'] is None,'initial_epoch_first_only')
    for key in ('original_inventory','driver_inventory','driver_authority','seed_inventory','seed_authority',
                'pair_inventory','pair_authority','strict_preprovision'):driver._reference(q[key])
    return cfg,raw


_AUTHORITY_TRUE=('epoch_certificate_observation_permitted','epoch_certificate_install_permitted',
    'initial_bind_only','preserve_originals')
_AUTHORITY_FALSE=('automatic_retry','roles_permitted','credentials_permitted','trust_permitted',
    'provider_permitted','kubernetes_mutation_permitted','pg_mutation_permitted','ready_permitted',
    'release_permitted','promotion_permitted','cleanup_permitted')


def _requested_scope(loaded):
    cfg,raw=loaded
    return {'schema_version':1,'kind':'openlegal_initial_epoch_certificate_authority',
        'operation_id':cfg['operation_id'],'inventory_sha256':digest(raw),
        'source_graph_sha256':digest(canonical(cfg['source_graph'])),
        'source_sha256':cfg['source_graph']['initial_epoch_certificate.py'],
        **dict.fromkeys(_AUTHORITY_TRUE,True),**dict.fromkeys(_AUTHORITY_FALSE,False)}


def _load(inventory_ref,authority_ref):
    loaded=_load_inventory(inventory_ref);cfg,raw=loaded;scope=_requested_scope(loaded)
    authraw=driver._reference(authority_ref);auth=decode(authraw)
    _shape(auth,(*scope,'approved','valid_until'),'initial_epoch_authority')
    need(type(auth['schema_version']) is int and auth['approved'] is True
         and type(auth['valid_until']) is int and time.time()<auth['valid_until']
         and {k:auth[k] for k in scope}==scope
         and all(auth[k] is True for k in _AUTHORITY_TRUE)
         and all(auth[k] is False for k in _AUTHORITY_FALSE),'initial_epoch_authority_scope')
    return cfg,auth,raw,authraw


# Approval transport is bounded DATA within the completed Pair phase.
INTAKE_REQUEST_MAX=64*1024
INTAKE_ENVELOPE_MAX=16*1024
INTAKE_CONTROL_MAX=4096
INTAKE_RECORDS=9
INTAKE_BYTES=3*MAX_INPUT+2*INTAKE_REQUEST_MAX+INTAKE_ENVELOPE_MAX+3*INTAKE_CONTROL_MAX
INTAKE_ENVELOPE='initial-certificate-capability.json'
INTAKE_LABEL='initial-certificate-intake'
_ACTIVE_KEYS=frozenset(('workloads','jobs','config_maps','persistent_volumes',
    'serving_image','serving_runtime_image_id','serving_service'))


class _Intake:
    def __init__(self,b,lock):
        self.b=b;self.lock=lock;self.state='PREPARING';self.j=None;self.budget=None
        self.inbox_fd=None;self.finished=None;self.entered_run=False


def _dedicated_root(b,lock):
    need(type(b) is driver.Backend and type(lock) is journals.SharedLock and os.geteuid()==0
         and threading.current_thread() is threading.main_thread()
         and len(list(Path('/proc/self/task').iterdir()))==1 and not processes._children(os.getpid())
         and not any(callable(v) for v in vars(b).values())
         and not getattr(b,'_initial_epoch_certificate_started',False),'initial_epoch_live_dedicated_root')


def _intake_fail(s):
    if type(s) is _Intake:
        s.state='UNKNOWN'
        if s.budget is not None and s.finished is None:
            try:s.j.fail_reservation(s.budget)
            except BaseException:pass


def _intake_context(s):
    need(type(s) is _Intake and getattr(s.b,'_initial_epoch_certificate_intake',None) is s
         and s.state in ('PREPARING','WAITING','ACCEPTING','DISPATCHING'),
         'initial_epoch_intake_live_instance')
    _dedicated_root(s.b,s.lock);driver._check_end(s.b)
    need(_parent()==s.parent and type(s.clock) is driver._Clock and s.clock is s.b._initial_driver_clock
         and s.clock.backend is s.b and s.clock.shared_lock is s.lock and s.clock.lock_fd==s.lock.fd
         and s.b._initial_driver_clock_attempted is True
         and s.original_end==s.b._driver_deadline==s.clock.end,'initial_epoch_intake_original_context')
    need(s.j is s.b._initial_driver_journal and s.j.operation==s.operation_id and s.j.phase_paths
         and s.j.phase_paths[0][0].name==pair_module.PHASE,'initial_epoch_intake_original_phase')
    s.lock.check();driver._check_end(s.b)


def _intake_record(s,suffix,raw,limit):
    _intake_context(s)
    need(type(raw) is bytes and len(raw)<=limit,'initial_epoch_intake_record_bound')
    name='certificate-intake-'+suffix+'.json';sha=s.j.save_bytes(name,raw,reservation=s.budget)
    _intake_context(s)
    ref={'phase':pair_module.PHASE,'name':name,'sha256':sha}
    need(s.j.read_phase_bytes(ref['phase'],name,sha)==raw,'initial_epoch_intake_dual_ack')
    _intake_context(s)
    return {'path':str(s.j.phase_paths[0][0]/name),'sha256':sha},ref


def _intake_inbox(s):
    _intake_context(s)
    recheck_private_inbox(s.inbox['path'],s.inbox['identity'],s.inbox_fd,s.inbox_mount)
    _intake_context(s)


def _intake_read_envelope(s,ref,previous=None):
    _intake_context(s)
    need(driver._clock_reference(ref)[0]==str(Path(s.inbox['path'])/INTAKE_ENVELOPE),
         'initial_epoch_intake_envelope_path')
    _intake_inbox(s)
    raw,witness=read_private_file_at(s.inbox_fd,INTAKE_ENVELOPE,ref['sha256'],
        INTAKE_ENVELOPE_MAX,mount_id=s.inbox_mount,previous=previous)
    _intake_context(s);_intake_inbox(s)
    return raw,witness


def _preprovision_data(data,p):
    _shape(data,('schema_version','kind','operation_id','revision','linux','strict_preprovision','epoch_roots'),
        'initial_epoch_intake_preprovision')
    need(type(data['schema_version']) is int and data['schema_version']==1
         and data['kind']=='openlegal_initial_certificate_preprovision'
         and data['operation_id']==p.cfg['operation_id'] and type(data['revision']) is str
         and re.fullmatch('[0-9a-f]{40}',data['revision']),'initial_epoch_intake_preprovision_identity')
    _shape(data['linux'],_ACTIVE_KEYS,'initial_epoch_intake_mutable_fields')
    _profile(decode(driver._reference(data['strict_preprovision'])))
    rows=data['epoch_roots']
    need(type(rows) is list and len(rows)==2,'initial_epoch_two_control_roots')
    for row in rows:
        _shape(row,('label','path','descriptor','media'),'initial_epoch_control_shape')
        manual._path(row['path'])
        need(type(row['label']) is str and row['label'] in ('hdd','ssd')
             and type(row['descriptor']) is dict and type(row['media']) is dict,'initial_epoch_control_data')
    need({r['label'] for r in rows}=={'hdd','ssd'}
         and {r['path'] for r in rows}=={str(path) for path,_ in p.j.base},
         'initial_epoch_intake_original_control_bases')
    return data


def _candidate_inventory(p,value,data,graph,certificate_id):
    _preprovision_data(data,p)
    cfg=copy.deepcopy(p.work)
    cfg.update(kind='openlegal_initial_epoch_certificate_inventory',operation_id=p.cfg['operation_id'],
        revision=data['revision'],managed_epoch=value['managed_epoch'],source_graph=copy.deepcopy(graph))
    cfg['linux'].update(copy.deepcopy(data['linux']))
    native=getattr(p.b,'_initial_pg_pair_standby_native',None)
    need(type(native) is dict and native.get('container_id')==p.standby
         and type(native.get('observed_mounts')) is list,'initial_epoch_intake_observed_standby_mounts')
    cfg['linux']['postgres']={'database':'openlegal','admin_user':p.role_context['names']['recovery_login'],
        'uid':70,'primary':copy.deepcopy(p.work['linux']['postgres']['primary']),
        'standby':{'container_id':p.standby,'image_id':p.p['image_id'],
            'mounts':copy.deepcopy(native['observed_mounts'])},'binding':copy.deepcopy(value['binding'])}
    cfg['linux']['bound_roots']=sorted([{'role':r['role'],'path':r['host_path'],'uid':r['root_uid'],
        'dev':r['root_dev'],'ino':r['root_ino']} for r in value['blob_roots']],key=lambda r:r['role'])
    cfg['linux']['blob_binding_sha256']=value['binding']['blob_binding_sha256']
    cfg['epoch_certificate']={'certificate_id':certificate_id,'reason':'initial_bind','generation':1,
        'predecessor_certificate_sha256':None,'original_inventory':copy.deepcopy(p.sc[4]['legacy_inventory']),
        'driver_inventory':copy.deepcopy(p.sc[0]['driver_inventory']),
        'driver_authority':copy.deepcopy(p.sc[0]['driver_authority']),
        'seed_inventory':copy.deepcopy(p.cfg['seed_inventory']),'seed_authority':copy.deepcopy(p.cfg['seed_authority']),
        'pair_inventory':copy.deepcopy(p.inventory_ref),'pair_authority':copy.deepcopy(p.authority_ref),
        'pair_complete':pair_module.completed_reference(p.b,p.lock),
        'role_prepare':copy.deepcopy(p.role_context['reference']),'maintenance':copy.deepcopy(p.cfg['maintenance']),
        'strict_preprovision':copy.deepcopy(data['strict_preprovision']),'epoch_roots':copy.deepcopy(data['epoch_roots'])}
    _shape(cfg,BASE_FIELDS,'initial_epoch_inventory');_active(cfg,p,value)
    return cfg


def _intake_seal(s):
    _intake_context(s);s.j.check();_intake_context(s);_intake_inbox(s)
    for ref,raw in s.input_bytes:
        need(driver._reference(ref)==raw,'initial_epoch_intake_input_changed');_intake_context(s)
    loaded=_load_inventory(s.inventory_ref);_intake_context(s)
    need(loaded==s.loaded and _requested_scope(loaded)==s.scope,'initial_epoch_intake_candidate_changed')
    p,value=_consume_pair(s.b,s.lock,loaded[0]);_intake_context(s)
    need(p is s.p and canonical(value)==s.pair_raw,'initial_epoch_intake_pair_changed')
    rebuilt=_candidate_inventory(p,value,decode(s.preprovision_raw),s.cfg['source_graph'],s.certificate_id)
    _intake_context(s)
    need(canonical(rebuilt)==s.loaded[1],'initial_epoch_intake_derived_candidate_changed')
    _active(s.cfg,p,value)
    need(_ref(s.j,s.request_ref)==s.request_raw,'initial_epoch_intake_request_changed');_intake_context(s)
    if hasattr(s,'authority_raw'):
        approved=_load(s.inventory_ref,s.authority_ref);_intake_context(s)
        need(approved==(s.loaded[0],decode(s.authority_raw),s.loaded[1],s.authority_raw),
             'initial_epoch_intake_approval_changed')


def prepare_certificate_request(backend,shared_lock,*,preprovision_ref,source_sha256,inbox):
    """Archive unapproved candidate DATA from this ROOT's genuine Pair owner."""
    _dedicated_root(backend,shared_lock)
    if hasattr(backend,'_initial_epoch_certificate_intake'):
        _intake_fail(backend._initial_epoch_certificate_intake);raise Refused('initial_epoch_intake_no_replay')
    s=_Intake(backend,shared_lock);backend._initial_epoch_certificate_intake=s
    try:
        s.parent=_parent();s.clock=backend._initial_driver_clock;s.original_end=backend._driver_deadline
        s.j=backend._initial_driver_journal
        need(type(s.j) is journals.DualJournal,'initial_epoch_intake_live_journal')
        s.operation_id=s.j.operation;_intake_context(s)
        ref=pair_module.completed_reference(backend,shared_lock);_intake_context(s)
        value=pair_module.consume_completed_live(backend,shared_lock,ref);_intake_context(s)
        p=backend._initial_pg_pair_live;s.p=p;s.pair_raw=canonical(value)
        need(value['operation_id']==s.operation_id,'initial_epoch_intake_same_operation')
        s.budget=s.j.reserve(INTAKE_LABEL,INTAKE_RECORDS,INTAKE_BYTES);_intake_context(s)
        _shape(inbox,('path','identity'),'initial_epoch_intake_inbox')
        _shape(inbox['identity'],('dev','ino'),'initial_epoch_intake_inbox_identity')
        need(type(inbox['path']) is str and Path(inbox['path']).is_absolute()
             and str(Path(inbox['path']))==inbox['path'] and '..' not in Path(inbox['path']).parts
             and all(type(v) is int and v>=0 for v in inbox['identity'].values()),
             'initial_epoch_intake_inbox_identity')
        s.inbox=copy.deepcopy(inbox)
        need(not any(Path(s.inbox['path']).is_relative_to(path) for path,_ in s.j.paths),
             'initial_epoch_intake_inbox_inside_journal')
        journals.directory(s.inbox['path'],s.inbox['identity']);_intake_context(s)
        s.inbox_fd=os.open(s.inbox['path'],os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
        _intake_context(s);s.inbox_mount=private_fd_mount_id(s.inbox_fd);_intake_context(s);_intake_inbox(s)
        need(pin(source_sha256),'initial_epoch_intake_source')
        private_bytes(Path(__file__).resolve(),source_sha256);_intake_context(s)
        driver._clock_reference(preprovision_ref)
        s.preprovision_raw=driver._reference(preprovision_ref);_intake_context(s)
        data=_preprovision_data(decode(s.preprovision_raw),p);_intake_context(s)
        graph=copy.deepcopy(p.sc[4]['source_graph'])
        graph.update({'initial_blob_seed.py':p.sc[0]['source_sha256'],
            'initial_role_prepare.py':p.role_context['producer_source']['sha256'],
            'initial_provider_maintenance.py':p.role_context['maintenance_source']['sha256'],
            'initial_pg_pair.py':p.cfg['source_sha256'],'initial_epoch_certificate.py':source_sha256})
        base=Path(__file__).resolve().parent
        for name in ('epoch_observation.py','epoch_primary_fence.py','epoch_certificate.py','epoch_current.py'):
            graph[name]=digest(private_bytes(base/name));_intake_context(s)
        _source_seal(graph);_intake_context(s)
        s.input_bytes=[(copy.deepcopy(preprovision_ref),s.preprovision_raw)]
        _intake_record(s,'preprovision',s.preprovision_raw,MAX_INPUT)
        s.certificate_id=str(uuid.uuid4())
        cfg=_candidate_inventory(p,value,data,graph,s.certificate_id);_intake_context(s)
        s.inventory_ref,_=_intake_record(s,'candidate',canonical(cfg),MAX_INPUT)
        s.loaded=_load_inventory(s.inventory_ref);_intake_context(s)
        s.cfg=s.loaded[0];s.scope=_requested_scope(s.loaded);s.nonce=str(uuid.uuid4())
        _consume_pair(backend,shared_lock,s.cfg);_intake_context(s)
        request={'schema_version':1,'kind':'openlegal_initial_certificate_stage_request',
            'operation_id':s.operation_id,'stage':'initial_epoch_certificate','stage_nonce':s.nonce,
            'root':s.parent,'original_absolute_deadline':s.original_end,
            'source_profile':'initial_epoch_certificate_intake_v1','source_graph_sha256':digest(canonical(graph)),
            'previous_result':ref,'candidate_inventory':s.inventory_ref,'requested_authority_scope':s.scope,
            'authority_scope_sha256':digest(canonical(s.scope)),'status':'UNAPPROVED_CANDIDATE_DATA','ready':False}
        s.request_raw=canonical(request)
        _,s.request_ref=_intake_record(s,'request',s.request_raw,INTAKE_REQUEST_MAX)
        s.state='WAITING';_intake_seal(s)
        return s,s.request_raw
    except BaseException:
        _intake_fail(s);raise


def execute_approved_certificate(intake,envelope_ref):
    """Consume one external exact authority and dispatch the fixed producer."""
    need(type(intake) is _Intake and getattr(intake.b,'_initial_epoch_certificate_intake',None) is intake
         and intake.state=='WAITING','initial_epoch_intake_one_use')
    s=intake;s.state='ACCEPTING'
    try:
        _intake_seal(s);raw,witness=_intake_read_envelope(s,envelope_ref);value=decode(raw)
        _shape(value,('schema_version','kind','operation_id','stage','stage_nonce','request_sha256',
            'inventory','authority'),'initial_epoch_intake_envelope')
        need(canonical(value)==raw and type(value['schema_version']) is int and value['schema_version']==1
             and value['kind']=='openlegal_initial_stage_capability_envelope'
             and value['operation_id']==s.operation_id and value['stage']=='initial_epoch_certificate'
             and value['stage_nonce']==s.nonce and value['request_sha256']==digest(s.request_raw)
             and value['inventory']==s.inventory_ref,'initial_epoch_intake_envelope_binding')
        s.authority_ref=copy.deepcopy(value['authority']);driver._clock_reference(s.authority_ref)
        s.envelope_ref=copy.deepcopy(envelope_ref);s.envelope_raw=raw;s.envelope_identity=witness
        _intake_record(s,'envelope',raw,INTAKE_ENVELOPE_MAX)
        _intake_record(s,'consumed',canonical({'operation_id':s.operation_id,'stage_nonce':s.nonce,
            'request_sha256':digest(s.request_raw),'envelope_sha256':digest(raw),
            'status':'ATTEMPT_CONSUMED_NOT_APPROVAL'}),INTAKE_CONTROL_MAX)
        loaded=_load(s.inventory_ref,s.authority_ref);_intake_context(s)
        need(loaded[0]==s.cfg and {k:loaded[1][k] for k in s.scope}==s.scope,
             'initial_epoch_intake_approval_scope')
        s.authority_raw=loaded[3]
        _intake_record(s,'authority',s.authority_raw,MAX_INPUT)
        _,s.accepted_ref=_intake_record(s,'accepted',canonical({'operation_id':s.operation_id,
            'stage_nonce':s.nonce,'request':s.request_ref,'inventory':s.inventory_ref,
            'authority':s.authority_ref,'authority_sha256':digest(s.authority_raw),
            'original_absolute_deadline':s.original_end,'ready':False}),INTAKE_REQUEST_MAX)
        _intake_seal(s)
        need(_intake_read_envelope(s,s.envelope_ref,s.envelope_identity)[0]==s.envelope_raw,
             'initial_epoch_intake_approval_changed')
        _intake_context(s);s.finished=s.j.finish_reservation(s.budget)
        _intake_seal(s);s.state='DISPATCHING'
        result=run(s.b,s.lock,s.inventory_ref,s.authority_ref);s.state='CONSUMED'
        return result
    except BaseException:
        _intake_fail(s);raise


def _intake_run_gate(b,lock,inventory_ref,authority_ref):
    if not hasattr(b,'_initial_epoch_certificate_intake'):return
    s=b._initial_epoch_certificate_intake
    try:
        need(type(s) is _Intake and s.b is b and s.lock is lock and s.state=='DISPATCHING'
             and s.finished is not None and not s.entered_run
             and inventory_ref==s.inventory_ref and authority_ref==s.authority_ref,
             'initial_epoch_intake_dispatch_required')
        _intake_seal(s)
        need(_intake_read_envelope(s,s.envelope_ref,s.envelope_identity)[0]==s.envelope_raw,
             'initial_epoch_intake_approval_changed')
        _ref(s.j,s.finished);_intake_context(s);s.entered_run=True
    except BaseException:
        _intake_fail(s);raise


def _ack(j,value,binding,*,first):
    """Use the Pair owner's native historical proof reconstruction."""
    return pair_module._completed_ack(j,value,binding,first=first)


def _consume_pair(backend,lock,cfg):
    p=getattr(backend,'_initial_pg_pair_live',None);q=cfg['epoch_certificate']
    need(type(p) is pair_module._Pair and p.b is backend and p.lock is lock
         and p.j is getattr(backend,'_initial_driver_journal',None)
         and p.parent==os.getpid() and p.ticks==processes._identity(os.getpid())
         and p.j.phase_paths and p.j.phase_paths[0][0].name==pair_module.PHASE
         and getattr(backend,'_initial_pg_pair_bind_state',None)=='ACKNOWLEDGED'
         and not getattr(backend,'_initial_pg_pair_cancelled',False),'initial_epoch_live_pair_required')
    completed=pair_module.consume_completed_live(backend,lock,q['pair_complete'])
    need(q['pair_inventory']==p.inventory_ref and q['pair_authority']==p.authority_ref
         and driver._reference(q['pair_inventory'])==p.raw and driver._reference(q['pair_authority'])==p.authraw
         and q['driver_inventory']==p.sc[0]['driver_inventory'] and q['driver_authority']==p.sc[0]['driver_authority']
         and q['seed_inventory']==p.cfg['seed_inventory'] and q['seed_authority']==p.cfg['seed_authority']
         and q['original_inventory']==p.sc[4]['legacy_inventory']
         and q['role_prepare']==p.role_context['reference'] and q['maintenance']==p.cfg['maintenance'],
         'initial_epoch_pair_input_chain')
    need(q['pair_complete']['phase']==pair_module.PHASE,'initial_epoch_pair_current_complete')
    value=decode(_ref(p.j,q['pair_complete']))
    need(canonical(value)==canonical(completed),'initial_epoch_pair_completed_owner_changed')
    need(value['kind']=='openlegal_initial_pg_pair_result' and value['operation_id']==cfg['operation_id']==p.cfg['operation_id']
         and value['inventory_sha256']==digest(p.raw) and value['authority_sha256']==digest(p.authraw)
         and value['source_sha256']==p.cfg['source_sha256']==cfg['source_graph']['initial_pg_pair.py']
         and value['primary_container_id']==p.primary and value['standby_container_id']==p.standby
         and value['primary_process']==p.native[0]['process']
         and value['standby_process']==backend._initial_pg_pair_standby_native['process']
         and value['blob_roots']==p.seed_result['roots'] and value['managed_epoch']==cfg['managed_epoch']
         and all(value[k] is False for k in ('ready','provider_called','roles_created','released','automatic_retry')),
         'initial_epoch_pair_raw_result')
    _ack(p.j,value['first_bind_ack'],value['binding'],first=True)
    _ack(p.j,value['final_ack'],value['binding'],first=False)
    inputs=decode(_ref(p.j,value['input_binding']))
    need(inputs['inventory']==p.inventory_ref and inputs['authority']==p.authority_ref
         and inputs['original_absolute_deadline']==backend._driver_deadline,'initial_epoch_pair_original_end')
    return p,value


def _active(cfg,p,value):
    """Source-derived whitelist; mutable Kube/image pins are observed separately."""
    mutable={'kind','operation_id','revision','managed_epoch','linux','source_graph','epoch_certificate'}
    need(all(cfg[k]==v for k,v in p.work.items() if k not in mutable),'initial_epoch_original_inventory_changed')
    actual=cfg['linux'];old=p.work['linux']
    changed={'postgres','bound_roots','blob_binding_sha256','workloads','jobs','config_maps',
             'persistent_volumes','serving_image','serving_runtime_image_id','serving_service'}
    need(set(actual)==set(old) and all(actual[k]==v for k,v in old.items() if k not in changed),
         'initial_epoch_active_inventory_delta')
    role=p.role_context['names'];pg=actual['postgres']
    _shape(pg,('database','admin_user','uid','primary','standby','binding'),'initial_epoch_active_postgres')
    need(pg['database']=='openlegal' and type(pg['uid']) is int and pg['uid']==70
         and pg['admin_user']==role['recovery_login'] and pg['binding']==value['binding']
         and pg['primary']==old['postgres']['primary'] and pg['standby']['container_id']==p.standby
         and pg['standby']['image_id']==p.p['image_id']
         and actual['blob_binding_sha256']==value['binding']['blob_binding_sha256'],
         'initial_epoch_active_native_pair')
    _shape(pg['standby'],('container_id','image_id','mounts'),'initial_epoch_active_standby')
    mounts=pg['standby']['mounts'];need(type(mounts) is list,'initial_epoch_standby_mounts')
    need(sorted([r for r in mounts if r['type']=='bind'],key=lambda r:r['destination'])==
         sorted(p.created[p.standby]['mounts'],key=lambda r:r['destination'])
         and all(r['type']=='bind' or r['type']=='tmpfs' and r['destination'] in ('/tmp','/var/run/postgresql')
                 and r['rw'] is True for r in mounts),'initial_epoch_active_standby_mounts')
    roots=sorted([{'role':r['role'],'path':r['host_path'],'uid':r['root_uid'],'dev':r['root_dev'],'ino':r['root_ino']}
                  for r in value['blob_roots']],key=lambda r:r['role'])
    need(sorted(actual['bound_roots'],key=lambda r:r['role'])==roots,'initial_epoch_active_four_roots')
    need({(r['name'],r['uid']) for r in actual['workloads']}=={(r['name'],r['uid']) for r in old['workloads']}
         and {(r['name'],r['uid']) for r in actual['jobs']}=={(r['name'],r['uid']) for r in old['jobs']},
         'initial_epoch_active_writer_identity')
    return roots


class _Producer:
    def __init__(self,b,lock,inventory_ref,authority_ref,loaded,p,value):
        self.b=b;self.lock=lock;self.inventory_ref=inventory_ref;self.authority_ref=authority_ref
        self.cfg,self.auth,self.raw,self.authraw=loaded;self.q=self.cfg['epoch_certificate'];self.p=p;self.pair_value=value
        self.j=p.j;self.parent=_parent();self.original_end=b._driver_deadline;self.original_clock=getattr(b,'_initial_driver_clock',None)
        self.pair_cap=getattr(b,'_initial_pg_pair_complete',None);self.phase=OBSERVATION
        self.serial=0;self.child=False;self.parentfd=None;self.cancelled=False;self.control=[]
        self.start_count=self.j.count;self.start_bytes=self.j.bytes;self.count_end=self.start_count+RECORDS;self.bytes_end=self.start_bytes+BYTES
        need(self.count_end<=4096 and self.bytes_end<=journals.MAX_PHASE_BYTES,'initial_epoch_global_budget')
        # Pair's reserve records the actual M32 SQL metadata size. p.baseline is
        # instead the legacy blob manifest and must never stand in for it.
        baseline=p.budget
        raw_bytes=baseline['baseline_raw_bytes'];bundles=baseline['baseline_bundles']
        need(type(raw_bytes) is int and raw_bytes>=0 and type(bundles) is int and bundles>=1
             and 2*(raw_bytes+4096*bundles)+32*1024**2+COMMAND_HEADROOM<=BYTES,
             'initial_epoch_metadata_estimated_bound')
        self.common=observation.NativeObservation(b);self.role_names=copy.deepcopy(p.role_context['names'])
        self.runtime_role=p.role_context['runtime'];self.input_raw=canonical(self.cfg)
        self.pair_raw=(p.raw,p.authraw,canonical(p.work),canonical(value))
        fds=[lock.fd,*b._initial_pg_pair_retained,*[row[0] for row in b._initial_blob_seed_fds.values()]]
        for name in driver.GUARDS:
            fd=getattr(b,name,None)
            if fd is not None:fds.append(fd)
        self.inherited={fd:(os.fstat(fd).st_dev,os.fstat(fd).st_ino,os.fstat(fd).st_mode) for fd in fds}
        self._set_op()

    def _set_op(self):
        self.op=Operation(self.cfg['operation_id'],'strict_prepare',self.phase,self.cfg,digest(self.raw),
            digest(canonical(self.cfg['source_graph'])),digest(self.authraw),self.j,Path(self.cfg['backup_root']['path']),
            {},self.seal,freeze_authority(self.auth))

    def local(self):
        driver._check_end(self.b)
        need(self.b._driver_deadline==self.original_end
             and getattr(self.b,'_initial_driver_clock',None) is self.original_clock
             and self.pair_cap is not None and getattr(self.b,'_initial_pg_pair_complete',None) is self.pair_cap
             and not self.cancelled
             and not getattr(self.b,'_initial_epoch_certificate_unknown',False),'initial_epoch_original_end_or_unknown')
        need(Path('/proc/sys/kernel/random/boot_id').read_text().strip()==self.parent['boot_id'],'initial_epoch_boot_changed')
        if self.child:
            driver._child_parent(self.parent['pid'],self.parentfd)
            need(processes._identity(self.parent['pid'])==self.parent['start_ticks'],'initial_epoch_parent_reused')
        else:need(_parent()==self.parent,'initial_epoch_root_changed')
        self.lock.check();driver._writer_fds(self.b);seed._bound(self.b)
        for fd,identity in self.inherited.items():
            s=os.fstat(fd);need((s.st_dev,s.st_ino,s.st_mode)==identity,'initial_epoch_inherited_fd_changed')
        need(not select.select([self.p.primary_pidfd,self.b._initial_pg_pair_standby_native['pidfd']],[],[],0)[0],
             'initial_epoch_pair_native_exited')
        for row in self.control:certificates._root(row['fd'],row['descriptor'],self.original_end)

    def seal(self):
        self.local()
        need(driver._reference(self.inventory_ref)==self.raw and driver._reference(self.authority_ref)==self.authraw
             and canonical(self.cfg)==self.input_raw and time.time()<self.auth['valid_until'], 'initial_epoch_sealed_input_changed')
        _source_seal(self.cfg['source_graph'])
        q=self.q
        need(driver._reference(q['pair_inventory'])==self.pair_raw[0]
             and driver._reference(q['pair_authority'])==self.pair_raw[1]
             and canonical(self.p.work)==self.pair_raw[2] and canonical(self.pair_value)==self.pair_raw[3],
             'initial_epoch_pair_context_changed')
        for key in ('original_inventory','driver_inventory','driver_authority','seed_inventory','seed_authority','strict_preprovision'):
            driver._reference(q[key])
        if hasattr(self,'install_scope'):
            need(canonical({'certificate_sha256':digest(self.certificate_raw),'certificate':self.certificate_ref,
                'intent':self.install_intent,'roots':[{'path':r['path'],'descriptor':r['descriptor']} for r in self.control]})
                ==self.install_scope,'initial_epoch_install_scope_changed')
            need(_ref(self.j,self.certificate_ref)==self.certificate_raw,'initial_epoch_certificate_bytes_changed')
            _ref(self.j,self.install_intent)
        driver._docker_binding(self.b,self.cfg['operation_id']);self.j.check()
        need(self.j.phase_paths and self.j.phase_paths[0][0].name==self.phase,'initial_epoch_current_phase_changed')
        need(self.j.count+1<=self.count_end and self.j.bytes+COMMAND_HEADROOM<=self.bytes_end,
             'initial_epoch_reserved_budget_exhausted')
        driver._check_end(self.b)

    def save(self,label,value):
        self.seal();self.serial+=1;name=self.phase+'-epoch-%04d-'%self.serial+label+'.json';raw=canonical(value)
        need(self.j.count+1<=self.count_end and self.j.bytes+len(raw)<=self.bytes_end,'initial_epoch_prewrite_budget')
        sha=self.j.save_bytes(name,raw);need(self.j.read_phase_bytes(self.phase,name,sha)==raw,'initial_epoch_dual_ack')
        self.seal();return {'phase':self.phase,'name':name,'sha256':sha}

    def sql(self,kind):
        need(kind=='migrations','initial_epoch_fixed_metadata_sql')
        pg=self.cfg['linux']['postgres'];self.seal()
        args=['exec','-i','-u','70',pg['primary']['container_id'],'/usr/bin/env','-i',
            'PATH=/usr/local/bin:/usr/bin:/bin','HOME=/nonexistent','LANG=C','LC_ALL=C','PGPASSFILE=/dev/null',
            '/usr/local/bin/psql','-X','-w','-h','/var/run/postgresql','-p','5432','-qAt','-v','ON_ERROR_STOP=1',
            '-U',self.role_names['recovery_login'],'-d','openlegal']
        script=("BEGIN READ ONLY; SET LOCAL statement_timeout='2s'; SET LOCAL lock_timeout='1s'; "
            "SET LOCAL transaction_timeout='5s'; SET LOCAL search_path=pg_catalog; "+migration.MIGRATIONS_SQL+'; ROLLBACK;\n').encode()
        raw=self.b._run(self.op,'docker',args,stdin=script,timeout=3,deadline=self.original_end)
        name=self.phase+'-command-%04d.json'%self.b._sequence;sha=self.j.output_hashes()[name]
        row=decode(self.j.read_phase_bytes(self.phase,name,sha));fences._command_ok(row)
        dispatch=['--config',str(self.b._docker_config[0]),'--host','unix:///var/run/docker.sock']
        need(row['arguments_sha256']==digest(canonical(dispatch+args)) and row['stdin_sha256']==digest(script)
             and row['stdout_hex']==raw.hex() and row['stdout_sha256']==digest(raw),'initial_epoch_migration_native_receipt')
        value=decode(raw)
        expected=[dict(r,success=True) for r in migration._manifest(self.p.work['initial_migration'])]
        need(value==expected,'initial_epoch_actual_migration_manifest')
        return self.save('migrations',{'rows':value,'actual_command':{'phase':self.phase,'name':name,'sha256':sha}})

    def physical(self):
        self.seal();cfg=self.cfg['linux'];nonces={(r['kind'],r['name']):r['fence_nonce'] for r in self.p.old['payloads']['drain']['resources']}
        for row in cfg['workloads']:
            item=self.common._kube(self.op,'deployment',row['name'],self.original_end)
            need(item['metadata']['uid']==row['uid'] and item['spec'].get('replicas')==0
                 and digest(canonical(item['spec']['template']['spec']))==row['pod_spec_sha256'],
                 'initial_epoch_writer_reopened')
            need(item['metadata'].get('annotations',{}).get('openlegal4everyone.stream/operation-fence')==
                 nonces[('deployment',row['name'])],'initial_epoch_writer_fence_changed')
        for row in cfg['jobs']:
            item=self.common._kube(self.op,'job',row['name'],self.original_end)
            terminal=any(r.get('type') in ('Complete','Failed') and r.get('status')=='True' for r in item.get('status',{}).get('conditions',[]))
            need(item['metadata']['uid']==row['uid'] and digest(canonical(item['spec']))==row['spec_sha256']
                 and (terminal or item['spec'].get('suspend') is True),'initial_epoch_job_reopened')
            if not terminal:need(item['metadata'].get('annotations',{}).get('openlegal4everyone.stream/operation-fence')==
                nonces[('job',row['name'])],'initial_epoch_job_fence_changed')
        closed=self.b._physical_writers_closed(self.op)
        return self.save('physical-fence',closed)

    def roots(self):
        self.seal();sc=self.p.sc[0];seed._bound(self.b)
        # These source helpers validate actual WWIDs/kernel mounts and all full
        # generations, including unreferenced objects. They perform no copy.
        seed._qualify_media(self.b,self.op,sc,empty=False)
        references=[]
        for ref in self.p.seed_result['reference_pages']:
            raw=_ref(self.j,{'phase':seed.PHASE,'name':ref['name'],'sha256':ref['sha256']});page=decode(raw)
            need(page['offset']==len(references),'initial_epoch_reference_order');references.extend(page['rows'])
        rows={};audit=[]
        for r in self.p.seed_result['roots']:
            fd=self.b._initial_blob_seed_fds['root-'+r['role']][0]
            seed._metadata_tree(self.b,r['host_path'],new=True)
            actual=blobs.blob_inventory(r['host_path'],deadline=self.original_end,controls=True,online=False)
            need(actual['permanent_sha256']==r['manifest_sha256'] and not actual['health'], 'initial_epoch_generations_changed')
            manual._reference_fd(fd,[v for v in references if v['role']==r['role'].split('_')[0]],self.original_end)
            blob=dict(observation._descriptor(r['host_path'],fd),container_path=r['container_path'],
                filesystem_id=r['filesystem_id'],marker_sha256=r['marker_sha256'])
            observation._verify_marker(blob,r['role'],self.cfg['managed_epoch']);rows[r['role']]=blob
            audit.append({'role':r['role'],'permanent_sha256':actual['permanent_sha256'],
                          'generations':len(actual['permanent']),'references':sum(v['role']==r['role'].split('_')[0] for v in references)})
        self.references=references
        return rows,self.save('four-root-audit',{'roots':rows,'audit':audit,'references_sha256':digest(canonical(references))})

    def draft(self,roots):
        value=self.pair_value;pg=self.cfg['linux']['postgres'];native=self.p.native[0]
        first={'container_id':pg['primary']['container_id'],'image_id':pg['primary']['image_id'],
            'postmaster_started_at':value['final_ack']['after']['postmaster_started_at'],
            'data_directory':self.p.work['legacy_prepare']['pgdata']['container_path'],
            'data_root':observation._descriptor(self.p.work['legacy_prepare']['pgdata']['host_path'],native['data_fd']),
            'process':value['primary_process']}
        second={'container_id':pg['standby']['container_id'],'image_id':pg['standby']['image_id'],
            'postmaster_started_at':None,'data_directory':pair_module.PGDATA,
            'data_root':observation._descriptor(str(Path(self.p.p['target']['parent'])/'data'),self.p.data_fd),
            'process':value['standby_process']}
        return {'managed_epoch':self.cfg['managed_epoch'],'host_boot_id':self.cfg['host']['boot_id'],
            'runtime':None,'control':copy.deepcopy(value['binding']),'primary':first,'standby':second,
            'blob_roots':roots,'original_inventory':self.q['original_inventory'],'topology':observation.TOPOLOGY,
            'ssd_survivors':list(observation.SSD_SURVIVORS),'hdd_lost':list(observation.HDD_LOST)}

    def fence(self,subject):
        deps={'sources':{name:{'path':str(Path(__file__).resolve().parent/name),'sha256':self.cfg['source_graph'][name]}
                         for name in fences.SOURCES},'inventory':self.inventory_ref,'authority':self.authority_ref}
        prepared=fences.prepare_primary_fence(self.b,self.op,self.common,subject,subject['control'],
            self.role_names,self.runtime_role,self.original_end,deps)
        try:window=fences.capture_primary_fence(prepared)
        except fences.FenceUnknown as error:
            # Archive only genuine captured failure; never manufacture success.
            fences.archive_primary_fence(self.op,error.window);raise
        ref=fences.archive_primary_fence(self.op,window)
        return window.proof,ref


def _image(reference,kind):
    need(type(reference) is str and re.fullmatch('ghcr.io/publicdata-stream/openlegal-'+kind+'@sha256:[0-9a-f]{64}',reference)
         and not reference.endswith('0'*64),'initial_epoch_immutable_image')


def _strict_toml(raw,subject,images,*,ingestion):
    need(type(raw) is bytes and len(raw)<=PROFILE_LIMIT,'initial_epoch_toml_bound')
    try:value=tomllib.loads(raw.decode('utf8'))
    except (tomllib.TOMLDecodeError,UnicodeError):raise Refused('initial_epoch_toml_parse') from None
    cache=value.get('cache');db=value.get('database')
    need(type(cache) is dict and type(db) is dict and cache.get('mode')=='persistent','initial_epoch_persistent_config')
    pg=cache.get('postgres');blob=cache.get('blob');roots=subject['blob_roots']
    need(type(pg) is dict and type(blob) is dict and pg.get('replication_mode')=='required'
         and pg.get('replication_epoch')==subject['managed_epoch'] and pg.get('tls_mode')=='verify-full'
         and pg.get('ca_file')=='/run/secrets/postgres-ca/ca.crt'
         and pg.get('url_env')=='OPENLEGAL_DATABASE_URL','initial_epoch_strict_postgres_config')
    need(blob=={'kind':'filesystem_mirror','path':roots['cache_primary']['container_path'],
                'replica_path':roots['cache_replica']['container_path']}
         and db.get('blob_path')==roots['corpus_primary']['container_path']
         and db.get('blob_replica_path')==roots['corpus_replica']['container_path']
         and db.get('storage_failure_policy')=='recover','initial_epoch_strict_four_root_config')
    paths=[Path(roots[r]['container_path']) for r in certificates.ROLES]
    need(all(a!=b and not a.is_relative_to(b) and not b.is_relative_to(a)
             for n,a in enumerate(paths) for b in paths[n+1:]),'initial_epoch_root_path_alias')
    if ingestion:
        i=db.get('ingestion');need(type(i) is dict and i.get('collection_job_template_path')=='/etc/openlegal/collection-job.json'
            and i.get('worker_image')==images['document_worker'],'initial_epoch_ingestion_template_config')
    return value


def _profile(profile):
    _shape(profile,('schema_version','kind','serving','ingestion','images'),'initial_epoch_strict_profile')
    need(type(profile['schema_version']) is int and profile['schema_version']==1
         and profile['kind']=='openlegal_initial_epoch_strict_configmap_pvc_v1','initial_epoch_strict_profile_version')
    _shape(profile['serving'],('deployment_name','configmap_name','configmap_uid','toml_key','toml_source'),
           'initial_epoch_serving_profile')
    _shape(profile['ingestion'],('deployment_name','configmap_name','configmap_uid','toml_key','toml_source',
           'collection_template_key','collection_template_source'),'initial_epoch_ingestion_profile')
    _shape(profile['images'],('serving','ingestion','document_worker'),'initial_epoch_profile_images')
    for key,kind in (('serving','server'),('ingestion','server-ingestion'),('document_worker','document-worker')):
        _image(profile['images'][key],kind)
    for key in ('serving','ingestion'):
        row=profile[key]
        for name in ('deployment_name','configmap_name'):linux.identifier(row[name],linux.KUBE_NAME)
        need(valid_uuid(row['configmap_uid']) and row['toml_key']=='server.toml','initial_epoch_profile_configmap')
        driver._reference(row['toml_source'])
    need(profile['ingestion']['collection_template_key']=='collection-job.json','initial_epoch_profile_job_key')
    driver._reference(profile['ingestion']['collection_template_source']);return profile


def _pod_bindings(m,pod,subject,cm,*,kind,image_data):
    """Exact-root PVC-only subset. No parent mount/suffix or hostPath fallback."""
    containers=pod.get('containers');need(type(containers) is list and len(containers)==1
        and not pod.get('initContainers') and not pod.get('ephemeralContainers'),'initial_epoch_single_main_container')
    app=containers[0];images=m.profile['images'];wanted=images['serving' if kind=='serving' else 'ingestion']
    need(app.get('image')==wanted and app.get('command') in (None,[],['/usr/local/bin/openlegal-server']),
         'initial_epoch_template_image_command')
    args=app.get('args')
    if kind=='serving':
        need(args==['/etc/openlegal/server.toml'] or args in (None,[]) and
            image_data['Entrypoint']==['/usr/local/bin/openlegal-server'] and image_data['Cmd']==['/etc/openlegal/server.toml'],
            'initial_epoch_serving_config_argument')
    else:
        expected=['--collection-scheduler','/etc/openlegal/server.toml'] if kind=='scheduler' else [
            '--collection-job','00000000-0000-0000-0000-000000000000','/etc/openlegal/server.toml']
        need(args==expected,'initial_epoch_ingestion_config_argument')
    volumes=pod.get('volumes',[]);mounts=app.get('volumeMounts',[])
    need(type(volumes) is list and type(mounts) is list and not any('hostPath' in v for v in volumes)
         and len({v['name'] for v in volumes})==len(volumes),
         'initial_epoch_unique_volumes')
    # A full ConfigMap hash does not identify what the process sees if a
    # second mount shadows its directory or a selected child file. Validate
    # lexical container paths without resolving them against the ROOT host.
    paths=[]
    for row in mounts:
        need(type(row) is dict and type(row.get('mountPath')) is str,'initial_epoch_mount_path')
        raw=row['mountPath'];path=Path(raw)
        need(0<len(raw)<=4096 and '\x00' not in raw and path.anchor=='/'
             and str(path)==raw and '..' not in path.parts,'initial_epoch_canonical_mount_path')
        paths.append(path)
    need(len(set(paths))==len(paths),'initial_epoch_duplicate_mount_path')
    for protected in (Path('/etc/openlegal'),Path('/run/secrets/postgres-ca')):
        overlaps=[path for path in paths if path.is_relative_to(protected) or protected.is_relative_to(path)]
        need(overlaps==[protected],'initial_epoch_protected_mount_shadow')
    byname={v['name']:v for v in volumes}
    config=[v for v in mounts if v['mountPath']=='/etc/openlegal']
    need(len(config)==1 and config[0].get('readOnly') is True and not config[0].get('subPath')
         and not config[0].get('subPathExpr'),'initial_epoch_config_mount')
    volume=byname[config[0]['name']]
    need(set(volume)=={'name','configMap'} and volume['configMap']['name']==cm['metadata']['name']
         and volume['configMap'].get('defaultMode',0o644)==0o444,'initial_epoch_static_configmap_volume')
    items=volume['configMap'].get('items')
    if items is not None:
        required=['server.toml']+(['collection-job.json'] if kind=='scheduler' else [])
        need(type(items) is list and len(items)==len(required) and {r.get('path') for r in items}==set(required)
             and all(set(r) in ({'key','path'},{'key','path','mode'}) and r.get('key')==r.get('path')
                 and r.get('mode',0o444)==0o444 for r in items),'initial_epoch_configmap_items')
    need(all(not r.get('subPathExpr') for r in mounts),'initial_epoch_dynamic_mount')
    ca=[r for r in mounts if r['mountPath']=='/run/secrets/postgres-ca']
    need(len(ca)==1 and ca[0].get('readOnly') is True and not ca[0].get('subPath')
         and 'secret' in byname[ca[0]['name']],'initial_epoch_ca_mount_reference')
    env=[r for r in app.get('env',[]) if r.get('name')=='OPENLEGAL_DATABASE_URL']
    need(len(env)==1 and set(env[0])=={'name','valueFrom'} and set(env[0]['valueFrom'])=={'secretKeyRef'}
         and type(env[0]['valueFrom']['secretKeyRef'].get('name')) is str
         and env[0]['valueFrom']['secretKeyRef'].get('key')=='OPENLEGAL_DATABASE_URL',
         'initial_epoch_database_env_reference')
    mappings=[]
    for role,root in subject['blob_roots'].items():
        chosen=[r for r in mounts if r['mountPath']==root['container_path']]
        need(len(chosen)==1 and not chosen[0].get('readOnly',False)
             and not chosen[0].get('subPath') and not chosen[0].get('subPathExpr'),'initial_epoch_exact_root_mount')
        path=Path(root['container_path'])
        need(all(r is chosen[0] or not(Path(r['mountPath']).is_relative_to(path) or path.is_relative_to(r['mountPath']))
             for r in mounts),'initial_epoch_conflicting_root_mount')
        v=byname[chosen[0]['name']]
        need(set(v)=={'name','persistentVolumeClaim'} and not v['persistentVolumeClaim'].get('readOnly',False),
             'initial_epoch_pvc_only_root')
        claim=m.common._kube(m.op,'persistentvolumeclaim',v['persistentVolumeClaim']['claimName'],m.original_end)
        pv=m.common._kube(m.op,'persistentvolume',claim['spec']['volumeName'],m.original_end,False)
        expected=next((r for r in m.cfg['linux']['persistent_volumes'] if r['name']==pv['metadata']['name']),None)
        need(expected is not None and pv['metadata']['uid']==expected['uid']
             and pv['spec'].get('local',{}).get('path')==expected['path']==root['path'],'initial_epoch_pv_exact_root')
        need(claim['spec'].get('volumeName')==pv['metadata']['name']
             and pv['spec'].get('claimRef',{}).get('uid')==claim['metadata']['uid'],'initial_epoch_pvc_claim_binding')
        mappings.append({'role':role,'pvc_uid':claim['metadata']['uid'],'pv_uid':pv['metadata']['uid'],
            'pv_spec_sha256':digest(canonical(pv['spec'])),'host_root':root['path'],'container_root':root['container_path']})
    return mappings


def _strict_configuration(m,subject):
    m.seal();profile=_profile(decode(driver._reference(m.q['strict_preprovision'])));m.profile=profile
    need(profile['serving']['deployment_name']==m.cfg['linux']['serving_workload']
         and {profile[k]['deployment_name'] for k in ('serving','ingestion')}==
             {r['name'] for r in m.cfg['linux']['workloads']}
         and profile['images']['serving']==m.cfg['linux']['serving_image'],'initial_epoch_profile_scope')
    image={}
    for key in ('serving','ingestion'):
        value=decode(m.common._run(m.op,'docker',['image','inspect','--format',IMAGE_FORMAT,profile['images'][key]],m.original_end))
        need(value['Revision']==m.cfg['revision'] and value['Entrypoint']==['/usr/local/bin/openlegal-server'],
             'initial_epoch_native_image_entrypoint')
        if key=='serving':need(value['Id']==m.cfg['linux']['serving_runtime_image_id'],'initial_epoch_native_serving_image')
        image[key]=value
    proofs=[];cms={};tomls={}
    for key in ('serving','ingestion'):
        row=profile[key];cm=m.common._kube(m.op,'configmap',row['configmap_name'],m.original_end);cms[key]=cm
        raw=private_bytes(row['toml_source']['path'],row['toml_source']['sha256'],limit=PROFILE_LIMIT)
        cm_pin=next((v for v in m.cfg['linux']['config_maps'] if v['name']==row['configmap_name']),None)
        need(cm_pin is not None and cm_pin['uid']==cm['metadata']['uid'] and cm_pin['data_sha256']==digest(canonical(cm.get('data',{})))
             and not cm.get('binaryData') and cm['metadata']['uid']==row['configmap_uid']
             and cm.get('data',{}).get(row['toml_key'],'').encode()==raw,
             'initial_epoch_actual_toml_source')
        tomls[key]=_strict_toml(raw,subject,profile['images'],ingestion=key=='ingestion')
        pod=m.common._kube(m.op,'deployment',row['deployment_name'],m.original_end)['spec']['template']['spec']
        mapping=_pod_bindings(m,pod,subject,cm,kind='serving' if key=='serving' else 'scheduler',image_data=image[key])
        proofs.append({'profile':key,'toml_sha256':digest(raw),'mapping':mapping})
    row=profile['ingestion'];raw=private_bytes(row['collection_template_source']['path'],
        row['collection_template_source']['sha256'],limit=PROFILE_LIMIT)
    need(cms['ingestion'].get('data',{}).get('collection-job.json','').encode()==raw,'initial_epoch_actual_collection_template')
    template=decode(raw)
    need(template.get('apiVersion')=='batch/v1' and template.get('kind')=='Job'
         and template.get('metadata',{}).get('namespace')==m.cfg['linux']['namespace'],'initial_epoch_collection_template_shape')
    mapping=_pod_bindings(m,template['spec']['template']['spec'],subject,cms['ingestion'],kind='collection',image_data=image['ingestion'])
    observed=m.common._configuration_projection(m.op,subject,m.original_end)
    need(observed['projection']['runtime']['revision']==m.cfg['revision'],'initial_epoch_current_runtime_revision')
    ref=m.save('strict-configuration',{'source_profile':m.q['strict_preprovision'],'profiles':proofs,
        'collection_template_sha256':digest(raw),'collection_mapping':mapping,'projection':observed['projection'],
        'configuration_sha256':observed['configuration_sha256'],'runtime_authenticated_or_ready':False})
    return observed,ref


def _controls(m):
    rows=m.q['epoch_roots'];need(type(rows) is list and len(rows)==2,'initial_epoch_two_control_roots')
    need({r['label'] for r in rows}=={'hdd','ssd'},'initial_epoch_control_domains')
    need({r['path'] for r in rows}=={str(path) for path,_ in m.j.base},'initial_epoch_control_root_base_only')
    domains,hardware=blobs._media_hardware(m.b,m.op);proof=[]
    for row in rows:
        _shape(row,('label','path','descriptor','media'),'initial_epoch_control_shape')
        manual._path(row['path']);fd=root_fd(row['path'])
        # Immediately retain the supplied capability even when qualification fails.
        m.b._initial_epoch_certificate_fds.append(fd)
        certificates._root(fd,row['descriptor'],m.original_end)
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        mount=blobs._path_mount(fd,row['path'],blobs._mounts());blobs._no_nested_mounts(row['path'],blobs._mounts())
        media=row['media']
        if row['label']=='hdd':
            _shape(media,('kind','dataset','guid','mountpoint'),'initial_epoch_hdd_control')
            need(media['kind']=='hdd' and type(media['dataset']) is str and
                 re.fullmatch('[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+',media['dataset'])
                 and '..' not in Path(media['dataset']).parts,'initial_epoch_hdd_control_dataset')
            pools={r['pool'] for r in blobs.all_datasets(m.cfg)}
            need(len(pools)==1 and media['dataset'].split('/')[0] in pools,'initial_epoch_hdd_control_pool')
            raw=m.b._run(m.op,'zfs',['get','-Hp','-o','property,value','guid,mountpoint,mounted',media['dataset']],deadline=m.original_end)
            entries=[line.split('\t') for line in raw.decode('ascii').splitlines()]
            need(len(entries)==3 and all(len(r)==2 for r in entries) and len({r[0] for r in entries})==3,
                 'initial_epoch_hdd_control_output')
            values=dict(entries)
            need(values=={'guid':media['guid'],'mountpoint':media['mountpoint'],'mounted':'yes'}
                 and mount['filesystem']=='zfs' and mount['source']==media['dataset']
                 and mount['mountpoint']==media['mountpoint'] and mount['root']=='/','initial_epoch_hdd_control_mount')
            leaves=set(domains['hdd'])
        else:
            _shape(media,('kind','physical_leaves'),'initial_epoch_ssd_control')
            leaves=m.b._device_leaves(os.fstat(fd).st_dev)
            need(media['kind']=='ssd' and leaves==set(media['physical_leaves'])
                 and leaves<=set(domains['ssd']) and not leaves&set(domains['hdd']),'initial_epoch_ssd_control_mount')
        need(not any(name==certificates.HEAD_NAME or name.startswith(('certificate-','.pending-head-','.install-intent-'))
                     for name in os.listdir(fd)),'initial_epoch_existing_control_state')
        certificates._root(fd,row['descriptor'],m.original_end)
        need(blobs._path_mount(fd,row['path'],blobs._mounts())==mount,'initial_epoch_control_mount_changed')
        blobs._no_nested_mounts(row['path'],blobs._mounts())
        selected={'fd':fd,'path':row['path'],'descriptor':copy.deepcopy(row['descriptor'])}
        m.control.append(selected)
        proof.append({'label':row['label'],'path':row['path'],'descriptor':row['descriptor'],'mount':mount,
                      'hardware':{path:hardware[path] for path in sorted(leaves)}})
    need(m.control[0]['descriptor']['dev']!=m.control[1]['descriptor']['dev'],'initial_epoch_control_same_filesystem')
    m.save('qualified-control-roots',proof)


def _position(value,subject):
    fields=('schema_version','kind','in_recovery','system_identifier','timeline','postmaster_started_at','data_directory',
            'control','receive_lsn','replay_lsn','replay_paused','wal_receiver_count')
    _shape(value,fields,'initial_epoch_standby_position')
    need(type(value['schema_version']) is int and value['schema_version']==1
         and value['kind']=='openlegal_survivor_position' and value['in_recovery'] is True
         and value['system_identifier']==subject['control']['primary_system_identifier']
         and value['timeline']==subject['control']['timeline'] and value['data_directory']==subject['standby']['data_directory']
         and pair_module._normalized_binding(value['control'])==subject['control']
         and value['replay_paused'] is False and type(value['wal_receiver_count']) is int
         and 0<=value['wal_receiver_count']<=1,'initial_epoch_standby_lineage')
    need(type(value['postmaster_started_at']) is str and value['postmaster_started_at'], 'initial_epoch_standby_postmaster')
    if subject['standby']['postmaster_started_at'] is not None:
        need(value['postmaster_started_at']==subject['standby']['postmaster_started_at'],'initial_epoch_standby_restarted')
    pair_module._lsn(value['receive_lsn']);pair_module._lsn(value['replay_lsn']);return value


def _closures(m,subject):
    refs=[]
    for side in ('primary','standby'):
        result=m.common._query_metadata(m.op,subject,'sql-closure',m.original_end,which=side)
        need(result['result']=={'sessions':[]},'initial_epoch_sql_sessions_remain')
        refs.append(m.save('sql-closure-'+side,result))
    return m.save('both-sql-closed',{'observations':refs,'sessions':[]})


def _snapshot(value,subject,target,references):
    _shape(value,('position_before','position_after','transaction','retained_tables',
        'retained_references','provider','storage_metadata'),'initial_epoch_snapshot_shape')
    for key in ('position_before','position_after'):
        _position(value[key],subject)
        need(pair_module._lsn(value[key]['replay_lsn'])>=pair_module._lsn(target),
             'initial_epoch_snapshot_before_fixed_target')
    need(value['transaction']=={'isolation':'repeatable read','read_only':True,'statement_timeout_ms':30000,
        'lock_timeout_ms':1000,'transaction_timeout_ms':35000,'idle_in_transaction_session_timeout_ms':5000},
        'initial_epoch_snapshot_transaction')
    manual._table_proof(value['retained_tables']);manual._references(value['retained_references'])
    need(value['retained_references']==references,'initial_epoch_snapshot_reference_changed')
    storage=value['storage_metadata'];_shape(storage,('corpus_control','cache_storage','cache_totals'),
        'initial_epoch_storage_metadata')
    corpus=storage['corpus_control'];cache=storage['cache_storage'];totals=storage['cache_totals']
    need(type(corpus) is dict and type(corpus.get('next_event')) is int and corpus['next_event']>=1
         and type(corpus.get('index_ack')) is int and 0<=corpus['index_ack']<corpus['next_event']
         and type(cache) is dict and valid_uuid(cache.get('id')),'initial_epoch_storage_identity')
    _shape(totals,('snapshots','queries','referenced_bytes'),'initial_epoch_cache_totals')
    need(all(type(v) is int and v>=0 for v in totals.values()),'initial_epoch_cache_totals')
    provider=value['provider'];_shape(provider,('state','hold_control','lock_provenance'),'initial_epoch_provider_shape')
    state=provider['state'];control=provider['hold_control']
    _shape(state,('budget','control','admissions','canary_control','canaries'),'initial_epoch_provider_state')
    need(provider['lock_provenance']=='primary_advisory_locks_not_observable_on_standby'
         and type(state['budget']) is dict and state['budget'].get('operator_suspended') is True
         and type(state['budget'].get('unresolved_response')) is bool
         and (state['budget'].get('admission_owner') is None or valid_uuid(state['budget']['admission_owner']))
         and type(state['admissions']) is list and len(state['admissions'])<=16
         and type(state['canaries']) is list and len(state['canaries'])<=1
         and type(control) is dict and valid_uuid(control.get('generation')) and control.get('recovery_hold') is True
         and state['control']=={k:v for k,v in control.items() if k not in ('generation','recovery_hold','hold_started_at')},
         'initial_epoch_provider_hold_or_metadata_changed')
    migration_digest=value['retained_tables']['public._sqlx_migrations']['sha256']
    need(pin(migration_digest),'initial_epoch_actual_migration_digest');return migration_digest


def _observe(m):
    physical=m.physical();roots,rootref=m.roots();subject=m.draft(roots)
    m.common._native(m.op,subject,m.original_end,which='primary');m.common._native(m.op,subject,m.original_end)
    pos=m.common._query_metadata(m.op,subject,'position',m.original_end)['result'];_position(pos,subject)
    subject['standby']['postmaster_started_at']=pos['postmaster_started_at']
    configuration,configref=_strict_configuration(m,subject)
    initial,f0ref=m.fence(subject);target=initial['fixed_target'];end=min(m.original_end,time.monotonic()+30);polls=0
    while True:
        need(time.monotonic()<end and polls<=16,'initial_epoch_standby_fixed_replay_deadline')
        pos=m.common._query_metadata(m.op,subject,'position',end)['result'];_position(pos,subject)
        if pair_module._lsn(pos['replay_lsn'])>=pair_module._lsn(target):break
        polls+=1;time.sleep(.05)
    snapshot=m.common._query_metadata(m.op,subject,'snapshot',m.original_end)
    value=snapshot['result'];migration_digest=_snapshot(value,subject,target,m.references)
    migrations=m.sql('migrations');snapshotref=m.save('standby-snapshot',dict(snapshot,fixed_primary_target=target,initial_primary_fence=f0ref))
    runtime=configuration['projection']['runtime']
    subject['runtime']={'revision':runtime['revision'],'image_id':runtime['docker_image_id'],
        'configuration_sha256':configuration['configuration_sha256'],'migration_table_sha256':migration_digest}
    certificates._subject(subject)
    final,configref=_strict_configuration(m,subject)
    need(final['configuration_sha256']==configuration['configuration_sha256'],'initial_epoch_configuration_changed')
    m.common._native(m.op,subject,m.original_end,which='primary');m.common._native(m.op,subject,m.original_end)
    physical=m.physical();_,lastref=m.fence(subject);closed=_closures(m,subject)
    return m.save('observation-pending-parent-exit',{'schema_version':1,
        'kind':'openlegal_initial_epoch_observation_pending_parent_exit','operation_id':m.cfg['operation_id'],
        'inventory_sha256':digest(m.raw),'authority_sha256':digest(m.authraw),'source_graph_sha256':m.op.source_graph_sha256,
        'subject':subject,'configuration':configref,'standby_snapshot':snapshotref,'migrations':migrations,
        'four_root_audit':rootref,'physical_fence':physical,'primary_fence':lastref,'sql_closure':closed})


def _install(m):
    need(hasattr(m,'install_scope'),'initial_epoch_install_intent_required')
    m.seal();intent=decode(_ref(m.j,m.install_intent))
    need(intent['kind']=='openlegal_initial_epoch_dual_install_intent'
         and intent['operation_id']==m.cfg['operation_id'] and intent['certificate']==m.certificate_ref
         and intent['certificate_sha256']==digest(m.certificate_raw)
         and intent['head_sha256']==digest(certificates.make_head(m.certificate_raw))
         and intent['roots']==[{'path':r['path'],'descriptor':r['descriptor'],'expected_head':None} for r in m.control]
         and intent['automatic_retry'] is False,'initial_epoch_install_intent_binding')
    certificates.validate_transition(m.certificate_raw,None)
    cert=m.certificate_raw;head=certificates.make_head(cert);facts=[]
    for row in m.control:
        m.seal();facts.append(certificates.install_one(row['fd'],row['descriptor'],cert,None,m.original_end));m.seal()
    return m.save('installation-pending-parent-exit',{'schema_version':1,
        'kind':'openlegal_initial_epoch_install_pending_parent_exit','operation_id':m.cfg['operation_id'],
        'certificate_sha256':digest(cert),'head_sha256':digest(head),'intent':m.install_intent,'installations':facts})


def _index(m,result):
    return {'phase':m.phase,'operation_id':m.cfg['operation_id'],'serial':m.serial,'sequence':m.b._sequence,
        'backend_artifacts':dict(m.b._artifacts),'artifacts':[{'name':name,'sha256':row['sha256'],'bytes':row['bytes']}
            for name,row in sorted(m.j.artifacts.items())],'result':result}


def _reconcile(m,index):
    _shape(index,('phase','operation_id','serial','sequence','backend_artifacts','artifacts','result'),'initial_epoch_child_index')
    need(index['phase']==m.phase and index['operation_id']==m.cfg['operation_id']
         and type(index['serial']) is int and m.serial<=index['serial']<=m.serial+RECORDS
         and type(index['sequence']) is int and m.b._sequence<=index['sequence']<=m.b._sequence+RECORDS,
         'initial_epoch_child_index_binding')
    need(type(index['artifacts']) is list and 1<=len(index['artifacts'])<=RECORDS,'initial_epoch_child_index_bound')
    rebuilt={};total=0
    for row in index['artifacts']:
        _shape(row,('name','sha256','bytes'),'initial_epoch_child_artifact')
        name=row['name'];need(type(name) is str and journals.NAME.fullmatch(name) and name not in rebuilt
             and (name=='intent.json' or name.startswith(m.phase+'-')) and pin(row['sha256'])
             and type(row['bytes']) is int and 0<=row['bytes']<=journals.MAX_ARTIFACT,'initial_epoch_child_artifact_domain')
        copies=[private_bytes(path/name,row['sha256'],limit=row['bytes']) for path,_ in m.j.phase_paths]
        need(copies[0]==copies[1] and len(copies[0])==row['bytes'],'initial_epoch_child_raw_mismatch')
        rebuilt[name]={'raw':copies[0],'sha256':row['sha256'],'bytes':row['bytes']};total+=row['bytes']
    need(all(rebuilt.get(name)==row for name,row in m.j.artifacts.items()),'initial_epoch_prefork_artifact_changed')
    count=m.j.count-len(m.j.artifacts)+len(rebuilt)
    size=m.j.bytes-sum(r['bytes'] for r in m.j.artifacts.values())+total
    need(count<=m.count_end and size+COMMAND_HEADROOM<=m.bytes_end,'initial_epoch_child_global_budget')
    for path,identity in m.j.phase_paths:
        journals.directory(path,identity);need({p.name for p in path.iterdir()}==set(rebuilt),'initial_epoch_child_partial_artifact')
    artifacts=index['backend_artifacts']
    need(type(artifacts) is dict and all(artifacts.get(k)==v for k,v in m.b._artifacts.items())
         and all((name in rebuilt and rebuilt[name]['sha256']==sha) or m.b._artifacts.get(name)==sha
                 for name,sha in artifacts.items()),'initial_epoch_child_backend_index')
    m.j.artifacts=rebuilt;m.j.count=count;m.j.bytes=size;m.serial=index['serial']
    m.b._sequence=index['sequence'];m.b._artifacts=dict(artifacts);m.j.check()
    result=index['result']
    need(result is None or result['phase']==m.phase and _ref(m.j,result) is not None,'initial_epoch_child_result_ref')
    return result


def _child(m,channel,parentfd,stage):
    status=1;result=None;m.child=True;m.parentfd=parentfd
    try:
        signal.signal(signal.SIGCHLD,signal.SIG_DFL)
        for number in (signal.SIGTERM,signal.SIGINT):signal.signal(number,lambda *_:setattr(m,'cancelled',True))
        driver._child_parent(m.parent['pid'],parentfd)
        need(channel.recv(3)==b'go\n','initial_epoch_child_gate');m.seal()
        result=_observe(m) if stage==OBSERVATION else _install(m)
        m.seal();need(not processes._children(os.getpid()),'initial_epoch_child_native_descendants_remain');status=0
    except BaseException as error:
        try:m.save('child-failed',{'error_type':type(error).__name__,'automatic_retry':False,'ready':False})
        except BaseException:pass
    finally:
        # Failed/unknown SQL or daemon-mediated effects remain owned by ROOT's
        # live pair and sticky operation. Never report child0 on that branch.
        try:
            need(not processes._children(os.getpid()),'initial_epoch_child_processes_remain')
            raw=canonical(_index(m,result));need(len(raw)<=MAX_HANDOFF,'initial_epoch_child_handoff_bound')
            channel.sendall(raw)
        except BaseException:status=1
        channel.close();os.close(parentfd);os._exit(status)


def _hold(leader,owned,*,terminate):
    if terminate:
        # Error teardown must not depend on the capped identity/discovery path
        # which may itself have failed. This owns the raw fork PID through real
        # waitpid, and the dedicated subreaper through actual descendant closure.
        processes._drain(leader,owned,time.monotonic())
        return
    while True:
        try:
            leader.poll()
            if owned is None:
                if leader.returncode is not None and not processes._children(os.getpid()):return
            elif owned.closed():return
        except BaseException:pass
        time.sleep(.05)


def _fork(m):
    """Two source-fixed branches only. No actor body/callback/phase from DATA."""
    need(type(m) is _Producer and m.phase in (OBSERVATION,INSTALL)
         and threading.current_thread() is threading.main_thread() and len(list(Path('/proc/self/task').iterdir()))==1
         and not processes._children(os.getpid()) and _SERIAL.acquire(False),'initial_epoch_dedicated_fork')
    rootfd=None;channel=other=None;leader=owned=None;oldsub=None;previous={};raw=bytearray();ended=False
    interrupted=[];stopping=None;unexpected=False;witness=None;drained=False
    try:
        m.seal();rootfd=os.pidfd_open(m.parent['pid']);channel,other=socket.socketpair()
        previous[signal.SIGCHLD]=signal.getsignal(signal.SIGCHLD);signal.signal(signal.SIGCHLD,signal.SIG_DFL)
        for number in (signal.SIGINT,signal.SIGTERM):
            previous[number]=signal.getsignal(number);signal.signal(number,lambda n,_:interrupted.append(n))
        oldsub=processes._subreaper();processes._subreaper(1)
        # Allocate the wait-only owner BEFORE fork: its constructor cannot fail
        # after a child exists. Assign the raw child PID before ANY fallible FD
        # cleanup, identity lookup or pidfd ownership construction. Even if those
        # fail, this owner needs only waitpid, never a successful /proc identity.
        leader=driver._Leader(None);pid=os.fork()
        if pid==0:
            # Even a pre-prologue FD error must exit this fork child, never
            # unwind into ROOT's run()/caller and duplicate its control flow.
            try:
                channel.close();_child(m,other,rootfd,m.phase)
            finally:os._exit(1)
        leader.pid=pid
        other.close();other=None;owned=processes._Ownership(leader)
        witness={'pid':pid,'start_ticks':processes._identity(pid),'boot_id':m.parent['boot_id']}
        need(witness['start_ticks'] is not None,'initial_epoch_child_native_identity')
        channel.sendall(b'go\n');channel.shutdown(socket.SHUT_WR)
        while True:
            owned.capture();owned.reap()
            if leader.returncode is not None and any(v.pid!=pid for v in owned.living()):unexpected=True
            if stopping is None and (interrupted or unexpected or time.monotonic()>=m.original_end):
                stopping=time.monotonic();owned.signal_all(signal.SIGTERM)
            if stopping is not None and time.monotonic()-stopping>=processes.TERM_GRACE:
                owned.signal_all(signal.SIGKILL)
            if not ended and select.select([channel],[],[],.02)[0]:
                data=channel.recv(32768);raw.extend(data);ended=not data
                need(len(raw)<=MAX_HANDOFF,'initial_epoch_parent_handoff_bound')
            if owned.closed() and ended:break
            if ended:time.sleep(.01)
        exitrow=dict(witness,actual_exit_code=leader.returncode,remaining_children=[])
        m.b._initial_epoch_certificate_last_exit=exitrow;m.b._initial_epoch_certificate_last_index=bytes(raw)
        result=_reconcile(m,decode(bytes(raw)))
        need(type(leader.returncode) is int and leader.returncode==0 and stopping is None and not interrupted
             and not unexpected and result is not None,'initial_epoch_child_not_successfully_closed')
        m.local();return result,exitrow
    except BaseException:
        if leader is not None and leader.pid is not None:
            if channel is not None:
                try:channel.close()
                except OSError:pass
                channel=None
            _hold(leader,owned,terminate=True);drained=True
            m.b._initial_epoch_certificate_last_exit=dict(witness or {'pid':leader.pid},
                actual_exit_code=leader.returncode,remaining_children=[])
        m.b._initial_epoch_certificate_last_index=bytes(raw)
        raise
    finally:
        if leader is not None and leader.pid is not None and not drained:_hold(leader,owned,terminate=False)
        if owned is not None:owned.close_fds()
        for sock in (channel,other):
            if sock is not None:sock.close()
        if rootfd is not None:os.close(rootfd)
        for number,handler in previous.items():signal.signal(number,handler)
        if oldsub is not None:processes._subreaper(oldsub)
        _SERIAL.release()


def _certificate(m,observation_ref,exitref):
    value=decode(_ref(m.j,observation_ref))
    _shape(value,('schema_version','kind','operation_id','inventory_sha256','authority_sha256','source_graph_sha256',
        'subject','configuration','standby_snapshot','migrations','four_root_audit','physical_fence','primary_fence','sql_closure'),
        'initial_epoch_observation_result')
    need(type(value['schema_version']) is int and value['schema_version']==1
         and value['kind']=='openlegal_initial_epoch_observation_pending_parent_exit'
         and value['operation_id']==m.cfg['operation_id'] and value['inventory_sha256']==digest(m.raw)
         and value['authority_sha256']==digest(m.authraw) and value['source_graph_sha256']==m.op.source_graph_sha256,
         'initial_epoch_observation_binding')
    actual=decode(_ref(m.j,exitref))
    need(actual==m.b._initial_epoch_certificate_last_exit and type(actual['actual_exit_code']) is int
         and actual['actual_exit_code']==0 and actual['remaining_children']==[],'initial_epoch_observation_actual_exit')
    manual._process_absent({k:actual[k] for k in ('pid','start_ticks','boot_id')})
    graph=m.save('source-graph',m.cfg['source_graph'])
    ssd=next(i for i,(path,_) in enumerate(m.j.base) if str(path)==next(r['path'] for r in m.q['epoch_roots'] if r['label']=='ssd'))
    graphref={'path':str(m.j.phase_paths[ssd][0]/graph['name']),'sha256':graph['sha256']}
    need(driver._reference(graphref)==canonical(m.cfg['source_graph']),'initial_epoch_graph_raw_reference')
    seedraw,seedsha=seed._phase_raw(m.j,seed.PHASE,seed.PHASE+'-complete.json')
    need(decode(seedraw)==m.p.seed_result,'initial_epoch_seed_result_changed')
    certificate={'schema_version':2,'kind':'openlegal_strict_epoch_certificate','certificate_id':m.q['certificate_id'],
        'generation':1,'predecessor_certificate_sha256':None,'subject':value['subject'],
        'provenance':{'operation_id':m.cfg['operation_id'],'reason':'initial_bind',
            'producer_source':{'path':str(Path(__file__).resolve()),'sha256':m.cfg['source_graph']['initial_epoch_certificate.py']},
            'source_graph':graphref,'inventory':m.inventory_ref,'authority':m.authority_ref,
            'seed':{'phase':seed.PHASE,'name':seed.PHASE+'-complete.json','sha256':seedsha},
            'first_pair':m.q['pair_complete'],'observation':observation_ref,'observation_child_exit':exitref}}
    raw=canonical(certificate);certificates.validate_transition(raw,None)
    return raw,value['subject']


def _readback(m):
    rows=current.read_pair([r['fd'] for r in m.control],[r['descriptor'] for r in m.control],m.original_end)
    need(all(row['certificate_raw']==m.certificate_raw and row['head_raw']==certificates.make_head(m.certificate_raw) for row in rows),
         'initial_epoch_parent_current_head_changed')
    return m.save('independent-dual-readback',{'observations':[
        {k:v for k,v in row.items() if k not in ('certificate_raw','head_raw')} for row in rows],
        'certificate_sha256':digest(m.certificate_raw),'head_sha256':digest(certificates.make_head(m.certificate_raw))})


def run(backend,shared_lock,inventory_ref,authority_ref):
    """Initial only; failure keeps the same backend, locks, roots and both PGs held."""
    _dedicated_root(backend,shared_lock)
    _intake_run_gate(backend,shared_lock,inventory_ref,authority_ref)
    driver._check_end(backend);loaded=_load(inventory_ref,authority_ref);cfg,auth,raw,authraw=loaded
    p,value=_consume_pair(backend,shared_lock,cfg);_active(cfg,p,value)
    backend._initial_epoch_certificate_started=True;backend._initial_epoch_certificate_unknown=False
    backend._initial_epoch_certificate_fds=[]
    try:
        m=_Producer(backend,shared_lock,inventory_ref,authority_ref,loaded,p,value)
        backend._initial_epoch_certificate_live=m;m.j.begin(OBSERVATION)
        m.save('input-binding',{'inventory':inventory_ref,'authority':authority_ref,'pair_complete':m.q['pair_complete'],
            'source_graph_sha256':m.op.source_graph_sha256,'original_absolute_deadline':m.original_end,
            'reserved_records':RECORDS,'reserved_bytes':BYTES,'ready':False})
        _controls(m)
        observation_ref,obs_exit=_fork(m)
        exitref=m.save('observation-child-actual-exit',obs_exit)
        m.certificate_raw,subject=_certificate(m,observation_ref,exitref)
        certificate_ref=m.save('canonical-certificate',decode(m.certificate_raw));m.certificate_ref=certificate_ref
        m.phase=INSTALL;m.j.begin(INSTALL);m._set_op()
        m.install_intent=m.save('dual-install-intent',{'schema_version':1,'kind':'openlegal_initial_epoch_dual_install_intent',
            'operation_id':cfg['operation_id'],'certificate':certificate_ref,'certificate_sha256':digest(m.certificate_raw),
            'head_sha256':digest(certificates.make_head(m.certificate_raw)),
            'roots':[{'path':r['path'],'descriptor':r['descriptor'],'expected_head':None} for r in m.control],
            'observation':observation_ref,'observation_child_exit':exitref,'automatic_retry':False})
        m.install_scope=canonical({'certificate_sha256':digest(m.certificate_raw),'certificate':m.certificate_ref,
            'intent':m.install_intent,'roots':[{'path':r['path'],'descriptor':r['descriptor']} for r in m.control]})
        installations,install_exit=_fork(m);install_exit_ref=m.save('install-child-actual-exit',install_exit)
        body=decode(_ref(m.j,installations))
        _shape(body,('schema_version','kind','operation_id','certificate_sha256','head_sha256','intent','installations'),
               'initial_epoch_installer_result_shape')
        need(type(body['schema_version']) is int and body['schema_version']==1
             and body['operation_id']==cfg['operation_id']
             and body['head_sha256']==digest(certificates.make_head(m.certificate_raw))
             and body['kind']=='openlegal_initial_epoch_install_pending_parent_exit'
             and body['certificate_sha256']==digest(m.certificate_raw) and body['intent']==m.install_intent
             and len(body['installations'])==2,'initial_epoch_installer_result')
        readback=_readback(m)
        m.physical();roots,_=m.roots();need(roots==subject['blob_roots'],'initial_epoch_postinstall_roots_changed')
        final_config,configref=_strict_configuration(m,subject)
        need(final_config['configuration_sha256']==subject['runtime']['configuration_sha256'],'initial_epoch_postinstall_config_changed')
        for side in ('primary','standby'):m.common._native(m.op,subject,m.original_end,which=side)
        pos=m.common._query_metadata(m.op,subject,'position',m.original_end)['result'];_position(pos,subject)
        _,lastfence=m.fence(subject);lastclosure=_closures(m,subject);readback=_readback(m)
        proof={'schema_version':2,'kind':'openlegal_pre_ack_epoch_install','operation_id':cfg['operation_id'],
            'inventory':inventory_ref,'authority':authority_ref,'source_graph_sha256':m.op.source_graph_sha256,
            'certificate_id':m.q['certificate_id'],'certificate_sha256':digest(m.certificate_raw),
            'subject_sha256':digest(canonical(subject)),'generation':1,'predecessor_certificate_sha256':None,
            'observation':observation_ref,'observation_child_exit':exitref,'install_intent':m.install_intent,
            'installations':installations,'install_child_exit':install_exit_ref,'independent_readback':readback,
            'final_configuration':configref,'final_primary_fence':lastfence,'final_sql_closure':lastclosure,'parent':m.parent,
            'ready':False,'public_ack':False,'automatic_retry':False,'outer_actual_os_exit':'NOT_OBSERVABLE_IN_PROCESS'}
        ref=m.save('detached-complete',proof);m.seal()
        backend._initial_epoch_certificate_complete={'raw':canonical(proof),'reference':ref,'parent':m.parent}
        return ref
    except BaseException:
        backend._initial_epoch_certificate_unknown=True
        # Parent-owned root/seed/native descriptors, all journal partials, both
        # current heads and same-OFD locks are deliberately retained. No retry.
        raise
