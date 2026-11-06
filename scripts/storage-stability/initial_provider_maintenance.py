"""One held provider recovery, inside the original live initial driver.

No CLI, role provisioning, authentication edits, provider HTTP, resume, retry,
strict binding or Ready port exists here. Inputs are closed, byte-pinned DATA.
The parent owns the producer's actual waitpid result. Each offline server helper
has an independently checked native PID/cgroup/socket and fixed executable file;
a Docker client's successful exit never substitutes for helper closure.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import platform
import re
import select
import signal
import socket
import stat
import threading
import time
import uuid

import command_process as processes
import initial_blob_seed as seed
import initial_driver as driver
import initial_migration as migration
import journal as journals
import legacy_prepare as legacy
import linux_backend as linux
import manual_backend as manual
from copy_tree import root_fd
from interface import Operation, Refused, freeze_authority, need
from inventory import (MAX_INPUT, canonical, decode, digest, pin, private_bytes, valid_uuid,
    private_fd_mount_id, recheck_private_inbox, read_private_file_at)

PHASE = 'initial_provider_maintenance'
PROFILE = migration.PROFILE
CONTROL = '/maintenance-control'
FUNCTIONS = ('openlegal_admin.provider_inspect()',
    'openlegal_admin.provider_inspect_selected(jsonb)',
    'openlegal_admin.provider_apply(jsonb)', 'openlegal_admin.provider_readback(uuid)')
OPTIONAL_CANARY = ('openlegal_admin.canary_init(uuid,uuid)','openlegal_admin.canary_close(uuid,uuid)')
MAX_HANDOFF = 4 * 1024**2
MAX_CLI = 256 * 1024
_SERIAL = threading.Lock()
SAFE_ENV = ('PATH=/usr/local/bin:/usr/bin:/bin', 'HOME=/nonexistent', 'LANG=C.UTF-8',
            'LC_ALL=C.UTF-8', 'PGPASSFILE=/dev/null', 'PGSERVICEFILE=/dev/null')
INSPECT = ('{"Id":{{json .Id}},"Image":{{json .Image}},"State":'
    '{"Running":{{json .State.Running}},"Pid":{{json .State.Pid}},"ExitCode":{{json .State.ExitCode}},'
    '"Paused":{{json .State.Paused}},"Restarting":{{json .State.Restarting}},'
    '"OOMKilled":{{json .State.OOMKilled}},"Dead":{{json .State.Dead}}},'
    '"User":{{json .Config.User}},"Entrypoint":{{json .Config.Entrypoint}},"Cmd":{{json .Config.Cmd}},'
    '"Labels":{{json .Config.Labels}},"Readonly":{{json .HostConfig.ReadonlyRootfs}},'
    '"Network":{{json .HostConfig.NetworkMode}},"Restart":{{json .HostConfig.RestartPolicy}},'
    '"CapDrop":{{json .HostConfig.CapDrop}},"SecurityOpt":{{json .HostConfig.SecurityOpt}},'
    '"Mounts":{{json .Mounts}}}')
# The sole DBA exception is fixed read-only local authentication metadata. No
# TCP rows, arbitrary SQL, passwords, LDAP/PAM options or raw parser errors are
# returned. Peer map options have a closed identifier grammar; other options are
# replaced before leaving PostgreSQL, and can never qualify a peer/trust rule.
AUTH_SQL = ("BEGIN READ ONLY; SET LOCAL statement_timeout='2s'; SET LOCAL lock_timeout='1s'; "
    "SET LOCAL transaction_timeout='5s'; SET LOCAL search_path=pg_catalog; "
    "SET LOCAL TIME ZONE 'UTC'; SET LOCAL DateStyle='ISO,YMD'; "
    "SELECT jsonb_build_object('session_user',session_user,'current_user',current_user,"
    "'superuser',(SELECT rolsuper FROM pg_roles WHERE rolname=current_user),"
    "'transaction_read_only',current_setting('transaction_read_only'),'hba',"
    "COALESCE((SELECT jsonb_agg(jsonb_build_object('rule_number',rule_number,'file_name',file_name,"
    "'line_number',line_number,'type',type,'database',database,'user_name',user_name,"
    "'auth_method',auth_method,'options',CASE "
    "WHEN auth_method='peer' AND (options IS NULL OR (cardinality(options)=1 "
    "AND options[1] ~ '^map=[a-z_][a-z0-9_]{0,62}$')) THEN options "
    "WHEN auth_method='trust' AND (options IS NULL OR cardinality(options)=0) THEN options "
    "ELSE ARRAY['UNSUPPORTED_REDACTED']::text[] END,"
    "'error',CASE WHEN error IS NULL THEN NULL ELSE 'invalid_auth_rule' END) ORDER BY rule_number) "
    "FROM pg_hba_file_rules WHERE type='local'),'[]'::jsonb),'ident',COALESCE((SELECT "
    "jsonb_agg(jsonb_build_object('map_number',map_number,'file_name',file_name,'line_number',line_number,"
    "'map_name',map_name,'sys_name',sys_name,'pg_username',pg_username,"
    "'error',CASE WHEN error IS NULL THEN NULL ELSE 'invalid_ident_rule' END) ORDER BY map_number) "
    "FROM pg_ident_file_mappings),'[]'::jsonb),'hba_path',current_setting('hba_file'),"
    "'ident_path',current_setting('ident_file')); COMMIT;\n").encode()


def _shape(value, fields, code):
    need(type(value) is dict and set(value) == set(fields), code)


def _name(value):
    need(type(value) is str and re.fullmatch('[a-z_][a-z0-9_]{0,62}', value), 'maintenance_named_role')
    return value


def _quote(value):
    need(type(value) is str and '\x00' not in value and len(value) <= MAX_CLI, 'maintenance_sql_literal')
    return "'" + value.replace("'", "''") + "'"


def _request(value):
    _shape(value, ('actor','reason','quiescence','resolve_legacy','owners','resume','waits'), 'maintenance_request')
    need(value['resume'] is False and value['resolve_legacy'] is False and value['waits'] == [],
         'maintenance_no_release_or_legacy_resolution')
    for key, size in (('actor',200),('reason',1000)):
        need(type(value[key]) is str and 1 <= len(value[key]) <= size
             and not any(ord(c) < 32 or ord(c) == 127 for c in value[key]), 'maintenance_request_text')
    owners = value['owners']
    need(type(owners) is list and len(owners) <= 16 and owners == sorted(set(owners))
         and all(valid_uuid(v) for v in owners), 'maintenance_reviewed_owners')
    q = value['quiescence']
    _shape(q, ('writers_stopped_at','deployment_revision','stopped_writer_ids','evidence_reference'), 'maintenance_quiescence')
    need(type(q['writers_stopped_at']) is int and 0 <= q['writers_stopped_at'] <= int(time.time()),
         'maintenance_actual_stop_time')
    need(type(q['stopped_writer_ids']) is list and 1 <= len(q['stopped_writer_ids']) <= 64
         and len(set(q['stopped_writer_ids'])) == len(q['stopped_writer_ids']), 'maintenance_stopped_writers')
    for text, size in [(q['deployment_revision'],200),(q['evidence_reference'],1000)] + [
            (v,200) for v in q['stopped_writer_ids']]:
        need(type(text) is str and 1 <= len(text) <= size
             and not any(ord(c) < 32 or ord(c) == 127 for c in text), 'maintenance_quiescence_text')
    return value


def _provision(value, candidate):
    """Explicit pre-existing protected UDS profiles; no implicit trust fallback.

    TLS is deliberately not inferred from a certificate-shaped field. A future
    TLS profile needs its own reviewed loaded-peer and native-mount contract.
    """
    _shape(value, ('schema_version','kind','candidate','operator_role','operator_uid',
        'authentication','function_owners'), 'maintenance_preprovision')
    need(type(value['schema_version']) is int and value['schema_version'] == 1
         and value['kind'] == 'openlegal_initial_provider_maintenance_preprovision'
         and value['candidate'] == candidate and type(value['operator_uid']) is int
         and value['operator_uid'] == 10004, 'maintenance_preprovision_identity')
    _name(value['operator_role'])
    auth = value['authentication']
    need(type(auth) is dict and auth.get('kind') in ('protected_uds_peer_v1','protected_uds_trust_v1'),
         'maintenance_authentication_profile')
    fields=('kind','hba_file','ident_file','local_rules_sha256','ident_rules_sha256')
    _shape(auth,fields+(('system_user','map_name') if auth['kind']=='protected_uds_peer_v1' else ()),
        'maintenance_authentication')
    if auth['kind']=='protected_uds_peer_v1':_name(auth['system_user']); _name(auth['map_name'])
    need(pin(auth['local_rules_sha256']) and pin(auth['ident_rules_sha256']),'maintenance_auth_catalog_pins')
    for key in ('hba_file','ident_file'):
        driver._reference(auth[key])
    _shape(value['function_owners'], FUNCTIONS, 'maintenance_function_owners')
    for row in value['function_owners'].values():
        _shape(row, ('name','body_sha256'), 'maintenance_function_owner')
        _name(row['name']); need(pin(row['body_sha256']), 'maintenance_function_body')
    return value


def _load_inventory(inventory_ref):
    raw = driver._reference(inventory_ref); cfg = decode(raw)
    _shape(cfg, ('schema_version','kind','operation_id','driver_inventory_sha256',
        'driver_authority_sha256','legacy_inventory_sha256','source_sha256',
        'seed_inventory','seed_authority','role_prepare','provision','request'), 'maintenance_inventory')
    need(type(cfg['schema_version']) is int and cfg['schema_version'] == 1
         and cfg['kind'] == 'openlegal_initial_provider_maintenance_inventory'
         and valid_uuid(cfg['operation_id']) and all(pin(cfg[k]) for k in
             ('driver_inventory_sha256','driver_authority_sha256','legacy_inventory_sha256','source_sha256')),
         'maintenance_inventory_identity')
    private_bytes(Path(__file__).resolve(), cfg['source_sha256'])
    sc = seed._load(cfg['seed_inventory'], cfg['seed_authority'])
    need(cfg['operation_id'] == sc[0]['operation_id'] and cfg['driver_inventory_sha256'] == digest(sc[7])
         and cfg['driver_authority_sha256'] == digest(sc[9])
         and cfg['legacy_inventory_sha256'] == digest(sc[8]), 'maintenance_original_driver_binding')
    praw = driver._reference(cfg['provision']); p = _provision(decode(praw), sc[6]['candidate'])
    _request(cfg['request'])
    _shape(cfg['role_prepare'], ('phase','name','sha256'), 'maintenance_role_prepare_reference')
    need(cfg['role_prepare']['phase'] == 'initial_role_prepare'
         and pin(cfg['role_prepare']['sha256']), 'maintenance_role_prepare_phase')
    return cfg, raw, p, praw, sc


_AUTHORITY_TRUE = ('maintenance_only', 'provider_apply_permitted', 'preserve_originals')
_AUTHORITY_FALSE = ('strict_ack', 'automatic_retry', 'resume_permitted',
    'legacy_resolution_permitted', 'provider_http_permitted', 'roles_permitted', 'provisioning_permitted',
    'postgres_binding_permitted', 'workload_permitted', 'ready_permitted', 'cleanup_permitted')


def _requested_scope(loaded):
    """Restriction DATA only: deliberately lacks approval and expiry fields."""
    cfg, raw, _, praw, _ = loaded
    return {'schema_version': 1, 'kind': 'openlegal_initial_provider_maintenance_authority',
        'operation_id': cfg['operation_id'], 'inventory_sha256': digest(raw),
        **{k: cfg[k] for k in ('driver_inventory_sha256', 'driver_authority_sha256',
                              'legacy_inventory_sha256', 'source_sha256')},
        'provision_sha256': digest(praw), 'request_sha256': digest(canonical(cfg['request'])),
        **dict.fromkeys(_AUTHORITY_TRUE, True), **dict.fromkeys(_AUTHORITY_FALSE, False)}


def _load(inventory_ref, authority_ref):
    loaded = _load_inventory(inventory_ref)
    cfg, raw, p, praw, sc = loaded
    authraw = driver._reference(authority_ref); auth = decode(authraw)
    scope = _requested_scope(loaded)
    _shape(auth, (*scope, 'approved', 'valid_until'), 'maintenance_authority')
    need(type(auth['schema_version']) is int and auth['approved'] is True
         and type(auth['valid_until']) is int and time.time() < auth['valid_until']
         and {k: auth[k] for k in scope} == scope
         and all(auth[k] is True for k in _AUTHORITY_TRUE)
         and all(auth[k] is False for k in _AUTHORITY_FALSE), 'maintenance_authority_scope')
    return cfg, auth, raw, authraw, p, praw, sc


# First dynamic boundary only. These are DATA/transport bounds, not a native
# enrollment profile or an allocation for a future supervisor/observer.
INTAKE_REQUEST_MAX = 64 * 1024
INTAKE_ENVELOPE_MAX = 16 * 1024
INTAKE_CONTROL_MAX = 4096
INTAKE_RECORDS = 10
INTAKE_BYTES = 4 * MAX_INPUT + 2 * INTAKE_REQUEST_MAX + INTAKE_ENVELOPE_MAX + 3 * INTAKE_CONTROL_MAX
INTAKE_ENVELOPE = 'provider-maintenance-capability.json'
INTAKE_LABEL = 'provider-maintenance-intake'


class _Intake:
    """Only prepare_maintenance_request creates and attaches a live instance."""
    def __init__(self, backend, lock):
        self.b = backend; self.lock = lock; self.state = 'PREPARING'
        self.j = None; self.budget = None; self.inbox_fd = None; self.finished = None
        self.entered_run = False


def _root_context(backend, lock):
    need(type(backend) is driver.Backend and type(lock) is journals.SharedLock
         and os.geteuid() == 0 and threading.current_thread() is threading.main_thread()
         and len(list(Path('/proc/self/task').iterdir())) == 1 and not processes._children(os.getpid())
         and not any(callable(v) for v in vars(backend).values()),
         'maintenance_live_dedicated_root_backend')


def _dedicated_root(backend, lock):
    _root_context(backend, lock)
    need(not getattr(backend, '_initial_provider_maintenance_started', False),
         'maintenance_live_dedicated_root_backend')


def _intake_parent():
    pid = os.getpid(); ticks = processes._identity(pid)
    need(type(ticks) is str and ticks.isdecimal(), 'maintenance_intake_parent')
    return {'pid': pid, 'start_ticks': ticks,
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def _intake_mount(fd):
    return private_fd_mount_id(fd)


def _intake_ref(ref):
    return driver._clock_reference(ref)


def _intake_fail(s):
    if type(s) is _Intake:
        s.state = 'UNKNOWN'
        if s.budget is not None and s.finished is None:
            # Preserve the original error as well as every held descriptor/file.
            try: s.j.fail_reservation(s.budget)
            except BaseException: pass


def _intake_context(s):
    need(type(s) is _Intake and getattr(s.b, '_initial_provider_maintenance_intake', None) is s
         and s.state in ('PREPARING', 'WAITING', 'ACCEPTING', 'DISPATCHING'),
         'maintenance_intake_live_instance')
    _dedicated_root(s.b, s.lock); driver._check_end(s.b)
    need(_intake_parent() == s.parent and s.clock is s.b._initial_driver_clock
         and s.clock.backend is s.b and s.clock.shared_lock is s.lock
         and s.clock.lock_fd == s.lock.fd and s.b._driver_deadline == s.original_end == s.clock.end,
         'maintenance_intake_original_context')
    need(s.j is s.b._initial_driver_journal and s.j.operation == s.operation_id
         and s.j.phase_paths and s.j.phase_paths[0][0].name == 'initial_role_prepare',
         'maintenance_intake_original_phase')
    s.lock.check()


def _intake_record(s, suffix, raw, bound):
    _intake_context(s)
    need(type(raw) is bytes and len(raw) <= bound, 'maintenance_intake_record_bound')
    name = 'maintenance-intake-' + suffix + '.json'
    sha = s.j.save_bytes(name, raw, reservation=s.budget)
    ref = {'phase': 'initial_role_prepare', 'name': name, 'sha256': sha}
    need(s.j.read_phase_bytes(ref['phase'], name, sha) == raw, 'maintenance_intake_dual_ack')
    _intake_context(s)
    return {'path': str(s.j.phase_paths[0][0] / name), 'sha256': sha}, ref


def _intake_inbox(s):
    # Keep the stage-owned mount hook for end-to-end context fault injection.
    need(_intake_mount(s.inbox_fd) == s.inbox_mount, 'maintenance_intake_held_inbox')
    recheck_private_inbox(s.inbox['path'], s.inbox['identity'], s.inbox_fd, s.inbox_mount)


def _intake_read_envelope(s, ref, expected=None):
    _intake_context(s)
    need(_intake_ref(ref)[0] == str(Path(s.inbox['path']) / INTAKE_ENVELOPE),
         'maintenance_intake_envelope_path')
    _intake_inbox(s)
    raw, identity = read_private_file_at(s.inbox_fd, INTAKE_ENVELOPE, ref['sha256'],
        INTAKE_ENVELOPE_MAX, mount_id=s.inbox_mount, previous=expected)
    _intake_inbox(s); _intake_context(s)
    return raw, identity


def _intake_seal(s):
    _intake_context(s)
    s.j.check(); _intake_inbox(s)
    for ref, raw in s.input_bytes:
        need(driver._reference(ref) == raw, 'maintenance_intake_input_changed')
    loaded = _load_inventory(s.inventory_ref)
    need(loaded == s.loaded and _requested_scope(loaded) == s.scope, 'maintenance_intake_candidate_changed')
    live = _live(s.b, s.lock, loaded[0], loaded[4])
    need(live[0] is s.j and canonical(live[1:]) == s.live_raw, 'maintenance_intake_role_changed')
    need(s.j.read_phase_bytes(s.request_ref['phase'], s.request_ref['name'], s.request_ref['sha256']) == s.request_raw,
         'maintenance_intake_request_changed')
    _intake_context(s)


def prepare_maintenance_request(backend, shared_lock, *, provision_ref, request_ref, source_sha256, inbox):
    """Dual-ACK unapproved candidate DATA from this live RolePrep, then wait.

    The returned opaque object remains owned by this ROOT, with the inherited
    original clock/lock and a retained inbox FD. No authority is created, native
    command dispatched, journal phase advanced or supervisor exit observed.
    """
    _dedicated_root(backend, shared_lock)
    if hasattr(backend, '_initial_provider_maintenance_intake'):
        _intake_fail(backend._initial_provider_maintenance_intake)
        raise Refused('maintenance_intake_no_replay')
    s = _Intake(backend, shared_lock); backend._initial_provider_maintenance_intake = s
    try:
        s.parent = _intake_parent(); s.clock = backend._initial_driver_clock
        need(type(s.clock) is driver._Clock and s.clock.backend is backend
             and s.clock.shared_lock is shared_lock and s.clock.lock_fd == shared_lock.fd
             and backend._initial_driver_clock_attempted is True,
             'maintenance_intake_prepared_clock')
        driver._check_end(backend); s.original_end = backend._driver_deadline
        import initial_role_prepare as roles
        prior = getattr(backend, '_initial_role_prepare_complete', None)
        need(type(prior) is dict and 'reference' in prior, 'maintenance_intake_role_required')
        result = roles.consume_live(backend, shared_lock, prior['reference'])
        rc, _, _, _, sc = roles._load(result['inventory'], result['authority'])
        s.j = backend._initial_driver_journal; s.operation_id = result['operation_id']
        need(type(s.j) is journals.DualJournal and s.j.operation == result['operation_id']
             and s.j.phase_paths[0][0].name == roles.PHASE, 'maintenance_intake_role_phase')
        s.budget = s.j.reserve(INTAKE_LABEL, INTAKE_RECORDS, INTAKE_BYTES)
        _shape(inbox, ('path', 'identity'), 'maintenance_intake_inbox')
        _shape(inbox['identity'], ('dev', 'ino'), 'maintenance_intake_inbox_identity')
        need(type(inbox['path']) is str and all(type(v) is int and v >= 0 for v in inbox['identity'].values()),
             'maintenance_intake_inbox_identity')
        s.inbox = copy.deepcopy(inbox)
        journals.directory(s.inbox['path'], s.inbox['identity'])
        need(not any(Path(s.inbox['path']).is_relative_to(path) for path, _ in s.j.paths),
             'maintenance_intake_inbox_inside_journal')
        s.inbox_fd = os.open(s.inbox['path'], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        s.inbox_mount = _intake_mount(s.inbox_fd); _intake_inbox(s)
        need(pin(source_sha256), 'maintenance_intake_source')
        private_bytes(Path(__file__).resolve(), source_sha256)
        _intake_ref(provision_ref); _intake_ref(request_ref)
        praw = driver._reference(provision_ref); request_raw = driver._reference(request_ref)
        _provision(decode(praw), sc[6]['candidate']); req = _request(decode(request_raw))
        s.input_bytes = [(copy.deepcopy(provision_ref), praw), (copy.deepcopy(request_ref), request_raw)]
        provision, _ = _intake_record(s, 'provision', praw, MAX_INPUT)
        _intake_record(s, 'request-input', request_raw, MAX_INPUT)
        cfg = {'schema_version': 1, 'kind': 'openlegal_initial_provider_maintenance_inventory',
            'operation_id': result['operation_id'], 'driver_inventory_sha256': digest(sc[7]),
            'driver_authority_sha256': digest(sc[9]), 'legacy_inventory_sha256': digest(sc[8]),
            'source_sha256': source_sha256, 'seed_inventory': copy.deepcopy(rc['seed_inventory']),
            'seed_authority': copy.deepcopy(rc['seed_authority']), 'role_prepare': copy.deepcopy(prior['reference']),
            'provision': provision, 'request': req}
        s.inventory_ref, _ = _intake_record(s, 'candidate', canonical(cfg), MAX_INPUT)
        s.loaded = _load_inventory(s.inventory_ref); s.cfg = s.loaded[0]; s.scope = _requested_scope(s.loaded)
        live = _live(backend, shared_lock, s.cfg, s.loaded[4]); need(live[0] is s.j, 'maintenance_intake_same_journal')
        s.live_raw = canonical(live[1:]); s.nonce = str(uuid.uuid4())
        graph = dict(sc[4]['source_graph'], **{'initial_blob_seed.py': sc[0]['source_sha256'],
            'initial_role_prepare.py': rc['source_sha256'], 'initial_provider_maintenance.py': source_sha256})
        value = {'schema_version': 1, 'kind': 'openlegal_initial_provider_maintenance_stage_request',
            'operation_id': s.cfg['operation_id'], 'stage': 'provider_maintenance', 'stage_nonce': s.nonce,
            'root': s.parent, 'original_absolute_deadline': s.original_end,
            'source_profile': 'initial_provider_maintenance_intake_v1', 'source_graph_sha256': digest(canonical(graph)),
            'previous_result': s.cfg['role_prepare'], 'candidate_inventory': s.inventory_ref,
            'requested_authority_scope': s.scope, 'authority_scope_sha256': digest(canonical(s.scope)),
            'status': 'UNAPPROVED_CANDIDATE_DATA', 'ready': False}
        s.request_raw = canonical(value)
        _, s.request_ref = _intake_record(s, 'request', s.request_raw, INTAKE_REQUEST_MAX)
        s.state = 'WAITING'; _intake_seal(s)
        return s, s.request_raw
    except BaseException:
        _intake_fail(s); raise


def execute_approved_maintenance(intake, envelope_ref):
    """Consume one external capability and dispatch only the existing run()."""
    need(type(intake) is _Intake and getattr(intake.b, '_initial_provider_maintenance_intake', None) is intake
         and intake.state == 'WAITING', 'maintenance_intake_one_use')
    s = intake; s.state = 'ACCEPTING'
    try:
        _intake_seal(s)
        raw, identity = _intake_read_envelope(s, envelope_ref); value = decode(raw)
        _shape(value, ('schema_version', 'kind', 'operation_id', 'stage', 'stage_nonce', 'request_sha256',
                       'inventory', 'authority'), 'maintenance_intake_envelope')
        need(canonical(value) == raw and type(value['schema_version']) is int and value['schema_version'] == 1
             and value['kind'] == 'openlegal_initial_stage_capability_envelope'
             and value['operation_id'] == s.cfg['operation_id'] and value['stage'] == 'provider_maintenance'
             and value['stage_nonce'] == s.nonce and value['request_sha256'] == digest(s.request_raw)
             and value['inventory'] == s.inventory_ref, 'maintenance_intake_envelope_binding')
        s.authority_ref = copy.deepcopy(value['authority']); _intake_ref(s.authority_ref)
        s.envelope_ref = copy.deepcopy(envelope_ref); s.envelope_raw = raw; s.envelope_identity = identity
        _intake_record(s, 'envelope', raw, INTAKE_ENVELOPE_MAX)
        _intake_record(s, 'consumed', canonical({'operation_id': s.cfg['operation_id'], 'stage_nonce': s.nonce,
            'request_sha256': digest(s.request_raw), 'envelope_sha256': digest(raw),
            'status': 'ATTEMPT_CONSUMED_NOT_APPROVAL'}), INTAKE_CONTROL_MAX)
        loaded = _load(s.inventory_ref, s.authority_ref)
        need(loaded[0] == s.cfg and {k: loaded[1][k] for k in s.scope} == s.scope,
             'maintenance_intake_approval_scope')
        s.authority_raw = loaded[3]
        _intake_record(s, 'authority', s.authority_raw, MAX_INPUT)
        _, s.accepted_ref = _intake_record(s, 'accepted', canonical({'operation_id': s.cfg['operation_id'],
            'stage_nonce': s.nonce, 'request': s.request_ref, 'inventory': s.inventory_ref,
            'authority': s.authority_ref, 'envelope': s.envelope_ref,
            'original_absolute_deadline': s.original_end, 'ready': False}), INTAKE_REQUEST_MAX)
        _intake_seal(s)
        need(_intake_read_envelope(s, s.envelope_ref, s.envelope_identity)[0] == s.envelope_raw
             and driver._reference(s.authority_ref) == s.authority_raw, 'maintenance_intake_approval_changed')
        _intake_context(s)
        s.finished = s.j.finish_reservation(s.budget)
        _intake_context(s)
        s.state = 'DISPATCHING'
        result = run(s.b, s.lock, s.inventory_ref, s.authority_ref)
        s.state = 'CONSUMED'
        return result
    except BaseException:
        _intake_fail(s); raise


def _intake_run_gate(backend, lock, inventory_ref, authority_ref):
    if not hasattr(backend, '_initial_provider_maintenance_intake'): return
    s = backend._initial_provider_maintenance_intake
    try:
        need(type(s) is _Intake and s.b is backend and s.lock is lock and s.state == 'DISPATCHING'
             and s.finished is not None and not s.entered_run
             and inventory_ref == s.inventory_ref and authority_ref == s.authority_ref,
             'maintenance_intake_dispatch_required')
        _intake_seal(s)
        need(_intake_read_envelope(s, s.envelope_ref, s.envelope_identity)[0] == s.envelope_raw
             and driver._reference(s.authority_ref) == s.authority_raw, 'maintenance_intake_approval_changed')
        s.j.read_phase_bytes(s.finished['phase'], s.finished['name'], s.finished['sha256'])
        s.entered_run = True
    except BaseException:
        _intake_fail(s); raise


def _raw(j, ref):
    _shape(ref, ('phase','name','sha256'), 'maintenance_ack_reference')
    need(pin(ref['sha256']), 'maintenance_ack_hash')
    return seed._read_large(j.read_phase_bytes(ref['phase'], ref['name'], ref['sha256']))


def _live(backend, lock, cfg, sc):
    """Consume a typed role-preparation result, never a promoted JSON flag."""
    j = getattr(backend, '_initial_driver_journal', None)
    need(type(j) is journals.DualJournal and j.operation == cfg['operation_id']
         and j.binding['inventory_sha256'] == digest(sc[7])
         and j.binding['authority_sha256'] == digest(sc[9]) and j.phase_paths
         and j.phase_paths[0][0].name == cfg['role_prepare']['phase'], 'maintenance_same_live_driver')
    need(lock.path == Path(sc[5]['shared_lock']['path']) and lock.identity == sc[5]['shared_lock']['identity'],
         'maintenance_same_shared_lock')
    lock.check(); j.check(); seed._bound(backend)
    need(getattr(backend, '_initial_blob_seed_started', False) is True,
         'maintenance_live_seed_required')
    complete = decode(seed._phase_raw(j, seed.PHASE, seed.PHASE + '-complete.json')[0])
    need(complete['kind'] == 'openlegal_initial_blob_seed_result'
         and complete['operation_id'] == cfg['operation_id'] and complete['managed_epoch'] == sc[0]['managed_epoch']
         and complete['all_original_generations_preserved'] is True
         and all(complete[k] is False for k in ('postgres_replica_created','roles_created',
              'workload_started','provider_called','ready','automatic_retry')), 'maintenance_actual_seed_complete')
    initial = decode(seed._phase_raw(j, migration.PHASE, 'driver-complete.json')[0])
    for key in ('legacy_actual_exit','initial_actual_exit'):
        row = initial[key]
        need(type(row['actual_exit_code']) is int and row['actual_exit_code'] == 0
             and row['remaining_children'] == [], 'maintenance_actual_driver_child_zero')
        manual._process_absent({k:row[k] for k in ('pid','start_ticks','boot_id')})
    anchor = decode(seed._phase_raw(j, legacy.PHASE, legacy.PHASE + '-parent-fd-anchor.json')[0])
    need(anchor['pid'] == os.getpid() and anchor['start_ticks'] == processes._identity(os.getpid())
         and anchor['boot_id'] == sc[5]['host']['boot_id'], 'maintenance_original_root_parent')
    old = decode(seed._phase_raw(j, legacy.PHASE, legacy.PHASE + '-returned-payloads.json')[0])
    current = copy.deepcopy(sc[6]); current.update(old['fresh_primary'])
    current['prerequisites'] = {k:{'phase':legacy.PHASE,'name':'live-' + k + '-proof.json',
        'sha256':seed._phase_raw(j,legacy.PHASE,'live-' + k + '-proof.json')[1]}
        for k in ('drain','snapshot','preservation')}
    current['legacy_binding'] = {'inventory_sha256':digest(sc[8]),
        'source_graph_sha256':digest(canonical(sc[4]['source_graph'])),'authority_sha256':digest(sc[9])}
    work = copy.deepcopy(sc[5]); work['initial_migration'] = current
    # Role preparation runs in this ROOT process, with a real native command;
    # its owner verifies the live raw capability. Never invent a child0 for it.
    import initial_role_prepare as roles
    prior = roles.consume_live(backend, lock, cfg['role_prepare'])
    need(prior['kind'] == 'openlegal_initial_role_prepare_result'
         and prior['operation_id'] == cfg['operation_id']
         and prior['producer_execution'] == 'IN_PROCESS_WITH_NATIVE_COMMAND_CLOSURE'
         and prior['maintenance_only'] is True and prior['strict_ack'] is False,
         'maintenance_live_role_prepare_required')
    return j, work, old


class _Maintenance:
    def __init__(self, backend, lock, inventory_ref, authority_ref, loaded, live):
        self.b=backend; self.lock=lock; self.iref=inventory_ref; self.aref=authority_ref
        self.cfg,self.auth,self.raw,self.authraw,self.p,self.praw,self.sc=loaded
        self.j,self.work,self.old=live; self.frozen=canonical(self.work); self.serial=0
        self.primary=self.work['linux']['postgres']['primary']['container_id']; self.helpers=[]
        self.workspace=Path('/tmp/openlegal-initial-provider-maintenance-' + self.cfg['operation_id'])
        self.socketfd=None; self.rootfd=None; self.cancelled=False; self.control={}; self.workspace_identity=None
        self.sql_uncertain=False
        self.parent={'pid':os.getpid(),'start_ticks':processes._identity(os.getpid()),
            'boot_id':self.work['host']['boot_id']}
        self.original_deadline=self.b._driver_deadline
        self.original_clock=getattr(self.b,'_initial_driver_clock',None); self._completed=None
        self.role_result=_raw(self.j,self.cfg['role_prepare'])
        self.role_live=copy.deepcopy(self.b._initial_role_prepare_complete)
        self.budget={'start_count':self.j.count,'start_bytes':self.j.bytes,'records':1024,'bytes':64*1024**2}
        need(self.j.count+self.budget['records']<=4096
             and self.j.bytes+self.budget['bytes']<=journals.MAX_PHASE_BYTES,
             'maintenance_whole_operation_journal_capacity')
        self.op=Operation(self.cfg['operation_id'],'strict_prepare',PHASE,self.work,digest(self.raw),
            digest(canonical(self.sc[4]['source_graph'])),digest(self.authraw),self.j,
            Path(self.work['backup_root']['path']),{},self.seal,freeze_authority(self.auth))

    def seal(self):
        need(self.j.count<=self.budget['start_count']+self.budget['records']
             and self.j.bytes<=self.budget['start_bytes']+self.budget['bytes'],
             'maintenance_reserved_journal_exhausted')
        self.seal_context()

    def seal_context(self):
        # Completed native work retains its original context, while subsequent
        # input DATA has a separate reservation in the same global journal.
        driver._check_end(self.b); need(not self.cancelled,'maintenance_interrupted')
        need(self.b._driver_deadline==self.original_deadline
             and getattr(self.b,'_initial_driver_clock',None) is self.original_clock,
             'maintenance_original_deadline_changed')
        self.lock.check(); driver._writer_fds(self.b); seed._bound(self.b)
        need(canonical(self.work)==self.frozen and time.time()<self.auth['valid_until'], 'maintenance_sealed_context')
        need(driver._reference(self.iref)==self.raw and driver._reference(self.aref)==self.authraw
             and driver._reference(self.cfg['provision'])==self.praw,'maintenance_sealed_inputs')
        driver._source_seal(self.sc[4]['source_graph']); private_bytes(Path(__file__).resolve(),self.cfg['source_sha256'])
        need(seed._load(self.cfg['seed_inventory'],self.cfg['seed_authority'])==self.sc,'maintenance_seed_inputs_changed')
        driver._docker_binding(self.b,self.cfg['operation_id']); self.j.check()
        need(Path('/proc/sys/kernel/random/boot_id').read_text().strip()==self.work['host']['boot_id'],
             'maintenance_boot_changed')
        import initial_role_prepare as roles
        if os.getpid()==self.parent['pid']:
            need(roles.consume_archived_live(self.b,self.lock,self.cfg['role_prepare'])==self.role_result,
                 'maintenance_archived_role_changed')
        else:
            # The role capability belongs to the original ROOT. The child does
            # not impersonate that parent or loosen the role consumer's rules.
            need(os.getppid()==self.parent['pid']
                 and processes._identity(os.getppid())==self.parent['start_ticks']
                 and self.b._initial_role_prepare_complete==self.role_live
                 and _raw(self.j,self.cfg['role_prepare'])==self.role_result,
                 'maintenance_child_original_role_parent')
            rolecfg,roleauth,roleraw,roleauthraw,rolesc=roles._load(
                self.role_result['inventory'],self.role_result['authority'])
            need(rolesc==self.sc and rolecfg['operation_id']==self.cfg['operation_id']
                 and self.role_result['producer_source']=={'path':str(Path(__file__).resolve().parent/'initial_role_prepare.py'),
                     'sha256':rolecfg['source_sha256']},'maintenance_child_role_chain')
            driver._reference(self.role_result['producer_source'])
        if self.rootfd is not None:
            held=os.fstat(self.rootfd); now=self.workspace.lstat()
            need((held.st_dev,held.st_ino)==(now.st_dev,now.st_ino)==self.workspace_identity
                 and now.st_uid==now.st_gid==0 and stat.S_IMODE(now.st_mode)==0o755
                 and set(os.listdir(self.rootfd))==set(self.control),'maintenance_control_root_changed')
            for name,expected in self.control.items():
                fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=self.rootfd)
                try:
                    st=os.fstat(fd); need(stat.S_ISREG(st.st_mode) and st.st_uid==st.st_gid==0
                        and st.st_nlink==1 and stat.S_IMODE(st.st_mode)==0o644
                        and st.st_size==len(expected) and os.read(fd,len(expected)+1)==expected,
                        'maintenance_control_file_changed')
                finally:os.close(fd)

    def save(self, label, value):
        self.seal(); self.serial+=1; name=PHASE+'-%04d-'%self.serial+label+'.json'
        raw=canonical(value)
        need(self.j.count+1<=self.budget['start_count']+self.budget['records']
             and self.j.bytes+len(raw)<=self.budget['start_bytes']+self.budget['bytes'],
             'maintenance_prewrite_reserved_journal')
        sha=self.j.save_bytes(name,raw)
        need(self.j.read_phase_bytes(PHASE,name,sha)==raw,'maintenance_dual_ack')
        return {'phase':PHASE,'name':name,'sha256':sha}

    def command(self,args,*,stdin=None,timeout=30):
        self.seal(); raw=self.b._run(self.op,'docker',args,stdin=stdin,timeout=timeout,
            deadline=self.b._driver_deadline,record=False)
        name=PHASE+'-command-%04d.json'%self.b._sequence; sha=self.j.output_hashes().get(name)
        need(pin(sha),'maintenance_actual_command_receipt')
        ref={'phase':PHASE,'name':name,'sha256':sha}; receipt=_raw(self.j,ref)
        dispatch=['--config',str(self.b._docker_config[0]),'--host','unix:///var/run/docker.sock']
        need(receipt['arguments_sha256']==digest(canonical(dispatch+args))
             and receipt['stdin_sha256']==(digest(stdin) if stdin is not None else None)
             and receipt['stdout_sha256']==digest(raw),'maintenance_native_command_binding')
        _command_ok(receipt); return raw,ref

    def sql(self,payload):
        # All callers are fixed source expressions, never inventory SQL.
        prefix=("SET statement_timeout='30s'; SET lock_timeout='1s'; SET transaction_timeout='35s'; "
            "SET standard_conforming_strings=on; SET TIME ZONE 'UTC'; SET DateStyle='ISO,YMD'; "
            "SET bytea_output='hex'; SET extra_float_digits=3; ")
        args=['exec','-i','-u','70',self.primary,'/usr/bin/env','-i',*SAFE_ENV,
            '/usr/local/bin/psql','-X','-w','-h','/var/run/postgresql','-p','5432','-qAt',
            '-v','ON_ERROR_STOP=1','-U','openlegal_migration','-d','openlegal']
        self.seal(); self.sql_uncertain=True
        raw,cmd=self.command(args,stdin=(prefix+payload+';\n').encode(),timeout=31)
        value=seed._read_large(raw); self.save('sql-raw',{'actual_command':cmd,'raw_utf8':raw.decode('utf8')})
        return value

    def sessions(self):
        _auth_socket(self)
        value=self.sql("SELECT jsonb_build_object('sessions',COALESCE((SELECT jsonb_agg("
            "jsonb_build_object('pid',pid,'backend_start',backend_start::text,'user',usename) ORDER BY pid) "
            "FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()),'[]'::jsonb))")
        need(value=={'sessions':[]},'maintenance_global_sql_not_closed')
        _auth_socket(self)
        self.sql_uncertain=False
        return value


def _command_ok(value):
    need(type(value.get('actual_exit_code')) is int and value['actual_exit_code']==0
         and value.get('started') is True and value.get('owned_processes_closed') is True
         and all(value.get(k) is False for k in ('expired','interrupted','unexpected_descendants',
             'output_limit_exceeded')),'maintenance_native_command_failed')


def _plan(value, request, before, outer):
    _shape(value,('version','operation_id','created_at','expires_at','snapshot','request'),'maintenance_cli_plan')
    need(type(value['version']) is int and value['version']==1 and valid_uuid(value['operation_id'])
         and value['operation_id']!=outer and value['request']==request,'maintenance_nested_plan_binding')
    snap=value['snapshot']; _shape(snap,('version','observed_at','state','waiting','waiting_truncated'),'maintenance_plan_snapshot')
    need(snap['state']==before and snap['waiting']==[] and snap['waiting_truncated'] is False
         and type(value['created_at']) is int and type(value['expires_at']) is int
         and value['created_at']==snap['observed_at'] and value['expires_at']==value['created_at']+900
         and value['created_at']<=int(time.time())<value['expires_at'],'maintenance_plan_timing_or_state')
    return value


def _state_before(value, owners):
    _shape(value,('state','all_slots_free','uncertainty'),'maintenance_provider_observation')
    state=value['state']; _shape(state,('storage_id','corpus_control','schema','budget','control',
        'legacy_lock_held','ledger'),'maintenance_provider_state')
    need(value['all_slots_free'] is True and state['legacy_lock_held'] is False
         and state['budget']['unresolved_response'] is False and state['budget']['admission_owner'] is None,
         'maintenance_live_provider_owner')
    ledger=state['ledger']
    need(type(ledger) is list and len(ledger)<=16 and sorted(r['owner'] for r in ledger)==owners
         and all(r['lock_held'] is False and type(r['slot']) is int and 1<=r['slot']<=16 for r in ledger)
         and len({r['slot'] for r in ledger})==len(ledger),'maintenance_full_unlocked_ledger_review')
    need(type(value['uncertainty']) is list,'maintenance_uncertainty_observation')
    return state


def _observe(owners):
    # Only at most sixteen reviewed identities need before/after materialization.
    # The separate full-row legacy digest proves ALL other old history remains.
    need(type(owners) is list and len(owners)<=16 and all(valid_uuid(v) for v in owners),
         'maintenance_observe_owner_scope')
    predicate='identity IN ('+','.join(_quote(v)+'::uuid' for v in owners)+')' if owners else 'false'
    return ("SELECT jsonb_build_object('state',openlegal_admin.provider_state(),'all_slots_free',"
        "NOT EXISTS(SELECT 1 FROM generate_series(0,16)s WHERE openlegal_admin.slot_locked(s)),"
        "'uncertainty',COALESCE((SELECT jsonb_agg(to_jsonb(u) ORDER BY identity) FROM "
        "openlegal_admin.uncertainty_observation u WHERE "+predicate+"),'[]'::jsonb))")


def _audit(plan, applied, readback, audit, before, after, role):
    """Validate actual CLI/SQL correspondence and the only permitted row deltas."""
    _shape(audit,('operation_id','plan','actor','database_actor','actual_writers_stopped_at',
        'applied_at','before_state','result'),'maintenance_full_audit')
    need(readback==applied==audit['result'] and audit['plan']==plan and audit['before_state']==before['state']
         and audit['operation_id']==plan['operation_id']==applied['operation_id']
         and audit['actor']==plan['request']['actor'] and audit['database_actor']==role
         and audit['actual_writers_stopped_at']==plan['request']['quiescence']['writers_stopped_at']
         and audit['applied_at']==applied['applied_at']
         and plan['created_at']<=audit['applied_at']<plan['expires_at'],'maintenance_cli_audit_correspondence')
    req=plan['request']; _request(req)
    _shape(applied,('version','operation_id','applied_at','recovery_hold','hold_generation',
        'resolved_legacy','resolved_owners','advanced_waits','unchanged_waits'),'maintenance_cli_apply')
    a=before['state']; b=after['state']
    need(after['all_slots_free'] is True and b['ledger']==[] and b['legacy_lock_held'] is False
         and all(b[k]==a[k] for k in ('storage_id','corpus_control','schema','budget'))
         and applied['recovery_hold'] is True and applied['resolved_legacy'] is False
         and applied['advanced_waits']==applied['unchanged_waits']==[]
         and sorted(applied['resolved_owners'])==req['owners'],'maintenance_preserve_accounting_and_hold')
    omit=lambda row:{k:v for k,v in row.items() if k not in ('generation','recovery_hold','hold_started_at')}
    need(omit(b['control'])==omit(a['control']) and b['control']['recovery_hold'] is True
         and b['control']['generation']==applied['hold_generation']!=a['control']['generation']
         and b['control']['hold_started_at']==(a['control']['hold_started_at']
             if a['control']['hold_started_at'] is not None else audit['applied_at']), 'maintenance_control_delta')
    old={r['identity']:r for r in before['uncertainty']}; new={r['identity']:r for r in after['uncertainty']}
    need(len(old)==len(before['uncertainty']) and len(new)==len(after['uncertainty'])
         and all(new.get(k)==v for k,v in old.items()),'maintenance_old_uncertainty_changed')
    added=sorted(set(new)-set(old)); ledger={r['owner']:r['slot'] for r in a['ledger']}
    need(len(added)<=17 and set(added)<=set(ledger),'maintenance_unexplained_uncertainty')
    for key in added:
        row=new[key]; _shape(row,('identity','kind','owner','slot','first_observed_at'),'maintenance_new_uncertainty_shape')
        need(valid_uuid(key) and row=={'identity':key,'kind':'slot','owner':key,
            'slot':ledger[key],'first_observed_at':None},'maintenance_new_uncertainty_identity')
    return added


def _catalog(m):
    role=m.p['operator_role']; signatures=','.join(_quote(v) for v in FUNCTIONS)
    allowed=','.join(_quote(v) for v in FUNCTIONS+OPTIONAL_CANARY)
    query=("SELECT jsonb_build_object('role',(SELECT to_jsonb(r)-'rolpassword' FROM pg_roles r WHERE rolname="
        +_quote(role)+"),'memberships',COALESCE((SELECT jsonb_agg(rolname ORDER BY rolname) FROM pg_roles "
        "WHERE rolname<>"+_quote(role)+" AND pg_has_role("+_quote(role)+",oid,'MEMBER')),'[]'::jsonb),"
        "'broad_privileges',EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname IN ('openlegal','openlegal_admin','openlegal_checkpoint','openlegal_replication','public') "
        "AND c.relkind IN ('r','p','v','m','f') AND ((has_table_privilege("+_quote(role)+",c.oid,'SELECT') "
        "AND NOT(n.nspname='public' AND c.relname='_sqlx_migrations')) "
        "OR has_table_privilege("+_quote(role)+",c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))),"
        "'migration_read',has_table_privilege("+_quote(role)+",'public._sqlx_migrations','SELECT'),"
        "'schema_create',EXISTS(SELECT 1 FROM pg_namespace n WHERE n.nspname !~ '^pg_temp_' "
        "AND n.nspname !~ '^pg_toast_temp_' AND has_schema_privilege("+_quote(role)+",n.oid,'CREATE')),"
        "'database_create',has_database_privilege("+_quote(role)+",current_database(),'CREATE'),"
        "'column_privileges',EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
        "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE a.attnum>0 AND NOT a.attisdropped "
        "AND n.nspname IN ('openlegal','openlegal_admin','openlegal_checkpoint','openlegal_replication','public') "
        "AND ((has_column_privilege("+_quote(role)+",c.oid,a.attnum,'SELECT') "
        "AND NOT(n.nspname='public' AND c.relname='_sqlx_migrations')) OR has_column_privilege("
        +_quote(role)+",c.oid,a.attnum,'INSERT,UPDATE,REFERENCES'))),"
        "'other_definer_execute',EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname IN ('openlegal','openlegal_admin','openlegal_checkpoint','openlegal_replication') "
        "AND p.prosecdef AND has_function_privilege("+_quote(role)+",p.oid,'EXECUTE') "
        "AND p.oid<>to_regprocedure('openlegal.replication_required()') "
        "AND p.oid NOT IN (SELECT to_regprocedure(v) FROM unnest(ARRAY["+allowed+"]::text[])v)),"
        "'nonpublic_extra_execute',EXISTS(SELECT 1 FROM pg_proc p "
        "WHERE has_function_privilege("+_quote(role)+",p.oid,'EXECUTE') "
        "AND NOT EXISTS(SELECT 1 FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a "
        "WHERE a.grantee=0 AND a.privilege_type='EXECUTE') "
        "AND p.oid NOT IN (SELECT to_regprocedure(v) FROM unnest(ARRAY["+allowed+"]::text[])v)),"
        "'optional_canary',(SELECT jsonb_agg(jsonb_build_object('signature',v,'owner',r.rolname,"
        "'body_sha256',encode(sha256(convert_to(p.prosrc,'UTF8')),'hex'),'definer',p.prosecdef,"
        "'config',p.proconfig,'public_execute',EXISTS(SELECT 1 FROM "
        "aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a "
        "WHERE a.grantee=0 AND a.privilege_type='EXECUTE')) ORDER BY v) "
        "FROM unnest(ARRAY["+','.join(_quote(v) for v in OPTIONAL_CANARY)+"]::text[])v "
        "LEFT JOIN pg_proc p ON p.oid=to_regprocedure(v) LEFT JOIN pg_roles r ON r.oid=p.proowner),"
        "'public_observer',(SELECT jsonb_build_object('owner',r.rolname,'body_sha256',"
        "encode(sha256(convert_to(p.prosrc,'UTF8')),'hex'),'definer',p.prosecdef,'config',p.proconfig) "
        "FROM pg_proc p JOIN pg_roles r ON r.oid=p.proowner "
        "WHERE p.oid=to_regprocedure('openlegal.replication_required()')),"
        "'functions',(SELECT jsonb_agg(jsonb_build_object('signature',v,'owner',r.rolname,"
        "'owner_flags',jsonb_build_object('login',r.rolcanlogin,'super',r.rolsuper,'createrole',r.rolcreaterole,"
        "'createdb',r.rolcreatedb,'replication',r.rolreplication,'bypass',r.rolbypassrls),"
        "'definer',p.prosecdef,'body_sha256',encode(sha256(convert_to(p.prosrc,'UTF8')),'hex'),"
        "'config',p.proconfig,'execute',has_function_privilege("+_quote(role)+",p.oid,'EXECUTE'),"
        "'usage',has_schema_privilege("+_quote(role)+",n.oid,'USAGE'),'public_execute',"
        "EXISTS(SELECT 1 FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a "
        "WHERE a.grantee=0 AND a.privilege_type='EXECUTE')) ORDER BY v) FROM unnest(ARRAY["
        +signatures+"]::text[]) v LEFT JOIN pg_proc p ON p.oid=to_regprocedure(v) "
        "LEFT JOIN pg_roles r ON r.oid=p.proowner LEFT JOIN pg_namespace n ON n.oid=p.pronamespace))")
    value=m.sql(query); _operator_privileges(value,role); r=value['role']
    import initial_role_prepare as roles
    observer=roles.FUNCTIONS['openlegal.replication_required()']; actual=value['public_observer']
    need(actual['owner']==m.role_result['role_names'][observer[0]]
         and actual['body_sha256']==observer[1] and actual['definer'] is True
         and 'search_path=pg_catalog' in (actual['config'] or []),'maintenance_public_observer_changed')
    need(type(value['optional_canary']) is list and len(value['optional_canary'])==2,
         'maintenance_optional_canary_count')
    for row in value['optional_canary']:
        source=roles.FUNCTIONS[row['signature']]
        need(row['owner']==m.role_result['role_names'][source[0]] and row['body_sha256']==source[1]
             and row['definer'] is True and row['config']==roles._function_config(row['signature'])
             and row['public_execute'] is False,'maintenance_optional_canary_changed')
    need(type(value['functions']) is list and len(value['functions'])==len(FUNCTIONS),'maintenance_function_count')
    for row in value['functions']:
        expected=m.p['function_owners'][row['signature']]
        need(row['owner']==expected['name'] and row['body_sha256']==expected['body_sha256']
             and type(row['owner_flags']['login']) is bool
             and all(row['owner_flags'][k] is False for k in ('super','createrole','createdb','replication','bypass'))
             and row['definer'] is True and 'search_path=pg_catalog' in (row['config'] or [])
             and row['execute'] is True and row['usage'] is True and row['public_execute'] is False,
             'maintenance_function_owner_or_acl')
    auth=m.p['authentication']; authentication=_auth_observation(m)
    _auth_catalog(authentication,auth,role)
    primary=m.work['initial_migration']['primary_process']['pid']
    for key,path in (('hba_file',authentication['hba_path']),('ident_file',authentication['ident_path'])):
        need(type(path) is str and path.startswith('/') and '..' not in Path(path).parts,'maintenance_native_auth_path')
        fd=os.open('/proc/'+str(primary)+'/root'+path,os.O_RDONLY|os.O_CLOEXEC)
        try:
            meta=os.fstat(fd); need(stat.S_ISREG(meta.st_mode) and meta.st_uid in (0,70)
                and not meta.st_mode&0o022 and meta.st_size<=MAX_CLI,'maintenance_native_auth_metadata')
            raw=os.read(fd,MAX_CLI+1)
            need(raw==driver._reference(auth[key]),'maintenance_native_auth_changed')
        finally:os.close(fd)
    m.save('operator-catalog',value); m.sessions()


def _auth_socket(m):
    """Fresh original native primary/socket identity, never a supplied endpoint."""
    m.seal(); fd=migration._primary(m.b,m.op,m.work['initial_migration'])
    try:
        if m.socketfd is None:
            # The ROOT parent receives no child's open FD. It freshly binds the
            # SAME immutable original process/socket descriptors before SQL.
            m.socketfd=fd; fd=None; return
        held=os.fstat(m.socketfd); current=os.fstat(fd)
        need((held.st_dev,held.st_ino)==(current.st_dev,current.st_ino),
             'maintenance_auth_socket_changed')
    finally:
        if fd is not None:os.close(fd)


def _auth_observation(m):
    """Fixed UID70 UDS postgres read-only metadata; never a provider action.

    The migration observer receives no new grant and remains the observer for
    operator privilege checks. This method accepts no SQL/role/endpoint input.
    """
    _auth_socket(m)
    args=['exec','-i','-u','70',m.primary,'/usr/bin/env','-i',*SAFE_ENV,
        '/usr/local/bin/psql','-X','-w','-h','/var/run/postgresql','-p','5432','-qAt',
        '-v','ON_ERROR_STOP=1','-U','postgres','-d','openlegal']
    m.seal(); m.sql_uncertain=True
    raw,cmd=m.command(args,stdin=AUTH_SQL,timeout=3)
    value=decode(raw)
    m.save('auth-readonly-raw',{'actual_command':cmd,'raw_utf8':raw.decode('utf8')})
    _shape(value,('session_user','current_user','superuser','transaction_read_only',
        'hba','ident','hba_path','ident_path'),'maintenance_auth_observation')
    need(value['session_user']==value['current_user']=='postgres' and value['superuser'] is True
         and value['transaction_read_only']=='on','maintenance_auth_readonly_dba_identity')
    need(type(value['hba']) is list and type(value['ident']) is list
         and all(r['file_name']==value['hba_path'] for r in value['hba'])
         and all(r['file_name']==value['ident_path'] for r in value['ident']),
         'maintenance_auth_unapproved_include')
    _auth_socket(m); m.sessions(); m.seal()
    return {k:value[k] for k in ('hba','ident','hba_path','ident_path')}


def _operator_privileges(value,role):
    """Effective privilege gate includes restricted builtins in every schema."""
    r=value['role']
    need(type(r) is dict and r['rolname']==role and r['rolcanlogin'] is True
         and all(r[k] is False for k in ('rolsuper','rolcreaterole','rolcreatedb','rolreplication','rolbypassrls'))
         and value['memberships']==[] and value['broad_privileges'] is False
         and value['migration_read'] is True
         and value['column_privileges'] is False and value['other_definer_execute'] is False
         and value['nonpublic_extra_execute'] is False
         and value['schema_create'] is False and value['database_create'] is False,
         'maintenance_operator_broad_authority')
    # rolinherit=true is harmless only after the actual empty membership and
    # broad-grant checks; existing operators are never arbitrarily ALTERed.
    need(type(r['rolinherit']) is bool,'maintenance_operator_inherit_flag')


def _auth_catalog(value, auth, role):
    """Evaluate the pinned complete local rule order, never a TCP auth rule.

    Trust authenticates no identity. Its authority is the protected native socket
    and source-owned fixed helper boundary; it is never labelled peer or TLS.
    """
    rules=value['hba']; maps=value['ident']
    need(type(rules) is list and type(maps) is list
         and digest(canonical(rules))==auth['local_rules_sha256']
         and digest(canonical(maps))==auth['ident_rules_sha256'],'maintenance_auth_catalog_changed')
    numbers=[r['rule_number'] for r in rules]
    need(all(type(n) is int and n>0 for n in numbers) and numbers==sorted(set(numbers)),
         'maintenance_hba_rule_order')
    need(all(r['error'] is None for r in rules+maps),'maintenance_auth_parse_error')
    match=None
    for row in rules:
        need(row['type']=='local' and type(row['database']) is list and type(row['user_name']) is list,
             'maintenance_hba_local_only')
        db=row['database']; users=row['user_name']
        # Regex, external lists, role selectors and include ambiguity are not
        # inferred. This initial profile consumes literal local rules only.
        need(all(type(v) is str and not v.startswith(('+','@','/')) for v in db+users)
             and not set(db)&{'samegroup','samerole','replication'},'maintenance_hba_literal_selectors')
        applies_db='openlegal' in db or 'all' in db or 'sameuser' in db and role=='openlegal'
        applies_user=role in users or 'all' in users
        if applies_db and applies_user:match=row; break
    need(match is not None,'maintenance_matching_local_auth_missing')
    if auth['kind']=='protected_uds_trust_v1':
        need(match['auth_method']=='trust' and match['options'] in (None,[]),
             'maintenance_explicit_local_trust_required')
    else:
        need(match['user_name']==[role] and match['database']==['openlegal']
             and match['auth_method']=='peer' and match['options']==['map='+auth['map_name']],
             'maintenance_exact_peer_hba')
        matching=[r for r in maps if r['map_name']==auth['map_name']]
        need(len(matching)==1 and matching[0]['sys_name']==auth['system_user']
             and matching[0]['pg_username']==role,'maintenance_exact_peer_map')


def _gate(action, role):
    need(action in ('plan','apply','readback'),'maintenance_fixed_action'); _name(role)
    argument={'plan':CONTROL+'/request.json','apply':CONTROL+'/plan.json',
        'readback':'$(cat '+CONTROL+'/recovery-id)'}[action]
    # The readback ID is written solely from the parsed native plan, and its
    # canonical UUID grammar is checked before this fixed script is built.
    url=('OPENLEGAL_PROVIDER_ADMIN_DATABASE_URL=postgresql://'+role+'@localhost/openlegal'
         '?host=%2Fvar%2Frun%2Fpostgresql&port=5432')
    script=('n=0; while [ ! -f '+CONTROL+'/go-'+action+' ]; do n=$((n+1)); '
        '[ "$n" -lt 600 ] || exit 125; sleep 0.1; done; '
        'exec /usr/bin/env -i '+ ' '.join(SAFE_ENV) + " '"+url+"' "
        '/usr/local/bin/openlegal-server --provider-admin '+action+' '+argument+' '+CONTROL+'/config.toml')
    return script


def _inspect(m, helper, running):
    raw,_=m.command(['inspect','--type','container','--format',INSPECT,helper['cid']]); item=decode(raw)
    need(item['Id']==helper['cid'] and item['Image']==m.p['candidate']['image_id']
         and item['State']['Running'] is running and all(item['State'][k] is False
             for k in ('Paused','Restarting','OOMKilled','Dead')),'maintenance_helper_native_state')
    need(item['User']=='10004:10004' and item['Entrypoint']==['/bin/sh']
         and item['Cmd']==['-ec',helper['gate']] and item['Readonly'] is True and item['Network']=='none'
         and item['Restart']=={'Name':'no','MaximumRetryCount':0} and item['CapDrop']==['ALL']
         and 'no-new-privileges' in item['SecurityOpt'] and item['Labels'].get('openlegal.operation')==m.cfg['operation_id']
         and item['Labels'].get('openlegal.maintenance-action')==helper['action'],'maintenance_fixed_helper_spec')
    mounts=sorted((r['Source'],r['Destination'],r['RW'],r['Type']) for r in item['Mounts'] if r['Type']!='tmpfs')
    need(mounts==sorted([(helper['socket_source'],'/var/run/postgresql',False,'bind'),
        (str(m.workspace),CONTROL,False,'bind')]) and all(r['Destination']=='/tmp' for r in item['Mounts']
        if r['Type']=='tmpfs'),'maintenance_fixed_helper_mounts')
    return item


def _write(m,name,raw):
    need(name in ('request.json','plan.json','config.toml','recovery-id','go-plan','go-apply','go-readback'),
         'maintenance_fixed_control_file')
    m.seal(); migration._write(m.workspace,name,raw,0o644,0); m.control[name]=raw; m.seal()


def _helper(m, action):
    m.seal(); script=_gate(action,m.p['operator_role'])
    primary=m.work['initial_migration']['primary_process']['pid']
    source='/proc/'+str(primary)+'/root/var/run/postgresql'
    args=['create','--pull=never','--name','openlegal-maintenance-'+m.cfg['operation_id']+'-'+action,
        '--label','openlegal.operation='+m.cfg['operation_id'],'--label','openlegal.maintenance-action='+action,
        '--restart=no','--network=none','--read-only','--user=10004:10004','--cap-drop=ALL',
        '--security-opt=no-new-privileges','--pids-limit=64','--memory=512m','--memory-swap=512m','--cpus=1',
        '--log-driver=json-file','--log-opt=max-size=1m','--log-opt=max-file=1',
        '--tmpfs','/tmp:rw,noexec,nosuid,nodev,size=16m',
        '--mount','type=bind,src='+source+',dst=/var/run/postgresql,readonly',
        '--mount','type=bind,src='+str(m.workspace)+',dst='+CONTROL+',readonly',
        '--entrypoint=/bin/sh',m.p['candidate']['image_id'],'-ec',script]
    # Even a password-free connection URL is excluded from journal argv and
    # Docker Config.Env projections. Only the fixed source argument hash is kept.
    m.save('helper-create-intent',{'action':action,'argv_sha256':digest(canonical(args)),
        'image':m.p['candidate'],'native_exec_profile':'FIXED_IMAGE_EXEC_V1'})
    raw,_=m.command(args); need(re.fullmatch(b'[0-9a-f]{64}\n',raw),'maintenance_created_identity_unknown')
    h={'action':action,'cid':raw.strip().decode(),'gate':script,'socket_source':source,
       'pidfd':None,'groupfd':None,'process':None,'closed':False,'started':False,'known':False}
    m.helpers.append(h); m.save('helper-created',{'action':action,'container_id':h['cid']})
    # A failed start ACK may already have dispatched the native helper. Never
    # replace missing native qualification with Docker-client closure.
    h['started']=True; m.command(['start',h['cid']]); item=_inspect(m,h,True); pid=item['State']['Pid']
    need(type(pid) is int and pid>0,'maintenance_native_init_pid')
    h['pidfd']=os.pidfd_open(pid); h['process']={'pid':pid,'start_ticks':processes._identity(pid),
        'boot_id':m.work['host']['boot_id']}
    need(type(h['process']['start_ticks']) is str,'maintenance_native_init_identity')
    status=dict(line.split(':',1) for line in (Path('/proc')/str(pid)/'status').read_text().splitlines()
        if ':' in line)
    need(status['Uid'].split()==['10004']*4 and status['Gid'].split()==['10004']*4
         and int(status['CapEff'].strip(),16)==0 and status['NoNewPrivs'].strip()=='1',
         'maintenance_native_helper_privileges')
    namespace=(Path('/proc')/str(pid)/'ns/net').stat(); parentnet=Path('/proc/self/ns/net').stat()
    need((namespace.st_dev,namespace.st_ino)!=(parentnet.st_dev,parentnet.st_ino),
         'maintenance_native_helper_shared_network')
    groups=linux.process_cgroup(pid); need(len(groups)==1 and groups[0].startswith('0::/')
        and any(v in (h['cid'],'docker-'+h['cid']+'.scope') for v in Path(groups[0][3:]).parts),
        'maintenance_native_helper_cgroup')
    h['groups']=groups; h['groupfd']=root_fd('/sys/fs/cgroup'+groups[0][3:]); cg=os.fstat(h['groupfd'])
    h['group_identity']=(cg.st_dev,cg.st_ino)
    mapped=m.b._container_directory(pid,'/var/run/postgresql')
    try:
        a=os.fstat(mapped); b=os.fstat(m.socketfd)
        need((a.st_dev,a.st_ino)==(b.st_dev,b.st_ino),'maintenance_native_socket_mount_correspondence')
    finally:os.close(mapped)
    native_bin=m.b._container_directory(pid,'/usr/local/bin')
    try:
        fd=os.open('openlegal-server',os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=native_bin)
        try:
            meta=os.fstat(fd); need(stat.S_ISREG(meta.st_mode) and meta.st_size<=128*1024**2
                and meta.st_uid==0 and not meta.st_mode&0o022,'maintenance_native_server_file')
            legacy._hash_file(fd,m.p['candidate']['executable_sha256'])
            h['binary_identity']=(meta.st_dev,meta.st_ino)
        finally:os.close(fd)
    finally:os.close(native_bin)
    if m.p['authentication']['kind']=='protected_uds_peer_v1':
        # Peer resolves the client UID in the PostgreSQL server's namespace.
        passwd=(Path('/proc')/str(primary)/'root/etc/passwd').read_text()
        accounts=[r.split(':') for r in passwd.splitlines() if r and not r.startswith('#')]
        need([r[0] for r in accounts if len(r)==7 and r[2]=='10004']==[m.p['authentication']['system_user']],
             'maintenance_peer_system_user')
    need(processes._identity(pid)==h['process']['start_ticks'] and not select.select([h['pidfd']],[],[],0)[0],
         'maintenance_native_helper_reused')
    h['known']=True
    m.save('helper-native-owner',{'action':action,'container_id':h['cid'],'process':h['process'],
        'groups':groups,'cgroup_identity':{'dev':cg.st_dev,'ino':cg.st_ino},
        'server_file_identity':{'dev':h['binary_identity'][0],'ino':h['binary_identity'][1]},
        'server_file_sha256':m.p['candidate']['executable_sha256'],
        'authentication_profile':m.p['authentication']['kind'],
        'identity_authentication':m.p['authentication']['kind']=='protected_uds_peer_v1',
        'native_uid':10004,'native_gid':10004,'native_cap_eff':0,'native_no_new_privileges':True,
        'network_namespace':{'dev':namespace.st_dev,'ino':namespace.st_ino},
        'native_exec_profile':'FIXED_IMAGE_EXEC_V1','native_server_exec_observed':False})
    m.sessions(); migration._fence(m.b,m.op,m.old['payloads']['drain']); m.seal()
    m.save('helper-dispatch-intent',{'action':action,'container_id':h['cid'],
        'native_exec_profile':'FIXED_IMAGE_EXEC_V1','single_apply':action=='apply','automatic_retry':False})
    m.seal(); m.sql_uncertain=True
    _write(m,'go-'+action,b'go\n')
    raw,waitref=m.command(['wait',h['cid']],timeout=120)
    need(raw==b'0\n','maintenance_native_helper_wait_not_zero')
    final=_inspect(m,h,False)
    need(type(final['State']['Pid']) is int and final['State']['Pid']==0
         and type(final['State']['ExitCode']) is int and final['State']['ExitCode']==0,
         'maintenance_native_helper_exit_unknown')
    _helper_closed(h); h['closed']=True
    raw,logs=m.command(['logs',h['cid']])
    need(len(raw)<=MAX_CLI,'maintenance_cli_output_bound'); value=decode(raw)
    m.save('cli-raw',{'action':action,'actual_logs_command':logs,'raw_utf8':raw.decode('utf8')})
    receipt={'actual_exit_code':0,'started':True,'owned_processes_closed':True,'expired':False,
        'interrupted':False,'unexpected_descendants':False,'output_limit_exceeded':False,
        'container_id':h['cid'],'native_init':h['process'],'wait_command':waitref,
        'native_exec_profile':'FIXED_IMAGE_EXEC_V1','native_server_exec_observed':False,
        'authentication_profile':m.p['authentication']['kind'],
        'identity_authentication':m.p['authentication']['kind']=='protected_uds_peer_v1',
        'server_file_sha256':m.p['candidate']['executable_sha256'],'owned_members':[]}
    native=m.save('helper-native-closed',receipt); m.sessions()
    return value,native


def _helper_closed(h):
    need(h['known'] is True and h['pidfd'] is not None and h['groupfd'] is not None,
         'maintenance_native_helper_ownership_unknown')
    need(select.select([h['pidfd']],[],[],0)[0] and linux.members(h['groups'])==[],
         'maintenance_native_helper_not_closed')
    manual._process_absent(h['process']); cg=os.fstat(h['groupfd'])
    need((cg.st_dev,cg.st_ino)==h['group_identity'],'maintenance_helper_cgroup_rebound')


def _retain_until_helpers_closed(m):
    """Failure is retained; no rm, replay, SQL cancel or success is fabricated.

    Known native actors receive TERM once as ownership reconciliation. A started
    helper with missing identity is UNKNOWN and holds the producer indefinitely.
    Payload expiry does not release process ownership or restore SIGCHLD state.
    """
    for h in m.helpers:
        if not h['started']:continue
        if not h['known']:
            while True:time.sleep(.1)
        attempted=False
        while True:
            try:
                _helper_closed(h); h['closed']=True; break
            except BaseException:
                # Even ESRCH/EIO/procfs/identity errors cannot unwind the native
                # ownership hold. No negative observation is guessed closure.
                try:
                    if not attempted and not select.select([h['pidfd']],[],[],0)[0]:
                        need(processes._identity(h['process']['pid'])==h['process']['start_ticks'],
                             'maintenance_failure_actor_reused')
                        cg=os.fstat(h['groupfd'])
                        need((cg.st_dev,cg.st_ino)==h['group_identity'],
                             'maintenance_failure_cgroup_rebound')
                        attempted=True  # Unknown signal ACK never permits replay.
                        signal.pidfd_send_signal(h['pidfd'],signal.SIGTERM,None,0)
                except BaseException:
                    pass
                time.sleep(.1)
    # No descriptor error may skip a later helper's native closure obligation.
    for h in m.helpers:
        for fd in (h['pidfd'],h['groupfd']):
            if fd is not None:
                try:os.close(fd)
                except OSError:pass


def _retain_sql_closed(m):
    """A transmitted SQL/CLI intent stays uncertain until global closure.

    Only a fresh migration-observer closure query may follow an error. Never
    replay the DBA metadata query, provider CLI or any SQL payload. Original
    authority/deadline/cancellation checks remain mandatory; expiry means this
    producer retains ownership as UNKNOWN indefinitely for ROOT reconciliation.
    """
    while m.sql_uncertain:
        try:
            m.seal(); m.sessions()
            need(m.sql_uncertain is False,'maintenance_sql_closure_not_observed')
        except BaseException:
            time.sleep(.1)


def _produce(m):
    """Only this fixed child body may execute the provider mutation."""
    m.seal(); migration._fence(m.b,m.op,m.old['payloads']['drain'])
    m.socketfd=migration._primary(m.b,m.op,m.work['initial_migration'])
    m.sessions(); _catalog(m)
    image=decode(m.command(['image','inspect','--format',migration.IMAGE_FORMAT,m.p['candidate']['reference']])[0])
    need(image=={'Id':m.p['candidate']['image_id'],'Os':'linux','Architecture':
        {'x86_64':'amd64','aarch64':'arm64'}.get(platform.machine()),'Volumes':None},
        'maintenance_official_native_image')
    os.mkdir(m.workspace,0o755); os.chmod(m.workspace,0o755); m.rootfd=root_fd(m.workspace)
    st=os.fstat(m.rootfd); m.workspace_identity=(st.st_dev,st.st_ino)
    _write(m,'config.toml',PROFILE); _write(m,'request.json',canonical(m.cfg['request']))
    observe=_observe(m.cfg['request']['owners'])
    before=m.sql(observe); state=_state_before(before,m.cfg['request']['owners'])
    m.save('provider-before',before)
    plan,plan_native=_helper(m,'plan'); _plan(plan,m.cfg['request'],state,m.cfg['operation_id'])
    planref=m.save('plan',plan)
    _write(m,'plan.json',canonical(plan)); _write(m,'recovery-id',(plan['operation_id']+'\n').encode())
    need(m.sql(observe)==before,'maintenance_provider_changed_before_apply')
    # This is the sole apply call. There is no retry/readback-as-apply branch.
    applied,native=_helper(m,'apply'); applyref=m.save('apply',applied)
    readback,read_native=_helper(m,'readback'); readref=m.save('readback',readback)
    audit=m.sql('SELECT to_jsonb(a) FROM openlegal_admin.provider_recovery_audit a WHERE operation_id='
        +_quote(plan['operation_id'])+'::uuid'); auditref=m.save('audit',audit)
    after=m.sql(observe); added=_audit(plan,applied,readback,audit,before,after,m.p['operator_role'])
    _preserved(m,plan['operation_id'],added)
    m.save('provider-after',after); closure=m.save('sql-closure',m.sessions())
    need(all(h['closed'] for h in m.helpers),'maintenance_helpers_not_closed_before_exit')
    native=m.save('native-commands-closed',{'actual_exit_code':0,'started':True,
        'owned_processes_closed':True,'expired':False,'interrupted':False,
        'unexpected_descendants':False,'output_limit_exceeded':False,
        'native_exec_profile':'FIXED_IMAGE_EXEC_V1','native_server_exec_observed':False,
        'helper_receipts':[plan_native,native,read_native]})
    pending={'provider_recovery_operation':plan['operation_id'],'plan':planref,'apply':applyref,
        'readback':readref,'audit':auditref,'native_command':native,'sql_closure':closure,
        'new_uncertainty_ids':added}
    m.save('pending-parent-exit',pending); m.seal()
    return pending


def _preserved(m, operation, added):
    """Reconstruct old full rows in the original SQL to_jsonb(t)::text domain."""
    old=m.old['payloads']['preservation']; op=_quote(operation)+'::uuid'
    audit='(SELECT a.* FROM openlegal_admin.provider_recovery_audit a WHERE operation_id='+op+')'
    ids=','.join(_quote(v)+'::uuid' for v in added)
    transformed={
        'openlegal_admin.provider_control':"(SELECT (jsonb_populate_record(NULL::openlegal_admin.provider_control,a.before_state->'control')).* FROM "+audit+' a)',
        'openlegal.provider_request_admission':"(SELECT r.* FROM "+audit+" a CROSS JOIN LATERAL jsonb_populate_recordset(NULL::openlegal.provider_request_admission,COALESCE((SELECT jsonb_agg(v-'lock_held') FROM jsonb_array_elements(a.before_state->'ledger')v),'[]'::jsonb)) r)",
        'openlegal_admin.provider_recovery_audit':'(SELECT a.* FROM openlegal_admin.provider_recovery_audit a WHERE operation_id<>'+op+')',
        'openlegal_admin.uncertainty_observation':'(SELECT u.* FROM openlegal_admin.uncertainty_observation u WHERE '+('identity NOT IN ('+ids+')' if ids else 'true')+')'}
    observed={}
    for table,expected in old['tables'].items():
        need(type(table) is str and re.fullmatch(r'openlegal(?:_admin)?\.[a-z_][a-z0-9_]{0,62}',table),
             'maintenance_old_table_domain')
        sql=("SELECT jsonb_build_object('rows',count(*),'sha256',encode(sha256(convert_to("
            "COALESCE(string_agg(h,'' ORDER BY h COLLATE \"C\"),''),'UTF8')),'hex')) FROM "
            "(SELECT encode(sha256(convert_to(to_jsonb(t)::text,'UTF8')),'hex') h FROM "
            +transformed.get(table,table)+' t) q')
        actual=m.sql(sql); need(actual==expected,'maintenance_old_rows_changed'); observed[table]=actual
    for sql,key in ((migration.REFERENCES_SQL,'references'),(migration.OLD_MIGRATIONS_SQL,'migration_history')):
        need(m.sql(sql)==old[key],'maintenance_old_history_or_references_changed')
    need(m.sql(migration.MIGRATIONS_SQL)==[dict(r,success=True) for r in migration._manifest(m.work['initial_migration'])],
         'maintenance_schema33_checksums_changed')
    m.save('old-full-rows-preserved',{'tables':observed,'references':old['references'],
        'migration_history':old['migration_history'],'sql_hash_domain':'to_jsonb(t)::text'})


def _index(m):
    return {'phase':PHASE,'operation_id':m.cfg['operation_id'],'serial':m.serial,
        'sequence':m.b._sequence,'backend_artifacts':m.b._artifacts,
        'artifacts':[{'name':name,'sha256':row['sha256'],'bytes':row['bytes']}
            for name,row in sorted(m.j.artifacts.items())]}


def _reconcile(m, index):
    """Rebuild ONLY the new phase from both physical raw copies, after waitpid.

    No source-supplied phase handler, arbitrary FD, success JSON or index can
    create a parent-observed exit. Existing driver history is left unchanged.
    """
    _shape(index,('phase','operation_id','serial','sequence','backend_artifacts','artifacts'),
        'maintenance_child_index')
    need(index['phase']==PHASE and index['operation_id']==m.cfg['operation_id']
         and type(index['serial']) is int and m.serial<=index['serial']<=4096
         and type(index['sequence']) is int and m.b._sequence<=index['sequence']<=m.b._sequence+4096,
         'maintenance_child_index_binding')
    need(type(index['artifacts']) is list and 1<=len(index['artifacts'])<=4096,
         'maintenance_child_artifact_bound')
    rebuilt={}; total=0
    for row in index['artifacts']:
        _shape(row,('name','sha256','bytes'),'maintenance_child_artifact')
        name=row['name']; need(type(name) is str and journals.NAME.fullmatch(name)
            and (name=='intent.json' or name.startswith(PHASE+'-')) and name not in rebuilt
            and pin(row['sha256']) and type(row['bytes']) is int and 0<=row['bytes']<=journals.MAX_ARTIFACT,
            'maintenance_child_artifact_domain')
        total+=row['bytes']; need(total<=journals.MAX_PHASE_BYTES,'maintenance_child_phase_bound')
        copies=[]
        for path,identity in m.j.phase_paths:
            journals.directory(path,identity)
            raw=private_bytes(path/name,row['sha256'],limit=row['bytes'])
            need(len(raw)==row['bytes'],'maintenance_child_raw_size'); copies.append(raw)
        need(copies[0]==copies[1],'maintenance_child_dual_raw_mismatch')
        rebuilt[name]={'sha256':row['sha256'],'bytes':row['bytes'],'raw':copies[0]}
    need(all(name in rebuilt and rebuilt[name]==row for name,row in m.j.artifacts.items()),
         'maintenance_prefork_artifact_changed')
    count=m.j.count-len(m.j.artifacts)+len(rebuilt)
    size=m.j.bytes-sum(v['bytes'] for v in m.j.artifacts.values())+total
    need(count<=4096 and size<=journals.MAX_PHASE_BYTES,'maintenance_global_journal_bound')
    for path,identity in m.j.phase_paths:
        journals.directory(path,identity)
        need({p.name for p in path.iterdir()}==set(rebuilt),'maintenance_partial_or_extra_artifact')
    artifacts=index['backend_artifacts']
    need(type(artifacts) is dict and all(artifacts.get(k)==v for k,v in m.b._artifacts.items())
         and all((name in rebuilt and rebuilt[name]['sha256']==sha) or m.b._artifacts.get(name)==sha
             for name,sha in artifacts.items()),'maintenance_backend_artifacts')
    m.j.artifacts=rebuilt; m.j.count=count; m.j.bytes=size
    m.b._sequence=index['sequence']; m.b._artifacts=dict(artifacts); m.serial=index['serial']
    m.j.check()


def _child(m, channel, parent, parentfd):
    status=1
    try:
        signal.signal(signal.SIGCHLD,signal.SIG_DFL)
        for number in (signal.SIGTERM,signal.SIGINT):
            signal.signal(number,lambda *_:setattr(m,'cancelled',True))
        driver._child_parent(parent,parentfd)
        need(channel.recv(3)==b'go\n','maintenance_producer_gate')
        driver._child_parent(parent,parentfd); _produce(m); status=0
    except BaseException as error:
        try:m.save('failed',{'status':'FAILED_OR_UNKNOWN','error_type':type(error).__name__,
            'automatic_retry':False,'resume_permitted':False,'native_success_claimed':False})
        except BaseException:pass
    finally:
        # Child0 is impossible until all daemon-owned helpers and owned native
        # process groups really close. Failure cleanup does not replay payload.
        try:
            _retain_until_helpers_closed(m)
            _retain_sql_closed(m)
            need(not processes._children(os.getpid()),'maintenance_child_descendants_remain')
            raw=canonical(_index(m)); need(len(raw)<=MAX_HANDOFF,'maintenance_index_bound')
            channel.sendall(raw)
        except BaseException:status=1
        # A descriptor/socket cleanup error must never unwind into ROOT's
        # caller. Helpers/SQL have already gone through their closure barrier.
        try:
            for fd in (m.socketfd,m.rootfd,parentfd):
                if fd is not None:os.close(fd)
            channel.close()
        except BaseException:status=1
        finally:os._exit(status)


def _hold_producer(leader, owned, *, terminate):
    """Never restore parent ownership state while any owned child is unknown."""
    if leader is None or leader.pid is None:return
    attempted=False
    while True:
        try:
            if owned is None:
                leader.poll()
                if leader.returncode is not None and not processes._children(os.getpid()):return
            else:
                owned.capture(); owned.reap()
                if owned.closed():return
                if terminate and not attempted:
                    for entry in owned.entries.values():
                        if entry.pid==leader.pid:
                            attempted=True; entry.send(signal.SIGTERM)
        except BaseException:
            # Unknown discovery/signal ACK is an ownership hold, not permission
            # to kill away daemon-owned helper reconciliation in the producer.
            pass
        time.sleep(.05)


def _fork(m):
    """Fixed producer fork; no supplied callable and no shared PHASES edit."""
    need(threading.current_thread() is threading.main_thread()
         and len(list(Path('/proc/self/task').iterdir()))==1 and not processes._children(os.getpid())
         and _SERIAL.acquire(False),'maintenance_dedicated_parent')
    parentfd=None; channel=other=None; leader=owned=None; previous={}; oldsub=None
    raw=bytearray(); ended=False; interrupted=[]; stopping=False; unexpected=False
    try:
        m.seal(); parentfd=os.pidfd_open(os.getpid()); channel,other=socket.socketpair()
        previous[signal.SIGCHLD]=signal.getsignal(signal.SIGCHLD); signal.signal(signal.SIGCHLD,signal.SIG_DFL)
        for number in (signal.SIGINT,signal.SIGTERM):
            previous[number]=signal.getsignal(number); signal.signal(number,lambda n,_:interrupted.append(n))
        oldsub=processes._subreaper(); processes._subreaper(1); parent=os.getpid()
        # Allocate a wait-only owner before a child exists. The parent stores
        # the actual fork PID before any fallible cleanup/ownership/identity IO.
        leader=driver._Leader(None); pid=os.fork()
        if pid==0:
            try:
                channel.close(); _child(m,other,parent,parentfd)
            finally:os._exit(1)
        leader.pid=pid
        other.close(); other=None
        owned=processes._Ownership(leader)
        witness={'pid':pid,'start_ticks':processes._identity(pid),'boot_id':m.work['host']['boot_id']}
        need(witness['start_ticks'] is not None,'maintenance_producer_identity')
        channel.sendall(b'go\n'); channel.shutdown(socket.SHUT_WR)
        while True:
            owned.capture(); owned.reap()
            if leader.returncode is not None and any(v.pid!=pid for v in owned.living()):unexpected=True
            if not stopping and (interrupted or unexpected or time.monotonic()>=m.b._driver_deadline):
                stopping=True
                # Never SIGKILL away the producer's daemon-owned native actor
                # obligations. Retain this parent until actual closure instead.
                for entry in owned.entries.values():
                    if entry.pid==pid:entry.send(signal.SIGTERM)
            if not ended and select.select([channel],[],[],.02)[0]:
                data=channel.recv(32768); raw.extend(data); ended=not data
                need(len(raw)<=MAX_HANDOFF,'maintenance_live_handoff_bound')
            if owned.closed() and ended:break
            if ended:time.sleep(.01)
        exitrow=dict(witness,actual_exit_code=leader.returncode,remaining_children=[])
        m.b._initial_provider_maintenance_last_exit=exitrow
        m.b._initial_provider_maintenance_failure_index=bytes(raw)
        _reconcile(m,decode(bytes(raw)))
        need(type(leader.returncode) is int and leader.returncode==0 and not interrupted
             and not stopping and not unexpected,'maintenance_producer_not_successfully_closed')
        return exitrow
    except BaseException:
        if leader is not None and leader.pid is not None:
            if channel is not None:
                try:channel.shutdown(socket.SHUT_WR)
                except OSError:pass
                try:channel.close()
                except OSError:pass
                channel=None
            _hold_producer(leader,owned,terminate=True)
        if leader is not None and leader.pid is not None:
            m.b._initial_provider_maintenance_last_exit={'pid':leader.pid,
                'start_ticks':locals().get('witness',{}).get('start_ticks'),
                'boot_id':m.work['host']['boot_id'],'actual_exit_code':leader.returncode,'remaining_children':[]}
        m.b._initial_provider_maintenance_failure_index=bytes(raw)
        raise
    finally:
        if leader is not None and leader.pid is not None:_hold_producer(leader,owned,terminate=False)
        if owned is not None:owned.close_fds()
        for sock in (channel,other):
            if sock is not None:sock.close()
        if parentfd is not None:os.close(parentfd)
        for number,handler in previous.items():signal.signal(number,handler)
        if oldsub is not None:processes._subreaper(oldsub)
        _SERIAL.release()


def run(backend, shared_lock, inventory_ref, authority_ref):
    """Execute once after live RolePrep; retain all evidence on failure.

    The actual parent-owned producer exit is written only after waitpid and
    descendant closure. The final result is then BOTH-journal ACKed in this
    phase, and the parent freshly rechecks global SQL closure before return.
    """
    _dedicated_root(backend, shared_lock)
    _intake_run_gate(backend, shared_lock, inventory_ref, authority_ref)
    driver._check_end(backend); loaded=_load(inventory_ref,authority_ref)
    live=_live(backend,shared_lock,loaded[0],loaded[6])
    backend._initial_provider_maintenance_started=True
    m=_Maintenance(backend,shared_lock,inventory_ref,authority_ref,loaded,live)
    backend._initial_provider_maintenance_live=m; m.j.begin(PHASE)
    try:
        m.save('input-binding',{'inventory':inventory_ref,'authority':authority_ref,
            'role_prepare':m.cfg['role_prepare'],'original_absolute_deadline':backend._driver_deadline,
            'maintenance_only':True,'strict_ack':False,'automatic_retry':False})
        exitrow=_fork(m)
        # Parent never opens the child's mutable control directory as a success
        # capability. The native actor results and raw dual phase are reconciled.
        matches=[name for name in m.j.artifacts if name.endswith('-pending-parent-exit.json')]
        need(len(matches)==1,'maintenance_pending_result_missing')
        pending=decode(m.j.artifacts[matches[0]]['raw'])
        _shape(pending,('provider_recovery_operation','plan','apply','readback','audit','native_command',
            'sql_closure','new_uncertainty_ids'),'maintenance_pending_result')
        m.sessions(); closure=m.save('parent-sql-closure',{'sessions':[]})
        result={'schema_version':1,'kind':'openlegal_initial_provider_maintenance_result',
            'operation_id':m.cfg['operation_id'],'provider_recovery_operation':pending['provider_recovery_operation'],
            'driver_inventory_sha256':m.cfg['driver_inventory_sha256'],'inventory':inventory_ref,
            'authority':authority_ref,'authority_sha256':digest(m.authraw),
            'source_graph_sha256':digest(canonical(m.sc[4]['source_graph'])),
            'producer_source':{'path':str(Path(__file__).resolve()),'sha256':m.cfg['source_sha256']},
            'producer_exit':exitrow,**{k:v for k,v in pending.items() if k!='sql_closure'},'sql_closure':closure}
        ref=m.save('complete',result); m.seal(); m.sessions()
    finally:
        _retain_sql_closed(m)
    # The finally block is part of completion, not a post-success advisory.
    # Neither a saved complete-shaped JSON nor a reaped child alone mints this.
    m.seal()
    raw=canonical(result); _completed_result(m,ref,raw)
    m.seal(); driver._check_end(backend)
    cap=_Completed(m,tuple(ref[k] for k in ('phase','name','sha256')),raw,
        canonical(m.parent),m.original_deadline,m.original_clock,
        tuple(m.budget[k] for k in ('start_count','start_bytes','records','bytes')),
        m.j.count,m.j.bytes)
    m._completed=cap; backend._initial_provider_maintenance_complete=cap
    return ref


@dataclass(frozen=True, eq=False, slots=True)
class _Completed:
    owner: object
    reference: tuple
    raw: bytes
    parent: bytes
    end: float
    clock: object
    budget: tuple
    count: int
    bytes: int


def _completed_result(m, reference, raw):
    need(m.sql_uncertain is False and m.j.phase_paths
         and m.j.phase_paths[0][0].name == PHASE, 'maintenance_completion_not_closed')
    need(type(reference) is dict and set(reference) == {'phase','name','sha256'}
         and reference['phase'] == PHASE and _raw(m.j,reference) == decode(raw)
         and m.j.read_phase_bytes(PHASE,reference['name'],reference['sha256']) == raw,
         'maintenance_completion_raw')
    result=decode(raw)
    _shape(result,('schema_version','kind','operation_id','provider_recovery_operation',
        'driver_inventory_sha256','inventory','authority','authority_sha256','source_graph_sha256',
        'producer_source','producer_exit','plan','apply','readback','audit','native_command',
        'sql_closure','new_uncertainty_ids'),'maintenance_completion_result')
    need(type(result['schema_version']) is int and result['schema_version']==1
         and result['kind']=='openlegal_initial_provider_maintenance_result'
         and result['operation_id']==m.cfg['operation_id'] and result['inventory']==m.iref
         and result['authority']==m.aref and result['authority_sha256']==digest(m.authraw)
         and result['driver_inventory_sha256']==m.cfg['driver_inventory_sha256']
         and result['source_graph_sha256']==digest(canonical(m.sc[4]['source_graph']))
         and result['producer_source']=={'path':str(Path(__file__).resolve()),'sha256':m.cfg['source_sha256']},
         'maintenance_completion_binding')
    row=result['producer_exit']
    _shape(row,('pid','start_ticks','boot_id','actual_exit_code','remaining_children'),
        'maintenance_completion_exit')
    need(type(row['pid']) is int and row['pid']>0 and type(row['start_ticks']) is str
         and row['start_ticks'].isdecimal() and row['boot_id']==m.parent['boot_id']
         and type(row['actual_exit_code']) is int and row['actual_exit_code']==0
         and row['remaining_children']==[]
         and canonical(row)==canonical(getattr(m.b,'_initial_provider_maintenance_last_exit',None)),
         'maintenance_completion_parent_exit')
    manual._process_absent({k:row[k] for k in ('pid','start_ticks','boot_id')})
    refs={}
    for key in ('plan','apply','readback','audit','native_command','sql_closure'):
        need(result[key]['phase']==PHASE,'maintenance_completion_phase_ref')
        refs[key]=_raw(m.j,result[key])
    _command_ok(refs['native_command'])
    need(refs['sql_closure']=={'sessions':[]} and refs['readback']==refs['apply']
         and refs['audit']['plan']==refs['plan'] and refs['audit']['result']==refs['apply']
         and refs['audit']['before_state']==refs['plan']['snapshot']['state']
         and refs['audit']['operation_id']==refs['plan']['operation_id']==refs['apply']['operation_id']
             ==result['provider_recovery_operation']
         and refs['plan']['request']['resume'] is False
         and refs['plan']['request']['resolve_legacy'] is False and refs['plan']['request']['waits']==[],
         'maintenance_completion_audit')
    return result


def completed_reference(backend, shared_lock):
    """Return this owner's actual final reference; historical DATA is insufficient."""
    cap=getattr(backend,'_initial_provider_maintenance_complete',None)
    need(type(cap) is _Completed, 'maintenance_completion_live_field')
    ref=dict(zip(('phase','name','sha256'),cap.reference))
    consume_completed_live(backend,shared_lock,ref)
    return ref


def consume_completed_live(backend, shared_lock, reference):
    """Read only: retain final native accounting and the original ROOT context."""
    _root_context(backend,shared_lock)
    cap=getattr(backend,'_initial_provider_maintenance_complete',None)
    need(type(cap) is _Completed and type(cap.owner) is _Maintenance,
         'maintenance_completion_live_field')
    m=cap.owner
    need(m is getattr(backend,'_initial_provider_maintenance_live',None) and m._completed is cap
         and m.b is backend and m.lock is shared_lock
         and m.j is getattr(backend,'_initial_driver_journal',None)
         and getattr(backend,'_initial_provider_maintenance_started',False) is True
         and _intake_parent()==m.parent and canonical(m.parent)==cap.parent
         and m.original_deadline==cap.end==backend._driver_deadline
         and m.original_clock is cap.clock is getattr(backend,'_initial_driver_clock',None),
         'maintenance_completion_same_owner')
    need(type(reference) is dict and reference==dict(zip(('phase','name','sha256'),cap.reference)),
         'maintenance_completion_reference')
    need(tuple(m.budget[k] for k in ('start_count','start_bytes','records','bytes'))==cap.budget
         and cap.budget[2:]==(1024,64*1024**2)
         and cap.budget[0]<=cap.count<=cap.budget[0]+cap.budget[2]
         and cap.budget[1]<=cap.bytes<=cap.budget[1]+cap.budget[3]
         and cap.count<=m.j.count<=4096 and cap.bytes<=m.j.bytes<=journals.MAX_PHASE_BYTES,
         'maintenance_completion_frozen_accounting')
    m.seal_context()
    result=_completed_result(m,reference,cap.raw)
    # Recheck the original deadline after the last raw-media/identity read.
    m.seal_context(); driver._check_end(backend)
    return result
