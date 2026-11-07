"""One held initial PostgreSQL pair; no provisioning, release, CLI or Ready.

Only the live initial driver may call run. All targets and maintenance authority
are required preprovisioned DATA, independently checked again before effects.
The plain SSD profile observes a copy budget and 20 percent of TOTAL filesystem
space as a free-space floor. This is NOT a quota, reservation or peak guarantee.
An optional preprovisioned reserved-ZFS profile is distinct. Neither profile
creates a pool, filesystem, dataset, quota, role, network or trust.
Native qualification and the future maintenance producer are separate gates.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
import hashlib
import ipaddress
import math
import os
from pathlib import Path
import re
import select
import signal
import socket
import ssl
import stat
import struct
import sys
import time
import threading
import types
import uuid

import bootstrap_backend as blobs
import initial_blob_seed as seed
import initial_driver as driver
import initial_migration as migration
import initial_role_prepare as role_prepare
import journal as journals
import legacy_prepare as legacy
import linux_backend as linux
import manual_backend as manual
import preservation_backend as preservation
from copy_tree import root_fd, signature
from interface import Operation, Refused, freeze_authority, need
from inventory import (MAX_INPUT, canonical, decode, digest, pin, private_bytes, private_parents,
    private_fd_mount_id, recheck_private_inbox, read_private_file_at, valid_uuid)

PHASE = 'initial_pg_pair'
MAINTENANCE = 'initial_provider_maintenance'
PGDATA = '/var/lib/postgresql'
TRUST = '/pair-trust'
CONTROL = '/pair-control'
BUNDLE = 8
FIXED_RECORDS = 1024
FIXED_BYTES = 64 * 1024**2
TOOLS = ('postgres', 'pg_basebackup', 'pg_verifybackup', 'pg_waldump', 'pg_controldata', 'psql', 'pg_isready')
BIND_KEYS = frozenset(('managed_epoch', 'primary_system_identifier', 'timeline', 'standby_name',
    'standby_role', 'standby_addr', 'standby_client_dn', 'standby_issuer_dn',
    'standby_client_serial', 'blob_binding_sha256'))
FUNCTIONS = ('openlegal_checkpoint.bind_replica(uuid,text,jsonb,jsonb)',
    'openlegal_replication.observe_candidate(jsonb)', 'openlegal_replication.capture_prefix()',
    'openlegal_replication.observe_flush()', 'openlegal_replication.replication_required()',
    'openlegal_checkpoint.database_position()', 'openlegal_checkpoint.database_layout()',
    'openlegal_checkpoint.client_sessions()', 'openlegal_checkpoint.observe(uuid)', 'openlegal_checkpoint.state()',
    'openlegal_checkpoint.retained_references()', 'openlegal_checkpoint.preservation_page(text,jsonb,integer)')
# replication_required lives in openlegal, unlike the observers.
FUNCTIONS = tuple(s.replace('openlegal_replication.replication_required', 'openlegal.replication_required') for s in FUNCTIONS)
UNSET = ('PGSERVICE', 'PGSERVICEFILE', 'PGPASSWORD', 'PGHOST', 'PGHOSTADDR', 'PGPORT', 'PGDATABASE',
    'PGUSER', 'PGOPTIONS', 'PGSSLCERT', 'PGSSLKEY', 'PGSSLROOTCERT', 'PGSSLCRL', 'PGSSLCRLDIR',
    'PGTARGETSESSIONATTRS', 'PGCONNECT_TIMEOUT', 'PGREQUIREPEER', 'PGCHANNELBINDING',
    'PGSSLMODE', 'PGREQUIRESSL', 'PGGSSENCMODE', 'PGCLIENTENCODING', 'PGAPPNAME')
INSPECT = ('{"Id":{{json .Id}},"Image":{{json .Image}},"State":{"Running":{{json .State.Running}},'
    '"Paused":{{json .State.Paused}},"Restarting":{{json .State.Restarting}},"Dead":{{json .State.Dead}},'
    '"OOMKilled":{{json .State.OOMKilled}},"Pid":{{json .State.Pid}},"ExitCode":{{json .State.ExitCode}}},'
    '"Config":{"User":{{json .Config.User}},"Labels":{{json .Config.Labels}},'
    '"Entrypoint":{{json .Config.Entrypoint}},"Cmd":{{json .Config.Cmd}}},'
    '"HostConfig":{"NetworkMode":{{json .HostConfig.NetworkMode}},"ReadonlyRootfs":{{json .HostConfig.ReadonlyRootfs}},'
    '"RestartPolicy":{{json .HostConfig.RestartPolicy}},"CapDrop":{{json .HostConfig.CapDrop}},'
    '"Memory":{{json .HostConfig.Memory}},"MemorySwap":{{json .HostConfig.MemorySwap}},'
    '"PidsLimit":{{json .HostConfig.PidsLimit}},"Tmpfs":{{json .HostConfig.Tmpfs}},'
    '"SecurityOpt":{{json .HostConfig.SecurityOpt}}},"Mounts":{{json .Mounts}},'
    '"NetworkSettings":{"Networks":{{json .NetworkSettings.Networks}}}}')


def _shape(value, fields, code):
    need(type(value) is dict and set(value) == set(fields), code)


def _name(value):
    need(type(value) is str and re.fullmatch('[a-z_][a-z0-9_]{0,62}', value), 'pair_named_role')
    return value


def _quote(value):
    need(type(value) is str and '\x00' not in value and len(value) <= 1024**2, 'pair_sql_literal')
    return "'" + value.replace("'", "''") + "'"


def _check(backend, end=None):
    driver._check_end(backend)
    need(end is None or time.monotonic() < end, 'pair_original_deadline')


def _lsn(value):
    need(type(value) is str and re.fullmatch('[0-9A-F]{1,8}/[0-9A-F]{1,8}', value), 'pair_lsn')
    a, b = value.split('/'); return (int(a, 16) << 32) + int(b, 16)


def _binding(value):
    _shape(value, BIND_KEYS, 'pair_binding_fields')
    need(valid_uuid(value['managed_epoch']) and pin(value['blob_binding_sha256'])
         and type(value['primary_system_identifier']) is str and value['primary_system_identifier'].isdecimal()
         and 0 < int(value['primary_system_identifier']) < 2**64
         and type(value['timeline']) is int and 0 < value['timeline'] < 2**32, 'pair_binding_identity')
    _name(value['standby_name']); _name(value['standby_role'])
    need(re.fullmatch('[a-z][a-z0-9_]{0,62}', value['standby_name']), 'pair_standby_name')
    ipaddress.ip_address(value['standby_addr'])
    need(all(type(value[k]) is str and 1 <= len(value[k]) <= 1024 and '\x00' not in value[k]
             for k in ('standby_client_dn', 'standby_issuer_dn'))
         and type(value['standby_client_serial']) is str and re.fullmatch('[1-9][0-9]{0,59}', value['standby_client_serial']),
         'pair_binding_certificate')
    return value


def _peer(value, expected):
    """Normalize only PostgreSQL numeric/inet rendering, never rename fields."""
    value = copy.deepcopy(value)
    need(type(value) is dict, 'pair_peer_shape')
    basic=(BIND_KEYS-{'managed_epoch','blob_binding_sha256'}) | {'postmaster_started_at','sender_pid','sender_started_at','prefix_lsn','flush_lsn'}
    need(set(value) in (basic,basic|{'managed_epoch','blob_binding_sha256'}), 'pair_peer_closed_fields')
    for k, v in expected.items():
        if k in ('managed_epoch', 'blob_binding_sha256'): continue
        actual = value.get(k)
        if k == 'standby_client_serial':
            need(type(actual) in (str, int) and type(actual) is not bool and str(actual).isdecimal(), 'pair_peer_serial')
            need(int(actual) == int(v), 'pair_peer_certificate')
        elif k == 'standby_addr':
            need(type(actual) is str and _host_address(actual) == str(ipaddress.ip_address(v)), 'pair_peer_address')
        else: need(type(actual) is type(v) and actual == v, 'pair_peer_identity')
    need(type(value.get('sender_pid')) is int and value['sender_pid'] > 0
         and type(value.get('sender_started_at')) is str and value['sender_started_at']
         and type(value.get('postmaster_started_at')) is str and value['postmaster_started_at'], 'pair_peer_lifetime')
    _lsn(value['prefix_lsn']); _lsn(value['flush_lsn'])
    for key in ('managed_epoch','blob_binding_sha256'):
        if key in value:need(value[key]==expected[key], 'pair_peer_control_changed')
    return value


def _proof(before, after, expected, *, previous=None):
    # M28 observes exact static lifetime; volatile flush may advance.
    need(all(before.get(k)==after.get(k)==expected[k] for k in ('managed_epoch','blob_binding_sha256')), 'pair_ack_control_missing')
    a = _peer(before, expected); b = _peer(after, expected)
    static = set(a) - {'prefix_lsn', 'flush_lsn'}
    need(static == set(b) - {'prefix_lsn', 'flush_lsn'} and all(a[k] == b[k] for k in static), 'pair_ack_lifetime_changed')
    need(_lsn(b['flush_lsn']) >= _lsn(a['prefix_lsn']), 'pair_ack_not_durable')
    if previous is not None:
        p = _peer(previous, expected)
        need(all(p[k] == a[k] for k in static if k not in ('managed_epoch', 'blob_binding_sha256'))
             and _lsn(a['prefix_lsn']) >= _lsn(p['prefix_lsn']), 'pair_ack_precommit_changed')
    return {'before': before, 'after': after, 'fixed_target': a['prefix_lsn']}


def _reserve(journal, bundles, raw_bytes, legacy_tables=0):
    """GLOBAL bounds; no reset, new journal, or capacity increase is permitted.

    Baseline has already been ACKed. Three further byte-identical metadata scans
    may differ only in fixed envelope/identity decimal widths (+4096/bundle).
    Every future bundle gets ONE raw ACK and ONE actual command receipt; all
    other commands, intents, audits, failures and final indices fit fixed reserve.
    The sealed phase also enforces this reservation on every dispatch/save.
    """
    need(type(journal.count) is int and type(journal.bytes) is int
         and type(bundles) is int and bundles >= 1 and type(raw_bytes) is int and raw_bytes >= 0,
         'pair_journal_accounting')
    need(type(legacy_tables) is int and 0 <= legacy_tables <= 256, 'pair_old_table_count')
    count = FIXED_RECORDS + 8 * legacy_tables + 6 * bundles
    amount = FIXED_BYTES + 3 * (raw_bytes + 4096 * bundles)
    need(journal.count + count <= 4096 and journal.bytes + amount <= journals.MAX_PHASE_BYTES,
         'pair_whole_operation_journal_budget')
    return {'start_count': journal.count, 'start_bytes': journal.bytes, 'records': count, 'bytes': amount,
            'm32_future_scans': 3, 'baseline_bundles': bundles, 'baseline_raw_bytes': raw_bytes}


def _load_inventory(inventory_ref):
    raw = driver._reference(inventory_ref); cfg = decode(raw)
    _shape(cfg, ('schema_version', 'kind', 'operation_id', 'source_sha256', 'seed_inventory', 'seed_authority',
                 'maintenance', 'provision'), 'pair_inventory')
    need(type(cfg['schema_version']) is int and cfg['schema_version'] == 1
         and cfg['kind'] == 'openlegal_initial_pg_pair_inventory' and valid_uuid(cfg['operation_id'])
         and pin(cfg['source_sha256']), 'pair_inventory_identity')
    private_bytes(Path(__file__).resolve(), cfg['source_sha256'])
    sc = seed._load(cfg['seed_inventory'], cfg['seed_authority'])
    need(cfg['operation_id'] == sc[0]['operation_id'] and sc[0]['media_profile'] == blobs.MEDIA_PROFILE,
         'pair_seed_identity')
    provision_raw = driver._reference(cfg['provision']); provision = decode(provision_raw)
    _provision(provision, cfg['operation_id'])
    return cfg, raw, provision, provision_raw, sc


_AUTHORITY_TRUE = ('create_pair_permitted', 'initial_bind_permitted', 'preserve_originals')
_AUTHORITY_FALSE = ('automatic_retry', 'provisioning_permitted', 'provider_permitted',
    'release_permitted', 'ready_permitted', 'cleanup_permitted', 'promotion_permitted')


def _requested_scope(loaded):
    """Restriction DATA only; ROOT supplies neither approval nor expiry."""
    cfg, raw, _, provision_raw, _ = loaded
    return {'schema_version': 1, 'kind': 'openlegal_initial_pg_pair_authority',
        'operation_id': cfg['operation_id'], 'inventory_sha256': digest(raw),
        'source_sha256': cfg['source_sha256'], 'provision_sha256': digest(provision_raw),
        'seed_inventory_sha256': cfg['seed_inventory']['sha256'],
        'seed_authority_sha256': cfg['seed_authority']['sha256'],
        **dict.fromkeys(_AUTHORITY_TRUE, True), **dict.fromkeys(_AUTHORITY_FALSE, False)}


def _load(inventory_ref, authority_ref):
    loaded = _load_inventory(inventory_ref)
    cfg, raw, provision, provision_raw, sc = loaded
    authraw = driver._reference(authority_ref); auth = decode(authraw)
    scope = _requested_scope(loaded)
    _shape(auth, (*scope, 'approved', 'valid_until'), 'pair_authority')
    need(type(auth['schema_version']) is int and auth['approved'] is True
         and type(auth['valid_until']) is int and time.time() < auth['valid_until']
         and {k: auth[k] for k in scope} == scope
         and all(auth[k] is True for k in _AUTHORITY_TRUE)
         and all(auth[k] is False for k in _AUTHORITY_FALSE), 'pair_authority_scope')
    return cfg, auth, raw, authraw, provision, provision_raw, sc


# Stage-local DATA intake shares only filesystem witnesses with the earlier stage.
INTAKE_REQUEST_MAX = 64 * 1024
INTAKE_ENVELOPE_MAX = 16 * 1024
INTAKE_CONTROL_MAX = 4096
INTAKE_RECORDS = 10
INTAKE_BYTES = 4 * MAX_INPUT + 2 * INTAKE_REQUEST_MAX + INTAKE_ENVELOPE_MAX + 3 * INTAKE_CONTROL_MAX
INTAKE_ENVELOPE = 'pg-pair-capability.json'
INTAKE_LABEL = 'pg-pair-intake'


class _Intake:
    """One live ROOT-owned Pair request, never serialized execution authority."""
    def __init__(self, backend, lock):
        self.b = backend; self.lock = lock; self.state = 'PREPARING'
        self.j = None; self.budget = None; self.inbox_fd = None; self.finished = None
        self.entered_run = False


def _dedicated_root(backend, lock):
    need(type(backend) is driver.Backend and type(lock) is journals.SharedLock
         and os.geteuid() == 0 and threading.current_thread() is threading.main_thread()
         and len(list(Path('/proc/self/task').iterdir())) == 1
         and not driver.processes._children(os.getpid())
         and not any(callable(v) for v in vars(backend).values())
         and not getattr(backend, '_initial_pg_pair_started', False), 'pair_intake_dedicated_root')


def _intake_parent():
    pid = os.getpid(); ticks = driver.processes._identity(pid)
    need(type(ticks) is str and ticks.isdecimal(), 'pair_intake_parent')
    return {'pid': pid, 'start_ticks': ticks,
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}


def _intake_fail(s):
    if type(s) is _Intake:
        s.state = 'UNKNOWN'
        if s.budget is not None and s.finished is None:
            try: s.j.fail_reservation(s.budget)
            except BaseException: pass


def _intake_context(s):
    need(type(s) is _Intake and getattr(s.b, '_initial_pg_pair_intake', None) is s
         and s.state in ('PREPARING', 'WAITING', 'ACCEPTING', 'DISPATCHING'),
         'pair_intake_live_instance')
    _dedicated_root(s.b, s.lock); _check(s.b)
    need(_intake_parent() == s.parent and s.clock is s.b._initial_driver_clock
         and type(s.clock) is driver._Clock and s.clock.backend is s.b
         and s.clock.shared_lock is s.lock and s.clock.lock_fd == s.lock.fd
         and s.b._initial_driver_clock_attempted is True
         and s.b._driver_deadline == s.original_end == s.clock.end,
         'pair_intake_original_context')
    need(s.j is s.b._initial_driver_journal and s.j.operation == s.operation_id
         and s.j.phase_paths and s.j.phase_paths[0][0].name == MAINTENANCE,
         'pair_intake_original_phase')
    s.lock.check(); _check(s.b)


def _maintenance_owner(backend):
    # The fixed owner must already be loaded. Never execute a module selected
    # by capability DATA or reopen authenticated source as a second import.
    owner = sys.modules.get('initial_provider_maintenance')
    fixed = Path(__file__).resolve().parent / 'initial_provider_maintenance.py'
    need(type(owner) is types.ModuleType and getattr(owner, '__file__', None) == str(fixed)
         and Path(owner.__file__).resolve() == fixed, 'pair_intake_loaded_maintenance_owner')
    live = getattr(backend, '_initial_provider_maintenance_live', None)
    need(live is not None and live.b is backend and type(live.cfg) is dict
         and pin(live.cfg.get('source_sha256')), 'pair_intake_live_maintenance_owner')
    private_bytes(fixed, live.cfg['source_sha256'])
    return owner


def _intake_record(s, suffix, raw, bound):
    _intake_context(s)
    need(type(raw) is bytes and len(raw) <= bound, 'pair_intake_record_bound')
    name = 'pair-intake-' + suffix + '.json'
    sha = s.j.save_bytes(name, raw, reservation=s.budget)
    _intake_context(s)
    ref = {'phase': MAINTENANCE, 'name': name, 'sha256': sha}
    need(s.j.read_phase_bytes(ref['phase'], name, sha) == raw, 'pair_intake_dual_ack')
    _intake_context(s)
    return {'path': str(s.j.phase_paths[0][0] / name), 'sha256': sha}, ref


def _intake_inbox(s):
    _intake_context(s)
    recheck_private_inbox(s.inbox['path'], s.inbox['identity'], s.inbox_fd, s.inbox_mount)
    _intake_context(s)


def _intake_read_envelope(s, ref, expected=None):
    _intake_context(s)
    need(driver._clock_reference(ref)[0] == str(Path(s.inbox['path']) / INTAKE_ENVELOPE),
         'pair_intake_envelope_path')
    _intake_inbox(s)
    raw, witness = read_private_file_at(s.inbox_fd, INTAKE_ENVELOPE, ref['sha256'],
        INTAKE_ENVELOPE_MAX, mount_id=s.inbox_mount, previous=expected)
    _intake_context(s); _intake_inbox(s)
    return raw, witness


def _intake_seal(s):
    _intake_context(s)
    s.j.check(); _intake_context(s); _intake_inbox(s)
    for ref, raw in s.input_bytes:
        need(driver._reference(ref) == raw, 'pair_intake_input_changed')
        _intake_context(s)
    loaded = _load_inventory(s.inventory_ref)
    _intake_context(s)
    need(loaded == s.loaded and _requested_scope(loaded) == s.scope, 'pair_intake_candidate_changed')
    owner = _maintenance_owner(s.b)
    _intake_context(s)
    need(owner is s.owner, 'pair_intake_maintenance_owner_changed')
    reference = owner.completed_reference(s.b, s.lock)
    _intake_context(s)
    need(reference == s.cfg['maintenance'], 'pair_intake_completion_reference_changed')
    result = owner.consume_completed_live(s.b, s.lock, reference)
    _intake_context(s)
    need(canonical(result) == s.maintenance_raw, 'pair_intake_completion_changed')
    live = _live(s.b, s.lock, loaded[0], loaded[4])
    _intake_context(s)
    need(live[0] is s.j and canonical(live[1:]) == s.live_raw, 'pair_intake_live_changed')
    maintenance, audit = _maintenance(s.j, reference, s.cfg, loaded[4][4], loaded[4][8], loaded[4][9],
        getattr(s.b, '_initial_provider_maintenance_last_exit', None), loaded[4][7])
    _intake_context(s)
    need(canonical(maintenance) == s.maintenance_raw and canonical(audit) == s.audit_raw,
         'pair_intake_maintenance_proof_changed')
    role = _qualified_role_context(s.b, s.lock, loaded[2], maintenance)
    _intake_context(s)
    need(role == s.role_context, 'pair_intake_role_changed')
    need(s.j.read_phase_bytes(s.request_ref['phase'], s.request_ref['name'], s.request_ref['sha256'])
         == s.request_raw, 'pair_intake_request_changed')
    _intake_context(s)
    if hasattr(s, 'authority_raw'):
        approved = _load(s.inventory_ref, s.authority_ref)
        _intake_context(s)
        need(approved == (s.loaded[0], decode(s.authority_raw), s.loaded[1], s.authority_raw,
                         s.loaded[2], s.loaded[3], s.loaded[4]), 'pair_intake_approval_changed')


def prepare_pair_request(backend, shared_lock, *, provision_ref, source_sha256, inbox):
    """Archive unapproved Pair candidate DATA after actual live maintenance.

    The source-owned completion and original ROOT/clock/lock remain mandatory.
    No native actor, authority, new phase, journal or deadline is created.
    """
    _dedicated_root(backend, shared_lock)
    if hasattr(backend, '_initial_pg_pair_intake'):
        _intake_fail(backend._initial_pg_pair_intake)
        raise Refused('pair_intake_no_replay')
    s = _Intake(backend, shared_lock); backend._initial_pg_pair_intake = s
    try:
        s.parent = _intake_parent(); s.clock = backend._initial_driver_clock
        s.original_end = backend._driver_deadline; s.j = backend._initial_driver_journal
        need(type(s.j) is journals.DualJournal, 'pair_intake_live_journal')
        s.operation_id = s.j.operation
        _intake_context(s)
        s.owner = _maintenance_owner(backend)
        _intake_context(s)
        reference = s.owner.completed_reference(backend, shared_lock)
        _intake_context(s)
        result = s.owner.consume_completed_live(backend, shared_lock, reference)
        _intake_context(s)
        need(result['operation_id'] == s.operation_id, 'pair_intake_same_operation')
        mraw = driver._reference(result['inventory']); mc = decode(mraw)
        _intake_context(s)
        maraw = driver._reference(result['authority'])
        _intake_context(s)
        need(result['producer_source'] == {'path': str(Path(s.owner.__file__)),
             'sha256': mc['source_sha256']}, 'pair_intake_maintenance_source')
        s.budget = s.j.reserve(INTAKE_LABEL, INTAKE_RECORDS, INTAKE_BYTES)
        _intake_context(s)
        _shape(inbox, ('path', 'identity'), 'pair_intake_inbox')
        _shape(inbox['identity'], ('dev', 'ino'), 'pair_intake_inbox_identity')
        need(type(inbox['path']) is str and Path(inbox['path']).is_absolute()
             and str(Path(inbox['path'])) == inbox['path'] and '..' not in Path(inbox['path']).parts
             and all(type(v) is int and v >= 0 for v in inbox['identity'].values()),
             'pair_intake_inbox_identity')
        s.inbox = copy.deepcopy(inbox)
        need(not any(Path(s.inbox['path']).is_relative_to(path) for path, _ in s.j.paths),
             'pair_intake_inbox_inside_journal')
        journals.directory(s.inbox['path'], s.inbox['identity'])
        _intake_context(s)
        s.inbox_fd = os.open(s.inbox['path'], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        _intake_context(s)
        s.inbox_mount = private_fd_mount_id(s.inbox_fd)
        _intake_context(s); _intake_inbox(s)
        need(pin(source_sha256), 'pair_intake_source')
        private_bytes(Path(__file__).resolve(), source_sha256)
        _intake_context(s)
        driver._clock_reference(provision_ref)
        praw = driver._reference(provision_ref)
        _intake_context(s)
        _provision(decode(praw), s.operation_id)
        _intake_context(s)
        s.input_bytes = [(copy.deepcopy(provision_ref), praw), (copy.deepcopy(result['inventory']), mraw),
                         (copy.deepcopy(result['authority']), maraw)]
        provision, _ = _intake_record(s, 'provision', praw, MAX_INPUT)
        cfg = {'schema_version': 1, 'kind': 'openlegal_initial_pg_pair_inventory',
            'operation_id': s.operation_id, 'source_sha256': source_sha256,
            'seed_inventory': copy.deepcopy(mc['seed_inventory']),
            'seed_authority': copy.deepcopy(mc['seed_authority']),
            'maintenance': copy.deepcopy(reference), 'provision': provision}
        s.inventory_ref, _ = _intake_record(s, 'candidate', canonical(cfg), MAX_INPUT)
        s.loaded = _load_inventory(s.inventory_ref)
        _intake_context(s)
        s.cfg = s.loaded[0]; s.scope = _requested_scope(s.loaded)
        live = _live(backend, shared_lock, s.cfg, s.loaded[4])
        _intake_context(s)
        need(live[0] is s.j, 'pair_intake_same_journal'); s.live_raw = canonical(live[1:])
        maintenance, audit = _maintenance(s.j, reference, s.cfg, s.loaded[4][4], s.loaded[4][8], s.loaded[4][9],
            getattr(backend, '_initial_provider_maintenance_last_exit', None), s.loaded[4][7])
        _intake_context(s)
        need(maintenance == result, 'pair_intake_completion_proof_mismatch')
        s.maintenance_raw = canonical(maintenance); s.audit_raw = canonical(audit)
        role = _qualified_role_context(backend, shared_lock, s.loaded[2], maintenance)
        _intake_context(s)
        s.role_context = copy.deepcopy(role); s.nonce = str(uuid.uuid4())
        graph = dict(s.loaded[4][4]['source_graph'], **{'initial_blob_seed.py': s.loaded[4][0]['source_sha256'],
            'initial_role_prepare.py': role['producer_source']['sha256'],
            'initial_provider_maintenance.py': result['producer_source']['sha256'], 'initial_pg_pair.py': source_sha256})
        value = {'schema_version': 1, 'kind': 'openlegal_initial_pg_pair_stage_request',
            'operation_id': s.operation_id, 'stage': 'pg_pair', 'stage_nonce': s.nonce,
            'root': s.parent, 'original_absolute_deadline': s.original_end,
            'source_profile': 'initial_pg_pair_intake_v1', 'source_graph_sha256': digest(canonical(graph)),
            'previous_result': s.cfg['maintenance'], 'candidate_inventory': s.inventory_ref,
            'requested_authority_scope': s.scope, 'authority_scope_sha256': digest(canonical(s.scope)),
            'status': 'UNAPPROVED_CANDIDATE_DATA', 'ready': False}
        s.request_raw = canonical(value)
        _, s.request_ref = _intake_record(s, 'request', s.request_raw, INTAKE_REQUEST_MAX)
        s.state = 'WAITING'; _intake_seal(s)
        return s, s.request_raw
    except BaseException:
        _intake_fail(s); raise


def execute_approved_pair(intake, envelope_ref):
    """Consume one external capability and dispatch only the fixed Pair run."""
    need(type(intake) is _Intake and getattr(intake.b, '_initial_pg_pair_intake', None) is intake
         and intake.state == 'WAITING', 'pair_intake_one_use')
    s = intake; s.state = 'ACCEPTING'
    try:
        _intake_seal(s)
        raw, witness = _intake_read_envelope(s, envelope_ref); value = decode(raw)
        _shape(value, ('schema_version', 'kind', 'operation_id', 'stage', 'stage_nonce', 'request_sha256',
                       'inventory', 'authority'), 'pair_intake_envelope')
        need(canonical(value) == raw and type(value['schema_version']) is int and value['schema_version'] == 1
             and value['kind'] == 'openlegal_initial_stage_capability_envelope'
             and value['operation_id'] == s.operation_id and value['stage'] == 'pg_pair'
             and value['stage_nonce'] == s.nonce and value['request_sha256'] == digest(s.request_raw)
             and value['inventory'] == s.inventory_ref, 'pair_intake_envelope_binding')
        s.authority_ref = copy.deepcopy(value['authority']); driver._clock_reference(s.authority_ref)
        s.envelope_ref = copy.deepcopy(envelope_ref); s.envelope_raw = raw; s.envelope_identity = witness
        _intake_record(s, 'envelope', raw, INTAKE_ENVELOPE_MAX)
        _intake_record(s, 'consumed', canonical({'operation_id': s.operation_id, 'stage_nonce': s.nonce,
            'request_sha256': digest(s.request_raw), 'envelope_sha256': digest(raw),
            'status': 'ATTEMPT_CONSUMED_NOT_APPROVAL'}), INTAKE_CONTROL_MAX)
        loaded = _load(s.inventory_ref, s.authority_ref)
        _intake_context(s)
        need(loaded[0] == s.cfg and {k: loaded[1][k] for k in s.scope} == s.scope,
             'pair_intake_approval_scope')
        s.authority_raw = loaded[3]
        _intake_record(s, 'authority', s.authority_raw, MAX_INPUT)
        _, s.accepted_ref = _intake_record(s, 'accepted', canonical({'operation_id': s.operation_id,
            'stage_nonce': s.nonce, 'request': s.request_ref, 'inventory': s.inventory_ref,
            'authority': s.authority_ref, 'envelope': s.envelope_ref,
            'original_absolute_deadline': s.original_end, 'ready': False}), INTAKE_REQUEST_MAX)
        _intake_seal(s)
        need(_intake_read_envelope(s, s.envelope_ref, s.envelope_identity)[0] == s.envelope_raw,
             'pair_intake_approval_changed')
        _intake_context(s)
        s.finished = s.j.finish_reservation(s.budget)
        # Keep a successful finish receipt if expiry or seal fails afterwards.
        _intake_seal(s)
        s.state = 'DISPATCHING'
        result = run(s.b, s.lock, s.inventory_ref, s.authority_ref)
        s.state = 'CONSUMED'
        return result
    except BaseException:
        _intake_fail(s); raise


def _intake_run_gate(backend, lock, inventory_ref, authority_ref):
    if not hasattr(backend, '_initial_pg_pair_intake'): return
    s = backend._initial_pg_pair_intake
    try:
        need(type(s) is _Intake and s.b is backend and s.lock is lock and s.state == 'DISPATCHING'
             and s.finished is not None and not s.entered_run
             and inventory_ref == s.inventory_ref and authority_ref == s.authority_ref,
             'pair_intake_dispatch_required')
        _intake_seal(s)
        need(_intake_read_envelope(s, s.envelope_ref, s.envelope_identity)[0] == s.envelope_raw,
             'pair_intake_approval_changed')
        s.j.read_phase_bytes(s.finished['phase'], s.finished['name'], s.finished['sha256'])
        _intake_context(s)
        s.entered_run = True
    except BaseException:
        _intake_fail(s); raise


def _provision(p, operation):
    _shape(p, ('schema_version', 'kind', 'image_id', 'tools', 'network', 'peer', 'recovery_role', 'function_owners',
        'target', 'primary_slot', 'standby_trust', 'source_receipts', 'ordinary_roles'), 'pair_provision')
    need(type(p['schema_version']) is int and p['schema_version'] == 1
         and p['kind'] == 'openlegal_initial_pair_preprovision'
         and type(p['image_id']) is str and re.fullmatch('sha256:[0-9a-f]{64}', p['image_id']), 'pair_provision_identity')
    need(type(p['tools']) is dict and set(p['tools']) == set(TOOLS) and all(pin(v) for v in p['tools'].values()), 'pair_tools')
    _name(p['recovery_role'])
    need(type(p['function_owners']) is dict and set(p['function_owners']) == set(FUNCTIONS)
         and all(type(r) is dict and set(r) == {'name', 'body_sha256'} and _name(r['name']) != p['recovery_role']
                 and pin(r['body_sha256']) for r in p['function_owners'].values()), 'pair_function_manifest')
    need(type(p['ordinary_roles']) is list and 1 <= len(p['ordinary_roles']) <= 16
         and p['ordinary_roles'] == sorted(set(p['ordinary_roles'])) and p['recovery_role'] not in p['ordinary_roles']
         and all(_name(v) for v in p['ordinary_roles']), 'pair_ordinary_roles')
    n = p['network']; _shape(n, ('id', 'name', 'subnet', 'primary_address', 'backup_address', 'standby_address'), 'pair_network')
    need(pin(n['id']) and type(n['name']) is str and re.fullmatch('[A-Za-z0-9_.-]{1,128}', n['name']), 'pair_network_identity')
    subnet = ipaddress.ip_network(n['subnet'], strict=True)
    ips = [ipaddress.ip_address(n[k]) for k in ('primary_address', 'backup_address', 'standby_address')]
    need(len(set(ips)) == 3 and all(ip in subnet for ip in ips), 'pair_distinct_addresses')
    q = p['peer']; _shape(q, ('application_name', 'role', 'server_name', 'server_certificate_sha256',
        'client_dn', 'issuer_dn', 'client_serial', 'client_certificate_sha256', 'ca', 'certificate', 'private_key'), 'pair_trust')
    _name(q['application_name']); _name(q['role'])
    need(re.fullmatch('[a-z][a-z0-9_]{0,62}', q['application_name']), 'pair_application_name')
    need(type(q['server_name']) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9.-]{0,252}', q['server_name'])
         and pin(q['server_certificate_sha256']) and pin(q['client_certificate_sha256'])
         and type(q['client_serial']) is str and re.fullmatch('[1-9][0-9]{0,59}', q['client_serial'])
         and all(type(q[k]) is str and 1 <= len(q[k]) <= 1024 and '\x00' not in q[k] for k in ('client_dn', 'issuer_dn')),
         'pair_public_tls_identity')
    for k in ('ca', 'certificate', 'private_key'):
        row = q[k]; _shape(row, ('path', 'sha256'), 'pair_trust_file'); _trust_file(row)
    trust = p['standby_trust']; need(type(trust) is dict and set(trust) == {'ca', 'certificate', 'private_key', 'hba'}, 'pair_standby_trust')
    for row in trust.values(): _trust_file(row)
    t = p['target']; _shape(t, ('parent', 'identity', 'kernel_filesystem_id', 'capacity'), 'pair_target')
    private_parents(t['parent']); need(Path(t['parent']).resolve() == Path(t['parent']), 'pair_target_path')
    _shape(t['identity'], ('dev', 'ino'), 'pair_target_identity')
    c = t['capacity']
    common = {'kind', 'copy_max_bytes', 'reserve_bytes', 'physical_leaves', 'max_files',
              'max_rate_kib', 'filesystem_total_bytes'}
    need(type(c) is dict and c.get('kind') in ('plain_ssd_bounded_copy_v1', 'reserved_zfs_dataset'), 'pair_capacity_profile')
    _shape(c, common | ({'dataset','guid','quota_bytes','reservation_bytes'} if c['kind']=='reserved_zfs_dataset' else set()), 'pair_capacity_fields')
    need(all(type(c[k]) is int and c[k] > 0 for k in ('copy_max_bytes','reserve_bytes','max_files','max_rate_kib','filesystem_total_bytes'))
         and c['reserve_bytes'] >= (c['filesystem_total_bytes'] + 4)//5
         and c['copy_max_bytes'] + c['reserve_bytes'] < c['filesystem_total_bytes']
         and 1 <= c['max_files'] <= 1000000 and 32 <= c['max_rate_kib'] <= 1048576
         and type(c['physical_leaves']) is list and 1 <= len(c['physical_leaves']) <= 16
         and c['physical_leaves'] == sorted(set(c['physical_leaves']))
         and all(type(v) is str and re.fullmatch('/dev/[A-Za-z0-9_.-]{1,128}',v) for v in c['physical_leaves']), 'pair_copy_budget_policy')
    if c['kind']=='reserved_zfs_dataset':
        need(type(c['dataset']) is str and re.fullmatch('[A-Za-z][A-Za-z0-9_.-]*/[A-Za-z0-9_./-]+',c['dataset'])
             and '..' not in Path(c['dataset']).parts and type(c['guid']) is str and c['guid'].isdecimal()
             and type(c['quota_bytes']) is int and type(c['reservation_bytes']) is int
             and c['copy_max_bytes']+c['reserve_bytes'] <= c['quota_bytes'] <= c['reservation_bytes'], 'pair_zfs_reservation_policy')
    slot = p['primary_slot']; _shape(slot, ('path', 'identity', 'before', 'after'), 'pair_slot')
    private_parents(str(Path(slot['path']).parent)); _shape(slot['identity'], ('dev', 'ino'), 'pair_slot_identity')
    for k in ('before', 'after'): driver._reference(slot[k])
    expected = ("synchronous_standby_names='FIRST 1 (" + q['application_name'] + ")'\n").encode()
    need(driver._reference(slot['after']) == expected and slot['path'] != slot['after']['path']
         and slot['path'] != slot['before']['path'], 'pair_exact_sync_slot_bytes')
    need(type(p['source_receipts']) is list and 1 <= len(p['source_receipts']) <= 32, 'pair_preprovision_raw_receipts')
    for r in p['source_receipts']:
        _shape(r, ('path', 'sha256'), 'pair_source_receipt'); driver._reference(r)


def _trust_file(ref):
    _shape(ref, ('path', 'sha256'), 'pair_tls_reference')
    need(pin(ref['sha256']), 'pair_tls_hash'); path = Path(ref['path']); private_parents(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        s = os.fstat(fd)
        need(stat.S_ISREG(s.st_mode) and s.st_nlink == 1 and 1 <= s.st_size <= 1024**2
             and (s.st_uid == s.st_gid == 70 and stat.S_IMODE(s.st_mode) == 0o600
                  or s.st_uid == 0 and s.st_gid == 70 and stat.S_IMODE(s.st_mode) == 0o640), 'pair_pg_readable_trust')
        raw = os.read(fd, 1024**2 + 1)
        need(digest(raw) == ref['sha256'] and signature(os.fstat(fd)) == signature(s)
             and signature(path.lstat()) == signature(s), 'pair_tls_file_changed')
        return raw
    finally: os.close(fd)


def _raw_reference(journal, row):
    _shape(row, ('phase', 'name', 'sha256'), 'pair_ack_reference')
    need(pin(row['sha256']), 'pair_ack_hash')
    raw = journal.read_phase_bytes(row['phase'], row['name'], row['sha256'])
    need(digest(raw) == row['sha256'], 'pair_ack_raw_changed')
    return seed._read_large(raw)


def _live(backend, lock, cfg, sc):
    seedcfg, _, _, _, dc, inv, template, draw, invraw, daraw = sc
    j = getattr(backend, '_initial_driver_journal', None)
    need(type(j) is journals.DualJournal and j.operation == cfg['operation_id']
         and j.binding['inventory_sha256'] == digest(draw) and j.binding['authority_sha256'] == digest(daraw)
         and j.phase_paths and j.phase_paths[0][0].name == MAINTENANCE,
         'pair_same_live_driver_and_maintenance_required')
    need(getattr(backend, '_initial_blob_seed_started', False) is True
         and type(getattr(backend, '_initial_blob_seed_fds', None)) is dict, 'pair_live_seed_fds')
    lock.check(); j.check(); _check(backend); driver._writer_fds(backend); seed._bound(backend)
    need(lock.path == Path(inv['shared_lock']['path']) and lock.identity == inv['shared_lock']['identity'], 'pair_shared_lock')
    initialraw, _ = seed._phase_raw(j, migration.PHASE, 'driver-complete.json'); initial = decode(initialraw)
    need(initial['operation_id'] == cfg['operation_id'] and initial['schema_version'] == 33, 'pair_driver_complete')
    for k in ('legacy_actual_exit', 'initial_actual_exit'):
        row = initial[k]
        need(type(row['actual_exit_code']) is int and row['actual_exit_code'] == 0 and row['remaining_children'] == []
             and row['boot_id'] == inv['host']['boot_id'], 'pair_actual_initial_child_zero')
        manual._process_absent({n: row[n] for n in ('pid', 'start_ticks', 'boot_id')})
    anchraw, _ = seed._phase_raw(j, legacy.PHASE, legacy.PHASE + '-parent-fd-anchor.json'); anchor = decode(anchraw)
    need(anchor['pid'] == os.getpid() and anchor['start_ticks'] == Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]
         and anchor['boot_id'] == inv['host']['boot_id'], 'pair_same_root_parent')
    need({(r['fd'], r['dev'], r['ino'], r['path']) for r in anchor['cgroups']} ==
         {(fd, r['dev'], r['ino'], r['path']) for fd, r in backend._cgroup_fds.values()}, 'pair_parent_writer_fds')
    oldraw, _ = seed._phase_raw(j, legacy.PHASE, legacy.PHASE + '-returned-payloads.json'); old = decode(oldraw)
    baseraw, _ = seed._phase_raw(j, legacy.PHASE, legacy.PHASE + '-blob-manifests-parent-live.json')
    baseline = seed._read_large(baseraw)
    complete, _ = seed._phase_raw(j, seed.PHASE, seed.PHASE + '-complete.json'); complete = decode(complete)
    need(complete['kind'] == 'openlegal_initial_blob_seed_result' and complete['operation_id'] == cfg['operation_id']
         and complete['managed_epoch'] == seedcfg['managed_epoch'] and complete['media_profile'] == blobs.MEDIA_PROFILE
         and pin(complete['blob_binding_sha256']) and set(r['role'] for r in complete['roots']) == set(blobs.ROLES)
         and all(complete[k] is False for k in ('postgres_replica_created', 'roles_created', 'workload_started',
                                               'provider_called', 'ready', 'automatic_retry')), 'pair_seed_complete')
    # Reconstruct the SAME initial-native primary policy from actual parent proof.
    current = copy.deepcopy(template); current.update(old['fresh_primary'])
    prereqs = {}
    for key in ('drain', 'snapshot', 'preservation'):
        raw, sha = seed._phase_raw(j, legacy.PHASE, 'live-' + key + '-proof.json'); value = decode(raw)
        need(value['producer_exit'] == initial['legacy_actual_exit'] and value['payload'] == old['payloads'][key]
             and value['inventory_sha256'] == digest(invraw) and value['authority_sha256'] == digest(daraw), 'pair_initial_proof')
        prereqs[key] = {'phase': legacy.PHASE, 'name': 'live-' + key + '-proof.json', 'sha256': sha}
    current['prerequisites'] = prereqs
    current['legacy_binding'] = {'inventory_sha256': digest(invraw), 'source_graph_sha256': digest(canonical(dc['source_graph'])),
                                 'authority_sha256': digest(daraw)}
    work = copy.deepcopy(inv); work['initial_migration'] = current
    return j, work, old, baseline, complete


def _maintenance(j, ref, cfg, dc, invraw, daraw, parent_exit, driver_raw):
    """Closed future producer contract. Not an inventory-supplied success flag.

    The parent must retain its actual observed exit in the SAME live Backend.
    Loading a historical JSON/forged child0 is insufficient. Producer phase and
    its raw plan/apply/readback/native/SQL-closure commands are dual-ACKed first.
    No implementation of that separate provider mutation is hidden here.
    """
    m = _raw_reference(j, ref)
    _shape(m, ('schema_version', 'kind', 'operation_id', 'provider_recovery_operation', 'driver_inventory_sha256', 'inventory', 'authority',
        'authority_sha256', 'source_graph_sha256', 'producer_source', 'producer_exit', 'plan', 'apply',
        'readback', 'audit', 'native_command', 'sql_closure', 'new_uncertainty_ids'), 'pair_maintenance_contract')
    need(ref['phase'] == MAINTENANCE and type(m['schema_version']) is int and m['schema_version'] == 1
         and m['kind'] == 'openlegal_initial_provider_maintenance_result' and m['operation_id'] == cfg['operation_id']
         and valid_uuid(m['provider_recovery_operation'])
         and m['driver_inventory_sha256'] == digest(driver_raw)
         and m['authority_sha256'] == m['authority']['sha256']
         and m['source_graph_sha256'] == digest(canonical(dc['source_graph'])), 'pair_maintenance_binding')
    # The future producer owns a separate self pin and inventory/authority refs;
    # its parent-observed exit tuple must already live in this backend.
    producer_inventory = decode(driver._reference(m['inventory'])); producer_authority = decode(driver._reference(m['authority']))
    need(producer_inventory['operation_id'] == cfg['operation_id']
         and producer_inventory['driver_inventory_sha256'] == m['driver_inventory_sha256']
         and producer_inventory['source_sha256'] == m['producer_source']['sha256']
         and producer_inventory['driver_authority_sha256'] == digest(daraw)
         and producer_inventory['legacy_inventory_sha256'] == digest(invraw)
         and producer_authority['inventory_sha256'] == m['inventory']['sha256']
         and producer_authority['operation_id'] == cfg['operation_id'], 'pair_maintenance_producer_binding')
    need(m['producer_source']['path'] == str(Path(__file__).resolve().parent/'initial_provider_maintenance.py'), 'pair_fixed_maintenance_producer_source')
    driver._reference(m['producer_source'])
    row = m['producer_exit']
    _shape(row, ('pid', 'start_ticks', 'boot_id', 'actual_exit_code', 'remaining_children'), 'pair_maintenance_exit')
    need(type(row['pid']) is int and row['pid'] > 0 and type(row['start_ticks']) is str and row['start_ticks'].isdecimal()
         and row['boot_id'] == Path('/proc/sys/kernel/random/boot_id').read_text().strip()
         and canonical(row) == canonical(parent_exit) and type(row['actual_exit_code']) is int and row['actual_exit_code'] == 0
         and row['remaining_children'] == [], 'pair_parent_observed_maintenance_zero')
    manual._process_absent({k: row[k] for k in ('pid', 'start_ticks', 'boot_id')})
    need(all(m[k]['phase'] == MAINTENANCE for k in ('plan','apply','readback','audit','native_command','sql_closure')), 'pair_maintenance_phase_refs')
    cmd = _raw_reference(j, m['native_command'])
    need(type(cmd['actual_exit_code']) is int and cmd['actual_exit_code'] == 0 and cmd['started'] is True
         and cmd['owned_processes_closed'] is True and all(cmd[k] is False for k in
             ('expired', 'interrupted', 'unexpected_descendants', 'output_limit_exceeded')), 'pair_maintenance_native_zero')
    closed = _raw_reference(j, m['sql_closure']); need(closed == {'sessions': []}, 'pair_maintenance_sql_not_closed')
    plan = _raw_reference(j, m['plan']); applied = _raw_reference(j, m['apply'])
    readback = _raw_reference(j,m['readback']); audit = _raw_reference(j, m['audit'])
    need(readback == applied,'pair_actual_cli_readback_changed')
    need(plan['operation_id'] == m['provider_recovery_operation']
         and applied['operation_id'] == m['provider_recovery_operation'] and plan['request']['resume'] is False
         and plan['request']['resolve_legacy'] is False and plan['request']['waits'] == []
         and audit['plan'] == plan and audit['before_state'] == plan['snapshot']['state']
         and audit['result'] == applied and audit['operation_id'] == m['provider_recovery_operation'], 'pair_exact_maintenance_audit')
    need(type(m['new_uncertainty_ids']) is list and len(m['new_uncertainty_ids']) <= 17
         and all(valid_uuid(v) for v in m['new_uncertainty_ids'])
         and m['new_uncertainty_ids'] == sorted(set(m['new_uncertainty_ids'])), 'pair_uncertainty_delta')
    return m, audit


class _Pair:
    """Fixed native ports; tests may patch class methods, inventory never may."""
    def __init__(self, backend, lock, cfg, auth, raw, authraw, p, praw, sc, live, maintenance):
        self.b = backend; self.lock = lock; self.cfg = cfg; self.auth = auth; self.p = p
        self.raw = raw; self.authraw = authraw; self.praw = praw; self.sc = sc
        self.j, self.work, self.old, self.baseline, self.seed_result = live
        self.parent = os.getpid(); self.ticks = Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]
        self.native = []; self.fds = []; self.created = {}; self.helpers = []; self.frozen_work = canonical(self.work); self.budget = None; self.serial = 0
        self.b._initial_pg_pair_pending_ownership = {}
        self.original_end = backend._driver_deadline
        self.original_clock = getattr(backend, '_initial_driver_clock', None)
        self._completed = None; self._run_return = None
        self.primary = self.work['linux']['postgres']['primary']['container_id']; self.standby = None
        self.role_context = _role_context(self,maintenance)
        self.op = Operation(cfg['operation_id'], 'strict_prepare', PHASE, self.work, digest(raw),
            digest(canonical({'driver': sc[4]['source_graph'], 'initial_blob_seed.py': sc[0]['source_sha256'],
                              'initial_role_prepare.py':self.role_context['producer_source']['sha256'],
                              'initial_provider_maintenance.py':maintenance['producer_source']['sha256'],
                              'initial_pg_pair.py': cfg['source_sha256']})), digest(authraw), self.j,
            Path(self.work['backup_root']['path']), {}, self.seal, freeze_authority(auth))

    def seal(self):
        if self.budget is not None:
            need(self.j.count <= self.budget['start_count'] + self.budget['records']
                 and self.j.bytes <= self.budget['start_bytes'] + self.budget['bytes'], 'pair_reserved_journal_exhausted')
        self.seal_context()

    def seal_context(self):
        # Later stages reserve input DATA separately; the native Pair budget is
        # never renewed, cleared or charged for that later historical read.
        _check(self.b); need(not getattr(self.b,'_initial_pg_pair_cancelled',False),'pair_cancelled')
        need(self.b._driver_deadline == self.original_end
             and getattr(self.b, '_initial_driver_clock', None) is self.original_clock,
             'pair_original_clock_changed')
        need(canonical(self.work)==self.frozen_work and Path('/proc/sys/kernel/random/boot_id').read_text().strip()==self.work['host']['boot_id'], 'pair_work_or_boot_changed')
        self.lock.check(); driver._writer_fds(self.b); seed._bound(self.b)
        need(os.getpid() == self.parent and self.ticks == Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]
             and time.time() < self.auth['valid_until'], 'pair_parent_or_authority_changed')
        need(driver._reference(self.inventory_ref) == self.raw and driver._reference(self.authority_ref) == self.authraw, 'pair_inputs_changed')
        driver._source_seal(self.sc[4]['source_graph']); private_bytes(Path(__file__).resolve(), self.cfg['source_sha256'])
        need(driver._reference(self.cfg['seed_inventory']) == self.sc[2]
             and driver._reference(self.cfg['seed_authority']) == self.sc[3]
             and driver._reference(self.cfg['provision']) == self.praw, 'pair_dependency_changed')
        context=self.role_context
        need(driver._reference(context['inventory_ref'])==context['inventory_raw']
             and driver._reference(context['authority_ref'])==context['authority_raw']
             and driver._reference(context['maintenance_inventory_ref'])==context['maintenance_inventory_raw'],
             'pair_role_dependency_changed')
        driver._reference(context['producer_source']);driver._reference(context['maintenance_source'])
        for fd, path, ident in self.fds:
            fresh = root_fd(path)
            try: need(_fd_id(fd) == ident == _fd_id(fresh), 'pair_held_root_rebound')
            finally: os.close(fresh)
        self.j.check(); _check(self.b, self.original_end)

    def save(self, name, value):
        self.seal(); self.serial += 1; name = PHASE + '-%04d-' % self.serial + name + '.json'
        raw = canonical(value)
        if self.budget is not None:
            need(self.j.count+1 <= self.budget['start_count']+self.budget['records']
                 and self.j.bytes+len(raw) <= self.budget['start_bytes']+self.budget['bytes'], 'pair_prewrite_reserved_budget')
        sha = self.j.save_bytes(name, raw)
        need(self.j.read_phase_bytes(PHASE, name, sha) == raw, 'pair_dual_ack'); self.seal()
        return {'phase': PHASE, 'name': name, 'sha256': sha}

    def command(self, args, *, stdin=None, end=None, timeout=30, raw_limit=4 * 1024**2):
        self.seal(); end = min(self.b._driver_deadline, end) if end is not None else self.b._driver_deadline
        _check(self.b, end)
        if self.budget is not None:
            need(self.j.count+1 <= self.budget['start_count']+self.budget['records']
                 and self.j.bytes+16384 <= self.budget['start_bytes']+self.budget['bytes'], 'pair_predispatch_reserved_budget')
        raw = self.b._run(self.op, 'docker', args, stdin=stdin, timeout=timeout, deadline=end, record=False)
        _check(self.b, end); need(len(raw) <= raw_limit, 'pair_fixed_output_bound')
        name = PHASE + '-command-%04d.json' % self.b._sequence
        sha = self.j.output_hashes().get(name); need(pin(sha), 'pair_actual_command_receipt')
        cmd = decode(self.j.read_phase_bytes(PHASE, name, sha))
        dispatch = ['--config', str(self.b._docker_config[0]), '--host', 'unix:///var/run/docker.sock']
        need(cmd['binary'] == 'docker' and cmd['arguments_sha256'] == digest(canonical(dispatch + args))
             and cmd['stdin_sha256'] == (digest(stdin) if stdin is not None else None)
             and cmd['stdout_sha256'] == digest(raw) and cmd['stdout_bytes'] == len(raw)
             and type(cmd['actual_exit_code']) is int and cmd['actual_exit_code'] == 0
             and cmd['started'] is True and cmd['owned_processes_closed'] is True
             and all(cmd[k] is False for k in ('expired', 'interrupted', 'unexpected_descendants', 'output_limit_exceeded')),
             'pair_actual_command_failed')
        return raw, {'phase': PHASE, 'name': name, 'sha256': sha}

    def sql(self, payload, *, cid=None, legacy_read=False, end=None, audit=False, multi=False):
        # payload is exclusively assembled by this module, never taken from DATA.
        cid = cid or self.primary
        need(cid == self.primary or cid == self.standby and cid in self.created, 'pair_unowned_sql_target')
        role = 'openlegal_migration' if legacy_read else self.p['recovery_role']
        seconds = 30 if audit else 2; transaction = 35 if audit else 5
        prefix = ("SET statement_timeout='%ds'; SET lock_timeout='1s'; SET transaction_timeout='%ds'; " % (seconds, transaction)
                  + "SET standard_conforming_strings=on; SET TIME ZONE 'UTC'; SET DateStyle='ISO,YMD'; SET bytea_output='hex'; SET extra_float_digits=3; ")
        unset = [v for k in UNSET for v in ('-u', k)]
        args = ['exec', '-i', '-u', '70', cid, '/usr/bin/env', *unset, 'PGPASSFILE=/dev/null',
            '/usr/local/bin/psql', '-X', '-w', '-h', '/var/run/postgresql', '-p', '5432', '-qAt', '-v', 'ON_ERROR_STOP=1',
            '-U', role, '-d', 'openlegal']
        stdin = (prefix + payload + ';\n').encode()
        raw, cmd = self.command(args, stdin=stdin, end=end, timeout=31 if audit else 3,
                                raw_limit=preservation.MAX_BUNDLE_BYTES)
        value = [decode(line) for line in raw.splitlines()] if multi else seed._read_large(raw)
        need(not multi or len(value) in (2,3), 'pair_sql_fixed_result_count')
        record = {'actual_command': cmd, 'raw_utf8': raw.decode('utf8')}
        ref = self.save('sql-raw', record)
        return value, ref, len(canonical(record))

    def sessions(self, cid=None):
        value, _, _ = self.sql('SELECT openlegal_checkpoint.client_sessions()', cid=cid)
        need(value == {'sessions': []}, 'pair_global_sql_not_closed')
        return value

    def position(self, cid=None):
        value, _, _ = self.sql('SELECT openlegal_checkpoint.database_position()', cid=cid)
        return manual._checkpoint(value, recovering=cid is not None and cid == self.standby)

    def hold(self, path):
        fd = root_fd(path); ident = _fd_id(fd); self.fds.append((fd, str(path), ident))
        return fd


def _fd_id(fd):
    s = os.fstat(fd)
    return (s.st_dev, s.st_ino, os.fstatvfs(fd).f_fsid, blobs._fd_mount_id(fd), s.st_uid, s.st_gid, stat.S_IMODE(s.st_mode))


def _settings(pair):
    query = ("SELECT jsonb_build_object('sync',current_setting('synchronous_standby_names'),"
        "'commit',current_setting('synchronous_commit'),'fsync',current_setting('fsync'),"
        "'full_page_writes',current_setting('full_page_writes'),'wal_level',current_setting('wal_level'),"
        "'senders',current_setting('max_wal_senders'),'pending_restart',"
        "EXISTS(SELECT 1 FROM pg_settings WHERE pending_restart),'source',"
        "(SELECT source FROM pg_settings WHERE name='synchronous_standby_names'))")
    return pair.sql(query)[0]


def _role_context(pair, maintenance):
    return _qualified_role_context(pair.b, pair.lock, pair.p, maintenance)


def _qualified_role_context(backend, lock, provision, maintenance):
    """Consume the exact live role handoff linked by the maintenance producer.

    This never spoofs the old current phase or accepts a historical role file.
    The role producer owns its source-fixed complete role/ACL grammar.
    """
    producer_raw = driver._reference(maintenance['inventory']);producer = decode(producer_raw)
    result = role_prepare.consume_archived_live(backend, lock, producer['role_prepare'])
    inventory_raw = driver._reference(result['inventory']);inventory = decode(inventory_raw)
    need(result['producer_source']['path'] == str(Path(__file__).resolve().parent/'initial_role_prepare.py'),
         'pair_fixed_role_producer_source')
    driver._reference(result['producer_source'])
    names = result['role_names']; runtime = inventory['runtime_role']
    expected = role_prepare.expected_catalog(names, runtime)
    need(provision['recovery_role'] == names['recovery_login'] and provision['peer']['role'] == names['peer_login']
         and provision['function_owners'] == {sig:{'name':expected['functions'][sig]['owner'],
            'body_sha256':expected['functions'][sig]['body_sha256']} for sig in FUNCTIONS}
         and provision['ordinary_roles'] == sorted([runtime,names['storage_login'],names['canary_login']]),
         'pair_source_role_manifest_changed')
    return {'names':names,'runtime':runtime,'result':result,'inventory_raw':inventory_raw,
        'inventory_ref':result['inventory'],'authority_ref':result['authority'],
        'authority_raw':driver._reference(result['authority']),'producer_source':result['producer_source'],
        'maintenance_inventory_ref':maintenance['inventory'],'maintenance_inventory_raw':producer_raw,
        'maintenance_source':maintenance['producer_source'],'reference':producer['role_prepare']}


def _catalog(pair):
    context = pair.role_context
    value = pair.sql(role_prepare.catalog_sql(context['names'],context['runtime']),audit=True)[0]
    need(value['session_user'] == value['current_user'] == pair.p['recovery_role'], 'pair_catalog_login_identity')
    # Re-derive every grant and membership from SOURCE, never provision DATA.
    # Includes effective table/column/schema/catalog-function privileges and
    # indirect MEMBER/SET/ADMIN closure for all source-created roles.
    role_prepare.check_catalog(value,context['names'],context['runtime'])
    owners = ','.join(_quote(context['names'][k]) for k in role_prepare.ROLE_KEYS[:5])
    for role in pair.p['ordinary_roles']:
        denied = pair.sql('SELECT NOT has_function_privilege(' + _quote(role) + ','
            + _quote('openlegal_checkpoint.bind_replica(uuid,text,jsonb,jsonb)') + ",'EXECUTE') AND NOT EXISTS("
            'SELECT 1 FROM pg_roles WHERE rolname IN ('+owners+') AND pg_has_role('+_quote(role)+",oid,'MEMBER'))")[0]
        need(denied is True,'pair_ordinary_role_has_bind_or_owner')
    pair.save('catalog-preflight', value)


def _network(pair):
    n = pair.p['network']
    raw, _ = pair.command(['network', 'inspect', '--format',
        '{"Id":{{json .Id}},"Name":{{json .Name}},"Internal":{{json .Internal}},"IPAM":{{json .IPAM}},"Containers":{{json .Containers}}}', n['id']])
    value = decode(raw)
    need(value['Id'] == n['id'] and value['Name'] == n['name'] and value['Internal'] is True
         and any(r.get('Subnet') == n['subnet'] for r in value['IPAM']['Config']), 'pair_internal_network_changed')
    endpoints = value['Containers']; need(type(endpoints) is dict and pair.primary in endpoints, 'pair_primary_network')
    address = lambda r: ipaddress.ip_interface(r['IPv6Address' if ':' in n['primary_address'] else 'IPv4Address']).ip
    need(address(endpoints[pair.primary]) == ipaddress.ip_address(n['primary_address']), 'pair_primary_address')
    occupied = {address(r) for cid, r in endpoints.items() if cid not in pair.created}
    need(ipaddress.ip_address(n['standby_address']) not in occupied
         and ipaddress.ip_address(n['backup_address']) not in occupied, 'pair_reserved_address_in_use')


def _tls(pair):
    """Native SSLRequest: CA+DNS and exact currently loaded server DER pin."""
    q = pair.p['peer']; end = min(pair.b._driver_deadline, time.monotonic() + 5)
    for k in ('ca', 'certificate', 'private_key'): _trust_file(q[k])
    certificate = _trust_file(q['certificate']).decode('ascii')
    need(digest(ssl.PEM_cert_to_DER_cert(certificate)) == q['client_certificate_sha256'], 'pair_client_der_changed')
    ctx = ssl.create_default_context(cafile=q['ca']['path']); ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(q['certificate']['path'], q['private_key']['path'])
    ip = pair.p['network']['primary_address']; family = socket.AF_INET6 if ':' in ip else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as sock:
        _check(pair.b, end); sock.settimeout(max(.001, end - time.monotonic())); sock.connect((ip, 5432))
        _check(pair.b, end); sock.settimeout(max(.001, end - time.monotonic()))
        sock.sendall(struct.pack('!II', 8, 80877103)); need(sock.recv(1) == b'S', 'pair_tls_required')
        _check(pair.b, end); sock.settimeout(max(.001, end - time.monotonic()))
        with ctx.wrap_socket(sock, server_hostname=q['server_name']) as secured:
            need(digest(secured.getpeercert(binary_form=True)) == q['server_certificate_sha256'], 'pair_loaded_server_cert_changed')
    _check(pair.b, end)
    for k in ('ca', 'certificate', 'private_key'): _trust_file(q[k])
    pair.save('loaded-tls-peer', {'address': ip, 'server_name': q['server_name'],
        'server_der_sha256': q['server_certificate_sha256'], 'client_der_sha256': q['client_certificate_sha256']})


def _native_primary(pair):
    c = pair.work['legacy_prepare']; end = pair.b._driver_deadline
    legacy._images(pair.b,pair.op,c,end); legacy._control(pair.b,pair.op,c,end)
    native = legacy._native_primary(pair.b, pair.op, c, end)
    pair.native.append(native)
    pair.b._initial_pg_pair_retained.extend(native[k] for k in ('data_fd', 'wal_fd', 'cgroup_fd'))
    need(native['process'] == pair.work['initial_migration']['primary_process'], 'pair_original_primary_lifetime')
    legacy._recheck_native_mounts(pair.b, pair.op, c, native, end)
    legacy._datasets(pair.b, pair.op, c, end, native)
    pair.primary_pidfd = os.pidfd_open(native['process']['pid']); pair.b._initial_pg_pair_retained.append(pair.primary_pidfd)
    directory = pair.b._container_directory(native['process']['pid'], '/usr/local/bin')
    try:
        for tool in TOOLS:
            fd = os.open(tool, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory)
            try: legacy._hash_file(fd, pair.p['tools'][tool])
            finally: os.close(fd)
    finally: os.close(directory)
    need(pair.p['image_id'] == pair.work['linux']['postgres']['primary']['image_id'], 'pair_native_image')
    return native


def _primary_stable(pair):
    native = pair.native[0]; process = native['process']
    need(not select.select([pair.primary_pidfd], [], [], 0)[0], 'pair_primary_exited')
    need(Path('/proc/' + str(process['pid']) + '/stat').read_text().rsplit(')', 1)[1].split()[19] == process['start_ticks'], 'pair_primary_reused')
    legacy._recheck_native_mounts(pair.b, pair.op, pair.work['legacy_prepare'], native, pair.b._driver_deadline)


def _capacity(pair, *, empty):
    t = pair.p['target']; c = t['capacity']; fd = pair.target_fd
    st = os.fstat(fd); table = blobs._mounts(); mount = blobs._path_mount(fd,t['parent'],table)
    need((st.st_dev,st.st_ino)==(t['identity']['dev'],t['identity']['ino']) and st.st_uid==st.st_gid==0
         and stat.S_IMODE(st.st_mode)==0o700 and os.fstatvfs(fd).f_fsid==t['kernel_filesystem_id']
         and sorted(os.listdir(fd)) == ([] if empty else ['data']), 'pair_capacity_parent')
    blobs._no_nested_mounts(t['parent'],table)
    domains,hardware=blobs._media_hardware(pair.b,pair.op)
    if c['kind']=='reserved_zfs_dataset':
        need(mount['filesystem']=='zfs' and mount['source']==c['dataset'] and mount['root']=='/'
             and mount['mountpoint']==t['parent'],'pair_actual_reserved_dataset_required')
        raw=pair.b._run(pair.op,'zfs',['get','-Hp','-o','property,value',
            'guid,mountpoint,mounted,quota,reservation,refquota,refreservation,readonly',c['dataset']],
            record=False,deadline=pair.b._driver_deadline)
        fields=dict(r.split('\t') for r in raw.decode('ascii').splitlines())
        need(fields=={'guid':c['guid'],'mountpoint':t['parent'],'mounted':'yes','quota':str(c['quota_bytes']),
            'reservation':str(c['reservation_bytes']),'refquota':'0','refreservation':'0','readonly':'off'},'pair_quota_or_reservation_changed')
        raw=pair.b._run(pair.op,'zpool',['status','-P',c['dataset'].split('/')[0]],record=False,
                        deadline=pair.b._driver_deadline).decode('ascii')
        need(re.search(r'^\s*state:\s+ONLINE\s*$',raw,re.M)
             and re.search(r'^errors: No known data errors\s*$',raw,re.M),'pair_ssd_pool_unhealthy')
        leaves=set()
        for line in raw.splitlines():
            parts=line.split()
            if parts and parts[0].startswith('/dev/'):
                need(parts[1:]==['ONLINE','0','0','0'],'pair_ssd_leaf_unhealthy')
                node=Path(parts[0]).resolve(strict=True).stat();need(stat.S_ISBLK(node.st_mode),'pair_ssd_leaf_type')
                leaves.update(pair.b._device_leaves(node.st_rdev))
    else:
        need(mount['filesystem'] in ('ext4','xfs'),'pair_plain_ssd_filesystem')
        leaves=pair.b._device_leaves(st.st_dev)
    need(leaves==set(c['physical_leaves']) and leaves<=set(domains['ssd']) and not leaves&set(domains['hdd'])
         and not {hardware[v] for v in leaves}&{hardware[v] for v in domains['hdd']},'pair_ssd_physical_domain')
    space=os.fstatvfs(fd);_space_budget(c,space,0 if empty else _copy_size(pair,pair.data_path)['logical_bytes'])
    return {'capacity':c,'mount':mount,'parent_identity':t['identity'],
        'capacity_evidence':'observed_budget_only' if c['kind']=='plain_ssd_bounded_copy_v1' else 'preprovisioned_zfs_reservation',
        'peak_guarantee':False}


def _space_budget(cap, space, copied):
    total=space.f_blocks*space.f_frsize;available=space.f_bavail*space.f_frsize
    need(total==cap['filesystem_total_bytes'] and cap['reserve_bytes'] >= (total+4)//5,
         'pair_filesystem_total_or_floor_changed')
    need(type(copied) is int and 0<=copied<=cap['copy_max_bytes'],'pair_copy_byte_bound')
    remaining=cap['copy_max_bytes']-copied
    need(available >= cap['reserve_bytes']+remaining,'pair_live_space_budget')
    return {'total_bytes':total,'available_bytes':available,'remaining_copy_bound':remaining,
            'free_floor_bytes':cap['reserve_bytes'],'hard_reservation':False,'peak_guarantee':False}


def _copy_size(pair,path):
    cap=pair.p['target']['capacity'];fd=root_fd(path);root=_fd_id(fd);count=0;size=0
    try:
        def visit(directory,depth):
            nonlocal count,size
            _check(pair.b);need(depth<=64,'pair_cluster_depth')
            with os.scandir(directory) as entries:
                for entry in entries:
                    _check(pair.b);count+=1;need(count<=cap['max_files'],'pair_cluster_file_bound')
                    info=os.stat(entry.name,dir_fd=directory,follow_symlinks=False)
                    if stat.S_ISDIR(info.st_mode):
                        child=os.open(entry.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=directory)
                        try:
                            need(_fd_id(child)[3]==root[3] and (os.fstat(child).st_dev,os.fstat(child).st_ino)==(info.st_dev,info.st_ino), 'pair_cluster_nested_mount')
                            visit(child,depth+1)
                        finally:os.close(child)
                    else:
                        need(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_dev==root[0], 'pair_cluster_external_file')
                        size+=info.st_size;need(size<=cap['copy_max_bytes'],'pair_cluster_byte_bound')
        visit(fd,0);_check(pair.b)
        fresh=root_fd(path)
        try:need(_fd_id(fd)==root==_fd_id(fresh),'pair_cluster_root_rebound')
        finally:os.close(fresh)
        return {'entries':count,'logical_bytes':size}
    finally:os.close(fd)


def _copy_watch(pair):
    pair.seal();_primary_stable(pair)
    # PG background WAL growth is allowed ONLY inside the fixed cluster bound.
    src=pair.work['legacy_prepare']['pgdata']['host_path'];source=_copy_size(pair,src)
    target=_copy_size(pair,pair.data_path)
    space=_space_budget(pair.p['target']['capacity'],os.fstatvfs(pair.target_fd),target['logical_bytes'])
    return {'source':source,'target':target,'space':space}


def _helper_groups(groups, cid):
    return (len(groups) == 1 and groups[0].startswith('0::/')
            and any(part in (cid, 'docker-' + cid + '.scope') for part in Path(groups[0][3:]).parts))


def _discover_started_helper(pair, native):
    """Failure-only reconciliation of this exact source-created CID.

    No Docker command, guessed PID, adopted name or renewed logical deadline.
    An absent observation cannot prove that a transmitted start had no effect.
    The caller therefore keeps ROOT ownership until an actual anchor is found.
    """
    cid = native['container_id']; need(pin(cid) and cid in pair.created, 'pair_helper_created_identity')
    candidates = {}; inspected = 0
    for entry in Path('/proc').iterdir():
        if not entry.name.isdecimal(): continue
        inspected += 1; need(inspected <= 65536, 'pair_helper_proc_bound')
        pid = int(entry.name); groups = linux.process_cgroup(pid)
        if _helper_groups(groups, cid): candidates.setdefault(groups[0], []).append(pid)
    need(len(candidates) <= 1, 'pair_helper_multiple_native_cgroups')
    if not candidates: return False
    group_name, pids = next(iter(candidates.items()))
    path = '/sys/fs/cgroup' + group_name[3:]; group = root_fd(path)
    try:
        for pid in sorted(pids):
            try:
                ticks = Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()[19]
                pidfd = os.pidfd_open(pid)
            except (FileNotFoundError, ProcessLookupError): continue
            try:
                if (linux.process_cgroup(pid) != [group_name]
                        or Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()[19] != ticks):
                    continue
                fresh = root_fd(path)
                try: need(_fd_id(fresh) == _fd_id(group), 'pair_helper_discovery_cgroup_changed')
                finally: os.close(fresh)
                native.update(cgroup_fd=group, pidfd=pidfd, groups=[group_name],
                    process={'pid':pid, 'start_ticks':ticks, 'boot_id':pair.work['host']['boot_id']})
                pair.b._initial_pg_pair_retained.extend((group,pidfd))
                group = pidfd = None
                return True
            except (FileNotFoundError, ProcessLookupError): pass
            finally:
                if pidfd is not None: os.close(pidfd)
        return False
    finally:
        if group is not None: os.close(group)


def _stop_owned_helper(pair,native):
    """Physical ownership outlives logical expiry. No new Docker command/killpg.

    Only this newly created container's exact retained cgroup and stable pidfds.
    CAP_DROP/read-only spec prevents helper cgroup escape. Ambiguous /proc or
    cgroup observation keeps this root/lock alive; never guessed closure.
    """
    if native.get('start_transmitted') is False: return
    while 'cgroup_fd' not in native:
        try:
            if _discover_started_helper(pair,native): break
        except (OSError,RuntimeError): pass
        time.sleep(.05)
    started=time.monotonic();known={};group=native['cgroup_fd'];stamp=(os.fstat(group).st_dev,os.fstat(group).st_ino)
    while True:
        try:
            need((os.fstat(group).st_dev,os.fstat(group).st_ino)==stamp,'pair_helper_cgroup_rebound')
            if native.get('closure') is not None:
                closed=_raw_reference(pair.j,native['closure'])
                need(closed['container_id']==native['container_id'] and closed['process']==native['process']
                     and type(closed['actual_exit_code']) is int and closed['actual_exit_code']==0
                     and closed['owned_members']==[], 'pair_helper_closed_receipt_changed')
                cmd=_raw_reference(pair.j,closed['wait_command'])
                dispatch=['--config','/tmp/openlegal-storage-docker-client-'+pair.cfg['operation_id'],
                    '--host','unix:///var/run/docker.sock','wait',native['container_id']]
                need(type(cmd['actual_exit_code']) is int and cmd['actual_exit_code']==0
                     and cmd['started'] is True and cmd['owned_processes_closed'] is True
                     and cmd['binary']=='docker' and cmd['arguments_sha256']==digest(canonical(dispatch))
                     and cmd['stdin_sha256'] is None and cmd['stdout_sha256']==digest(b'0\n') and cmd['stdout_bytes']==2
                     and all(cmd[k] is False for k in ('expired','interrupted','unexpected_descendants','output_limit_exceeded')),
                     'pair_helper_closed_command_changed')
                # A historical marker never waives CURRENT physical absence.
                if select.select([native['pidfd']],[],[],0)[0] and not linux.members(native['groups']):
                    manual._process_absent(native['process']);return
            path='/sys/fs/cgroup'+native['groups'][0][3:]
            try:
                fresh=root_fd(path)
                try:need((os.fstat(fresh).st_dev,os.fstat(fresh).st_ino)==stamp,'pair_helper_named_cgroup_rebound')
                finally:os.close(fresh)
            except FileNotFoundError:
                events=os.open('cgroup.events',os.O_RDONLY|os.O_CLOEXEC|os.O_NOFOLLOW,dir_fd=group)
                try:raw=os.read(events,4097)
                finally:os.close(events)
                need(len(raw)<=4096,'pair_cgroup_events_bound')
                rows=[row.split() for row in raw.splitlines()]
                need(all(len(row)==2 for row in rows),'pair_cgroup_events_shape')
                observed=dict(rows)
                if observed.get(b'populated')==b'0' and select.select([native['pidfd']],[],[],0)[0]:return
                raise Refused('pair_helper_cgroup_closure_unknown')
            for pid in linux.members(native['groups']):
                ticks=Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()[19]
                key=(pid,ticks)
                if key not in known:
                    need(len(known) < 4096,'pair_helper_retained_pidfd_bound')
                    fd=os.pidfd_open(pid)
                    if Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()[19]!=ticks:
                        os.close(fd);continue
                    known[key]=fd;pair.b._initial_pg_pair_retained.append(fd)
                if not select.select([known[key]],[],[],0)[0]:
                    signal.pidfd_send_signal(known[key],signal.SIGKILL if time.monotonic()-started>=5 else signal.SIGTERM,None,0)
            if select.select([native['pidfd']],[],[],0)[0] and not linux.members(native['groups']):return
        except (OSError,RuntimeError):
            pass  # Keep ownership, bounded RAM and retry observations, not backup.
        time.sleep(.05)


def _m32(pair, *, cid=None):
    counts={k:0 for k in preservation.KINDS}
    active = [(kind, None) for kind in preservation.KINDS]; pages = {k: [] for k in preservation.KINDS}
    refs = []; raw_bytes = 0; rows = 0
    while active:
        pair.seal(); batch, active = active[:BUNDLE], active[BUNDLE:]
        sql = 'SELECT jsonb_build_array(' + ','.join('openlegal_checkpoint.preservation_page(' + _quote(k)
            + ',' + ('NULL::jsonb' if after is None else _quote(canonical(after).decode().strip()) + '::jsonb') + ',128)' for k, after in batch) + ')'
        values, ref, size = pair.sql(sql, cid=cid, audit=True)
        need(type(values) is list and len(values) == len(batch), 'pair_m32_bundle')
        raw_bytes += size + 256; refs.append(ref)
        for (kind, after), value in zip(batch, values):
            preservation.validate_page(kind, value, after)
            counts[kind] += len(value['rows']); need(counts[kind] <= preservation.MAX_ROWS, 'pair_m32_relation_bound')
            rows += len(value['rows']); need(rows <= 15 * preservation.MAX_ROWS and len(refs) <= preservation.MAX_PAGES, 'pair_m32_total_bound')
            # Only compact page hashes/cursors in RAM/index; raw bundle already
            # exists ONCE on both media with its actual command reference.
            pages[kind].append(digest(canonical(value)))
            if value['has_more']: active.append((kind, value['next_key']))
    return {'page_hashes': pages, 'raw_bundles': refs, 'raw_bytes': raw_bytes, 'bundles': len(refs)}


def _digest_sql(source):
    return ("SELECT jsonb_build_object('rows',count(*),'sha256',encode(sha256(convert_to("
        "COALESCE(string_agg(h,'' ORDER BY h COLLATE \"C\"),''),'UTF8')),'hex')) FROM "
        "(SELECT encode(sha256(convert_to(to_jsonb(t)::text,'UTF8')),'hex') h FROM " + source + ' t) q')


def _audit_legacy(pair, maintenance, audit, *, cid=None):
    """Reconstruct exactly the four permitted old-row deltas in SQL's old domain.

    Old full-row hashes, migration1..27 and reference hash must still equal the
    real legacy baseline. No M29 hash is compared to the legacy hash domain.
    """
    old = pair.old['payloads']['preservation']; operation = _quote(maintenance['provider_recovery_operation']) + '::uuid'
    ids = maintenance['new_uncertainty_ids']; idsql = ','.join(_quote(v) + '::uuid' for v in ids)
    audit_where = '(SELECT a.* FROM openlegal_admin.provider_recovery_audit a WHERE operation_id=' + operation + ')'
    query = ("SELECT jsonb_build_object('audit',(SELECT to_jsonb(a) FROM " + audit_where + " a),"
        "'state',openlegal_admin.provider_state(),'all_slots_free',NOT EXISTS(SELECT 1 FROM generate_series(0,16)s "
        "WHERE openlegal_admin.slot_locked(s)),'new_observations',COALESCE((SELECT jsonb_agg(to_jsonb(u) ORDER BY identity) "
        "FROM openlegal_admin.uncertainty_observation u WHERE " + ('identity IN (' + idsql + ')' if ids else 'false') + "),'[]'::jsonb))")
    value = pair.sql(query, cid=cid, legacy_read=True, audit=True)[0]
    need(value['audit'] == audit and value['all_slots_free'] is True, 'pair_recovery_audit_or_owner_changed')
    state = value['state']; before = audit['before_state']; result = audit['result']; req = audit['plan']['request']
    need(audit['actor'] == req['actor'] and audit['actual_writers_stopped_at'] == req['quiescence']['writers_stopped_at']
         and audit['applied_at'] == result['applied_at']
         and audit['plan']['created_at'] <= audit['applied_at'] < audit['plan']['expires_at'], 'pair_audit_native_correspondence')
    need(audit['plan']['snapshot']['state'] == before and req['resume'] is False and req['resolve_legacy'] is False
         and req['waits'] == [] and state['budget'] == before['budget'] and state['schema'] == before['schema']
         and state['storage_id'] == before['storage_id'] and state['corpus_control'] == before['corpus_control']
         and state['budget']['unresolved_response'] is False and state['budget']['admission_owner'] is None
         and state['ledger'] == [] and state['legacy_lock_held'] is False, 'pair_recovery_charges_or_data_changed')
    immutable = lambda r: {k: v for k, v in r.items() if k not in ('generation', 'recovery_hold', 'hold_started_at')}
    need(immutable(state['control']) == immutable(before['control']) and state['control']['recovery_hold'] is True
         and state['control']['generation'] == result['hold_generation']
         and state['control']['generation'] != before['control']['generation']
         and state['control']['hold_started_at'] == (before['control']['hold_started_at']
             if before['control']['hold_started_at'] is not None else audit['applied_at'])
         and result['recovery_hold'] is True and result['resolved_legacy'] is False
         and result['advanced_waits'] == [] and result['unchanged_waits'] == []
         and sorted(result['resolved_owners']) == sorted(req['owners'])
         == sorted(r['owner'] for r in before['ledger']), 'pair_recovery_delta')
    owners = {r['owner']: r['slot'] for r in before['ledger']}
    need(set(ids) <= set(owners) and len(value['new_observations']) == len(ids), 'pair_recovery_observation_delta')
    for row in value['new_observations']:
        _shape(row, ('identity','kind','owner','slot','first_observed_at'), 'pair_new_observation_shape')
        need(row['first_observed_at'] is None, 'pair_observation_timestamp_not_fabricated')
        need(row['identity'] in ids and row['kind'] == 'slot' and row['owner'] == row['identity']
             and row['slot'] == owners[row['identity']], 'pair_recovery_observation_identity')
    transformed = {
        'openlegal_admin.provider_control': '(SELECT (jsonb_populate_record(NULL::openlegal_admin.provider_control,a.before_state->\'control\')).* FROM ' + audit_where + ' a)',
        'openlegal.provider_request_admission': '(SELECT r.* FROM ' + audit_where + " a CROSS JOIN LATERAL jsonb_populate_recordset(NULL::openlegal.provider_request_admission,COALESCE((SELECT jsonb_agg(v-'lock_held') FROM jsonb_array_elements(a.before_state->'ledger')v),'[]'::jsonb)) r)",
        'openlegal_admin.provider_recovery_audit': '(SELECT a.* FROM openlegal_admin.provider_recovery_audit a WHERE operation_id<>' + operation + ')',
        'openlegal_admin.uncertainty_observation': '(SELECT u.* FROM openlegal_admin.uncertainty_observation u WHERE ' + ('identity NOT IN (' + idsql + ')' if ids else 'true') + ')'}
    observed = {}
    for table, expected in old['tables'].items():
        need(re.fullmatch(r'openlegal(?:_admin)?\.[a-z_][a-z0-9_]{0,62}', table), 'pair_old_table_domain')
        actual = pair.sql(_digest_sql(transformed.get(table, table)), cid=cid, legacy_read=True, audit=True)[0]
        need(actual == expected, 'pair_old_rows_not_preserved'); observed[table] = actual
    for sql, key in ((migration.REFERENCES_SQL, 'references'), (migration.OLD_MIGRATIONS_SQL, 'migration_history')):
        need(pair.sql(sql, cid=cid, legacy_read=True, audit=True)[0] == old[key], 'pair_old_references_or_migrations_changed')
    expected = [dict(row, success=True) for row in migration._manifest(pair.work['initial_migration'])]
    need(pair.sql(migration.MIGRATIONS_SQL, cid=cid, legacy_read=True)[0] == expected, 'pair_schema33_checksums')
    pair.sessions(cid)
    return digest(canonical(observed))


def _roots(pair):
    """Original full generations AND four seeded manifests/markers/references."""
    cfg = pair.sc[0]; seed._scan_sources(pair.b, cfg, pair.baseline); seed._bound(pair.b)
    seed._qualify_media(pair.b, pair.op, cfg, empty=False)
    references = []
    for ref in pair.seed_result['reference_pages']:
        row = _raw_reference(pair.j, {'phase': seed.PHASE, 'name': ref['name'], 'sha256': ref['sha256']})
        need(row['offset'] == len(references), 'pair_seed_reference_page_order')
        references.extend(row['rows']); need(len(references) <= preservation.MAX_ROWS, 'pair_seed_reference_bound')
    doc = []
    for root in pair.seed_result['roots']:
        path = root['host_path']; role = root['role']; seed._metadata_tree(pair.b, path, new=True)
        actual = blobs.blob_inventory(path, deadline=pair.b._driver_deadline, controls=True, online=False)
        seed._metadata_tree(pair.b, path, new=True); seed._bound(pair.b)
        need(actual['permanent_sha256'] == root['manifest_sha256'] and actual['root_identity'] ==
             {'dev': root['root_dev'], 'ino': root['root_ino']} and not actual['health'], 'pair_seed_generation_changed')
        expected_controls = {blobs.MARKER} | ({blobs.GUARDS + '/.directory.lock'} if role == 'cache_replica' else set())
        need({r['path'] for r in actual['controls']} == expected_controls, 'pair_blob_control_changed')
        fd = pair.b._initial_blob_seed_fds['root-' + role][0]
        marker, _ = blobs._regular(fd, blobs.MARKER, 0, (0o640,), 16384)
        need(digest(marker) == root['marker_sha256'], 'pair_seed_marker_changed')
        manual._reference_fd(fd, [r for r in references if r['role'] == role.split('_')[0]], pair.b._driver_deadline)
        doc.append({k: root[k] for k in ('container_path', 'filesystem_id', 'kernel_filesystem_id', 'marker_sha256',
            'role', 'root_dev', 'root_gid', 'root_ino', 'root_mode', 'root_uid')})
    binding = digest(b'openlegal-blob-seed-binding-v1\n' + canonical({'boot_id': pair.work['host']['boot_id'],
        'kind': 'openlegal_blob_seed_binding', 'managed_epoch': cfg['managed_epoch'],
        'roots': sorted(doc, key=lambda r: r['role']), 'schema_version': 1}))
    need(binding == pair.seed_result['blob_binding_sha256'], 'pair_seed_four_root_binding')
    _check(pair.b); return binding


def _spec(pair, action):
    t = pair.p['target']; q = pair.p['peer']; n = pair.p['network']
    mounts = [{'source': str(Path(t['parent']) / 'data'), 'destination': PGDATA, 'rw': action == 'backup' or action == 'standby', 'type': 'bind'},
              {'source': str(pair.workspace), 'destination': CONTROL, 'rw': False, 'type': 'bind'}]
    for key in ('ca', 'certificate', 'private_key'):
        mounts.append({'source': q[key]['path'], 'destination': TRUST + '/client_' + key, 'rw': False, 'type': 'bind'})
    if action == 'standby':
        for key, ref in pair.p['standby_trust'].items():
            mounts.append({'source': ref['path'], 'destination': TRUST + '/server_' + key, 'rw': False, 'type': 'bind'})
    name = 'openlegal-initial-' + action + '-' + pair.cfg['operation_id']
    if action == 'backup':
        command = ['/usr/local/bin/pg_basebackup', '--no-password', '--pgdata=' + PGDATA, '--format=plain',
            '--wal-method=stream', '--max-rate=' + str(pair.p['target']['capacity']['max_rate_kib']), '--checkpoint=fast', '--write-recovery-conf', '--manifest-checksums=SHA256', '--dbname=' + _conninfo(pair)]
    elif action == 'verify': command = ['/usr/local/bin/pg_verifybackup', PGDATA]
    else: command = ['/usr/local/bin/postgres', '-D', PGDATA, '-c', 'config_file=' + CONTROL + '/standby.conf']
    if action != 'standby':
        gate = 'n=0; while [ ! -f ' + CONTROL + '/go-' + action + ' ]; do n=$((n+1)); [ "$n" -lt 600 ] || exit 125; sleep 0.1; done; exec "$@"'
        entry = '/bin/sh'; argv = ['-c', gate, '--', *command]
    else: entry, argv = command[0], command[1:]
    network = 'none' if action == 'verify' else n['id']
    args = ['create', '--pull=never', '--name', name, '--restart=no', '--read-only', '--network', network,
        '--user', '70:70', '--label', 'openlegal.operation=' + pair.cfg['operation_id'],
        '--label', 'openlegal.epoch=' + pair.sc[0]['managed_epoch'], '--cap-drop=ALL', '--security-opt=no-new-privileges',
        '--pids-limit=256', '--memory=1g', '--memory-swap=1g', '--cpus=1',
        '--log-driver=json-file', '--log-opt=max-size=1m', '--log-opt=max-file=1',
        '--tmpfs', '/tmp:rw,noexec,nosuid,size=32m', '--tmpfs', '/var/run/postgresql:rw,nosuid,size=16m,uid=70,gid=70,mode=0700',
        '-e', 'LC_ALL=C', '-e', 'LANG=C', '-e', 'PGPASSFILE=/dev/null', '--entrypoint', entry]
    if action != 'verify':
        address = n['standby_address'] if action == 'standby' else n['backup_address']
        args += ['--ip6' if ':' in address else '--ip', address]
    args += blobs._mount_args(mounts) + [pair.p['image_id'], *argv]
    return {'name': name, 'mounts': mounts, 'entrypoint': entry, 'argv': argv, 'network': network, 'command': command, 'args': args}


def _conninfo(pair):
    q = pair.p['peer']; n = pair.p['network']
    # Every token has a closed non-quoting grammar. No password/ambient service.
    return ('host=' + q['server_name'] + ' hostaddr=' + n['primary_address'] + ' port=5432 user=' + q['role']
        + ' application_name=' + q['application_name'] + ' sslmode=verify-full sslrootcert=' + TRUST + '/client_ca'
        + ' sslcert=' + TRUST + '/client_certificate sslkey=' + TRUST + '/client_private_key'
        + ' passfile=/dev/null connect_timeout=5')


def _inspect(pair, cid, spec, *, running):
    need(cid in pair.created, 'pair_unowned_container')
    raw, _ = pair.command(['inspect', '--type', 'container', '--format', INSPECT, cid]); item = decode(raw)
    need(item['Id'] == cid and item['Image'] == pair.p['image_id']
         and item['State']['Running'] is running and all(item['State'][k] is False for k in ('Paused', 'Restarting', 'Dead', 'OOMKilled')),
         'pair_native_state')
    need(item['Config']['User'] == '70:70' and item['Config']['Entrypoint'] == [spec['entrypoint']]
         and item['Config']['Cmd'] == spec['argv'] and item['Config']['Labels']['openlegal.operation'] == pair.cfg['operation_id']
         and item['Config']['Labels']['openlegal.epoch'] == pair.sc[0]['managed_epoch']
         and item['HostConfig']['ReadonlyRootfs'] is True and item['HostConfig']['NetworkMode'] == spec['network']
         and item['HostConfig']['RestartPolicy']['Name'] == 'no' and item['HostConfig']['CapDrop'] == ['ALL']
         and item['HostConfig']['Memory']==1073741824 and item['HostConfig']['MemorySwap']==1073741824
         and item['HostConfig']['PidsLimit']==256
         and item['HostConfig']['Tmpfs']=={'/tmp':'rw,noexec,nosuid,size=32m',
             '/var/run/postgresql':'rw,nosuid,size=16m,uid=70,gid=70,mode=0700'}
         and 'no-new-privileges' in item['HostConfig']['SecurityOpt'], 'pair_native_spec')
    mounts = sorted([{'source': m['Source'], 'destination': m['Destination'], 'rw': m['RW'], 'type': m['Type']}
                     for m in item['Mounts']], key=lambda m: m['destination'])
    # Docker includes explicitly declared tmpfs mounts in inspect.Mounts on some
    # versions. Only the two exact source-owned tmpfs destinations are additional.
    fixed = [m for m in mounts if m['type'] != 'tmpfs']
    need(fixed == sorted(spec['mounts'], key=lambda m: m['destination'])
         and all(m['destination'] in ('/tmp', '/var/run/postgresql') for m in mounts if m['type'] == 'tmpfs'), 'pair_native_mounts')
    if running and spec['network'] != 'none':
        endpoint = item['NetworkSettings']['Networks'][pair.p['network']['name']]
        address = pair.p['network']['standby_address' if spec['command'][0].endswith('/postgres') else 'backup_address']
        need(endpoint['NetworkID'] == pair.p['network']['id']
             and (endpoint['GlobalIPv6Address'] if ':' in address else endpoint['IPAddress']) == address, 'pair_native_endpoint')
    return item


def _start(pair, action):
    payload_end=min(pair.b._driver_deadline,time.monotonic()+(900 if action=='backup' else 120 if action=='verify' else 30))
    spec = _spec(pair, action); pair.save(action + '-create-intent', {'args_sha256': digest(canonical(spec['args'])), 'name': spec['name']})
    raw, _ = pair.command(spec['args'],end=payload_end); cid = blobs._created_id(raw)
    need(cid not in pair.created and cid != pair.primary, 'pair_new_container_identity')
    pair.created[cid] = spec
    # Register the exact CREATED CID before any start transmission or journal
    # I/O can fail. Unknown start keeps this provisional owner even when no
    # Docker inspection/PID receipt was obtained. Never infer absence from it.
    owned={'container_id':cid,'spec':spec,'payload_end':payload_end,'start_transmitted':False,'closure':None}
    owned['authority_binding']={'operation_id':pair.cfg['operation_id'],'parent_pid':pair.parent,
        'parent_start_ticks':pair.ticks,'journal_operation':pair.j.operation,
        'inventory_sha256':digest(pair.raw),'authority_sha256':digest(pair.authraw),
        'original_absolute_deadline':pair.b._driver_deadline}
    pair.helpers.append(owned)
    pair.b._initial_pg_pair_pending_ownership[cid]=owned
    pair.save(action + '-created', {'container_id': cid, 'name': spec['name']})
    owned['start_transmitted']=True
    pair.command(['start', cid],end=payload_end); item = _inspect(pair, cid, spec, running=True)
    pid = item['State']['Pid']; need(type(pid) is int and pid > 0, 'pair_native_pid')
    process = {'pid': pid, 'start_ticks': Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()[19],
               'boot_id': pair.work['host']['boot_id']}
    groups = linux.process_cgroup(pid)
    need(_helper_groups(groups,cid), 'pair_native_cgroup')
    group = root_fd('/sys/fs/cgroup' + groups[0][3:])
    pair.b._initial_pg_pair_retained.append(group)
    pidfd = os.pidfd_open(pid); pair.b._initial_pg_pair_retained.append(pidfd)
    need(linux.process_cgroup(pid)==groups
         and Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()[19]==process['start_ticks'],
         'pair_native_anchor_changed')
    owned.update(process=process,groups=groups,pidfd=pidfd,cgroup_fd=group)
    pair.save(action + '-native-start', {'container_id': cid, 'process': process, 'groups': groups,
        'cgroup_identity': {'dev': os.fstat(group).st_dev, 'ino': os.fstat(group).st_ino}})
    if action!='standby':
        _control_write(pair,'go-'+action,b'go\n',mode=0o644)
    # Observe the REAL executable transition; a missed fast success refuses.
    end = min(pair.b._driver_deadline,payload_end,time.monotonic() + 15)
    tool = Path(spec['command'][0]).name
    while True:
        _check(pair.b, end)
        need(not select.select([pidfd], [], [], 0)[0], 'pair_missed_native_exec_transition')
        try:
            exe = os.open('/proc/' + str(pid) + '/exe', os.O_RDONLY | os.O_CLOEXEC)
            try:
                h = hashlib.sha256()
                while block := os.read(exe, 1024**2): h.update(block)
                observed = h.hexdigest()
            finally: os.close(exe)
        except FileNotFoundError:
            need(False, 'pair_native_exec_missing')
        need(Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()[19] == process['start_ticks'], 'pair_native_pid_reused')
        if observed == pair.p['tools'][tool]: break
        time.sleep(.01)
    pair.save(action + '-native-exec', {'container_id': cid, 'process': process, 'tool': tool, 'sha256': observed})
    # A qualified running standby intentionally remains held. Only failed
    # startup is a helper teardown target; later bind/audit failure cannot stop
    # the already returned standby or downgrade synchronous configuration.
    if action=='standby':
        # Preserve exactly Docker's observed normalized mounts, including only
        # tmpfs entries actually returned by this Docker version.
        owned['observed_mounts'] = sorted([{'source':m['Source'],'destination':m['Destination'],
            'rw':m['RW'],'type':m['Type']} for m in item['Mounts']],key=lambda m:m['destination'])
        pair.b._initial_pg_pair_standby_native=owned
        pair.helpers.remove(owned)
        del pair.b._initial_pg_pair_pending_ownership[cid]
    return cid, owned


def _wait(pair, cid, native, timeout):
    end=min(pair.b._driver_deadline,native.get('payload_end',pair.b._driver_deadline),time.monotonic()+timeout);last=None
    try:
        while not select.select([native['pidfd']],[],[],0)[0]:
            _check(pair.b,end)
            if native['spec']['command'][0].endswith('/pg_basebackup'):last=_copy_watch(pair)
            time.sleep(min(.1,max(0,end-time.monotonic())))
        _check(pair.b,end)
        raw,cmd=pair.command(['wait',cid],timeout=3,end=end)
        need(raw==b'0\n','pair_native_wait_not_zero')
        item=_inspect(pair,cid,native['spec'],running=False)
        need(type(item['State']['ExitCode']) is int and item['State']['ExitCode']==0 and item['State']['Pid']==0
             and select.select([native['pidfd']],[],[],0)[0],'pair_native_exit_unknown')
        manual._process_absent(native['process']);need(linux.members(native['groups'])==[],'pair_native_descendants_remain')
        if native['spec']['command'][0].endswith('/pg_basebackup'):last=_copy_watch(pair)
        native['closure']=pair.save('native-closed',{'container_id':cid,'process':native['process'],'actual_exit_code':0,
            'owned_members':[],'wait_command':cmd,'last_observed_copy_budget':last,
            'monitor_profile':'best_effort_observed_not_peak_or_reservation'})
        del pair.b._initial_pg_pair_pending_ownership[cid]
    except BaseException:
        _stop_owned_helper(pair,native)
        raise


def _control_write(pair, name, raw, *, mode=0o640):
    need(re.fullmatch('[a-z][a-z0-9.-]{0,63}', name), 'pair_control_name')
    pair.seal(); fd = root_fd(pair.workspace)
    try: blobs._write_exclusive(fd, name, raw, 0, 70, mode)
    finally: os.close(fd)
    _check(pair.b)


def _prepare_destination(pair):
    pair.seal(); parent = pair.target_fd
    need(not os.listdir(parent), 'pair_target_not_new')
    os.mkdir('data', 0o700, dir_fd=parent)
    fd = os.open('data', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
    pair.b._initial_pg_pair_retained.append(fd)
    os.fchown(fd, 70, 70); os.fchmod(fd, 0o700); os.fsync(fd); os.fsync(parent)
    pair.data_fd = fd; pair.data_path = str(Path(pair.p['target']['parent']) / 'data')
    pair.fds.append((fd, pair.data_path, _fd_id(fd)))
    pair.workspace = Path('/tmp/openlegal-initial-pg-pair-' + pair.cfg['operation_id'])
    os.mkdir(pair.workspace, 0o710); os.chown(pair.workspace, 0, 70); os.chmod(pair.workspace, 0o710)
    w = pair.hold(pair.workspace); pair.b._initial_pg_pair_retained.append(w)
    pair.save('destination-created', {'path': pair.data_path, 'identity': {'dev': os.fstat(fd).st_dev, 'ino': os.fstat(fd).st_ino},
        'control_path': str(pair.workspace)})


def _layout(pair, *, cid=None, native=None):
    fd = pair.data_fd; path = pair.data_path
    s = os.fstat(fd); need(s.st_uid == s.st_gid == 70 and stat.S_IMODE(s.st_mode) == 0o700, 'pair_pgdata_owner')
    wal = os.open('pg_wal', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
    spaces = os.open('pg_tblspc', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
    try:
        need(not os.listdir(spaces) and _fd_id(fd)[3] == _fd_id(wal)[3], 'pair_external_wal_or_tablespace')
        table = blobs._mounts(); blobs._path_mount(fd, path, table); blobs._no_nested_mounts(path, table)
        if native is not None:
            pid = native['process']['pid']; before = Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()[19]
            need(before == native['process']['start_ticks'] and not select.select([native['pidfd']], [], [], 0)[0], 'pair_standby_lifetime')
            data = pair.b._container_directory(pid, PGDATA); nw = pair.b._container_directory(pid, PGDATA + '/pg_wal')
            cwd = os.open('/proc/' + str(pid) + '/cwd', os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            try:
                inode = lambda f: (os.fstat(f).st_dev, os.fstat(f).st_ino)
                need(inode(data) == inode(fd) == inode(cwd) and inode(nw) == inode(wal), 'pair_native_pgdata_correspondence')
                legacy._native_mounts(pid, PGDATA, data, nw)
                if 'data_fd' not in native:
                    native['data_fd']=os.dup(data);native['wal_fd']=os.dup(nw)
                    pair.b._initial_pg_pair_retained.extend((native['data_fd'],native['wal_fd']))
            finally:
                for f in (data, nw, cwd): os.close(f)
            need(Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()[19] == before, 'pair_standby_reused')
    finally: os.close(wal); os.close(spaces)
    cap = pair.p['target']['capacity']
    observed = _copy_size(pair,path)
    _check(pair.b); pair.seal(); return observed


def _configure_standby(pair):
    _layout(pair)
    # Only new backup destination files are replaced. No original PG/Blob file.
    fd = pair.data_fd; before, info = blobs._regular(fd, 'postgresql.auto.conf', 70, (0o600,), 1024**2)
    _control_write(pair, 'backup-auto-sha256', (digest(before) + '\n').encode())
    need(signature(os.stat('postgresql.auto.conf', dir_fd=fd, follow_symlinks=False)) == signature(info), 'pair_backup_auto_changed')
    auto = ("primary_conninfo='" + _conninfo(pair) + "'\n").encode()
    blobs._write_exclusive(fd, 'openlegal-initial.auto.pending', auto, 70, 70, 0o600)
    need(signature(os.stat('postgresql.auto.conf', dir_fd=fd, follow_symlinks=False)) == signature(info), 'pair_backup_auto_cas')
    os.rename('openlegal-initial.auto.pending', 'postgresql.auto.conf', src_dir_fd=fd, dst_dir_fd=fd); os.fsync(fd)
    actual, _ = blobs._regular(fd, 'postgresql.auto.conf', 70, (0o600,), 1024**2)
    need(actual == auto, 'pair_standby_auto_readback')
    fixed = ("data_directory='" + PGDATA + "'\nlisten_addresses='*'\nport=5432\nssl=on\n"
        "ssl_ca_file='" + TRUST + "/server_ca'\nssl_cert_file='" + TRUST + "/server_certificate'\n"
        "ssl_key_file='" + TRUST + "/server_private_key'\nhba_file='" + TRUST + "/server_hba'\n"
        "fsync=on\nfull_page_writes=on\nsynchronous_commit=on\nsynchronous_standby_names=''\n"
        "shared_preload_libraries=''\nrestore_command=''\narchive_command=''\narchive_mode=off\n"
        "unix_socket_directories='/var/run/postgresql'\n").encode()
    _control_write(pair, 'standby.conf', fixed)
    need(os.stat('standby.signal', dir_fd=fd, follow_symlinks=False).st_size == 0, 'pair_backup_not_standby')
    pair.save('standby-configuration', {'old_auto_sha256': digest(before), 'new_auto_sha256': digest(auto), 'fixed_config_sha256': digest(fixed)})


def _replay(pair, primary):
    end = min(pair.b._driver_deadline, time.monotonic() + 15); first = None
    target = _lsn(primary['wal_lsn']); polls=0
    while True:
        polls+=1;need(polls<=64,'pair_replay_poll_bound')
        _check(pair.b, end)
        row = pair.position(pair.standby)
        need(row['system_identifier'] == primary['system_identifier'] and row['timeline'] == primary['timeline'], 'pair_standby_control_identity')
        if first is None: first = row
        need(row['postmaster_started_at'] == first['postmaster_started_at'], 'pair_standby_postmaster_changed')
        if _lsn(row['receive_lsn']) >= target and _lsn(row['replay_lsn']) >= target: return row
        time.sleep(.25)


def _slot(pair, *, apply=False):
    p = pair.p['primary_slot']; native = pair.native[0]
    data = Path(pair.work['legacy_prepare']['pgdata']['host_path'])
    need(Path(p['path']) == data / 'postgresql.auto.conf', 'pair_only_prepared_auto_slot')
    before = driver._reference(p['before']); after = driver._reference(p['after'])
    need(before == b"synchronous_standby_names=''\n", 'pair_auto_slot_must_be_prepared_exclusively')
    fd = native['data_fd']; raw, info = blobs._regular(fd, 'postgresql.auto.conf', 70, (0o600,), 1024**2)
    need(raw == before and {'dev': info.st_dev, 'ino': info.st_ino} == p['identity'], 'pair_prepared_slot_cas')
    own = pair.b._container_directory(native['process']['pid'], pair.work['legacy_prepare']['pgdata']['container_path'])
    try: need((os.fstat(own).st_dev, os.fstat(own).st_ino) == (os.fstat(fd).st_dev, os.fstat(fd).st_ino), 'pair_native_slot_parent')
    finally: os.close(own)
    if not apply: return
    _primary_stable(pair); pair.save('sync-slot-cas-intent', {'before_sha256': digest(before), 'after_sha256': digest(after), 'identity': p['identity']})
    name = 'openlegal-initial-' + pair.cfg['operation_id'] + '.pending'
    blobs._write_exclusive(fd, name, after, 70, 70, 0o600)
    fresh, now = blobs._regular(fd, 'postgresql.auto.conf', 70, (0o600,), 1024**2)
    need(fresh == before and signature(now) == signature(info), 'pair_slot_changed_before_replace')
    _primary_stable(pair); pair.seal()
    os.rename(name, 'postgresql.auto.conf', src_dir_fd=fd, dst_dir_fd=fd); os.fsync(fd)
    actual, _ = blobs._regular(fd, 'postgresql.auto.conf', 70, (0o600,), 1024**2)
    need(actual == after and Path(p['path']).read_bytes() == after, 'pair_slot_readback')
    _primary_stable(pair)
    signal.pidfd_send_signal(pair.primary_pidfd, signal.SIGHUP, None, 0)
    end = min(pair.b._driver_deadline, time.monotonic() + 15); polls=0
    while True:
        polls+=1;need(polls<=64,'pair_reload_poll_bound')
        _check(pair.b, end); value = _settings(pair)
        need(value['commit'] == value['fsync'] == value['full_page_writes'] == 'on' and value['pending_restart'] is False, 'pair_strict_gucs')
        if value['sync'] == 'FIRST 1 (' + pair.p['peer']['application_name'] + ')': break
        need(value['sync'] == '', 'pair_unexpected_sync_configuration'); time.sleep(.25)
    _primary_stable(pair); pair.save('sync-effective', value)


def _candidate(pair, expected):
    value = pair.sql('SELECT openlegal_replication.observe_candidate(' + _quote(canonical(expected).decode().strip()) + '::jsonb)')[0]
    _shape(value, ('kind', 'proposed_binding', 'candidate'), 'pair_candidate_shape')
    need(value['kind'] == 'openlegal_replication_candidate_peer' and value['proposed_binding'] == expected, 'pair_candidate_intent')
    need(set(value['candidate']) == (BIND_KEYS-{'managed_epoch','blob_binding_sha256'}) | {'postmaster_started_at','sender_pid','sender_started_at','prefix_lsn','flush_lsn'}, 'pair_candidate_not_stored_control')
    return _peer(value['candidate'], expected)


def _strict(pair, expected, *, candidate=None, bind=False):
    """Native COMMIT/fresh proof ONE2s; historical archives use original end.

    No journal walk occurs inside this timer. Native results are held in bounded
    RAM, then BOTH durable archives AND actual SQL closure must succeed before
    this held phase acknowledges anything. This is never a public ACK/Ready.
    """
    pair.seal();intent=None
    if bind:
        need(pair.b._initial_pg_pair_bind_state=='UNATTEMPTED','pair_bind_once')
        intent=pair.save('first-bind-intent',{'operation_id':pair.cfg['operation_id'],'expected_old':None,
            'proposed_binding':expected,'candidate':candidate,'ack_budget_seconds':2,
            'caller_original_absolute_deadline':pair.b._driver_deadline})
    end=min(pair.b._driver_deadline,time.monotonic()+2);records=[];proof=None;failure=None
    native_started=time.monotonic()
    try:
        _check(pair.b,end)
        if bind:pair.b._initial_pg_pair_bind_state='TRANSMITTED_UNKNOWN'
        values,record=_critical_execute(pair,'bind' if bind else 'proof',expected,intent,end)
        records.append(record)
        need(type(values) is list and len(values)==(3 if bind else 2),'pair_commit_output_count')
        before,after=values[-2:];polls=0
        while _lsn(after['flush_lsn']) < _lsn(before['prefix_lsn']):
            _check(pair.b,end);polls+=1;need(polls<=32,'pair_ack_poll_bound')
            a=_peer(before,expected);b=_peer(after,expected)
            need(all(a[k]==b[k] for k in a if k not in ('prefix_lsn','flush_lsn')),'pair_ack_poll_lifetime')
            time.sleep(min(.02,max(0,end-time.monotonic())))
            next_values,record=_critical_execute(pair,'flush',expected,None,end);records.append(record)
            need(len(next_values)==1,'pair_flush_output_count');after=next_values[0]
        if bind:
            result=values[0]
            _shape(result,('operation_id','intent_sha256','previous_binding','new_binding','peer_before_commit'),'pair_bind_result')
            need(result['operation_id']==pair.cfg['operation_id'] and result['intent_sha256']==intent['sha256']
                 and result['previous_binding'] is None and _normalized_binding(result['new_binding'])==expected,'pair_initial_bind_changed')
            committed=_peer(result['peer_before_commit'],expected)
            _proof(committed,after,expected,previous=candidate)
            proof=_proof(before,after,expected,previous=committed)
        else:proof=_proof(before,after,expected,previous=candidate)
        _critical_light(pair,end);_check(pair.b,end)
    except BaseException as error:
        failure=error
    native_finished=time.monotonic()
    # ALWAYS record exact captured native history if original authority/time
    # still permits it. Never relabel it a fresh observation after archival I/O.
    refs=[];archive_start=time.monotonic()
    for record in records:refs.append(pair.save('strict-native-command',record))
    pair.save('strict-native-window',{'native_started_at':native_started,'native_finished_at':native_finished,
        'native_absolute_deadline':end,'native_elapsed_seconds':native_finished-native_started,
        'archive_started_at':archive_start,'archive_elapsed_seconds':time.monotonic()-archive_start,
        'native_command_refs':refs,'native_proof_valid':failure is None,'public_ack':False,
        'observation_status':'historical_native_window_not_current_after_archive'})
    if failure is not None:raise failure
    pair.sessions();pair.seal()
    need(proof is not None,'pair_native_proof_missing')
    if bind:pair.b._initial_pg_pair_bind_state='ACKNOWLEDGED'
    return dict(proof,native_window={'started':native_started,'finished':native_finished,'deadline':end},
                native_command_refs=refs,observation_status='historical_native_proof')


def _critical_light(pair,end):
    """Only local sealed authority/FD checks; NO journal or external command."""
    _check(pair.b,end);pair.lock.check();driver._docker_binding(pair.b,pair.cfg['operation_id'])
    need(not getattr(pair.b,'_initial_pg_pair_cancelled',False),'pair_cancelled')
    need(driver._reference(pair.inventory_ref)==pair.raw and driver._reference(pair.authority_ref)==pair.authraw
         and time.time()<pair.auth['valid_until'],'pair_critical_authority_changed')
    private_bytes(Path(__file__).resolve(),pair.cfg['source_sha256'])
    native=pair.native[0];process=native['process']
    need(Path('/proc/sys/kernel/random/boot_id').read_text().strip()==process['boot_id']
         and not select.select([pair.primary_pidfd],[],[],0)[0]
         and Path('/proc/'+str(process['pid'])+'/stat').read_text().rsplit(')',1)[1].split()[19]==process['start_ticks'],
         'pair_critical_native_lifetime')
    path=pair.work['legacy_prepare']['pgdata']['host_path']
    fresh=root_fd(path)
    try:need(_fd_id(fresh)==_fd_id(native['data_fd']),'pair_critical_pgdata_rebound')
    finally:os.close(fresh)
    _check(pair.b,end)


def _critical_execute(pair,kind,expected,intent,end):
    """Source-fixed critical SQL on ONE held/pinned native Docker executable.

    No inventory SQL/argv fragment, callback, general-purpose record flag or
    journal adapter is accepted. This dedicated port exists only because a
    full-history journal walk cannot be included in the native2s proof clock.
    """
    need(kind in ('bind','proof','flush'),'pair_critical_fixed_kind');_binding(expected)
    _critical_light(pair,end)
    if kind=='bind':
        need(type(intent) is dict and pin(intent.get('sha256')),'pair_critical_durable_intent')
        sql=('BEGIN; SELECT openlegal_checkpoint.bind_replica('+_quote(pair.cfg['operation_id'])+'::uuid,'
            +_quote(intent['sha256'])+',NULL::jsonb,'+_quote(canonical(expected).decode().strip())+'::jsonb); COMMIT; '
            'SELECT openlegal_replication.capture_prefix(); SELECT openlegal_replication.observe_flush()')
    elif kind=='proof':sql='SELECT openlegal_replication.capture_prefix(); SELECT openlegal_replication.observe_flush()'
    else:sql='SELECT openlegal_replication.observe_flush()'
    prefix=("SET statement_timeout='2s'; SET lock_timeout='1s'; SET transaction_timeout='5s'; "
        "SET standard_conforming_strings=on; SET TIME ZONE 'UTC'; SET DateStyle='ISO,YMD'; SET bytea_output='hex'; ")
    args=['exec','-i','-u','70',pair.primary,'/usr/bin/env',*[v for key in UNSET for v in ('-u',key)],
        'PGPASSFILE=/dev/null','/usr/local/bin/psql','-X','-w','-h','/var/run/postgresql','-p','5432','-qAt',
        '-v','ON_ERROR_STOP=1','-U',pair.p['recovery_role'],'-d','openlegal']
    stdin=(prefix+sql+';\n').encode();binding=pair.work['linux']['binaries']['docker'];path=binding['path']
    linux.secure_path(path);fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:
        before=os.fstat(fd);need(stat.S_ISREG(before.st_mode) and before.st_uid==0 and not before.st_mode&0o022
            and before.st_size<=256*1024**2,'pair_critical_binary_identity')
        h=hashlib.sha256()
        while block:=os.read(fd,1024**2):_check(pair.b,end);h.update(block)
        need(h.hexdigest()==binding['sha256'] and signature(os.fstat(fd))==signature(before),'pair_critical_binary_changed')
        _critical_light(pair,end)
        dispatch=['--config',str(pair.b._docker_config[0]),'--host','unix:///var/run/docker.sock']
        started=time.monotonic()
        result=driver.processes.execute([path,*dispatch,*args],executable_fd=fd,stdin=stdin,
            env=linux.ENV,deadline=end,max_output=65536)
        finished=time.monotonic()
    finally:os.close(fd)
    record={k:getattr(result,k) for k in ('actual_exit_code','expired','output_limit_exceeded','owned_processes_closed',
        'unexpected_descendants','started','interrupted','elapsed_seconds')}
    record.update(kind='openlegal_initial_pair_native_sql',query_kind=kind,binary_sha256=binding['sha256'],
        arguments_sha256=digest(canonical(dispatch+args)),stdin_sha256=digest(stdin),
        stdout_sha256=digest(result.stdout),stdout_bytes=len(result.stdout),stdout_hex=result.stdout.hex(),
        stderr_sha256=digest(result.stderr),stderr_bytes=len(result.stderr),
        native_started_at=started,native_finished_at=finished,native_absolute_deadline=end,
        original_operation_deadline=pair.b._driver_deadline)
    # Record is retained even if the native result is unsuccessful. The caller
    # archives it first and then rejects; no guessed exit/closure is invented.
    pair.b._initial_pg_pair_critical_result=record
    valid=(result.started and result.owned_processes_closed and not result.expired
        and not result.output_limit_exceeded and not result.unexpected_descendants and not result.interrupted
        and type(result.actual_exit_code) is int and result.actual_exit_code==0 and finished<end)
    if not valid:return _CriticalFailure(record),record
    try:
        values=[decode(row) for row in result.stdout.splitlines()]
    except BaseException:
        return _CriticalFailure(record),record
    return values,record


class _CriticalFailure:
    """Opaque unsuccessful native outcome; never a parsed SQL success value."""
    def __init__(self,record):self.record=record


def _normalized_binding(value):
    _shape(value, BIND_KEYS, 'pair_stored_binding_fields')
    value = copy.deepcopy(value)
    serial = value['standby_client_serial']
    need(type(serial) in (str, int) and type(serial) is not bool and str(serial).isdecimal(), 'pair_stored_serial')
    value['standby_client_serial'] = str(int(serial)); value['standby_addr'] = _host_address(value['standby_addr'])
    return _binding(value)


def _run(backend, shared_lock, inventory_ref, authority_ref):
    """Create/bind ONE held pair from the same live seeded ROOT operation.

    Return is not a native producer OS exit, pre-ACK certificate or Ready. All
    retained FDs/state stay on backend, including on failure. No automatic retry,
    cleanup, async downgrade, provider apply/release or fabricated manual history.
    """
    need(type(backend) is driver.Backend and type(shared_lock) is journals.SharedLock and os.geteuid() == 0
         and not any(callable(v) for v in vars(backend).values())
         and not getattr(backend, '_initial_pg_pair_started', False), 'pair_live_native_backend')
    _check(backend)
    cfg, auth, raw, authraw, p, praw, sc = _load(inventory_ref, authority_ref)
    live = _live(backend, shared_lock, cfg, sc)
    maintenance, audit = _maintenance(live[0], cfg['maintenance'], cfg, sc[4], sc[8], sc[9],
        getattr(backend, '_initial_provider_maintenance_last_exit', None), sc[7])
    backend._initial_pg_pair_started = True; backend._initial_pg_pair_retained = []
    backend._initial_pg_pair_bind_state = 'UNATTEMPTED'
    pair = _Pair(backend, shared_lock, cfg, auth, raw, authraw, p, praw, sc, live, maintenance)
    pair.inventory_ref = inventory_ref; pair.authority_ref = authority_ref
    backend._initial_pg_pair_live = pair
    pair.j.begin(PHASE)
    try:
        input_binding=pair.save('input-binding', {'inventory':inventory_ref,'authority':authority_ref,
            'inventory_sha256':digest(raw),'authority_sha256':digest(authraw),'source_sha256':cfg['source_sha256'],
            'source_graph_sha256':pair.op.source_graph_sha256,'original_absolute_deadline':backend._driver_deadline,
            'role_prepare':pair.role_context['reference'],'role_source':pair.role_context['producer_source'],
            'maintenance':cfg['maintenance'],'seed_inventory':cfg['seed_inventory'],'seed_authority':cfg['seed_authority']})
        pair.seal(); _native_primary(pair); _primary_stable(pair)
        migration._fence(backend, pair.op, pair.old['payloads']['drain'])
        _catalog(pair); pair.sessions(); original = pair.position()
        need(original['system_identifier'] == pair.work['legacy_prepare']['expected_system_identifier']
             and original['timeline'] == pair.work['legacy_prepare']['expected_timeline'], 'pair_primary_control')
        need(pair.sql('SELECT openlegal.replication_required()')[0] is False, 'pair_control_already_bound')
        settings = _settings(pair)
        need(settings['sync'] == '' and settings['source'] == 'configuration file'
             and settings['commit'] == settings['fsync'] == settings['full_page_writes'] == 'on'
             and settings['wal_level'] == 'replica' and int(settings['senders']) >= 2
             and settings['pending_restart'] is False, 'pair_primary_not_prepared')
        _slot(pair); _network(pair); _tls(pair)
        for ref in p['standby_trust'].values(): _trust_file(ref)
        for role, target in sc[0]['targets'].items():
            for oldpath in (Path(target['host_path']), Path(target['parent']['path'])):
                need(not Path(p['target']['parent']).is_relative_to(oldpath)
                     and not oldpath.is_relative_to(p['target']['parent']), 'pair_pg_blob_target_overlap')
        protected = [Path(r['path']) for r in sc[0]['sources'].values()]
        protected += [Path(pair.work['legacy_prepare']['pgdata']['host_path']), Path(pair.work['backup_root']['path'])]
        protected += [Path(r['path']) for r in pair.work['journal_roots']]
        need(all(not Path(p['target']['parent']).is_relative_to(v) and not v.is_relative_to(p['target']['parent']) for v in protected),
             'pair_protected_target_overlap')
        pair.target_fd = pair.hold(p['target']['parent']); backend._initial_pg_pair_retained.append(pair.target_fd)
        _capacity(pair, empty=True)
        cap = p['target']['capacity']; src = pair.work['legacy_prepare']['pgdata']
        whole = _copy_size(pair,src['host_path'])
        _check(backend)
        _audit_legacy(pair, maintenance, audit); binding = _roots(pair)
        control_state=pair.sql('SELECT openlegal_checkpoint.state()')[0]
        baseline = _m32(pair)
        pair.preservation_baseline = copy.deepcopy(baseline)
        pair.budget = _reserve(pair.j, baseline['bundles'], baseline['raw_bytes'], len(pair.old['payloads']['preservation']['tables']))
        pair.reservation_ref = pair.save('global-journal-reservation', dict(pair.budget, whole_cluster=whole,
            acknowledged_seed_reference_pages=pair.seed_result['reference_pages']))
        # Last read-only boundaries precede the first mutation. The selected
        # observed-SSD or explicit-ZFS profile, journal budget, maintenance and
        # trust are required. Plain SSD has NO hard allocation guarantee.
        _capacity(pair, empty=True); _primary_stable(pair); pair.sessions(); _slot(pair)
        migration._fence(backend, pair.op, pair.old['payloads']['drain'])
        _prepare_destination(pair)
        backup, native_backup = _start(pair, 'backup'); _wait(pair, backup, native_backup, 900)
        _layout(pair); _capacity(pair, empty=False)
        manifest_before=_manifest_hash(pair)
        verified, native_verify = _start(pair, 'verify'); _wait(pair, verified, native_verify, 120)
        need(_manifest_hash(pair)==manifest_before,'pair_backup_manifest_changed')
        pair.save('verified-backup-manifest',{'sha256':manifest_before})
        _layout(pair); _configure_standby(pair)
        standby, native_standby = _start(pair, 'standby'); pair.standby = standby
        wait='n=0; until /usr/local/bin/pg_isready -q -h /var/run/postgresql -p 5432; do n=$((n+1)); [ "$n" -lt 150 ] || exit 125; sleep 0.1; done'
        pair.command(['exec','-u','70',standby,'/bin/sh','-c',wait],timeout=16)
        _layout(pair, cid=standby, native=native_standby)
        current = pair.position(); _same_primary(original, current)
        _replay(pair, current); pair.sessions(standby)
        _audit_legacy(pair, maintenance, audit, cid=standby)
        need(pair.sql('SELECT openlegal_checkpoint.state()',cid=standby)[0]==control_state,'pair_standby_controls_changed')
        standby_before = _m32(pair, cid=standby)
        need(standby_before['page_hashes'] == baseline['page_hashes'], 'pair_standby_preservation_before_bind')
        _slot(pair, apply=True)
        expected = _binding({'managed_epoch': sc[0]['managed_epoch'], 'primary_system_identifier': current['system_identifier'],
            'timeline': current['timeline'], 'standby_name': p['peer']['application_name'], 'standby_role': p['peer']['role'],
            'standby_addr': p['network']['standby_address'], 'standby_client_dn': p['peer']['client_dn'],
            'standby_issuer_dn': p['peer']['issuer_dn'], 'standby_client_serial': p['peer']['client_serial'],
            'blob_binding_sha256': binding})
        candidate = _candidate(pair, expected)
        # All long pre-bind audits completed; refresh actual lifetime/fences,
        # namespace mounts/config/trust and peer immediately before sole intent.
        _primary_stable(pair); _layout(pair, cid=standby, native=native_standby); _tls(pair)
        migration._fence(backend, pair.op, pair.old['payloads']['drain']); pair.sessions(); pair.sessions(standby)
        _catalog(pair)
        candidate = _candidate(pair, expected)
        acknowledged = _strict(pair, expected, candidate=candidate, bind=True)
        _audit_legacy(pair, maintenance, audit)
        need(pair.sql('SELECT openlegal_checkpoint.state()')[0]==control_state,'pair_primary_final_controls_changed')
        need(_m32(pair)['page_hashes'] == baseline['page_hashes'], 'pair_primary_final_preservation')
        final_primary = pair.position(); _same_primary(original, final_primary); _replay(pair, final_primary)
        _audit_legacy(pair, maintenance, audit, cid=standby)
        need(pair.sql('SELECT openlegal_checkpoint.state()',cid=standby)[0]==control_state,'pair_standby_final_controls_changed')
        need(_m32(pair, cid=standby)['page_hashes'] == baseline['page_hashes'], 'pair_standby_final_preservation')
        need(_roots(pair) == binding, 'pair_final_blob_binding')
        _capacity(pair, empty=False); _primary_stable(pair); _layout(pair, cid=standby, native=native_standby)
        migration._fence(backend, pair.op, pair.old['payloads']['drain']); pair.sessions(); pair.sessions(standby)
        _catalog(pair)
        final_ack = _strict(pair, expected, candidate=_candidate(pair, expected))
        complete_ref=pair.save('complete', {'kind': 'openlegal_initial_pg_pair_result', 'operation_id': cfg['operation_id'],
            'input_binding':input_binding,'source_sha256':cfg['source_sha256'],
            'inventory_sha256':digest(raw),'authority_sha256':digest(authraw),
            'managed_epoch': expected['managed_epoch'], 'primary_container_id': pair.primary, 'standby_container_id': standby,
            'primary_process': pair.native[0]['process'], 'standby_process': native_standby['process'],
            'binding': expected, 'first_bind_ack': acknowledged, 'final_ack': final_ack,
            'blob_roots': pair.seed_result['roots'], 'preservation_page_hashes': baseline['page_hashes'],
            'maintenance_audit_sha256': digest(canonical(audit)),
            'provider_recovery_operation':maintenance['provider_recovery_operation'], 'media_profile': blobs.MEDIA_PROFILE,
            'ready': False, 'provider_called': False, 'roles_created': False, 'released': False,
            'automatic_retry': False, 'outer_actual_os_exit': 'NOT_OBSERVABLE_IN_PROCESS'})
        pair.seal()
        pending = {'kind': 'openlegal_initial_pg_pair_pending_parent', 'complete':complete_ref, 'standby_container_id': standby,
                'binding': expected, 'ready': False, 'outer_actual_os_exit': 'NOT_OBSERVABLE_IN_PROCESS'}
        pair._run_return = canonical(pending)
        return pending
    except BaseException:
        for helper in pair.helpers:_stop_owned_helper(pair,helper)
        try: pair.save('failed', {'status': 'FAILED_OR_UNKNOWN', 'bind_state': backend._initial_pg_pair_bind_state,
            'known_created_ids': sorted(pair.created), 'automatic_retry': False, 'automatic_cleanup': False,
            'ready': False, 'sync_downgrade_permitted': False, 'retained_fd_count': len(backend._initial_pg_pair_retained)})
        except BaseException: pass
        raise


def _same_primary(before, after):
    need(all(after[k] == before[k] for k in before if k != 'wal_lsn')
         and _lsn(after['wal_lsn']) >= _lsn(before['wal_lsn']), 'pair_primary_database_lifetime_changed')


def _manifest_hash(pair):
    fd=os.open('backup_manifest',os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=pair.data_fd)
    try:
        st=os.fstat(fd);need(stat.S_ISREG(st.st_mode) and st.st_nlink==1 and st.st_uid==70
            and stat.S_IMODE(st.st_mode)==0o600 and 1<=st.st_size<=64*1024**2,'pair_backup_manifest_profile')
        h=hashlib.sha256();size=0
        while block:=os.read(fd,1024**2):
            _check(pair.b);size+=len(block);need(size<=64*1024**2,'pair_backup_manifest_bound');h.update(block)
        need(size==st.st_size and signature(os.fstat(fd))==signature(st)
            and signature(os.stat('backup_manifest',dir_fd=pair.data_fd,follow_symlinks=False))==signature(st), 'pair_manifest_rebound')
        return h.hexdigest()
    finally:os.close(fd)


def run(backend, shared_lock, inventory_ref, authority_ref):
    """Dedicated ROOT-only entry; cancellation is a sticky refusal, never retry."""
    need(type(backend) is driver.Backend and threading.current_thread() is threading.main_thread()
         and len(list(Path('/proc/self/task').iterdir()))==1
         and not driver.processes._children(os.getpid()), 'pair_dedicated_root_context')
    _intake_run_gate(backend, shared_lock, inventory_ref, authority_ref)
    previous={};backend._initial_pg_pair_cancelled=False
    def interrupted(number,frame):
        del number,frame
        backend._initial_pg_pair_cancelled=True
    try:
        for number in (signal.SIGTERM,signal.SIGINT):
            previous[number]=signal.getsignal(number);signal.signal(number,interrupted)
        pending = _run(backend,shared_lock,inventory_ref,authority_ref)
    finally:
        for number,handler in previous.items():signal.signal(number,handler)
    # Restoring BOTH handlers is part of success. No saved JSON, native bind or
    # successful inner return can mint completion before that finally succeeds.
    _complete_run(backend, shared_lock, pending)
    return pending


@dataclass(frozen=True, eq=False, slots=True)
class _Completed:
    """Identity-only live completion, minted after the outer finally succeeds."""
    owner: object
    backend: object
    lock: object
    journal: object
    lock_fd: int
    reference: tuple
    raw: bytes
    pending: bytes
    parent: bytes
    end: float
    clock: object
    budget: bytes
    count: int
    bytes: int
    context: bytes
    native: bytes
    held: tuple


def _completed_command(row):
    need(type(row) is dict and type(row.get('actual_exit_code')) is int
         and row['actual_exit_code'] == 0 and row.get('started') is True
         and row.get('owned_processes_closed') is True
         and all(row.get(k) is False for k in ('expired', 'interrupted',
             'unexpected_descendants', 'output_limit_exceeded')), 'pair_completed_native_not_closed_zero')


def _completed_ack(j, value, binding, *, first):
    """Reconstruct the actual archived native first-bind/final proof, read only."""
    _binding(binding)
    _shape(value, ('before','after','fixed_target','native_window','native_command_refs','observation_status'),
           'pair_completed_ack_shape')
    need(value['observation_status'] == 'historical_native_proof'
         and type(value['native_command_refs']) is list
         and 1 <= len(value['native_command_refs']) <= 33, 'pair_completed_ack_kind')
    records = []; outputs = []
    for ref in value['native_command_refs']:
        need(ref['phase'] == PHASE, 'pair_completed_ack_phase')
        row = _raw_reference(j, ref); _completed_command(row)
        need(row['kind'] == 'openlegal_initial_pair_native_sql'
             and row['query_kind'] in ('bind','proof','flush'), 'pair_completed_native_query')
        need(type(row['stdout_hex']) is str and len(row['stdout_hex']) <= 2 * 65536,
             'pair_completed_native_output_bound')
        raw = bytes.fromhex(row['stdout_hex'])
        need(type(row['stdout_bytes']) is int and row['stdout_bytes'] == len(raw)
             and row['stdout_sha256'] == digest(raw), 'pair_completed_native_output_hash')
        records.append(row); outputs.append([decode(line) for line in raw.splitlines()])
    need(records[0]['query_kind'] == ('bind' if first else 'proof')
         and all(r['query_kind'] == 'flush' for r in records[1:])
         and len(outputs[0]) == (3 if first else 2)
         and all(len(v) == 1 for v in outputs[1:]), 'pair_completed_ack_sequence')
    initial = outputs[0]
    if first:
        bound = initial[0]
        _shape(bound, ('operation_id','intent_sha256','previous_binding','new_binding','peer_before_commit'),
               'pair_completed_bind_shape')
        need(bound['previous_binding'] is None and _normalized_binding(bound['new_binding']) == binding,
             'pair_completed_not_first_bind')
        intents = [row['raw'] for paths, rows in j.history + [(j.current(), j.artifacts)]
                   for name, row in rows.items()
                   if paths and paths[0][0].name == PHASE and name.endswith('-first-bind-intent.json')]
        need(len(intents) == 1 and digest(intents[0]) == bound['intent_sha256'],
             'pair_completed_bind_intent')
        intent = decode(intents[0])
        need(intent['operation_id'] == bound['operation_id'] == j.operation
             and intent['expected_old'] is None and canonical(intent['proposed_binding']) == canonical(binding),
             'pair_completed_initial_intent')
    before = initial[-2]; final = initial[-1] if len(records) == 1 else outputs[-1][0]
    if first:
        committed = _peer(bound['peer_before_commit'], binding)
        _proof(committed, final, binding, previous=intent['candidate'])
        proof = _proof(before, final, binding, previous=committed)
    else:
        proof = _proof(before, final, binding)
    need(all(canonical(value[k]) == canonical(proof[k]) for k in ('before','after','fixed_target')),
         'pair_completed_ack_proof')
    window = value['native_window']
    _shape(window, ('started','finished','deadline'), 'pair_completed_native_window')
    need(all(type(window[k]) in (int,float) and math.isfinite(window[k]) for k in window)
         and window['started'] <= window['finished'] < window['deadline'] <= window['started'] + 2,
         'pair_completed_native_budget')
    last = window['started']
    for row in records:
        need(all(type(row[k]) in (int,float) and math.isfinite(row[k]) for k in
                 ('native_started_at','native_finished_at','native_absolute_deadline','original_operation_deadline'))
             and last <= row['native_started_at'] <= row['native_finished_at'] <= window['finished']
             and row['native_absolute_deadline'] == window['deadline'] <= row['original_operation_deadline'],
             'pair_completed_native_history')
        last = row['native_finished_at']
    return records


def _completed_context(p):
    # Preserve exact original inputs (including source/role/history bytes), not
    # merely the mutable Python fields used to recheck their filesystem pins.
    context = {k: (digest(v) if type(v) is bytes else v) for k,v in p.role_context.items()}
    return canonical({'cfg':p.cfg, 'authority':p.auth, 'provision':p.p,
        'inventory_ref':p.inventory_ref, 'authority_ref':p.authority_ref,
        'raw_sha256':[digest(v) for v in (p.raw,p.authraw,p.praw)],
        'seed_chain':[digest(v if type(v) is bytes else canonical(v)) for v in p.sc],
        'work':p.work, 'old':p.old, 'baseline':p.baseline, 'seed_result':p.seed_result,
        'role_context':context, 'preservation_baseline':p.preservation_baseline,
        'reservation_ref':p.reservation_ref,
        'held_roots':[(fd,path,list(identity)) for fd,path,identity in p.fds],
        'operation_source_graph_sha256':p.op.source_graph_sha256,
        'journal_binding':p.j.binding, 'journal_operation':p.j.operation})


def _completed_native_binding(p):
    def row(native):
        return {k:native[k] for k in ('container_id','process','groups','pidfd','cgroup_fd','spec',
                                      'closure','observed_mounts','data_fd','wal_fd',
                                      'authority_binding','start_transmitted','payload_end') if k in native}
    return canonical({'primary_pidfd':p.primary_pidfd, 'primary':p.primary,
        'primary_native':{k:p.native[0][k] for k in ('process','data_fd','wal_fd','cgroup_fd','groups')},
        'standby':p.standby, 'standby_native':row(p.b._initial_pg_pair_standby_native),
        'helpers':[row(v) for v in p.helpers], 'created':p.created})


def _completed_held(p):
    descriptors = set(p.b._initial_pg_pair_retained)
    descriptors.add(p.lock.fd)
    descriptors.update(getattr(p.b,name) for name in driver.GUARDS if hasattr(p.b,name))
    descriptors.update(row[0] for row in p.b._cgroup_fds.values())
    descriptors.update(row[0] for row in p.b._initial_blob_seed_fds.values())
    descriptors.update(getattr(p.b,'_legacy_prepare_primary_fds',[]))
    descriptors.update(getattr(p.b,'_initial_blob_seed_native_fds',[]))
    rows = []
    for fd in sorted(descriptors):
        st = os.fstat(fd)
        rows.append((fd,st.st_dev,st.st_ino,st.st_mode,st.st_uid,st.st_gid))
    return tuple(rows)


def _completed_local_mounts(p, native, container_path):
    """Only held namespace FDs and procfs; never Docker inspection or fresh SQL."""
    process = native['process']; pid = process['pid']
    need(type(pid) is int and pid > 0 and process['boot_id'] == p.work['host']['boot_id']
         and driver.processes._identity(pid) == process['start_ticks'], 'pair_completed_native_lifetime')
    data = p.b._container_directory(pid, container_path)
    wal = None
    try:
        wal = p.b._container_directory(pid, container_path + '/pg_wal')
        # Retained primary FDs name HOST paths; these fresh FDs name the
        # postmaster's bind mounts. Compare the underlying file/FS identity,
        # preserving namespace mount IDs for the separate native validator.
        for actual, held in ((data,native['data_fd']),(wal,native['wal_fd'])):
            a, b = _fd_id(actual), _fd_id(held)
            need(a[:3] + a[4:] == b[:3] + b[4:], 'pair_completed_native_mount_rebound')
        legacy._native_mounts(pid, container_path, data, wal)
        need(driver.processes._identity(pid) == process['start_ticks'], 'pair_completed_native_reused')
    finally:
        os.close(data)
        if wal is not None: os.close(wal)


def _completed_native(p):
    need(type(p.helpers) is list and len(p.helpers) == 2
         and getattr(p.b,'_initial_pg_pair_pending_ownership',None) == {}, 'pair_completed_pending_ownership')
    authority_binding = canonical({'operation_id':p.cfg['operation_id'],'parent_pid':p.parent,
        'parent_start_ticks':p.ticks,'journal_operation':p.j.operation,
        'inventory_sha256':digest(p.raw),'authority_sha256':digest(p.authraw),
        'original_absolute_deadline':p.original_end})
    tools = []
    for helper in p.helpers:
        cid = helper['container_id']; tools.append(Path(helper['spec']['command'][0]).name)
        need(canonical(helper['authority_binding']) == authority_binding
             and p.created.get(cid) is helper['spec'] and helper['closure'] is not None
             and helper['start_transmitted'] is True
             and select.select([helper['pidfd']],[],[],0)[0] and linux.members(helper['groups']) == [],
             'pair_completed_helper_not_closed')
        os.fstat(helper['cgroup_fd']); manual._process_absent(helper['process'])
        closure = _raw_reference(p.j, helper['closure'])
        need(helper['closure']['phase'] == PHASE and closure['container_id'] == cid
             and canonical(closure['process']) == canonical(helper['process'])
             and type(closure['actual_exit_code']) is int and closure['actual_exit_code'] == 0
             and closure['owned_members'] == [], 'pair_completed_helper_closure')
        need(closure['wait_command']['phase'] == PHASE, 'pair_completed_helper_wait_phase')
        wait = _raw_reference(p.j, closure['wait_command']); _completed_command(wait)
        dispatch = ['--config',str(p.b._docker_config[0]),'--host','unix:///var/run/docker.sock','wait',cid]
        need(wait['binary'] == 'docker' and wait['arguments_sha256'] == digest(canonical(dispatch))
             and wait['stdin_sha256'] is None and wait['stdout_sha256'] == digest(b'0\n')
             and type(wait['stdout_bytes']) is int and wait['stdout_bytes'] == 2,
             'pair_completed_helper_wait_zero')
    need(sorted(tools) == ['pg_basebackup','pg_verifybackup'], 'pair_completed_backup_verify_required')
    standby = p.b._initial_pg_pair_standby_native
    need(canonical(standby['authority_binding']) == authority_binding
         and standby['container_id'] == p.standby and p.created.get(p.standby) is standby['spec']
         and standby['spec']['command'][0] == '/usr/local/bin/postgres'
         and standby['closure'] is None and standby['start_transmitted'] is True
         and not select.select([p.primary_pidfd,standby['pidfd']],[],[],0)[0], 'pair_completed_held_pair_exited')
    mounts = standby['observed_mounts']
    need(type(mounts) is list and mounts == sorted(mounts,key=lambda m:m['destination'])
         and canonical([m for m in mounts if m['type'] != 'tmpfs'])
             == canonical(sorted(standby['spec']['mounts'],key=lambda m:m['destination']))
         and all(m['destination'] in ('/tmp','/var/run/postgresql') for m in mounts if m['type'] == 'tmpfs'),
         'pair_completed_observed_mounts')
    need(linux.process_cgroup(p.native[0]['process']['pid']) == p.native[0]['groups'],
         'pair_completed_primary_cgroup')
    need(linux.process_cgroup(standby['process']['pid']) == standby['groups'], 'pair_completed_standby_cgroup')
    _completed_local_mounts(p,p.native[0],p.work['legacy_prepare']['pgdata']['container_path'])
    _completed_local_mounts(p,standby,PGDATA)


def _completed_result(p, reference, raw):
    _shape(reference, ('phase','name','sha256'), 'pair_completed_reference')
    need(reference['phase'] == PHASE and pin(reference['sha256']) and type(raw) is bytes
         and p.j.read_phase_bytes(PHASE,reference['name'],reference['sha256']) == raw,
         'pair_completed_raw')
    value = decode(raw)
    _shape(value, ('kind','operation_id','input_binding','source_sha256','inventory_sha256','authority_sha256',
        'managed_epoch','primary_container_id','standby_container_id','primary_process','standby_process','binding',
        'first_bind_ack','final_ack','blob_roots','preservation_page_hashes','maintenance_audit_sha256',
        'provider_recovery_operation','media_profile','ready','provider_called','roles_created','released',
        'automatic_retry','outer_actual_os_exit'), 'pair_completed_result')
    need(value['kind'] == 'openlegal_initial_pg_pair_result' and value['operation_id'] == p.cfg['operation_id']
         and value['inventory_sha256'] == digest(p.raw) and value['authority_sha256'] == digest(p.authraw)
         and value['source_sha256'] == p.cfg['source_sha256']
         and value['managed_epoch'] == value['binding']['managed_epoch'] == p.sc[0]['managed_epoch']
         and value['primary_container_id'] == p.primary and value['standby_container_id'] == p.standby
         and canonical(value['primary_process']) == canonical(p.native[0]['process'])
         and canonical(value['standby_process']) == canonical(p.b._initial_pg_pair_standby_native['process'])
         and canonical(value['blob_roots']) == canonical(p.seed_result['roots'])
         and canonical(value['preservation_page_hashes']) == canonical(p.preservation_baseline['page_hashes'])
         and value['binding']['blob_binding_sha256'] == p.seed_result['blob_binding_sha256']
         and value['binding']['primary_system_identifier'] == p.work['legacy_prepare']['expected_system_identifier']
         and type(value['binding']['timeline']) is int
         and value['binding']['timeline'] == p.work['legacy_prepare']['expected_timeline']
         and value['media_profile'] == blobs.MEDIA_PROFILE
         and all(value[k] is False for k in ('ready','provider_called','roles_created','released','automatic_retry'))
         and value['outer_actual_os_exit'] == 'NOT_OBSERVABLE_IN_PROCESS', 'pair_completed_result_binding')
    reservation = _raw_reference(p.j,p.reservation_ref)
    need(p.reservation_ref['phase'] == PHASE
         and all(type(reservation[k]) is type(v) and reservation[k] == v for k,v in p.budget.items())
         and reservation['acknowledged_seed_reference_pages'] == p.seed_result['reference_pages'],
         'pair_completed_original_reservation')
    maintenance, audit = _maintenance(p.j,p.cfg['maintenance'],p.cfg,p.sc[4],p.sc[8],p.sc[9],
        getattr(p.b,'_initial_provider_maintenance_last_exit',None),p.sc[7])
    need(value['maintenance_audit_sha256'] == digest(canonical(audit))
         and value['provider_recovery_operation'] == maintenance['provider_recovery_operation'],
         'pair_completed_maintenance_binding')
    need(value['input_binding']['phase'] == PHASE, 'pair_completed_input_phase')
    inputs = _raw_reference(p.j,value['input_binding'])
    expected = {'inventory':p.inventory_ref,'authority':p.authority_ref,
        'inventory_sha256':digest(p.raw),'authority_sha256':digest(p.authraw),'source_sha256':p.cfg['source_sha256'],
        'source_graph_sha256':p.op.source_graph_sha256,'original_absolute_deadline':p.original_end,
        'role_prepare':p.role_context['reference'],'role_source':p.role_context['producer_source'],
        'maintenance':p.cfg['maintenance'],'seed_inventory':p.cfg['seed_inventory'],'seed_authority':p.cfg['seed_authority']}
    need(canonical(inputs) == canonical(expected), 'pair_completed_input_binding')
    for first,key in ((True,'first_bind_ack'),(False,'final_ack')):
        records = _completed_ack(p.j,value[key],value['binding'],first=first)
        need(all(type(r['original_operation_deadline']) is type(p.original_end)
                 and r['original_operation_deadline'] == p.original_end for r in records),
             'pair_completed_native_original_end')
    return value


def _completed_clock(p):
    """Retain supported driver clocks; this is no supervisor fork provenance."""
    _check(p.b,p.original_end)
    clock = p.original_clock
    need(clock is getattr(p.b,'_initial_driver_clock',None)
         and type(p.original_end) is float and p.b._driver_deadline == p.original_end,
         'pair_completed_original_clock')
    if clock is None:
        # Legacy driver.run(...), without prepare_clock, remains supported.
        # A failed/prepared/consumed clock can never downgrade to this branch.
        need(p.b._initial_driver_clock_started is None
             and p.b._initial_driver_clock_attempted is False,
             'pair_completed_unprepared_clock')
        return
    need(type(clock) is driver._Clock and clock.backend is p.b
         and clock.shared_lock is p.lock and clock.lock_fd == p.lock.fd
         and type(clock.end) is float and clock.end == p.original_end
         and p.b._initial_driver_clock_attempted is True
         and type(p.b._initial_driver_clock_started) is float
         and p.b._initial_driver_clock_started == clock.started
         and clock.boot_id == p.work['host']['boot_id']
         == Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
         'pair_completed_original_clock')
    need(type(clock.creator_pid) is int and clock.creator_pid > 0
         and clock.creator_pid in (os.getpid(),os.getppid())
         and driver.processes._identity(clock.creator_pid) == clock.creator_ticks,
         'pair_completed_clock_creator')
    _check(p.b,p.original_end)


def _complete_run(backend, shared_lock, pending):
    p = getattr(backend,'_initial_pg_pair_live',None)
    need(type(p) is _Pair and p.b is backend and p.lock is shared_lock
         and p.j is getattr(backend,'_initial_driver_journal',None) and p._completed is None
         and type(pending) is dict and canonical(pending) == p._run_return,
         'pair_completed_actual_run_return')
    p.seal()
    _shape(pending, ('kind','complete','standby_container_id','binding','ready','outer_actual_os_exit'),
           'pair_completed_pending_result')
    need(pending['kind'] == 'openlegal_initial_pg_pair_pending_parent' and pending['ready'] is False
         and pending['standby_container_id'] == p.standby
         and pending['outer_actual_os_exit'] == 'NOT_OBSERVABLE_IN_PROCESS'
         and p.j.phase_paths and p.j.phase_paths[0][0].name == PHASE
         and getattr(backend,'_initial_pg_pair_bind_state',None) == 'ACKNOWLEDGED',
         'pair_completed_current_result')
    ref = pending['complete']; raw = p.j.read_phase_bytes(ref['phase'],ref['name'],ref['sha256'])
    result = _completed_result(p,ref,raw)
    need(canonical(result['binding']) == canonical(pending['binding']), 'pair_completed_pending_binding')
    _completed_native(p); p.seal()
    context = _completed_context(p); native = _completed_native_binding(p); held = _completed_held(p)
    parent = canonical(_intake_parent())
    p.seal(); _check(backend,p.original_end)
    cap = _Completed(p,backend,shared_lock,p.j,shared_lock.fd,tuple(ref[k] for k in ('phase','name','sha256')),
        raw,canonical(pending),parent,p.original_end,p.original_clock,canonical(p.budget),
        p.j.count,p.j.bytes,context,native,held)
    _completed_clock(p)
    need(not getattr(backend,'_initial_pg_pair_cancelled',False), 'pair_cancelled')
    _check(backend,p.original_end)
    p._completed = cap; backend._initial_pg_pair_complete = cap


def completed_reference(backend, shared_lock):
    """Return this live owner's final reference after full outer completion."""
    cap = getattr(backend,'_initial_pg_pair_complete',None)
    need(type(cap) is _Completed, 'pair_completed_live_field')
    ref = dict(zip(('phase','name','sha256'),cap.reference))
    consume_completed_live(backend,shared_lock,ref)
    return ref


def _consume_completed(backend, shared_lock, reference, *, archived):
    need(type(backend) is driver.Backend and type(shared_lock) is journals.SharedLock
         and os.geteuid() == 0 and threading.current_thread() is threading.main_thread()
         and len(list(Path('/proc/self/task').iterdir())) == 1
         and not driver.processes._children(os.getpid())
         and not any(callable(v) for v in vars(backend).values()), 'pair_completed_root_context')
    cap = getattr(backend,'_initial_pg_pair_complete',None)
    need(type(cap) is _Completed and type(cap.owner) is _Pair, 'pair_completed_live_field')
    p = cap.owner
    need(p is getattr(backend,'_initial_pg_pair_live',None) and p._completed is cap
         and cap.backend is p.b is backend and cap.lock is p.lock is shared_lock
         and cap.journal is p.j is getattr(backend,'_initial_driver_journal',None)
         and type(shared_lock.fd) is int and shared_lock.fd == cap.lock_fd
         and canonical(_intake_parent()) == cap.parent and p.parent == os.getpid()
         and p.ticks == driver.processes._identity(os.getpid())
         and type(p.original_end) is type(cap.end) is type(backend._driver_deadline)
         and p.original_end == cap.end == backend._driver_deadline
         and p.original_clock is cap.clock is getattr(backend,'_initial_driver_clock',None)
         and getattr(backend,'_initial_pg_pair_started',False) is True
         and getattr(backend,'_initial_pg_pair_bind_state',None) == 'ACKNOWLEDGED',
         'pair_completed_same_owner')
    need(p.j.phase_paths and p.j.phase_paths[0][0].name in
         (('initial_epoch_observation','initial_epoch_install_verify') if archived else (PHASE,)),
         'pair_completed_current_phase')
    need(type(reference) is dict and canonical(reference) == canonical(dict(zip(('phase','name','sha256'),cap.reference))),
         'pair_completed_reference')
    need(type(p.budget) is dict and canonical(p.budget) == cap.budget
         and type(cap.count) is int and type(cap.bytes) is int
         and all(type(p.budget[k]) is int for k in ('start_count','start_bytes','records','bytes'))
         and p.budget['start_count'] <= cap.count <= p.budget['start_count'] + p.budget['records']
         and p.budget['start_bytes'] <= cap.bytes <= p.budget['start_bytes'] + p.budget['bytes']
         and type(p.j.count) is int and type(p.j.bytes) is int
         and cap.count <= p.j.count <= 4096 and cap.bytes <= p.j.bytes <= journals.MAX_PHASE_BYTES,
         'pair_completed_frozen_accounting')
    p.seal_context()
    need(_completed_context(p) == cap.context and _completed_native_binding(p) == cap.native
         and _completed_held(p) == cap.held and p._run_return == cap.pending,
         'pair_completed_frozen_context')
    result = _completed_result(p,reference,cap.raw)
    _completed_native(p)
    # No slow raw/source/mount read may renew or outlive the original clock.
    p.seal_context(); _completed_clock(p); _check(backend,cap.end)
    return result


def consume_completed_live(backend, shared_lock, reference):
    """Read-only Pair-phase consumer; no native commands, SQL or new budget."""
    return _consume_completed(backend,shared_lock,reference,archived=False)


def consume_archived_completed_live(backend, shared_lock, reference):
    """Only the two fixed existing certificate producer phases may read history."""
    return _consume_completed(backend,shared_lock,reference,archived=True)


def _host_address(value):
    need(type(value) is str, 'pair_host_inet')
    address=ipaddress.ip_interface(value)
    need(address.network.prefixlen==address.max_prefixlen,'pair_inet_must_be_host_prefix')
    return str(address.ip)
