"""Read-only sampled six-hour canary watch; never activation or final acceptance.

Only a hash-bound live canary_supervisor.Session caller may arm this module.
The supervisor owns activation and final hold/drain/preservation/public gates.
No serialized monotonic timestamp, callback, provider HTTP, SQL mutation, retry,
checkpoint action or automatic cleanup is accepted here. Imports are inert.
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import importlib.util
import json
import math
import os
import re
import stat
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

SELF = Path('/usr/local/lib/openlegal-storage-stability/canary_watch.py')
DURATION = 21600
INTERVAL = 55
SAMPLE_TIMEOUT = 5
MAX_GAP = 60
MAX_SAMPLES = 400
MAX_BUNDLE = 1024 * 1024
FILES = frozenset({'interface.py','inventory.py','journal.py','copy_tree.py','retention.py',
                  'checkpoint.py','linux_backend.py','restore_backend.py','manual_backend.py','provider_backend.py','bootstrap_backend.py','command_process.py','canary_native.py','preservation_backend.py','preservation_capture.py'})


class Refused(RuntimeError):
    """Static sanitized error code only."""


def need(ok, code):
    if not ok:
        raise Refused(code)


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)+'\n').encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def pin(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def uid(value):
    try:
        return type(value) is str and str(uuid.UUID(value)) == value and int(uuid.UUID(value)) != 0
    except (ValueError, AttributeError):
        return False


def decode(raw):
    need(type(raw) is bytes and len(raw) <= 4*1024*1024, 'json_bound')
    def pairs(rows):
        result = {}
        for key, value in rows:
            need(key not in result, 'json_duplicate')
            result[key] = value
        return result
    try:
        value = json.loads(raw, object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(Refused('json_nonfinite')))
    except (ValueError, UnicodeError, RecursionError):
        raise Refused('json_invalid') from None
    def walk(item, depth=0):
        need(depth <= 32, 'json_depth')
        if type(item) is dict:
            for child in item.values(): walk(child, depth+1)
        elif type(item) is list:
            for child in item: walk(child, depth+1)
        elif type(item) is float:
            need(math.isfinite(item), 'json_nonfinite')
    walk(value)
    return value


def private_bytes(path, expected=None):
    p = Path(path)
    need(p.is_absolute() and p.resolve() == p and '..' not in p.parts, 'private_path')
    for parent in p.parents:
        meta = parent.lstat()
        need(stat.S_ISDIR(meta.st_mode) and meta.st_uid == 0 and
             (not meta.st_mode & 0o022 or parent == Path('/tmp') and meta.st_mode & stat.S_ISVTX), 'private_ancestor')
    fd = os.open(p, os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
    with os.fdopen(fd, 'rb') as stream:
        a = os.fstat(stream.fileno())
        need(stat.S_ISREG(a.st_mode) and a.st_uid == a.st_gid == 0 and a.st_nlink == 1 and
             stat.S_IMODE(a.st_mode) == 0o600 and a.st_size <= 4*1024*1024, 'private_file')
        raw = stream.read(4*1024*1024+1); z = os.fstat(stream.fileno()); now = p.lstat()
    need(len(raw) == a.st_size and all(getattr(a,k) == getattr(z,k) == getattr(now,k)
         for k in ('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns')), 'private_changed')
    need(expected is None or pin(expected) and digest(raw) == expected, 'private_hash')
    return raw


def timestamp(value):
    need(type(value) is str and len(value) <= 40, 'utc_timestamp')
    try:
        stamp = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
        need(stamp.tzinfo is not None and stamp.utcoffset() == dt.timedelta(0), 'utc_timestamp')
        return stamp.timestamp()
    except (ValueError, OverflowError):
        raise Refused('utc_timestamp') from None


def next_timer(stamp):
    now = dt.datetime.fromtimestamp(stamp, dt.timezone.utc)
    boundary = now.replace(hour=19, minute=0, second=0, microsecond=0)
    if boundary.timestamp() <= stamp:
        boundary += dt.timedelta(days=1)
    return boundary.timestamp()


def validate_inventory(value):
    fields = {'schema_version','kind','operation_id','canary_id','recovery_operation_id',
              'storage_inventory','preservation_baseline','expected_pod_uid','supervisor_source','canary_policy'}
    need(type(value) is dict and set(value) == fields and type(value['schema_version']) is int and
         value['schema_version'] == 1 and value['kind'] == 'openlegal_canary_watch_inventory', 'watch_inventory')
    for field in ('operation_id','canary_id','recovery_operation_id','expected_pod_uid'):
        need(uid(value[field]), 'watch_uuid')
    for field in ('storage_inventory','preservation_baseline','supervisor_source'):
        row = value[field]
        need(type(row) is dict and set(row) == {'path','sha256'} and pin(row['sha256']) and
             type(row['path']) is str and Path(row['path']).is_absolute(), 'watch_input_pin')
    need(value['supervisor_source']['path'] == str(SELF.parent/'canary_supervisor.py'), 'fixed_supervisor')
    policy=value['canary_policy']
    need(type(policy)is dict and set(policy)=={'allowed_active_guard_keys','workload_pins','collection_job_pins'},'canary_policy_shape')
    keys=policy['allowed_active_guard_keys']
    need(type(keys)is list and 1<=len(keys)<=18 and all(type(k)is str for k in keys) and len(set(keys))==len(keys) and keys[0]=='serve' and
         all(type(k)is str and (k in ('serve','scheduler') or re.fullmatch(r'collection\.[0-9a-f-]{36}\.(?:0|[1-9][0-9]*)',k)) for k in keys),'canary_active_keys')
    workloads=policy['workload_pins'];jobs=policy['collection_job_pins']
    need(type(workloads)is list and 1<=len(workloads)<=2 and type(jobs)is list and len(jobs)<=16,'canary_workload_bound')
    for row in workloads:
        need(type(row)is dict and set(row)=={'kind','name','uid','role','runtime_image_id'} and row['kind']=='Deployment' and
             row['role']in('serve','scheduler') and type(row['runtime_image_id'])is str and re.fullmatch(r'[A-Za-z0-9:/._@-]*sha256:[0-9a-f]{64}',row['runtime_image_id']) and uid(row['uid']) and type(row['name'])is str and
             re.fullmatch('[a-z0-9][a-z0-9.-]{0,252}',row['name']),'canary_workload_pin')
    need(len({r['role']for r in workloads})==len(workloads) and len({r['uid']for r in workloads})==len(workloads),'canary_unique_workloads')
    need({r['role']for r in workloads}=={k for k in keys if k in('serve','scheduler')},'canary_workload_key_map')
    jobkeys=[]
    for row in jobs:
        need(type(row)is dict and set(row)=={'name','uid','request_id','epoch','spec_sha256','runtime_image_id'} and
             type(row['runtime_image_id'])is str and re.fullmatch(r'[A-Za-z0-9:/._@-]*sha256:[0-9a-f]{64}',row['runtime_image_id']) and uid(row['uid']) and uid(row['request_id']) and type(row['epoch'])is int and 0<=row['epoch']<=18446744073709551615 and
             pin(row['spec_sha256']) and type(row['name'])is str and re.fullmatch('[a-z0-9][a-z0-9.-]{0,252}',row['name']),'canary_job_pin')
        jobkeys.append('collection.'+row['request_id']+'.'+str(row['epoch']))
    need(len(set(jobkeys))==len(jobkeys) and set(jobkeys)==set(keys)-{'serve','scheduler'},'canary_job_key_map')
    return value


def validate_authority(value, inventory, inventory_sha, source_sha, now):
    expected = {'schema_version':1,'kind':'openlegal_canary_watch_authority','approved':True,
        'operation_id':inventory['operation_id'],'canary_id':inventory['canary_id'],
        'recovery_operation_id':inventory['recovery_operation_id'],'inventory_sha256':inventory_sha,
        'watch_source_sha256':source_sha,'supervisor_source_sha256':inventory['supervisor_source']['sha256'],
        'read_only':True,'duration_secs':DURATION,'max_global_requests':100,
        'sampling_interval_secs':INTERVAL,'mutations_permitted':False,'final_acceptance_permitted':False}
    need(type(value) is dict and set(value) == set(expected)|{'valid_until'} and
         all(type(value[k]) is type(v) and value[k] == v for k,v in expected.items()), 'watch_authority')
    need(type(value['valid_until']) in (int,float) and math.isfinite(value['valid_until']) and
         now+DURATION+MAX_GAP < value['valid_until'], 'watch_authority_window')
    need(now+DURATION+MAX_GAP < next_timer(now), 'daily_timer_overlap')


def canary(value, inventory):
    fields = {'schema_version','kind','observed_at','required_id','canary'}
    need(type(value) is dict and set(value) == fields and type(value['schema_version']) is int and
         value['schema_version'] == 1 and value['kind'] == 'openlegal_canary_observation', 'canary_observation')
    timestamp(value['observed_at'])
    need(value['required_id'] is None or uid(value['required_id']), 'canary_required_id')
    row = value['canary']
    if row is None:
        return None
    need(type(row) is dict and set(row) == {'id','operation_id','consumed','started_at','deadline','last_observed_at','ended_at'} and
         row['id'] == inventory['canary_id'] and row['operation_id'] == inventory['recovery_operation_id'] and
         value['required_id'] == row['id'] and type(row['consumed']) is int and 0 <= row['consumed'] <= 100,
         'canary_identity_or_cap')
    start, end, last = (timestamp(row[k]) for k in ('started_at','deadline','last_observed_at'))
    need(end-start == DURATION and start <= last <= min(end,timestamp(value['observed_at'])) and
         row['ended_at'] is None, 'canary_original_window_or_closed')
    return row


class Timeline:
    """Pure validation for actual clock samples; fake clocks never run Session."""
    def __init__(self, inventory):
        self.inventory = inventory; self.before = None; self.after = None; self.last = None
        self.count = 0; self.anchor = None; self.policy = None

    def accept(self, sample, stage):
        need(stage in ('before','after','observe') and type(sample) is dict, 'sample_stage')
        mono, wall = sample['monotonic'], sample['wall_time']
        need(all(type(x) in (int,float) and math.isfinite(x) for x in (mono,wall)), 'sample_clock')
        need(type(sample['elapsed_seconds']) in (int,float) and 0 <= sample['elapsed_seconds'] <= SAMPLE_TIMEOUT,
             'sample_duration')
        need(self.count < MAX_SAMPLES and abs(timestamp(sample['canary']['observed_at'])-wall) <= SAMPLE_TIMEOUT,
             'sample_bound_or_database_clock')
        if self.last is not None:
            delta = mono-self.last['monotonic']
            need(0 <= delta <= MAX_GAP and abs((wall-self.last['wall_time'])-delta) <= 1, 'sample_gap_or_clock_jump')
            need(abs((wall-self.before['wall_time'])-(mono-self.before['monotonic'])) <= 1, 'cumulative_clock_jump')
        need(wall < next_timer(self.before['wall_time'] if self.before else wall), 'daily_timer_crossed')
        row = canary(sample['canary'], self.inventory)
        native = sample['native']; pods = native.get('pods')
        need(type(pods) is list and len(pods) == 1 and pods[0]['uid'] == self.inventory['expected_pod_uid'] and
             pods[0].get('ready') is True and type(pods[0].get('restarts')) is int and pods[0]['restarts'] == 0,
             'watch_native_identity')
        if self.before is not None:
            need(pods == self.before['native']['pods'] and sample['provider']['state']['control'] == self.policy,
                 'watch_image_epoch_or_policy_changed')
        state = sample['provider']['state']; ownership = sample['provider']['admission_ownership']
        need(type(state['budget']['unresolved_response']) is bool and type(ownership['legacy_locked']) is bool and
             all(type(r.get('locked')) is bool and r['locked'] for r in ownership['slots']), 'unknown_provider_owner')
        if state['budget']['unresolved_response']:
            need(state['budget']['admission_owner'] is not None and ownership['legacy_locked'], 'provider_uncertainty')
        if stage == 'before':
            need(self.before is None and row is None, 'before_activation_one_use')
            self.before = copy.deepcopy(sample); self.policy = copy.deepcopy(state['control'])
        elif stage == 'after':
            need(self.before is not None and self.after is None and row is not None, 'after_activation_order')
            need(timestamp(self.before['canary']['observed_at']) <= timestamp(row['started_at']) <=
                 timestamp(sample['canary']['observed_at']) and mono-self.before['monotonic'] <= MAX_GAP,
                 'activation_not_bracketed')
            need(wall+DURATION < next_timer(self.before['wall_time']), 'daily_timer_overlap')
            self.after = copy.deepcopy(sample); self.anchor = mono
        else:
            need(self.after is not None and row is not None, 'observe_before_activation')
            original = self.after['canary']['canary']; previous = self.last['canary']['canary']
            need(all(row[k] == original[k] for k in ('id','operation_id','started_at','deadline')) and
                 row['consumed'] >= previous['consumed'] and
                 timestamp(row['last_observed_at']) >= timestamp(previous['last_observed_at']), 'canary_reset')
            if timestamp(self.last['canary']['observed_at']) >= timestamp(row['deadline']):
                need(row['consumed'] == previous['consumed'], 'canary_post_deadline_charge')
        self.last = copy.deepcopy(sample); self.count += 1
        return (self.anchor is not None and mono-self.anchor >= DURATION and
                timestamp(sample['canary']['observed_at']) >= timestamp(row['deadline']))


class SampleBuffer:
    """Read-only command evidence is accepted only after its dual-media bundle."""
    def __init__(self): self.values = {}; self.bytes = 0
    def save(self, name, value): return self.save_bytes(name, canonical(value))
    def save_bytes(self, name, raw):
        need(type(name) is str and re.fullmatch('[A-Za-z0-9_.-]{1,127}',name) and name not in self.values and
             type(raw) is bytes and self.bytes+len(raw) <= MAX_BUNDLE and len(self.values) < 128, 'sample_artifact_bound')
        self.values[name] = {'sha256':digest(raw),'json':decode(raw)}; self.bytes += len(raw)
        return digest(raw)


def _modules(storage, base):
    graph = storage['source_graph']
    need(type(graph) is dict and set(graph) == FILES and all(pin(p) for p in graph.values()), 'watch_exact_source_graph')
    sources = {name:private_bytes(base/name, sha) for name,sha in graph.items()}
    result = {}
    for name in ('interface','inventory','journal','copy_tree','retention','linux_backend','restore_backend','manual_backend','provider_backend','bootstrap_backend','command_process','canary_native','preservation_backend','preservation_capture','checkpoint'):
        path = base/(name+'.py'); spec = importlib.util.spec_from_file_location(name,path)
        module = importlib.util.module_from_spec(spec); sys.modules[name] = module
        exec(compile(sources[name+'.py'],str(path),'exec'),module.__dict__); result[name] = module
    result['inventory'].validate_inventory(storage)
    return result


def _prepared(inventory_path, inventory_sha256, authority_path, authority_sha256, source_sha256):
    need(os.geteuid() == 0 and sys.dont_write_bytecode and Path(__file__).resolve() == SELF, 'installed_root_no_bytecode')
    need(all(pin(p) for p in (inventory_sha256,authority_sha256,source_sha256)), 'required_raw_pins')
    files = {str(SELF):source_sha256, str(inventory_path):inventory_sha256, str(authority_path):authority_sha256}
    value = validate_inventory(decode(private_bytes(inventory_path,inventory_sha256)))
    authority = decode(private_bytes(authority_path,authority_sha256))
    validate_authority(authority,value,inventory_sha256,source_sha256,time.time())
    private_bytes(SELF,source_sha256)
    for name in ('storage_inventory','preservation_baseline','supervisor_source'):
        row = value[name]; private_bytes(row['path'],row['sha256']); files[row['path']] = row['sha256']
    storage = decode(private_bytes(value['storage_inventory']['path'],value['storage_inventory']['sha256']))
    modules = _modules(storage,SELF.parent)
    for name,sha in storage['source_graph'].items(): files[str(SELF.parent/name)] = sha
    return value,authority,storage,modules,files


class Session:
    """Live same-process lock owner. Methods accept no activation/effect callback."""
    @classmethod
    def arm(cls, *, inventory_path, inventory_sha256, authority_path, authority_sha256, source_sha256):
        args = (inventory_path,inventory_sha256,authority_path,authority_sha256,source_sha256)
        value,authority,storage,modules,files = _prepared(*args)
        main = getattr(sys.modules.get('__main__'),'__file__',None)
        need(main is not None and Path(main).resolve() == Path(value['supervisor_source']['path']), 'live_pinned_supervisor_required')
        obj = cls(); obj.value=value; obj.authority=authority; obj.storage=storage; obj.modules=modules; obj.files=files
        obj.pid=os.getpid(); obj.ticks=obj._ticks(); obj.nonce=str(uuid.uuid4()); obj.boot=storage['host']['boot_id']
        obj.lock=None; obj.journal=None; obj.failed=False; obj.timeline=Timeline(value); obj.sample_deadline=None; obj.cleanup_deadline=None; obj.cleanup_queries=(); obj.completed=False
        obj.binding={'kind':'openlegal_canary_watch_session','operation_id':value['operation_id'],
            'canary_id':value['canary_id'],'recovery_operation_id':value['recovery_operation_id'],
            'inventory_sha256':inventory_sha256,'watch_source_sha256':source_sha256,'authority_sha256':authority_sha256,
            'supervisor_source_sha256':value['supervisor_source']['sha256'],'session_nonce':obj.nonce,
            'pid':obj.pid,'process_start_ticks':obj.ticks,'host_boot_id':obj.boot,'sampling_not_continuous_proof':True,
            'preservation_baseline_sha256':value['preservation_baseline']['sha256'],
            'full_old_subset_proof':'REQUIRED_SEPARATE_FINAL_GATE','eligible_ack_publication_proof':'REQUIRED_SEPARATE_FINAL_GATE'}
        try:
            obj.lock=modules['journal'].SharedLock(storage['shared_lock']).acquire()
            obj.journal=modules['journal'].DualJournal(storage['journal_roots'],value['operation_id'],obj.binding)
            obj.journal.begin('canary-watch');obj.backend=obj._backend();obj._seal()
            return obj
        except BaseException:
            try:
                backend=getattr(obj,'backend',None)
                if backend is not None:backend.close()
            except BaseException:pass  # Preserve the original arm failure.
            finally:
                if obj.lock is not None:obj.lock.close()
            raise

    @staticmethod
    def _ticks():
        return int(Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19])

    def _seal(self):
        need(not self.failed and os.getpid()==self.pid and self._ticks()==self.ticks and
             Path('/proc/sys/kernel/random/boot_id').read_text().strip()==self.boot, 'watch_live_session_identity')
        need(time.time()<self.authority['valid_until'], 'watch_authority_expired')
        deadline=self.cleanup_deadline if self.cleanup_deadline is not None else self.sample_deadline
        if deadline is not None: need(time.monotonic()<deadline,'sample_deadline')
        for path,sha in self.files.items(): private_bytes(path,sha)
        self.lock.check();self.journal.check()

    def _backend(self):
        base=self.modules['linux_backend'].LinuxBackend
        allowed={f"SELECT openlegal_checkpoint.canary_observation('{self.value['canary_id']}'::uuid)",
                 f"SELECT openlegal_checkpoint.observe('{self.value['recovery_operation_id']}'::uuid)",
                 'SELECT openlegal_replication.capture_prefix()','SELECT openlegal_replication.observe_flush()'}
        session=self
        class ReadOnly(base):
            def _sql(inner,op,sql,**kwargs):
                need(sql in allowed,'watch_sql_not_readonly_allowlisted')
                kwargs['timeout']=min(kwargs.get('timeout',2),2)
                kwargs['deadline']=min(kwargs.get('deadline') or session.sample_deadline,session.sample_deadline)
                return super()._sql(op,sql,**kwargs)
            def _sql_mutation(inner,*args,**kwargs): raise Refused('watch_mutation_forbidden')
            def _close_owned_sql(inner,op,target,application):
                # A failed sample is never accepted late. Its exact owned SQL
                # nevertheless needs real closure even after its read deadline.
                need(target==op.inventory['linux']['postgres']['primary']['container_id'] and
                     re.fullmatch('openlegal_storage_'+op.operation_id.replace('-','')+'_[0-9]{1,8}',application),
                     'watch_cleanup_ownership')
                predicate="datname=current_database() AND usename=current_user AND pid<>pg_backend_pid() AND application_name='"+application+"'"
                session.cleanup_queries=(
                    "SELECT COALESCE(jsonb_agg(jsonb_build_object('pid',pid,'backend_start',backend_start,'cancelled',pg_cancel_backend(pid))),'[]'::jsonb) FROM pg_stat_activity WHERE "+predicate+';\n',
                    "SELECT COALESCE(jsonb_agg(jsonb_build_object('pid',pid,'backend_start',backend_start)),'[]'::jsonb) FROM pg_stat_activity WHERE "+predicate+';\n')
                session.cleanup_deadline=time.monotonic()+15
                try:return super()._close_owned_sql(op,target,application)
                finally:session.cleanup_deadline=None;session.cleanup_queries=()
            def _run(inner,op,binary,args,**kwargs):
                # Only fixed base-method reads and exact owned-SQL cancellation
                # may reach here; no checkpoint/manual action is exposed.
                need(binary in ('kubectl','docker','crictl'),'watch_binary_forbidden')
                if binary=='kubectl':
                    need('get' in args and not any(x in args for x in ('apply','patch','create','delete','scale','replace')),
                         'watch_kube_mutation_forbidden')
                elif binary=='crictl':
                    endpoint=['--runtime-endpoint',op.inventory['linux']['cri_endpoint']]
                    listing=args==endpoint+['ps','-a','-o','json']
                    inspecting=(len(args)==6 and args[:5]==endpoint+['inspect','-o','json'] and
                        type(args[5])is str and re.fullmatch('[0-9a-f]{64}',args[5]) is not None and
                        kwargs.get('record') is False)
                    need(listing or inspecting,'watch_cri_mutation_forbidden')
                else:
                    need(args and args[0] in ('inspect','exec'),'watch_docker_mutation_forbidden')
                    if args[0]=='exec': need('psql' in args and kwargs.get('stdin') is not None,'watch_exec_forbidden')
                if binary=='docker' and args[0]=='exec':
                    raw=kwargs['stdin'];prefix=session.modules['linux_backend'].SQL_SESSION_PREFIX.encode()
                    need(type(raw)is bytes and raw.startswith(prefix),'watch_sql_prefix')
                    query=raw[len(prefix):].decode('utf8','strict')
                    extra="SET statement_timeout='2s'; SET lock_timeout='1s'; SET transaction_timeout='5s'; "
                    while query.startswith(extra):query=query[len(extra):]
                    if session.cleanup_deadline is None:
                        need(query.endswith(';\n') and query[:-2]in allowed,'watch_sql_command_allowlist')
                    else:need(query in session.cleanup_queries,'watch_owned_cleanup_query')
                    kwargs['stdin']=prefix+extra.encode()+query.encode()
                deadline=session.cleanup_deadline or session.sample_deadline
                kwargs['timeout']=min(kwargs.get('timeout',30),max(.001,deadline-time.monotonic()))
                return super()._run(op,binary,args,**kwargs)
        return ReadOnly()

    def _sample(self, stage):
        start=time.monotonic();self.sample_deadline=start+SAMPLE_TIMEOUT;buffer=SampleBuffer()
        op=SimpleNamespace(operation_id=self.value['operation_id'],action='canary_watch',phase='canary-watch',
            inventory=self.storage,inventory_sha256=self.value['storage_inventory']['sha256'],
            source_graph_sha256=digest(canonical(self.storage['source_graph'])),authority_sha256=self.binding['authority_sha256'],
            journal=buffer,previous={},seal=self._seal,canary_policy=copy.deepcopy(self.value['canary_policy']),
            watch_stage={'before':'before_activation','after':'after_activation','observe':'observe'}[stage])
        try:
            self._seal()
            native_port=getattr(self.backend,'_canary_native',None)
            need(callable(native_port),'reviewed_canary_native_port_required')
            native=native_port(op)
            barrier=self.backend._read_replica_barrier(op,deadline=self.sample_deadline)
            observation=self.backend._sql(op,f"SELECT openlegal_checkpoint.canary_observation('{self.value['canary_id']}'::uuid)")
            provider=self.backend._sql(op,f"SELECT openlegal_checkpoint.observe('{self.value['recovery_operation_id']}'::uuid)")
            self.modules['provider_backend']._project(provider)
            self._seal();now=time.monotonic();sample={'monotonic':now,'wall_time':time.time(),'elapsed_seconds':now-start,
                'canary':observation,'native':native,'replica_barrier':barrier,'provider':provider}
            complete=self.timeline.accept(sample,stage)
            self.sample_deadline=None;self.backend._artifacts.clear()
            self.journal.save('sample-%04d.json'%self.timeline.count,{'kind':'openlegal_canary_watch_sample_bundle',
                **self.binding,'stage':stage,'sample':sample,'actual_command_evidence':buffer.values})
            return complete
        except BaseException:
            self.sample_deadline=None;self.failed=True
            try:self.journal.save('failed.json',{'kind':'openlegal_canary_watch_failure',**self.binding,
                'status':'FAILED_OR_UNKNOWN','sampled_observations':self.timeline.count,
                'actual_command_evidence':buffer.values,'automatic_retry':False,'final_acceptance':False})
            except BaseException:pass
            raise

    def before_activation(self):
        self._sample('before')
        return copy.deepcopy(self.binding)

    def after_activation(self):
        self._sample('after')
        return {'kind':'openlegal_canary_watch_live_after_activation','session_nonce':self.nonce,
                'sample_count':self.timeline.count,'serialized_monotonic_resume_permitted':False}

    def observe(self):
        need(self.timeline.after is not None and not self.failed and not self.completed,'watch_not_activated_or_completed')
        while True:
            self._seal()
            left=DURATION-(time.monotonic()-self.timeline.anchor)
            if left>0:
                delay=min(INTERVAL,left)
            else:
                # Full real observation time cannot substitute for an actual
                # DB observation of the unchanged canary deadline. This is
                # read-only post-window grace, never a DB budget extension.
                grace_left=DURATION+MAX_GAP-(time.monotonic()-self.timeline.anchor)
                need(grace_left>0,'database_window_grace_expired')
                delay=min(1,grace_left)
            time.sleep(delay)
            if self._sample('observe'):break
        result={**self.binding,'kind':'openlegal_canary_watch_observation_complete',
            'status':'OBSERVATION_COMPLETE_PENDING_FINAL_GATES','elapsed_monotonic_seconds':
                self.timeline.last['monotonic']-self.timeline.anchor,'sample_count':self.timeline.count,
            'consumed':self.timeline.last['canary']['canary']['consumed'],
            'actual_supervisor_os_exit':'REQUIRED_AFTER_FINAL_GATES','physical_drain_snapshot_close':
                'REQUIRED_SEPARATE_FINAL_GATE','public_16_matrix':'REQUIRED_SEPARATE_FINAL_GATE',
            'eligible_ack_publication_proof':'REQUIRED_SEPARATE_FINAL_GATE','final_acceptance':False}
        self.journal.save('observation-complete.json',result)
        self.completed=True
        return result

    def __enter__(self):return self
    def __exit__(self,*args):
        # The supervisor may keep this context open across its separate final
        # gates. Releasing this lock never releases a provider hold or guard.
        if args[0] is not None and not self.failed:
            self.failed=True
            try:self.journal.save('supervisor-body-failed.json',{'kind':'watch_supervisor_body_failed',**self.binding,
                'status':'FAILED_OR_UNKNOWN','final_acceptance':False})
            except BaseException:pass
        try:
            self.backend.close()
        except BaseException:
            try:self.journal.save('watch-backend-close-failed.json',{'kind':'watch_backend_close_failed',
                **self.binding,'status':'FAILED_OR_UNKNOWN','metadata_fd_cleanup_failed':True,'final_acceptance':False})
            except BaseException:pass
            if args[0] is None:raise
        finally:self.lock.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=('qualify',))
    for name in ('inventory','authority'):parser.add_argument('--'+name,type=Path,required=True);parser.add_argument('--'+name+'-sha256',required=True)
    parser.add_argument('--source-sha256',required=True);args=parser.parse_args()
    value,_,_,_,_=_prepared(args.inventory,args.inventory_sha256,args.authority,args.authority_sha256,args.source_sha256)
    print(canonical({'kind':'openlegal_canary_watch_source_qualification','operation_id':value['operation_id'],
        'status':'SOURCE_AND_INPUTS_ONLY','commands_executed':False,'session_armed':False,
        'watch_started':False,'canary_admitted':False,'final_acceptance':False}).decode(),end='')


if __name__=='__main__':
    try:main()
    except BaseException:
        print('{"status":"REFUSED_OR_UNKNOWN","watch_started":false}',file=sys.stderr);raise SystemExit(1)
