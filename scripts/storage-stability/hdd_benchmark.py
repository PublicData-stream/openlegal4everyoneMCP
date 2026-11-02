#!/usr/bin/env python3
"""Explicit fresh-dataset HDD diagnostic. No effects on import or qualification.

This is a synthetic diagnostic, not authority to tune or return active storage.
The 20 minute bound is logical: a blocked kernel syscall cannot be interrupted.
Such a call never yields PASS and keeps its owned operation lock until return.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import platform
import re
import stat
import sys
import types
import time
import uuid
from pathlib import Path

SELF = Path('/usr/local/lib/openlegal-storage-stability/hdd_benchmark.py')
ENV = {'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
       'LANG': 'C', 'LC_ALL': 'C', 'TMPDIR': '/tmp'}
HEX = re.compile(r'[0-9a-f]{64}\Z')
ROUNDS = 3
COMMITS = 128
SAMPLES = 128
PAYLOAD = 65536
LIMIT = 2 * 1024 * 1024
SQL_BUDGET_US = 2_000_000
PHASES = ('queue', 'data_write', 'file_fsync', 'rename', 'directory_fsync', 'readback', 'total')


class Refused(RuntimeError):
    pass


def need(value, code):
    if not value:
        raise Refused(code)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                       allow_nan=False) + '\n').encode()


def decode(raw):
    need(type(raw) is bytes and len(raw) <= LIMIT, 'input_bound')
    def pairs(rows):
        out = {}
        for key, value in rows:
            need(key not in out, 'duplicate_key')
            out[key] = value
        return out
    try:
        value = json.loads(raw, object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(Refused('nonfinite')))
    except (UnicodeError, ValueError, RecursionError):
        raise Refused('json_shape') from None
    def visit(item, depth=0):
        need(depth <= 24, 'input_depth')
        if type(item) is dict:
            for key, child in item.items():
                need(type(key) is str and len(key) <= 512, 'input_key')
                visit(child, depth + 1)
        elif type(item) is list:
            need(len(item) <= 4096, 'input_list')
            for child in item:
                visit(child, depth + 1)
        elif type(item) is float:
            need(math.isfinite(item), 'nonfinite')
    visit(value)
    return value


def pin(value):
    return type(value) is str and HEX.fullmatch(value) is not None


def operation_id(value):
    try:
        need(type(value) is str and str(uuid.UUID(value)) == value and uuid.UUID(value).int,
             'operation_uuid')
    except (ValueError, AttributeError):
        raise Refused('operation_uuid') from None
    return value


def private_bytes(path, expected=None, owner=0):
    path = Path(path)
    need(path.is_absolute() and path.resolve() == path, 'private_path')
    for parent in path.parents:
        s = parent.lstat()
        need(stat.S_ISDIR(s.st_mode) and s.st_uid == owner and
             (not s.st_mode & 0o022 or parent == Path('/tmp') and s.st_mode & stat.S_ISVTX),
             'private_parent')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        need(stat.S_ISREG(before.st_mode) and before.st_uid == owner and before.st_nlink == 1
             and stat.S_IMODE(before.st_mode) == 0o600 and before.st_size <= LIMIT, 'private_file')
        raw = stream.read(LIMIT + 1)
        after = os.fstat(stream.fileno())
        current = path.lstat()
    need(len(raw) == before.st_size and all(getattr(before, k) == getattr(after, k) == getattr(current, k)
         for k in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')), 'private_changed')
    need(expected is None or pin(expected) and sha(raw) == expected, 'raw_hash')
    return raw


def validate_inventory(value, operation):
    operation_id(operation)
    fields = {'schema_version', 'kind', 'revision', 'pool', 'binaries', 'native_pg_image',
              'evidence_root', 'total_deadline_secs', 'host_boot_id', 'process_sources', 'docker'}
    need(type(value) is dict and set(value) == fields and type(value['schema_version']) is int
         and value['schema_version'] == 1 and value['kind'] == 'openlegal_hdd_benchmark_inventory',
         'inventory_shape')
    need(type(value['revision']) is str and re.fullmatch('[0-9a-f]{40}', value['revision']), 'revision')
    need(value['evidence_root'] == '/tmp/openlegal-hdd-benchmark-' + operation, 'evidence_namespace')
    need(type(value['total_deadline_secs']) is int and 1 <= value['total_deadline_secs'] <= 1200,
         'deadline_bound')
    operation_id(value['host_boot_id'])
    validate_docker(value['docker'], operation)
    need(type(value['process_sources'])is dict and set(value['process_sources'])=={'interface.py','command_process.py'}and
         all(pin(v)for v in value['process_sources'].values()),'exact_process_sources')
    pool = value['pool']
    need(type(pool) is dict and set(pool) == {'name', 'guid', 'leaf_devices', 'reserve_bytes'} and
         type(pool['name']) is str and re.fullmatch('[A-Za-z][A-Za-z0-9_.-]{0,62}', pool['name'])
         and type(pool['guid']) is str and re.fullmatch('[0-9]{1,20}', pool['guid']), 'pool_pin')
    need(type(pool['reserve_bytes'])is int and 0<pool['reserve_bytes']<=9223372036854775807,'pool_reserve_bound')
    need(type(pool['leaf_devices']) is list and 1 <= len(pool['leaf_devices']) <= 64, 'leaf_bound')
    paths = set()
    for leaf in pool['leaf_devices']:
        need(type(leaf) is dict and set(leaf) == {'path', 'major', 'minor', 'sysfs_path',
             'identity_file', 'identity_sha256'} and type(leaf['path']) is str and
             re.fullmatch('/dev/[A-Za-z0-9_.-]{1,128}', leaf['path']) and leaf['path'] not in paths,
             'leaf_path')
        need(all(type(leaf[k]) is int and leaf[k] >= 0 for k in ('major', 'minor')) and
             type(leaf['sysfs_path']) is str and leaf['sysfs_path'].startswith('/sys/devices/') and
             '..' not in Path(leaf['sysfs_path']).parts and leaf['identity_file'] in ('wwid', 'serial')
             and pin(leaf['identity_sha256']), 'leaf_identity')
        paths.add(leaf['path'])
    binaries = value['binaries']
    need(type(binaries) is dict and set(binaries) == {'zfs', 'zpool', 'docker'}, 'binaries')
    for name, entry in binaries.items():
        need(type(entry) is dict and set(entry) == {'path', 'sha256'} and
             type(entry['path']) is str and entry['path'].startswith('/') and
             '..' not in Path(entry['path']).parts and pin(entry['sha256']), 'binary_pin')
        need(Path(entry['path']).name == name,'binary_fixed_name')
    image = value['native_pg_image']
    need(type(image) is dict and set(image) == {'reference', 'image_id', 'os', 'architecture',
         'major', 'uid', 'gid'} and type(image['image_id'])is str and type(image['reference'])is str and re.fullmatch('sha256:[0-9a-f]{64}', image['image_id']) and
         re.fullmatch('(?:[a-z0-9./:_-]+@)?sha256:[0-9a-f]{64}', image['reference']) and
         image['os'] == 'linux' and image['architecture'] in ('amd64', 'arm64') and
         type(image['major']) is int and image['major'] == 18 and
         all(type(image[k]) is int and 1 <= image[k] <= 65535 for k in ('uid', 'gid')), 'native_image')
    return value


def validate_docker(value, operation):
    need(type(value) is dict and set(value) == {'endpoint', 'root_dir'}, 'docker_shape')
    base = '/tmp/openlegal-hdd-docker-' + operation_id(operation)
    need(value['endpoint'] in ('unix:///var/run/docker.sock', 'unix://' + base + '/docker.sock'),
         'docker_endpoint')
    root = value['root_dir']
    need(type(root) is str and root.startswith('/tmp/') and str(Path(root)) == root and
         '..' not in Path(root).parts and '.' not in Path(root).parts, 'docker_temporary_root')
    if value['endpoint'] != 'unix:///var/run/docker.sock':
        need(root == base + '/data', 'docker_private_root')
    return value


def plan(value, operation):
    validate_inventory(value, operation)
    root = Path(value['evidence_root'])
    parent = value['pool']['name'] + '/openlegal-hdd-benchmark-' + operation
    return {'kind': 'openlegal_hdd_benchmark_fixed_plan', 'operation_id': operation,
            'datasets': [{'name': parent + '/r' + str(size), 'recordsize': str(size) + 'K',
                          'mountpoint': str(root / ('r' + str(size))), 'compression': 'lz4',
                          'sync': 'standard', 'atime': 'off', 'quota': 1073741824}
                         for size in (128, 32)],
            'dataset_parent': parent, 'round_order': [128, 32, 32, 128, 128, 32],
            'commits_per_round': COMMITS, 'blob_samples_per_budget_round': SAMPLES,
            'blob_budgets_secs': [5, 30], 'maximum_logical_seconds': value['total_deadline_secs'],
            'maximum_live_payload_bytes': 2 * 1073741824, 'pool_reserve_bytes':value['pool']['reserve_bytes'], 'production_tuning_permitted': False,
            'same_host_other_workload_unchanged': True, 'process_sources':dict(value['process_sources']),
            'docker':dict(value['docker'])}


def validate_approval(value, inventory, inventory_pin, source_pin, operation):
    fields = {'schema_version', 'kind', 'approved', 'operation_id', 'inventory_sha256',
              'source_sha256', 'plan_sha256', 'pool_guid', 'valid_until',
              'fresh_test_datasets_only', 'destruction_of_exact_owned_test_datasets',
              'production_tuning_permitted', 'provider_calls_permitted'}
    need(type(value) is dict and set(value) == fields and type(value['schema_version']) is int and
         value['schema_version'] == 1 and value['kind'] == 'openlegal_hdd_benchmark_authority' and
         value['approved'] is True and value['operation_id'] == operation and
         value['inventory_sha256'] == inventory_pin and value['source_sha256'] == source_pin and
         value['plan_sha256'] == sha(canonical(plan(inventory, operation))) and
         value['pool_guid'] == inventory['pool']['guid'] and
         type(value['valid_until']) in (int, float) and time.time() < value['valid_until'] <= time.time() + 86400
         and value['fresh_test_datasets_only'] is True and
         value['destruction_of_exact_owned_test_datasets'] is True and
         value['production_tuning_permitted'] is False and value['provider_calls_permitted'] is False,
         'actual_diagnostic_authority')


class Journal:
    def __init__(self, root, binding):
        self.root = Path(root)
        os.mkdir(self.root, 0o700)
        self.fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        self.identity = os.fstat(self.fd)
        self.sequence = 0
        self.total_bytes=0
        self.lock=None
        try:
            self.save('operation.json', binding)
            self.lock = os.open('operation.lock', os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                                0o600, dir_fd=self.fd)
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.fsync(self.fd)
        except BaseException:
            if self.lock is not None:os.close(self.lock)
            os.close(self.fd);raise

    def check(self):
        now = self.root.lstat()
        need((now.st_dev, now.st_ino) == (self.identity.st_dev, self.identity.st_ino) and
             now.st_uid == 0 and stat.S_IMODE(now.st_mode) == 0o700, 'journal_rebound')

    def save(self, name, value):
        self.check()
        need(re.fullmatch('[a-zA-Z0-9_.-]{1,128}', name), 'journal_name')
        raw = canonical(value)
        need(len(raw) <= LIMIT and self.total_bytes+len(raw)<=32*1024*1024,'journal_bound')
        self.total_bytes+=len(raw)
        fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                     0o600, dir_fd=self.fd)
        try:
            view = memoryview(raw)
            while view:
                n = os.write(fd, view)
                need(n > 0, 'journal_write')
                view = view[n:]
            os.fsync(fd)
            os.lseek(fd, 0, os.SEEK_SET)
            need(os.read(fd, len(raw) + 1) == raw, 'journal_readback')
            os.fsync(self.fd)
            self.check()
        finally:
            os.close(fd)
        return sha(raw)

    def close(self):
        if getattr(self, 'lock', None) is not None:
            os.close(self.lock)
            self.lock = None
        os.close(self.fd)


def process_source_bytes(inventory):
    pins=inventory['process_sources']
    need(type(pins)is dict and set(pins)=={'interface.py','command_process.py'}and all(pin(v)for v in pins.values()),
         'exact_process_sources')
    # Both inputs are authenticated BEFORE either is compiled/imported.
    return {name:private_bytes(SELF.parent/name,pins[name])for name in ('interface.py','command_process.py')}


def load_process_sources(inventory):
    sources=process_source_bytes(inventory);pins=inventory['process_sources']
    identity=types.ModuleType('_openlegal_hdd_interface_'+pins['interface.py'])
    identity.__file__=str(SELF.parent/'interface.py')
    process=types.ModuleType('_openlegal_hdd_process_'+pins['interface.py']+'_'+pins['command_process.py'])
    process.__file__=str(SELF.parent/'command_process.py')
    absent=object();previous=sys.modules.get('interface',absent)
    sys.modules[identity.__name__]=identity;sys.modules[process.__name__]=process
    try:
        exec(compile(sources['interface.py'],identity.__file__,'exec'),identity.__dict__)
        sys.modules['interface']=identity
        exec(compile(sources['command_process.py'],process.__file__,'exec'),process.__dict__)
    except BaseException:
        sys.modules.pop(identity.__name__,None);sys.modules.pop(process.__name__,None)
        raise
    finally:
        if previous is absent:sys.modules.pop('interface',None)
        else:sys.modules['interface']=previous
    return process


class Runner:
    """Pinned executables; owned subreaper CLI closure beyond process groups.

    Only a dedicated single-main-thread process with no unrelated children may
    call this runner. Logical expiry never asserts physical kernel closure.
    """
    def __init__(self, inventory, journal, deadline, seal):
        self.inventory, self.journal, self.deadline, self.seal = inventory, journal, deadline, seal
        self.closed = True
        self.last_elapsed_us = 0
        self.process=load_process_sources(inventory)
        self.docker_client = None
        self.docker_identities = None

    def run(self, binary, args, stdin=None, allowed_failure=False):
        if binary == 'docker':
            need(type(args) is list and all(type(a) is str for a in args), 'argument_bound')
            # No top-level option (including short -c/--context) may precede
            # the fixed subcommand. PostgreSQL's later argv still needs -c.
            need(args and not args[0].startswith('-') and not any(a in ('-H', '--host', '--context', '--config') or
                 a.startswith(('-H', '--host=', '--context=', '--config=')) for a in args),
                 'docker_route_override')
            self.qualify_docker_root()
            result = self._run_command(binary, self._docker_args(args), stdin, allowed_failure)
            self._docker_paths()
            return result
        if binary == 'zfs' and args and args[0] in ('create', 'destroy', 'unmount'):
            self.qualify_docker_root()
        return self._run_command(binary, args, stdin, allowed_failure)

    def _docker_paths(self):
        """No client HOME/context/auth, and no adoption of a replaced socket/root."""
        self.seal(); self.journal.check()
        config = validate_docker(self.inventory['docker'],
            self.inventory['evidence_root'].removeprefix('/tmp/openlegal-hdd-benchmark-'))
        if self.docker_client is None:
            os.mkdir('docker-client', 0o700, dir_fd=self.journal.fd)
            os.fsync(self.journal.fd)
            self.docker_client = (self.journal.root / 'docker-client').lstat()
        client = self.journal.root / 'docker-client'
        fd = os.open('docker-client', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                     dir_fd=self.journal.fd)
        try:
            current = os.fstat(fd)
            need(current.st_uid == 0 and current.st_gid == 0 and stat.S_IMODE(current.st_mode) == 0o700
                 and (current.st_dev, current.st_ino) == (self.docker_client.st_dev, self.docker_client.st_ino)
                 and not os.listdir(fd), 'docker_client_changed')
            need((client.lstat().st_dev, client.lstat().st_ino) == (current.st_dev, current.st_ino),
                 'docker_client_rebound')
        finally: os.close(fd)
        root = Path(config['root_dir'])
        need(root.resolve(strict=True) == root, 'docker_root_symlink')
        for parent in (root, *root.parents):
            meta = parent.lstat()
            need(stat.S_ISDIR(meta.st_mode) and meta.st_uid == 0 and
                 (not meta.st_mode & 0o022 or parent == Path('/tmp') and meta.st_mode & stat.S_ISVTX),
                 'docker_root_parent')
        identities = [(root.stat().st_dev, root.stat().st_ino)]
        if config['endpoint'] != 'unix:///var/run/docker.sock':
            socket = Path(config['endpoint'].removeprefix('unix://'))
            parent = socket.parent.lstat(); node = socket.lstat()
            need(socket.parent.resolve() == socket.parent and parent.st_uid == 0 and
                 stat.S_ISDIR(parent.st_mode) and stat.S_IMODE(parent.st_mode) == 0o700 and
                 stat.S_ISSOCK(node.st_mode) and node.st_uid == 0 and node.st_nlink == 1,
                 'docker_private_socket')
            identities.extend(((parent.st_dev, parent.st_ino), (node.st_dev, node.st_ino)))
        if self.docker_identities is None: self.docker_identities = identities
        need(identities == self.docker_identities, 'docker_endpoint_rebound')

    def _docker_args(self, args):
        self._docker_paths()
        return ['--config', str(self.journal.root / 'docker-client'), '--host',
                self.inventory['docker']['endpoint'], *args]

    def qualify_docker_root(self):
        # Only this read-only command may precede temporary-daemon qualification.
        _, raw, _ = self._run_command('docker', self._docker_args(['info', '--format', '{{.DockerRootDir}}']))
        need(raw.decode('utf8', 'strict').strip() == self.inventory['docker']['root_dir'],
             'docker_actual_root_mismatch')
        self._docker_paths()

    def _run_command(self, binary, args, stdin=None, allowed_failure=False):
        self.seal();process_source_bytes(self.inventory)
        need(time.monotonic() < self.deadline, 'operation_deadline')
        need(binary in ('docker','zfs','zpool')and type(args)is list and len(args)<=8192 and
             all(type(a)is str and '\x00'not in a and len(a)<=16384 for a in args),'argument_bound')
        need(type(allowed_failure)is bool and (stdin is None or type(stdin)is bytes and len(stdin)<=LIMIT),'stdin_or_failure_bound')
        entry=self.inventory['binaries'][binary];path=Path(entry['path'])
        need(path.is_absolute()and path.resolve()==path,'binary_symlink')
        for parent in path.parents:
            meta=parent.lstat();need(stat.S_ISDIR(meta.st_mode)and meta.st_uid==0 and
                (not meta.st_mode&0o022 or parent==Path('/tmp')and meta.st_mode&stat.S_ISVTX),'binary_parent')
        fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
        label=None
        try:
            before=os.fstat(fd)
            need(stat.S_ISREG(before.st_mode)and before.st_uid==0 and not before.st_mode&0o022 and
                 before.st_nlink==1 and before.st_size<=128*1024*1024,'binary_metadata')
            hashed=hashlib.sha256()
            while block:=os.read(fd,1024*1024):hashed.update(block)
            after=os.fstat(fd);current=path.lstat()
            need(all(getattr(before,k)==getattr(after,k)==getattr(current,k)for k in
                ('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns'))and hashed.hexdigest()==entry['sha256'],'binary_changed')
            self.seal();process_source_bytes(self.inventory)
            self.journal.sequence+=1;label=f'command-{self.journal.sequence:04d}'
            self.journal.save(label+'-intent.json',{'binary':binary,'argv':args,
                'stdin_sha256':sha(stdin)if stdin is not None else None,
                'process_sources':dict(self.inventory['process_sources']),'original_absolute_deadline':self.deadline})
            self.closed=False
            result=self.process.execute([str(path),*args],executable_fd=fd,stdin=stdin,env=ENV,
                deadline=self.deadline,max_output=LIMIT)
        except BaseException:
            # No structured result means actual rc/closure are UNKNOWN. The
            # helper retains/drains its ownership before raising, but that is
            # not an observed result authorizing Docker/resource cleanup here.
            if label is not None:
                try:self.journal.save(label+'-failed.json',{'status':'FAILED_OR_UNKNOWN','actual_exit_code':None,
                    'owned_processes_closed':'UNKNOWN','automatic_cleanup':False,'automatic_retry':False})
                except BaseException:pass
            raise
        finally:os.close(fd)
        need(all(type(getattr(result,key))is bool for key in ('started','expired','output_limit_exceeded',
             'owned_processes_closed','unexpected_descendants','interrupted')) and
             type(result.stdout)is bytes and type(result.stderr)is bytes and
             type(result.elapsed_seconds)in(int,float)and math.isfinite(result.elapsed_seconds)and result.elapsed_seconds>=0,
             'invalid_command_result')
        self.last_elapsed_us=int(result.elapsed_seconds*1_000_000)
        self.closed=result.owned_processes_closed is True
        combined_exceeded=len(result.stdout)+len(result.stderr)>LIMIT
        record={'actual_exit_code':result.actual_exit_code,'started':result.started,'expired':result.expired,
            'output_limit_exceeded':result.output_limit_exceeded,'combined_output_limit_exceeded':combined_exceeded,
            'owned_processes_closed':result.owned_processes_closed,'unexpected_descendants':result.unexpected_descendants,
            'interrupted':result.interrupted,'elapsed_us':self.last_elapsed_us,
            'stdout_sha256':sha(result.stdout),'stderr_sha256':sha(result.stderr),
            'stdout_bytes':len(result.stdout),'stderr_bytes':len(result.stderr),'stdout_utf8':None,'stderr_utf8':None}
        valid_utf8=True
        try:record.update(stdout_utf8=result.stdout.decode('utf8','strict'),stderr_utf8=result.stderr.decode('utf8','strict'))
        except UnicodeError:valid_utf8=False
        record['utf8_valid']=valid_utf8
        if len(canonical(record))>LIMIT:
            record.update(stdout_utf8=None,stderr_utf8=None,raw_output_retained=False)
        else:record['raw_output_retained']=valid_utf8
        self.journal.save(label+'-exit.json',record)
        need(result.started is True and result.owned_processes_closed is True and not result.expired and
             not result.output_limit_exceeded and not combined_exceeded and not result.unexpected_descendants and
             not result.interrupted and valid_utf8 and type(result.actual_exit_code)is int and
             (result.actual_exit_code==0 or allowed_failure),'command_failed_or_unclosed')
        return result.actual_exit_code,result.stdout,result.stderr


def pool_leaves(raw):
    leaves = set()
    excluded = False
    for line in raw.decode('utf8', 'strict').splitlines():
        parts = line.split()
        if not parts:
            continue
        if parts[0] in ('cache', 'spares'):
            excluded = True
        elif parts[0] in ('logs', 'special', 'dedup'):
            excluded = False
        if parts[0].startswith('/dev/') and not excluded:
            leaves.add(parts[0])
    need(leaves, 'pool_data_leaves_missing')
    return leaves


def device_identity(leaf):
    path = Path(leaf['path'])
    info = path.stat()
    need(path.resolve() == path and stat.S_ISBLK(info.st_mode) and
         (os.major(info.st_rdev), os.minor(info.st_rdev)) == (leaf['major'], leaf['minor']), 'leaf_device_changed')
    sysfs = Path('/sys/dev/block') / f"{leaf['major']}:{leaf['minor']}"
    current = sysfs.resolve(strict=True)
    need(str(current) == leaf['sysfs_path'], 'leaf_sysfs_changed')
    physical = current.parent if (current / 'partition').exists() else current
    raw = (physical / 'device' / leaf['identity_file']).read_bytes()
    need(len(raw) <= 4096 and sha(raw) == leaf['identity_sha256'], 'leaf_hardware_changed')


def latency_samples(stderr, round_id):
    values = []
    for line in stderr.decode('utf8', 'strict').splitlines():
        match = re.fullmatch(r'NOTICE:\s+openlegal_hdd_commit:(\d+):(\d+):(\d+)', line)
        if match:
            need(int(match[1]) == round_id and int(match[2]) == len(values) + 1, 'commit_sample_order')
            values.append(int(match[3]))
    need(len(values) == COMMITS and all(0 <= n <= 1200_000_000 for n in values), 'commit_sample_count')
    return values


def percentile99(values):
    need(type(values) is list and len(values) >= 128 and all(type(x) is int and x >= 0 for x in values),
         'latency_shape')
    return sorted(values)[math.ceil(len(values) * 0.99) - 1]


def assess(rounds):
    need(type(rounds) is list and len(rounds) == 6, 'three_pairs_required')
    indexed = {}
    for row in rounds:
        need(type(row) is dict and row.get('recordsize') in (128, 32) and type(row.get('round')) is int
             and 0 <= row['round'] < ROUNDS and (row['recordsize'], row['round']) not in indexed,
             'round_identity')
        samples = row['commits_us']
        need(len(samples) == COMMITS, 'commit_round_count')
        server_p99 = percentile99(samples)
        need(type(row['client_roundtrip_us']) is int and row['client_roundtrip_us'] >= 0,
             'client_latency')
        overhead = max(0, row['client_roundtrip_us'] - sum(samples))
        upper = [value + overhead for value in samples]
        absolute = percentile99(upper) <= SQL_BUDGET_US // 2 and max(upper) < SQL_BUDGET_US * 0.8
        for budget in (5, 30):
            probe = row['blob'][str(budget)]
            need(set(probe['phases_us']) == set(PHASES) and type(probe['expiries']) is int
                 and probe['expiries'] >= 0 and probe.get('physical_calls_completed')is True and probe.get('budget_secs')==budget, 'probe_shape')
            absolute = absolute and probe['expiries'] == 0
            for timings in probe['phases_us'].values():
                need(len(timings) == SAMPLES, 'probe_sample_count')
                absolute = absolute and percentile99(timings) <= budget * 500000 and max(timings) < budget * 800000
        indexed[(row['recordsize'], row['round'])] = (server_p99, absolute, {str(b):percentile99(row['blob'][str(b)]['phases_us']['total'])for b in(5,30)})
    gains = []
    for number in range(ROUNDS):
        baseline, baseline_absolute, baseline_blob = indexed[(128, number)]
        candidate, absolute, candidate_blob = indexed[(32, number)]
        gains.append(baseline > 0 and candidate * 100 <= baseline * 80 and absolute and baseline_absolute and all(baseline_blob[str(b)] > 0 and candidate_blob[str(b)] * 100 <= baseline_blob[str(b)] * 80 for b in(5,30)))
    return {'kind': 'openlegal_hdd_benchmark_diagnostic_verdict',
            'candidate_32k_repeatably_better': all(gains), 'paired_rounds_pass': gains,
            'recommendation': 'candidate_for_separate_review' if all(gains) else 'preserve_128k_inconclusive',
            'production_tuning_permitted': False, 'capacity_or_causality_proven': False,
            'client_latency_method': 'server_commit_samples_plus_entire_batch_client_overhead_upper_bound'}


def blob_probe(path, budget, count, deadline, *, payload=PAYLOAD):
    """Actual syscall timing; await physical completion, reject late logical proof."""
    need(budget in (5, 30) and type(count) is int and 1 <= count <= SAMPLES and
         type(payload) is int and 1 <= payload <= PAYLOAD, 'probe_bound')
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    identity = os.fstat(fd)
    values = {key: [] for key in PHASES}
    expiries = 0
    data = bytes(range(256)) * (payload // 256) + bytes(range(payload % 256))
    expected = sha(data)
    try:
        for index in range(count):
            queued = time.monotonic_ns()
            need(time.monotonic() < deadline, 'probe_operation_deadline')
            start = time.monotonic_ns()
            values['queue'].append((start - queued) // 1000)
            stage, final = f'.stage-{budget}-{index}', f'probe-{budget}-{index}'
            try:os.stat(final,dir_fd=fd,follow_symlinks=False)
            except FileNotFoundError:pass
            else:raise Refused('probe_destination_exists')
            out = os.open(stage, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                          0o600, dir_fd=fd)
            try:
                tick = time.monotonic_ns()
                view = memoryview(data)
                while view:
                    n = os.write(out, view)
                    need(n > 0, 'probe_zero_write')
                    view = view[n:]
                values['data_write'].append((time.monotonic_ns() - tick) // 1000)
                tick = time.monotonic_ns(); os.fsync(out)
                values['file_fsync'].append((time.monotonic_ns() - tick) // 1000)
                tick = time.monotonic_ns(); os.rename(stage, final, src_dir_fd=fd, dst_dir_fd=fd)
                values['rename'].append((time.monotonic_ns() - tick) // 1000)
                tick = time.monotonic_ns(); os.fsync(fd)
                values['directory_fsync'].append((time.monotonic_ns() - tick) // 1000)
                tick = time.monotonic_ns(); os.lseek(out, 0, os.SEEK_SET)
                readback = bytearray()
                while len(readback) <= payload:
                    chunk=os.read(out,payload+1-len(readback))
                    if not chunk:break
                    readback.extend(chunk)
                need(sha(readback) == expected and len(readback) == payload, 'probe_readback')
                values['readback'].append((time.monotonic_ns() - tick) // 1000)
            finally:
                os.close(out)
            os.unlink(final, dir_fd=fd); os.fsync(fd)
            total = (time.monotonic_ns() - queued) // 1000
            values['total'].append(total)
            expiries += int(total >= budget * 1_000_000)
            need(time.monotonic() < deadline, 'probe_operation_deadline')
        now = Path(path).lstat()
        need((now.st_dev, now.st_ino) == (identity.st_dev, identity.st_ino), 'probe_root_rebound')
        return {'budget_secs': budget, 'payload_bytes': payload, 'phases_us': values,
                'expiries': expiries, 'physical_calls_completed': True}
    finally:
        os.close(fd)


def mount_present(path, dataset):
    found = []
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        left, right = line.split(' - ', 1)
        fields, tail = left.split(), right.split()
        if fields[4] == str(path):
            found.append((tail[0], tail[1]))
    return found == [('zfs', dataset)]


def mounts_clear(path,dataset):
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        left,right=line.split(' - ',1);fields=left.split();tail=right.split()
        if fields[4]==str(path)or tail[0]=='zfs'and tail[1]==dataset:return False
    return True


def cgroup_identity(pid):
    rows = Path(f'/proc/{pid}/cgroup').read_text().splitlines()
    need(len(rows) == 1 and rows[0].startswith('0::/'), 'unified_cgroup_required')
    group = rows[0][3:]
    need(group != '/' and '..' not in Path(group).parts, 'unscoped_container_cgroup')
    path = Path('/sys/fs/cgroup') / group.lstrip('/')
    metadata = path.stat()
    ticks = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]
    return {'path': str(path), 'group': group, 'dev': metadata.st_dev, 'ino': metadata.st_ino,
            'pid': pid, 'start_ticks': int(ticks)}


def cgroup_closed(witness, deadline):
    path = Path(witness['path'])
    if path.exists():
        info = path.stat()
        need((info.st_dev, info.st_ino) == (witness['dev'], witness['ino']), 'container_cgroup_reused')
        need(path.joinpath('cgroup.procs').read_text().split() == [] and
             'populated 0' in path.joinpath('cgroup.events').read_text().splitlines(), 'container_cgroup_populated')
    count = 0
    for entry in Path('/proc').iterdir():
        if not entry.name.isdecimal():
            continue
        count += 1
        need(count <= 131072 and time.monotonic() < deadline, 'closure_scan_bound')
        try:
            rows = (entry / 'cgroup').read_text().splitlines()
        except FileNotFoundError:
            continue
        for row in rows:
            group = row.split(':', 2)[2]
            need(group != witness['group'] and not group.startswith(witness['group'] + '/'),
                 'container_process_remains')
    return {'original_cgroup': witness, 'all_matching_processes_absent': True,
            'cgroup_absent_or_same_empty': True}


class Benchmark:
    def __init__(self, inventory, operation, journal, runner, deadline):
        self.inventory, self.operation = inventory, operation
        self.journal, self.runner, self.deadline = journal, runner, deadline
        self.fixed = plan(inventory, operation)
        self.datasets = {}
        self.containers = {}
        self.rounds = []
        self.cpu=min(os.sched_getaffinity(0))
        self.capacity_sequence=0
        self.last_sql_elapsed_us=0
        self.gates={}

    def command(self, binary, args, stdin=None, allowed_failure=False):
        if binary=='zfs'and args and args[0]in('create','unmount','destroy'):
            self.verify_pool()
        if binary=='zfs'and args and args[0]=='create':self.check_capacity('before-dataset-create')
        result=self.runner.run(binary,args,stdin,allowed_failure)
        if binary=='zfs'and args and args[0]=='create':
            name=args[-1];actual=self.properties(name)
            need(actual['org.openlegal:hdd-benchmark']==self.operation and re.fullmatch('[0-9]{1,20}',actual['guid']),'created_dataset_identity')
            need(name not in self.datasets,'created_dataset_reuse');self.datasets[name]=actual['guid']
            self.check_capacity('after-dataset-create')
        return result

    def out(self, binary, args, stdin=None):
        return self.command(binary, args, stdin)[1]

    def verify_pool(self):
        pool=self.inventory['pool']
        guid=self.out('zpool',['get','-Hp','-o','value','guid',pool['name']]).decode().strip()
        need(guid==pool['guid'],'actual_pool_guid_changed')
        actual=pool_leaves(self.out('zpool',['status','-P','-L',pool['name']]))
        need(actual=={d['path']for d in pool['leaf_devices']},'pool_data_device_set_changed')
        for leaf in pool['leaf_devices']:device_identity(leaf)
        return guid

    def check_capacity(self,phase):
        """Observation, NOT a reservation against unrelated concurrent writers."""
        need(re.fullmatch('[a-z-]{1,48}',phase),'capacity_phase')
        pool=self.inventory['pool'];parent=self.fixed['dataset_parent']
        def number(raw):
            text=raw.decode().strip();need(re.fullmatch('[0-9]{1,20}',text),'capacity_number');return int(text)
        free=number(self.out('zpool',['list','-Hp','-o','free',pool['name']]))
        available=number(self.out('zfs',['get','-Hp','-o','value','available',pool['name']]))
        used=0
        if parent in self.datasets:
            raw=self.out('zfs',['get','-Hp','-o','property,value','guid,used,quota,org.openlegal:hdd-benchmark',parent]).decode()
            rows={}
            for line in raw.splitlines():
                key,value=line.split('\t');need(key not in rows,'capacity_duplicate');rows[key]=value
            need(set(rows)=={'guid','used','quota','org.openlegal:hdd-benchmark'}and rows['guid']==self.datasets[parent]and rows['org.openlegal:hdd-benchmark']==self.operation and rows['quota']=='2147483648','capacity_parent_identity_or_quota')
            used=number(rows['used'].encode());need(used<=2147483648,'capacity_parent_over_quota')
        remaining=max(0,2147483648-used);required=remaining+pool['reserve_bytes'];effective=min(free,available)
        self.capacity_sequence+=1;need(self.capacity_sequence<=1024,'capacity_evidence_bound')
        self.journal.save('capacity-'+str(self.capacity_sequence).zfill(4)+'.json',{'phase':phase,'at':time.time(),'zpool_free_bytes':free,'root_dataset_available_bytes':available,'effective_available_bytes':effective,'parent_used_bytes':used,'remaining_own_maximum_bytes':remaining,'approved_reserve_bytes':pool['reserve_bytes'],'required_available_bytes':required,'is_space_reservation':False})
        need(effective>=required,'approved_pool_headroom_unavailable')

    def qualify_host(self):
        need(Path('/proc/sys/kernel/random/boot_id').read_text().strip() == self.inventory['host_boot_id'],
             'host_rebooted')
        native = {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(platform.machine())
        image = self.inventory['native_pg_image']
        need(platform.system() == 'Linux' and native == image['architecture'], 'native_host_required')
        pool=self.inventory['pool'];guid=self.verify_pool()
        self.check_capacity('before-host-probes')
        template = '{{.Id}} {{.Os}} {{.Architecture}}'
        for ref in (image['reference'], image['image_id']):
            parts = self.out('docker', ['image', 'inspect', '--format', template, ref]).decode().split()
            need(parts == [image['image_id'], image['os'], image['architecture']], 'existing_native_image_required')
        volumes=decode(self.out('docker',['image','inspect','--format','{{json .Config.Volumes}}',image['image_id']]))
        need(volumes in(None,{'/var/lib/postgresql':{}}),'unsupported_image_volume_layout')
        # Existing image only, no pull/platform fallback; synthetic command only.
        for label, argv, expected in (
            ('version', ['postgres', '--version'], None),
            ('uid', ['id', '-u', 'postgres'], str(image['uid'])),
            ('gid', ['id', '-g', 'postgres'], str(image['gid'])),
        ):
            cid = self.create_container(label, None, argv)
            output = self.run_short(cid).decode().strip()
            if label == 'version':
                need(re.fullmatch(r'postgres \(PostgreSQL\) 18(?:\.[0-9]+)?(?: .*|)', output), 'postgres18_required')
            else:
                need(output == expected, 'image_uid_gid_changed')
            self.finish_container(cid)
        self.journal.save('host-qualified.json', {'pool_guid': guid, 'leaf_devices': pool['leaf_devices'],
                          'native_pg_image': image, 'observed_at': time.time(),
                          'io_pressure': Path('/proc/pressure/io').read_text()})

    def inspect_container(self, cid):
        template = '{"Id":{{json .Id}},"Image":{{json .Image}},"State":{{json .State}},"Mounts":{{json .Mounts}},"Labels":{{json .Config.Labels}},"User":{{json .Config.User}},"Entrypoint":{{json .Config.Entrypoint}},"Cmd":{{json .Config.Cmd}},"Limits":{"readonly":{{json .HostConfig.ReadonlyRootfs}},"network":{{json .HostConfig.NetworkMode}},"cpu":{{json .HostConfig.NanoCpus}},"cpuset":{{json .HostConfig.CpusetCpus}},"memory":{{json .HostConfig.Memory}},"swap":{{json .HostConfig.MemorySwap}},"pids":{{json .HostConfig.PidsLimit}},"privileged":{{json .HostConfig.Privileged}},"capdrop":{{json .HostConfig.CapDrop}},"ports":{{json .HostConfig.PortBindings}},"restart":{{json .HostConfig.RestartPolicy}}}}'
        value = decode(self.out('docker', ['inspect', '--format', template, cid]))
        need(value['Id'] == cid and value['Image'] == self.inventory['native_pg_image']['image_id'] and
             value['Labels'].get('openlegal.hdd-benchmark.operation') == self.operation, 'container_identity_changed')
        return value

    def create_container(self, label, dataset, command):
        self.check_capacity('before-container-create')
        need(re.fullmatch('[a-z0-9-]{1,32}', label), 'container_label')
        image = self.inventory['native_pg_image']
        gates=self.journal.root/'gates'
        if not gates.exists():os.mkdir(gates,0o700)
        gate=gates/label;os.mkdir(gate,0o755);os.chmod(gate,0o755);gmeta=gate.lstat()
        self.gates[label]={'path':gate,'dev':gmeta.st_dev,'ino':gmeta.st_ino,'released':False}
        token=self.operation+':'+label
        script='n=0; while [ ! -f /openlegal-start/go ]; do n=$((n+1)); [ "$n" -lt 300 ] || exit 125; sleep 0.1; done; IFS= read -r token < /openlegal-start/go; [ "$token" = "'+token+'" ] || exit 126; exec "$@"'
        wrapped=['-ec',script,'openlegal-hdd-start',*command]
        args = ['create', '--pull=never', '--name', 'openlegal-hdd-' + self.operation + '-' + label,
                '--label', 'openlegal.hdd-benchmark.operation=' + self.operation,
                '--network=none', '--read-only', '--cpus=1', '--cpuset-cpus='+str(self.cpu), '--memory=512m', '--memory-swap=512m',
                '--pids-limit=128', '--cap-drop=ALL', '--security-opt=no-new-privileges',
                '--restart=no','--log-driver=json-file','--log-opt=max-size=1m','--log-opt=max-file=1', '--stop-signal=SIGINT', '--stop-timeout=10',
                '--user', f"{image['uid']}:{image['gid']}",
                '--tmpfs', '/tmp:rw,nosuid,nodev,noexec,size=64m,mode=1777',
                '--entrypoint','/bin/sh','--mount','type=bind,src='+str(gate)+',dst=/openlegal-start,readonly']
        if dataset is not None:
            args += ['--mount', 'type=bind,src=' + str(dataset / 'pgdata') + ',dst=/var/lib/postgresql']
        else:
            args += ['--tmpfs','/var/lib/postgresql:rw,nosuid,nodev,noexec,size=1m,mode=0700']
        args += [image['image_id'],*wrapped]
        cid = self.out('docker', args).decode().strip()
        need(re.fullmatch('[0-9a-f]{64}', cid), 'create_ack_unknown')
        self.containers[cid] = {'label': label, 'running_witness': None, 'removed': False}
        observed = self.inspect_container(cid)
        need(not observed['State']['Running'] and observed['State']['Pid'] == 0, 'create_state_unexpected')
        expected_limits={'readonly':True,'network':'none','cpu':1000000000,'cpuset':str(self.cpu),'memory':536870912,'swap':536870912,'pids':128,'privileged':False,'capdrop':['ALL'],'ports':None,'restart':{'Name':'no','MaximumRetryCount':0}}
        actual_limits=observed['Limits'];expected_limits['ports']={}if actual_limits.get('ports')=={}else None
        need(actual_limits==expected_limits and actual_limits['readonly']is True and actual_limits['privileged']is False and observed['User']==f"{image['uid']}:{image['gid']}",'actual_container_resource_boundary')
        need(observed['Entrypoint']==['/bin/sh']and observed['Cmd']==wrapped,'fixed_start_gate_command')
        expected_mounts = [(str(gate),'/openlegal-start',False)]
        if dataset is not None:expected_mounts.append((str(dataset/'pgdata'),'/var/lib/postgresql',True))
        actual_mounts = [(m['Source'], m['Destination'], m['RW']) for m in observed['Mounts']if m['Type']!='tmpfs']
        need(all(m['Type']in('bind','tmpfs')and(m['Type']!='tmpfs'or m['Destination']in('/tmp','/var/lib/postgresql'))for m in observed['Mounts']),'unexpected_image_volume')
        need(sorted(actual_mounts)==sorted(expected_mounts),'only_owned_test_mount_allowed')
        self.journal.save('container-' + label + '-created.json', {'id': cid, 'observed': observed})
        return cid

    def start_container(self,cid):
        """Seal actual PID/start/cgroup before any short or long payload exec."""
        owned=self.containers[cid];gate=self.gates[owned['label']]
        need(not gate['released'],'start_gate_one_use')
        self.check_capacity('before-container-start')
        need(self.out('docker',['start',cid]).decode().strip()==cid,'start_ack_unknown')
        observed=self.inspect_container(cid)
        need(observed['State']['Running']is True and type(observed['State']['Pid'])is int and observed['State']['Pid']>0,'gated_actor_not_running')
        witness=cgroup_identity(observed['State']['Pid']);need(cid in witness['group'],'unsupported_container_cgroup_layout')
        owned['running_witness']=witness
        self.journal.save('container-'+owned['label']+'-running.json',{'id':cid,'actual_gated_pid_cgroup':witness,'original_command_still_inhibited':True})
        self.check_capacity('before-start-release')
        self.runner.seal()
        fd=os.open(gate['path'],os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
        try:
            held=os.fstat(fd);now=gate['path'].lstat();need((held.st_dev,held.st_ino)==(now.st_dev,now.st_ino)==(gate['dev'],gate['ino'])and held.st_uid==0 and stat.S_IMODE(held.st_mode)==0o755,'start_gate_rebound')
            out=os.open('go',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o644,dir_fd=fd)
            try:
                raw=(self.operation+':'+owned['label']+'\n').encode();need(os.write(out,raw)==len(raw),'start_gate_write');os.fchmod(out,0o644);os.fsync(out)
            finally:os.close(out)
            os.fsync(fd)
            gate['released']=True
        finally:os.close(fd)
        self.journal.save('container-'+owned['label']+'-released.json',{'id':cid,'gated_witness_sha256':sha(canonical(witness)),'one_fixed_payload_released':True})

    def run_short(self,cid):
        self.start_container(cid)
        need(self.out('docker',['wait',cid]).decode().strip()=='0','short_actor_actual_exit')
        output=self.out('docker',['logs','--tail','2048',cid])
        self.check_capacity('after-short-actor')
        return output

    def finish_container(self, cid):
        owned = self.containers[cid]
        observed = self.inspect_container(cid)
        if observed['State']['Running']:
            need(owned['running_witness'] is not None, 'missing_running_container_witness')
            self.out('docker', ['stop', '--time', '10', cid])
            rc = self.out('docker', ['wait', cid]).decode().strip()
            need(rc == '0', 'postgres_actual_exit_nonzero')
            observed = self.inspect_container(cid)
        need(observed['State']['Running'] is False and observed['State']['Pid'] == 0 and
             type(observed['State']['ExitCode']) is int and observed['State']['ExitCode'] == 0,
             'container_exit_not_proven')
        need(owned['running_witness']is not None,'missing_actual_container_cgroup_witness')
        closure=cgroup_closed(owned['running_witness'],self.deadline)
        need(self.runner.closed, 'local_cli_unclosed')
        self.journal.save('container-' + owned['label'] + '-closed.json',
                          {'id': cid, 'actual_exit_code': 0, 'observed': observed, 'physical_closure': closure})
        need(self.out('docker', ['rm', cid]).decode().strip() == cid, 'container_remove_ack')
        owned['removed'] = True

    def properties(self, name):
        keys = 'guid,mountpoint,recordsize,compression,sync,atime,quota,org.openlegal:hdd-benchmark'
        raw = self.out('zfs', ['get', '-Hp', '-o', 'property,value', keys, name]).decode()
        result = {}
        for line in raw.splitlines():
            key, value = line.split('\t')
            need(key not in result, 'dataset_properties_duplicate')
            result[key] = value
        need(set(result) == set(keys.split(',')), 'dataset_properties_missing')
        return result

    def create_datasets(self):
        parent = self.fixed['dataset_parent']
        # No pre-existing namespace may be adopted, and an error is not absence.
        names = self.out('zfs', ['list', '-Hp', '-o', 'name', '-r', self.inventory['pool']['name']]).decode().splitlines()
        need(not any(n == parent or n.startswith(parent + '/') for n in names), 'benchmark_namespace_exists')
        self.out('zfs', ['create', '-o', 'canmount=off', '-o', 'mountpoint=none', '-o', 'quota=2147483648',
                         '-o', 'org.openlegal:hdd-benchmark=' + self.operation, parent])
        need(self.datasets[parent]==self.properties(parent)['guid'],'parent_guid_changed_after_create')
        for record in self.fixed['datasets']:
            args = ['create']
            for key in ('mountpoint', 'recordsize', 'compression', 'sync', 'atime', 'quota'):
                args += ['-o', key + '=' + str(record[key])]
            args += ['-o', 'org.openlegal:hdd-benchmark=' + self.operation, record['name']]
            self.out('zfs', args)
            actual = self.properties(record['name'])
            expected = {'mountpoint': record['mountpoint'], 'recordsize': str(int(record['recordsize'][:-1]) * 1024),
                        'compression': 'lz4', 'sync': 'standard', 'atime': 'off', 'quota': '1073741824',
                        'org.openlegal:hdd-benchmark': self.operation}
            need(all(actual[k] == v for k, v in expected.items()) and re.fullmatch('[0-9]{1,20}', actual['guid']),
                 'fresh_dataset_properties')
            need(self.datasets[record['name']]==actual['guid'],'child_guid_changed_after_create')
            mount = Path(record['mountpoint'])
            need(mount_present(mount, record['name']), 'kernel_dataset_mount_binding')
            os.chmod(mount, 0o700)
            os.mkdir(mount / 'pgdata', 0o700)
            image = self.inventory['native_pg_image']
            os.chown(mount / 'pgdata', image['uid'], image['gid'])
            os.mkdir(mount / 'blobs', 0o700)
        self.journal.save('created-datasets.json', {'guids': self.datasets, 'fixed_plan': self.fixed})

    def start_postgres(self, record, number):
        size = record['recordsize'][:-1]
        mount = Path(record['mountpoint'])
        if number==0:
            cid = self.create_container('init-' + size, mount,
                  ['initdb', '-D', '/var/lib/postgresql/data', '--auth-local=trust', '--auth-host=reject',
                   '--no-locale', '--encoding=UTF8', '--username=postgres'])
            self.run_short(cid)
            self.finish_container(cid)
        cid = self.create_container('postgres-' + size + '-' + str(number), mount,
              ['postgres', '-D', '/var/lib/postgresql/data', '-k', '/tmp', '-c', "listen_addresses=",
               '-c', 'fsync=on', '-c', 'full_page_writes=on', '-c', 'synchronous_commit=on',
               '-c', 'shared_buffers=64MB', '-c', 'max_connections=4', '-c', 'work_mem=4MB',
               '-c', 'max_wal_size=128MB', '-c', 'min_wal_size=32MB', '-c', 'temp_file_limit=64MB',
               '-c', 'log_statement=none', '-c', 'log_min_messages=warning'])
        self.start_container(cid)
        observed = self.inspect_container(cid)
        need(observed['State']['Running'] is True and observed['State']['Pid'] > 0, 'postgres_not_running')
        need(observed['State']['Pid']==self.containers[cid]['running_witness']['pid'],'gated_postgres_pid_changed')
        end = min(self.deadline, time.monotonic() + 30)
        # A fixed read-only readiness query may observe startup, never replay DML.
        while True:
            rc, output, _ = self.command('docker', ['exec', cid, 'pg_isready', '-h', '/tmp', '-U', 'postgres'],
                                         allowed_failure=True)
            if rc == 0:
                break
            need(time.monotonic() < end, 'postgres_ready_deadline')
            time.sleep(0.2)
        self.sql(cid, b"CREATE TABLE IF NOT EXISTS benchmark_commits (r integer NOT NULL, i integer NOT NULL, payload text NOT NULL);\n")
        return cid

    def sql(self, cid, statement):
        self.check_capacity('before-sql')
        result=self.command('docker', ['exec', '-i', cid, 'psql', '-X', '-A', '-t', '-v', 'ON_ERROR_STOP=1',
                                      '-h', '/tmp', '-U', 'postgres', '-d', 'postgres'], statement)
        elapsed=self.runner.last_elapsed_us
        self.check_capacity('after-sql')
        self.last_sql_elapsed_us=elapsed
        return result

    def settings(self, cid):
        _, raw, _ = self.sql(cid, b"SELECT current_setting('server_version_num'),current_setting('fsync'),current_setting('full_page_writes'),current_setting('synchronous_commit'),current_setting('listen_addresses');\n")
        fields = raw.decode().strip().split('|')
        need(len(fields) == 5 and fields[0].isdigit() and 180000 <= int(fields[0]) < 190000 and
             fields[1:] == ['on', 'on', 'on', ''], 'actual_durability_settings')
        return fields

    def round(self, size, number, cid):
        self.verify_pool()
        self.settings(cid)
        self.sql(cid, b'CHECKPOINT;\n')
        before = {'at': time.time(), 'io_pressure': Path('/proc/pressure/io').read_text(),
                  'loadavg': Path('/proc/loadavg').read_text()}
        statement = ("DO $benchmark$ DECLARE s timestamptz; n integer; us bigint; BEGIN "
                     f"FOR n IN 1..{COMMITS} LOOP s:=clock_timestamp(); "
                     f"INSERT INTO benchmark_commits SELECT {number},n,string_agg(md5((n*256+g)::text),'') "
                     "FROM generate_series(1,256)g; COMMIT; "
                     "us:=floor(extract(epoch FROM clock_timestamp()-s)*1000000)::bigint; "
                     f"RAISE NOTICE 'openlegal_hdd_commit:{number}:%:%',n,us; "
                     "END LOOP; END $benchmark$;\n").encode()
        _, _, raw = self.sql(cid, statement)
        client_us = self.last_sql_elapsed_us
        commits = latency_samples(raw, number)
        record = next(r for r in self.fixed['datasets'] if r['recordsize'] == str(size) + 'K')
        blob = {}
        for budget in (5, 30):
            # Per-round unique subdirectory: a failed probe cannot be reused.
            target = Path(record['mountpoint']) / 'blobs' / f'round-{number}-budget-{budget}'
            self.check_capacity('before-blob-probe')
            os.mkdir(target, 0o700)
            self.journal.save(f'probe-{size}-{number}-{budget}-intent.json', {'path': str(target),
                              'logical_budget_secs': budget, 'samples': SAMPLES})
            blob[str(budget)] = blob_probe(target, budget, SAMPLES, self.deadline)
            self.check_capacity('after-blob-probe')
            self.journal.save(f'probe-{size}-{number}-{budget}-result.json', blob[str(budget)])
        self.settings(cid)
        row = {'recordsize': size, 'round': number, 'commits_us': commits,
               'client_roundtrip_us': client_us, 'blob': blob, 'source_observation_before': before,
               'source_observation_after': {'at': time.time(), 'io_pressure': Path('/proc/pressure/io').read_text(),
                                            'loadavg': Path('/proc/loadavg').read_text()}}
        self.journal.save(f'round-{number}-{size}.json', row)
        self.rounds.append(row)

    def cleanup(self):
        need(self.runner.closed and all(r['removed'] for r in self.containers.values()), 'owned_actors_not_closed')
        remaining = self.out('docker', ['ps', '-aq', '--filter', 'label=openlegal.hdd-benchmark.operation=' + self.operation]).decode().split()
        need(not remaining, 'unexpected_owned_container_remains')
        parent = self.fixed['dataset_parent']
        descendants = self.out('zfs', ['list', '-Hp', '-t', 'filesystem,snapshot', '-o', 'name', '-r', parent]).decode().splitlines()
        need(set(descendants) == set(self.datasets), 'unexpected_dataset_or_snapshot_preserved')
        for record in reversed(self.fixed['datasets']):
            name = record['name']
            current = self.properties(name)
            need(current['guid'] == self.datasets[name] and current['org.openlegal:hdd-benchmark'] == self.operation
                 and current['mountpoint'] == record['mountpoint'], 'cleanup_guid_changed')
            self.out('zfs', ['unmount', name])
            need(mounts_clear(record['mountpoint'],name),'benchmark_mount_remains')
            # Nonrecursive destroy, exact newly created GUID/name only.
            need(self.properties(name)['guid'] == self.datasets[name], 'cleanup_guid_race')
            self.out('zfs', ['destroy', name])
        current = self.properties(parent)
        need(current['guid'] == self.datasets[parent] and current['org.openlegal:hdd-benchmark'] == self.operation,
             'cleanup_parent_guid_changed')
        need(mounts_clear('none',parent),'benchmark_parent_mount_remains')
        self.out('zfs', ['destroy', parent])
        self.journal.save('cleanup.json', {'owned_container_ids': list(self.containers), 'created_dataset_guids': self.datasets,
                          'exact_owned_test_resources_removed': True, 'original_resources_deleted': False})

    def run(self):
        os.sched_setaffinity(0,{self.cpu})
        self.qualify_host()
        self.create_datasets()
        for number, order in enumerate(((128, 32), (32, 128), (128, 32))):
            for size in order:
                record=next(r for r in self.fixed['datasets']if r['recordsize']==str(size)+'K')
                cid=self.start_postgres(record,number)
                self.round(size,number,cid)
                self.finish_container(cid)
        verdict = assess(self.rounds)
        self.journal.save('diagnostic-verdict.json', verdict)
        self.cleanup()
        return verdict


class PoolLock:
    def __init__(self,pool_guid,operation):
        self.path=Path('/tmp/openlegal-hdd-benchmark-pool-'+pool_guid+'.lock')
        self.fd=os.open(self.path,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
        self.identity=os.fstat(self.fd)
        raw=canonical({'operation_id':operation,'pool_guid':pool_guid,'unknown_requires_operator_audit':True})
        self.raw=raw
        try:
            fcntl.flock(self.fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            need(os.write(self.fd,raw)==len(raw),'pool_lock_write');os.fsync(self.fd)
            parent=os.open('/tmp',os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
            try:os.fsync(parent)
            finally:os.close(parent)
        except BaseException:
            os.close(self.fd);raise
    def check(self):
        current=self.path.lstat();need((current.st_dev,current.st_ino)==(self.identity.st_dev,self.identity.st_ino)and current.st_uid==0 and current.st_nlink==1 and stat.S_IMODE(current.st_mode)==0o600,'pool_lock_rebound')
        need(os.pread(self.fd,len(self.raw)+1,0)==self.raw,'pool_lock_evidence_changed')
    def close(self,clean):
        try:
            self.check()
            if clean:
                os.unlink(self.path)
                parent=os.open('/tmp',os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
                try:os.fsync(parent)
                finally:os.close(parent)
        finally:os.close(self.fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('qualify', 'run'))
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--inventory-sha256', required=True)
    parser.add_argument('--source-sha256', required=True)
    parser.add_argument('--operation', required=True)
    parser.add_argument('--approval', type=Path)
    parser.add_argument('--approval-sha256')
    args = parser.parse_args()
    need(os.geteuid() == 0 and sys.dont_write_bytecode and Path(__file__).resolve() == SELF,
         'fixed_root_source_required')
    os.umask(0o077)
    source = private_bytes(SELF, args.source_sha256)
    raw = private_bytes(args.inventory, args.inventory_sha256)
    inventory = validate_inventory(decode(raw), args.operation)
    fixed = plan(inventory, args.operation)
    load_process_sources(inventory)  # Qualification authenticates dependencies too.
    if args.action == 'qualify':
        print(canonical({'status': 'source_and_schema_only', 'plan': fixed,
                         'plan_sha256': sha(canonical(fixed)), 'effects_permitted': False}).decode(), end='')
        return
    need(args.approval is not None and pin(args.approval_sha256), 'actual_authority_required')
    authority_raw = private_bytes(args.approval, args.approval_sha256)
    authority = decode(authority_raw)
    validate_approval(authority, inventory, args.inventory_sha256, args.source_sha256, args.operation)
    deadline = time.monotonic() + inventory['total_deadline_secs']
    pool_lock=PoolLock(inventory['pool']['guid'],args.operation)
    try:
        journal = Journal(inventory['evidence_root'], {'operation_id': args.operation,
              'inventory_sha256': args.inventory_sha256, 'source_sha256': sha(source),
              'authority_sha256': args.approval_sha256, 'plan': fixed, 'at': time.time()})
    except BaseException:
        pool_lock.close(False);raise
    clean=False
    def seal():
        need(time.monotonic() < deadline, 'operation_deadline')
        journal.check();pool_lock.check()
        private_bytes(SELF, args.source_sha256)
        process_source_bytes(inventory)
        private_bytes(args.inventory, args.inventory_sha256)
        private_bytes(args.approval, args.approval_sha256)
        validate_approval(authority, inventory, args.inventory_sha256, args.source_sha256, args.operation)
        need(Path('/proc/sys/kernel/random/boot_id').read_text().strip() == inventory['host_boot_id'], 'host_rebooted')
    runner = None
    try:
        runner = Runner(inventory, journal, deadline, seal)
        result = Benchmark(inventory, args.operation, journal, runner, deadline).run()
        seal()
        journal.save('completed.json', {'status': 'diagnostic_complete_pending_actual_operator_exit',
                     'actual_owned_cli_closed': runner.closed, 'verdict': result, 'at': time.time()})
        print(canonical(result).decode(), end='')
        clean=True
    except BaseException:
        try: journal.save('failed.json', {'status': 'FAILED_OR_UNKNOWN', 'automatic_cleanup': False,
                          'automatic_retry': False, 'operator_reconciliation_required': True, 'at': time.time()})
        except BaseException: pass
        raise
    finally:
        try:journal.close()
        finally:pool_lock.close(clean)


if __name__ == '__main__':
    try:
        main()
    except BaseException:
        print('{"status":"FAILED_OR_UNKNOWN","automatic_retry":false}', file=sys.stderr)
        raise SystemExit(1)
