#!/usr/bin/env python3
"""One-use /tmp-only Docker daemon for the independently approved HDD probe.

This is not a service installer. Imports and qualify have no effects. A failed
operation retains all files/datasets as UNKNOWN. Logical deadlines cannot close
kernel-uninterruptible I/O. The outer supervisor must observe this CLI's OS exit.
"""
from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import json
import math
import os
import platform
import re
import resource
import select
import signal
import stat
import subprocess
import sys
import tarfile
import threading
import time
import tomllib
import types
import uuid
from pathlib import Path

SELF = Path('/usr/local/lib/openlegal-storage-stability/diagnostic_daemon.py')
HDD_SOURCE = '98371e85779a9dad21b10b79454d5f434c29d4ffb70190091b6c65ab948b95c4'
SOURCES = {'hdd_benchmark.py', 'interface.py', 'command_process.py'}
# This literal config profile is qualified for these installed runtime builds.
RUNTIME_PINS = {
    'containerd': '6c9b8815dc49b9ae1c45acb4b0273d15cd79a74ba8fc5f9b985e4379dd528230',
    'containerd-shim-runc-v2': 'ca4c4b5919655332666dda634338b826d73a12cdd3b6633627e95d1ab4bc138a',
    'runc': '0b151efc81aee92882be80424b407e668b74f119f4497f2cdc9ef848dfb17581'}
RUNTIME_DIRS = ('cd', 'cs', 'ct', 'co')
DISABLED_PLUGINS = ('io.containerd.cri.v1.images', 'io.containerd.cri.v1.runtime',
    'io.containerd.grpc.v1.cri', 'io.containerd.nri.v1.nri',
    'io.containerd.image-verifier.v1.bindir', 'io.containerd.server.v1.grpc-tcp',
    'io.containerd.server.v1.debug', 'io.containerd.server.v1.metrics')
LIMIT = 2 * 1024**2
ENV = {'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
       'LANG': 'C', 'LC_ALL': 'C', 'TMPDIR': '/tmp'}
DIGEST = re.compile(r'sha256:[0-9a-f]{64}\Z')
MAX_IMAGE_METADATA = 64 * 1024
MAX_ARCHIVE_MEMBERS = 4096
INDEX_MEDIA = 'application/vnd.oci.image.index.v1+json'
MANIFEST_MEDIA = 'application/vnd.oci.image.manifest.v1+json'
CONFIG_MEDIA = 'application/vnd.oci.image.config.v1+json'
LAYER_MEDIA = 'application/vnd.oci.image.layer.v1.tar+gzip'
ACTIONS = ('default-inspect', 'save-image', 'runtime-profile', 'containerd', 'daemon', 'private-info',
           'load-image', 'private-inspect', 'private-empty', 'benchmark')


class Refused(RuntimeError):
    pass


def need(value, code):
    if not value:
        raise Refused(code)


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'),
                       allow_nan=False, ensure_ascii=False) + '\n').encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def pin(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def uid(value):
    try:
        return type(value) is str and str(uuid.UUID(value)) == value and uuid.UUID(value).int != 0
    except (ValueError, AttributeError):
        return False


def decode(raw):
    need(type(raw) is bytes and len(raw) <= LIMIT, 'json_bound')
    def pairs(rows):
        out = {}
        for key, value in rows:
            need(key not in out, 'json_duplicate')
            out[key] = value
        return out
    try:
        value = json.loads(raw, object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(Refused('json_nonfinite')))
    except (ValueError, UnicodeError, RecursionError):
        raise Refused('json_invalid') from None
    def visit(item, depth=0):
        need(depth <= 24, 'json_depth')
        if type(item) is dict:
            for child in item.values(): visit(child, depth + 1)
        elif type(item) is list:
            need(len(item) <= 4096, 'json_list')
            for child in item: visit(child, depth + 1)
    visit(value)
    return value


def private_bytes(path, expected=None, *, modes=(0o600,), limit=LIMIT):
    path = Path(path)
    need(path.is_absolute() and path.resolve() == path, 'private_path')
    for parent in path.parents:
        meta = parent.lstat()
        need(stat.S_ISDIR(meta.st_mode) and meta.st_uid == 0 and
             (not meta.st_mode & 0o022 or parent == Path('/tmp') and meta.st_mode & stat.S_ISVTX),
             'private_parent')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        need(stat.S_ISREG(before.st_mode) and before.st_uid == 0 and before.st_nlink == 1
             and stat.S_IMODE(before.st_mode) in modes and before.st_size <= limit, 'private_file')
        chunks = []; total = 0
        while block := os.read(fd, min(1024 * 1024, limit + 1 - total)):
            chunks.append(block); total += len(block)
            need(total <= limit, 'private_bound')
        after = os.fstat(fd); current = path.lstat()
        need(all(getattr(before, key) == getattr(after, key) == getattr(current, key)
                 for key in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')),
             'private_changed')
        raw = b''.join(chunks)
        need(len(raw) == before.st_size and (expected is None or pin(expected) and sha(raw) == expected),
             'private_hash')
        return raw
    finally:
        os.close(fd)


def executable(entry):
    need(type(entry) is dict and set(entry) == {'path', 'sha256'} and pin(entry['sha256']), 'binary_pin')
    path = Path(entry['path'])
    need(path.is_absolute() and path.resolve() == path, 'binary_path')
    for parent in path.parents:
        meta = parent.lstat()
        need(stat.S_ISDIR(meta.st_mode) and meta.st_uid == 0 and
             (not meta.st_mode & 0o022 or parent == Path('/tmp') and meta.st_mode & stat.S_ISVTX),
             'binary_parent')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        need(stat.S_ISREG(before.st_mode) and before.st_uid == 0 and before.st_nlink == 1
             and not before.st_mode & 0o022 and before.st_mode & 0o111 and before.st_size <= 128 * 1024**2,
             'binary_metadata')
        hashed = hashlib.sha256()
        while block := os.read(fd, 1024 * 1024): hashed.update(block)
        after = os.fstat(fd); current = path.lstat()
        need(all(getattr(before, key) == getattr(after, key) == getattr(current, key)
                 for key in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns'))
             and hashed.hexdigest() == entry['sha256'], 'binary_hash')
        return fd
    except BaseException:
        os.close(fd); raise


def root_path(value):
    return Path('/tmp/openlegal-hdd-docker-' + value['operation_id'])


def runtime_socket_root(value):
    # v2.4.2 shim CreateSocketAddress appends '/' plus 64 hexadecimal bytes.
    # Full operation-root/sh would exceed its 106-byte pathname limit.
    return Path('/tmp/olc-' + sha(value['operation_id'].encode())[:12])


def runtime_namespaces(value):
    suffix = sha(value['operation_id'].encode())[:12]
    return ('olh-' + suffix, 'olp-' + suffix)


def runtime_config(value):
    root = root_path(value); sockets = runtime_socket_root(value)
    need(len(os.fsencode(sockets / ('f' * 64))) <= 106, 'runtime_socket_length')
    lines = ['version = 4', 'root = ' + json.dumps(str(root / 'cd')),
        'state = ' + json.dumps(str(root / 'cs')), 'temp = ' + json.dumps(str(root / 'ct')),
        'imports = []', 'disabled_plugins = ' + json.dumps(list(DISABLED_PLUGINS))]
    for name, settings in (
        ('io.containerd.server.v1.grpc', {'address': str(root / 'c.sock'), 'uid': 0, 'gid': 0}),
        ('io.containerd.server.v1.ttrpc', {'address': str(root / 'c.ttrpc'), 'uid': 0, 'gid': 0}),
        ('io.containerd.internal.v1.opt', {'path': str(root / 'co')}),
        ('io.containerd.shim.v1.manager', {'socket_dir': str(sockets), 'env': []}),
        ('io.containerd.service.v1.tasks-service', {'blockio_config_file': '', 'rdt_config_file': ''})):
        lines.append('[plugins.' + json.dumps(name) + ']')
        lines.extend(key + ' = ' + json.dumps(item) for key, item in settings.items())
    lines.extend(['[cgroup]', 'path = ""'])
    return ('\n'.join(lines) + '\n').encode()


def runtime_args(value):
    root = root_path(value)
    return ['--config=' + str(root / 'containerd.toml'), '--root=' + str(root / 'cd'),
            '--state=' + str(root / 'cs'), '--address=' + str(root / 'c.sock')]


def payload_env(value):
    # Helpers resolved by containerd/runc use only their pinned /usr/bin directory.
    return dict(ENV, PATH='/usr/bin', TMPDIR=str(root_path(value) / 'ct'),
                DOCKER_TMPDIR=str(root_path(value) / 'ct'))


def check_runtime_binaries(value):
    for name in RUNTIME_PINS:
        fd = executable(value['binaries'][name]); os.close(fd)


def hold_runtime_binaries(p):
    need(not getattr(p, 'runtime_holds', {}), 'runtime_pins_already_held')
    p.runtime_holds = {}
    try:
        for name in RUNTIME_PINS:
            fd = executable(p.value['binaries'][name])
            p.runtime_holds[name] = (fd, os.fstat(fd))
    except BaseException:
        close_runtime_binaries(p); raise


def check_held_runtime(p):
    for name, (fd, before) in getattr(p, 'runtime_holds', {}).items():
        current = Path(p.value['binaries'][name]['path']).lstat(); held = os.fstat(fd)
        need(all(getattr(current, k) == getattr(held, k) == getattr(before, k) for k in
             ('st_dev', 'st_ino', 'st_size', 'st_mode', 'st_uid', 'st_gid', 'st_nlink', 'st_mtime_ns', 'st_ctime_ns')),
             'held_runtime_binary_changed')


def close_runtime_binaries(p):
    for fd, _ in getattr(p, 'runtime_holds', {}).values(): os.close(fd)
    p.runtime_holds = {}


def validate_inventory(value):
    fields = {'schema_version', 'kind', 'operation_id', 'host_boot_id', 'total_deadline_secs',
              'image_archive_max_bytes', 'official_reference', 'official_image_id',
              'platform_manifest_digest', 'image_config_digest', 'binaries', 'source_graph',
              'hdd_inventory', 'hdd_approval'}
    need(type(value) is dict and set(value) == fields and type(value['schema_version']) is int
         and value['schema_version'] == 1 and value['kind'] == 'openlegal_hdd_private_daemon_inventory',
         'inventory_shape')
    need(uid(value['operation_id']) and uid(value['host_boot_id']), 'inventory_identity')
    need(type(value['total_deadline_secs']) is int and 1 <= value['total_deadline_secs'] <= 1800
         and type(value['image_archive_max_bytes']) is int
         and 1024**2 <= value['image_archive_max_bytes'] <= 2 * 1024**3, 'inventory_bounds')
    need(type(value['official_reference']) is str and
         re.fullmatch(r'(?:postgres|docker.io/library/postgres)@sha256:[0-9a-f]{64}', value['official_reference']),
         'official_image_only')
    need(all(type(value[k]) is str and DIGEST.fullmatch(value[k]) for k in (
        'official_image_id','platform_manifest_digest','image_config_digest'))
        and value['official_reference'].split('@',1)[1] == value['official_image_id'],
        'typed_image_pins')
    need(type(value['source_graph']) is dict and set(value['source_graph']) == SOURCES
         and all(pin(v) for v in value['source_graph'].values())
         and value['source_graph']['hdd_benchmark.py'] == HDD_SOURCE, 'exact_reviewed_sources')
    binaries = value['binaries']
    need(type(binaries) is dict and set(binaries) == {'python', 'docker', 'dockerd', *RUNTIME_PINS}, 'exact_binaries')
    for name, entry in binaries.items():
        need(type(entry) is dict and set(entry) == {'path', 'sha256'} and pin(entry['sha256'])
             and type(entry['path']) is str and Path(entry['path']).is_absolute(), 'binary_pin')
        need(name == 'python' or Path(entry['path']).name == name, 'binary_fixed_name')
    need(Path(binaries['python']['path']) == Path(sys.executable).resolve(), 'same_python')
    need(all(binaries[name] == {'path': '/usr/bin/' + name, 'sha256': digest}
             for name, digest in RUNTIME_PINS.items()), 'fixed_runtime_profile')
    for key in ('hdd_inventory', 'hdd_approval'):
        row = value[key]
        need(type(row) is dict and set(row) == {'path', 'sha256'} and pin(row['sha256'])
             and type(row['path']) is str and row['path'].startswith('/tmp/'), 'hdd_input_pin')
    return value


def daemon_args(value):
    root = root_path(value)
    return ['--config-file=' + str(root / 'daemon.json'), '--host=unix://' + str(root / 'docker.sock'),
            '--data-root=' + str(root / 'data'), '--exec-root=' + str(root / 'exec'),
            '--pidfile=' + str(root / 'docker.pid'), '--bridge=none', '--iptables=false',
            '--ip6tables=false', '--ip-forward=false', '--ip-masq=false',
            '--userland-proxy=false', '--storage-driver=vfs', '--log-level=error',
            '--containerd=' + str(root / 'c.sock'),
            '--containerd-namespace=' + runtime_namespaces(value)[0],
            '--containerd-plugins-namespace=' + runtime_namespaces(value)[1]]


def plan(value):
    validate_inventory(value)
    return {'kind': 'openlegal_hdd_private_daemon_plan', 'operation_id': value['operation_id'],
            'root': str(root_path(value)), 'dockerd_argv': daemon_args(value),
            'containerd_argv': runtime_args(value), 'runtime_config_sha256': sha(runtime_config(value)),
            'runtime_socket_root': str(runtime_socket_root(value)),
            'runtime_namespaces': list(runtime_namespaces(value)),
            'official_image_id': value['official_image_id'],
            'platform_manifest_digest': value['platform_manifest_digest'],
            'image_config_digest': value['image_config_digest'],
            'default_daemon_operations': ['image inspect', 'image save'],
            'private_hdd_reference_domain': 'exact_image_id', 'registry_fallback': False,
            'production_daemon_changes': False, 'automatic_cleanup': False,
            'hdd_inventory': value['hdd_inventory'], 'hdd_approval': value['hdd_approval'],
            'archive_max_bytes': value['image_archive_max_bytes'], 'total_deadline_secs': value['total_deadline_secs']}


def validate_authority(authority, value, inventory_sha, source_sha):
    expected = {'schema_version': 1, 'kind': 'openlegal_hdd_private_daemon_authority',
                'approved': True, 'operation_id': value['operation_id'], 'inventory_sha256': inventory_sha,
                'source_sha256': source_sha, 'plan_sha256': sha(canonical(plan(value))),
                'private_daemon_permitted': True, 'default_daemon_read_only': True,
                'approved_hdd_cli_only': True, 'production_changes_permitted': False,
                'registry_calls_permitted': False, 'automatic_retry': False}
    need(type(authority) is dict and set(authority) == set(expected) | {'valid_until'} and
         all(type(authority[k]) is type(v) and authority[k] == v for k, v in expected.items()),
         'authority_binding')
    need(type(authority['valid_until']) in (int, float) and math.isfinite(authority['valid_until'])
         and time.time() < authority['valid_until'] <= time.time() + 86400, 'authority_window')


def load(args):
    need(os.geteuid() == 0 and sys.dont_write_bytecode and Path(__file__).resolve() == SELF,
         'installed_root_no_bytecode')
    private_bytes(SELF, args.source_sha256)
    value = validate_inventory(decode(private_bytes(args.inventory, args.inventory_sha256)))
    authority = decode(private_bytes(args.authority, args.authority_sha256))
    validate_authority(authority, value, args.inventory_sha256, args.source_sha256)
    sources = {name: private_bytes(SELF.parent / name, value['source_graph'][name]) for name in SOURCES}
    hdd = types.ModuleType('_openlegal_private_daemon_hdd'); hdd.__file__ = str(SELF.parent / 'hdd_benchmark.py')
    exec(compile(sources['hdd_benchmark.py'], hdd.__file__, 'exec'), hdd.__dict__)
    hi = value['hdd_inventory']; ha = value['hdd_approval']
    hv = hdd.validate_inventory(hdd.decode(private_bytes(hi['path'], hi['sha256'])), value['operation_id'])
    hdd.validate_approval(hdd.decode(private_bytes(ha['path'], ha['sha256'])), hv,
                         hi['sha256'], HDD_SOURCE, value['operation_id'])
    validate_hdd_binding(value, hv)
    for entry in value['binaries'].values():
        fd = executable(entry); os.close(fd)
    process = hdd.load_process_sources(hv)
    return types.SimpleNamespace(value=value, authority=authority, hdd=hdd, hv=hv, process=process, args=args)


def validate_hdd_binding(value, hv):
    root = root_path(value)
    need(hv['host_boot_id'] == value['host_boot_id'] and hv['docker'] == {
        'endpoint': 'unix://' + str(root / 'docker.sock'), 'root_dir': str(root / 'data')}, 'hdd_private_route')
    need(hv['process_sources'] == {n: value['source_graph'][n] for n in ('interface.py', 'command_process.py')}
         and hv['binaries']['docker'] == value['binaries']['docker'], 'hdd_dependency_binding')
    image = hv['native_pg_image']
    need(image['reference'] == image['image_id'] == value['image_config_digest'] and image['os'] == 'linux'
         and image['architecture'] == {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(platform.machine()),
         'native_image_id_reference')


def seal(p, deadline):
    check_held_runtime(p)
    need(not getattr(p, 'interrupted', False), 'controller_interrupted')
    need(time.monotonic() < deadline, 'operation_deadline')
    need(Path('/proc/sys/kernel/random/boot_id').read_text().strip() == p.value['host_boot_id'], 'host_rebooted')
    for path, pinvalue in ((SELF, p.args.source_sha256), (p.args.inventory, p.args.inventory_sha256),
                           (p.args.authority, p.args.authority_sha256)):
        private_bytes(path, pinvalue)
    validate_authority(p.authority, p.value, p.args.inventory_sha256, p.args.source_sha256)
    for name, pinvalue in p.value['source_graph'].items(): private_bytes(SELF.parent / name, pinvalue)
    for key in ('hdd_inventory', 'hdd_approval'):
        row = p.value[key]; private_bytes(row['path'], row['sha256'])


def witness(pid):
    proc = Path('/proc') / str(pid)
    tail = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
    exe = (proc / 'exe').stat()
    result = {'pid': pid, 'start_ticks': tail[19], 'ppid': int(tail[1]),
              'exe_dev': exe.st_dev, 'exe_ino': exe.st_ino,
              'cmdline_sha256': sha((proc / 'cmdline').read_bytes()),
              'cgroup_sha256': sha((proc / 'cgroup').read_bytes()),
              'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
    need((proc / 'stat').read_text().rsplit(')', 1)[1].split()[19] == result['start_ticks'], 'process_reused')
    return result


def same_process(before, *, parent=None):
    current = witness(before['pid'])
    need(all(current[k] == before[k] for k in before), 'process_changed')
    need(parent is None or current['ppid'] == parent, 'process_parent')
    return current


def alive(fd):
    poll = select.poll(); poll.register(fd, select.POLLIN)
    events = poll.poll(0)
    need(not any(flags & select.POLLNVAL for _, flags in events), 'pidfd_invalid')
    return not events


def signal_owned(row, number):
    if alive(row['pidfd']):
        # A known worker can die first, reparenting its witnessed daemon. The
        # retained pidfd/start/exe/argv identity remains owned across reparenting.
        current = witness(row['witness']['pid'])
        need(all(current[k] == v for k, v in row['witness'].items() if k != 'ppid'), 'signal_identity_changed')
        try: signal.pidfd_send_signal(row['pidfd'], number, None, 0)
        except ProcessLookupError: pass


class Evidence:
    def __init__(self, root):
        self.root = Path(root); self.identity = self.root.lstat()
        self.fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        self.directories = {}
        self.check()

    def check(self):
        now = self.root.lstat(); held = os.fstat(self.fd)
        need(now.st_uid == now.st_gid == 0 and stat.S_IMODE(now.st_mode) == 0o700 and
             (now.st_dev, now.st_ino) == (held.st_dev, held.st_ino) ==
             (self.identity.st_dev, self.identity.st_ino), 'operation_root_changed')

    def save(self, name, value):
        return self.save_bytes(name, canonical(value))

    def save_bytes(self, name, raw):
        self.check()
        need(re.fullmatch('[a-zA-Z0-9_.-]{1,128}', name) and type(raw) is bytes and len(raw) <= LIMIT,
             'artifact_bound')
        fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                     0o600, dir_fd=self.fd)
        try:
            view = memoryview(raw)
            while view:
                n = os.write(fd, view); need(n > 0, 'artifact_write'); view = view[n:]
            os.fchmod(fd, 0o600); os.fsync(fd); os.lseek(fd, 0, os.SEEK_SET)
            need(os.read(fd, len(raw) + 1) == raw, 'artifact_readback'); os.fsync(self.fd); self.check()
        finally: os.close(fd)
        return sha(raw)

    def close(self):
        for fd in self.directories.values(): os.close(fd)
        self.directories.clear()
        if self.fd is not None:
            os.close(self.fd); self.fd = None


def result_ok(result):
    flags = ('started', 'expired', 'output_limit_exceeded', 'owned_processes_closed',
             'unexpected_descendants', 'interrupted')
    need(all(type(getattr(result, k)) is bool for k in flags) and
         type(result.stdout) is bytes and type(result.stderr) is bytes, 'command_result_shape')
    need(result.started and result.owned_processes_closed and not result.expired and
         not result.output_limit_exceeded and not result.unexpected_descendants and not result.interrupted
         and type(result.actual_exit_code) is int and result.actual_exit_code == 0
         and len(result.stdout) + len(result.stderr) <= LIMIT, 'command_failed_or_unclosed')


def docker_args(p, private, args):
    root = root_path(p.value)
    endpoint = 'unix://' + str(root / 'docker.sock') if private else 'unix:///var/run/docker.sock'
    need(not os.listdir(root / 'client'), 'client_config_not_empty')
    return ['--config', str(root / 'client'), '--host', endpoint, *args]


def check_layout(evidence):
    evidence.check()
    rows = decode(private_bytes(evidence.root / 'layout.json'))
    value = {'operation_id': evidence.root.name.removeprefix('openlegal-hdd-docker-')}
    need(uid(value['operation_id']) and root_path(value) == evidence.root, 'layout_operation_root')
    short = runtime_socket_root(value)
    need(set(rows) == {'client', 'data', 'exec', *RUNTIME_DIRS, str(short)}, 'layout_shape')
    for name, expected in rows.items():
        path = evidence.root / name; meta = path.lstat()
        if name not in evidence.directories:
            evidence.directories[name] = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        held = os.fstat(evidence.directories[name])
        need(stat.S_ISDIR(meta.st_mode) and meta.st_uid == meta.st_gid == 0
             and not meta.st_mode & 0o022 and [meta.st_dev, meta.st_ino] == expected
             and (meta.st_dev, meta.st_ino) == (held.st_dev, held.st_ino),
             'layout_rebound')
    need(stat.S_IMODE((evidence.root / 'client').lstat().st_mode) == 0o700 and
         not os.listdir(evidence.root / 'client'), 'client_changed')
    need(stat.S_IMODE(short.lstat().st_mode) == 0o700 and
         decode(private_bytes(short / 'owner.json')) == {'operation_id': value['operation_id'],
             'root': str(evidence.root), 'root_identity': [evidence.identity.st_dev, evidence.identity.st_ino]},
         'runtime_socket_root_rebound')
    need(private_bytes(evidence.root / 'containerd.toml') == runtime_config(value), 'runtime_config_changed')


def owned_socket(pid, path):
    """Kernel socket IDs and filesystem node IDs are distinct namespaces."""
    node = path.lstat()
    need(stat.S_ISSOCK(node.st_mode) and node.st_uid == node.st_gid == 0 and node.st_nlink == 1,
         'runtime_socket_metadata')
    proc = Path('/proc') / str(pid)
    fds = list((proc / 'fd').iterdir()); need(len(fds) <= 4096, 'runtime_fd_bound')
    ids = set()
    for fd in fds:
        try: target = os.readlink(fd)
        except FileNotFoundError: continue
        match = re.fullmatch(r'socket:\[([0-9]+)\]', target)
        if match: ids.add(match[1])
    raw = (proc / 'net/unix').read_bytes(); need(len(raw) <= LIMIT, 'runtime_unix_bound')
    rows = [line.split() for line in raw.decode('ascii', 'strict').splitlines()[1:]]
    matches = [row for row in rows if len(row) == 8 and row[7] == str(path)]
    # Accepted stream sockets legitimately retain the listener pathname. Only
    # its unique LISTEN row establishes ownership; other bound/nonstream rows
    # must not disguise a replaced path or a second listener.
    listeners = [row for row in matches if row[3:6] == ['00010000', '0001', '01']]
    need(len(listeners) == 1 and listeners[0][6] in ids and
         all(row[3:6] in (['00010000', '0001', '01'], ['00000000', '0001', '03']) for row in matches),
         'runtime_socket_not_owned')
    after = path.lstat()
    need((node.st_dev, node.st_ino) == (after.st_dev, after.st_ino), 'runtime_socket_rebound')
    return {'dev': node.st_dev, 'ino': node.st_ino, 'kernel_socket_inode': listeners[0][6]}


def runtime_storage(pid, evidence):
    """Read back native metadata FD, not the output of a separate config parser."""
    path = evidence.root / 'cd/io.containerd.metadata.v1.bolt/meta.db'
    node = path.lstat()
    need(stat.S_ISREG(node.st_mode) and node.st_uid == 0 and node.st_nlink == 1, 'runtime_db_metadata')
    found = False
    fds = list((Path('/proc') / str(pid) / 'fd').iterdir()); need(len(fds) <= 4096, 'runtime_fd_bound')
    for fd in fds:
        try:
            if os.readlink(fd) == str(path):
                held = fd.stat(); found |= (held.st_dev, held.st_ino) == (node.st_dev, node.st_ino)
        except FileNotFoundError: continue
    need(found, 'runtime_db_not_owned')
    directories = {}
    for name in RUNTIME_DIRS:
        host = (evidence.root / name).lstat()
        native = (Path('/proc') / str(pid) / 'root' / str(evidence.root / name).lstrip('/')).stat()
        need((host.st_dev, host.st_ino) == (native.st_dev, native.st_ino), 'runtime_directory_namespace')
        directories[name] = [host.st_dev, host.st_ino]
    return {'path': str(path), 'dev': node.st_dev, 'ino': node.st_ino,
            'native_directories': directories}


def check_private_runtime(evidence):
    check_layout(evidence)
    row = decode(private_bytes(evidence.root / 'containerd-native-owner.json'))
    same_process(row['witness'])
    sockets = {name: owned_socket(row['witness']['pid'], evidence.root / name)
               for name in ('c.sock', 'c.ttrpc')}
    need(sockets == row['sockets'] and runtime_storage(row['witness']['pid'], evidence) == row['metadata_db'],
         'runtime_native_binding_changed')
    same_process(row['witness'])


def check_private_daemon(evidence):
    check_private_runtime(evidence)
    row = decode(private_bytes(evidence.root / 'daemon-native-owner.json'))
    same_process(row['witness'])
    node = (evidence.root / 'docker.sock').lstat()
    need(stat.S_ISSOCK(node.st_mode) and node.st_uid == 0 and node.st_nlink == 1 and
         {'dev': node.st_dev, 'ino': node.st_ino} == row['socket'], 'private_socket_rebound')


def image_deadline(deadline):
    need(type(deadline) in (int,float) and math.isfinite(deadline)
         and time.monotonic() < deadline, 'image_identity_deadline')


def rootfs_diff_ids(value):
    need(type(value) is list and 1 <= len(value) <= 128
         and all(type(v) is str and DIGEST.fullmatch(v) for v in value), 'image_rootfs_diff_ids')
    return value


def inspect_image(raw, p, *, private=False, expected_rootfs=None):
    value = decode(raw); image = p.hv['native_pg_image']
    expected = p.value['image_config_digest'] if private else p.value['official_image_id']
    need(type(value) is dict and set(value) == {'Id','Os','Architecture','Size','RootFS'}
         and [value[k] for k in ('Id','Os','Architecture')] ==
             [expected,image['os'],image['architecture']] and type(value['Size']) is int
         and 0 < value['Size'] <= p.value['image_archive_max_bytes'], 'image_identity_or_size')
    rootfs_diff_ids(value['RootFS'])
    if private:
        need(value['RootFS'] == rootfs_diff_ids(expected_rootfs), 'private_image_rootfs_changed')
    else:
        need(expected_rootfs is None, 'default_image_context')
    return value


IMAGE_FORMAT = '{"Id":{{json .Id}},"Os":{{json .Os}},"Architecture":{{json .Architecture}},"Size":{{json .Size}},"RootFS":{{json .RootFS.Layers}}}'


def action_commands(p, action):
    """Closed grammar. No inventory field is a command, shell or Docker option."""
    need(action in ACTIONS, 'worker_action')
    root = root_path(p.value); image = p.hv['native_pg_image']
    inspect = lambda ref: ['image', 'inspect', '--format', IMAGE_FORMAT, ref]
    if action == 'default-inspect':
        return [('docker', docker_args(p, False, inspect(ref)))
                for ref in (p.value['official_reference'], p.value['official_image_id'])]
    if action == 'save-image':
        return [('docker', docker_args(p, False, ['image', 'save', '--output', str(root / 'image.tar'), p.value['official_image_id']]))]
    if action == 'runtime-profile':
        return [('containerd', ['--version']),
                ('containerd', ['--config=' + str(root / 'containerd.toml'), 'config', 'dump'])]
    if action == 'containerd': return [('containerd', runtime_args(p.value))]
    if action == 'daemon': return [('dockerd', daemon_args(p.value))]
    if action == 'private-info':
        return [('docker', docker_args(p, True, ['info', '--format', '{{.DockerRootDir}}']))]
    if action == 'load-image':
        return [('docker', docker_args(p, True, ['image', 'load', '--input', str(root / 'image.tar')]))]
    if action == 'private-inspect':
        return [('docker', docker_args(p, True, inspect(p.value['image_config_digest'])))]
    if action == 'private-empty':
        return [('docker', docker_args(p, True, ['ps', '-aq', '--no-trunc'])),
                ('docker', docker_args(p, True, ['image', 'ls', '--all', '--no-trunc', '--quiet']))]
    hi = p.value['hdd_inventory']; ha = p.value['hdd_approval']
    return [('python', ['-I', '-B', str(SELF.parent / 'hdd_benchmark.py'), 'run',
                       '--inventory', hi['path'], '--inventory-sha256', hi['sha256'],
                       '--source-sha256', HDD_SOURCE, '--operation', p.value['operation_id'],
                       '--approval', ha['path'], '--approval-sha256', ha['sha256']])]


def _archive_member(archive, members, name, deadline, *, digest=None, size=None):
    image_deadline(deadline)
    need(name in members and members[name].isfile(), 'archive_member_missing')
    row = members[name]
    need(row.size <= MAX_IMAGE_METADATA and (size is None or row.size == size), 'archive_metadata_bound')
    with archive.extractfile(row) as stream:
        raw = stream.read(MAX_IMAGE_METADATA+1)
    image_deadline(deadline)
    need(len(raw) == row.size and (digest is None or sha(raw) == digest.split(':')[1]),
         'archive_metadata_hash')
    return decode(raw)


def _descriptor(value, media, *, digest=None, maximum=MAX_IMAGE_METADATA):
    need(type(value) is dict and value.get('mediaType') == media
         and type(value.get('digest')) is str and DIGEST.fullmatch(value['digest'])
         and type(value.get('size')) is int and 0 < value['size'] <= maximum
         and (digest is None or value['digest'] == digest), 'archive_descriptor')
    return 'blobs/sha256/'+value['digest'].split(':')[1]


def _tar_octal(raw, maximum):
    need(raw[-1:] in (b'\0',b' ') and re.fullmatch(rb'[0-7]{1,11}',raw.strip(b'\0 ')),
         'archive_physical_octal')
    value = int(raw.strip(b'\0 '),8)
    need(value <= maximum, 'archive_physical_size')
    return value


def _tar_physical_members(stream, total, maximum, deadline):
    """Precheck fixed512-byte USTAR headers BEFORE stdlib sees any extension.

    Only the supported Docker OCI short-name regular/directory profile is
    supported. PAX/GNU long-name/sparse/base256 extensions are refused rather
    than allowing tarfile to allocate hidden metadata before yielding a member.
    """
    need(total % 512 == 0, 'archive_physical_alignment')
    stream.seek(0); names = set(); offset = 0
    def read(count):
        image_deadline(deadline);raw = stream.read(count);image_deadline(deadline)
        need(len(raw) == count, 'archive_physical_truncated')
        return raw
    while offset < total:
        header = read(512);offset += 512
        if header == b'\0'*512:
            need(offset+512 <= total and read(512) == b'\0'*512, 'archive_physical_end')
            offset += 512
            while offset < total:
                count = min(1024*1024,total-offset)
                need(read(count) == b'\0'*count, 'archive_physical_trailer');offset += count
            stream.seek(0);return names
        # Reject x/g/L/K/S and every other extension before interpreting size
        # or reading its payload; tarfile is not called until this pass ends.
        kind = header[156:157]
        need(kind in (b'0',b'\0',b'5'), 'archive_physical_type')
        need(len(names) < MAX_ARCHIVE_MEMBERS, 'archive_physical_bound')
        need(header[257:263] == b'ustar\0' and header[263:265] == b'00' and header[345:500] == b'\0'*155
             and header[157:257] == b'\0'*100, 'archive_physical_ustar')
        checksum = _tar_octal(header[148:156],8*255*512)
        need(checksum == sum(header[:148])+8*32+sum(header[156:]), 'archive_physical_checksum')
        field = header[:100];name_raw,separator,padding = field.partition(b'\0')
        need(separator and not padding.strip(b'\0'), 'archive_physical_name')
        try:name = name_raw.decode('ascii')
        except UnicodeError:raise Refused('archive_physical_name') from None
        if kind == b'5':
            need(name in ('blobs','blobs/','blobs/sha256','blobs/sha256/'), 'archive_physical_directory')
            name = name.removesuffix('/')
        else:
            need(name in ('oci-layout','index.json','manifest.json')
                 or re.fullmatch(r'blobs/sha256/[0-9a-f]{64}',name), 'archive_physical_name')
        need(name not in names, 'archive_physical_duplicate');names.add(name)
        size = _tar_octal(header[124:136],maximum)
        need(kind != b'5' or size == 0, 'archive_physical_directory_size')
        padding = (-size)%512
        need(offset+size+padding <= total, 'archive_physical_truncated')
        stream.seek(size,1);offset += size
        need(read(padding) == b'\0'*padding, 'archive_physical_padding');offset += padding
    raise Refused('archive_physical_end')


def _archive_identity(p, stream, deadline):
    """One exact OCI native branch. No extraction, Env output or decompressor."""
    total = os.fstat(stream.fileno()).st_size
    physical = _tar_physical_members(stream,total,p.value['image_archive_max_bytes'],deadline)
    with tarfile.open(fileobj=stream, mode='r:') as archive:
        members = {}
        for row in archive:
            image_deadline(deadline)
            need(len(members) < MAX_ARCHIVE_MEMBERS and row.name not in members
                 and not row.linkname and not Path(row.name).is_absolute()
                 and '..' not in Path(row.name).parts, 'archive_entry')
            if row.isdir():
                need(row.name in ('blobs','blobs/sha256'), 'archive_directory')
            else:
                need(row.isfile() and 0 <= row.size <= p.value['image_archive_max_bytes']
                     and (row.name in ('oci-layout','index.json','manifest.json')
                          or re.fullmatch(r'blobs/sha256/[0-9a-f]{64}',row.name)), 'archive_regular_member')
            members[row.name] = row
        need(set(members) == physical, 'archive_physical_parser_disagreement')
        layout = _archive_member(archive,members,'oci-layout',deadline)
        need(layout == {'imageLayoutVersion':'1.0.0'}, 'archive_oci_layout')
        index = _archive_member(archive,members,'index.json',deadline)
        need(type(index) is dict and type(index.get('schemaVersion')) is int and index['schemaVersion'] == 2
             and index.get('mediaType') == INDEX_MEDIA and type(index.get('manifests')) is list
             and len(index['manifests']) == 1, 'archive_root_index')
        root = index['manifests'][0]
        name = _descriptor(root,INDEX_MEDIA,digest=p.value['official_image_id'])
        official = _archive_member(archive,members,name,deadline,digest=root['digest'],size=root['size'])
        need(type(official) is dict and type(official.get('schemaVersion')) is int and official['schemaVersion'] == 2
             and official.get('mediaType') == INDEX_MEDIA and type(official.get('manifests')) is list
             and 1 <= len(official['manifests']) <= 64, 'archive_official_index')
        platform = {'os':p.hv['native_pg_image']['os'],'architecture':p.hv['native_pg_image']['architecture']}
        selected = [r for r in official['manifests'] if type(r) is dict and r.get('platform') == platform]
        need(len(selected) == 1, 'archive_native_manifest_ambiguous')
        native = selected[0]
        name = _descriptor(native,MANIFEST_MEDIA,digest=p.value['platform_manifest_digest'])
        manifest = _archive_member(archive,members,name,deadline,digest=native['digest'],size=native['size'])
        need(type(manifest) is dict and type(manifest.get('schemaVersion')) is int and manifest['schemaVersion'] == 2
             and manifest.get('mediaType') == MANIFEST_MEDIA and 'artifactType' not in manifest,
             'archive_native_manifest')
        config_descriptor = manifest.get('config')
        config_name = _descriptor(config_descriptor,CONFIG_MEDIA,digest=p.value['image_config_digest'])
        config = _archive_member(archive,members,config_name,deadline,
                                 digest=config_descriptor['digest'],size=config_descriptor['size'])
        need(type(config) is dict and config.get('os') == platform['os']
             and config.get('architecture') == platform['architecture'] and type(config.get('rootfs')) is dict
             and set(config['rootfs']) == {'type','diff_ids'} and config['rootfs']['type'] == 'layers',
             'archive_native_config')
        diff_ids = rootfs_diff_ids(config['rootfs']['diff_ids'])
        layers = manifest.get('layers')
        need(type(layers) is list and len(layers) == len(diff_ids), 'archive_native_layers')
        paths = []
        for layer in layers:
            name = _descriptor(layer,LAYER_MEDIA,maximum=p.value['image_archive_max_bytes'])
            need(name in members and members[name].isfile() and members[name].size == layer['size'],
                 'archive_native_layer_missing')
            hashed = hashlib.sha256(); total = 0
            with archive.extractfile(members[name]) as raw:
                while True:
                    image_deadline(deadline); block = raw.read(1024*1024); image_deadline(deadline)
                    if not block:break
                    total += len(block); need(total <= layer['size'], 'archive_layer_grew'); hashed.update(block)
            need(total == layer['size'] and hashed.hexdigest() == layer['digest'].split(':')[1],
                 'archive_native_layer_hash')
            paths.append(name)
        legacy = _archive_member(archive,members,'manifest.json',deadline)
        need(type(legacy) is list and len(legacy) == 1 and type(legacy[0]) is dict
             and legacy[0].get('Config') == config_name and legacy[0].get('Layers') == paths
             and legacy[0].get('RepoTags') in (None,[]), 'archive_legacy_native_binding')
        return {'official_image_id':p.value['official_image_id'],
                'platform_manifest_digest':p.value['platform_manifest_digest'],
                'image_config_digest':p.value['image_config_digest'],'rootfs_diff_ids':diff_ids,
                'selected_layer_digests':[r['digest'] for r in layers]}


def validate_archive_image(p, path, *, deadline):
    """Read-only audit/producer validator; the failed operation is never adopted."""
    image_deadline(deadline)
    need(type(p.value.get('official_reference')) is str
         and re.fullmatch(r'(?:postgres|docker.io/library/postgres)@sha256:[0-9a-f]{64}',p.value['official_reference'])
         and p.value['official_reference'].split('@',1)[1] == p.value['official_image_id']
         and p.hv['native_pg_image']['image_id'] == p.hv['native_pg_image']['reference'] == p.value['image_config_digest'],
         'archive_typed_image_binding')
    need(Path(path) == root_path(p.value)/'image.tar', 'archive_fixed_path')
    fd = os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC|os.O_NOATIME)
    with os.fdopen(fd,'rb') as stream:
        before = os.fstat(stream.fileno())
        need(stat.S_ISREG(before.st_mode) and before.st_uid == 0 and before.st_nlink == 1
             and stat.S_IMODE(before.st_mode) == 0o600
             and 0 < before.st_size <= p.value['image_archive_max_bytes'], 'archive_metadata')
        hashed = hashlib.sha256()
        while True:
            image_deadline(deadline);block = stream.read(1024*1024);image_deadline(deadline)
            if not block:break
            hashed.update(block)
        stream.seek(0)
        try:identity = _archive_identity(p,stream,deadline)
        except tarfile.TarError:raise Refused('archive_format') from None
        after = os.fstat(stream.fileno()); current = Path(path).lstat()
        need(all(getattr(before,k) == getattr(after,k) == getattr(current,k)
             for k in ('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns')), 'archive_changed')
        image_deadline(deadline)
        return {'sha256':hashed.hexdigest(),'bytes':before.st_size,'dev':before.st_dev,'ino':before.st_ino,
                'official_reference':p.value['official_reference'],**identity,'source':'default_read_only_image_save'}


def archive(p, evidence, *, deadline, save=False):
    record = validate_archive_image(p,root_path(p.value)/'image.tar',deadline=deadline)
    default = decode(private_bytes(evidence.root/'default-image-binding.json'))
    need(type(default) is dict and set(default) == {'official_reference','official_image_id','inspections'}
         and default['official_reference'] == record['official_reference']
         and default['official_image_id'] == record['official_image_id']
         and type(default['inspections']) is list and len(default['inspections']) == 2,
         'archive_default_image_binding')
    for observed in default['inspections']:
        inspect_image(canonical(observed),p)
        need(observed['RootFS'] == record['rootfs_diff_ids'], 'archive_default_rootfs_changed')
    if save:evidence.save('image-archive.json',record)
    else:need(decode(private_bytes(evidence.root/'image-archive.json')) == record,'archive_provenance_changed')
    image_deadline(deadline)
    return record


def execute_action(p, evidence, action, deadline):
    seal(p, deadline); evidence.check()
    check_layout(evidence)
    check_runtime_binaries(p.value)
    if action == 'daemon': check_private_runtime(evidence)
    if action not in ('default-inspect', 'save-image', 'runtime-profile', 'containerd', 'daemon'):
        check_private_daemon(evidence)
    need(private_bytes(evidence.root / 'daemon.json') == b'{}\n' and
         not os.listdir(evidence.root / 'client'), 'empty_configuration_changed')
    if action == 'save-image':
        need(not os.path.lexists(evidence.root / 'image.tar'), 'archive_already_exists')
    if action == 'load-image': archive(p, evidence,deadline=deadline)
    commands = action_commands(p, action)
    outputs = []
    for number, (binary, args) in enumerate(commands):
        seal(p, deadline)
        check_layout(evidence)
        if action == 'daemon': check_private_runtime(evidence)
        if action not in ('default-inspect', 'save-image', 'runtime-profile', 'containerd', 'daemon'):
            check_private_daemon(evidence)
        fd = executable(p.value['binaries'][binary])
        try:
            label = 'worker-' + action + '-' + str(number)
            evidence.save(label + '-intent.json', {'binary': binary, 'argv': args,
                'original_absolute_deadline': deadline, 'inventory_sha256': p.args.inventory_sha256,
                'source_sha256': p.args.source_sha256, 'automatic_retry': False})
            if action == 'containerd':
                result = p.process.execute_retained_runtime(
                    [p.value['binaries'][binary]['path'], *args], executable_fd=fd,
                    stdin=None, env=payload_env(p.value), dispatch_deadline=deadline,
                    max_output=LIMIT, **p.runtime_control)
            else:
                result = p.process.execute([p.value['binaries'][binary]['path'], *args], executable_fd=fd,
                    stdin=None, env=payload_env(p.value), deadline=deadline, max_output=LIMIT)
        finally: os.close(fd)
        record = {k: getattr(result, k) for k in ('actual_exit_code', 'started', 'expired',
            'output_limit_exceeded', 'owned_processes_closed', 'unexpected_descendants', 'interrupted', 'elapsed_seconds')}
        record.update(stdout_sha256=sha(result.stdout), stderr_sha256=sha(result.stderr),
                      stdout_bytes=len(result.stdout), stderr_bytes=len(result.stderr))
        evidence.save(label + '-exit.json', record)
        if action in ('daemon', 'containerd'):
            evidence.save_bytes(action + '.log', (result.stdout + result.stderr)[:LIMIT])
        if action == 'runtime-profile':
            evidence.save_bytes('runtime-profile-' + str(number) + '.stdout', result.stdout)
            evidence.save_bytes('runtime-profile-' + str(number) + '.stderr', result.stderr)
        result_ok(result); outputs.append(result.stdout)
        if action == 'runtime-profile': need(not result.stderr.strip(), 'runtime_profile_warning')
    if action == 'runtime-profile':
        need(outputs[0] == b'containerd containerd v2.4.2 b7a25261e029d68978bbf290df0a62fc819b3c51\n',
             'runtime_version')
        actual = tomllib.loads(outputs[1].decode('utf8', 'strict'))
        expected = tomllib.loads(runtime_config(p.value).decode())
        need(all(actual.get(k) == expected[k] for k in ('version', 'root', 'state', 'temp', 'disabled_plugins')),
             'runtime_effective_paths')
        # v2.4.2 dump seeds compiled defaults and cannot clear its displayed
        # Imports by slice merge. Loader resolves only each FILE's imports;
        # the exact held file here has imports=[] (checked above).
        need(actual.get('imports') in ([], ['/etc/containerd/conf.d/*.toml']), 'runtime_dump_imports')
        for name, settings in expected['plugins'].items():
            need(all(actual.get('plugins', {}).get(name, {}).get(k) == v for k, v in settings.items()),
                 'runtime_effective_plugin')
        need(actual.get('cgroup', {}).get('path') == '', 'runtime_effective_cgroup')
    if action == 'default-inspect':
        observed = [inspect_image(raw,p) for raw in outputs]
        need(len(observed) == 2 and observed[0] == observed[1], 'default_image_observations_changed')
        evidence.save('default-image-binding.json',{'official_reference':p.value['official_reference'],
            'official_image_id':p.value['official_image_id'],'inspections':observed})
    if action == 'private-inspect':
        proof = archive(p,evidence,deadline=deadline)
        for raw in outputs:inspect_image(raw,p,private=True,expected_rootfs=proof['rootfs_diff_ids'])
    if action == 'save-image': archive(p, evidence,deadline=deadline,save=True)
    if action == 'private-info':
        need(outputs[0].decode('utf8', 'strict').strip() == str(evidence.root / 'data'), 'private_actual_root')
    if action == 'private-empty':
        need(outputs[0].strip() == b'' and set(outputs[1].decode('ascii', 'strict').split()) ==
             {p.hv['native_pg_image']['image_id']}, 'private_resources_not_exact')
    if action == 'load-image': archive(p, evidence,deadline=deadline)
    seal(p, deadline); check_runtime_binaries(p.value)
    evidence.save('worker-' + action + '-complete.json', {'action': action,
        'actual_command_exits_zero': True, 'owned_processes_closed': True,
        'output_hashes': [sha(raw) for raw in outputs]})


def worker(p, args):
    need(args.worker_action in ACTIONS and args.gate_fd is not None and args.deadline is not None,
         'worker_arguments')
    evidence = Evidence(root_path(p.value))
    try:
        binding = decode(private_bytes(evidence.root / 'operation.json'))
        need(binding['controller']['pid'] == os.getppid() and
             binding['inventory_sha256'] == args.inventory_sha256 and binding['source_sha256'] == args.source_sha256
             and binding['authority_sha256'] == args.authority_sha256, 'worker_parent_binding')
        same_process(binding['controller'])
        controls = (args.stop_fd, args.owner_fd, args.operation_lock_fd)
        need(all(type(fd) is int and fd >= 0 for fd in controls) if args.worker_action == 'containerd'
             else all(fd is None for fd in controls), 'worker_runtime_controls')
        if args.worker_action == 'containerd':
            held = os.fstat(args.operation_lock_fd); named = (evidence.root / 'operation.lock').lstat()
            need((held.st_dev, held.st_ino) == (named.st_dev, named.st_ino), 'worker_inherited_lock')
            p.runtime_control = {'shutdown_permission_fd': args.stop_fd,
                'owner_pidfd': args.owner_fd, 'operation_lock_fd': args.operation_lock_fd,
                'permission_token': (p.value['operation_id'] + ':containerd-stop\n').encode()}
        lockfd = os.open(evidence.root / 'operation.lock', os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            try:
                fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                fcntl.flock(lockfd, fcntl.LOCK_UN)
                raise Refused('controller_lock_not_held')
        finally: os.close(lockfd)
        evidence.save('worker-' + args.worker_action + '-ready.json', {
            'witness': witness(os.getpid()), 'action': args.worker_action, 'payload_inhibited': True})
        expected = (p.value['operation_id'] + ':' + args.worker_action + '\n').encode()
        need(os.read(args.gate_fd, 128) == expected and os.read(args.gate_fd, 1) == b'', 'worker_go_gate')
        os.close(args.gate_fd)
        resource.setrlimit(resource.RLIMIT_FSIZE, (p.value['image_archive_max_bytes'], p.value['image_archive_max_bytes']))
        hold_runtime_binaries(p)
        execute_action(p, evidence, args.worker_action, args.deadline)
    except BaseException:
        try: evidence.save('worker-' + args.worker_action + '-failed.json', {
            'status': 'FAILED_OR_UNKNOWN', 'automatic_retry': False, 'automatic_cleanup': False})
        except BaseException: pass
        raise
    finally:
        for name in ('stop_fd', 'owner_fd', 'operation_lock_fd'):
            fd = getattr(args, name, None)
            if fd is not None: os.close(fd)
        close_runtime_binaries(p); evidence.close()


class Controller:
    def __init__(self, p):
        self.p = p; self.deadline = time.monotonic() + p.value['total_deadline_secs']
        self.workers = {}; self.daemon = None; self.runtime = None
        self.evidence = None; self.socket_evidence = None; self.lock = None
        self.runtime_permission = None
        self.runtime_stop_sent = False
        self.owner_pid = os.getpid(); self.sigchld_owned = False; self.previous_sigchld = None

    def _child_context(self, *, initial=False):
        need(os.getpid() == self.owner_pid and threading.current_thread() is threading.main_thread()
             and len(list(Path('/proc/self/task').iterdir())) == 1, 'controller_dedicated_main_thread')
        fields = Path('/proc/self/task/' + str(os.getpid()) + '/children').read_text().split()
        need(len(fields) <= 4096 and all(x.isdecimal() for x in fields), 'controller_children_shape')
        children = set(map(int, fields))
        if initial:
            need(not children, 'controller_preexisting_children')
        else:
            need(self.sigchld_owned and signal.getsignal(signal.SIGCHLD) == signal.SIG_DFL,
                 'controller_sigchld_not_owned')
            need(children <= {row['process'].pid for row in self.workers.values()},
                 'controller_unowned_children')
        return children

    def _acquire_sigchld(self):
        if self.sigchld_owned:
            self._child_context(); return
        self._child_context(initial=True)
        previous = signal.getsignal(signal.SIGCHLD)
        need(previous in (signal.SIG_DFL, signal.SIG_IGN) or callable(previous), 'controller_unknown_sigchld')
        # SIG_IGN and a caller waitpid handler can auto-reap and make Popen
        # substitute zero for a lost status. Own this disposition until reaped.
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        self.previous_sigchld = previous; self.sigchld_owned = True
        self._child_context()

    def _restore_sigchld(self):
        if not self.sigchld_owned:return
        children = self._child_context()
        need(not children and all(row['process'].returncode is not None
             and (row['pidfd'] is None or not alive(row['pidfd'])) for row in self.workers.values())
             and (self.daemon is None or not alive(self.daemon['pidfd'])), 'controller_workers_not_reaped')
        need(self.runtime is None or not alive(self.runtime['pidfd']), 'controller_runtime_not_reaped')
        signal.signal(signal.SIGCHLD, self.previous_sigchld)
        self.sigchld_owned = False

    def setup(self):
        self._acquire_sigchld()
        p = self.p; seal(p, self.deadline)
        need(not p.process._children(os.getpid()), 'controller_preexisting_children')
        hold_runtime_binaries(p)
        root = root_path(p.value); os.mkdir(root, 0o700); os.chmod(root, 0o700)
        self.evidence = Evidence(root)
        self.lock = os.open(root / 'operation.lock', os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.evidence.save('operation.json', {'operation_id': p.value['operation_id'],
            'controller': witness(os.getpid()), 'inventory_sha256': p.args.inventory_sha256,
            'source_sha256': p.args.source_sha256, 'authority_sha256': p.args.authority_sha256,
            'original_absolute_deadline': self.deadline, 'plan': plan(p.value)})
        for name in ('client', 'data', 'exec', *RUNTIME_DIRS): os.mkdir(root / name, 0o700)
        short = runtime_socket_root(p.value); os.mkdir(short, 0o700); os.chmod(short, 0o700)
        self.socket_evidence = Evidence(short)
        self.socket_evidence.save('owner.json', {'operation_id': p.value['operation_id'], 'root': str(root),
            'root_identity': [self.evidence.identity.st_dev, self.evidence.identity.st_ino]})
        self.evidence.save('layout.json', {name: [(s := (root / name).lstat()).st_dev, s.st_ino]
            for name in ('client', 'data', 'exec', *RUNTIME_DIRS, str(short))})
        self.evidence.save_bytes('daemon.json', b'{}\n')
        self.evidence.save_bytes('containerd.toml', runtime_config(p.value))
        check_layout(self.evidence)

    def spawn(self, action):
        self._child_context()
        need(action in ACTIONS and action not in self.workers, 'one_worker_per_action')
        seal(self.p, self.deadline); self.evidence.check()
        check_runtime_binaries(self.p.value)
        if action == 'daemon': check_private_runtime(self.evidence)
        pyfd = executable(self.p.value['binaries']['python'])
        sourcefd = os.open(SELF, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        readfd, writefd = os.pipe2(os.O_CLOEXEC)
        files = []; inherited = []
        row = None
        try:
            need(sha(os.read(sourcefd, LIMIT + 1)) == self.p.args.source_sha256, 'held_worker_source')
            os.lseek(sourcefd, 0, os.SEEK_SET)
            for suffix in ('stdout', 'stderr'):
                files.append(os.open(self.evidence.root / ('worker-' + action + '.' + suffix),
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600))
            a = self.p.args
            argv = [self.p.value['binaries']['python']['path'], '-I', '-B', '/proc/self/fd/' + str(sourcefd),
                'worker', '--worker-action', action, '--gate-fd', str(readfd), '--deadline', str(self.deadline),
                '--inventory', str(a.inventory), '--inventory-sha256', a.inventory_sha256,
                '--authority', str(a.authority), '--authority-sha256', a.authority_sha256,
                '--source-sha256', a.source_sha256]
            if action == 'containerd':
                need(self.runtime_permission is None and self.lock is not None, 'runtime_permission_once')
                stopread, stopwrite = os.pipe2(os.O_CLOEXEC)
                self.runtime_permission = stopwrite
                inherited = [stopread, os.pidfd_open(os.getpid()), os.dup(self.lock)]
                for flag, fd in zip(('stop-fd', 'owner-fd', 'operation-lock-fd'), inherited):
                    argv += ['--' + flag, str(fd)]
            self._child_context()
            process = subprocess.Popen(argv, executable='/proc/self/fd/' + str(pyfd),
                pass_fds=(pyfd, sourcefd, readfd, *inherited), stdin=subprocess.DEVNULL,
                stdout=files[0], stderr=files[1], env=ENV, start_new_session=True)
            # Retain the Popen immediately, including failures during witness setup.
            row = {'process': process, 'pidfd': None, 'witness': None}
            self.workers[action] = row
            row['pidfd'] = os.pidfd_open(process.pid)
            # Popen's exec acknowledgement can precede a stable /proc cmdline
            # observation. Wait for authenticated Python bootstrap to reach its
            # inhibited gate, then independently check its actual kernel state.
            ready_path = self.evidence.root / ('worker-' + action + '-ready.json')
            ready_end = min(self.deadline, time.monotonic() + 15)
            while not ready_path.exists():
                self._child_context()
                seal(self.p, ready_end)
                need(time.monotonic() < ready_end and process.poll() is None, 'worker_not_ready')
                time.sleep(.01)
            ready = decode(private_bytes(ready_path))
            row['witness'] = witness(process.pid)
            need(ready == {'witness': row['witness'], 'action': action, 'payload_inhibited': True},
                 'worker_ready_binding')
            argv_sha = sha(b'\0'.join(x.encode() for x in argv) + b'\0')
            need(row['witness']['ppid'] == os.getpid() and
                 (row['witness']['exe_dev'], row['witness']['exe_ino']) ==
                 (os.fstat(pyfd).st_dev, os.fstat(pyfd).st_ino) and
                 row['witness']['cmdline_sha256'] == argv_sha, 'worker_native_owner')
            self.evidence.save('worker-' + action + '-owner.json', {'witness': row['witness'],
                'argv_sha256': argv_sha, 'payload_inhibited': True})
            same_process(row['witness'])
            self._child_context()
            token = (self.p.value['operation_id'] + ':' + action + '\n').encode()
            need(os.write(writefd, token) == len(token), 'worker_gate_write')
            return row
        finally:
            for fd in (pyfd, sourcefd, readfd, writefd, *files, *inherited): os.close(fd)

    def wait(self, action):
        self._child_context()
        row = self.workers[action]; process = row['process']
        while process.poll() is None:
            self._child_context()
            if action == 'benchmark': check_private_runtime(self.evidence)
            seal(self.p, self.deadline); self.evidence.check(); time.sleep(.05)
        self._child_context()
        rc = process.wait()
        self._child_context()
        self.evidence.save('worker-' + action + '-os-exit.json', {'actual_exit_code': rc,
            'witness': row['witness'], 'pidfd_exited': not alive(row['pidfd'])})
        need(type(rc) is int and rc == 0 and not alive(row['pidfd']), 'worker_actual_exit')
        result = decode(private_bytes(self.evidence.root / ('worker-' + action + '-complete.json')))
        need(result['action'] == action and result['actual_command_exits_zero'] is True
             and result['owned_processes_closed'] is True, 'worker_closure_receipt')
        return result

    def action(self, action):
        self.spawn(action); return self.wait(action)

    def bind_runtime(self):
        self._child_context()
        row = self.workers['containerd']; end = min(self.deadline, time.monotonic() + 60)
        observed = None
        expected = [self.p.value['binaries']['containerd']['path'], *runtime_args(self.p.value)]
        argv_sha = sha(b'\0'.join(x.encode() for x in expected) + b'\0')
        while observed is None:
            self._child_context(); seal(self.p, end)
            need(row['process'].poll() is None, 'runtime_worker_early_exit')
            children = self.p.process._children(row['process'].pid)
            need(len(children) <= 1, 'runtime_worker_children')
            if children:
                candidate = witness(next(iter(children)))
                # Exec acknowledgement may precede stable /proc argv.
                if candidate['cmdline_sha256'] == argv_sha: observed = candidate
            if observed is None: time.sleep(.02)
        fd = os.pidfd_open(observed['pid'])
        try:
            same_process(observed, parent=row['process'].pid)
            binary = executable(self.p.value['binaries']['containerd'])
            try:
                need((observed['exe_dev'], observed['exe_ino']) == (os.fstat(binary).st_dev, os.fstat(binary).st_ino),
                     'runtime_executable_changed')
            finally: os.close(binary)
        except BaseException:
            os.close(fd); raise
        self.runtime = {'witness': observed, 'pidfd': fd}
        while not all((self.evidence.root / name).exists() for name in
            ('c.sock', 'c.ttrpc', 'cd/io.containerd.metadata.v1.bolt/meta.db')):
            seal(self.p, end); same_process(observed); time.sleep(.02)
        sockets = {name: owned_socket(observed['pid'], self.evidence.root / name)
                   for name in ('c.sock', 'c.ttrpc')}
        self.evidence.save('containerd-native-owner.json', {'witness': observed, 'sockets': sockets,
            'metadata_db': runtime_storage(observed['pid'], self.evidence), 'fixed_argv': expected,
            'config_sha256': sha(runtime_config(self.p.value))})
        check_private_runtime(self.evidence)

    def bind_daemon(self):
        self._child_context()
        row = self.workers['daemon']; end = min(self.deadline, time.monotonic() + 60)
        path = self.evidence.root / 'docker.pid'
        while not path.exists():
            self._child_context()
            seal(self.p, end); need(row['process'].poll() is None, 'daemon_worker_early_exit'); time.sleep(.05)
        text = private_bytes(path, modes=(0o600, 0o644), limit=64).decode('ascii', 'strict').strip()
        need(re.fullmatch('[1-9][0-9]{0,9}', text), 'daemon_pid_shape')
        pid = int(text); observed = witness(pid); fd = os.pidfd_open(pid)
        try:
            same_process(observed, parent=row['process'].pid)
            dockfd = executable(self.p.value['binaries']['dockerd'])
            try:
                need((observed['exe_dev'], observed['exe_ino']) == (os.fstat(dockfd).st_dev, os.fstat(dockfd).st_ino),
                     'daemon_executable_changed')
            finally: os.close(dockfd)
            expected = [self.p.value['binaries']['dockerd']['path'], *daemon_args(self.p.value)]
            need(observed['cmdline_sha256'] == sha(b'\0'.join(x.encode() for x in expected) + b'\0'), 'daemon_argv_changed')
        except BaseException:
            os.close(fd); raise
        # A pidfile alone never authorizes containment of its claimed PID.
        self.daemon = {'witness': observed, 'pidfd': fd}
        socket = self.evidence.root / 'docker.sock'
        while not socket.exists():
            seal(self.p, end); same_process(observed); time.sleep(.05)
        node = socket.lstat()
        need(stat.S_ISSOCK(node.st_mode) and node.st_uid == 0 and node.st_nlink == 1, 'daemon_private_socket')
        self.evidence.save('daemon-native-owner.json', {'witness': observed,
            'socket': {'dev': node.st_dev, 'ino': node.st_ino}, 'fixed_argv': expected})

    def permit_runtime_stop(self):
        """Physical Docker AND worker descendant closure precede one token."""
        self._child_context()
        need(self.daemon is not None or 'daemon' not in self.workers, 'runtime_stop_daemon_identity_unknown')
        if self.daemon is not None: need(not alive(self.daemon['pidfd']), 'runtime_stop_daemon_alive')
        if 'daemon' in self.workers:
            row = self.workers['daemon']
            need(type(row['process'].returncode) is int and row['pidfd'] is not None
                 and not alive(row['pidfd']), 'runtime_stop_daemon_worker_alive')
            raw = decode(private_bytes(self.evidence.root / 'worker-daemon-0-exit.json'))
            need(raw.get('started') is True and raw.get('owned_processes_closed') is True
                 and type(raw.get('actual_exit_code')) is int, 'runtime_stop_descendant_closure_unknown')
        need(self.runtime_permission is not None and not self.runtime_stop_sent, 'runtime_stop_permission_missing')
        self.evidence.save('containerd-stop-permission-intent.json', {
            'operation_id': self.p.value['operation_id'], 'native_daemon_exited': self.daemon is not None,
            'daemon_worker_and_owned_descendants_closed': 'daemon' in self.workers,
            'token': 'one-use-private-pipe', 'automatic_retry': False})
        token = (self.p.value['operation_id'] + ':containerd-stop\n').encode()
        need(os.write(self.runtime_permission, token) == len(token), 'runtime_stop_permission_write')
        self.runtime_stop_sent = True
        os.close(self.runtime_permission); self.runtime_permission = None

    def contain(self):
        """Only retained pidfd identities; never rm, dataset cleanup or global kill."""
        self._child_context()
        results = []
        if self.daemon is not None:
            try: signal_owned(self.daemon, signal.SIGTERM); results.append('known_private_daemon_term')
            except BaseException: results.append('private_daemon_identity_unknown_no_signal')
        for action, row in self.workers.items():
            if action == 'containerd': continue
            if row['process'].poll() is None:
                try:
                    need(row['pidfd'] is not None and row['witness'] is not None, 'worker_identity_not_sealed')
                    signal_owned(row, signal.SIGTERM); results.append(action + '_worker_term')
                except BaseException: results.append(action + '_worker_identity_unknown_no_signal')
        # Physical owned-worker closure may outlive the logical deadline. Their
        # command_process helper owns TERM/KILL/reaping of only its descendants.
        for action, row in self.workers.items():
            if action == 'containerd': continue
            self._child_context(); row['process'].wait(); self._child_context()
        # A crashed/OOM-killed worker is not evidence that its native daemon
        # exited. Keep the runtime available until the retained native pidfd
        # proves exit. Unknown identity/closure never authorizes the next stop.
        try:
            need(self.daemon is not None or 'daemon' not in self.workers,
                 'private_daemon_identity_unknown_runtime_retained')
            if self.daemon is not None:
                while alive(self.daemon['pidfd']):
                    self._child_context()
                    need(time.monotonic() < self.deadline,
                         'private_daemon_closure_unknown_runtime_retained')
                    time.sleep(.05)
        except BaseException:
            if self.evidence is not None:
                try:self.evidence.save('containment-runtime-retained.json', {
                    'status': 'FAILED_OR_UNKNOWN', 'daemon_physical_closure': 'UNKNOWN',
                    'runtime_shutdown_dispatched': False, 'ownership_retained': True,
                    'automatic_retry': False, 'files_and_datasets_retained': True})
                except BaseException:pass
            raise
        runtime_worker = self.workers.get('containerd')
        if runtime_worker is not None:
            if runtime_worker['process'].poll() is None:
                if not self.runtime_stop_sent:
                    self.permit_runtime_stop(); results.append('containerd_stop_permission_sent')
            runtime_worker['process'].wait(); self._child_context()
        return {'attempts': results, 'status': 'FAILED_OR_UNKNOWN', 'automatic_retry': False,
                'files_and_datasets_retained': True, 'physical_daemon_closure':
                self.daemon is not None and not alive(self.daemon['pidfd']), 'physical_runtime_closure':
                self.runtime is not None and not alive(self.runtime['pidfd'])}

    def run(self):
        result = None
        previous = {}
        def interrupted(number, frame):
            del number, frame
            self.p.interrupted = True
        self._acquire_sigchld()
        try:
            for number in (signal.SIGTERM, signal.SIGINT):
                previous[number] = signal.getsignal(number)
                signal.signal(number, interrupted)
            self.setup()
            self.action('default-inspect'); self.action('save-image')
            self.action('runtime-profile'); self.spawn('containerd'); self.bind_runtime()
            self.spawn('daemon'); self.bind_daemon()
            self.action('private-info'); self.action('load-image'); self.action('private-inspect')
            self.action('benchmark'); self.action('private-empty')
            same_process(self.daemon['witness'])
            self.evidence.save('daemon-shutdown-intent.json', {'witness': self.daemon['witness'], 'signal': 'SIGTERM'})
            signal_owned(self.daemon, signal.SIGTERM); self.wait('daemon')
            need(not alive(self.daemon['pidfd']), 'daemon_still_alive')
            same_process(self.runtime['witness'])
            self.evidence.save('containerd-shutdown-intent.json', {'witness': self.runtime['witness'], 'signal': 'SIGTERM'})
            self.permit_runtime_stop(); self.wait('containerd')
            need(not alive(self.daemon['pidfd']) and not self.p.process._children(os.getpid()), 'owned_processes_remain')
            need(not alive(self.runtime['pidfd']), 'runtime_still_alive')
            result = {'kind': 'openlegal_hdd_private_daemon_result', 'status': 'COMPLETE_PENDING_OUTER_OS_EXIT',
                'operation_id': self.p.value['operation_id'], 'benchmark_actual_os_exit_zero': True,
                'private_containers_absent': True, 'daemon_actual_exit_zero_and_descendants_closed': True,
                'private_runtime_actual_exit_zero_and_descendants_closed': True,
                'all_worker_actual_os_exits_zero': True, 'files_retained': True,
                'production_tuning_permitted': False, 'outer_actual_os_exit': 'NOT_YET_OBSERVABLE'}
            seal(self.p, self.deadline); self.evidence.save('completed.json', result)
            return result
        except BaseException:
            containment = self.contain()
            if self.evidence is not None:
                try: self.evidence.save('failed.json', containment)
                except BaseException: pass
            raise
        finally:
            # A closure failure must retain signal/FD/lock ownership. Never
            # restore an auto-reaper while any owned worker can still exit.
            self._restore_sigchld()
            for row in self.workers.values():
                if row['pidfd'] is not None: os.close(row['pidfd'])
            if self.daemon is not None: os.close(self.daemon['pidfd'])
            if self.runtime is not None: os.close(self.runtime['pidfd'])
            if self.runtime_permission is not None: os.close(self.runtime_permission)
            if self.lock is not None: os.close(self.lock)
            if self.evidence is not None: self.evidence.close()
            if self.socket_evidence is not None: self.socket_evidence.close()
            close_runtime_binaries(self.p)
            for number, handler in previous.items(): signal.signal(number, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('qualify', 'run', 'worker'))
    for key in ('inventory', 'authority'):
        parser.add_argument('--' + key, type=Path, required=True)
        parser.add_argument('--' + key + '-sha256', required=True)
    parser.add_argument('--source-sha256', required=True)
    parser.add_argument('--worker-action', choices=ACTIONS)
    parser.add_argument('--gate-fd', type=int)
    parser.add_argument('--deadline', type=float)
    for flag in ('stop-fd', 'owner-fd', 'operation-lock-fd'): parser.add_argument('--' + flag, type=int)
    args = parser.parse_args(); os.umask(0o077); p = load(args)
    need(args.command == 'worker' or all(getattr(args, name) is None for name in
         ('stop_fd', 'owner_fd', 'operation_lock_fd')), 'controller_runtime_controls_forbidden')
    if args.command == 'qualify':
        print(canonical({'status': 'SOURCE_AND_INPUTS_ONLY', 'plan': plan(p.value),
                         'actors_started': False, 'native_acceptance': False}).decode(), end='')
    elif args.command == 'worker': worker(p, args)
    else:
        need(args.worker_action is None and args.gate_fd is None and args.deadline is None, 'controller_arguments')
        print(canonical(Controller(p).run()).decode(), end='')


if __name__ == '__main__':
    try: main()
    except BaseException:
        print('{"status":"REFUSED_OR_UNKNOWN","automatic_retry":false}', file=sys.stderr)
        raise SystemExit(1)
