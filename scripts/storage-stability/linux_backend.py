"""Fixed Linux/K3s/Docker/ZFS actions for the opt-in storage workflow.

This module is inert on import. All authority and the shared lock belong to the
caller. No inventory field is executable code, SQL, a shell command or a hook.
Command evidence is private, bounded and emitted through the dual journal.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import selectors
import signal
import stat
import subprocess
import time
import http.client
import fcntl
import ipaddress
import math
import posixpath
import uuid
from pathlib import Path

from interface import Operation, Refused, need

IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,62}\Z")
KUBE_NAME = re.compile(r"[a-z0-9][a-z0-9.-]{0,252}\Z")
HEX = re.compile(r"[0-9a-f]{64}\Z")
IMAGE = re.compile(r"(?:[a-z0-9./:_-]+@)?sha256:[0-9a-f]{64}\Z")
MAX_OUTPUT = 16 * 1024 * 1024
ENV = {'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
       'LANG': 'C', 'LC_ALL': 'C', 'TMPDIR': '/tmp'}
SQL_SESSION_PREFIX = ("SET statement_timeout='10s'; SET lock_timeout='1s'; "
    "SET TIME ZONE 'UTC'; SET DateStyle='ISO,YMD'; SET IntervalStyle='iso_8601'; "
    "SET bytea_output='hex'; SET extra_float_digits=3; SET client_encoding='UTF8'; ")


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False) + '\n').encode()


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def identifier(value: object, pattern: re.Pattern = IDENTIFIER) -> str:
    need(isinstance(value, str) and pattern.fullmatch(value) is not None, 'invalid_identifier')
    return value


def secure_path(path: str, *, directory: bool = False, owner: int = 0) -> os.stat_result:
    """Refuse symlink or writable ancestry before privileged access.

    Runtime roots may have a service-owned leaf; their root-controlled parents
    prevent replacement. Immutable config/tool leaves are root-owned as well.
    """
    p = Path(path)
    need(p.is_absolute() and '..' not in p.parts and len(p.parts) > 2, 'unsafe_path')
    for parent in reversed(p.parents):
        info = parent.lstat()
        need(stat.S_ISDIR(info.st_mode) and info.st_uid == 0
             and not info.st_mode & 0o022, 'unsafe_path_ancestry')
    info = p.lstat()
    need(not stat.S_ISLNK(info.st_mode) and info.st_uid == owner, 'unsafe_path_owner')
    need(stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode), 'unsafe_path_type')
    need(not info.st_mode & 0o022, 'unsafe_path_permissions')
    return info


def process_cgroup(pid: int) -> list[str]:
    try:
        return Path(f'/proc/{pid}/cgroup').read_text().splitlines()
    except FileNotFoundError:
        return []


def members(groups: list[str]) -> list[int]:
    need(bool(groups) and all(':' in g for g in groups), 'missing_cgroup_identity')
    paths = {g.split(':', 2)[2] for g in groups}
    need('/' not in paths, 'unscoped_cgroup_identity')
    found = []
    for entry in Path('/proc').iterdir():
        if entry.name.isdecimal():
            current = {g.split(':', 2)[2] for g in process_cgroup(int(entry.name))}
            if any(actual == root or actual.startswith(root + '/') for root in paths for actual in current):
                found.append(int(entry.name))
    return sorted(found)


def same_stamp(left, right) -> bool:
    return all(getattr(left, key) == getattr(right, key) for key in
               ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns'))


def tree_digest(record: dict, deadline: float) -> dict:
    """Read-only, descriptor-anchored bounded witness for a stopped tree."""
    rows = []
    witness = {}
    total = 0
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NOATIME | os.O_NONBLOCK
    root = os.open(record['path'], flags | os.O_DIRECTORY)
    try:
        root_info = os.fstat(root)
        need((root_info.st_dev, root_info.st_ino) == (record['identity']['dev'], record['identity']['ino']),
             'tree_root_changed')
    except BaseException:
        os.close(root)
        raise

    def visit(fd: int, prefix: str, depth: int) -> None:
        nonlocal total
        need(depth <= 64 and time.monotonic() < deadline, 'tree_depth_or_deadline')
        original = sorted(os.listdir(fd))
        for name in original:
            need(len(rows) < record['max_files'] and name not in ('.', '..'), 'tree_file_bound')
            relative = prefix + name
            before = os.stat(name, dir_fd=fd, follow_symlinks=False)
            need(before.st_dev == root_info.st_dev, 'nested_tree_mount')
            row = {'path': relative, 'uid': before.st_uid, 'gid': before.st_gid,
                   'mode': stat.S_IMODE(before.st_mode)}
            if stat.S_ISDIR(before.st_mode):
                row['kind'] = 'directory'
                rows.append(row)
                child = os.open(name, flags | os.O_DIRECTORY, dir_fd=fd)
                try:
                    held = os.fstat(child)
                    need((held.st_dev, held.st_ino) == (before.st_dev, before.st_ino), 'tree_directory_rebound')
                    visit(child, relative + '/', depth + 1)
                finally:
                    os.close(child)
            elif stat.S_ISREG(before.st_mode):
                need(before.st_nlink == 1, 'tree_hardlink')
                total += before.st_size
                need(total <= record['max_bytes'], 'tree_byte_bound')
                file = os.open(name, flags, dir_fd=fd)
                with os.fdopen(file, 'rb') as stream:
                    need(os.fstat(stream.fileno()).st_ino == before.st_ino, 'tree_file_rebound')
                    h = hashlib.sha256()
                    read_size = 0
                    while block := stream.read(1024 * 1024):
                        need(time.monotonic() < deadline, 'tree_deadline')
                        read_size += len(block)
                        need(read_size <= before.st_size, 'tree_file_grew')
                        h.update(block)
                    after = os.fstat(stream.fileno())
                row.update(kind='file', size=before.st_size, sha256=h.hexdigest())
                need(all(getattr(before, key) == getattr(after, key)
                         for key in ('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns')), 'tree_file_changed')
                rows.append(row)
            elif stat.S_ISLNK(before.st_mode):
                target = os.readlink(name, dir_fd=fd)
                need(record['symlinks'].get(relative) == target, 'unreviewed_tree_symlink')
                row.update(kind='symlink', target=target)
                rows.append(row)
            else:
                raise Refused('tree_special_file')
            after = os.stat(name, dir_fd=fd, follow_symlinks=False)
            need(all(getattr(before, key) == getattr(after, key)
                     for key in ('st_dev','st_ino','st_size','st_mtime_ns','st_ctime_ns')), 'tree_entry_changed')
            witness[relative] = before
        need(original == sorted(os.listdir(fd)), 'tree_directory_changed')

    try:
        visit(root, '', 0)
        # Recheck early entries after the complete byte pass; otherwise an
        # early replacement while a later file is read can produce a digest
        # assembled from tree states that never existed together.
        for relative, before in witness.items():
            need(time.monotonic() < deadline, 'tree_deadline')
            parts = relative.split('/')
            fd = os.dup(root)
            try:
                for part in parts[:-1]:
                    child = os.open(part,flags | os.O_DIRECTORY,dir_fd=fd)
                    os.close(fd); fd = child
                need(same_stamp(before,os.stat(parts[-1],dir_fd=fd,follow_symlinks=False)),
                     'tree_final_coherence')
            finally:
                os.close(fd)
        need(same_stamp(root_info,os.fstat(root)), 'tree_root_changed')
        now = Path(record['path']).lstat()
        need((now.st_dev, now.st_ino) == (root_info.st_dev, root_info.st_ino), 'tree_path_rebound')
    finally:
        os.close(root)
    rows.sort(key=lambda x: x['path'])
    return {'sha256': digest(canonical(rows)), 'entries': len(rows), 'regular_bytes': total}


class LinuxBackend:
    def __init__(self) -> None:
        self._sequence = 0
        self._artifacts: dict[str, str] = {}
        self._owned_sql_targets: dict[str, dict] = {}
        self._cgroup_fds: dict[str, tuple[int, dict]] = {}
        self._writer_processes: dict[tuple[int,str], dict] = {}
        self._docker_config: tuple[Path,tuple[int,int]] | None = None
        self._pg_directory_fds: dict[str,dict] = {}

    def close(self) -> None:
        """Release retained evidence descriptors at operator teardown only."""
        for row in self._pg_directory_fds.values():
            os.close(row['data_fd']);os.close(row['wal_fd'])
        self._pg_directory_fds.clear()
        for fd,_ in self._cgroup_fds.values():
            os.close(fd)
        self._cgroup_fds.clear()

    def _artifact(self, op: Operation, name: str, value: object) -> str:
        filename = f'{op.phase}-{name}.json'
        self._artifacts[filename] = op.journal.save(filename, value)
        return filename

    def _run(self, op: Operation, binary: str, args: list[str], *,
             stdin: bytes | None = None, timeout: float = 30,
             record: bool = True, deadline: float | None = None) -> bytes:
        start = time.monotonic()
        need(type(timeout) in (int,float) and math.isfinite(timeout) and 0 < timeout <= 1200
             and all(type(x) is str and '\x00' not in x and len(x) <= 16384 for x in args), 'command_shape')
        need(deadline is None or type(deadline) in (int,float) and math.isfinite(deadline),
             'command_deadline_shape')
        end = min(start+timeout,deadline) if deadline is not None else start+timeout
        op.seal()
        need(time.monotonic() < end,'command_original_deadline')
        binding = op.inventory['linux']['binaries'][binary]
        path = binding['path']
        secure_path(path)
        need(HEX.fullmatch(binding['sha256']) is not None, 'invalid_binary_hash')
        self._sequence += 1
        # K3s installs kubectl/crictl as symlinks to its multi-call binary. The
        # inventory pins the canonical regular k3s file; source fixes the only
        # allowed subcommands instead of executing a mutable symlink.
        dispatch = [binary] if binary in ('kubectl','crictl') and Path(path).name == 'k3s' else []
        if binary == 'docker':
            from inventory import valid_uuid
            need(valid_uuid(op.operation_id), 'docker_operation_identity')
            if args[:2] == ['--host','unix:///var/run/docker.sock']:
                args = args[2:]
            need(not any(value in ('--host','-H','--context','--config') or
                         value.startswith(('--host=','--context=','--config=','-H=')) for value in args),
                 'docker_fixed_local_context')
            if self._docker_config is None:
                directory = Path('/tmp/openlegal-storage-docker-client-' + op.operation_id)
                directory.mkdir(mode=0o700)
                info = directory.lstat()
                need(info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o700, 'docker_private_config')
                self._docker_config = (directory,(info.st_dev,info.st_ino))
            directory,identity = self._docker_config
            info = directory.lstat()
            need(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o700
                 and (info.st_dev,info.st_ino) == identity and not os.listdir(directory),
                 'docker_config_changed')
            dispatch = ['--config',str(directory),'--host','unix:///var/run/docker.sock']
        command = [path, *dispatch, *args]
        binary_fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            metadata = os.fstat(binary_fd)
            need(stat.S_ISREG(metadata.st_mode) and metadata.st_uid == 0
                 and not metadata.st_mode & 0o022 and metadata.st_size <= 256 * 1024**2,
                 'binary_file_identity')
            h = hashlib.sha256()
            while block := os.read(binary_fd, 1024 * 1024):
                h.update(block)
            need(same_stamp(metadata, os.fstat(binary_fd)) and h.hexdigest() == binding['sha256'],
                 'binary_changed')
            op.seal()
            need(time.monotonic() < end,'command_original_deadline')
            # Execute the held inode and retain ownership beyond process
            # groups: setsid/double-fork descendants must also be reaped.
            from command_process import execute
            result = execute(command, executable_fd=binary_fd, stdin=stdin,
                env=ENV, deadline=end, max_output=MAX_OUTPUT)
        finally:
            os.close(binary_fd)
        stdout, stderr = result.stdout, result.stderr
        expired = result.expired or time.monotonic() >= end
        exceeded = result.output_limit_exceeded
        closed = result.owned_processes_closed
        evidence = {
                'binary': binary, 'arguments_sha256': digest(canonical(dispatch + args)),
                'stdin_sha256': digest(stdin) if stdin is not None else None,
                'actual_exit_code': result.actual_exit_code, 'expired': expired,
                'started': result.started, 'interrupted': result.interrupted,
                'unexpected_descendants': result.unexpected_descendants,
                'output_limit_exceeded': exceeded,
                'owned_processes_closed': closed,
                'elapsed_seconds': time.monotonic() - start,
                'stdout_sha256': digest(bytes(stdout)), 'stderr_sha256': digest(bytes(stderr)),
                'stdout_bytes': len(stdout), 'stderr_bytes': len(stderr),
        }
        if record:
            evidence.update(stdout_hex=stdout.hex(), stderr_hex=stderr.hex())
        self._artifact(op, f'command-{self._sequence:04d}', evidence)
        need(result.started and closed and not expired and not exceeded
             and not result.interrupted and not result.unexpected_descendants
             and result.actual_exit_code == 0, 'command_failed_or_unclosed')
        return bytes(stdout)

    def _kube(self, op: Operation, args: list[str]) -> dict:
        config = op.inventory['linux']
        secure_path(config['kubeconfig']['path'])
        need(digest(Path(config['kubeconfig']['path']).read_bytes()) == config['kubeconfig']['sha256'],
             'kubeconfig_changed')
        data = self._run(op, 'kubectl', ['--kubeconfig', config['kubeconfig']['path'],
            '--context', config['context'], '--request-timeout=15s', *args], record=False)
        value = json.loads(data)
        need(isinstance(value, dict), 'kubernetes_output_shape')
        return value

    def _inspect(self, op: Operation, which: str, *, deadline: float | None = None) -> dict:
        expected = op.inventory['linux']['postgres'][which]
        identifier(expected['container_id'], HEX)
        identifier(expected['image_id'], IMAGE)
        raw = json.loads(self._run(op, 'docker', ['inspect', expected['container_id']],
                                   record=False,deadline=deadline))
        need(isinstance(raw, list) and len(raw) == 1, 'container_inspect_shape')
        item = raw[0]
        need(item['Id'] == expected['container_id'] and item['Image'] == expected['image_id'],
             'postgres_container_changed')
        mounts = sorted([{'source': x['Source'], 'destination': x['Destination'],
                          'rw': x['RW'], 'type': x['Type']} for x in item['Mounts']],
                        key=lambda x: x['destination'])
        need(mounts == expected['mounts'], 'postgres_mount_changed')
        # Never record Config.Env, passwords, entrypoints or URLs from inspect.
        state_keys = ('Status','Running','Paused','Restarting','OOMKilled','Dead','Pid',
                      'ExitCode','StartedAt','FinishedAt')
        projection = {'id': item['Id'], 'image': item['Image'],
                      'state': {key:item['State'][key] for key in state_keys if key in item['State']},
                      'mounts': mounts, 'restart_policy': item['HostConfig']['RestartPolicy']}
        self._artifact(op, f'{which}-inspect-{self._sequence:04d}', projection)
        return projection

    def _sql(self, op: Operation, sql: str, *, which: str = 'primary',
             timeout: float = 15, container: str | None = None,
             deadline: float | None = None) -> object:
        now = time.monotonic()
        deadline = now+timeout if deadline is None else min(now+timeout,deadline)
        pg = op.inventory['linux']['postgres']
        database = identifier(pg['database'])
        user = identifier(pg['admin_user'])
        target = container or pg[which]['container_id']
        need(HEX.fullmatch(target) is not None, 'unbound_sql_container')
        matching = [side for side in ('primary', 'standby')
                    if pg[side]['container_id'] == target]
        if matching:
            self._inspect(op, matching[0],deadline=deadline)
        else:
            # New recovery targets are admitted only after the fixed preparer
            # independently checks operation/epoch labels, image and all mounts.
            need(target in self._owned_sql_targets, 'unbound_sql_container')
            raw = json.loads(self._run(op, 'docker', ['inspect', target],record=False,deadline=deadline))
            need(type(raw) is list and len(raw) == 1, 'owned_sql_inspect')
            item = raw[0]; expected = self._owned_sql_targets[target]
            need(item['Id'] == target and item['Image'] == expected['image_id']
                 and item['Config']['Labels'].get('openlegal.operation') == op.operation_id
                 and item['Config']['Labels'].get('openlegal.epoch') == expected['epoch'],
                 'owned_sql_target_changed')
            mounts = sorted([{'source': row['Source'], 'destination': row['Destination'],
                              'rw': row['RW'], 'type': row['Type']} for row in item['Mounts']],
                            key=lambda row: row['destination'])
            need(mounts == expected['mounts'], 'owned_sql_mount_changed')
        prefix = SQL_SESSION_PREFIX
        if sql.startswith(('SELECT openlegal_checkpoint.observe(', 'SELECT openlegal_checkpoint.readback(')):
            prefix += "SET statement_timeout='2s'; SET lock_timeout='1s'; SET transaction_timeout='5s'; "
        application = 'openlegal_storage_' + op.operation_id.replace('-', '') + '_' + str(self._sequence + 1)
        timeout = min(timeout,deadline-time.monotonic())
        need(timeout > 0, 'sql_original_deadline')
        try:
            data = self._run(op, 'docker', ['exec', '-i', '-u', str(pg['uid']),
                '-e','PGPASSFILE=/dev/null','-e','PGSERVICE=','-e','PGHOSTADDR=',
                '-e','PGTARGETSESSIONATTRS=any','-e','PGAPPNAME=' + application, target,
                '/usr/bin/env','-u','PGSERVICE','psql','-X','-w','-h','/var/run/postgresql','-p','5432','-qAt',
                '-v','ON_ERROR_STOP=1','-U',user,'-d',database],
                stdin=(prefix + sql + ';\n').encode(),timeout=timeout,deadline=deadline)
            need(deadline is None or time.monotonic() < deadline, 'sql_original_deadline')
        except BaseException:
            # docker-exec/psql closure does not establish PostgreSQL backend
            # closure. Cancel only this exact per-call application identity,
            # then independently observe absence; never replay its mutation.
            self._close_owned_sql(op, target, application)
            raise
        return json.loads(data)

    def _close_owned_sql(self, op: Operation, target: str, application: str) -> None:
        pg = op.inventory['linux']['postgres']
        need(re.fullmatch(r'openlegal_storage_[0-9a-f]{32}_[0-9]{1,8}', application), 'owned_sql_identity')
        args = ['exec','-i','-u',str(pg['uid']),'-e','PGPASSFILE=/dev/null','-e','PGSERVICE=',
                '-e','PGHOSTADDR=','-e','PGTARGETSESSIONATTRS=any',
                '-e','PGAPPNAME=openlegal_storage_closure',target,
                '/usr/bin/env','-u','PGSERVICE','psql','-X','-w','-h','/var/run/postgresql','-p','5432','-qAt',
                '-v','ON_ERROR_STOP=1','-U',identifier(pg['admin_user']),
                '-d',identifier(pg['database'])]
        predicate = ("datname=current_database() AND usename=current_user AND pid<>pg_backend_pid() "
                     "AND application_name='" + application + "'")
        raw = self._run(op, 'docker', args, stdin=(SQL_SESSION_PREFIX +
            "SELECT COALESCE(jsonb_agg(jsonb_build_object('pid',pid,'backend_start',backend_start,"
            "'cancelled',pg_cancel_backend(pid))),'[]'::jsonb) FROM pg_stat_activity WHERE " +
            predicate + ';\n').encode(), timeout=15)
        self._artifact(op, f'owned-sql-cancel-{self._sequence:04d}', json.loads(raw))
        end = time.monotonic() + 15
        while True:
            raw = self._run(op, 'docker', args, stdin=(SQL_SESSION_PREFIX +
                "SELECT COALESCE(jsonb_agg(jsonb_build_object('pid',pid,'backend_start',backend_start)),"
                "'[]'::jsonb) FROM pg_stat_activity WHERE " + predicate + ';\n').encode(), timeout=5)
            remaining = json.loads(raw)
            if remaining == []:
                self._artifact(op, f'owned-sql-closed-{self._sequence:04d}', {'application_name': application,
                                                                          'sessions': remaining})
                return
            need(time.monotonic() < end, 'owned_sql_backend_unclosed')
            time.sleep(.1)

    def _sql_mutation(self, op: Operation, sql: str, *, container: str | None = None) -> object:
        """One mutation, never replayed; COMMIT and fresh WAL ACK share 2s."""
        deadline = time.monotonic() + 2
        initial = self._sql(op, 'SELECT openlegal_replication.capture_prefix()',
                            container=container, deadline=deadline)
        self._check_replica_observation(op, initial)
        result = self._sql(op, 'BEGIN; ' + sql + '; COMMIT', container=container, deadline=deadline)
        before = self._sql(op, 'SELECT openlegal_replication.capture_prefix()',
                           container=container, deadline=deadline)
        after = self._sql(op, 'SELECT openlegal_replication.observe_flush()',
                          container=container, deadline=deadline)
        self._check_replica_observation(op, before)
        self._check_replica_observation(op, after)
        need(all(initial[k] == before[k] == after[k] for k in initial
                 if k not in ('prefix_lsn','flush_lsn')),
             'operator_replica_identity_changed')
        def lsn(value):
            left,right = value.split('/')
            return int(left,16) << 32 | int(right,16)
        need(lsn(after['flush_lsn']) >= lsn(before['prefix_lsn']) and time.monotonic() < deadline,
             'operator_commit_replica_ack_unproven')
        self._artifact(op, f'operator-mutation-ack-{self._sequence:04d}',
                       {'initial': initial, 'before': before, 'after': after})
        return result

    def _check_replica_observation(self, op: Operation, observed: dict) -> None:
        expected = op.inventory['linux']['postgres']['binding']
        if op.phase == 'strict_resume' and op.action == 'manual_failover':
            seeded = op.previous['reseed']['facts']
            peer = op.inventory['linux']['manual_recovery']['replication_peer']
            primary = seeded['pair']['primary']['checkpoint']
            expected = {'managed_epoch': op.inventory['linux']['manual_recovery']['new_managed_epoch'],
                'blob_binding_sha256': seeded['blob_binding_sha256'],
                'primary_system_identifier': primary['system_identifier'], 'timeline': primary['timeline'],
                'standby_name': peer['application_name'], 'standby_role': peer['role'],
                'standby_addr': peer['host'], 'standby_client_dn': peer['client_dn'],
                'standby_issuer_dn': peer['issuer_dn'], 'standby_client_serial': peer['client_serial']}
        fields = set(expected) | {'postmaster_started_at','sender_pid','sender_started_at','prefix_lsn','flush_lsn'}
        need(type(observed) is dict and set(observed) == fields
             and all(observed.get(key) == value for key,value in expected.items() if key != 'standby_addr'),
             'operator_replica_binding_changed')
        address = ipaddress.ip_interface(observed.get('standby_addr', ''))
        need(address.network.prefixlen == address.max_prefixlen
             and str(address.ip) == expected['standby_addr'], 'operator_replica_address_changed')
        for value in (observed['prefix_lsn'], observed['flush_lsn']):
            need(type(value) is str and re.fullmatch(r'[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}', value),
                 'operator_wal_lsn')
        need(type(observed['sender_pid']) is int and observed['sender_pid'] > 0
             and all(type(observed[key]) is str and 0 < len(observed[key]) <= 128
                     for key in ('sender_started_at','postmaster_started_at')),
             'operator_replica_lifetime_missing')

    def _read_replica_barrier(self, op: Operation, *, container: str | None = None,
                              deadline: float | None = None) -> dict:
        deadline = time.monotonic()+2 if deadline is None else deadline
        before = self._sql(op,'SELECT openlegal_replication.capture_prefix()',
                           container=container,deadline=deadline)
        self._check_replica_observation(op,before)
        after = self._sql(op,'SELECT openlegal_replication.observe_flush()',
                          container=container,deadline=deadline)
        self._check_replica_observation(op,after)
        need(all(before[key] == after[key] for key in before if key not in ('prefix_lsn','flush_lsn')),
             'operator_replica_identity_changed')
        def lsn(value):
            left,right = value.split('/')
            return int(left,16) << 32 | int(right,16)
        need(lsn(after['flush_lsn']) >= lsn(before['prefix_lsn']) and time.monotonic() < deadline,
             'operator_current_prefix_not_flushed')
        result = {'before':before,'after':after}
        self._artifact(op,f'readonly-replica-ack-{self._sequence:04d}',result)
        return result

    def _runtime_guards(self, op: Operation, *, cache_path: str | None = None,
                        epoch: str | None = None, binding: str | None = None,
                        active: bool = False,
                        allowed_active_keys: tuple[str,...] | None = None) -> dict:
        """Read the actual shared guard protocol under its directory flock.

        This never removes, edits or blesses a record. In particular an unlocked
        active_unclean record remains a recovery blocker, even after Pod exit.
        """
        from inventory import decode, valid_uuid
        config = op.inventory['linux']
        root = next(row for row in config['bound_roots'] if row['role'] == 'cache_replica')
        path = Path(cache_path or root['path'])
        wanted_epoch = epoch or op.inventory['managed_epoch']
        wanted_binding = binding or config['blob_binding_sha256']
        need(valid_uuid(wanted_epoch) and HEX.fullmatch(wanted_binding), 'guard_binding_shape')
        permitted = {'serve'} if active else set()
        if allowed_active_keys is not None:
            need(active and type(allowed_active_keys) is tuple and 1 <= len(allowed_active_keys) <= 18
                 and len(set(allowed_active_keys)) == len(allowed_active_keys)
                 and 'serve' in allowed_active_keys, 'canary_guard_key_bound')
            for key in allowed_active_keys:
                need(type(key) is str and (key in ('serve','scheduler')
                     or re.fullmatch(r'collection\.[0-9a-f-]{36}\.(?:0|[1-9][0-9]*)',key)),
                     'canary_guard_key')
                if key.startswith('collection.'):
                    _,request,generation = key.split('.')
                    need(valid_uuid(request) and 0 <= int(generation) <= 18446744073709551615,
                         'canary_collection_identity')
            permitted = set(allowed_active_keys)
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
        parent = os.open(path, flags | os.O_DIRECTORY)
        directory = None; lock = None
        try:
            parent_info = os.fstat(parent)
            need(parent_info.st_uid == root['uid'] and stat.S_IMODE(parent_info.st_mode) == 0o700,
                 'guard_parent_owner_or_mode')
            parent_id = {'dev': parent_info.st_dev, 'ino': parent_info.st_ino,
                         'fsid': os.fstatvfs(parent).f_fsid}
            directory = os.open('.openlegal-runtime-guards', flags | os.O_DIRECTORY, dir_fd=parent)
            info = os.fstat(directory)
            actual = {'dev': info.st_dev, 'ino': info.st_ino, 'fsid': os.fstatvfs(directory).f_fsid}
            need(info.st_uid == root['uid'] and stat.S_IMODE(info.st_mode) == 0o700
                 and actual['dev'] == parent_id['dev'] and actual['fsid'] == parent_id['fsid'],
                 'guard_directory_identity')
            lock = os.open('.directory.lock', os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                           dir_fd=directory)
            lock_info = os.fstat(lock)
            need(stat.S_ISREG(lock_info.st_mode) and lock_info.st_uid == root['uid']
                 and stat.S_IMODE(lock_info.st_mode) == 0o600 and lock_info.st_nlink == 1
                 and lock_info.st_size == 0 and lock_info.st_dev == info.st_dev,
                 'guard_global_lock_identity')
            fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
            names = sorted(os.listdir(directory))
            need(len(names) <= 513 and '.directory.lock' in names, 'guard_directory_bound')
            rows = []; active_rows = []; process_rows = []; active_keys = set()
            fields = {'schema_version','managed_epoch','binding_sha256','key','operation_id',
                      'parent','directory','boot_id','pid','process_start_ticks','state'}
            for name in names:
                if name == '.directory.lock':
                    continue
                need(re.fullmatch(r'[a-z0-9.-]{1,123}\.json', name), 'guard_record_name')
                fd = os.open(name, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                             dir_fd=directory)
                try:
                    before = os.fstat(fd)
                    need(stat.S_ISREG(before.st_mode) and before.st_uid == root['uid']
                         and stat.S_IMODE(before.st_mode) == 0o600 and before.st_nlink == 1
                         and before.st_dev == info.st_dev and 0 < before.st_size <= 4096,
                         'guard_record_identity')
                    raw = os.read(fd, 4097)
                    need(len(raw) == before.st_size and same_stamp(before, os.fstat(fd))
                         and same_stamp(before, os.stat(name, dir_fd=directory, follow_symlinks=False)),
                         'guard_record_changed')
                    value = decode(raw)
                    need(type(value) is dict and set(value) == fields
                         and type(value['schema_version']) is int and value['schema_version'] == 1
                         and value['managed_epoch'] == wanted_epoch
                         and value['binding_sha256'] == wanted_binding
                         and value['parent'] == parent_id and value['directory'] == actual
                         and value['boot_id'] == op.inventory['host']['boot_id']
                         and valid_uuid(value['operation_id'])
                         and type(value['key']) is str
                         and (value['key'] in ('serve','scheduler','admin.maintain','admin.rebuild')
                              or re.fullmatch(r'collection\.[0-9a-f-]{36}\.(?:0|[1-9][0-9]*)', value['key']))
                         and name == value['key'] + '.' + value['operation_id'] + '.json'
                         and type(value['pid']) is int and value['pid'] > 0
                         and type(value['process_start_ticks']) is int and value['process_start_ticks'] > 0,
                         'guard_record_binding')
                    locked = False
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        locked = True
                    if value['state'] == 'active_unclean':
                        need(active and locked and value['key'] in permitted and value['key'] not in active_keys,
                             'guard_unclean_or_unexpected_writer')
                        active_keys.add(value['key'])
                        active_rows.append(value)
                    else:
                        need(value['state'] == 'closed_clean' and not locked, 'guard_unclean_or_locked')
                    rows.append({'name': name, 'sha256': digest(raw),
                                 'state': value['state'], 'locked': locked})
                    process_rows.append({key:value[key] for key in
                                         ('key','operation_id','pid','process_start_ticks','state')} |
                                        {'record_sha256':digest(raw),'locked':locked})
                finally:
                    os.close(fd)
            need(names == sorted(os.listdir(directory)), 'guard_directory_changed')
            need(('serve' in active_keys if active else not active_keys)
                 and (allowed_active_keys is not None or len(active_rows) == int(active)),
                 'guard_active_runtime_count')
            current = (path / '.openlegal-runtime-guards').lstat()
            need((current.st_dev, current.st_ino) == (info.st_dev, info.st_ino), 'guard_directory_rebound')
            return {'directory': {'path': str(path / '.openlegal-runtime-guards'),
                                  'dev': info.st_dev, 'ino': info.st_ino},
                    'records': rows,
                    'processes': process_rows,
                    'guard': {'path': str(path / '.openlegal-runtime-guards'),
                              'dev': info.st_dev, 'ino': info.st_ino,
                              'state': 'active' if active else 'prepared_clean',
                              'epoch': wanted_epoch, 'locked': active}}
        finally:
            for fd in (lock, directory, parent):
                if fd is not None:
                    os.close(fd)

    def _provider(self, op: Operation, *, container: str | None = None) -> object:
        return self._sql(op, 'SELECT openlegal_checkpoint.state()', container=container)

    def _fingerprints(self, op: Operation, *, container: str | None = None) -> dict:
        result = self._sql(op, 'SELECT openlegal_checkpoint.retained_tables()', container=container)
        need(type(result) is dict and 1 <= len(result) <= 256, 'table_inventory_bound')
        for name,row in result.items():
            need(type(name) is str and re.fullmatch(r'[a-z_][a-z0-9_]{0,62}\.[a-z_][a-z0-9_]{0,62}',name)
                 and type(row) is dict and set(row) == {'rows','sha256'}
                 and type(row['rows']) is int and row['rows'] >= 0 and HEX.fullmatch(row['sha256']),
                 'table_fingerprint_shape')
        return result

    def _references(self, op: Operation, *, container: str | None = None) -> list:
        result = self._sql(op, 'SELECT openlegal_checkpoint.retained_references()', container=container)
        need(isinstance(result, list) and len(result) <= 200000, 'reference_bound')
        for row in result:
            match = re.fullmatch(r'([0-9a-f]{2})/([0-9a-f]{64})-[0-9a-f-]{36}', row['storage_key'])
            need(match is not None and match[1] == match[2][:2] and row['sha256'] == match[2]
                 and type(row['size']) is int and 0 <= row['size'] <= 100 * 1024**2
                 and row['role'] in ('cache', 'corpus'), 'invalid_retained_reference')
        return result

    @staticmethod
    def _verify_references(root: Path, rows: list, *, deadline: float | None = None) -> None:
        need(deadline is None or type(deadline) in (int,float) and math.isfinite(deadline),
             'reference_deadline_shape')
        def check():
            need(deadline is None or time.monotonic() < deadline, 'reference_deadline')
        check()
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NOATIME | os.O_NONBLOCK
        directory = os.open(root, flags | os.O_DIRECTORY)
        try:
            root_info = os.fstat(directory)
            need(stat.S_ISDIR(root_info.st_mode) and stat.S_IMODE(root_info.st_mode) == 0o700,
                 'reference_root_metadata')
            witness = {}
            for row in rows:
                check()
                shard, name = row['storage_key'].split('/')
                child = os.open(shard, flags | os.O_DIRECTORY, dir_fd=directory)
                try:
                    shard_info = os.fstat(child)
                    need(stat.S_ISDIR(shard_info.st_mode) and shard_info.st_dev == root_info.st_dev
                         and shard_info.st_uid == root_info.st_uid and stat.S_IMODE(shard_info.st_mode) == 0o700,
                         'reference_shard_metadata')
                    fd = os.open(name, flags, dir_fd=child)
                finally:
                    os.close(child)
                with os.fdopen(fd, 'rb') as stream:
                    info = os.fstat(stream.fileno())
                    need(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                         and info.st_size == row['size'] and info.st_dev == root_info.st_dev
                         and info.st_uid == root_info.st_uid and stat.S_IMODE(info.st_mode) == 0o600,
                         'reference_file_identity')
                    h = hashlib.sha256()
                    total = 0
                    while True:
                        check();block = stream.read(1024 * 1024);check()
                        if not block:break
                        total += len(block)
                        need(total <= row['size'], 'reference_file_grew')
                        h.update(block)
                    after = os.fstat(stream.fileno())
                    need(h.hexdigest() == row['sha256'] and same_stamp(info,after), 'reference_hash_mismatch')
                    witness[(shard,name)] = (shard_info,info)
            # Revalidate the early files after the entire byte scan, including
            # path metadata: a late replacement cannot hide behind an old FD.
            for (shard,name),(shard_before,file_before) in witness.items():
                check()
                child = os.open(shard,flags | os.O_DIRECTORY,dir_fd=directory)
                try:
                    need(same_stamp(shard_before,os.fstat(child)) and
                         same_stamp(file_before,os.stat(name,dir_fd=child,follow_symlinks=False)),
                         'reference_final_coherence')
                finally:
                    os.close(child)
            now = Path(root).lstat()
            need((now.st_dev,now.st_ino) == (root_info.st_dev,root_info.st_ino), 'reference_root_rebound')
        finally:
            os.close(directory)
        check()

    def _receipt(self, op: Operation, facts: dict) -> dict:
        result = {'schema_version': 1, 'kind': 'openlegal_storage_stability_backend_receipt',
            'operation_id': op.operation_id, 'action': op.action, 'phase': op.phase,
            'inventory_sha256': op.inventory_sha256, 'source_graph_sha256': op.source_graph_sha256,
            'authority_sha256': op.authority_sha256, 'status': 'passed', 'actual_exit_code': 0,
            'owned_processes_closed': True, 'facts': facts, 'artifacts': dict(self._artifacts)}
        self._artifacts.clear()
        return result

    def qualify(self, op: Operation) -> dict:
        need(os.geteuid() == 0, 'root_required')
        self._validate_linux(op.inventory['linux'])
        host = op.inventory['host']
        need(Path('/proc/sys/kernel/random/boot_id').read_text().strip() == host['boot_id']
             and Path('/etc/machine-id').read_text().strip() == host['machine_id'], 'host_changed')
        config = op.inventory['linux']
        for key in ['database', 'admin_user']:
            identifier(config['postgres'][key])
        identifier(config['namespace'], KUBE_NAME)
        for which in ['primary', 'standby']:
            self._inspect(op, which)
        self._postgres_layout(op)
        for root in config['bound_roots']:
            info = secure_path(root['path'], directory=True, owner=root['uid'])
            need((info.st_dev, info.st_ino) == (root['dev'], root['ino']), 'root_identity_changed')
        # st_dev alone is not a failure domain. Bind exact backing device sets
        # supplied by reviewed findmnt/lsblk/ZFS leaf-device qualification.
        domains = config['failure_domains']
        actual_hdd = self._pool_devices(op)
        actual_ssd = set()
        for tree in op.inventory['ssd_trees']:
            actual_ssd.update(self._device_leaves(tree['identity']['dev']))
        need(sorted(actual_hdd) == sorted(domains['hdd']) and sorted(actual_ssd) == sorted(domains['ssd'])
             and bool(actual_hdd) and bool(actual_ssd) and not actual_hdd & actual_ssd, 'failure_domains_overlap_or_changed')
        need({row['path'] for row in domains['leaf_devices']} == actual_hdd | actual_ssd,
             'failure_domain_hardware_coverage')
        hardware = {leaf['path']:self._device_identity(leaf) for leaf in domains['leaf_devices']}
        need(not {hardware[path] for path in actual_hdd} & {hardware[path] for path in actual_ssd},
             'failure_domains_same_physical_identity')
        pvs = self._kube(op, ['get', 'pv', '-o', 'json'])['items']
        actual = {p['metadata']['name']: p for p in pvs}
        for pin in config['persistent_volumes']:
            p = actual[pin['name']]
            need(p['metadata']['uid'] == pin['uid'] and p['spec']['local']['path'] == pin['path']
                 and p['spec']['persistentVolumeReclaimPolicy'] == 'Retain', 'pv_binding_changed')
        for workload in config['workloads']:
            item = self._kube(op, ['-n', config['namespace'], 'get', workload['kind'], workload['name'], '-o', 'json'])
            need(item['metadata']['uid'] == workload['uid'], 'workload_uid_changed')
            template = item['spec']['template']['spec']
            need(digest(canonical(template)) == workload['pod_spec_sha256'], 'workload_spec_changed')
        self._capture_writer_cgroups(op)
        proof = self._sql(op, 'SELECT openlegal_replication.capture_prefix()')
        need(config['postgres']['binding']['managed_epoch'] == op.inventory['managed_epoch'],
             'inventory_replication_epoch_changed')
        self._check_replica_observation(op, proof)
        self._artifact(op, 'qualified-bindings', {'host': host, 'pvs': config['persistent_volumes'],
            'failure_domains': domains, 'workloads': config['workloads']})
        from provider_backend import observe, phase_facts
        before = observe(self, op, 'before')
        after = observe(self, op, 'after', baseline=before)
        provider = phase_facts(self, op, before, after)
        return self._receipt(op, dict(provider, inventory_devices_pv_images_match=True,
            host_identity_match=True, source_seal_match=True))

    def _pin_writer_cgroups(self, op: Operation, rows: list[dict]) -> None:
        """Retain each qualified cgroup inode across deletion during shutdown."""
        need(rows and len(rows) <= 512, 'missing_writer_cgroups')
        for row in rows:
            path = row['path']
            if path in self._cgroup_fds:
                held, expected = self._cgroup_fds[path]
                info = os.fstat(held)
                need(expected == row and (info.st_dev, info.st_ino) == (row['dev'],row['ino']),
                     'writer_cgroup_identity_changed')
                continue
            fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                info = os.fstat(fd)
                need(stat.S_ISDIR(info.st_mode) and (info.st_dev,info.st_ino) == (row['dev'],row['ino'])
                     and Path(path).resolve() == Path(path), 'writer_cgroup_identity_changed')
                # Confirm the held directory is the actual unified cgroup, not
                # a directory of the same name containing a forged procs file.
                need(os.fstatvfs(fd).f_fsid == os.statvfs('/sys/fs/cgroup').f_fsid
                     and str(Path('/sys/fs/cgroup') / row['groups'][0].split(':',2)[2].lstrip('/')) == path,
                     'writer_cgroup_filesystem_or_path')
                self._cgroup_fds[path] = (fd, row)
            except BaseException:
                os.close(fd)
                raise

    def _application_pods(self, op: Operation) -> list[dict]:
        config = op.inventory['linux']
        deployments = {row['uid'] for row in config['workloads']}
        jobs = {row['uid'] for row in config['jobs']}
        if op.action == 'canary':
            jobs.update(row['uid'] for row in op.canary_policy['collection_job_pins'])
        replicas = self._kube(op, ['-n', config['namespace'], 'get', 'replicasets', '-o', 'json'])['items']
        owned_replicas = {row['metadata']['uid'] for row in replicas
                 if any(owner.get('controller') is True and owner.get('kind') == 'Deployment'
                        and owner.get('uid') in deployments for owner in row['metadata'].get('ownerReferences', []))}
        pods = self._kube(op, ['-n', config['namespace'], 'get', 'pods', '-o', 'json'])['items']
        need(len(pods) <= 4096, 'mcp_pod_bound')
        accepted = []
        for pod in pods:
            owned = any(owner.get('controller') is True and
                     (owner.get('kind') == 'ReplicaSet' and owner.get('uid') in owned_replicas
                      or owner.get('kind') == 'Job' and owner.get('uid') in jobs)
                     for owner in pod['metadata'].get('ownerReferences', []))
            if not owned and op.action == 'canary' and pod.get('status',{}).get('phase') in ('Succeeded','Failed'):
                continue
            need(owned, 'uninventoried_mcp_pod')
            accepted.append(pod)
        return accepted

    def _cri_rows(self, op: Operation) -> list[dict]:
        config = op.inventory['linux']
        value = json.loads(self._run(op, 'crictl', ['--runtime-endpoint', config['cri_endpoint'],
                                                   'ps', '-a', '-o', 'json']))
        need(type(value) is dict and type(value.get('containers')) is list
             and len(value['containers']) <= 16384, 'cri_container_bound')
        rows = [row for row in value['containers']
                if row.get('labels', {}).get('io.kubernetes.pod.namespace') == config['namespace']]
        need(len(rows) <= 4096 and all(HEX.fullmatch(row.get('id', '')) for row in rows), 'mcp_cri_identity')
        return rows

    def _capture_writer_cgroups(self, op: Operation) -> None:
        """Bind current Pod PIDs, rather than reusing yesterday's Pod inodes."""
        pods = self._application_pods(op)
        known = {pod['metadata']['uid'] for pod in pods}
        rows = list(op.inventory['linux']['writer_cgroups'])
        identities = []
        for item in self._cri_rows(op):
            if item['state'] != 'CONTAINER_RUNNING':
                continue
            need(item.get('labels', {}).get('io.kubernetes.pod.uid') in known, 'mcp_cri_unknown_pod')
            current = json.loads(self._run(op, 'crictl', ['--runtime-endpoint',
                op.inventory['linux']['cri_endpoint'], 'inspect', '-o', 'json', item['id']], record=False))
            need(current['status']['id'] == item['id'] and current['status']['state'] == 'CONTAINER_RUNNING',
                 'mcp_cri_changed_during_capture')
            pid = current.get('info', {}).get('pid')
            need(type(pid) is int and pid > 0, 'mcp_cri_missing_actual_pid')
            proc_stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]
            groups = process_cgroup(pid)
            need(len(groups) == 1 and groups[0].startswith('0::/') and groups[0] != '0::/'
                 and item['id'] in groups[0], 'mcp_unqualified_cgroup_layout')
            path = Path('/sys/fs/cgroup') / groups[0].split(':',2)[2].lstrip('/')
            info = path.stat()
            rows.append({'path': str(path), 'dev': info.st_dev, 'ino': info.st_ino, 'groups': groups})
            identities.append({'container_id': item['id'], 'pid': pid, 'start_ticks': proc_stat,
                               'boot_id': op.inventory['host']['boot_id'], 'groups': groups})
            self._writer_processes[(pid,proc_stat)] = {'pid':pid,'start_ticks':proc_stat,
                                                     'boot_id':op.inventory['host']['boot_id']}
        # The previous qualified capture may remain after the final serving Pod
        # has already exited. Retained descriptors still pin its actual inode.
        by_path = {row['path']: row for row in rows}
        if by_path:
            self._pin_writer_cgroups(op, list(by_path.values()))
        need(self._cgroup_fds, 'no_actual_writer_cgroup_witness')
        self._artifact(op, f'writer-pid-capture-{self._sequence:04d}', identities)

    @staticmethod
    def _validate_linux(config: dict) -> None:
        fields = {'binaries','kubeconfig','context','namespace','postgres','bound_roots','failure_domains',
            'persistent_volumes','workloads','jobs','writer_cgroups','config_maps','blob_binding_sha256',
            'serving_workload','serving_selector','health_port','index_tree','original_tree_policies',
            'restore','manual_recovery','serving_image','serving_runtime_image_id','serving_service','cri_endpoint'}
        need(type(config) is dict and set(config) == fields, 'linux_inventory_shape')
        need(type(config['binaries']) is dict and set(config['binaries']) == {'kubectl','docker','crictl','zfs','zpool'}, 'binary_inventory')
        for pin in config['binaries'].values():
            need(type(pin) is dict and set(pin) == {'path','sha256'} and HEX.fullmatch(pin['sha256']) is not None, 'binary_pin')
            need(type(pin['path']) is str and Path(pin['path']).is_absolute(), 'binary_path')
        for name in ['context','namespace','serving_workload']:
            identifier(config[name], KUBE_NAME)
        need(type(config['serving_selector']) is str and re.fullmatch(r'[A-Za-z0-9_./-]+=[A-Za-z0-9_.-]+',config['serving_selector']) is not None,
             'serving_selector')
        need(type(config['health_port']) is int and 1 <= config['health_port'] <= 65535, 'health_port')
        need(HEX.fullmatch(config['blob_binding_sha256']) is not None, 'blob_binding_pin')
        identifier(config['serving_image'], IMAGE)
        need(type(config['serving_runtime_image_id']) is str
             and re.fullmatch(r'[A-Za-z0-9:/._@-]*sha256:[0-9a-f]{64}',config['serving_runtime_image_id']),
             'serving_runtime_image_pin')
        service = config['serving_service']
        from inventory import valid_uuid
        need(type(service) is dict and set(service) == {'name','uid','spec_sha256'}
             and valid_uuid(service['uid']) and HEX.fullmatch(service['spec_sha256']), 'serving_service_pin')
        identifier(service['name'],KUBE_NAME)
        need(type(config['cri_endpoint']) is str and config['cri_endpoint'].startswith('unix:///')
             and '..' not in Path(config['cri_endpoint'][7:]).parts, 'cri_endpoint')
        pg = config['postgres']
        need(type(pg) is dict and set(pg) == {'database','admin_user','uid','primary','standby','binding'}, 'postgres_inventory')
        identifier(pg['database']); identifier(pg['admin_user'])
        need(type(pg['uid']) is int and 1 <= pg['uid'] <= 65535, 'postgres_uid')
        for side in ('primary','standby'):
            pin = pg[side]
            need(type(pin) is dict and set(pin) == {'container_id','image_id','mounts'}, 'postgres_container_pin')
            identifier(pin['container_id'],HEX); identifier(pin['image_id'],IMAGE)
            need(type(pin['mounts']) is list and 1 <= len(pin['mounts']) <= 16, 'postgres_mounts')
            for mount in pin['mounts']:
                need(type(mount) is dict and set(mount) == {'source','destination','rw','type'}
                     and mount['type'] == 'bind' and type(mount['rw']) is bool, 'postgres_mount_shape')
                need(all(type(mount[k]) is str and Path(mount[k]).is_absolute() and '..' not in Path(mount[k]).parts
                         for k in ('source','destination')), 'postgres_mount_path')
        need(pg['primary']['container_id'] != pg['standby']['container_id'], 'postgres_identity_alias')
        binding = pg['binding']
        need(type(binding) is dict and set(binding) == {'managed_epoch','blob_binding_sha256',
             'primary_system_identifier','timeline','standby_name','standby_role','standby_addr',
             'standby_client_dn','standby_issuer_dn','standby_client_serial'}, 'postgres_replication_binding')
        need(binding['blob_binding_sha256'] == config['blob_binding_sha256']
             and type(binding['timeline']) is int and 1 <= binding['timeline'] <= 4294967295
             and type(binding['primary_system_identifier']) is str
             and re.fullmatch('[1-9][0-9]{0,19}',binding['primary_system_identifier'])
             and type(binding['standby_client_serial']) is str
             and re.fullmatch('[1-9][0-9]{0,59}',binding['standby_client_serial']), 'postgres_replication_identity')
        for key in ('standby_name','standby_role'):
            identifier(binding[key])
        for key in ('standby_client_dn','standby_issuer_dn'):
            need(type(binding[key]) is str and 0 < len(binding[key].encode()) <= 1024, 'postgres_replication_certificate')
        address = ipaddress.ip_address(binding['standby_addr'])
        need(not address.is_unspecified and not address.is_multicast
             and str(address) == binding['standby_addr'], 'postgres_replication_address')
        need(type(config['workloads']) is list and len(config['workloads']) == 2, 'workload_count')
        for row in config['workloads']:
            need(type(row) is dict and set(row) == {'kind','name','uid','pod_spec_sha256'} and row['kind'] == 'deployment', 'workload_shape')
            identifier(row['name'],KUBE_NAME)
            need(HEX.fullmatch(row['pod_spec_sha256']) is not None, 'workload_pin')
        need(len({x['name'] for x in config['workloads']}) == 2
             and config['serving_workload'] in {x['name'] for x in config['workloads']}, 'workload_identity')
        need(type(config['jobs']) is list and len(config['jobs']) <= 256, 'job_inventory_bound')
        for row in config['jobs']:
            need(type(row) is dict and set(row) == {'name','uid','spec_sha256'}, 'job_pin_shape')
            identifier(row['name'],KUBE_NAME)
            need(HEX.fullmatch(row['spec_sha256']) is not None, 'job_pin')
        need(type(config['writer_cgroups']) is list and len(config['writer_cgroups']) <= 512, 'cgroup_bound')
        need(type(config['config_maps']) is list and 1 <= len(config['config_maps']) <= 16, 'config_map_bound')
        for row in config['config_maps']:
            need(set(row) == {'name','uid','data_sha256'} and HEX.fullmatch(row['data_sha256']) is not None, 'config_map_pin')
            identifier(row['name'],KUBE_NAME)
        need(type(config['bound_roots']) is list and len(config['bound_roots']) == 4, 'four_bound_blob_roots')
        need({row.get('role') for row in config['bound_roots']} ==
             {'cache_primary','cache_replica','corpus_primary','corpus_replica'}, 'bound_blob_roles')
        for row in config['bound_roots']:
            need(type(row) is dict and set(row) == {'role','path','uid','dev','ino'}
                 and all(type(row[k]) is int and row[k] >= 0 for k in ('uid','dev','ino')),
                 'bound_blob_root_shape')
            need(type(row['path']) is str and Path(row['path']).is_absolute()
                 and '..' not in Path(row['path']).parts, 'bound_blob_root_path')
        for row in config['writer_cgroups']:
            need(type(row) is dict and set(row) == {'path','dev','ino','groups'}
                 and all(type(row[k]) is int and row[k] >= 0 for k in ('dev','ino'))
                 and type(row['groups']) is list and len(row['groups']) == 1
                 and type(row['groups'][0]) is str and row['groups'][0].startswith('0::/')
                 and row['groups'][0] != '0::/' and type(row['path']) is str
                 and row['path'].startswith('/sys/fs/cgroup/') and '..' not in Path(row['path']).parts,
                 'writer_cgroup_pin_shape')
        need(type(config['restore']) is dict and type(config['manual_recovery']) is dict, 'recovery_policy_shape')
        domains = config['failure_domains']
        need(type(domains) is dict and set(domains) == {'hdd','ssd','leaf_devices'},
             'failure_domain_shape')
        for key in ('hdd','ssd'):
            need(type(domains[key]) is list and 1 <= len(domains[key]) <= 64
                 and len(set(domains[key])) == len(domains[key])
                 and all(type(path) is str and re.fullmatch('/dev/[A-Za-z0-9_.-]{1,128}',path)
                         for path in domains[key]), 'failure_domain_device_paths')
        need(type(domains['leaf_devices']) is list and 1 <= len(domains['leaf_devices']) <= 128,
             'failure_domain_hardware_bound')
        for leaf in domains['leaf_devices']:
            need(type(leaf) is dict and set(leaf) == {'path','major','minor','sysfs_path',
                 'identity_file','identity_sha256'} and type(leaf['path']) is str
                 and re.fullmatch('/dev/[A-Za-z0-9_.-]{1,128}',leaf['path'])
                 and all(type(leaf[key]) is int and leaf[key] >= 0 for key in ('major','minor'))
                 and type(leaf['sysfs_path']) is str and leaf['sysfs_path'].startswith('/sys/devices/')
                 and '..' not in Path(leaf['sysfs_path']).parts
                 and leaf['identity_file'] == 'wwid'
                 and type(leaf['identity_sha256']) is str and HEX.fullmatch(leaf['identity_sha256']),
                 'failure_domain_hardware_pin')
        need(len({leaf['path'] for leaf in domains['leaf_devices']}) == len(domains['leaf_devices']),
             'failure_domain_duplicate_hardware')

    @staticmethod
    def _device_identity(leaf: dict) -> str:
        path = Path(leaf['path'])
        info = path.stat()
        need(path.resolve(strict=True) == path and stat.S_ISBLK(info.st_mode)
             and (os.major(info.st_rdev),os.minor(info.st_rdev)) == (leaf['major'],leaf['minor']),
             'failure_domain_device_changed')
        physical = (Path('/sys/dev/block') / f"{leaf['major']}:{leaf['minor']}").resolve(strict=True)
        need(str(physical) == leaf['sysfs_path'] and not (physical/'partition').exists(),
             'failure_domain_sysfs_changed')
        fd = os.open(physical/'device'/leaf['identity_file'],os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
        try:
            raw = os.read(fd,4097)
            need(raw.strip() and len(raw) <= 4096 and digest(raw) == leaf['identity_sha256'],
                 'failure_domain_hardware_changed')
            # Compare a common canonical hardware namespace across both media.
            # A serial/WWID mixture cannot establish disjoint physical LUNs.
            return digest(raw.strip().lower())
        finally:
            os.close(fd)

    @staticmethod
    def _snapshot_mount(held: int, snapshot: str, source: Path) -> dict:
        """Bind a held directory to the actual kernel ZFS snapshot mount."""
        fields = Path(f'/proc/self/fdinfo/{held}').read_text().splitlines()
        ids = [row.split(':',1)[1].strip() for row in fields if row.startswith('mnt_id:')]
        need(len(ids) == 1 and ids[0].isdecimal(), 'snapshot_mount_id')
        lines = Path('/proc/self/mountinfo').read_text().splitlines()
        need(len(lines) <= 65536, 'snapshot_mountinfo_bound')
        rows = [row.split() for row in lines if row.split()[0] == ids[0]]
        need(len(rows) == 1 and '-' in rows[0], 'snapshot_mount_missing')
        row = rows[0]; split = row.index('-')
        need(split >= 6 and len(row) == split+4, 'snapshot_mount_shape')
        def unescape(value):
            return re.sub(r'\\(040|011|012|134)',
                lambda match: chr(int(match[1],8)), value)
        need(row[split+1] == 'zfs' and unescape(row[split+2]) == snapshot
             and unescape(row[4]) == str(source) and unescape(row[3]) == '/'
             # ZFS snapshot automounts can show VFS "rw" while the actual
             # superblock is "ro". Require the latter and descriptor readback.
             and 'ro' in row[split+3].split(',') and os.fstatvfs(held).f_flag & os.ST_RDONLY,
             'snapshot_mount_source_or_readonly')
        return {'mount_id':int(ids[0]),'filesystem':'zfs','source':snapshot,
                'mountpoint':str(source),'readonly':True}

    @staticmethod
    def _device_leaves(device: int) -> set[str]:
        seen = set()
        def visit(path: Path) -> set[str]:
            path = path.resolve(strict=True)
            need(str(path) not in seen and len(seen) < 16, 'device_graph_cycle')
            seen.add(str(path))
            children = list((path / 'slaves').iterdir()) if (path / 'slaves').exists() else []
            if children:
                result = set()
                for child in children:
                    result.update(visit(child))
                return result
            if (path / 'partition').exists():
                path = path.parent
            need((path / 'device').exists(), 'not_a_physical_block_device')
            return {str(Path('/dev') / path.name)}
        return visit(Path('/sys/dev/block') / f'{os.major(device)}:{os.minor(device)}')

    def _pool_devices(self, op: Operation) -> set[str]:
        from inventory import all_datasets
        pools = {x['pool'] for x in all_datasets(op.inventory)}
        need(len(pools) == 1, 'pool_identity')
        raw = self._run(op, 'zpool', ['status', '-P', next(iter(pools))]).decode()
        need(re.search(r'^\s*state:\s+ONLINE\s*$',raw,re.MULTILINE)
             and re.search(r'^errors: No known data errors\s*$',raw,re.MULTILINE), 'pool_unhealthy')
        devices = set()
        excluded = False
        for line in raw.splitlines():
            tokens = line.split()
            if not tokens:
                continue
            if tokens[0] in ('cache','spares'):
                excluded = True
            elif tokens[0] in ('logs','special','dedup'):
                excluded = False
            if tokens[0].startswith('/dev/') and not excluded:
                need(len(tokens) >= 5 and tokens[1:] == ['ONLINE','0','0','0'], 'pool_leaf_unhealthy')
                info = Path(tokens[0]).resolve(strict=True).stat()
                need(stat.S_ISBLK(info.st_mode), 'pool_device_type')
                devices.update(self._device_leaves(info.st_rdev))
        need(devices, 'pool_devices_missing')
        return devices

    def _quiesce(self, op: Operation) -> dict:
        need(op.phase == 'stop' and op.action == 'checkpoint'
             or op.phase == 'fence_old' and op.action == 'manual_failover', 'quiesce_scope')
        config = op.inventory['linux']
        from provider_backend import observe, hold, phase_facts
        # Manual fencing combines drainage and installing its owned clean hold.
        # Its core before-observation is taken after physical drainage; prior
        # charged settlement is chained from qualify with the same closure.
        manual = op.phase == 'fence_old'
        before = None if manual else observe(self, op, 'before')
        self._capture_writer_cgroups(op)
        recorded = []
        for workload in config['workloads']:
            item = self._kube(op, ['-n', config['namespace'], 'get', workload['kind'], workload['name'], '-o', 'json'])
            need(item['metadata']['uid'] == workload['uid'], 'workload_uid_changed')
            recorded.append({'name': workload['name'], 'replicas': item['spec'].get('replicas', 1)})
            self._cas_replicas(op,workload['name'],workload['uid'],replicas=0,
                current=item['spec'].get('replicas',1),template_sha256=workload['pod_spec_sha256'])
        jobs = self._kube(op, ['-n', config['namespace'], 'get', 'jobs', '-o', 'json'])['items']
        pinned = {j['uid']: j for j in config['jobs']}
        need({j['metadata']['uid'] for j in jobs} == set(pinned), 'uninventoried_mcp_job')
        previous_jobs = []
        for job in jobs:
            pin = pinned[job['metadata']['uid']]
            need(job['metadata']['name'] == pin['name'] and digest(canonical(job['spec'])) == pin['spec_sha256'],
                 'job_spec_changed')
            finished = any(row.get('type') in ('Complete','Failed') and row.get('status') == 'True'
                           for row in job.get('status', {}).get('conditions', []))
            previous_jobs.append({'name': pin['name'], 'uid': pin['uid'], 'finished': finished,
                                  'suspended': job['spec'].get('suspend', False)})
            if finished:
                continue
            # Suspend preserves evidence and the Job object; physical drainage
            # is a separate mandatory phase.
            self._cas_suspend_job(op,pin,normalize_suspend=False)
        self._artifact(op, 'previous-replicas', recorded)
        self._artifact(op, 'previous-jobs', previous_jobs)
        closure = self._wait_writers_closed(op)
        if manual:
            before = observe(self, op, 'before')
        mutation = hold(self, op, before) if manual else None
        after = observe(self, op, 'after')
        provider = phase_facts(self, op, before, after, closure=closure, mutation=mutation)
        return dict(provider, serving_desired_zero=True, scheduler_desired_zero=True,
            all_mcp_jobs_suspended=True, provider_accounting_preserved=True,
            all_mcp_processes_absent=True, all_mcp_cgroups_empty=True,
            all_mcp_cri_containers_terminated=True, sql_application_sessions_zero=True,
            workloads_before=recorded, jobs_before=previous_jobs, closure=closure)

    def stop(self, op: Operation) -> dict:
        return self._receipt(op, self._quiesce(op))

    def _canary_stop_and_drain(self, op: Operation) -> dict:
        """Final canary closure; no hold mutation, credential or retry here."""
        need(op.action == 'canary' and op.phase in ('final_stop','failure_stop')
             and op.authority.get('canary_final_stop_permitted') is True,
             'canary_final_stop_scope')
        policy = op.canary_policy
        need(type(policy) is dict and set(policy) == {'allowed_active_guard_keys',
             'workload_pins','collection_job_pins'}, 'canary_final_stop_policy')
        config = op.inventory['linux']
        self._capture_writer_cgroups(op)
        for workload in config['workloads']:
            item = self._kube(op,['-n',config['namespace'],'get',workload['kind'],
                                  workload['name'],'-o','json'])
            self._cas_replicas(op,workload['name'],workload['uid'],replicas=0,
                current=item['spec'].get('replicas',1),template_sha256=workload['pod_spec_sha256'])
        jobs = self._kube(op,['-n',config['namespace'],'get','jobs','-o','json'])['items']
        old = {row['uid']:row for row in config['jobs']}
        selected = {row['uid']:row for row in policy['collection_job_pins']}
        def terminal(job):
            return any(row.get('type') in ('Complete','Failed') and row.get('status') == 'True'
                       for row in job.get('status',{}).get('conditions',[]))
        jobs = [job for job in jobs if job['metadata']['uid'] in set(old)|set(selected)
                or not terminal(job)]
        need(len(selected) == len(policy['collection_job_pins'])
             and {job['metadata']['uid'] for job in jobs} == set(old)|set(selected),
             'canary_final_stop_unknown_job')
        for job in jobs:
            meta = job['metadata']; uid = meta['uid']
            pin = selected.get(uid,old.get(uid))
            spec = dict(job['spec'])
            if uid in selected:
                spec.pop('suspend',None)
            need(meta['name'] == pin['name'] and digest(canonical(spec)) == pin['spec_sha256'],
                 'canary_final_stop_job_changed')
            finished = terminal(job)
            if finished:
                continue
            self._cas_suspend_job(op,pin,normalize_suspend=uid in selected)
        closure = self._wait_writers_closed(op)
        self._artifact(op,'canary-physical-closure',closure)
        return closure

    def _canary_restart_lookup_held(self, op: Operation, *, canary_id: str) -> dict:
        """Restore lookup under the closed canary's final hold, never collection.

        This runs after the six-hour watcher and database finish. A separate
        checkpoint and external acceptance runner still own the remaining gates.
        """
        from inventory import valid_uuid
        from provider_backend import _project
        need(op.action == 'canary' and op.phase == 'final_lookup'
             and op.authority.get('canary_held_lookup_resume_permitted') is True
             and valid_uuid(canary_id), 'canary_lookup_resume_scope')
        config = op.inventory['linux']
        self._closed_writers(op)
        readback = self._sql(op,f"SELECT openlegal_checkpoint.canary_readback('{canary_id}'::uuid)")
        need(type(readback) is dict and set(readback) == {'activation','finish'}
             and type(readback['finish']) is dict, 'canary_lookup_missing_finish')
        finished = readback['finish']
        need(finished['canary_id'] == canary_id and finished['recovery_hold'] is True
             and finished['final_checkpoint_operation'] == op.operation_id
             and finished['managed_epoch'] == op.inventory['managed_epoch']
             and finished['blob_binding_sha256'] == config['blob_binding_sha256'],
             'canary_lookup_finish_binding')
        def held():
            provider = self._sql(op,f"SELECT openlegal_checkpoint.observe('{op.operation_id}'::uuid)")
            projection = _project(provider)
            need(projection['clean'] and projection['recovery_hold']
                 and provider['state']['admissions'] == []
                 and provider['admission_ownership']['slots'] == []
                 and projection['generation'] == finished['hold_generation']
                 and projection['owner_operation_id'] == op.operation_id,
                 'canary_lookup_final_hold_changed')
            return provider
        before = held()
        barrier = self._read_replica_barrier(op)
        self._verify_workload_bindings(op)
        for workload in config['workloads']:
            item = self._kube(op,['-n',config['namespace'],'get',workload['kind'],
                                  workload['name'],'-o','json'])
            need(item['metadata']['uid'] == workload['uid'] and item['spec'].get('replicas') == 0
                 and digest(canonical(item['spec']['template']['spec'])) == workload['pod_spec_sha256'],
                 'canary_lookup_workload_changed')
        serving = next(row for row in config['workloads'] if row['name'] == config['serving_workload'])
        self._cas_replicas(op,serving['name'],serving['uid'],replicas=1,current=0,
                           template_sha256=serving['pod_spec_sha256'])
        lookup = self._wait_lookup(op)
        self._verify_workload_bindings(op)
        after = held()
        result = {'kind':'openlegal_canary_closed_lookup_resume','canary_id':canary_id,
            'finish_readback':readback,'provider_before':before,'provider_after':after,
            'replica_barrier':barrier,'lookup':lookup,'collection_resumed':False,
            'final_acceptance':False,'remaining_gates':'external_checkpoint_preservation_public_acceptance'}
        self._artifact(op,'canary-held-lookup',result)
        return result

    def _cas_suspend_job(self, op: Operation, pin: dict, *, normalize_suspend: bool) -> dict:
        """Fence even a suspended Job against an older queued unsuspend CAS."""
        from inventory import valid_uuid
        identifier(pin['name'],KUBE_NAME)
        need(valid_uuid(pin['uid']) and HEX.fullmatch(pin['spec_sha256'])
             and type(normalize_suspend) is bool,'job_suspend_intent_shape')
        config = op.inventory['linux']
        item = self._kube(op,['-n',config['namespace'],'get','job',pin['name'],'-o','json'])
        spec = dict(item['spec'])
        if normalize_suspend:spec.pop('suspend',None)
        need(item['metadata']['uid'] == pin['uid'] and digest(canonical(spec)) == pin['spec_sha256'],
             'job_suspend_identity_changed')
        finished = any(row.get('type') in ('Complete','Failed') and row.get('status') == 'True'
                       for row in item.get('status',{}).get('conditions',[]))
        if finished:
            return {'uid':pin['uid'],'terminal_untouched':True}
        request = str(uuid.uuid4());nonce = op.operation_id+'.'+request
        meta = item['metadata'];annotations = meta.get('annotations')
        need(annotations is None or type(annotations) is dict,'job_suspend_annotations_shape')
        patch = [{'op':'test','path':'/metadata/uid','value':pin['uid']},
                 {'op':'test','path':'/metadata/resourceVersion','value':meta['resourceVersion']},
                 {'op':'add','path':'/spec/suspend','value':True}]
        if annotations is None:
            patch.append({'op':'add','path':'/metadata/annotations',
                'value':{'openlegal4everyone.stream/operation-fence':nonce}})
        else:
            patch.append({'op':'add','path':'/metadata/annotations/openlegal4everyone.stream~1operation-fence',
                          'value':nonce})
        self._artifact(op,'job-suspend-intent-'+request,{'kind':'openlegal_fixed_job_suspend_intent',
            'operation_id':op.operation_id,'namespace':config['namespace'],'name':pin['name'],
            'uid':pin['uid'],'observed_resource_version':meta['resourceVersion'],'fence_annotation':nonce,
            'request_id':request,'patch_sha256':digest(canonical(patch))})
        result = self._kube(op,['-n',config['namespace'],'patch','job',pin['name'],'--type=json','-p',
                               json.dumps(patch,separators=(',',':')),'-o','json'])
        old_spec = dict(item['spec']);old_spec.pop('suspend',None)
        new_spec = dict(result['spec']);new_spec.pop('suspend',None)
        need(result['metadata']['uid'] == pin['uid'] and result['spec'].get('suspend') is True
             and result['metadata']['resourceVersion'] != meta['resourceVersion']
             and result['metadata']['annotations']['openlegal4everyone.stream/operation-fence'] == nonce
             and old_spec == new_spec,'job_suspend_revision_fence_unconfirmed')
        return result

    def _cas_replicas(self, op: Operation, name: str, uid: str, *, replicas: int, current: int,
                      template_sha256: str | None = None, full_spec_sha256: str | None = None) -> dict:
        """Atomic UID/RV/spec-bound scale; same-name replacements are untouched."""
        from inventory import valid_uuid
        identifier(name,KUBE_NAME)
        need(valid_uuid(uid) and type(replicas) is int and replicas in (0,1)
             and type(current) is int and current in (0,1)
             and (template_sha256 is None) != (full_spec_sha256 is None), 'scale_intent_shape')
        config = op.inventory['linux']
        item = self._kube(op,['-n',config['namespace'],'get','deployment',name,'-o','json'])
        need(item['metadata']['uid'] == uid and item['spec'].get('replicas') == current,
             'scale_workload_identity_changed')
        if template_sha256 is not None:
            need(HEX.fullmatch(template_sha256) and digest(canonical(item['spec']['template']['spec']))
                 == template_sha256,'scale_template_changed')
        else:
            need(HEX.fullmatch(full_spec_sha256) and digest(canonical(item['spec'])) == full_spec_sha256,
                 'scale_configuration_changed')
        patch = [{'op':'test','path':'/metadata/uid','value':uid},
                 {'op':'test','path':'/metadata/resourceVersion','value':item['metadata']['resourceVersion']},
                 {'op':'test','path':'/spec/replicas','value':current},
                 {'op':'replace','path':'/spec/replicas','value':replicas}]
        request_id = str(uuid.uuid4())
        fence = None
        if replicas == 0:
            # A zero-to-zero scale can be a Kubernetes no-op with no new RV.
            # Force a fresh metadata revision so an earlier queued/unknown
            # start carrying this observed RV cannot succeed after closure.
            fence = op.operation_id + '.' + request_id
            annotations = item['metadata'].get('annotations')
            need(annotations is None or type(annotations) is dict,'scale_annotations_shape')
            if annotations is None:
                patch.append({'op':'add','path':'/metadata/annotations',
                    'value':{'openlegal4everyone.stream/operation-fence':fence}})
            else:
                patch.append({'op':'add','path':'/metadata/annotations/openlegal4everyone.stream~1operation-fence',
                              'value':fence})
        self._artifact(op,'scale-intent-'+request_id,{'kind':'openlegal_fixed_scale_intent',
            'operation_id':op.operation_id,'namespace':config['namespace'],'name':name,'uid':uid,
            'observed_resource_version':item['metadata']['resourceVersion'],
            'from_replicas':current,'to_replicas':replicas,'fence_annotation':fence,
            'request_id':request_id,'patch_sha256':digest(canonical(patch))})
        result = self._kube(op,['-n',config['namespace'],'patch','deployment',name,'--type=json',
                               '-p',json.dumps(patch,separators=(',',':')),'-o','json'])
        need(result['metadata']['uid'] == uid and result['spec']['replicas'] == replicas,
             'scale_actual_readback')
        if fence is not None:
            need(result['metadata']['resourceVersion'] != item['metadata']['resourceVersion']
                 and result['metadata']['annotations']['openlegal4everyone.stream/operation-fence'] == fence,
                 'scale_zero_revision_fence_unconfirmed')
        return result

    def _physical_writers_closed(self, op: Operation) -> dict:
        config = op.inventory['linux']
        pods = self._application_pods(op)
        need(all(p.get('status', {}).get('phase') in ('Succeeded', 'Failed') for p in pods), 'mcp_pods_not_drained')
        cri = self._cri_rows(op)
        active = [c for c in cri if c['state'] != 'CONTAINER_EXITED']
        need(not active, 'mcp_cri_not_drained')
        need(self._cgroup_fds, 'missing_retained_writer_cgroups')
        groups = []
        for fd, row in self._cgroup_fds.values():
            info = os.fstat(fd)
            need((info.st_dev,info.st_ino) == (row['dev'],row['ino']), 'writer_cgroup_identity_changed')
            pids = members(row['groups'])
            need(not pids, 'mcp_cgroup_not_drained')
            groups.append({'path': row['path'], 'dev': row['dev'], 'ino': row['ino'], 'pids': pids})
        need(Path('/proc/sys/kernel/random/boot_id').read_text().strip() == op.inventory['host']['boot_id'],
             'writer_process_boot_changed')
        for record in self._writer_processes.values():
            need(record['boot_id'] == op.inventory['host']['boot_id'], 'writer_process_boot_changed')
            try:
                raw = Path('/proc/' + str(record['pid']) + '/stat').read_text()
                need(len(raw) <= 8192, 'writer_process_stat_bound')
                ticks = raw.rsplit(')',1)[1].split()[19]
            except FileNotFoundError:
                ticks = None
            need(ticks != record['start_ticks'], 'mcp_original_process_still_alive')
        return {'cri_containers': [{'id': row['id'], 'state': row['state']} for row in cri],
                'cgroups': groups}

    def _closed_writers(self, op: Operation) -> dict:
        closure = self._physical_writers_closed(op)
        # Check every openlegal SQL session, excluding only this fixed observer.
        state = self._sql(op, 'SELECT openlegal_checkpoint.client_sessions()')
        need(state == {'sessions': []}, 'sql_application_sessions_remain')
        guards = self._runtime_guards(op)
        closure.update(sql_sessions=state['sessions'], runtime_guard_directory=guards['directory'],
                       runtime_guards=guards['records'])
        self._artifact(op, f'writers-drained-{self._sequence:04d}', closure)
        return closure

    def _wait_writers_closed(self, op: Operation) -> dict:
        deadline = time.monotonic() + 180
        transient = {'mcp_pods_not_drained','mcp_cri_not_drained','mcp_cgroup_not_drained',
                     'mcp_original_process_still_alive',
                     'sql_application_sessions_remain','guard_unclean_or_locked',
                     'guard_unclean_or_unexpected_writer'}
        while True:
            op.seal()
            try:
                return self._closed_writers(op)
            except Refused as error:
                need(str(error) in transient and time.monotonic() < deadline, 'writers_not_physically_closed')
                time.sleep(.5)

    def drain_writers(self, op: Operation) -> dict:
        from provider_backend import observe, hold, phase_facts
        before = observe(self, op, 'before')
        closure = self._closed_writers(op)
        mutation = hold(self, op, before)
        after = observe(self, op, 'after')
        facts = phase_facts(self, op, before, after, closure=closure, mutation=mutation)
        return self._receipt(op, dict(facts, all_mcp_processes_absent=True, all_mcp_cgroups_empty=True,
            all_mcp_cri_containers_terminated=True, sql_application_sessions_zero=True,
            provider_accounting_preserved=True))

    @staticmethod
    def _container_directory(pid: int, path: str) -> int:
        """Open the qualified native mount namespace, not Docker source text."""
        need(type(pid) is int and pid > 0 and type(path) is str and path.startswith('/')
             and posixpath.normpath(path) == path and '..' not in Path(path).parts,
             'postgres_container_directory_shape')
        # /proc/PID/root is an intentional kernel magic-link. Each directory
        # inside that pinned view is then traversed without following links.
        fd = os.open(f'/proc/{pid}/root',os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
        try:
            for part in Path(path).parts[1:]:
                child = os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd)
                os.close(fd);fd = child
            result = fd;fd = -1
            return result
        finally:
            if fd >= 0:
                os.close(fd)

    def _revalidate_postgres_directories(self, op: Operation, *, running: bool) -> None:
        need(set(self._pg_directory_fds) == {'primary','standby'}, 'postgres_native_layout_missing')
        for side,record in self._pg_directory_fds.items():
            for key in ('data','wal'):
                fd = record[key+'_fd'];path = Path(record[key+'_path'])
                held = os.fstat(fd);now = path.lstat()
                need(path.resolve(strict=True) == path and stat.S_ISDIR(now.st_mode)
                     and (now.st_dev,now.st_ino) == (held.st_dev,held.st_ino)
                     == record[key+'_identity'], 'postgres_native_directory_rebound')
            if running:
                current = self._inspect(op,side)
                need(current['state']['Running'] and current['state']['Pid'] == record['pid']
                     and current['state']['StartedAt'] == record['started_at']
                     and Path(f"/proc/{record['pid']}/stat").read_text().rsplit(')',1)[1].split()[19]
                       == record['start_ticks'], 'postgres_native_process_changed')
            else:
                proc = Path(f"/proc/{record['pid']}/stat")
                try:
                    ticks = proc.read_text().rsplit(')',1)[1].split()[19]
                except (FileNotFoundError,ProcessLookupError):
                    ticks = None
                need(ticks != record['start_ticks'], 'postgres_native_process_still_present')
        need(Path('/proc/sys/kernel/random/boot_id').read_text().strip() == op.inventory['host']['boot_id'],
             'postgres_native_boot_changed')

    def _postgres_layout(self, op: Operation) -> dict:
        """Bind each effective PGDATA/WAL to its actual native backup source.

        Neither PGDATA environment variables nor the presence of some postgres
        tree proves that the selected backup contains the current pair.
        """
        from inventory import all_datasets
        from restore_backend import validate
        config = validate(op.inventory['linux']['restore'], op.inventory)
        pg = op.inventory['linux']['postgres']
        need({'dataset' if 'dataset' in config[side]['data'] else 'tree'
              for side in ('primary','standby')} == {'dataset','tree'},
             'postgres_layout_requires_both_media')
        def selected(spec):
            key = 'dataset' if 'dataset' in spec else 'tree'
            rows = all_datasets(op.inventory) if key == 'dataset' else op.inventory['ssd_trees']
            row = next(row for row in rows if row['name'] == spec[key])
            root = Path(row['mountpoint'] if key == 'dataset' else row['path'])
            path = root / spec['relative']
            need(path.is_absolute() and path.resolve(strict=True) == path,
                 'postgres_layout_source_rebound')
            return path
        result = {}
        for side in ('primary','standby'):
            current = self._inspect(op, side)
            need(current['state']['Running'] and current['state']['Pid'] > 0,
                 'postgres_layout_not_running')
            pid = current['state']['Pid']
            ticks = Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()[19]
            layout = self._sql(op, 'SELECT openlegal_checkpoint.database_layout()', which=side)
            need(type(layout) is dict and set(layout) == {'data_directory'}, 'postgres_layout_shape')
            data = layout['data_directory']
            need(type(data) is str and len(data) <= 4096 and data.startswith('/')
                 and posixpath.normpath(data) == data and '..' not in Path(data).parts,
                 'postgres_layout_data_directory')
            def mapped(path):
                candidates = [row for row in current['mounts']
                    if Path(path).is_relative_to(Path(row['destination']))]
                need(candidates, 'postgres_layout_unmounted')
                mount = max(candidates,key=lambda row: len(Path(row['destination']).parts))
                need(mount['type'] == 'bind' and mount['rw'], 'postgres_layout_mount_readonly')
                host = Path(mount['source']) / Path(path).relative_to(mount['destination'])
                need(host.resolve(strict=True) == host, 'postgres_layout_host_symlink')
                return host
            data_host = mapped(data)
            need(data_host == selected(config[side]['data']), 'postgres_layout_wrong_data_source')
            fd = os.open(data_host, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
            native_data = native_wal = -1
            try:
                before = os.fstat(fd)
                need(before.st_uid == pg['uid'], 'postgres_layout_data_owner')
                native_data = self._container_directory(pid,data)
                actual = os.fstat(native_data)
                need((actual.st_dev,actual.st_ino) == (before.st_dev,before.st_ino),
                     'postgres_native_data_inode_mismatch')
                wal_info = os.stat('pg_wal',dir_fd=fd,follow_symlinks=False)
                if stat.S_ISLNK(wal_info.st_mode):
                    need(config[side]['wal'] is not None, 'postgres_layout_unbound_wal_link')
                    target = os.readlink('pg_wal',dir_fd=fd)
                    need(len(target) <= 4096, 'postgres_layout_wal_link_bound')
                    wal_container = posixpath.normpath(posixpath.join(data,target))
                    wal_host = mapped(wal_container)
                    need(wal_host == selected(config[side]['wal']), 'postgres_layout_wrong_wal_source')
                else:
                    need(stat.S_ISDIR(wal_info.st_mode) and config[side]['wal'] is None,
                         'postgres_layout_inline_wal_binding')
                    wal_host = data_host / 'pg_wal'
                    wal_container = data + '/pg_wal'
                native_wal = self._container_directory(pid,wal_container)
                wal_native_info = os.fstat(native_wal)
                wal_host_fd = os.open(wal_host,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
                try:
                    actual_wal = os.fstat(wal_host_fd)
                    need((actual_wal.st_dev,actual_wal.st_ino) ==
                         (wal_native_info.st_dev,wal_native_info.st_ino), 'postgres_native_wal_inode_mismatch')
                finally:
                    os.close(wal_host_fd)
                need(wal_host.is_dir() and wal_host.stat().st_uid == pg['uid'],
                     'postgres_layout_wal_owner')
                now = data_host.lstat()
                need((now.st_dev,now.st_ino) == (before.st_dev,before.st_ino)
                     and same_stamp(wal_info,os.stat('pg_wal',dir_fd=fd,follow_symlinks=False)),
                     'postgres_layout_changed')
                need(Path(f'/proc/{pid}/stat').read_text().rsplit(')',1)[1].split()[19] == ticks,
                     'postgres_native_process_changed')
                old = self._pg_directory_fds.get(side)
                if old is not None:
                    need(old['data_identity'] == (actual.st_dev,actual.st_ino)
                         and old['wal_identity'] == (wal_native_info.st_dev,wal_native_info.st_ino),
                         'postgres_native_qualified_directory_changed')
                    os.close(old['data_fd']);os.close(old['wal_fd'])
                self._pg_directory_fds[side] = {'data_fd':native_data,'wal_fd':native_wal,
                    'data_identity':(actual.st_dev,actual.st_ino),
                    'wal_identity':(wal_native_info.st_dev,wal_native_info.st_ino),
                    'data_path':str(data_host),'wal_path':str(wal_host),'pid':pid,
                    'started_at':current['state']['StartedAt'],'start_ticks':ticks}
                native_data = native_wal = -1
            finally:
                os.close(fd)
                for owned in (native_data,native_wal):
                    if owned >= 0:
                        os.close(owned)
            after = self._inspect(op, side)
            need(after['state']['Pid'] == current['state']['Pid']
                 and after['state']['StartedAt'] == current['state']['StartedAt'],
                 'postgres_layout_postmaster_changed')
            result[side] = {'container_id':pg[side]['container_id'],
                'data_host_path':str(data_host),'wal_host_path':str(wal_host),
                'source_data':config[side]['data'],'source_wal':config[side]['wal']}
        self._artifact(op, f'postgres-layout-{self._sequence:04d}',result)
        self._revalidate_postgres_directories(op,running=True)
        return result

    def stop_postgres(self, op: Operation) -> dict:
        self._closed_writers(op)
        layout = self._postgres_layout(op)
        fingerprints = self._fingerprints(op)
        references = self._references(op)
        self._artifact(op, 'retained-tables', fingerprints)
        self._artifact(op, 'retained-references', references)
        checkpoint = self._sql(op, 'SELECT openlegal_checkpoint.database_position()')
        barrier = self._read_replica_barrier(op)
        need(checkpoint['system_identifier'] == barrier['before']['primary_system_identifier']
             and checkpoint['timeline'] == barrier['before']['timeline'], 'checkpoint_primary_changed')
        index = tree_digest(op.inventory['linux']['index_tree'], time.monotonic() + 600)
        self._artifact(op, 'index-witness', index)
        self._artifact(op, 'database-checkpoint', checkpoint)
        groups = {}
        for which in ['primary', 'standby']:
            current = self._inspect(op, which)
            need(current['state']['Running'] and current['state']['Pid'] > 0, 'postgres_not_running')
            groups[which] = process_cgroup(current['state']['Pid'])
        # Stop primary first, while its synchronous peer remains available.
        self._revalidate_postgres_directories(op,running=True)
        for which in ['primary', 'standby']:
            target = op.inventory['linux']['postgres'][which]['container_id']
            self._run(op, 'docker', ['stop', '--timeout', '90', target], timeout=100)
            current = self._inspect(op, which)
            need(not current['state']['Running'] and current['state']['ExitCode'] == 0
                 and not members(groups[which]), 'postgres_stop_not_closed')
            # A new exec cannot connect to a stopped, ID-bound container; actual
            # state and cgroup absence are also checked above.
        self._artifact(op, 'postgres-stopped', {'groups': groups, 'checkpoint': checkpoint,
                                               'replica_barrier':barrier})
        return self._receipt(op, {'primary_actual_exit_zero': True, 'standby_actual_exit_zero': True,
            'postgres_processes_absent': True, 'postgres_cgroups_empty': True,
            'sql_endpoints_unreachable': True, 'wal_archive_workers_absent': True,
            'checkpoint': checkpoint, 'cgroups': groups,
            'retained_tables': fingerprints, 'retained_references': references, 'index': index,
            'postgres_layout':layout})

    def _postgres_still_stopped(self, op: Operation) -> None:
        self._physical_writers_closed(op)
        self._revalidate_postgres_directories(op,running=False)
        stopped = op.previous['stop_postgres']['facts']
        for which in ['primary', 'standby']:
            current = self._inspect(op, which)
            need(not current['state']['Running'] and current['state']['ExitCode'] == 0
                 and not members(stopped['cgroups'][which]), 'postgres_reopened')

    def snapshot_same_pool(self, op: Operation) -> dict:
        from inventory import all_datasets
        self._postgres_still_stopped(op)
        originals = all_datasets(op.inventory)
        need(len(op.inventory['original_datasets']) == 7 and 7 <= len(originals) <= 23
             and len({x['pool'] for x in originals}) == 1, 'snapshot_group_incomplete')
        tag = 'openlegal-checkpoint-' + op.operation_id.replace('-', '')
        snapshots = []
        for item in originals:
            name = item['name']
            need(re.fullmatch(r'[a-zA-Z0-9_./-]+', name) is not None
                 and name.startswith(item['pool'] + '/') and '..' not in name, 'unsafe_dataset')
            guid = self._run(op, 'zfs', ['get', '-Hp', '-o', 'value', 'guid', name]).decode().strip()
            need(guid == item['guid'], 'dataset_guid_changed')
            snapshots.append(name + '@' + tag)
        self._run(op, 'zfs', ['snapshot', *snapshots], timeout=120)
        proof = []
        for item, snapshot in zip(originals, snapshots, strict=True):
            guid = self._run(op, 'zfs', ['get', '-Hp', '-o', 'value', 'guid', snapshot]).decode().strip()
            proof.append({'dataset': item['name'], 'guid': item['guid'],
                          'snapshot': snapshot, 'snapshot_guid': guid})
        self._postgres_still_stopped(op)
        checkpoint = op.previous['stop_postgres']['facts']['checkpoint']
        return self._receipt(op, {'original_datasets_exact': True, 'active_datasets_exact': True,
            'one_coordinated_snapshot_group': True,
            'all_writers_still_stopped': True, 'latest_wal_and_index_sealed': True,
            'latest_wal_lsn': checkpoint['wal_lsn'], 'index_sha256': op.previous['stop_postgres']['facts']['index']['sha256'],
            'snapshots': proof})

    def resume_owned_hold(self, op: Operation) -> dict:
        from provider_backend import observe, release, phase_facts
        config = op.inventory['linux']
        self._postgres_still_stopped(op)
        for which in ('primary', 'standby'):
            target = config['postgres'][which]['container_id']
            self._run(op, 'docker', ['start', target], timeout=30)
        # Read-only observer proof after both processes exist. No mutation is
        # replayed when startup or this observation fails.
        deadline = time.monotonic() + 120
        while True:
            try:
                proof = self._sql(op, 'SELECT openlegal_replication.capture_prefix()')
                self._check_replica_observation(op, proof)
                after = self._sql(op, 'SELECT openlegal_replication.observe_flush()')
                self._check_replica_observation(op, after)
                need(all(proof[k] == after[k] for k in proof if k not in ('prefix_lsn','flush_lsn')),
                     'postgres_restart_identity_changed')
                def lsn(value):
                    left,right = value.split('/')
                    return int(left,16) << 32 | int(right,16)
                need(lsn(after['flush_lsn']) >= lsn(proof['prefix_lsn']), 'postgres_restart_not_flushed')
                break
            except Refused:
                need(time.monotonic() < deadline, 'postgres_restart_not_ready')
                time.sleep(1)
        before = observe(self, op, 'before')
        origin = op.previous['drain_writers']['facts']
        own_hold = origin['hold_created_by_operation'] is True
        mutation = None
        permitted = op.authority.get('release_owned_clean_hold_permitted', False) is True
        if own_hold and permitted:
            mutation = release(self, op, before)
        self._verify_workload_bindings(op)
        workload = next(x for x in config['workloads'] if x['name'] == config['serving_workload'])
        replicas = next(x['replicas'] for x in op.previous['stop']['facts']['workloads_before']
                        if x['name'] == config['serving_workload'])
        need(replicas == 1, 'lookup_resume_requires_previous_single_replica')
        self._cas_replicas(op,workload['name'],workload['uid'],replicas=1,current=0,
                           template_sha256=workload['pod_spec_sha256'])
        self._wait_lookup(op)
        # Record the protected post-release state BEFORE starting any provider
        # actor. Subsequent normal charged progress belongs to the next actual
        # observation, never to an invented unchanged before/after projection.
        after = observe(self, op, 'after')
        resume = (own_hold and permitted and mutation is not None
                  and op.authority.get('collection_resume_permitted', False) is True)
        facts = phase_facts(self, op, before, after, mutation=mutation, collectors_resumed=resume)
        if resume:
            self._resume_collectors(op)
        return self._receipt(op, dict(facts, same_images_config_epoch=True,
            provider_accounting_preserved=True, lookup_ready=True, strict_durability_ready=True))

    def _resume_collectors(self, op: Operation) -> None:
        # Permission remains a pre-effect gate even if a caller passed a
        # success-shaped provider receipt. Only prior known active actors resume.
        need(op.authority.get('collection_resume_permitted', False) is True
             and op.authority.get('release_owned_clean_hold_permitted', False) is True,
             'collection_resume_scope')
        config = op.inventory['linux']
        pins = {row['name']: row for row in config['jobs']}
        for row in op.previous['stop']['facts']['jobs_before']:
            if row['finished'] or row['suspended']:
                continue
            item = self._kube(op, ['-n',config['namespace'],'get','job',row['name'],'-o','json'])
            need(item['metadata']['uid'] == row['uid'] and item['spec'].get('suspend') is True,
                 'collection_job_changed')
            prior = dict(item['spec']); prior.pop('suspend', None)
            alternative = dict(prior, suspend=False)
            need(pins[row['name']]['spec_sha256'] in (digest(canonical(prior)),digest(canonical(alternative))),
                 'collection_job_template_changed')
            self._kube(op, ['-n',config['namespace'],'patch','job',row['name'],'--type=json','-p',
                json.dumps([{'op':'test','path':'/metadata/uid','value':row['uid']},
                    {'op':'test','path':'/metadata/resourceVersion','value':item['metadata']['resourceVersion']},
                    {'op':'test','path':'/spec/suspend','value':True},
                    {'op':'replace','path':'/spec/suspend','value':False}]),'-o','json'])
        for row in op.previous['stop']['facts']['workloads_before']:
            if row['name'] == config['serving_workload'] or row['replicas'] == 0:
                continue
            need(type(row['replicas']) is int and row['replicas'] == 1, 'single_scheduler_required')
            pin = next(workload for workload in config['workloads'] if workload['name'] == row['name'])
            self._cas_replicas(op,pin['name'],pin['uid'],replicas=1,current=0,
                               template_sha256=pin['pod_spec_sha256'])

    def _verify_workload_bindings(self, op: Operation) -> None:
        config = op.inventory['linux']
        for workload in config['workloads']:
            item = self._kube(op, ['-n', config['namespace'], 'get', workload['kind'], workload['name'], '-o', 'json'])
            need(item['metadata']['uid'] == workload['uid'] and
                 digest(canonical(item['spec']['template']['spec'])) == workload['pod_spec_sha256'], 'workload_spec_changed')
        for cm in config['config_maps']:
            item = self._kube(op, ['-n', config['namespace'], 'get', 'configmap', cm['name'], '-o', 'json'])
            need(item['metadata']['uid'] == cm['uid']
                 and digest(canonical(item.get('data', {}))) == cm['data_sha256'], 'runtime_configuration_changed')

    def _lookup_native(self, op: Operation, *, deployment: str | None = None,
                       deployment_uid: str | None = None,
                       epoch: str | None = None, cache_path: str | None = None,
                       binding: str | None = None, runtime_image: str | None = None,
                       allowed_active_guard_keys: tuple[str,...] | None = None) -> dict:
        """Fresh Pod, Service/EndpointSlice, HTTP and actual process-guard proof."""
        config = op.inventory['linux']
        wanted_epoch = epoch or op.inventory['managed_epoch']
        workload = self._kube(op, ['-n',config['namespace'],'get','deployment',
                                  deployment or config['serving_workload'],'-o','json'])
        expected_uid = deployment_uid or next(row['uid'] for row in config['workloads']
                                              if row['name'] == config['serving_workload'])
        need(workload['metadata']['uid'] == expected_uid and workload['spec'].get('replicas') == 1,
             'lookup_desired_replica_count')
        replicas = self._kube(op, ['-n',config['namespace'],'get','replicasets','-o','json'])['items']
        owned = {row['metadata']['uid'] for row in replicas
                 if any(owner.get('controller') is True and owner.get('kind') == 'Deployment'
                        and owner.get('uid') == workload['metadata']['uid']
                        for owner in row['metadata'].get('ownerReferences',[]))}
        pods = self._kube(op, ['-n',config['namespace'],'get','pods','-l',config['serving_selector'],
                             '-o','json'])['items']
        need(len(pods) == 1, 'lookup_pod_count')
        pod = pods[0]; statuses = pod.get('status',{}).get('containerStatuses',[])
        containers = pod['spec'].get('containers',[])
        need(any(owner.get('controller') is True and owner.get('kind') == 'ReplicaSet'
                 and owner.get('uid') in owned for owner in pod['metadata'].get('ownerReferences',[])),
             'lookup_pod_owner_changed')
        need(len(statuses) == len(containers) == 1 and statuses[0].get('ready') is True
             and statuses[0].get('restartCount') == 0
             and containers[0]['image'] == config['serving_image']
             and statuses[0].get('imageID') == (runtime_image or config['serving_runtime_image_id'])
             and any(row.get('type') == 'Ready' and row.get('status') == 'True'
                     for row in pod['status'].get('conditions',[])), 'lookup_pod_not_ready')
        service = config['serving_service']
        actual_service = self._kube(op, ['-n',config['namespace'],'get','service',service['name'],'-o','json'])
        need(actual_service['metadata']['uid'] == service['uid']
             and digest(canonical(actual_service['spec'])) == service['spec_sha256'], 'lookup_service_changed')
        slices = self._kube(op, ['-n',config['namespace'],'get','endpointslices','-l',
                                  'kubernetes.io/service-name=' + service['name'],'-o','json'])['items']
        endpoints = []
        for row in slices:
            need(any(owner.get('kind') == 'Service' and owner.get('uid') == service['uid']
                     for owner in row['metadata'].get('ownerReferences',[])), 'lookup_endpoint_owner')
            for endpoint in row.get('endpoints',[]):
                if endpoint.get('conditions',{}).get('ready') is True:
                    target = endpoint.get('targetRef',{})
                    need(target.get('kind') == 'Pod' and target.get('uid') == pod['metadata']['uid'],
                         'lookup_endpoint_target')
                    endpoints.append(target['uid'])
        need(endpoints and set(endpoints) == {pod['metadata']['uid']}, 'lookup_endpoint_missing')
        ip = pod.get('status',{}).get('podIP')
        address = ipaddress.ip_address(ip)
        need(not address.is_unspecified and not address.is_multicast, 'lookup_pod_address')
        connection = http.client.HTTPConnection(str(address),config['health_port'],timeout=3)
        try:
            connection.request('GET','/ready',headers={'Connection':'close'})
            response = connection.getresponse(); body = response.read(1025)
            need(response.status == 200 and len(body) <= 1024, 'lookup_health_failed')
        finally:
            connection.close()
        guards = self._runtime_guards(op, cache_path=cache_path, epoch=wanted_epoch,
                                      binding=binding, active=True,allowed_active_keys=allowed_active_guard_keys)
        return {'pods':[{'uid':pod['metadata']['uid'],'configured_image':containers[0]['image'],
                         'runtime_image_id':statuses[0]['imageID'],'ready':True,'restarts':0,
                         'epoch':guards['guard']['epoch']}],
                'endpoints':sorted(set(endpoints)),
                'health':{'pod_uid':pod['metadata']['uid'],'status':200,'body_sha256':digest(body),
                          'observed_at':time.time()}, 'runtime_guard':guards['guard']}

    def _wait_lookup(self, op: Operation, **options) -> dict:
        deadline = time.monotonic() + 180
        transient = {'lookup_pod_count','lookup_pod_not_ready','lookup_endpoint_missing','lookup_health_failed',
                     'guard_active_runtime_count'}
        while True:
            op.seal()
            try:
                value = self._lookup_native(op, **options)
                self._artifact(op, f'lookup-ready-{self._sequence:04d}',value)
                return value
            except (OSError,http.client.HTTPException):
                need(time.monotonic() < deadline, 'lookup_restart_not_ready')
            except Refused as error:
                need(str(error) in transient and time.monotonic() < deadline, 'lookup_restart_not_ready')
            time.sleep(.5)

    def _preservation_baseline(self, op: Operation, *, deadline: float) -> dict:
        """Fixed pre-activation baseline; never install a pointer or start serving."""
        from preservation_capture import capture_baseline
        need(op.action == 'strict_prepare' and op.phase == 'preservation_baseline'
             and op.authority.get('preservation_baseline_permitted') is True,
             'preservation_baseline_scope')
        op.seal()
        return capture_baseline(self,op,deadline=deadline)

    def _preservation_load_baseline(self, op: Operation, reference: dict, *, deadline: float) -> dict:
        """Read the unchanged original pointer and BOTH sealed old journals."""
        from preservation_capture import load_capture
        need(op.action == 'canary' and op.phase == 'preflight'
             and op.authority.get('canary_final_preservation_permitted') is True
             and type(reference) is dict and reference.get('phase') == 'preservation_baseline',
             'preservation_preflight_scope')
        op.seal()
        return load_capture(self,op,reference,deadline=deadline)

    def _preservation_final(self, op: Operation, reference: dict, *, deadline: float) -> dict:
        """Full original subset under the owned M29 hold, before M31 finish."""
        from preservation_capture import capture_final
        need(op.action == 'canary' and op.phase == 'preservation_final'
             and op.authority.get('canary_final_preservation_permitted') is True,
             'preservation_final_scope')
        op.seal()
        return capture_final(self,op,reference,deadline=deadline)

    def _canary_native(self, op: Operation) -> dict:
        from canary_native import observe
        return observe(self, op)

    def export_snapshots(self, op: Operation) -> dict:
        from copy_tree import copy_tree
        from inventory import all_datasets
        snapshots = op.previous['snapshot_same_pool']['facts']['snapshots']
        target = op.chain_root / 'originals'
        op.seal()
        target.mkdir(mode=0o700)
        manifests = {}
        for snapshot in snapshots:
            guid = self._run(op, 'zfs', ['get', '-Hp', '-o', 'value', 'guid', snapshot['snapshot']]).decode().strip()
            need(guid == snapshot['snapshot_guid'], 'snapshot_guid_changed')
            dataset = next(x for x in all_datasets(op.inventory) if x['name'] == snapshot['dataset'])
            policy = op.inventory['linux']['original_tree_policies'][dataset['name']]
            source = Path(dataset['mountpoint']) / '.zfs' / 'snapshot' / snapshot['snapshot'].split('@')[1]
            from copy_tree import root_fd
            # Opening the .zfs view may trigger its automount. Bind the actual
            # mounted descriptor, never a pre-open lstat of the automount stub.
            held = root_fd(source)
            try:
                info = os.fstat(held)
                mount = self._snapshot_mount(held,snapshot['snapshot'],source)
                actual = self._run(op,'zfs',['get','-Hp','-o','value','guid',snapshot['snapshot']]).decode().strip()
                need(actual == snapshot['snapshot_guid'], 'snapshot_guid_changed')
                record = dict(policy,path=str(source),identity={'dev':info.st_dev,'ino':info.st_ino},
                    uid=info.st_uid,gid=info.st_gid,mode=stat.S_IMODE(info.st_mode))
                name = dataset['name'].split('/')[-1]
                op.seal()
                manifest = copy_tree(record,target/name,time.monotonic()+1200)
                need(manifest['source_identity'] == record['identity']
                     and (os.fstat(held).st_dev,os.fstat(held).st_ino) == (info.st_dev,info.st_ino)
                     and os.fstatvfs(held).f_flag & os.ST_RDONLY,'snapshot_view_changed')
                need(self._snapshot_mount(held,snapshot['snapshot'],source) == mount,
                     'snapshot_mount_changed')
                actual = self._run(op,'zfs',['get','-Hp','-o','value','guid',snapshot['snapshot']]).decode().strip()
                need(actual == snapshot['snapshot_guid'],'snapshot_guid_changed')
            finally:
                os.close(held)
            manifests[dataset['name']] = {'destination': str(target / name), 'manifest': manifest,
                                        'snapshot_guid': guid,'mount':mount}
            self._artifact(op, 'export-' + name, manifests[dataset['name']])
        return self._receipt(op, {'all_snapshot_guids_match': True, 'export_bytes_verified': True,
            'chain_inputs_complete': True, 'exports': manifests,
            'full_self_contained_export': True, 'parent_chains': []})

    # Missing substeps refuse rather than issuing a success-shaped receipt.
    # Implementations are completed and independently reviewed before installation.

    def verify_isolated_restore(self, op: Operation) -> dict:
        from restore_backend import verify
        return verify(self, op)

    def _manual_verify_prepared(self, op: Operation) -> dict:
        from bootstrap_backend import verify_prepared
        return verify_prepared(self, op)

    def _manual_quiesce_and_capture(self, op: Operation) -> dict:
        from manual_backend import CHECKPOINT_SQL, save_evidence
        facts = self._quiesce(op)
        config = op.inventory['linux']
        current_workloads = []
        for row in config['workloads']:
            item = self._kube(op, ['-n',config['namespace'],'get',row['kind'],row['name'],'-o','json'])
            need(item['metadata']['uid'] == row['uid'] and item['spec'].get('replicas') == 0
                 and digest(canonical(item['spec']['template']['spec'])) == row['pod_spec_sha256'],
                 'manual_old_workload_reopened')
            current_workloads.append({key:row[key] for key in ('kind','name','uid')} | {'replicas':0})
        jobs = self._kube(op, ['-n',config['namespace'],'get','jobs','-o','json'])['items']
        pins = {row['uid']:row for row in config['jobs']}
        need({row['metadata']['uid'] for row in jobs} == set(pins), 'manual_old_job_inventory_changed')
        current_jobs = []
        for item in jobs:
            row = pins[item['metadata']['uid']]
            finished = any(condition.get('type') in ('Complete','Failed') and condition.get('status') == 'True'
                           for condition in item.get('status',{}).get('conditions',[]))
            suspended = item['spec'].get('suspend',False)
            need(item['metadata']['name'] == row['name'] and (finished or suspended), 'manual_job_reopened')
            current_jobs.append({'name':row['name'],'uid':row['uid'],'finished':finished,'suspended':suspended})
        closure = self._closed_writers(op)
        checkpoint = self._sql(op, CHECKPOINT_SQL)
        need(checkpoint['index_ack'] == checkpoint['watermark'], 'manual_index_not_caught_up')
        index = tree_digest(config['index_tree'], time.monotonic()+600)
        index['watermark'] = checkpoint['watermark']
        data = {'checkpoint':checkpoint, 'retained_tables':self._fingerprints(op),
                'retained_references':self._references(op), 'index':index, 'provider':self._provider(op),
                'closure':{'workloads':current_workloads,'jobs':current_jobs,
                    'cri':closure['cri_containers'],
                    'cgroups':[{'groups':row['groups'],'pids':[]} for _,row in self._cgroup_fds.values()],
                    'processes':list(self._writer_processes.values()),'sql_sessions':closure['sql_sessions'],
                    'runtime_guards':closure['runtime_guards']},
                'provider_receipt_facts':{key:value for key,value in facts.items()
                                          if key.startswith('provider_') or key in
                                          ('hold_created_by_operation','collectors_resumed')}}
        return save_evidence(self,op,'captured',data)

    def _manual_reseed_and_bind(self, op: Operation, promoted: dict, sealed: dict) -> dict:
        from bootstrap_backend import reseed_and_bind
        return reseed_and_bind(self, op, promoted, sealed)

    def _manual_bind_replication_control(self, op: Operation, container: str, expected: dict) -> dict:
        from inventory import decode
        from manual_backend import _old_fence, _root_binding, _observer
        need(op.action == 'manual_failover' and op.phase == 'reseed'
             and op.authority.get('approved',False) is True
             and 'manual_failover' in op.authority.get('actions',()), 'manual_binding_scope')
        promoted = op.previous['promote']['facts']
        need(container == promoted['promoted_container_id'], 'manual_binding_primary_target')
        fields = set(op.inventory['linux']['postgres']['binding'])
        need(type(expected) is dict and set(expected) == fields, 'manual_binding_fields')
        name = op.phase + '-bootstrap-seed-intent.json'
        pin = op.journal.output_hashes().get(name)
        need(pin is not None, 'manual_binding_seed_intent_missing')
        seed = decode(op.journal.read_phase_bytes(op.phase,name,pin))
        seed_fields = {'schema_version','kind','operation_id','new_epoch','primary_container_id',
                       'standby_container_id','blob_roots','blob_binding_sha256',
                       'seed_manifest_sha256','expected_binding'}
        need(type(seed) is dict and set(seed) == seed_fields and type(seed['schema_version']) is int
             and seed['schema_version'] == 1 and seed['kind'] == 'openlegal_bootstrap_seed_intent'
             and seed['operation_id'] == op.operation_id and seed['primary_container_id'] == container
             and seed['expected_binding'] == expected
             and seed['new_epoch'] == op.inventory['linux']['manual_recovery']['new_managed_epoch']
             and expected['managed_epoch'] == seed['new_epoch']
             and seed['standby_container_id'] in self._owned_sql_targets, 'manual_binding_seed_intent')
        sealed = promoted['sealed_source']; prepared = op.previous['fence_old']['facts']['prepared']
        binding = _root_binding(op,seed['blob_roots'],prepared,sealed['retained_references'])
        need(binding == seed['blob_binding_sha256'] == expected['blob_binding_sha256'],
             'manual_binding_seed_hash')
        correspondence = {'schema_version':1,'kind':'openlegal_manual_seed_correspondence',
            'managed_epoch':seed['new_epoch'],'operation_id':op.operation_id,
            'roots':sorted([{key:row[key] for key in ('role','manifest_sha256','reference_sha256')}
                            for row in seed['blob_roots']],key=lambda row:row['role'])}
        need(digest(canonical(correspondence)) == seed['seed_manifest_sha256'],
             'manual_binding_correspondence')
        current_primary = promoted['promoted_checkpoint']
        peer = op.inventory['linux']['manual_recovery']['replication_peer']
        wanted = {'managed_epoch':seed['new_epoch'],'blob_binding_sha256':binding,
            'primary_system_identifier':current_primary['system_identifier'],
            'timeline':current_primary['timeline'],'standby_name':peer['application_name'],
            'standby_role':peer['role'],'standby_addr':peer['host'],
            'standby_client_dn':peer['client_dn'],'standby_issuer_dn':peer['issuer_dn'],
            'standby_client_serial':peer['client_serial']}
        need(expected == wanted, 'manual_binding_not_prepared_peer')
        _old_fence(self,op)
        intent = {'schema_version':1,'kind':'openlegal_manual_replication_binding_intent',
            'operation_id':op.operation_id,'source_graph_sha256':op.source_graph_sha256,
            'authority_sha256':op.authority_sha256,'seed_intent_sha256':pin,
            'primary_container_id':container,'expected_old':op.inventory['linux']['postgres']['binding'],
            'proposed':expected}
        self._artifact(op,'manual-binding-intent',intent)
        intent_sha = digest(canonical(intent))
        def sql_json(value):
            return "convert_from(decode('" + canonical(value).hex() + "','hex'),'UTF8')::jsonb"
        # The protected function checks the old row, held provider, free owner
        # locks and actual new peer inside this transaction. An old-epoch
        # capture_prefix precheck is intentionally impossible after promotion.
        deadline = time.monotonic()+2
        sql = ("BEGIN; SELECT openlegal_checkpoint.bind_replica('" + op.operation_id + "'::uuid,'" +
               intent_sha + "'," + sql_json(intent['expected_old']) + ',' + sql_json(expected) + '); COMMIT')
        result = self._sql(op,sql,container=container,deadline=deadline)
        need(type(result) is dict and set(result) == {'operation_id','intent_sha256','previous_binding',
                                                      'new_binding','peer_before_commit'}
             and result['operation_id'] == op.operation_id and result['intent_sha256'] == intent_sha,
             'manual_binding_result_identity')
        def normalize(row):
            need(type(row) is dict and set(row) == fields,'manual_binding_result_shape')
            normalized = dict(row)
            normalized['standby_client_serial'] = str(row['standby_client_serial'])
            address = ipaddress.ip_interface(row['standby_addr'])
            need(address.network.prefixlen == address.max_prefixlen,'manual_binding_network_mask')
            normalized['standby_addr'] = str(address.ip)
            return normalized
        need(normalize(result['previous_binding']) == intent['expected_old']
             and normalize(result['new_binding']) == expected,'manual_binding_actual_row')
        checks = dict(expected); checks.pop('standby_addr')
        checks['postmaster_started_at'] = current_primary['postmaster_started_at']
        first = _observer(result['peer_before_commit'],checks,peer['host'])
        before = self._sql(op,'SELECT openlegal_replication.capture_prefix()',container=container,deadline=deadline)
        after = self._sql(op,'SELECT openlegal_replication.observe_flush()',container=container,deadline=deadline)
        _observer(before,checks,peer['host']); _observer(after,checks,peer['host'])
        need(all(first[key] == before[key] == after[key] for key in first
                 if key not in ('prefix_lsn','flush_lsn')), 'manual_binding_peer_lifetime_changed')
        def lsn(value):
            left,right = value.split('/')
            return int(left,16) << 32 | int(right,16)
        need(lsn(after['flush_lsn']) >= lsn(before['prefix_lsn']) >= lsn(first['prefix_lsn'])
             and time.monotonic() < deadline,'manual_binding_commit_ack_unproven')
        proof = {'binding_result':result,'before':before,'after':after,'seed_intent_sha256':pin}
        self._artifact(op,'manual-binding-ack',proof)
        return proof

    def _manual_candidate_peer(self, op: Operation, container: str, expected: dict) -> dict:
        need(op.action == 'manual_failover' and op.phase == 'reseed'
             and container == op.previous['promote']['facts']['promoted_container_id'],
             'manual_candidate_scope')
        raw = canonical(expected)
        need(len(raw) <= 16384,'manual_candidate_bound')
        sql = "SELECT openlegal_replication.observe_candidate(convert_from(decode('" + raw.hex() + "','hex'),'UTF8')::jsonb)"
        value = self._sql(op,sql,container=container,timeout=2)
        need(type(value) is dict and set(value) == {'kind','proposed_binding','candidate'}
             and value['kind'] == 'openlegal_replication_candidate_peer'
             and value['proposed_binding'] == expected,'manual_candidate_identity')
        return value

    def _manual_apply_target_configuration(self, op: Operation, promoted: dict, reseeded: dict) -> dict:
        from bootstrap_backend import verify_configuration
        return verify_configuration(self, op, promoted, reseeded)

    def _manual_strict_start(self, op: Operation, promoted: dict, reseeded: dict,
                             configured: dict, *, release_owned_hold: bool,
                             resume_collection: bool) -> dict:
        from bootstrap_backend import read_prepared_configuration, read_pair
        from manual_backend import save_evidence, POST_START_OPERATIONAL_TABLES, _pair
        from provider_backend import observe, release, phase_facts
        need(op.action == 'manual_failover' and op.phase == 'strict_resume'
             and type(release_owned_hold) is bool and resume_collection is False,
             'manual_lookup_only_start_scope')
        if release_owned_hold:
            need(op.authority.get('release_owned_clean_hold_permitted',False) is True,
                 'manual_hold_release_scope')
        seeded = op.previous['reseed']['facts']
        prepared = seeded['prepared']
        doc = read_prepared_configuration(op)
        # Revalidate native SQL/root correspondence immediately before effects;
        # neither a prior reseed receipt nor Ready state is a cached ACK.
        current = read_pair(self,op,promoted,promoted['sealed_source'],prepared,active=False)
        _pair(self,op,current,promoted,prepared)
        target = promoted['promoted_container_id']
        before = observe(self,op,'before',container=target)
        mutation = release(self,op,before,container=target) if release_owned_hold else None
        config = op.inventory['linux']
        serving = doc['serving_workload']
        # The private prepared document pins all resources; only its single
        # lookup Deployment starts here. Scheduler/collection remain inhibited.
        item = self._kube(op,['-n',config['namespace'],'get','deployment',serving['name'],'-o','json'])
        need(item['metadata']['uid'] == serving['uid'] and item['spec'].get('replicas') == 0,
             'manual_prepared_lookup_changed')
        for key,kind in (('scheduler_workload','deployment'),('collection_job','job')):
            row = doc[key]
            resource = self._kube(op,['-n',config['namespace'],'get',kind,row['name'],'-o','json'])
            need(resource['metadata']['uid'] == row['uid'] and
                 (resource['spec'].get('replicas') == 0 if kind == 'deployment'
                  else resource['spec'].get('suspend') is True), 'manual_collector_not_inhibited')
        pin = next(row for row in configured['resources']
                   if row['kind'] == 'deployment' and row['name'] == serving['name']
                   and row['uid'] == serving['uid'])
        self._cas_replicas(op,serving['name'],serving['uid'],replicas=1,current=0,
                           full_spec_sha256=pin['spec_sha256'])
        lookup = self._wait_lookup(op,deployment=serving['name'],deployment_uid=serving['uid'],
            epoch=doc['new_epoch'],cache_path=doc['blob_roots']['cache_replica']['host_path'],
            binding=seeded['blob_binding_sha256'],runtime_image=doc['serving_runtime_image_id'])
        pair = read_pair(self,op,promoted,promoted['sealed_source'],prepared,active=True)
        after = observe(self,op,'after',container=target)
        provider = phase_facts(self,op,before,after,mutation=mutation,collectors_resumed=False)
        changed = set(POST_START_OPERATIONAL_TABLES) | {'openlegal_replication.control'}
        if release_owned_hold:
            changed |= {'openlegal_admin.provider_control','openlegal_checkpoint.operation'}
        current_immutable = {}; operational = {}
        for name in ('primary','standby'):
            tables = pair[name]['retained_tables']
            current_immutable[name] = {key:value for key,value in tables.items() if key not in changed}
            operational[name] = {key:value for key,value in tables.items() if key in changed}
        reference = seeded['pair_reference']
        pair.update(pods=lookup['pods'],endpoints=lookup['endpoints'],provider_receipt_facts=provider,
            preservation_before_start=dict(reference,phase='reseed'),
            current_immutable_tables=current_immutable,operational_metadata=operational)
        return save_evidence(self,op,'started',pair)

    def _manual_lookup_readback(self, op: Operation) -> dict:
        from bootstrap_backend import read_prepared_configuration
        from manual_backend import save_evidence
        doc = read_prepared_configuration(op)
        seeded = op.previous['reseed']['facts']
        target = seeded['pair']['primary']['container_id']
        # Retained metadata is collected afresh after the preceding long root
        # scans; the manual protocol then performs its final fixed WAL barrier.
        tables = self._fingerprints(op,container=target)
        references = self._references(op,container=target)
        provider = self._provider(op,container=target)
        serving = doc['serving_workload']
        lookup = self._lookup_native(op,deployment=serving['name'],deployment_uid=serving['uid'],
            epoch=doc['new_epoch'],cache_path=doc['blob_roots']['cache_replica']['host_path'],
            binding=seeded['blob_binding_sha256'],runtime_image=doc['serving_runtime_image_id'])
        lookup.update(provider=provider,retained_tables=tables,retained_references=references)
        return save_evidence(self,op,'lookup',lookup)

    def fence_old(self, op: Operation) -> dict:
        from manual_backend import fence_old
        return fence_old(self, op)

    def verify_latest(self, op: Operation) -> dict:
        from manual_backend import verify_latest
        return verify_latest(self, op)

    def promote(self, op: Operation) -> dict:
        from manual_backend import promote
        return promote(self, op)

    def reseed(self, op: Operation) -> dict:
        from manual_backend import reseed
        return reseed(self, op)

    def strict_resume(self, op: Operation) -> dict:
        from manual_backend import strict_resume
        return strict_resume(self, op)
