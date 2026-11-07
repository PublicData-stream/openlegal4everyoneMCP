"""Actual driver -> seed -> roles -> maintenance -> Pair owner history.

Native PostgreSQL, Docker, TLS, device, namespace, cgroup and external
standby-parentage observations are labelled synthetic ports. Pair's real intake/loader, producer, dual journal, preservation
and audit checks, strict first/final ACK capture, and helper wait/closure run.
Harmless helper children really exit and are reaped; standby stays held until
fixture cleanup. This is no native acceptance, independent-media or HDD proof.
"""
import copy
from contextlib import ExitStack, contextmanager
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import sys
import time
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(BASE), str(Path(__file__).resolve().parent)]
import initial_pg_pair as p
import test_pair_intake as intake_tests
import test_initial_pg_pair as pair_tests
import test_initial_role_prepare as role_tests
from inventory import canonical, decode, digest


class GenuinePair(unittest.TestCase):
    """Composition keeps prior test methods out of each completion test class."""
    def setUp(self):
        self.primary_process = {'pid': os.getpid(), 'start_ticks': p.driver.processes._identity(os.getpid()),
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
        driver_tests = intake_tests.history.intake_tests.driver_tests
        real_clock = driver_tests.DriverTests.prepare_clock
        real_artifact = p.driver.Backend._artifact
        real_seed_saved = p.seed._saved
        parent_process = copy.deepcopy(self.primary_process)
        def before_clock(case):
            # Add fictional native DATA before the one immutable clock/input
            # binding. No completed source history is edited after production.
            pgdata = case.root / 'fictional-original-pgdata'; pgdata.mkdir(mode=0o700)
            os.chown(pgdata, 70, 70)
            case.inv['legacy_prepare'].update(expected_system_identifier='12345678901234567890',
                expected_timeline=1, pgdata={'host_path': str(pgdata), 'container_path': p.PGDATA})
            case.inv.update(schema_version=1, kind='openlegal_fixture_legacy_inventory', operation_id=case.opid,
                revision='0' * 40, original_datasets=[], active_datasets=[], ssd_trees=[], coverage={},
                kubernetes={}, postgres={}, provider_hold={})
            case.inv['linux'].update(workloads=[], jobs=[], config_maps=[], persistent_volumes=[],
                serving_image=None, serving_runtime_image_id=None, serving_service=None,
                bound_roots=[], blob_binding_sha256=None)
            case.inv['linux']['postgres'].update(uid=70, database='openlegal')
            case.inv['linux']['postgres']['primary'].update(image_id='sha256:' + '3' * 64)
            manifest = {'kind': 'openlegal_compiled_migration_manifest', 'candidate': case.template['candidate'],
                'native_architecture': {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(platform.machine()),
                'migrations': [{'version': n, 'checksum': '%096x' % n} for n in range(1, 34)]}
            case.template['compiled_manifest'] = case.save('pair-compiled-manifest.json', manifest)
            case.tref = case.save('pair-driver-template.json', case.template)
            case.invref = case.save('pair-driver-legacy.json', case.inv)
            case.config.update(initial_template=case.tref, legacy_inventory=case.invref)
            case.configref = case.save('pair-driver-config.json', case.config)
            case.auth.update(inventory_sha256=case.configref['sha256'],
                initial_template_sha256=case.tref['sha256'], legacy_inventory_sha256=case.invref['sha256'])
            case.authref = case.save('pair-driver-authority.json', case.auth)
            return real_clock(case)
        def artifact(backend, op, label, value):
            if op.phase == p.legacy.PHASE and label == 'returned-payloads':
                value = copy.deepcopy(value); value['fresh_primary']['primary_process'] = parent_process
            return real_artifact(backend, op, label, value)
        def seed_saved(backend, op, label, value):
            if label.startswith('fixture-reference-'):
                value = dict(value, offset=0)
            return real_seed_saved(backend, op, label, value)
        self.hpair = intake_tests.PairIntake('test_genuine_completed_history_creates_only_exact_restriction_data')
        self.addCleanup(self.hpair.doCleanups)
        with patch.object(driver_tests.DriverTests, 'prepare_clock', before_clock), \
             patch.object(p.driver.Backend, '_artifact', artifact), \
             patch.object(p.seed, '_saved', side_effect=seed_saved):
            self.hpair.setUp()
        for key in ('b', 'lock', 'j', 'clock', 'root', 'case', 'seedcase', 'role_cfg', 'role_result',
                    'audit', 'after', 'provision', 'provision_ref', 'pair_source', 'inbox'):
            setattr(self, key, getattr(self.hpair, key))
        self.children = []; self.native_commands = []; self.sql_calls = []; self.standby_child = None
        target = Path(tempfile.mkdtemp(prefix='pair-native-target-', dir='/tmp')); target.chmod(0o700)
        self.addCleanup(shutil.rmtree, target, True)
        self.provision['target'].update(parent=str(target), identity={'dev': target.stat().st_dev, 'ino': target.stat().st_ino})
        self.hpair.provision_ref = self.write('genuine-pair-provision.json', self.provision)
        self.provision_ref = self.hpair.provision_ref
        original_members = p.linux.members; original_groups = p.linux.process_cgroup
        original_children = p.driver.processes._children
        def members(groups):
            if groups in (['0::/fixture-backup'], ['0::/fixture-verify']): return []
            return original_members(groups)
        def groups(pid):
            if pid == self.primary_process['pid']: return ['0::/fixture-primary']
            if self.standby_child is not None and pid == self.standby_child.pid: return ['0::/fixture-standby']
            return original_groups(pid)
        def children(pid):
            found = original_children(pid)
            if pid == os.getpid() and self.standby_child is not None: found.discard(self.standby_child.pid)
            return found
        for item in (patch.object(p.linux, 'members', side_effect=members),
                     patch.object(p.linux, 'process_cgroup', side_effect=groups),
                     patch.object(p.driver.processes, '_children', side_effect=children),
                     patch.object(p, '_completed_local_mounts', side_effect=self.synthetic_local_mounts)):
            item.start(); self.addCleanup(item.stop)
        self.addCleanup(self.cleanup_pair)

    def synthetic_local_mounts(self, owner, native, container_path):
        # Explicit synthetic namespace observation only. Keep genuine process
        # lifetime and held FD checks; these dirs are real disposable /tmp DATA.
        process = native['process']; self.assertEqual(p.driver.processes._identity(process['pid']), process['start_ticks'])
        for key in ('data_fd', 'wal_fd', 'cgroup_fd'): os.fstat(native[key])
        self.assertEqual(container_path, p.PGDATA)

    def write(self, name, value): return self.hpair.write(name, value)
    def prepare_pair(self): return self.hpair.prepare()
    def pair_envelope(self, s, **kwargs): return self.hpair.envelope(s, **kwargs)

    def cleanup_pair(self):
        for child in self.children:
            if child.poll() is None: child.terminate()
            child.wait(timeout=5)
        fds = set(getattr(self.b, '_initial_pg_pair_retained', []))
        for fd in fds:
            try: os.close(fd)
            except OSError: pass
        shutil.rmtree('/tmp/openlegal-initial-pg-pair-' + self.case.opid, ignore_errors=True)

    def expected_binding(self, owner):
        n, q = owner.p['network'], owner.p['peer']
        return p._binding({'managed_epoch': owner.sc[0]['managed_epoch'],
            'primary_system_identifier': owner.work['legacy_prepare']['expected_system_identifier'],
            'timeline': owner.work['legacy_prepare']['expected_timeline'], 'standby_name': q['application_name'],
            'standby_role': q['role'], 'standby_addr': n['standby_address'], 'standby_client_dn': q['client_dn'],
            'standby_issuer_dn': q['issuer_dn'], 'standby_client_serial': q['client_serial'],
            'blob_binding_sha256': owner.seed_result['blob_binding_sha256']})

    @contextmanager
    def pair_native_ports(self):
        """Fixed synthetic native ports; actual owner/proof/wait implementations."""
        owner_holder = []
        def command(owner, args, **kwargs):
            owner.seal(); self.native_commands.append(copy.deepcopy(args))
            if args[0] == 'wait':
                native = next(h for h in owner.helpers if h['container_id'] == args[1])
                self.assertEqual(native['fixture_actual_wait'], 0); raw = b'0\n'
            else:
                self.assertEqual(args[:3], ['exec', '-u', '70']); self.assertEqual(args[3], owner.standby)
                self.assertEqual(args[4:6], ['/bin/sh', '-c']); raw = b''
            receipt = owner.save('synthetic-native-command', {'actual_exit_code': 0, 'started': True,
                'owned_processes_closed': True, 'stdout_sha256': digest(raw), 'stdout_bytes': len(raw),
                'fixture_only': True, 'binary': 'docker', 'stdin_sha256': None,
                'arguments_sha256': digest(canonical(['--config', str(owner.b._docker_config[0]), '--host', 'unix:///var/run/docker.sock', *args])),
                **dict.fromkeys(('expired', 'interrupted', 'unexpected_descendants', 'output_limit_exceeded'), False)})
            return raw, receipt
        def sql(owner, payload, **kwargs):
            owner.seal(); self.sql_calls.append(payload)
            if payload == 'SELECT openlegal_checkpoint.client_sessions()': value = {'sessions': []}
            elif payload == 'SELECT openlegal_checkpoint.database_position()':
                value = {'system_identifier': owner.work['legacy_prepare']['expected_system_identifier'],
                    'timeline': owner.work['legacy_prepare']['expected_timeline'],
                    'postmaster_started_at': '2026-11-02 00:00:00+00', 'in_recovery': kwargs.get('cid') == owner.standby and owner.standby is not None}
                value.update({'replay_lsn': '0/100', 'receive_lsn': '0/100'} if value['in_recovery'] else
                    {'wal_lsn': '0/100', 'index_ack': 0, 'watermark': 0})
            elif payload == 'SELECT openlegal.replication_required()': value = False
            elif "'sync',current_setting" in payload:
                value = {'sync': '', 'source': 'configuration file', 'commit': 'on', 'fsync': 'on',
                    'full_page_writes': 'on', 'wal_level': 'replica', 'senders': '2', 'pending_restart': False}
            elif payload == 'SELECT openlegal_checkpoint.state()': value = {'fixture_controls': 'unchanged'}
            elif payload == p.role_prepare.catalog_sql(owner.role_context['names'], owner.role_context['runtime']):
                value = role_tests.catalog(self.role_cfg)
                value.update(session_user=owner.p['recovery_role'], current_user=owner.p['recovery_role'])
            elif payload.startswith('SELECT NOT has_function_privilege('): value = True
            elif payload.startswith("SELECT jsonb_build_object('audit'"):
                value = {'audit': copy.deepcopy(self.audit), 'state': copy.deepcopy(self.after['state']),
                    'all_slots_free': True, 'new_observations': []}
            elif payload == p.migration.REFERENCES_SQL: value = copy.deepcopy(owner.old['payloads']['preservation']['references'])
            elif payload == p.migration.OLD_MIGRATIONS_SQL: value = copy.deepcopy(owner.old['payloads']['preservation']['migration_history'])
            elif payload == p.migration.MIGRATIONS_SQL: value = [dict(row, success=True) for row in p.migration._manifest(owner.work['initial_migration'])]
            elif payload.startswith('SELECT jsonb_build_array('):
                kinds = re.findall(r"preservation_page\('([a-z_]+)'", payload)
                self.assertTrue(kinds)
                value = [{'schema_version': 1, 'kind': 'openlegal_preservation_page', 'relation': kind,
                    'rows': [], 'next_key': None, 'has_more': False} for kind in kinds]
            elif payload.startswith('SELECT openlegal_replication.observe_candidate('):
                expected = self.expected_binding(owner)
                value = {'kind': 'openlegal_replication_candidate_peer', 'proposed_binding': expected,
                    'candidate': pair_tests.peer(expected, candidate=True, prefix='0/80')}
            else: raise AssertionError('Unexpected fixture SQL: ' + payload)
            raw = canonical(value)
            native_ref = owner.save('synthetic-native-sql-command', {'fixture_only': True, 'payload_sha256': digest(payload.encode()),
                'actual_exit_code': 0, 'started': True, 'owned_processes_closed': True})
            row = {'actual_command': native_ref, 'raw_utf8': raw.decode()}
            return copy.deepcopy(value), owner.save('sql-raw', row), len(canonical(row))
        def native_primary(owner):
            owner_holder.append(owner); owner.seal()
            fds = [os.open(owner.work['legacy_prepare']['pgdata']['host_path'], os.O_RDONLY | os.O_DIRECTORY) for _ in range(2)]
            fds.append(os.open(self.root, os.O_RDONLY | os.O_DIRECTORY))
            native = {'process': copy.deepcopy(self.primary_process), 'data_fd': fds[0], 'wal_fd': fds[1],
                'cgroup_fd': fds[2], 'groups': ['0::/fixture-primary'], 'fixture_only': True}
            owner.native.append(native); owner.b._initial_pg_pair_retained.extend(fds)
            owner.primary_pidfd = os.pidfd_open(os.getpid()); owner.b._initial_pg_pair_retained.append(owner.primary_pidfd)
            return native
        def start(owner, action):
            owner.seal(); spec = p._spec(owner, action); cid = {'backup': 'a', 'verify': 'b', 'standby': 'd'}[action] * 64
            owner.created[cid] = spec
            child = subprocess.Popen([sys.executable, '-I', '-B', '-c', 'import time; time.sleep(120)' if action == 'standby' else 'pass'])
            self.children.append(child); pidfd = os.pidfd_open(child.pid)
            process = {'pid': child.pid, 'start_ticks': p.driver.processes._identity(child.pid), 'boot_id': self.primary_process['boot_id']}
            group = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            owner.b._initial_pg_pair_retained.extend([pidfd, group])
            native = {'container_id': cid, 'spec': spec, 'process': process, 'pidfd': pidfd, 'cgroup_fd': group,
                'groups': ['0::/fixture-' + action], 'payload_end': owner.b._driver_deadline,
                'start_transmitted': True, 'closure': None, 'fixture_only': True,
                'authority_binding': {'operation_id': owner.cfg['operation_id'], 'parent_pid': owner.parent,
                    'parent_start_ticks': owner.ticks, 'journal_operation': owner.j.operation,
                    'inventory_sha256': digest(owner.raw), 'authority_sha256': digest(owner.authraw),
                    'original_absolute_deadline': owner.b._driver_deadline}}
            if action == 'standby':
                self.standby_child = child
                native['observed_mounts'] = sorted(copy.deepcopy(spec['mounts']), key=lambda m: m['destination'])
                native['data_fd'] = os.dup(owner.data_fd); native['wal_fd'] = os.dup(owner.data_fd)
                owner.b._initial_pg_pair_retained.extend([native['data_fd'], native['wal_fd']])
                owner.b._initial_pg_pair_standby_native = native
            else:
                owner.helpers.append(native); owner.b._initial_pg_pair_pending_ownership[cid] = native
                native['fixture_actual_wait'] = child.wait(timeout=5)
                if action == 'backup':
                    fd = os.open('backup_manifest', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=owner.data_fd)
                    try: os.fchown(fd, 70, 70); os.write(fd, b'labelled synthetic pg_basebackup manifest\n'); os.fsync(fd)
                    finally: os.close(fd)
            owner.save('synthetic-' + action + '-native', {'fixture_only': True, 'container_id': cid, 'process': process})
            return cid, native
        def critical_execute(argv, **kwargs):
            # Real _critical_execute builds/hashes fixed argv/stdin and emits
            # native receipts. Replace only its native subprocess SQL output.
            owner = owner_holder[0]; expected = self.expected_binding(owner); stdin = kwargs['stdin'].decode()
            self.assertIn('capture_prefix()', stdin)
            values = [pair_tests.peer(expected), pair_tests.peer(expected, prefix='0/900')]
            if 'bind_replica(' in stdin:
                intents = [row['raw'] for name, row in owner.j.artifacts.items() if name.endswith('-first-bind-intent.json')]
                self.assertEqual(len(intents), 1)
                values.insert(0, {'operation_id': owner.cfg['operation_id'], 'intent_sha256': digest(intents[0]),
                    'previous_binding': None, 'new_binding': expected,
                    'peer_before_commit': pair_tests.peer(expected, prefix='0/90')})
            return p.driver.processes.CommandResult(b''.join(canonical(v) for v in values), b'', 0,
                False, False, True, False, .001, True, False)
        def inspect(owner, cid, spec, *, running):
            self.assertFalse(running)
            return {'State': {'ExitCode': 0, 'Pid': 0}}
        with ExitStack() as stack:
            for item in (patch.object(p._Pair, 'command', command), patch.object(p._Pair, 'sql', sql),
                patch.object(p, '_native_primary', side_effect=native_primary),
                patch.object(p.legacy, '_recheck_native_mounts', return_value=None),
                patch.object(p, '_network', side_effect=lambda owner: owner.seal()),
                patch.object(p, '_tls', side_effect=lambda owner: owner.seal()),
                patch.object(p, '_capacity', side_effect=lambda owner, **kw: owner.seal()),
                patch.object(p, '_copy_size', return_value={'fixture_only': True, 'bytes': 0}),
                patch.object(p, '_copy_watch', side_effect=lambda owner: {'fixture_only': True}),
                patch.object(p, '_slot', side_effect=lambda owner, **kw: owner.seal()),
                patch.object(p, '_start', side_effect=start), patch.object(p, '_inspect', side_effect=inspect),
                patch.object(p, '_layout', side_effect=lambda owner, **kw: owner.seal()),
                patch.object(p, '_configure_standby', side_effect=lambda owner: owner.seal()),
                patch.object(p.driver.processes, 'execute', side_effect=critical_execute)):
                stack.enter_context(item)
            yield

    def run_pair(self):
        self.pair_intake, _ = self.prepare_pair()
        envelope = self.pair_envelope(self.pair_intake)
        with self.pair_native_ports():
            self.pair_pending = p.execute_approved_pair(self.pair_intake, envelope)
        self.owner = self.b._initial_pg_pair_live
        self.pair_complete_ref = p.completed_reference(self.b, self.lock)
        self.result = p.consume_completed_live(self.b, self.lock, self.pair_complete_ref)
        return self.pair_complete_ref
