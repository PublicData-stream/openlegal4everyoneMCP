"""Harmless real CP/private-file tests; Docker, SQL and native qualification synthetic.

The real temporary journals share a filesystem deliberately. Catalog/native
qualification is replaced by source-class test fixtures. Capture executes an
owned harmless Python fixture through the actual retained-executable CP port.
No PostgreSQL, Docker, runtime daemon, provider or production action is run.
"""
import copy
from dataclasses import replace
from contextlib import ExitStack
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
import types
import unittest
import uuid
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(BASE))
import epoch_primary_fence as f
from inventory import canonical, decode, digest
from interface import Refused, Operation, freeze_authority


def catalog(names, runtime):
    e = f.roles.expected_catalog(names, runtime); funcs = {}
    for sig, row in e['functions'].items():
        acl = [[name, 'EXECUTE', False] for name, grants in e['grants'].items()
               if ['function', sig, 'EXECUTE'] in grants]
        if sig in f.roles.RUNTIME_EXECUTE: acl.append([runtime, 'EXECUTE', False])
        if sig == 'openlegal.replication_required()': acl.append(['PUBLIC', 'EXECUTE', False])
        funcs[sig] = dict(row, acl=sorted(acl), definer=True,
            language='sql' if sig in f.roles.SQL_FUNCTIONS else 'plpgsql')
    effective = {'functions': [], 'tables': [], 'columns': [], 'schemas': [], 'create': False,
        'membership_paths': sorted([[m, g, True, True, False] for m, g, _, _, _ in e['memberships']]),
        'admin_edges': []}
    for name, grants in e['grants'].items():
        for kind, obj, priv in grants:
            if kind == 'function' and obj != 'openlegal.replication_required()': effective['functions'].append([name, obj])
            elif kind in ('table', 'column'): effective['tables' if kind == 'table' else 'columns'].append([name, obj, priv])
            elif kind == 'schema' and obj != 'public': effective['schemas'].append([name, obj])
    for key in ('functions', 'tables', 'columns', 'schemas'): effective[key].sort()
    return {'session_user': names['recovery_login'], 'current_user': names['recovery_login'],
        'roles': copy.deepcopy(e['flags']), 'grants': sorted([n, *r, False] for n, rows in e['grants'].items() for r in rows),
        'memberships': e['memberships'], 'functions': funcs, 'other_owned': False, 'effective': effective,
        'runtime': {'exists': True, 'admin_usage': True, 'new_usage': True, 'executes': [True] * 3}}


@unittest.skipUnless(os.geteuid() == 0, 'private root/UID70 fixture metadata')
class FenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='epoch-fence-harmless-', dir='/tmp')
        self.root = Path(self.tmp.name); self.root.chmod(0o700); self.stack = ExitStack()
        self.modules = {'epoch_primary_fence.py': f, 'epoch_observation.py': f.observation,
            'initial_pg_pair.py': f.pair, 'initial_role_prepare.py': f.roles, 'command_process.py': f.processes,
            'interface.py': f.interface, 'inventory.py': f.inventory, 'linux_backend.py': f.linux,
            'initial_driver.py': f.driver}
        stage = self.root / 'private-source'; stage.mkdir(mode=0o700); self.sources = {}
        # Exact current bytes, with private installed-source metadata. __file__
        # locations are patched only in this synthetic installation fixture.
        for name, module in self.modules.items():
            raw = Path(module.__file__).read_bytes(); path = stage / name; path.write_bytes(raw); path.chmod(0o600)
            self.sources[name] = {'path': str(path), 'sha256': digest(raw)}
            self.stack.enter_context(patch.object(module, '__file__', str(path)))
        self.operation = str(uuid.uuid4()); self.backend = f.linux.LinuxBackend()
        self.common = f.observation.NativeObservation(self.backend); self.end = time.monotonic() + 60
        self.config = Path('/tmp/openlegal-storage-docker-client-' + self.operation); self.config.mkdir(mode=0o700)
        st = self.config.stat(); self.backend._docker_config = (self.config, (st.st_dev, st.st_ino))
        self.data = self.root / 'data'; self.data.mkdir(mode=0o700); (self.data / 'pg_wal').mkdir(mode=0o700)
        for path in (self.data, self.data / 'pg_wal'): os.chown(path, 70, 70)
        self.names = {k: 'fixture_' + k for k in f.roles.ROLE_KEYS}; self.runtime = 'fixture_runtime'
        self.expected = {'managed_epoch': str(uuid.uuid4()), 'primary_system_identifier': '12345678901234567890',
            'timeline': 1, 'standby_name': 'fixture_peer', 'standby_role': self.names['peer_login'],
            'standby_addr': '172.31.64.3', 'standby_client_dn': 'CN=fictional', 'standby_issuer_dn': 'CN=fictional-ca',
            'standby_client_serial': '123456789012345678901234567890123456789012345678901234567890',
            'blob_binding_sha256': 'a' * 64}
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        fd = f.observation._directory(self.data)
        try: desc = f.observation._descriptor(self.data, fd)
        finally: os.close(fd)
        self.subject = {'managed_epoch': self.expected['managed_epoch'], 'host_boot_id': boot,
            'control': self.expected, 'primary': {'container_id': '1' * 64, 'image_id': 'sha256:' + '2' * 64,
                'process': {'pid': os.getpid(), 'start_ticks': f.processes._identity(os.getpid()), 'boot_id': boot},
                'postmaster_started_at': '2026-11-02 00:00:00+00', 'data_directory': str(self.data), 'data_root': desc}}
        self.output = self.root / 'proof-output'; self.flush_output = self.root / 'flush-output'; self.exit_file = self.root / 'exit-code'
        self.binary = self.root / 'explicitly-synthetic-docker'
        self.binary.write_text('#!/usr/bin/python3 -IB\nimport pathlib,sys\ns=sys.stdin.buffer.read()\n'
            'p=' + repr(str(self.output)) + ' if b"capture_prefix" in s else ' + repr(str(self.flush_output)) + '\n'
            'sys.stdout.buffer.write(pathlib.Path(p).read_bytes())\nsys.exit(int(pathlib.Path(' + repr(str(self.exit_file)) + ').read_text()))\n')
        self.binary.chmod(0o755); self.exit_file.write_text('0')
        self.graph = {name: ref['sha256'] for name, ref in self.sources.items()}
        self.graph_sha = digest(canonical(self.graph))
        self.inv = {'managed_epoch': self.expected['managed_epoch'], 'host': {'boot_id': boot},
            'source_graph': self.graph,
            'linux': {'postgres': {'database': 'openlegal', 'uid': 70, 'admin_user': self.names['recovery_login'],
                'binding': self.expected,
                'primary': {'container_id': '1' * 64, 'image_id': 'sha256:' + '2' * 64}},
                'binaries': {'docker': {'path': str(self.binary), 'sha256': digest(self.binary.read_bytes())}}}}
        invref = self.ref('inventory.json', self.inv)
        auth = {'approved': True, 'operation_id': self.operation, 'inventory_sha256': invref['sha256'],
            'source_graph_sha256': self.graph_sha,
            'valid_until': int(time.time()) + 120, 'epoch_certificate_observation_permitted': True,
            'source_members': ['actual', 'tuple-freeze-regression']}
        authref = self.ref('authority.json', auth)
        self.envelope = {'sources': self.sources, 'inventory': invref, 'authority': authref}
        self.j = self.journal(); self.op = Operation(self.operation, 'strict_prepare', 'initial_epoch_observation',
            self.inv, invref['sha256'], self.graph_sha, authref['sha256'], self.j, self.root, {}, self.j.check, freeze_authority(auth))
        self.calls = []; self.catalog = catalog(self.names, self.runtime); self.closure = {'sessions': []}
        def native(common, op, subject, deadline, *, which='standby'):
            self.assertEqual(which, 'primary')
            key = (op.operation_id, subject['primary']['container_id'])
            row = {'container_id': '1' * 64, 'image_id': 'sha256:' + '2' * 64,
                'data_directory': str(self.data), 'data_root': desc}
            if key not in common.anchors:
                fds = [f.observation._directory(path) for path in (self.data, self.data / 'pg_wal', self.data, self.data / 'pg_wal')]
                common.anchors[key] = {'fds': fds, 'record': row, 'identities': [f.observation._fd_identity(fd) for fd in fds]}
            return row
        def run(common, op, binary, args, deadline, **kw):
            self.calls.append((op.phase, args, kw['stdin'], deadline))
            value = self.catalog if b'nonpublic' in kw['stdin'] or b'role_effective' in kw['stdin'] or b'other_owned' in kw['stdin'] else {
                'session_user': self.names['recovery_login'], 'current_user': self.names['recovery_login'], 'result': self.closure}
            raw = canonical(value); self.backend._sequence += 1
            name = op.phase + '-command-%04d.json' % self.backend._sequence
            dispatch = ['--config', str(self.config), '--host', 'unix:///var/run/docker.sock']
            receipt = {'binary': 'docker', 'arguments_sha256': digest(canonical(dispatch + args)),
                'stdin_sha256': digest(kw['stdin']), 'stdout_sha256': digest(raw), 'stdout_hex': raw.hex(),
                'stdout_bytes': len(raw), 'actual_exit_code': 0, 'started': True, 'owned_processes_closed': True,
                **dict.fromkeys(('expired', 'interrupted', 'unexpected_descendants', 'output_limit_exceeded'), False)}
            op.journal.save_bytes(name, canonical(receipt)); return raw
        self.stack.enter_context(patch.object(f.observation.NativeObservation, '_native', native))
        self.stack.enter_context(patch.object(f.observation.NativeObservation, '_run', run))
        secure = f.linux.secure_path
        self.stack.enter_context(patch.object(f.linux, 'secure_path', side_effect=lambda path, **kw:
            self.binary.stat() if path == str(self.binary) else secure(path, **kw)))
        self.set_output()

    def tearDown(self):
        for handle in getattr(self.backend, '_epoch_primary_fence_handles', []):
            for fd in handle.fds:
                try: os.close(fd)
                except OSError: pass
        self.common.close(); self.stack.close(); shutil.rmtree(self.config); self.tmp.cleanup()

    def ref(self, name, value):
        path = self.root / name; raw = canonical(value); path.write_bytes(raw); path.chmod(0o600)
        return {'path': str(path), 'sha256': digest(raw)}

    def journal(self):
        # Actual DualJournal methods, explicitly synthetic same-device roots.
        j = f.driver.journals.DualJournal.__new__(f.driver.journals.DualJournal)
        j.operation = self.operation; j.binding = {'operation_id': self.operation}; j.base = []; j.paths = []
        j.phase_paths = None; j.artifacts = {}; j.history = []; j.phase_names = set(); j.count = j.bytes = 0
        for name in ('media-a-synthetic', 'media-b-synthetic'):
            root = self.root / name; root.mkdir(mode=0o700); op = root / self.operation; op.mkdir(mode=0o700)
            j.base.append((root, f.driver.journals.directory(root))); j.paths.append((op, f.driver.journals.directory(op)))
        j.save('operation.json', j.binding); j.begin('initial_epoch_observation'); return j

    def peer(self, prefix='0/100', flush='0/100'):
        return dict(self.expected, postmaster_started_at=self.subject['primary']['postmaster_started_at'],
            sender_pid=1234, sender_started_at='2026-11-02 00:00:01+00', prefix_lsn=prefix, flush_lsn=flush)

    def wrapped(self, peer):
        return {'session_user': self.names['recovery_login'], 'current_user': self.names['recovery_login'],
            'read_only': 'on', 'result': peer}

    def set_output(self, before=None, after=None, flush=None):
        self.output.write_bytes(canonical(self.wrapped(before or self.peer())) + canonical(self.wrapped(after or self.peer(prefix='0/200'))))
        self.flush_output.write_bytes(canonical(self.wrapped(flush or self.peer(prefix='0/999'))))

    def prepare(self, **overrides):
        args = dict(backend=self.backend, op=self.op, common=self.common, subject=self.subject,
            expected=self.expected, role_names=self.names, runtime_role=self.runtime, original_end=self.end,
            dependency_pins=self.envelope); args.update(overrides)
        return f.prepare_primary_fence(**args)

    def graph_bound_operation(self, graph, *, auth_graph=None):
        inv = copy.deepcopy(self.inv); inv['source_graph'] = copy.deepcopy(graph)
        invref = self.ref('graph-inventory.json', inv); graph_sha = digest(canonical(graph))
        auth = decode(Path(self.envelope['authority']['path']).read_bytes())
        auth.update(inventory_sha256=invref['sha256'], source_graph_sha256=auth_graph or graph_sha)
        authref = self.ref('graph-authority.json', auth)
        op = replace(self.op, inventory=inv, inventory_sha256=invref['sha256'],
            source_graph_sha256=graph_sha, authority_sha256=authref['sha256'], authority=freeze_authority(auth))
        return op, dict(self.envelope, inventory=invref, authority=authref)

    def test_exact_types_and_source_registry_refuse_before_native_dispatch(self):
        for backend in (object(), types.SimpleNamespace()):
            with self.assertRaisesRegex(Refused, 'fixed_backend'): self.prepare(backend=backend)
        bad = copy.deepcopy(self.envelope); bad['sources']['extra.sql'] = bad['inventory']
        with self.assertRaisesRegex(Refused, 'source_registry'): self.prepare(dependency_pins=bad)
        bad = copy.deepcopy(self.envelope); bad['sql'] = 'COMMIT'
        with self.assertRaisesRegex(Refused, 'dependency_envelope'): self.prepare(dependency_pins=bad)
        bad = copy.deepcopy(self.envelope); bad['sources'].pop('epoch_primary_fence.py')
        with self.assertRaisesRegex(Refused, 'source_registry'): self.prepare(dependency_pins=bad)
        with self.assertRaisesRegex(Refused, 'fixed_operation'): self.prepare(op=replace(self.op, phase='initial_pg_pair'))
        self.assertEqual(self.calls, [])

    def test_raw_inventory_authority_and_named_role_are_exact(self):
        with self.assertRaisesRegex(Refused, 'raw_operation_binding'):
            self.prepare(op=replace(self.op, inventory_sha256='f' * 64))
        with self.assertRaisesRegex(Refused, 'fixed_observer'):
            self.prepare(role_names=dict(self.names, recovery_login='another_login'))
        bad = copy.deepcopy(self.expected); bad['managed_epoch'] = str(uuid.uuid4())
        with self.assertRaisesRegex(Refused, 'subject_binding'): self.prepare(expected=bad)
        self.assertEqual(self.calls, [])

    def test_native_catalog_rejects_column_escalation(self):
        self.catalog['effective']['columns'].append([self.names['recovery_login'], 'openlegal_admin.provider_control.recovery_hold', 'UPDATE'])
        with self.assertRaises(Refused): self.prepare()
        self.assertTrue(self.backend._epoch_primary_fence_unknown)

    def test_actual_authority_permission_cannot_be_inferred_from_other_flags(self):
        auth = decode(Path(self.envelope['authority']['path']).read_bytes())
        auth['epoch_certificate_observation_permitted'] = False
        ref = self.ref('denied-authority.json', auth); envelope = dict(self.envelope, authority=ref)
        op = replace(self.op, authority_sha256=ref['sha256'], authority=freeze_authority(auth))
        with self.assertRaisesRegex(Refused, 'observation_authority'): self.prepare(op=op, dependency_pins=envelope)
        self.assertEqual(self.calls, [])

    def test_unapproved_raw_authority_refuses_before_any_native_observation(self):
        for label, value in (('false', False), ('missing', None), ('integer', 1)):
            with self.subTest(approval=label):
                auth = decode(Path(self.envelope['authority']['path']).read_bytes())
                if value is None: auth.pop('approved')
                else: auth['approved'] = value
                ref = self.ref('unapproved-' + label + '-authority.json', auth)
                envelope = dict(self.envelope, authority=ref)
                op = replace(self.op, authority_sha256=ref['sha256'], authority=freeze_authority(auth))
                with patch.object(f.observation.NativeObservation, '_native') as native:
                    with self.assertRaisesRegex(Refused, 'observation_authority'):
                        self.prepare(op=op, dependency_pins=envelope)
                    native.assert_not_called()
                self.assertEqual(self.calls, [])

    def test_global_graph_hash_and_authority_graph_are_bound_to_actual_raw_data(self):
        with self.assertRaisesRegex(Refused, 'source_graph_binding'):
            self.prepare(op=replace(self.op, source_graph_sha256='f' * 64))
        op, envelope = self.graph_bound_operation(self.graph, auth_graph='f' * 64)
        with self.assertRaisesRegex(Refused, 'source_graph_binding'):
            self.prepare(op=op, dependency_pins=envelope)
        self.assertEqual(self.calls, [])

    def test_substituted_critical_pin_refuses_even_when_private_file_hash_is_valid(self):
        envelope = copy.deepcopy(self.envelope); ref = envelope['sources']['initial_pg_pair.py']
        substituted = b'# explicitly synthetic substituted critical source\n'
        Path(ref['path']).write_bytes(substituted); ref['sha256'] = digest(substituted)
        self.assertEqual(f.private_bytes(ref['path'], ref['sha256']), substituted)
        with self.assertRaisesRegex(Refused, 'critical_source_graph_binding'):
            self.prepare(dependency_pins=envelope)
        self.assertEqual(self.calls, [])

    def test_authorized_graph_cannot_omit_a_critical_source(self):
        graph = dict(self.graph); del graph['epoch_primary_fence.py']
        op, envelope = self.graph_bound_operation(graph)
        with self.assertRaisesRegex(Refused, 'global_source_graph'):
            self.prepare(op=op, dependency_pins=envelope)
        self.assertEqual(self.calls, [])

    def test_extra_global_dependency_is_checked_by_closed_caller_seal_without_dispatch(self):
        raw = b'# harmless transitive source fixture\n'; path = self.root / 'transitive_fixture.py'
        path.write_bytes(raw); path.chmod(0o600)
        graph = dict(self.graph, transitive_fixture_py=digest(raw))
        # Global graph names belong to the caller's closed loader, not this
        # port's exact nine-file envelope. This source-only fixture validates
        # the whole declared graph and never executes the transitive source.
        op, envelope = self.graph_bound_operation(graph); checked = []
        def seal():
            self.assertEqual(set(op.inventory['source_graph']), set(f.SOURCES) | {'transitive_fixture_py'})
            for name, ref in envelope['sources'].items():
                f.private_bytes(ref['path'], op.inventory['source_graph'][name])
            f.private_bytes(path, op.inventory['source_graph']['transitive_fixture_py'])
            checked.append(True); self.j.check()
        p = self.prepare(op=replace(op, seal=seal), dependency_pins=envelope)
        self.assertTrue(checked); self.assertEqual(p.op.source_graph_sha256, digest(canonical(graph)))
        self.assertEqual(len(envelope['sources']), 9)

    def test_actual_cp_zero_and_archive_are_historical_in_genuine_new_phase(self):
        prepared = self.prepare(); before_count = self.j.count
        window = f.capture_primary_fence(prepared)
        self.assertEqual(self.j.count, before_count)  # no journal operation in native clock
        self.assertEqual(window.proof['fixed_target'], '0/100'); self.assertEqual(len(window.records), 1)
        self.assertEqual(window.records[0]['actual_exit_code'], 0); self.assertTrue(window.records[0]['owned_processes_closed'])
        altered = window.records; altered[0]['actual_exit_code'] = 7
        self.assertEqual(window.records[0]['actual_exit_code'], 0)
        with self.assertRaises(AttributeError): window.failure = None
        self.assertLess(window.finished, window.deadline); self.assertLessEqual(window.deadline, window.started + 2)
        self.j.begin('initial_epoch_install_verify'); archive_op = replace(self.op, phase='initial_epoch_install_verify')
        ref = f.archive_primary_fence(archive_op, window); value = decode(self.j.read_phase_bytes(ref['phase'], ref['name'], ref['sha256']))
        self.assertEqual(self.op.phase, 'initial_epoch_observation'); self.assertEqual(value['archive_phase'], archive_op.phase)
        self.assertEqual(value['native_phase'], self.op.phase); self.assertFalse(value['ready']); self.assertFalse(value['public_ack'])
        self.assertIn('historical', value['observation_status']); self.assertEqual(prepared.state, 'ARCHIVED')
        self.assertEqual(value['native_finished_at'], window.finished)
        self.assertEqual(self.calls[-1][0], archive_op.phase); self.assertIn(b'client_sessions()', self.calls[-1][2])
        with self.assertRaisesRegex(Refused, 'capture_once'): f.capture_primary_fence(prepared)

    def test_actual_exit_seven_is_unknown_retained_and_never_recaptured(self):
        p = self.prepare(); self.exit_file.write_text('7')
        with self.assertRaises(f.FenceUnknown) as caught: f.capture_primary_fence(p)
        self.assertEqual(caught.exception.window.records[0]['actual_exit_code'], 7)
        self.assertEqual(p.state, 'UNKNOWN'); self.assertTrue(all(os.fstat(fd) for fd in p.fds))
        with self.assertRaisesRegex(Refused, 'capture_once'): f.capture_primary_fence(p)
        with self.assertRaisesRegex(Refused, 'held_unknown'): self.prepare()
        with self.assertRaises(f.FenceUnknown): f.archive_primary_fence(self.op, caught.exception.window)
        self.assertTrue(any('historical' in name for name in self.j.artifacts))

    def test_poll_keeps_one_deadline_and_frozen_prefix_without_recommit(self):
        p = self.prepare(); self.set_output(after=self.peer(flush='0/FF'))
        window = f.capture_primary_fence(p)
        self.assertEqual(window.proof['fixed_target'], '0/100'); self.assertEqual(len(window.records), 2)
        self.assertEqual(window.records[0]['native_absolute_deadline'], window.records[1]['native_absolute_deadline'])
        self.assertEqual(window.records[1]['query_kind'], 'flush')
        for kind in ('proof', 'flush'):
            sql = f._capture_sql(kind); self.assertIn(b'BEGIN READ ONLY', sql); self.assertNotIn(b'bind_replica', sql)
            self.assertNotIn(b'COMMIT', sql)
            self.assertIn(b"DateStyle='ISO,YMD'", sql)
        with self.assertRaises(Refused): f._capture_sql('bind')

    def test_changed_peer_refuses(self):
        p = self.prepare(); bad = self.peer(); bad['sender_pid'] += 1; self.set_output(after=bad)
        with self.assertRaises(f.FenceUnknown): f.capture_primary_fence(p)

    def test_changed_source_refuses_before_cp_and_holds_descriptors(self):
        p = self.prepare(); Path(self.sources['initial_pg_pair.py']['path']).write_bytes(b'changed source\n')
        with patch.object(f.processes, 'execute', side_effect=AssertionError('no native dispatch after source change')):
            with self.assertRaises(f.FenceUnknown): f.capture_primary_fence(p)
        self.assertEqual(p.state, 'UNKNOWN')

    def test_private_context_cannot_mutate_the_prepared_expected_role_set(self):
        p = self.prepare(); p.role_names['binding_owner'] = 'another_owner'
        with patch.object(f.processes, 'execute', side_effect=AssertionError('no SQL with changed role context')):
            with self.assertRaises(f.FenceUnknown): f.capture_primary_fence(p)
        self.assertEqual(p.state, 'UNKNOWN')

    def test_unclosed_command_is_never_validated_as_zero(self):
        p = self.prepare()
        result = f.processes.CommandResult(b'', b'', 0, False, False, False, False, .001, True, False)
        with patch.object(f.processes, 'execute', return_value=result):
            with self.assertRaises(f.FenceUnknown): f.capture_primary_fence(p)
        self.assertIs(p.window.records[0]['owned_processes_closed'], False)

    def test_actual_zero_arriving_after_native_deadline_is_unknown(self):
        p = self.prepare(); real_execute = f.processes.execute; late = []
        clock = types.SimpleNamespace(monotonic=lambda: time.monotonic() + (3 if late else 0),
            time=time.time, sleep=time.sleep)
        def completed_too_late(*args, **kwargs):
            result = real_execute(*args, **kwargs); late.append(True); return result
        with patch.object(f, 'time', clock), patch.object(f.processes, 'execute', side_effect=completed_too_late):
            with self.assertRaises(f.FenceUnknown): f.capture_primary_fence(p)
        row = p.window.records[0]
        self.assertEqual(row['actual_exit_code'], 0)
        self.assertGreater(row['native_finished_at'], row['native_absolute_deadline'])

    def test_rebound_native_directory_refuses_before_command_dispatch(self):
        p = self.prepare(); self.data.rename(self.root / 'old-retained-data')
        self.data.mkdir(mode=0o700); (self.data / 'pg_wal').mkdir(mode=0o700)
        for path in (self.data, self.data / 'pg_wal'): os.chown(path, 70, 70)
        with patch.object(f.processes, 'execute', side_effect=AssertionError('no SQL after native rebind')):
            with self.assertRaises(f.FenceUnknown): f.capture_primary_fence(p)
        self.assertEqual(p.state, 'UNKNOWN')

    def test_capture_never_calls_caller_seal_inside_native_clock(self):
        holder = {}; original = self.op.seal
        def seal():
            if holder and holder['prepared'].state == 'CAPTURING': self.fail('journal-walking seal inside native2s')
            original()
        p = self.prepare(op=replace(self.op, seal=seal)); holder['prepared'] = p
        self.assertIsNotNone(f.capture_primary_fence(p).proof)

    def test_private_docker_context_change_refuses_before_native_sql(self):
        p = self.prepare(); (self.config / 'injected-config.json').write_bytes(b'{}')
        with patch.object(f.processes, 'execute', side_effect=AssertionError('no SQL with changed context')):
            with self.assertRaises(f.FenceUnknown): f.capture_primary_fence(p)

    def test_archive_unknown_sessions_never_complete_or_recapture(self):
        p = self.prepare(); window = f.capture_primary_fence(p); self.closure = {'sessions': [{'pid': 123}]}
        with self.assertRaisesRegex(Refused, 'global_sql_not_closed'): f.archive_primary_fence(self.op, window)
        self.assertTrue(self.backend._epoch_primary_fence_unknown)
        with self.assertRaisesRegex(Refused, 'archive_once'): f.archive_primary_fence(self.op, window)

    def test_plain_success_data_cannot_be_promoted_to_a_native_window(self):
        with self.assertRaisesRegex(Refused, 'archive_once'):
            f.archive_primary_fence(self.op, {'actual_exit_code': 0, 'native_proof_valid': True})
        self.assertEqual(self.calls, [])

    def test_an_archived_operation_cannot_renew_its_original_end(self):
        p = self.prepare(); f.archive_primary_fence(self.op, f.capture_primary_fence(p)); calls = len(self.calls)
        with self.assertRaisesRegex(Refused, 'deadline_renewal'): self.prepare(original_end=self.end + 60)
        self.assertEqual(len(self.calls), calls)

    def test_archive_expiry_permits_no_new_command_and_keeps_unknown_hold(self):
        p = self.prepare(); window = f.capture_primary_fence(p); before = len(self.calls)
        clock = types.SimpleNamespace(monotonic=lambda: time.monotonic() + 120, time=time.time, sleep=time.sleep)
        with patch.object(f, 'time', clock):
            with self.assertRaisesRegex(Refused, 'original_deadline'): f.archive_primary_fence(self.op, window)
        self.assertEqual(len(self.calls), before); self.assertEqual(p.state, 'UNKNOWN')
        self.assertTrue(all(os.fstat(fd) for fd in p.fds))

    def test_global_budget_and_original_expiry_do_not_create_a_fresh_window(self):
        self.j.count = 4090
        with self.assertRaisesRegex(Refused, 'global_journal_budget'): self.prepare()
        self.j.count = 2
        with self.assertRaisesRegex(Refused, 'original_deadline'): self.prepare(original_end=time.monotonic() - 1)

    def test_driver_original_end_caps_capture(self):
        self.backend = f.driver.Backend(); self.backend._docker_config = (self.config, (self.config.stat().st_dev, self.config.stat().st_ino))
        self.backend._driver_deadline = time.monotonic() + 30; self.common.backend = self.backend
        p = self.prepare(); self.assertEqual(p.original_end, self.backend._driver_deadline)
        window = f.capture_primary_fence(p); self.assertLessEqual(window.deadline, self.backend._driver_deadline)


if __name__ == '__main__': unittest.main()
