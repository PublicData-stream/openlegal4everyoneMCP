"""Genuine maintenance history into Pair DATA/approval; no native Pair body.

The fixed run dispatch gate and readonly loaders/live/proof/role qualification
execute. Its _run seam returns an explicit synthetic NONREADY result. All image,
network, trust and target DATA below is operator-supplied fictional fixture DATA;
neither maintenance success nor a JSON reference provisions any resource.
"""
import copy
import json
import os
from pathlib import Path
import sys
import time
import unittest
import uuid
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(BASE), str(Path(__file__).resolve().parent)]
import initial_pg_pair as p
import test_maintenance_completion as history
from interface import Refused
from inventory import canonical, decode, digest


class PairIntake(history.GenuineMaintenance):
    def setUp(self):
        super().setUp(); self.run_maintenance()
        original = p.private_bytes
        fixed = {BASE / 'initial_pg_pair.py', BASE / 'initial_provider_maintenance.py'}
        def source_private(path, sha=None, **kwargs):
            if Path(path) in fixed:
                raw = Path(path).read_bytes(); self.assertEqual(digest(raw), sha); return raw
            return original(path, sha, **kwargs)
        item = patch.object(p, 'private_bytes', side_effect=source_private)
        item.start(); self.addCleanup(item.stop)
        self.pair_source = digest((BASE / 'initial_pg_pair.py').read_bytes())
        self.counter = 0
        parent = self.root / 'pair-target'; parent.mkdir(mode=0o700); st = parent.stat()
        def trust(name):
            ref = self.write('pair-' + name, b'fictional operator TLS reference ' + name.encode())
            os.chown(ref['path'], 70, 70)
            return ref
        names = self.role_cfg['roles']; expected = p.role_prepare.expected_catalog(names, self.role_cfg['runtime_role'])
        self.provision = {'schema_version': 1, 'kind': 'openlegal_initial_pair_preprovision',
            'image_id': 'sha256:' + '3' * 64, 'tools': {name: '4' * 64 for name in p.TOOLS},
            'network': {'id': '5' * 64, 'name': 'internal-fixture', 'subnet': '172.31.64.0/24',
                        'primary_address': '172.31.64.2', 'backup_address': '172.31.64.4', 'standby_address': '172.31.64.3'},
            'peer': {'application_name': 'strict_peer', 'role': names['peer_login'], 'server_name': 'primary.fixture',
                'server_certificate_sha256': '1' * 64, 'client_certificate_sha256': '2' * 64,
                'client_dn': 'CN=fictional-client', 'issuer_dn': 'CN=fictional-ca', 'client_serial': '12345678901234567890',
                **{name: trust('client-' + name) for name in ('ca', 'certificate', 'private_key')}},
            'standby_trust': {name: trust('server-' + name) for name in ('ca', 'certificate', 'private_key', 'hba')},
            'recovery_role': names['recovery_login'],
            'function_owners': {sig: {'name': expected['functions'][sig]['owner'],
                'body_sha256': expected['functions'][sig]['body_sha256']} for sig in p.FUNCTIONS},
            'ordinary_roles': sorted([self.role_cfg['runtime_role'], names['storage_login'], names['canary_login']]),
            'target': {'parent': str(parent), 'identity': {'dev': st.st_dev, 'ino': st.st_ino}, 'kernel_filesystem_id': 1,
                'capacity': {'kind': 'plain_ssd_bounded_copy_v1', 'copy_max_bytes': 500, 'reserve_bytes': 200,
                    'filesystem_total_bytes': 1000, 'max_files': 1000, 'physical_leaves': ['/dev/sda'], 'max_rate_kib': 65536}},
            'primary_slot': {'path': str(self.root / 'postgresql.auto.conf'), 'identity': {'dev': st.st_dev, 'ino': 123},
                'before': self.write('pair-slot-before', b"synchronous_standby_names=''\n"),
                'after': self.write('pair-slot-after', b"synchronous_standby_names='FIRST 1 (strict_peer)'\n")},
            'source_receipts': [self.write('pair-operator-receipt', b'fictional explicit preprovision observations')]}
        # Preserve operator formatting and prove its archived pin is the raw pin.
        self.provision_raw = (json.dumps(self.provision, indent=2) + '\n').encode()
        self.provision_ref = self.write('pair-provision.json', self.provision_raw)
        inbox = self.root / 'pair-inbox'; inbox.mkdir(mode=0o700); st = inbox.stat()
        self.inbox = {'path': str(inbox), 'identity': {'dev': st.st_dev, 'ino': st.st_ino}}
        self.addCleanup(self.cleanup_pair_intake)

    def cleanup_pair_intake(self):
        intake = getattr(self.b, '_initial_pg_pair_intake', None)
        if type(intake) is p._Intake and intake.inbox_fd is not None:
            try: os.close(intake.inbox_fd)
            except OSError: pass

    def prepare(self):
        return p.prepare_pair_request(self.b, self.lock, provision_ref=self.provision_ref,
            source_sha256=self.pair_source, inbox=self.inbox)

    def authority(self, s, *, change=None):
        self.counter += 1
        value = dict(s.scope, approved=True, valid_until=int(time.time()) + 300)
        if change: change(value)
        return self.write('external-pair-authority-%d.json' % self.counter, value)

    def envelope(self, s, *, change=None, raw=None, authority_ref=None):
        value = {'schema_version': 1, 'kind': 'openlegal_initial_stage_capability_envelope',
            'operation_id': self.case.opid, 'stage': 'pg_pair', 'stage_nonce': s.nonce,
            'request_sha256': digest(s.request_raw), 'inventory': copy.deepcopy(s.inventory_ref),
            'authority': authority_ref or self.authority(s)}
        if change: change(value)
        data = raw if raw is not None else canonical(value)
        path = Path(self.inbox['path']) / p.INTAKE_ENVELOPE
        path.write_bytes(data); path.chmod(0o600)
        return {'path': str(path), 'sha256': digest(data)}

    def readonly_dispatch(self, backend, lock, inventory_ref, authority_ref):
        # run has already enforced its actual sticky intake gate. Reproduce the
        # unchanged _run admission and only its genuine readonly qualification.
        self.assertIs(type(backend), p.driver.Backend); self.assertIs(type(lock), p.journals.SharedLock)
        self.assertEqual(os.geteuid(), 0)
        self.assertFalse(any(callable(v) for v in vars(backend).values()))
        self.assertFalse(getattr(backend, '_initial_pg_pair_started', False)); p._check(backend)
        cfg, auth, raw, authraw, provision, praw, sc = p._load(inventory_ref, authority_ref)
        live = p._live(backend, lock, cfg, sc)
        maintenance, audit = p._maintenance(live[0], cfg['maintenance'], cfg, sc[4], sc[8], sc[9],
            getattr(backend, '_initial_provider_maintenance_last_exit', None), sc[7])
        role = p._qualified_role_context(backend, lock, provision, maintenance)
        self.assertEqual(role['reference'], decode(p.driver._reference(maintenance['inventory']))['role_prepare'])
        self.assertIs(live[0], self.j); self.assertEqual(audit['result'], self.applied)
        return {'status': 'SYNTHETIC_DISPATCH_QUALIFIED_NONREADY', 'ready': False,
                'authority_sha256': digest(authraw), 'maintenance': cfg['maintenance']}

    def assert_unknown(self, s):
        self.assertEqual(s.state, 'UNKNOWN'); self.lock.check()
        self.assertEqual(self.b._driver_deadline, self.clock.end)
        self.assertEqual(self.j.phase_paths[0][0].name, p.MAINTENANCE)
        if s.inbox_fd is not None: self.assertEqual(os.fstat(s.inbox_fd).st_ino, self.inbox['identity']['ino'])
        self.assertEqual(p.driver.processes._children(os.getpid()), set())
        with self.assertRaises(Refused): self.prepare()

    def refuse(self, s, ref):
        with patch.object(p, '_run', side_effect=AssertionError('invalid capability dispatched')) as native:
            with self.assertRaises((Refused, OSError)): p.execute_approved_pair(s, ref)
            native.assert_not_called()
        self.assert_unknown(s)

    def test_genuine_completed_history_creates_only_exact_restriction_data(self):
        cap = self.b._initial_provider_maintenance_complete
        with patch.object(p, '_run', side_effect=AssertionError('candidate executed')) as native:
            s, raw = self.prepare()
        native.assert_not_called(); value = decode(raw)
        self.assertEqual(raw, canonical(value)); self.assertEqual(s.state, 'WAITING')
        self.assertEqual(value['status'], 'UNAPPROVED_CANDIDATE_DATA'); self.assertFalse(value['ready'])
        self.assertEqual(value['previous_result'], self.complete_ref)
        self.assertEqual(value['candidate_inventory'], s.inventory_ref)
        self.assertNotIn('approved', s.scope); self.assertNotIn('valid_until', s.scope)
        self.assertEqual(s.cfg, {'schema_version': 1, 'kind': 'openlegal_initial_pg_pair_inventory',
            'operation_id': self.case.opid, 'source_sha256': self.pair_source,
            'seed_inventory': self.seedcase.cfgref, 'seed_authority': self.seedcase.authref,
            'maintenance': self.complete_ref, 'provision': s.cfg['provision']})
        self.assertEqual(s.cfg['provision']['sha256'], self.provision_ref['sha256'])
        self.assertEqual(p.driver._reference(s.cfg['provision']), self.provision_raw)
        self.assertIs(self.b._initial_provider_maintenance_complete, cap)
        self.assertIs(s.clock, self.clock); self.assertEqual(s.original_end, self.clock.end)
        self.assertEqual(self.j.phase_paths[0][0].name, p.MAINTENANCE)
        self.assertFalse(getattr(self.b, '_initial_pg_pair_started', False))
        for root, _ in self.j.phase_paths:
            self.assertEqual((root / s.request_ref['name']).read_bytes(), raw)
            self.assertEqual((root / Path(s.inventory_ref['path']).name).read_bytes(), canonical(s.cfg))
        for field in p._AUTHORITY_TRUE: self.assertIs(s.scope[field], True)
        for field in p._AUTHORITY_FALSE: self.assertIs(s.scope[field], False)
        for ref in self.provision['standby_trust'].values():
            self.assertEqual(Path(ref['path']).stat().st_uid, 70)
            self.assertNotIn(Path(ref['path']).read_bytes(), raw)
            for name, record in self.j.artifacts.items():
                if name.startswith('pair-intake-'): self.assertNotIn(Path(ref['path']).read_bytes(), record['raw'])

    def test_external_exact_authority_passes_common_loader_and_fixed_readonly_dispatch(self):
        s, _ = self.prepare(); authority = self.authority(s)
        self.assertEqual(p._load(s.inventory_ref, authority)[0], s.cfg)
        ref = self.envelope(s, authority_ref=authority)
        with patch.object(p, '_run', side_effect=self.readonly_dispatch) as native:
            result = p.execute_approved_pair(s, ref)
            self.assertEqual(result['status'], 'SYNTHETIC_DISPATCH_QUALIFIED_NONREADY')
            self.assertFalse(result['ready']); self.assertEqual(s.state, 'CONSUMED')
            self.assertTrue(s.entered_run); self.assertIsNotNone(s.finished)
            native.assert_called_once_with(self.b, self.lock, s.inventory_ref, authority)
            with self.assertRaises(Refused): p.execute_approved_pair(s, ref)
            self.assertEqual(native.call_count, 1)
        self.assertEqual(self.j.phase_paths[0][0].name, p.MAINTENANCE)
        self.assertIs(self.b._initial_driver_clock, self.clock)
        self.assertEqual(self.b._driver_deadline, self.clock.end)

    def test_every_authority_permission_and_strict_bool_is_checked_by_full_loader(self):
        s, _ = self.prepare()
        changes = [(key, not s.scope[key]) for key in (*p._AUTHORITY_TRUE, *p._AUTHORITY_FALSE)]
        changes += [(key, int(s.scope[key])) for key in (*p._AUTHORITY_TRUE, *p._AUTHORITY_FALSE)]
        changes += [('approved', 1), ('approved', False), ('schema_version', True),
                    ('valid_until', True), ('valid_until', int(time.time()) - 1),
                    ('operation_id', str(uuid.uuid4())), ('inventory_sha256', '0' * 64),
                    ('provision_sha256', '0' * 64), ('source_sha256', '0' * 64)]
        with patch.object(p, '_run', side_effect=AssertionError('loader executed')):
            for key, value in changes:
                ref = self.authority(s, change=lambda v, k=key, x=value: v.update({k: x}))
                with self.subTest(key=key, value=value), self.assertRaises(Refused): p._load(s.inventory_ref, ref)
        self.assertEqual(s.state, 'WAITING')

    def test_exact_candidate_reference_is_envelope_bound(self):
        s, _ = self.prepare()
        ref = self.envelope(s, change=lambda v: v['inventory'].update(sha256='0' * 64))
        self.refuse(s, ref)

    def test_wrong_nonce_is_sticky_unknown(self):
        s, _ = self.prepare(); self.refuse(s, self.envelope(s, change=lambda v: v.update(stage_nonce=str(uuid.uuid4()))))

    def test_wrong_operation_is_sticky_unknown(self):
        s, _ = self.prepare(); self.refuse(s, self.envelope(s, change=lambda v: v.update(operation_id=str(uuid.uuid4()))))

    def test_wrong_stage_cannot_reuse_maintenance_capability(self):
        s, _ = self.prepare(); self.refuse(s, self.envelope(s, change=lambda v: v.update(stage='provider_maintenance')))

    def test_wrong_request_hash_cannot_reuse_capability(self):
        s, _ = self.prepare(); self.refuse(s, self.envelope(s, change=lambda v: v.update(request_sha256='0' * 64)))

    def test_noncanonical_envelope_is_not_approval(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        value = decode(Path(ref['path']).read_bytes())
        self.refuse(s, self.envelope(s, raw=(json.dumps(value, indent=2) + '\n').encode()))

    def test_duplicate_key_envelope_is_not_approval(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        raw = Path(ref['path']).read_bytes()
        self.refuse(s, self.envelope(s, raw=b'{"schema_version":1,' + raw[1:]))

    def test_unknown_envelope_field_is_sticky_unknown(self):
        s, _ = self.prepare(); self.refuse(s, self.envelope(s, change=lambda v: v.update(command='forbidden')))

    def test_direct_run_bypass_with_pending_or_unknown_intake_never_reaches_native(self):
        s, _ = self.prepare(); authority = self.authority(s)
        with patch.object(p, '_run', side_effect=AssertionError('bypass')) as native:
            with self.assertRaisesRegex(Refused, 'dispatch_required'):
                p.run(self.b, self.lock, s.inventory_ref, authority)
            self.assertEqual(s.state, 'UNKNOWN')
            with self.assertRaises(Refused): p.run(self.b, self.lock, s.inventory_ref, authority)
        native.assert_not_called(); self.assert_unknown(s)

    def test_duplicate_prepare_poisoning_prevents_dispatch(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        with self.assertRaisesRegex(Refused, 'no_replay'): self.prepare()
        self.refuse(s, ref)

    def test_post_finish_expiry_keeps_finish_receipt_and_blocks_dispatch(self):
        s, _ = self.prepare(); ref = self.envelope(s); real = self.j.finish_reservation
        clock_patch = None
        def expire(token):
            nonlocal clock_patch
            value = real(token)
            clock_patch = patch.object(p.time, 'monotonic', return_value=self.clock.end + 1)
            clock_patch.start()
            return value
        try:
            with patch.object(self.j, 'finish_reservation', side_effect=expire), \
                 patch.object(p, '_run', side_effect=AssertionError('expired dispatch')) as native:
                with self.assertRaisesRegex(Refused, 'deadline'): p.execute_approved_pair(s, ref)
                native.assert_not_called()
        finally:
            if clock_patch is not None: clock_patch.stop()
        self.assertIsNotNone(s.finished); self.assertIsNone(self.j._reservation)
        self.assertEqual(s.state, 'UNKNOWN')
        for root, _ in self.j.phase_paths:
            self.assertEqual(digest((root / s.finished['name']).read_bytes()), s.finished['sha256'])
        self.assert_unknown(s)

    def test_mirrored_write_failure_retains_unknown_original_reservation(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        other = self.j.phase_paths[1][0] / 'pair-intake-envelope.json'
        real = p.journals.new_file
        def failed(path, raw):
            if Path(path) == other: raise OSError('synthetic second-copy write failure')
            return real(path, raw)
        with patch.object(p.journals, 'new_file', side_effect=failed): self.refuse(s, ref)
        self.assertIsNone(s.finished); self.assertIs(self.j._reservation, s.budget)
        first = self.j.phase_paths[0][0] / other.name
        self.assertEqual(first.read_bytes(), Path(ref['path']).read_bytes())
        self.assertFalse(other.exists())

    def test_parent_change_blocks_approval(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        with patch.object(p, '_intake_parent', return_value=dict(s.parent, pid=os.getpid() + 1)):
            with patch.object(p, '_run', side_effect=AssertionError('changed parent')) as native:
                with self.assertRaises(Refused): p.execute_approved_pair(s, ref)
                native.assert_not_called()
        self.assert_unknown(s)

    def test_original_prepared_clock_replacement_blocks_approval(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        with patch.object(self.b, '_initial_driver_clock', object()):
            with patch.object(p, '_run', side_effect=AssertionError('changed clock')) as native:
                with self.assertRaises(Refused): p.execute_approved_pair(s, ref)
                native.assert_not_called()
        self.assert_unknown(s)

    def test_bool_retained_exit_is_rejected_by_both_owner_and_pair_proof(self):
        s, _ = self.prepare(); ref = self.envelope(s); sc = s.loaded[4]
        invalid = dict(self.b._initial_provider_maintenance_last_exit, actual_exit_code=False)
        # The legacy readonly proof validator independently rejects Python's
        # bool/int equality even when supplied a genuinely archived int-zero row.
        with self.assertRaises(Refused):
            p._maintenance(self.j, self.complete_ref, s.cfg, sc[4], sc[8], sc[9], invalid, sc[7])
        with patch.object(self.b, '_initial_provider_maintenance_last_exit', invalid): self.refuse(s, ref)

    def test_operator_provision_mutation_after_request_blocks_approval(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        path = Path(self.provision_ref['path']); path.write_bytes(self.provision_raw + b' ')
        self.refuse(s, ref)

    def test_missing_completion_capability_never_generates_candidate_or_authority(self):
        del self.b._initial_provider_maintenance_complete
        with patch.object(p, '_run', side_effect=AssertionError('missing completion')) as native:
            with self.assertRaises(Refused): self.prepare()
            native.assert_not_called()
        s = self.b._initial_pg_pair_intake
        self.assertEqual(s.state, 'UNKNOWN'); self.assertIsNone(s.budget); self.assertIsNone(s.inbox_fd)
        self.assertFalse(any(name.startswith('pair-intake-') for name in self.j.artifacts))


if __name__ == '__main__':
    unittest.main()
