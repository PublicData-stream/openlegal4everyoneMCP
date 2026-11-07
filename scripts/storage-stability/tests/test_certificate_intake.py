"""Actual Pair owner history into certificate approval DATA, never native install.

Driver/seed/role/maintenance/Pair owners and their dual journal proofs execute.
The certificate fixed dispatch stops after actual readonly loaders and owner
qualification. Submitted preprovision DATA does not prove native configuration,
HDD qualification, enrollment, registration, Ready, or deployment acceptance.
"""
import copy
import os
from pathlib import Path
import sys
import time
import unittest
import uuid
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(BASE), str(Path(__file__).resolve().parent)]
import initial_epoch_certificate as e
import pair_history_fixture as history
from interface import Refused
from inventory import canonical, decode, digest


@unittest.skipUnless(os.geteuid() == 0, 'actual private root files and producer children')
class CertificateIntake(history.GenuinePair):
    def setUp(self):
        super().setUp(); self.run_pair(); self.counter = 0
        original = e.private_bytes
        fixed = {BASE / name for name in e.SOURCES}
        def sources(path, sha=None, **kwargs):
            if Path(path) in fixed:
                raw = Path(path).read_bytes()
                if sha is not None: self.assertEqual(digest(raw), sha)
                return raw
            return original(path, sha, **kwargs)
        item = patch.object(e, 'private_bytes', side_effect=sources)
        item.start(); self.addCleanup(item.stop)
        self.certificate_source = digest((BASE / 'initial_epoch_certificate.py').read_bytes())
        images = {'serving': 'ghcr.io/publicdata-stream/openlegal-server@sha256:' + '1' * 64,
            'ingestion': 'ghcr.io/publicdata-stream/openlegal-server-ingestion@sha256:' + '1' * 64,
            'document_worker': 'ghcr.io/publicdata-stream/openlegal-document-worker@sha256:' + '1' * 64}
        serving = {'deployment_name': 'fixture-serving', 'configmap_name': 'fixture-serving-config',
            'configmap_uid': str(uuid.uuid4()), 'toml_key': 'server.toml',
            'toml_source': self.write('certificate-serving.toml', b'fictional preprovision TOML DATA\n')}
        ingestion = {'deployment_name': 'fixture-scheduler', 'configmap_name': 'fixture-ingestion-config',
            'configmap_uid': str(uuid.uuid4()), 'toml_key': 'server.toml',
            'toml_source': self.write('certificate-ingestion.toml', b'fictional preprovision TOML DATA\n'),
            'collection_template_key': 'collection-job.json',
            'collection_template_source': self.write('certificate-job.json', b'fictional collection template DATA\n')}
        profile = {'schema_version': 1, 'kind': 'openlegal_initial_epoch_strict_configmap_pvc_v1',
            'serving': serving, 'ingestion': ingestion, 'images': images}
        roots = []
        for label, (path, _) in zip(('hdd', 'ssd'), self.j.base):
            fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
            try: descriptor = e.certificates.root_descriptor(fd)
            finally: os.close(fd)
            roots.append({'label': label, 'path': str(path), 'descriptor': descriptor,
                'media': ({'kind': 'hdd', 'dataset': 'fixturepool/control', 'guid': '123', 'mountpoint': str(path)}
                          if label == 'hdd' else {'kind': 'ssd', 'physical_leaves': ['/dev/fixture_ssd']})})
        active = {key: copy.deepcopy(self.owner.work['linux'][key]) for key in e._ACTIVE_KEYS}
        active.update(serving_image=images['serving'], serving_runtime_image_id='sha256:' + '1' * 64,
            serving_service={'name': 'fixture-service', 'uid': str(uuid.uuid4())})
        self.certificate_provision = {'schema_version': 1, 'kind': 'openlegal_initial_certificate_preprovision',
            'operation_id': self.case.opid, 'revision': 'a' * 40, 'linux': active,
            'strict_preprovision': self.write('certificate-strict.json', profile), 'epoch_roots': roots}
        self.certificate_provision_ref = self.write('certificate-provision.json', self.certificate_provision)
        inbox = self.root / 'certificate-inbox'; inbox.mkdir(mode=0o700); st = inbox.stat()
        self.certificate_inbox = {'path': str(inbox), 'identity': {'dev': st.st_dev, 'ino': st.st_ino}}
        self.addCleanup(self.cleanup_certificate_inbox)

    def cleanup_certificate_inbox(self):
        s = getattr(self.b, '_initial_epoch_certificate_intake', None)
        if type(s) is e._Intake and s.inbox_fd is not None:
            try: os.close(s.inbox_fd)
            except OSError: pass

    def prepare(self):
        return e.prepare_certificate_request(self.b, self.lock, preprovision_ref=self.certificate_provision_ref,
            source_sha256=self.certificate_source, inbox=self.certificate_inbox)

    def authority(self, s, change=None):
        self.counter += 1
        value = dict(s.scope, approved=True, valid_until=int(time.time()) + 300)
        if change: change(value)
        return self.write('certificate-external-authority-%d.json' % self.counter, value)

    def envelope(self, s, change=None, authority=None):
        value = {'schema_version': 1, 'kind': 'openlegal_initial_stage_capability_envelope',
            'operation_id': self.case.opid, 'stage': 'initial_epoch_certificate', 'stage_nonce': s.nonce,
            'request_sha256': digest(s.request_raw), 'inventory': copy.deepcopy(s.inventory_ref),
            'authority': authority or self.authority(s)}
        if change: change(value)
        raw = canonical(value); path = Path(self.certificate_inbox['path']) / e.INTAKE_ENVELOPE
        path.write_bytes(raw); path.chmod(0o600)
        return {'path': str(path), 'sha256': digest(raw)}

    def readonly_dispatch(self, b, lock, inventory_ref, authority_ref):
        e._dedicated_root(b, lock); e._intake_run_gate(b, lock, inventory_ref, authority_ref)
        cfg, auth, raw, authraw = e._load(inventory_ref, authority_ref)
        owner, result = e._consume_pair(b, lock, cfg); e._active(cfg, owner, result)
        self.assertIs(owner, self.owner); self.assertEqual(result, self.result)
        self.assertEqual(digest(raw), auth['inventory_sha256'])
        return {'status': 'SYNTHETIC_READONLY_CERTIFICATE_DISPATCH_NONREADY', 'ready': False}

    def refuse(self, s, ref):
        with patch.object(e, 'run', side_effect=AssertionError('unapproved native dispatch')) as dispatch:
            with self.assertRaises((Refused, OSError)): e.execute_approved_certificate(s, ref)
        dispatch.assert_not_called(); self.assertEqual(s.state, 'UNKNOWN')
        self.assertIs(self.b._initial_driver_clock, self.clock)
        self.assertEqual(self.b._driver_deadline, self.clock.end)
        self.assertEqual(self.j.phase_paths[0][0].name, e.pair_module.PHASE)
        self.lock.check()

    def test_actual_pair_derived_candidate_retains_exact_history_and_no_approval(self):
        before = dict(self.owner.budget)
        with patch.object(e, 'run', side_effect=AssertionError('candidate executed')) as dispatch, \
             patch.object(e.pair_module._Pair, 'sql', side_effect=AssertionError('candidate SQL')) as sql, \
             patch.object(e.pair_module._Pair, 'command', side_effect=AssertionError('candidate native command')) as native:
            s, raw = self.prepare()
        dispatch.assert_not_called(); sql.assert_not_called(); native.assert_not_called(); request = decode(raw); q = s.cfg['epoch_certificate']
        self.assertEqual(request['status'], 'UNAPPROVED_CANDIDATE_DATA'); self.assertNotIn('approved', s.scope)
        self.assertNotIn('valid_until', s.scope); self.assertEqual(self.owner.budget, before)
        self.assertEqual(q['pair_complete'], self.pair_complete_ref)
        self.assertEqual(q['role_prepare'], self.owner.role_context['reference'])
        self.assertEqual(q['maintenance'], self.owner.cfg['maintenance'])
        self.assertEqual(q['original_inventory'], self.owner.sc[4]['legacy_inventory'])
        self.assertEqual(set(s.cfg), e.BASE_FIELDS); self.assertEqual(set(s.cfg['source_graph']), e.SOURCES)
        self.assertEqual(s.cfg['linux']['postgres']['standby']['mounts'], self.b._initial_pg_pair_standby_native['observed_mounts'])
        self.assertEqual(s.cfg['linux']['postgres']['binding'], self.result['binding'])
        self.assertIs(s.clock, self.clock); self.assertEqual(self.clock.duration, 180)
        self.assertEqual(self.j.phase_paths[0][0].name, e.pair_module.PHASE)

    def test_external_approval_dispatches_exactly_once_after_real_reservation_finish(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        with patch.object(e, 'run', side_effect=self.readonly_dispatch) as dispatch:
            result = e.execute_approved_certificate(s, ref)
        self.assertFalse(result['ready']); self.assertEqual(dispatch.call_count, 1)
        self.assertEqual(s.state, 'CONSUMED'); self.assertTrue(s.entered_run); self.assertIsNotNone(s.finished)
        self.assertFalse(hasattr(self.b, '_initial_epoch_certificate_started'))
        with patch.object(e, 'run', side_effect=AssertionError('replay dispatch')):
            with self.assertRaises(Refused): e.execute_approved_certificate(s, ref)

    def test_every_permission_and_strict_approval_bool_is_validated_by_real_loader(self):
        s, _ = self.prepare()
        for field in s.scope:
            if type(s.scope[field]) is not bool: continue
            for value in (not s.scope[field], int(s.scope[field])):
                ref = self.authority(s, lambda row, k=field, v=value: row.update({k: v}))
                with self.subTest(field=field, value=value), self.assertRaises(Refused): e._load(s.inventory_ref, ref)
        for field, value in (('approved', False), ('approved', 1), ('valid_until', int(time.time()) - 1), ('valid_until', True)):
            ref = self.authority(s, lambda row, k=field, v=value: row.update({k: v}))
            with self.subTest(field=field, value=value), self.assertRaises(Refused): e._load(s.inventory_ref, ref)
        self.assertEqual(s.state, 'WAITING')

    def test_missing_pair_owner_completion_never_prepares_candidate(self):
        with patch.object(self.b, '_initial_pg_pair_complete', None), patch.object(e, 'run') as dispatch:
            with self.assertRaises(Refused): self.prepare()
        dispatch.assert_not_called(); self.assertEqual(self.b._initial_epoch_certificate_intake.state, 'UNKNOWN')

    def test_altered_pair_live_owner_prevents_approval_dispatch(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        with patch.object(self.b, '_initial_pg_pair_live', object()): self.refuse(s, ref)

    def test_wrong_nonce_is_sticky_unknown(self):
        s, _ = self.prepare(); ref = self.envelope(s, lambda row: row.update(stage_nonce=str(uuid.uuid4())))
        self.refuse(s, ref)
        with self.assertRaises(Refused): self.prepare()

    def test_prior_pair_stage_cannot_authorize_certificate(self):
        s, _ = self.prepare(); self.refuse(s, self.envelope(s, lambda row: row.update(stage='pg_pair')))

    def test_changed_request_hash_cannot_authorize_certificate(self):
        s, _ = self.prepare(); self.refuse(s, self.envelope(s, lambda row: row.update(request_sha256='0' * 64)))

    def test_changed_candidate_reference_cannot_authorize_certificate(self):
        s, _ = self.prepare(); self.refuse(s, self.envelope(s, lambda row: row['inventory'].update(sha256='0' * 64)))

    def test_provision_mutation_after_request_is_unknown_without_dispatch(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        path = Path(self.certificate_provision_ref['path']); path.write_bytes(path.read_bytes() + b'\n')
        self.refuse(s, ref)

    def test_duplicate_prepare_and_direct_run_bypass_are_refused(self):
        s, _ = self.prepare(); ref = self.envelope(s)
        with self.assertRaisesRegex(Refused, 'dispatch_required'): e.run(self.b, self.lock, s.inventory_ref, self.authority(s))
        self.assertEqual(s.state, 'UNKNOWN')
        with self.assertRaises(Refused): self.prepare()
        with self.assertRaises(Refused): e.execute_approved_certificate(s, ref)

    def test_partial_dual_archive_poison_prevents_dispatch(self):
        s, _ = self.prepare(); ref = self.envelope(s); real = e.journals.new_file
        second = self.j.phase_paths[1][0]
        def failed(path, raw):
            if Path(path).parent == second and Path(path).name == 'certificate-intake-envelope.json':
                raise OSError('fixture second medium write failure')
            return real(path, raw)
        with patch.object(e.journals, 'new_file', side_effect=failed): self.refuse(s, ref)
        self.assertIsNone(s.finished)
        with self.assertRaises(Refused): self.j.check()

    def test_post_finish_original_deadline_expiry_cannot_dispatch(self):
        s, _ = self.prepare(); ref = self.envelope(s); real = self.j.finish_reservation; expired = None
        def finish(token):
            nonlocal expired
            result = real(token); expired = patch.object(e.time, 'monotonic', return_value=self.clock.end + 1)
            expired.start(); return result
        try:
            with patch.object(self.j, 'finish_reservation', side_effect=finish), patch.object(e, 'run') as dispatch:
                with self.assertRaisesRegex(Refused, 'deadline'): e.execute_approved_certificate(s, ref)
            dispatch.assert_not_called(); self.assertIsNotNone(s.finished); self.assertEqual(s.state, 'UNKNOWN')
        finally:
            if expired is not None: expired.stop()
