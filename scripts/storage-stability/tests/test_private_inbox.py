"""Strict inbox IO against actual root-owned /tmp files and held Linux FDs.

Mount drift and during-read mutations use controlled ports around actual file
IO. These tests establish no operation/context, kernel deadline, Docker,
PostgreSQL, provider, physical-media or cross-fork admission acceptance.
"""
import io
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))
import inventory as inputs
from interface import Refused


@unittest.skipUnless(os.geteuid() == 0, 'actual root-private directory and file metadata')
class PrivateInbox(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir='/tmp')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.root.chmod(0o700)
        self.inbox = self.root / 'inbox'; self.inbox.mkdir(mode=0o700)
        self.held = os.open(self.inbox, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        self.addCleanup(os.close, self.held)
        st = os.fstat(self.held)
        self.identity = {'dev': st.st_dev, 'ino': st.st_ino}
        self.mount = inputs.private_fd_mount_id(self.held)
        self.name = 'request.json'; self.raw = b'{ "raw": "exact bytes" }\n'
        self.path = self.inbox / self.name; self.path.write_bytes(self.raw); self.path.chmod(0o600)
        self.sha = inputs.digest(self.raw)

    def check(self, **kwargs):
        args = {'path': self.inbox, 'identity': self.identity, 'held_fd': self.held, 'mount_id': self.mount}
        args.update(kwargs)
        return inputs.recheck_private_inbox(**args)

    def read(self, **kwargs):
        args = {'directory_fd': self.held, 'name': self.name, 'sha256': self.sha,
                'max_bytes': len(self.raw), 'mount_id': self.mount}
        args.update(kwargs)
        return inputs.read_private_file_at(**args)

    def assert_held(self):
        st = os.fstat(self.held)
        self.assertEqual((st.st_dev, st.st_ino), (self.identity['dev'], self.identity['ino']))

    def during_read(self, mutate):
        real = os.fdopen
        class Stream:
            def __init__(self, *args, **kwargs): self.stream = real(*args, **kwargs)
            def __enter__(self): return self
            def __exit__(self, *args): return self.stream.__exit__(*args)
            def read(self, limit):
                raw = self.stream.read(limit); mutate(); return raw
        return patch.object(inputs.os, 'fdopen', side_effect=Stream)

    def test_exact_raw_hash_and_nine_field_witness_repeat(self):
        self.check(); raw, witness = self.read()
        st = self.path.stat()
        self.assertEqual(raw, self.raw)
        self.assertIs(type(witness), tuple)
        self.assertEqual(witness, tuple(getattr(st, key) for key in
            ('st_dev', 'st_ino', 'st_size', 'st_uid', 'st_gid', 'st_nlink', 'st_mode', 'st_mtime_ns', 'st_ctime_ns')))
        self.assertEqual(self.read(previous=witness), (raw, witness))
        self.check(); self.assert_held()

    def test_pre_epoch_timestamps_have_reusable_exact_witness(self):
        os.utime(self.path, ns=(-1000000000, -1000000000))
        raw, witness = self.read()
        self.assertLess(witness[-2], 0)
        self.assertEqual(self.read(previous=witness), (raw, witness))

    def test_descriptors_are_closed_only_when_owned_and_have_required_flags(self):
        real_open = os.open; opened = []
        def track(*args, **kwargs):
            fd = real_open(*args, **kwargs); opened.append((fd, args[1])); return fd
        with patch.object(inputs.os, 'open', side_effect=track):
            self.check(); self.read()
        self.assertEqual(len(opened), 2)
        for fd, flags in opened:
            with self.assertRaises(OSError): os.fstat(fd)
            self.assertEqual(flags & (os.O_NOFOLLOW | os.O_CLOEXEC), os.O_NOFOLLOW | os.O_CLOEXEC)
        self.assertTrue(opened[0][1] & os.O_DIRECTORY)
        self.assertTrue(opened[1][1] & os.O_NONBLOCK)
        self.assert_held()

    def test_file_descriptor_closes_on_metadata_hash_and_read_failure(self):
        real_open = os.open
        for kind in ('metadata', 'hash', 'read'):
            opened = []
            def track(*args, **kwargs):
                fd = real_open(*args, **kwargs); opened.append(fd); return fd
            if kind == 'metadata': self.path.chmod(0o644)
            try:
                with patch.object(inputs.os, 'open', side_effect=track):
                    if kind == 'read':
                        with patch.object(inputs.os, 'fdopen', side_effect=OSError('controlled')):
                            with self.assertRaises(OSError): self.read()
                    else:
                        with self.assertRaises(Refused): self.read(sha256='0' * 64)
                self.assertEqual(len(opened), 1)
                with self.assertRaises(OSError): os.fstat(opened[0])
                self.assert_held()
            finally: self.path.chmod(0o600)

    def test_filename_bounds_and_traversal_are_rejected_before_open(self):
        for name in ('', '.', '..', '../request.json', '/request.json', 'x/y', 'x\x00y',
                     'x' * 129, 'has space', True, b'request.json'):
            with self.subTest(name=name), patch.object(inputs.os, 'open') as opened:
                with self.assertRaisesRegex(Refused, 'private_file_name'): self.read(name=name)
                opened.assert_not_called()
        name = 'a' * 128
        self.path.rename(self.inbox / name)
        self.assertEqual(self.read(name=name)[0], self.raw)

    def test_exact_integer_bounds_and_lowercase_sha_are_validated_before_open(self):
        for limit in (True, False, 0, -1, 1.0, '1', inputs.MAX_INPUT + 1):
            with self.subTest(limit=limit), patch.object(inputs.os, 'open') as opened:
                with self.assertRaisesRegex(Refused, 'private_file_bound'): self.read(max_bytes=limit)
                opened.assert_not_called()
        for sha in (None, b'a' * 64, 'a' * 63, 'a' * 65, 'A' * 64, 'g' * 64, 'a' * 64 + '\n'):
            with self.subTest(sha=sha), patch.object(inputs.os, 'open') as opened:
                with self.assertRaisesRegex(Refused, 'private_raw_hash'): self.read(sha256=sha)
                opened.assert_not_called()
        self.assertEqual(self.read(max_bytes=inputs.MAX_INPUT)[0], self.raw)

    def test_empty_oversized_and_wrong_hash_refuse(self):
        with self.assertRaisesRegex(Refused, 'private_file_metadata'): self.read(max_bytes=1)
        with self.assertRaisesRegex(Refused, 'private_file_changed'): self.read(sha256='0' * 64)
        self.path.write_bytes(b'')
        with self.assertRaisesRegex(Refused, 'private_file_metadata'): self.read(sha256=inputs.digest(b''))
        self.assert_held()

    def test_symlink_hardlink_fifo_and_directory_refuse(self):
        other = self.inbox / 'other'; self.path.rename(other)
        self.path.symlink_to(other)
        with self.assertRaises(OSError): self.read()
        self.path.unlink(); os.link(other, self.path)
        with self.assertRaisesRegex(Refused, 'private_file_metadata'): self.read()
        self.path.unlink(); os.mkfifo(self.path, 0o600)
        with self.assertRaisesRegex(Refused, 'private_file_metadata'): self.read()
        self.path.unlink(); self.path.mkdir(mode=0o600)
        with self.assertRaisesRegex(Refused, 'private_file_metadata'): self.read()
        self.assert_held()

    def test_file_uid_gid_and_mode_require_root_private(self):
        for uid, gid, mode in ((1, 0, 0o600), (0, 1, 0o600), (0, 0, 0o640), (0, 0, 0o4600)):
            try:
                os.chown(self.path, uid, gid); self.path.chmod(mode)
                with self.subTest(uid=uid, gid=gid, mode=mode), self.assertRaisesRegex(Refused, 'private_file_metadata'):
                    self.read()
            finally: os.chown(self.path, 0, 0); self.path.chmod(0o600)

    def test_exact_previous_witness_rejects_replacement_even_with_same_bytes(self):
        _, witness = self.read()
        other = self.inbox / 'replacement'; other.write_bytes(self.raw); other.chmod(0o600)
        os.replace(other, self.path)
        with self.assertRaisesRegex(Refused, 'private_file_changed'): self.read(previous=witness)
        self.assertEqual(self.read()[0], self.raw)

    def test_previous_shape_is_strict(self):
        _, witness = self.read()
        for previous in (list(witness), witness[:-1], witness + (0,), (True,) + witness[1:]):
            with self.subTest(previous=previous), self.assertRaisesRegex(Refused, 'private_file_previous'):
                self.read(previous=previous)

    def test_during_read_path_replacement_refuses_same_bytes(self):
        def replace():
            peer = self.inbox / 'replacement'; peer.write_bytes(self.raw); peer.chmod(0o600)
            os.replace(peer, self.path)
        with self.during_read(replace), self.assertRaisesRegex(Refused, 'private_file_changed'):
            self.read()
        self.assert_held()

    def test_during_read_all_metadata_and_growth_mutations_refuse(self):
        actions = {
            'uid': lambda: os.chown(self.path, 1, 0),
            'gid': lambda: os.chown(self.path, 0, 1),
            'mode': lambda: self.path.chmod(0o640),
            'link': lambda: os.link(self.path, self.inbox / 'hardlink'),
            'mtime': lambda: os.utime(self.path, ns=(1, 1)),
            'grow': lambda: self.path.write_bytes(self.raw + b'growth'),
        }
        for name, mutate in actions.items():
            self.path.write_bytes(self.raw); os.chown(self.path, 0, 0); self.path.chmod(0o600)
            with self.subTest(name=name), self.during_read(mutate), self.assertRaisesRegex(Refused, 'private_file_changed'):
                self.read()
            if (self.inbox / 'hardlink').exists(): (self.inbox / 'hardlink').unlink()
        self.assert_held()

    def test_file_read_is_bounded_even_if_file_grows_after_metadata(self):
        real = os.fdopen; requests = []
        class Growing:
            def __init__(self, *args, **kwargs): self.stream = real(*args, **kwargs)
            def __enter__(self): return self
            def __exit__(self, *args): return self.stream.__exit__(*args)
            def read(inner, limit):
                requests.append(limit); self.path.write_bytes(self.raw + b'x' * 10000)
                return inner.stream.read(limit)
        with patch.object(inputs.os, 'fdopen', side_effect=Growing), self.assertRaisesRegex(Refused, 'private_file_changed'):
            self.read()
        self.assertEqual(requests, [len(self.raw) + 1])

    def test_inbox_identity_shape_and_canonical_path_are_strict(self):
        for identity in ({'dev': True, 'ino': self.identity['ino']}, {}, dict(self.identity, extra=1),
                         {'dev': -1, 'ino': 0}):
            with self.subTest(identity=identity), self.assertRaisesRegex(Refused, 'private_inbox_identity'):
                self.check(identity=identity)
        for path in (str(self.inbox) + '/.', str(self.inbox) + '/', str(self.inbox) + '\x00',
                     str(self.root / 'missing' / '..' / 'inbox')):
            with self.subTest(path=path), self.assertRaises(Refused): self.check(path=path)
        alias = self.root / 'alias'; alias.symlink_to(self.inbox)
        with self.assertRaises(Refused): self.check(path=alias)

    def test_inbox_ownership_permissions_and_identity_refuse(self):
        for uid, gid, mode in ((1, 0, 0o700), (0, 1, 0o700), (0, 0, 0o750), (0, 0, 0o1700)):
            try:
                os.chown(self.inbox, uid, gid); self.inbox.chmod(mode)
                with self.subTest(uid=uid, gid=gid, mode=mode), self.assertRaisesRegex(Refused, 'private_inbox_metadata'):
                    self.check()
                with self.assertRaisesRegex(Refused, 'private_inbox_metadata'): self.read()
            finally: os.chown(self.inbox, 0, 0); self.inbox.chmod(0o700)
        with self.assertRaisesRegex(Refused, 'private_inbox_metadata'):
            self.check(identity={'dev': self.identity['dev'], 'ino': self.identity['ino'] + 1})
        self.assert_held()

    def test_writable_ancestor_and_symlink_ancestor_refuse(self):
        self.root.chmod(0o777)
        try:
            with self.assertRaisesRegex(Refused, 'writable_ancestor'): self.check()
        finally: self.root.chmod(0o700)
        alias = self.root / 'alias'; alias.symlink_to(self.root)
        with self.assertRaises(Refused): self.check(path=alias / 'inbox')

    def test_inbox_path_replacement_rejects_held_original(self):
        self.inbox.rename(self.root / 'old-inbox'); self.inbox.mkdir(mode=0o700)
        with self.assertRaisesRegex(Refused, 'private_inbox_metadata'): self.check()
        self.assert_held()

    def test_inbox_path_replacement_after_fresh_open_closes_only_fresh(self):
        real = inputs.private_fd_mount_id; calls = []
        def changed(fd):
            calls.append(fd)
            if len(calls) == 2:
                self.inbox.rename(self.root / 'old-inbox'); self.inbox.mkdir(mode=0o700)
            return real(fd)
        with patch.object(inputs, 'private_fd_mount_id', side_effect=changed):
            with self.assertRaisesRegex(Refused, 'private_inbox_metadata'): self.check()
        self.assertEqual(len(calls), 2)
        with self.assertRaises(OSError): os.fstat(calls[-1])
        self.assert_held()

    def test_mount_pins_and_repeated_directory_witnesses_refuse_drift(self):
        with self.assertRaisesRegex(Refused, 'private_inbox_mount'): self.check(mount_id=self.mount + 1)
        with self.assertRaises(Refused): self.read(mount_id=self.mount + 1)
        for position in (1, 2, 3, 4):
            for operation in (self.check, self.read):
                count = 0
                def drift(fd):
                    nonlocal count
                    count += 1
                    return self.mount + (1 if count == position else 0)
                with self.subTest(position=position, operation=operation.__name__), \
                     patch.object(inputs, 'private_fd_mount_id', side_effect=drift), self.assertRaises(Refused):
                    operation()
                self.assert_held()

    def test_closed_parent_fd_is_not_reopened_or_treated_as_path(self):
        closed = os.dup(self.held); os.close(closed)
        with self.assertRaises(OSError): self.check(held_fd=closed)
        with self.assertRaises(OSError): self.read(directory_fd=closed)
        for fd in (True, None, -1):
            with self.subTest(fd=fd), self.assertRaises(Refused): self.read(directory_fd=fd)
        self.assert_held()

    def test_non_directory_parent_fd_refuses(self):
        fd = os.open(self.path, os.O_RDONLY | os.O_CLOEXEC)
        try:
            with self.assertRaisesRegex(Refused, 'private_inbox_metadata'): self.read(directory_fd=fd)
            os.fstat(fd)
        finally: os.close(fd)

    def test_fd_mount_reader_is_bounded_and_strict(self):
        self.assertGreater(inputs.private_fd_mount_id(self.held), 0)
        for raw in (b'', b'mnt_id: 0\n', b'mnt_id: -1\n', b'mnt_id: 2\nmnt_id: 2\n',
                    b'mnt_id: 2x\n', b'mnt_id: 2\n' + b'x' * 4096):
            with self.subTest(raw=raw[:30]), patch('builtins.open', return_value=io.BytesIO(raw)), \
                 self.assertRaisesRegex(Refused, 'private_fd_mount_id'):
                inputs.private_fd_mount_id(self.held)
        for fd in (True, '1', -1, None):
            with self.subTest(fd=fd), self.assertRaisesRegex(Refused, 'private_fd_shape'):
                inputs.private_fd_mount_id(fd)


if __name__ == '__main__':
    unittest.main()
