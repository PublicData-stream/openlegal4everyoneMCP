"""Actual root-private /tmp readback, resource bounds and mutation detection."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))
import journal
from interface import Refused


@unittest.skipUnless(os.geteuid() == 0, 'root-private journal metadata')
class ReadbackTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='journal-readback-', dir='/tmp')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / 'proof'
        self.expected = b'proof bytes\n'
        self.put(self.expected)
        self.fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        self.addCleanup(os.close, self.fd)

    def put(self, raw):
        self.path.write_bytes(raw)
        self.path.chmod(0o600)

    def readback(self):
        journal.readback(self.fd, 'proof', self.expected)

    def test_small_and_empty_readback_fit_a_small_process_memory_budget(self):
        code = '''import os, resource, sys
sys.path.insert(0, sys.argv[1])
import journal
resource.setrlimit(resource.RLIMIT_AS, (128 * 1024**2, 128 * 1024**2))
fd = os.open(sys.argv[2], os.O_RDONLY | os.O_DIRECTORY)
try:
    for raw in (b'proof bytes\\n', b''):
        path = os.path.join(sys.argv[2], 'proof')
        with open(path, 'wb') as stream: stream.write(raw)
        journal.readback(fd, 'proof', raw)
finally: os.close(fd)
'''
        result = subprocess.run([sys.executable, '-I', '-B', '-c', code, str(BASE), str(self.root)],
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr.decode())

    def test_exact_artifact_limit_and_one_byte_over_global_read_ceiling(self):
        # Exercise the actual boundary at a small test allowance, avoiding a
        # disposable 256 MiB fixture in every full operator run.
        with patch.object(journal, 'MAX_ARTIFACT', 4096):
            self.expected = b'x' * 4096
            self.put(self.expected)
            self.readback()
            self.expected = b'x' * 4098
            self.put(self.expected)
            with self.assertRaisesRegex(Refused, 'journal_readback'): self.readback()

    def test_growth_shrink_content_and_name_replacement_during_read_refuse(self):
        original_fstat = os.fstat
        for kind in ('growth', 'shrink', 'content', 'replacement'):
            with self.subTest(kind=kind):
                self.put(self.expected)
                before = self.path.stat()
                changed = []
                def mutate(fd):
                    value = original_fstat(fd)
                    if not changed and value.st_ino == before.st_ino:
                        changed.append(True)
                        if kind == 'replacement':
                            other = self.root / 'replacement'
                            other.write_bytes(self.expected); other.chmod(0o600)
                            other.replace(self.path)
                        else:
                            raw = {'growth': self.expected + b'x', 'shrink': b'',
                                   'content': b'x' * len(self.expected)}[kind]
                            self.put(raw)
                            os.utime(self.path, ns=(before.st_atime_ns, before.st_mtime_ns))
                    return value
                with patch.object(journal.os, 'fstat', side_effect=mutate):
                    with self.assertRaisesRegex(Refused, 'journal_readback'): self.readback()
                self.assertTrue(changed)

    def test_invalid_metadata_and_read_errors_close_the_child_descriptor(self):
        for kind in ('mode', 'owner', 'hardlink', 'fifo', 'size', 'io'):
            with self.subTest(kind=kind):
                self.path.unlink()
                self.put(self.expected)
                if kind == 'mode': self.path.chmod(0o644)
                elif kind == 'owner': os.chown(self.path, 70, 70)
                elif kind == 'hardlink': os.link(self.path, self.root / 'link')
                elif kind == 'fifo':
                    self.path.unlink(); os.mkfifo(self.path, 0o600)
                elif kind == 'size': self.put(self.expected + b'x')
                before = len(os.listdir('/proc/self/fd'))
                if kind == 'io':
                    with patch.object(journal.os, 'fstat', side_effect=OSError('readback fixture')):
                        with self.assertRaisesRegex(OSError, 'readback fixture'): self.readback()
                else:
                    with self.assertRaisesRegex(Refused, 'journal_file_metadata'): self.readback()
                self.assertEqual(len(os.listdir('/proc/self/fd')), before)
                # The supplied directory remains owned by the caller.
                os.fstat(self.fd)


if __name__ == '__main__': unittest.main()
