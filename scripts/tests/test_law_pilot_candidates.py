"""The manual export is an identity hint, never publishable evidence."""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "prepare-law-pilot-candidates.py"


class PilotCandidateTests(unittest.TestCase):
    def test_utf16_doctype_is_rejected_before_xml_expansion(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "행정심판례검색목록.xml").write_bytes(
                ('<!DOCTYPE cdList [<!ENTITY inflated "' + 'x' * 8192 + '">]>'
                 '<cdList><row><deccSeq>71</deccSeq><evtNm>&inflated;</evtNm></row></cdList>')
                .encode("utf-16")
            )
            output = root / "candidates.json"
            result = subprocess.run(
                [sys.executable, str(SCRIPT), "--input-dir", str(root),
                 "--output", str(output), "--allow-incomplete"],
                capture_output=True, text=True, check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("행정심판례검색목록.xml: skipped", result.stderr)
            self.assertEqual(json.loads(output.read_text())["candidates"], [])

    def test_zero_ids_are_skipped_and_incomplete_input_is_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "행정심판례검색목록.xml").write_text(
                "<cdList><row><deccSeq>0</deccSeq></row>"
                "<row><deccSeq>71</deccSeq><evtNm>synthetic A</evtNm></row>"
                "<row><deccSeq>72</deccSeq><evtNm>synthetic B</evtNm></row></cdList>"
            )
            output = root / "candidates.json"
            command = [sys.executable, str(SCRIPT), "--input-dir", str(root),
                       "--output", str(output)]
            rejected = subprocess.run(command, capture_output=True, text=True, check=False)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertFalse(output.exists())
            accepted = subprocess.run(command + ["--allow-incomplete"],
                                      capture_output=True, text=True, check=False)
            self.assertEqual(accepted.returncode, 0, accepted.stderr)
            candidates = json.loads(output.read_text())["candidates"]
            self.assertEqual([item["object"]["id"] for item in candidates], ["71", "72"])
            self.assertEqual({item["object"]["dataset"] for item in candidates},
                             {"administrative_appeal"})


if __name__ == "__main__":
    unittest.main()
