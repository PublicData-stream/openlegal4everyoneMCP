"""Synthetic regressions for fail-closed image notice collection."""

import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class NoticeCollection(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.helper = self.root / "collect-notices.py"
        shutil.copyfile(Path(__file__).parents[1] / "collect-notices.py", self.helper)
        self.supplements = self.root / "notices"
        self.supplements.mkdir()
        self.manifest = {"version": 1, "packages": {}}
        self.package = self.root / "fixture"
        self.package.mkdir()
        dictionary = self.root / "dictionary"
        dictionary.mkdir()
        (dictionary / "NOTICE.txt").write_text("Synthetic embedded dictionary notice\n")
        self.metadata = {
            "packages": [
                self.pkg("openlegal-server", self.root, source=None),
                self.pkg("fixture", self.package),
                self.pkg("lindera-ko-dic", dictionary),
            ],
            "resolve": {"nodes": [
                {"id": "openlegal-server", "deps": [
                    {"pkg": name, "dep_kinds": [{"kind": None}]}
                    for name in ("fixture", "lindera-ko-dic")
                ]},
                {"id": "fixture", "deps": []},
                {"id": "lindera-ko-dic", "deps": []},
            ]},
        }

    def pkg(self, name, root, source="registry+fixture"):
        return {"id": name, "name": name, "version": "1.0.0", "source": source,
                "manifest_path": str(root / "Cargo.toml"), "license": "MIT",
                "repository": "https://example.test/synthetic"}

    def run_collector(self):
        (self.supplements / "manifest.json").write_text(json.dumps(self.manifest))
        metadata = self.root / "metadata.json"
        metadata.write_text(json.dumps(self.metadata))
        return subprocess.run([sys.executable, str(self.helper), str(metadata),
                               str(self.root / "out")], capture_output=True, text=True,
                              check=False, timeout=10)

    def test_missing_notice_fails(self):
        result = self.run_collector()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Missing dependency notices: fixture@1.0.0", result.stderr)

    def test_license_directory_preserved_and_unrelated_cache_excluded(self):
        licenses = self.package / "LICENSES"
        licenses.mkdir()
        (licenses / "MIT.txt").write_text("Synthetic license\n")
        (self.package / "AUTHORS").write_text("Synthetic attributed authors\n")
        self.metadata["packages"].append(self.pkg("unrelated-cache", self.root / "absent"))
        result = self.run_collector()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.root / "out/fixture-1.0.0/LICENSES/MIT.txt").read_text(),
                         "Synthetic license\n")
        self.assertEqual((self.root / "out/fixture-1.0.0/AUTHORS").read_text(),
                         "Synthetic attributed authors\n")
        inventory = json.loads((self.root / "out/inventory.json").read_text())
        self.assertEqual({p["name"] for p in inventory}, {"fixture", "lindera-ko-dic"})

    def supplement(self):
        data = b"Synthetic supplemental notice\n"
        (self.supplements / "LICENSE").write_bytes(data)
        self.manifest["packages"]["fixture@1.0.0"] = {
            "revision": "a" * 40,
            "files": [{"path": "LICENSE", "sha256": hashlib.sha256(data).hexdigest(),
                       "source": "https://example.test/synthetic/" + "a" * 40 + "/LICENSE"}],
        }

    def test_supplement_is_preserved_with_provenance(self):
        self.supplement()
        result = self.run_collector()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.root / "out/fixture-1.0.0/supplemental/LICENSE").read_text(),
                         "Synthetic supplemental notice\n")

    def test_modified_supplement_fails(self):
        self.supplement()
        (self.supplements / "LICENSE").write_text("modified")
        result = self.run_collector()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Notice digest mismatch", result.stderr)

    def test_mismatched_upstream_revision_fails(self):
        self.supplement()
        (self.package / ".cargo_vcs_info.json").write_text(json.dumps({"git": {"sha1": "b" * 40}}))
        result = self.run_collector()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Notice revision mismatch", result.stderr)


if __name__ == "__main__":
    unittest.main()
