"""Synthetic regressions for fail-closed image notice collection."""

import copy
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
        self.messages = [
            self.artifact("fixture"),
            self.artifact("lindera-ko-dic"),
            self.artifact("openlegal-server", kind="bin", executable="/build/release/openlegal-server"),
            {"reason": "build-finished", "success": True},
        ]
        self.runs = 0

    def pkg(self, name, root, source="registry+fixture"):
        return {"id": name, "name": name, "version": "1.0.0", "source": source,
                "manifest_path": str(root / "Cargo.toml"), "license": "MIT",
                "repository": "https://example.test/synthetic"}

    def artifact(self, package_id, kind="lib", executable=None, fresh=False):
        return {"reason": "compiler-artifact", "package_id": package_id,
                "target": {"kind": [kind], "name": package_id},
                "profile": {"test": False}, "executable": executable, "fresh": fresh}

    def with_packaged_notice(self):
        (self.package / "LICENSE").write_text("Synthetic license\n")

    def run_collector(self, messages=None):
        (self.supplements / "manifest.json").write_text(json.dumps(self.manifest))
        metadata = self.root / "metadata.json"
        metadata.write_text(json.dumps(self.metadata))
        build_messages = self.root / "build-messages.jsonl"
        build_messages.write_text("".join(
            (record if isinstance(record, str) else json.dumps(record)) + "\n"
            for record in (self.messages if messages is None else messages)))
        self.runs += 1
        self.output = self.root / f"out-{self.runs}"
        return subprocess.run([sys.executable, str(self.helper), str(metadata),
                               str(build_messages), str(self.output)], capture_output=True, text=True,
                              check=False, timeout=10)

    def assert_collection_rejected(self, result):
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(result.stderr.strip())
        self.assertNotIn("Traceback", result.stderr)
        self.assertFalse((self.output / "inventory.json").exists())

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
        self.assertEqual((self.output / "fixture-1.0.0/LICENSES/MIT.txt").read_text(),
                         "Synthetic license\n")
        self.assertEqual((self.output / "fixture-1.0.0/AUTHORS").read_text(),
                         "Synthetic attributed authors\n")
        inventory = json.loads((self.output / "inventory.json").read_text())
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
        self.assertEqual((self.output / "fixture-1.0.0/supplemental/LICENSE").read_text(),
                         "Synthetic supplemental notice\n")
        inventory = json.loads((self.output / "inventory.json").read_text())
        fixture = next(package for package in inventory if package["name"] == "fixture")
        self.assertEqual(fixture["notices"][0]["source"],
                         self.manifest["packages"]["fixture@1.0.0"]["files"][0]["source"])
        self.assertEqual(fixture["notices"][0]["sha256"],
                         self.manifest["packages"]["fixture@1.0.0"]["files"][0]["sha256"])

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

    def test_inactive_optional_edge_without_notice_is_excluded(self):
        self.with_packaged_notice()
        optional = self.pkg("inactive-optional", self.root / "absent")
        optional["dependencies"] = []
        self.metadata["packages"].append(optional)
        self.metadata["packages"][0]["dependencies"] = [
            {"name": optional["name"], "optional": True, "kind": None},
        ]
        self.metadata["resolve"]["nodes"][0]["deps"].append(
            {"pkg": optional["id"], "dep_kinds": [{"kind": None}]})
        self.metadata["resolve"]["nodes"].append({"id": optional["id"], "deps": []})
        result = self.run_collector()
        self.assertEqual(result.returncode, 0, result.stderr)
        inventory = json.loads((self.output / "inventory.json").read_text())
        self.assertEqual({package["name"] for package in inventory}, {"fixture", "lindera-ko-dic"})

    def test_built_optional_dependency_without_notice_fails(self):
        self.with_packaged_notice()
        self.metadata["packages"].append(self.pkg("built-optional", self.root / "absent"))
        self.messages.insert(0, self.artifact("built-optional"))
        result = self.run_collector()
        self.assert_collection_rejected(result)
        self.assertIn("Missing dependency notices: built-optional@1.0.0", result.stderr)

    def test_cached_build_and_proc_macro_artifacts_outside_resolve_are_included_once(self):
        self.with_packaged_notice()
        for name, kind in (("build-helper", "lib"), ("derive-helper", "proc-macro")):
            root = self.root / name
            root.mkdir()
            (root / "LICENSE").write_text(f"Synthetic {name} license\n")
            self.metadata["packages"].append(self.pkg(name, root))
            artifact = self.artifact(name, kind=kind, fresh=True)
            self.messages[0:0] = [artifact, copy.deepcopy(artifact)]
        self.messages.insert(0, self.artifact("fixture", kind="custom-build", fresh=True))
        for message in self.messages:
            if message["reason"] == "compiler-artifact":
                message["fresh"] = True
        result = self.run_collector()
        self.assertEqual(result.returncode, 0, result.stderr)
        inventory = json.loads((self.output / "inventory.json").read_text())
        self.assertEqual(sorted(package["name"] for package in inventory),
                         ["build-helper", "derive-helper", "fixture", "lindera-ko-dic"])
        for name in ("build-helper", "derive-helper"):
            self.assertEqual((self.output / f"{name}-1.0.0/LICENSE").read_text(),
                             f"Synthetic {name} license\n")

    def test_exact_artifact_id_selects_package_from_metadata(self):
        self.with_packaged_notice()
        package_id = "registry+https://example.test/index#fixture@1.0.0"
        self.metadata["packages"][1]["id"] = package_id
        self.messages[0]["package_id"] = package_id
        other_source = self.pkg("fixture", self.root / "absent", source="registry+other")
        other_source["id"] = "registry+https://other.example.test/index#fixture@1.0.0"
        self.metadata["packages"].append(other_source)
        result = self.run_collector()
        self.assertEqual(result.returncode, 0, result.stderr)
        inventory = json.loads((self.output / "inventory.json").read_text())
        self.assertEqual({package["name"] for package in inventory}, {"fixture", "lindera-ko-dic"})

    def test_package_lookup_does_not_require_resolve_graph(self):
        self.with_packaged_notice()
        del self.metadata["resolve"]
        result = self.run_collector()
        self.assertEqual(result.returncode, 0, result.stderr)
        inventory = json.loads((self.output / "inventory.json").read_text())
        self.assertEqual({package["name"] for package in inventory}, {"fixture", "lindera-ko-dic"})

    def test_unknown_artifact_id_fails(self):
        self.with_packaged_notice()
        self.messages.insert(0, self.artifact("registry+https://example.test/index#unknown@1.0.0"))
        self.assert_collection_rejected(self.run_collector())

    def test_unknown_first_party_artifact_id_fails(self):
        self.with_packaged_notice()
        self.messages.insert(0, self.artifact("path+file:///src/crates/unknown#0.0.0"))
        self.assert_collection_rejected(self.run_collector())

    def test_missing_artifact_id_fails(self):
        self.with_packaged_notice()
        del self.messages[0]["package_id"]
        self.assert_collection_rejected(self.run_collector())

    def test_completion_must_be_present_successful_and_unique(self):
        self.with_packaged_notice()
        invalid_logs = {
            "absent": self.messages[:-1],
            "failed": self.messages[:-1] + [{"reason": "build-finished", "success": False}],
            "missing success": self.messages[:-1] + [{"reason": "build-finished"}],
            "nonboolean success": self.messages[:-1] + [{"reason": "build-finished", "success": 1}],
            "repeated": self.messages + [copy.deepcopy(self.messages[-1])],
        }
        for scenario, messages in invalid_logs.items():
            with self.subTest(scenario=scenario):
                self.assert_collection_rejected(self.run_collector(messages))

    def test_artifact_after_completion_fails(self):
        self.with_packaged_notice()
        self.messages.append(self.artifact("fixture", fresh=True))
        self.assert_collection_rejected(self.run_collector())

    def test_expected_server_executable_artifact_is_required(self):
        self.with_packaged_notice()
        invalid_roots = {
            "absent": None,
            "library": {"target": {"kind": ["lib"], "name": "openlegal-server"}},
            "build script": {"target": {"kind": ["custom-build"], "name": "openlegal-server"}},
            "other binary": {"target": {"kind": ["bin"], "name": "other-server"}},
            "test binary": {"profile": {"test": True}},
            "no executable": {"executable": None},
            "empty executable": {"executable": ""},
        }
        for scenario, updates in invalid_roots.items():
            with self.subTest(scenario=scenario):
                messages = copy.deepcopy(self.messages)
                if updates is None:
                    del messages[2]
                else:
                    messages[2].update(updates)
                self.assert_collection_rejected(self.run_collector(messages))

    def test_other_package_cannot_supply_server_binary_evidence(self):
        self.with_packaged_notice()
        self.messages[2]["package_id"] = "fixture"
        self.assert_collection_rejected(self.run_collector())

    def test_non_json_and_unrelated_json_messages_are_ignored(self):
        self.with_packaged_notice()
        self.messages[0:0] = [
            "Arbitrary procedural macro output",
            "",
            {"reason": "compiler-message", "package_id": "unrelated", "message": {}},
            {"reason": "build-script-executed", "package_id": "unrelated"},
            {"unrelated": "JSON output"},
        ]
        self.messages.insert(0, "  " + json.dumps(self.artifact("fixture", fresh=True)))
        result = self.run_collector()
        self.assertEqual(result.returncode, 0, result.stderr)
        inventory = json.loads((self.output / "inventory.json").read_text())
        self.assertEqual({package["name"] for package in inventory}, {"fixture", "lindera-ko-dic"})

    def test_malformed_json_looking_message_fails(self):
        self.with_packaged_notice()
        for prefix in ("", "  "):
            with self.subTest(prefix=prefix):
                self.assert_collection_rejected(self.run_collector(
                    [prefix + '{"reason":"compiler-artifact",'] + self.messages))

    def test_embedded_dictionary_must_be_built(self):
        self.with_packaged_notice()
        del self.messages[1]
        result = self.run_collector()
        self.assert_collection_rejected(result)
        self.assertIn("Expected the locked embedded Korean dictionary notice", result.stderr)


if __name__ == "__main__":
    unittest.main()
