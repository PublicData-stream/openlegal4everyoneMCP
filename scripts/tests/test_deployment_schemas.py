"""Offline provisioning failures and actual pinned Kubernetes schema checks."""

import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import deployment_schemas as schemas


def asset(data, url="https://raw.githubusercontent.com/synthetic-fixture"):
    return {"size": len(data), "sha256": hashlib.sha256(data).hexdigest(), "url": url}


class ProvisioningTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.tools = Path(self.directory.name)
        binary = b"synthetic executable, never invoked"
        license_text = b"synthetic license fixture"
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w:gz") as output:
            for name, data in (("kubeconform", binary), ("LICENSE", license_text)):
                member = tarfile.TarInfo(name)
                member.size = len(data)
                output.addfile(member, io.BytesIO(data))
        self.downloads = {"archive": archive.getvalue(), "license": license_text,
                          "schema": b'{"type":"object","$ref":"#/definitions/test"}'}
        self.lock = {
            "kubeconform": {"archives": {"amd64": {
                **asset(self.downloads["archive"], "archive"), "binary": asset(binary)}},
                "license_file": asset(license_text)},
            "schemas": {"license_file": asset(license_text, "license"),
                        "files": {"fixture/object.json": asset(self.downloads["schema"], "schema")}},
        }

    def provision(self):
        with patch.object(schemas, "download", side_effect=lambda item, deadline: self.downloads[item["url"]]):
            schemas.setup(self.tools, self.lock, "amd64")

    def test_complete_provision_and_verified_offline_reuse(self):
        self.provision()
        with patch.object(schemas, "download", side_effect=AssertionError("network attempted")):
            schemas.setup(self.tools, self.lock, "amd64")
            schemas.verify(self.tools, self.lock, "amd64")
        self.assertFalse(list(self.tools.glob(".schema-staging-*")))

    def test_interrupted_download_never_publishes_partial_bundle(self):
        def interrupted(item, deadline):
            if item["url"] == "schema":
                raise schemas.SchemaError("synthetic interrupted download")
            return self.downloads[item["url"]]
        with patch.object(schemas, "download", side_effect=interrupted):
            with self.assertRaises(schemas.SchemaError):
                schemas.setup(self.tools, self.lock, "amd64")
        self.assertFalse((self.tools / schemas.BUNDLE).exists())
        self.assertFalse(list(self.tools.glob(".schema-staging-*")))

    def test_missing_changed_extra_and_symlinked_assets_fail_without_download(self):
        for mutation in ("missing", "changed", "missing-schema", "changed-schema", "extra", "symlink"):
            with self.subTest(mutation=mutation):
                self.provision()
                bundle = self.tools / schemas.BUNDLE
                binary = bundle / "bin/kubeconform"
                if mutation == "missing":
                    binary.unlink()
                elif mutation == "changed":
                    binary.write_bytes(b"X" * binary.stat().st_size)
                elif mutation == "missing-schema":
                    (bundle / "schemas/fixture/object.json").unlink()
                elif mutation == "changed-schema":
                    schema = bundle / "schemas/fixture/object.json"
                    schema.write_bytes(b"X" * schema.stat().st_size)
                elif mutation == "extra":
                    (bundle / "extra").write_text("unexpected")
                else:
                    binary.unlink()
                    binary.symlink_to("/dev/null")
                with patch.object(schemas, "download", side_effect=AssertionError("network attempted")):
                    with self.assertRaisesRegex(schemas.SchemaError, "setup-deployment-tools"):
                        schemas.verify(self.tools, self.lock, "amd64")
                    with self.assertRaisesRegex(schemas.SchemaError, "setup-deployment-tools"):
                        schemas.setup(self.tools, self.lock, "amd64")
                shutil.rmtree(bundle)

    def test_extracted_binary_hash_is_verified(self):
        self.lock["kubeconform"]["archives"]["amd64"]["binary"]["sha256"] = "0" * 64
        with self.assertRaises(schemas.SchemaError):
            self.provision()
        self.assertFalse((self.tools / schemas.BUNDLE).exists())

    def test_external_schema_reference_cannot_be_published(self):
        self.downloads["schema"] = b'{"$ref":"https://example.invalid/external.json"}'
        self.lock["schemas"]["files"]["fixture/object.json"] = asset(self.downloads["schema"], "schema")
        with self.assertRaisesRegex(schemas.SchemaError, "External schema reference"):
            self.provision()
        self.assertFalse((self.tools / schemas.BUNDLE).exists())

    def test_download_size_hash_deadline_and_destinations(self):
        data = b"fixture bytes"
        for changed in (data[:-1], data + b"!", b"X" * len(data)):
            with self.assertRaises(schemas.SchemaError):
                schemas.checked_bytes(changed, asset(data))
        with self.assertRaisesRegex(schemas.SchemaError, "deadline"):
            schemas.download(asset(data), 0)
        for url in ("http://github.com/file", "https://example.invalid/file",
                    "https://user:password@github.com/file", "https://github.com:8080/file",
                    "file:///etc/passwd"):
            with self.assertRaises(schemas.SchemaError):
                schemas.check_download_url(url)


class SchemaIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tools = Path(os.environ["OPENLEGAL_DEPLOY_TOOLS"])
        cls.rendered = Path(os.environ["OPENLEGAL_SCHEMA_RENDERED_DIR"])
        cls.repo = Path(__file__).resolve().parents[2]
        cls.bundle = schemas.verify(cls.tools)
        cls.storage = schemas.read_documents(cls.rendered / "storage.yaml", "storage.yaml")
        cls.namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "fixture"}}

    def test_all_current_resources_against_both_versions(self):
        with patch.object(schemas, "download", side_effect=AssertionError("network attempted")):
            self.assertEqual(schemas.validate(self.tools, self.rendered, self.repo), 47)

    def test_unknown_fields_types_and_resource_types_rejected_by_both_versions(self):
        invalid = [
            {**self.namespace, "unknownField": "synthetic-secret-value"},
            {**self.namespace, "metadata": {"name": ["synthetic-secret-value"]}},
            {**self.namespace, "kind": "UnknownResource"},
        ]
        for version in ("1.36.0", "1.37.0"):
            for document in invalid:
                with self.subTest(version=version, variant=document.get("kind")):
                    with self.assertRaises(schemas.SchemaError) as caught:
                        schemas.validate_documents(self.bundle, version, [document], "fixture.yaml")
                    self.assertNotIn("synthetic-secret-value", str(caught.exception))
                    self.assertIn("fixture.yaml", str(caught.exception))

    def test_duplicate_keys_aliases_and_unknown_gvk_are_rejected_before_execution(self):
        invalid = (
            "apiVersion: v1\nkind: Namespace\nkind: Namespace\nmetadata: {name: fixture}\n",
            "apiVersion: v1\nkind: UnknownResource\nmetadata: {name: fixture}\n",
            "apiVersion: v1\nkind: Namespace\nmetadata: &x {name: fixture}\nspec: *x\n",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.yaml"
            for text in invalid:
                path.write_text(text)
                with self.assertRaises(schemas.SchemaError):
                    schemas.read_documents(path, "fixture.yaml")

    def test_schema_storage_copy_is_exact_and_does_not_mutate_original(self):
        original = copy.deepcopy(self.storage)
        transformed = schemas.storage_schema_copy(self.storage)
        self.assertEqual(self.storage, original)
        volumes = [item for item in transformed if item["kind"] == "PersistentVolume"]
        claims = [item for item in transformed if item["kind"] == "PersistentVolumeClaim"]
        self.assertEqual(len(volumes), 5)
        self.assertEqual(len(claims), 5)
        self.assertEqual(len({item["spec"]["local"]["path"] for item in volumes}), 5)
        self.assertTrue(all(item["spec"]["capacity"]["storage"] == "1Gi" for item in volumes))
        self.assertTrue(all(item["spec"]["resources"]["requests"]["storage"] == "1Gi" for item in claims))
        for version in ("1.36.0", "1.37.0"):
            schemas.validate_documents(self.bundle, version, transformed, "storage.yaml")

    def test_storage_identity_paths_and_original_values_must_match(self):
        for mutation in ("identity", "metadata", "capacity", "path", "node", "missing", "duplicate"):
            changed = copy.deepcopy(self.storage)
            volume = next(item for item in changed if item["kind"] == "PersistentVolume")
            if mutation == "identity":
                volume["metadata"]["name"] = "other"
            elif mutation == "metadata":
                volume["metadata"] = ["unexpected"]
            elif mutation == "capacity":
                volume["spec"]["capacity"]["storage"] = "1Gi"
            elif mutation == "path":
                del volume["spec"]["local"]["path"]
            elif mutation == "node":
                volume["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"] = []
            elif mutation == "missing":
                changed.remove(volume)
            else:
                changed.append(volume)
            with self.subTest(mutation=mutation):
                with self.assertRaises(schemas.SchemaError):
                    schemas.storage_schema_copy(changed)

    def test_missing_error_or_skipped_result_never_counts_as_success(self):
        for summary in ({}, {"valid": 0, "invalid": 0, "errors": 0, "skipped": 1},
                        {"valid": 1, "invalid": 0, "errors": 1, "skipped": 0}):
            def fake_run(command, **kwargs):
                kwargs["stdout"].write(json.dumps({"summary": summary}).encode())
                return subprocess.CompletedProcess(command, 0)
            with self.subTest(summary=summary), patch.object(schemas.subprocess, "run", fake_run):
                with self.assertRaises(schemas.SchemaError):
                    schemas.validate_documents(self.bundle, "1.36.0", [self.namespace], "fixture.yaml")

    def test_cli_diagnostics_never_repeat_input_values(self):
        sentinel = "synthetic-schema-credential-must-not-appear"
        for payload in (f"kind: [{sentinel}\n", json.dumps({**self.namespace, sentinel: sentinel})):
            with tempfile.TemporaryDirectory() as directory:
                (Path(directory) / "retained.yaml").write_text(payload)
                run = subprocess.run([
                    sys.executable, str(self.repo / "scripts/deployment_schemas.py"), "validate",
                    "--tools-dir", str(self.tools), "--rendered-dir", directory,
                    "--repo", str(self.repo),
                ], capture_output=True, text=True, check=False)
            self.assertEqual(run.returncode, 1)
            self.assertNotIn(sentinel, run.stdout + run.stderr)
            self.assertNotIn("Traceback", run.stderr)
            self.assertIn("rendered/retained.yaml", run.stderr)


if __name__ == "__main__":
    unittest.main()
