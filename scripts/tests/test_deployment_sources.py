"""Source admission regressions run against disposable copies of real inputs."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from deployment_sources import (EXTERNAL, INVENTORY, MAX_FILE_BYTES, ROOTS,
                                SourceValidationError, validate_sources)


REPO = Path(__file__).resolve().parents[2]
SERVING = "deploy/kubernetes/serving/deployment.yaml"
KUSTOMIZATION = "deploy/kubernetes/serving/kustomization.yaml"
CONFIG = "deploy/kubernetes/config/server.toml"
KUBECONFIG = "deploy/kubernetes/ingestion/kubeconfig"
SENTINEL = "synthetic-sensitive-value-never-print-this"


class DeploymentSourceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="openlegal-source-test-")
        self.addCleanup(temporary.cleanup)
        self.repo = Path(temporary.name)
        for root in ROOTS:
            shutil.copytree(REPO / root, self.repo / root)
        for relative in (*EXTERNAL, INVENTORY):
            target = self.repo / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REPO / relative, target)

    def write(self, relative, text):
        target = self.repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def append(self, relative, text):
        with (self.repo / relative).open("a") as target:
            target.write(text)

    def replace(self, relative, old, new):
        path = self.repo / relative
        raw = path.read_text()
        self.assertIn(old, raw)
        path.write_text(raw.replace(old, new))

    def rejected(self, rule=None):
        with self.assertRaises(SourceValidationError) as raised:
            validate_sources(self.repo)
        error = raised.exception
        if rule:
            self.assertEqual(error.rule, rule)
        self.assertNotIn(SENTINEL, str(error))
        return error

    def inventory(self):
        return json.loads((self.repo / INVENTORY).read_text())

    def save_inventory(self, inventory):
        self.write(INVENTORY, json.dumps(inventory))

    def test_current_sources_accept_references_placeholders_and_parent_edges(self):
        # Includes secretKeyRef, secret volumes, tokenFile, projected token path,
        # private-key paths, env names, and ../ / ../../ local resource references.
        validate_sources(self.repo)
        self.assertEqual(len(self.inventory()["sources"]), 49)

    def test_unknown_file_cannot_escape_even_without_known_extension(self):
        self.write("deploy/kubernetes/operator-credentials", SENTINEL)
        self.rejected("unexpected-source")

    def test_missing_source_is_not_silently_skipped(self):
        (self.repo / CONFIG).unlink()
        self.rejected("missing-source")

    def test_missing_external_source_is_not_silently_skipped(self):
        (self.repo / EXTERNAL[0]).unlink()
        self.rejected("missing-or-unreadable")

    def test_file_and_directory_symlinks_are_rejected(self):
        path = self.repo / CONFIG
        path.unlink()
        path.symlink_to(REPO / CONFIG)
        self.rejected("symlink")
        path.unlink()
        shutil.copyfile(REPO / CONFIG, path)
        directory = self.repo / "deploy/kubernetes/operator"
        directory.symlink_to(REPO / "deploy/kubernetes/serving", target_is_directory=True)
        self.rejected("symlink")

    def test_external_source_ancestor_symlink_is_rejected(self):
        directory = self.repo / "deploy/document-sandbox"
        shutil.rmtree(directory)
        directory.symlink_to(REPO / "deploy/document-sandbox", target_is_directory=True)
        self.rejected("symlink")

    def test_new_inventory_entry_needs_reachable_validation_owner(self):
        path = "deploy/kubernetes/unused.yaml"
        self.write(path, "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: unused\n")
        inventory = self.inventory()
        inventory["sources"].append({"path": path, "role": "resource", "owners": ["serving"]})
        self.save_inventory(inventory)
        self.rejected("unreachable-source")

    def test_owner_declarations_must_match_actual_graph(self):
        inventory = self.inventory()
        next(entry for entry in inventory["sources"] if entry["path"] == CONFIG)["owners"] = ["serving"]
        self.save_inventory(inventory)
        self.rejected("validation-owner-drift")

    def test_invalid_inventory_does_not_echo_values(self):
        self.write(INVENTORY, '{"version":1,"version":2,"sources":' + SENTINEL)
        self.rejected("invalid-inventory")

    def test_remote_absolute_missing_and_outside_references_are_rejected(self):
        original = (self.repo / KUSTOMIZATION).read_text()
        references = (
            "https://example.invalid/" + SENTINEL,
            "github.com/example/repo?ref=" + SENTINEL,
            "/tmp/" + SENTINEL,
            "missing.yaml",
            "../../../scripts/deployment-sources.json",
        )
        for reference in references:
            with self.subTest(reference_type=reference.split("/")[0]):
                self.write(KUSTOMIZATION, original.replace("  - namespace.yaml", "  - " + reference))
                self.rejected()

    def test_unsupported_execution_and_generation_are_rejected(self):
        original = (self.repo / KUSTOMIZATION).read_text()
        for field in ("transformers", "generators", "helmCharts", "components", "bases",
                      "configurations", "replacements", "patchesStrategicMerge"):
            with self.subTest(field=field):
                self.write(KUSTOMIZATION, original + f"\n{field}: [{SENTINEL}]\n")
                self.rejected("unsupported-kustomization")

    def test_generators_only_accept_inventoried_files(self):
        path = "deploy/kubernetes/config/kustomization.yaml"
        original = (self.repo / path).read_text()
        for field in ("literals", "envs", "options"):
            with self.subTest(field=field):
                self.write(path, original + f"\n    {field}: [{SENTINEL}]\n")
                self.rejected("unsupported-generator")
        self.write(path, original.replace("- server.toml", "- https://example.invalid/" + SENTINEL))
        self.rejected("local-reference-required")

    def test_inline_and_remote_patches_are_rejected(self):
        path = "deploy/kubernetes/ingestion/kustomization.yaml"
        original = (self.repo / path).read_text()
        self.write(path, original.replace("path: deployment-patch.yaml", "patch: " + SENTINEL))
        self.rejected("unsupported-patch")
        self.write(path, original.replace("deployment-patch.yaml", "https://example.invalid/" + SENTINEL))
        self.rejected("local-reference-required")

    def test_reference_role_and_cycles_are_rejected(self):
        self.replace(KUSTOMIZATION, "  - deployment.yaml", "  - ../config/server.toml")
        self.rejected("reference-role")
        shutil.copyfile(REPO / KUSTOMIZATION, self.repo / KUSTOMIZATION)
        self.append("deploy/kubernetes/config/kustomization.yaml", "\nresources:\n  - ../serving\n")
        self.rejected("kustomization-cycle")

    def test_private_keys_and_tokens_in_comments_are_rejected(self):
        original = (self.repo / CONFIG).read_text()
        for comment in (
            "-----BEGIN PRIVATE KEY----- " + SENTINEL,
            "-----BEGIN OPENSSH PRIVATE KEY----- " + SENTINEL,
            "postgresql://operator:" + SENTINEL + "@database.invalid/db",
            'password = "' + SENTINEL + '"',
            "ghp_" + "x" * 40,
        ):
            with self.subTest(pattern=comment.split()[0]):
                self.write(CONFIG, original + "\n# " + comment + "\n")
                self.rejected()

    def test_inline_environment_credentials_are_rejected(self):
        original = (self.repo / SERVING).read_text()
        replacement = 'value: "' + SENTINEL + '"\n              valueFrom:'
        self.write(SERVING, original.replace("valueFrom:", replacement))
        self.rejected("inline-credential-environment")

    def test_comment_fields_match_structured_credential_names(self):
        original = (self.repo / CONFIG).read_text()
        for field in ("client-key-data", "client_key_data", "clientKeyData", "authorization",
                      "provider_credential", "provider-credential", "access_token", "api-key"):
            with self.subTest(field=field):
                self.write(CONFIG, original + f"\n# {field}: {SENTINEL}\n")
                self.rejected("credential-assignment")
        self.write(CONFIG, original + '\n# tokenFile: /run/secrets/controller/token\n'
                   '# private_key = "/run/secrets/backend-tls/tls.key"\n')
        validate_sources(self.repo)

    def test_url_query_credentials_in_comments_are_rejected(self):
        original = (self.repo / CONFIG).read_text()
        for scheme, key in (("postgresql", "password"), ("postgres", "pass%77ord"),
                            ("https", "token"), ("https", "api_key"),
                            ("http", "access_token"), ("https", "provider-credential")):
            with self.subTest(scheme=scheme, key=key):
                self.write(CONFIG, original + f"\n# {scheme}://operator@database.invalid/db?{key}={SENTINEL}\n")
                self.rejected("credential-url-query")
        self.write(CONFIG, original + "\n# https://example.invalid/reference?version=1\n")
        validate_sources(self.repo)

    def test_inline_kubeconfig_credentials_are_rejected(self):
        original = (self.repo / KUBECONFIG).read_text()
        for field in ("token", "password", "client-key-data"):
            with self.subTest(field=field):
                self.write(KUBECONFIG, original.replace(
                    "tokenFile: /run/secrets/document-controller/identity/token", field + ": " + SENTINEL))
                self.rejected()

    def test_database_and_provider_inline_fields_are_rejected(self):
        original = (self.repo / CONFIG).read_text()
        self.write(CONFIG, original.replace('url_env = "OPENLEGAL_DATABASE_URL"', 'url = "' + SENTINEL + '"'))
        self.rejected("inline-database-url")
        self.write(CONFIG, original + '\ncredential = "' + SENTINEL + '"\n')
        self.rejected()

    def test_secret_resources_and_generators_are_rejected(self):
        original = (self.repo / SERVING).read_text()
        self.append(SERVING, "\n---\napiVersion: v1\nkind: Secret\nstringData:\n  secret: " + SENTINEL + "\n")
        self.rejected("secret-resource")
        self.write(SERVING, original)
        self.append(KUSTOMIZATION, "\nsecretGenerator: []\n")
        self.rejected("secret-generator")

    def test_mutable_images_in_source_patch_are_rejected(self):
        path = "deploy/kubernetes/ingestion/deployment-patch.yaml"
        self.replace(path, "registry.example/openlegal-server-ingestion@sha256:" + "0" * 64,
                     "registry.example/server:latest")
        self.rejected("image-digest")

    def test_malformed_values_duplicate_keys_and_aliases_are_safe(self):
        for text in (
            "kind: [" + SENTINEL,
            "kind: Deployment\nkind: " + SENTINEL,
            "value: &shared [*shared]\n",
        ):
            with self.subTest(syntax=text.split(":")[0]):
                self.write(SERVING, text)
                self.rejected()

    def test_file_size_limit(self):
        self.write(CONFIG, "#" + "x" * MAX_FILE_BYTES)
        self.rejected("file-size-limit")

    def test_cli_failure_has_no_traceback_or_source_excerpt(self):
        self.write(SERVING, "kind: [" + SENTINEL)
        result = subprocess.run(
            [sys.executable, str(REPO / "scripts/deployment_sources.py"), str(self.repo)],
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            text=True, capture_output=True, check=False,
        )
        self.assertEqual(result.returncode, 1)
        self.assertNotIn(SENTINEL, result.stdout + result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        self.assertIn("invalid-source-syntax", result.stderr)
        self.assertIn(SERVING, result.stderr)


if __name__ == "__main__":
    unittest.main()
