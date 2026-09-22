"""Exercise rejected deployment regressions against the actual rendered example."""

import copy
import os
import sys
import unittest
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from deployment_validation import ValidationError, load_documents, validate


class ServingValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.documents = load_documents(Path(os.environ["OPENLEGAL_RENDERED_SERVING"]).read_text())

    def setUp(self):
        self.docs = copy.deepcopy(self.documents)
        self.objects = {item["kind"]: item for item in self.docs}
        self.spec = self.objects["Deployment"]["spec"]
        self.pod = self.spec["template"]["spec"]
        self.container = self.pod["containers"][0]

    def rejected(self):
        with self.assertRaises(ValidationError):
            validate(self.docs)

    def test_real_template_and_published_digest(self):
        validate(self.docs)
        self.container["image"] = "ghcr.io/example/server@sha256:" + "abcde012" * 8
        validate(self.docs)

    def test_replica_and_update_overlap(self):
        for replicas in (0, 2, True):
            with self.subTest(replicas=replicas):
                self.spec["replicas"] = replicas
                self.rejected()
        self.spec["replicas"] = 1
        self.spec["strategy"] = {"type": "RollingUpdate"}
        self.rejected()

    def test_security_weakening(self):
        cases = [(self.pod["securityContext"], "runAsUser", 0),
                 (self.pod["securityContext"], "seccompProfile", {"type": "Unconfined"}),
                 (self.container["securityContext"], "allowPrivilegeEscalation", True),
                 (self.container["securityContext"], "readOnlyRootFilesystem", False),
                 (self.container["securityContext"], "capabilities", {"drop": ["ALL"], "add": ["NET_ADMIN"]}),
                 (self.pod, "automountServiceAccountToken", True),
                 (self.pod, "enableServiceLinks", True)]
        for mapping, key, value in cases:
            original = mapping[key]
            with self.subTest(key=key):
                mapping[key] = value
                self.rejected()
            mapping[key] = original

    def test_extra_execution_and_mount_surfaces(self):
        cases = [(self.pod, "hostNetwork", True), (self.pod, "runtimeClassName", "document-worker"),
                 (self.pod, "initContainers", [{"name": "extra", "image": "busybox"}]),
                 (self.container, "command", ["/bin/sh"]),
                 (self.container, "env", [{"name": "SECRET", "value": "unwanted"}])]
        for mapping, key, value in cases:
            with self.subTest(key=key):
                mapping[key] = value
                self.rejected()
            del mapping[key]
        self.pod["volumes"].append({"name": "writable", "emptyDir": {}})
        self.rejected()

    def test_digest_and_placeholder_rules(self):
        for image in ("example/server:latest", "example/server:v1", "example/server@sha256:bad",
                      "example/server@sha256:" + "0" * 64,
                      "example/server:latest@sha256:" + "a" * 64):
            with self.subTest(image=image):
                self.container["image"] = image
                self.rejected()

    def test_probe_config_and_secret_relationships(self):
        for mapping, key, value in (
            (self.container["readinessProbe"]["httpGet"], "port", "http"),
            (self.pod["volumes"][0]["configMap"], "name", "different-config"),
            (self.pod["volumes"][1]["secret"], "defaultMode", 0o444),
            (self.container["volumeMounts"][1], "readOnly", False),
            (self.pod["nodeSelector"], "openlegal.server/ready", True),
        ):
            original = mapping[key]
            with self.subTest(key=key):
                mapping[key] = value
                self.rejected()
            mapping[key] = original

    def test_namespace_security_and_extra_object(self):
        labels = self.objects["Namespace"]["metadata"]["labels"]
        labels["pod-security.kubernetes.io/enforce"] = "privileged"
        self.rejected()
        labels["pod-security.kubernetes.io/enforce"] = "restricted"
        self.docs.append({"apiVersion": "v1", "kind": "Secret", "data": {}})
        self.rejected()

    def test_database_ingestion_and_extra_config(self):
        data = self.objects["ConfigMap"]["data"]
        original = data["server.toml"]
        for section in ("database", "ingest", "demo", "cache"):
            with self.subTest(section=section):
                data["server.toml"] = original + f'\n[{section}]\nenabled = true\n'
                self.rejected()
        data["server.toml"] = original.replace('allowed_origins = []', 'allowed_origins = ["https://unreviewed.test"]')
        self.rejected()

    def test_duplicate_yaml_keys_and_malformed_objects(self):
        with self.assertRaises(ValidationError):
            load_documents("kind: Deployment\nkind: Secret\n")
        with self.assertRaises((ValidationError, yaml.YAMLError)):
            load_documents("base: &base {kind: Deployment}\ncopy: {<<: *base, kind: Secret}\n")
        for docs in ([], [None, {}, {}], [{"kind": []}, {}, {}]):
            with self.subTest(docs=docs), self.assertRaises(ValidationError):
                validate(docs)


if __name__ == "__main__":
    unittest.main()
