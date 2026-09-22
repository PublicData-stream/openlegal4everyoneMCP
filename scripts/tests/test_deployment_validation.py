"""Exercise rejected deployment regressions against the actual rendered example."""

import copy
import os
import sys
import unittest
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from deployment_validation import ValidationError, load_documents, validate, validate_admin, validate_oxibelt, validate_storage


class ServingValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.documents = load_documents(Path(os.environ["OPENLEGAL_RENDERED_SERVING"]).read_text())
        cls.text_only = load_documents(Path(os.environ["OPENLEGAL_RENDERED_TEXT_ONLY"]).read_text())
        cls.storage = load_documents(Path(os.environ["OPENLEGAL_STORAGE_EXAMPLES"]).read_text())
        cls.oxibelt = Path(os.environ["OPENLEGAL_OXIBELT_EXAMPLE"]).read_text()
        cls.admin = {operation: load_documents((Path(os.environ["OPENLEGAL_RENDERED_ADMIN_DIR"])
                                               / f"{operation}.yaml").read_text())
                     for operation in ("migrate", "maintain", "rebuild")}

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

    def test_service_exposes_only_selected_transports(self):
        service = self.objects["Service"]["spec"]
        for mapping, key, value in (
            (service, "type", "LoadBalancer"),
            (service, "externalTrafficPolicy", "Local"),
            (service, "selector", {"app.kubernetes.io/name": "other"}),
            (service["ports"][0], "nodePort", 30081),
            (service["ports"][1], "protocol", "TCP"),
            (service["ports"][0], "targetPort", 9090),
            (service["ports"][1], "port", 9090),
        ):
            original = mapping[key]
            with self.subTest(key=key, value=value):
                mapping[key] = value
                self.rejected()
            mapping[key] = original
        for key, value in (("externalIPs", ["192.0.2.1"]),
                           ("publishNotReadyAddresses", True),
                           ("healthCheckNodePort", 30909)):
            with self.subTest(key=key):
                service[key] = value
                self.rejected()
            del service[key]
        original = copy.deepcopy(service["ports"])
        for ports in (original[:1], original + [{"name": "health", "protocol": "TCP",
                       "port": 9090, "targetPort": 9090, "nodePort": 30909}]):
            with self.subTest(ports=ports):
                service["ports"] = ports
                self.rejected()
        self.docs.remove(self.objects["Service"])
        self.rejected()

    def test_oxibelt_handoff_transport_and_trust(self):
        service = self.objects["Service"]
        validate_oxibelt(self.oxibelt, service)
        for old, new in (
            (":30080", ":8080"), (":30433", ":4433"),
            ('https://replace-with-private-node', 'http://replace-with-private-node'),
            ('preserve_host = false', 'preserve_host = true'),
            ('webtransport = true', 'webtransport = false'),
            ('max_http_version = "h3"', 'max_http_version = "h2"'),
            ('trusted_ca_certs = ["backend-ca.pem"]', 'trusted_ca_certs = []'),
            ('response = "streaming"', 'response = "buffered"'),
            ('[cache]\nenabled = false', '[cache]\nenabled = true'),
            ('[compression]\nenabled = false', '[compression]\nenabled = true'),
            ('max_request_body_bytes = 16777216', 'max_request_body_bytes = 10485760'),
            ('hosts = ["openlegal4everyone.stream"]', 'hosts = ["*"]'),
            ('exact = "/mcp"', 'prefix = "/"'),
            ('exact = "/mcp-wt/v1"', 'exact = "/ready"'),
            ('methods = ["CONNECT"]', 'methods = ["GET"]'),
        ):
            with self.subTest(new=new):
                self.assertIn(old, self.oxibelt)
                with self.assertRaises(ValidationError):
                    validate_oxibelt(self.oxibelt.replace(old, new), service)
        with self.assertRaises(ValidationError):
            validate_oxibelt(self.oxibelt + '\n[[routes]]\nname = "health"\n', service)
        with self.assertRaises(ValidationError):
            validate_oxibelt(self.oxibelt.replace('[upstreams.tls.ech]',
                            '[upstreams.tls]\ninsecure = true\n[upstreams.tls.ech]'), service)

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
                 (self.container, "envFrom", [{"secretRef": {"name": "unwanted"}}])]
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
        for section in ("ingest", "demo", "database.ingestion"):
            with self.subTest(section=section):
                data["server.toml"] = original + f'\n[{section}]\nenabled = true\n'
                self.rejected()
        data["server.toml"] = original.replace('https://openlegal4everyone.stream', 'https://unreviewed.test')
        self.rejected()

    def test_secret_environment_is_runtime_only(self):
        original = copy.deepcopy(self.container["env"])
        for env in (
            [{"name": "OPENLEGAL_DATABASE_URL", "value": "postgres://inline-credential"}],
            original + [{"name": "OPENLEGAL_MIGRATION_DATABASE_URL", "valueFrom": {
                "secretKeyRef": {"name": "migration", "key": "url"}}}],
            original + [{"name": "OPENLEGAL_LAW_PROVIDER_CREDENTIAL", "value": "provider"}],
        ):
            with self.subTest(env=env):
                self.container["env"] = env
                self.rejected()
        self.container["env"] = original
        self.container["env"][0]["valueFrom"]["secretKeyRef"]["name"] = "migration"
        self.rejected()

    def test_retained_storage_and_startup_policy(self):
        for mapping, key, value in (
            (self.pod["securityContext"], "fsGroupChangePolicy", "Always"),
            (self.container["startupProbe"], "failureThreshold", 3),
            (self.container["volumeMounts"][-1], "readOnly", False),
            (self.pod["volumes"][-1]["persistentVolumeClaim"], "readOnly", False),
            (self.pod["volumes"][-2]["persistentVolumeClaim"], "claimName", "openlegal-corpus-blobs"),
            (self.pod["volumes"][2]["secret"], "defaultMode", 0o444),
        ):
            original = mapping[key]
            with self.subTest(key=key):
                mapping[key] = value
                self.rejected()
            mapping[key] = original
        del self.pod["securityContext"]["fsGroupChangePolicy"]
        self.rejected()

    def test_retained_configuration_failures(self):
        data = self.objects["ConfigMap"]["data"]
        original = data["server.toml"]
        for old, new in (
            ('tls_mode = "verify-full"', 'tls_mode = "plaintext"'),
            ('url_env = "OPENLEGAL_DATABASE_URL"', 'url = "postgres://inline-credential"'),
            ('/var/lib/openlegal/corpus-index/data', '/var/lib/openlegal/corpus-blobs/data/index'),
            ('/var/lib/openlegal/mecab-ko-dictionary/data', '/var/lib/openlegal/corpus-index/data'),
            ('REPLACE_WITH_RELEASE_REVISION', 'main'),
            ('REPLACE_WITH_OXIBELT_BACKEND_AUTHORITY', '*'),
        ):
            with self.subTest(new=new):
                data["server.toml"] = original.replace(old, new)
                self.rejected()
        for section in ('[cache]', '[database]'):
            data["server.toml"] = original[:original.index(section)]
            self.rejected()

    def test_text_only_fixture_remains_separate(self):
        validate(self.text_only, "text-only")
        with self.assertRaises(ValidationError):
            validate(self.text_only)
        with self.assertRaises(ValidationError):
            validate(self.docs, "text-only")
        docs = copy.deepcopy(self.text_only)
        deployment = next(item for item in docs if item["kind"] == "Deployment")
        deployment["spec"]["template"]["spec"]["containers"][0]["env"] = self.container["env"]
        with self.assertRaises(ValidationError):
            validate(docs, "text-only")

    def test_storage_examples_and_reservations(self):
        validate_storage(self.storage)
        cases = (
            ("StorageClass", ("volumeBindingMode",), "Immediate"),
            ("StorageClass", ("metadata", "annotations", "storageclass.kubernetes.io/is-default-class"), "true"),
            ("PersistentVolume", ("spec", "persistentVolumeReclaimPolicy"), "Delete"),
            ("PersistentVolume", ("spec", "claimRef", "name"), "other-claim"),
            ("PersistentVolume", ("spec", "nodeAffinity"), {}),
            ("PersistentVolume", ("spec", "local", "path"), "/host/real-path"),
            ("PersistentVolumeClaim", ("spec", "volumeName"), "other-volume"),
            ("PersistentVolumeClaim", ("spec", "accessModes"), ["ReadWriteMany"]),
        )
        for kind, path, value in cases:
            with self.subTest(kind=kind, path=path):
                docs = copy.deepcopy(self.storage)
                mapping = next(item for item in docs if item["kind"] == kind)
                for key in path[:-1]:
                    mapping = mapping[key]
                mapping[path[-1]] = value
                with self.assertRaises(ValidationError):
                    validate_storage(docs)
        with self.assertRaises(ValidationError):
            validate_storage(self.storage[:-1])

    def test_duplicate_yaml_keys_and_malformed_objects(self):
        with self.assertRaises(ValidationError):
            load_documents("kind: Deployment\nkind: Secret\n")
        with self.assertRaises((ValidationError, yaml.YAMLError)):
            load_documents("base: &base {kind: Deployment}\ncopy: {<<: *base, kind: Secret}\n")
        for docs in ([], [None, {}, {}], [{"kind": []}, {}, {}]):
            with self.subTest(docs=docs), self.assertRaises(ValidationError):
                validate(docs)

    def test_admin_roots_share_config_image_and_exclude_serving(self):
        for operation, documents in self.admin.items():
            with self.subTest(operation=operation):
                self.assertEqual(validate_admin(documents, self.docs, operation),
                                 self.objects["ConfigMap"]["data"]["server.toml"])
                job = next(obj for obj in documents if obj["kind"] == "Job")
                labels = job["spec"]["template"]["metadata"]["labels"]
                selector = self.objects["Service"]["spec"]["selector"]
                self.assertFalse(all(labels.get(key) == value for key, value in selector.items()))
                with self.assertRaises(ValidationError):
                    validate(self.docs + [job])
                with self.assertRaises(ValidationError):
                    validate_admin(documents + [self.objects["Deployment"]], self.docs, operation)
        published = "ghcr.io/example/server@sha256:" + "abcde012" * 8
        self.container["image"] = published
        for operation, documents in self.admin.items():
            docs = copy.deepcopy(documents)
            with self.assertRaises(ValidationError):
                validate_admin(docs, self.docs, operation)
            next(obj for obj in docs if obj["kind"] == "Job")["spec"]["template"]["spec"]["containers"][0]["image"] = published
            validate_admin(docs, self.docs, operation)

    def test_admin_rejects_automatic_retries_privilege_and_extra_execution(self):
        for operation, documents in self.admin.items():
            docs = copy.deepcopy(documents)
            job = next(obj for obj in docs if obj["kind"] == "Job")
            spec = job["spec"]
            template = spec["template"]
            pod = template["spec"]
            container = pod["containers"][0]
            cases = (
                (spec, "suspend", False), (spec, "backoffLimit", 1),
                (spec, "parallelism", 2), (spec, "completions", True),
                (spec, "podReplacementPolicy", "TerminatingOrFailed"),
                (spec, "activeDeadlineSeconds", 0), (spec, "ttlSecondsAfterFinished", 10),
                (pod, "restartPolicy", "OnFailure"),
                (pod, "automountServiceAccountToken", True), (pod, "enableServiceLinks", True),
                (pod, "hostNetwork", True), (pod, "initContainers", [{"name": "extra"}]),
                (pod["nodeSelector"], "openlegal.server/ready", "false"),
                (pod["securityContext"], "runAsUser", 0),
                (pod["securityContext"], "fsGroupChangePolicy", "Always"),
                (pod["securityContext"], "seccompProfile", {"type": "Unconfined"}),
                (template["metadata"], "labels", {"app.kubernetes.io/name": "openlegal-server"}),
                (container, "command", ["/bin/sh"]), (container, "args", ["/etc/openlegal/server.toml"]),
                (container, "ports", self.container["ports"]),
                (container, "livenessProbe", self.container["livenessProbe"]),
                (container, "envFrom", [{"secretRef": {"name": "provider"}}]),
                (container["securityContext"], "allowPrivilegeEscalation", True),
                (container["securityContext"], "readOnlyRootFilesystem", False),
                (container["securityContext"], "capabilities", {"add": ["SYS_ADMIN"]}),
                (container["resources"]["limits"], "memory", "16Mi"),
                (container, "image", "example/server:latest"),
            )
            missing = object()
            for mapping, key, value in cases:
                original = mapping.get(key, missing)
                with self.subTest(operation=operation, key=key):
                    mapping[key] = value
                    with self.assertRaises(ValidationError):
                        validate_admin(docs, self.docs, operation)
                if original is missing:
                    del mapping[key]
                else:
                    mapping[key] = original

    def test_admin_rejects_credential_mount_and_config_drift(self):
        for operation, documents in self.admin.items():
            docs = copy.deepcopy(documents)
            pod = next(obj for obj in docs if obj["kind"] == "Job")["spec"]["template"]["spec"]
            container = pod["containers"][0]
            original = copy.deepcopy(container["env"])
            wrong = "OPENLEGAL_DATABASE_URL" if operation == "migrate" else "OPENLEGAL_MIGRATION_DATABASE_URL"
            for env in ([], [{"name": wrong, "value": "postgres://inline-secret"}],
                        original + [{"name": "OPENLEGAL_LAW_PROVIDER_CREDENTIAL", "value": "provider"}]):
                with self.subTest(operation=operation, env=env):
                    container["env"] = env
                    with self.assertRaises(ValidationError):
                        validate_admin(docs, self.docs, operation)
            container["env"] = original
            for volume in ({"name": "backend-tls", "secret": {"secretName": "openlegal-backend-tls"}},
                           {"name": "tmp", "emptyDir": {}},
                           {"name": "old-index", "persistentVolumeClaim": {"claimName": "openlegal-corpus-index"}}):
                with self.subTest(operation=operation, volume=volume):
                    pod["volumes"].append(volume)
                    with self.assertRaises(ValidationError):
                        validate_admin(docs, self.docs, operation)
                    pod["volumes"].pop()
            for mount in container["volumeMounts"]:
                mount["readOnly"] = not mount["readOnly"]
                with self.subTest(operation=operation, mount=mount["name"]), self.assertRaises(ValidationError):
                    validate_admin(docs, self.docs, operation)
                mount["readOnly"] = not mount["readOnly"]
            cm = next(obj for obj in docs if obj["kind"] == "ConfigMap")
            cm["data"]["server.toml"] += "\n# stale configuration\n"
            with self.assertRaises(ValidationError):
                validate_admin(docs, self.docs, operation)

    def test_rebuild_rejects_old_or_incompatible_destination(self):
        docs = copy.deepcopy(self.admin["rebuild"])
        pod = next(obj for obj in docs if obj["kind"] == "Job")["spec"]["template"]["spec"]
        index = next(volume for volume in pod["volumes"] if volume["name"] == "corpus-index")
        index["persistentVolumeClaim"]["claimName"] = "openlegal-corpus-index"
        with self.assertRaises(ValidationError):
            validate_admin(docs, self.docs, "rebuild")
        for kind, path, value in (
            ("PersistentVolume", ("spec", "local", "path"), "/REPLACE_WITH_CORPUS_INDEX_HOST_PATH"),
            ("PersistentVolume", ("spec", "nodeAffinity"), {}),
            ("PersistentVolume", ("spec", "persistentVolumeReclaimPolicy"), "Delete"),
            ("PersistentVolumeClaim", ("spec", "volumeName"), "openlegal-corpus-index"),
        ):
            storage = copy.deepcopy(self.storage)
            mapping = next(obj for obj in storage if obj["kind"] == kind
                           and obj["metadata"]["name"] == "openlegal-corpus-index-rebuild")
            for key in path[:-1]:
                mapping = mapping[key]
            mapping[path[-1]] = value
            with self.subTest(kind=kind, path=path), self.assertRaises(ValidationError):
                validate_storage(storage)


if __name__ == "__main__":
    unittest.main()
