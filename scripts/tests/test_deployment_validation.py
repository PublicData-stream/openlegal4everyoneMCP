"""Exercise rejected deployment regressions against the actual rendered example."""

import copy
import ipaddress
import itertools
import os
import sys
import unittest
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from deployment_validation import (NETWORK_VARIANTS, ValidationError, load_documents, validate,
                                   validate_admin, validate_document_boundary, validate_network,
                                   validate_oxibelt, validate_storage, validate_ingestion, validate_ingestion_rbac,
                                   validate_document_controller_role)


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


class IngestionValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.retained = load_documents(Path(os.environ["OPENLEGAL_RENDERED_SERVING"]).read_text())
        cls.baseline = load_documents(Path(os.environ["OPENLEGAL_RENDERED_INGESTION"]).read_text())
        cls.binding = load_documents(Path(os.environ["OPENLEGAL_RENDERED_INGESTION_RBAC"]).read_text())

    def setUp(self):
        self.docs = copy.deepcopy(self.baseline)
        self.deployment = next(obj for obj in self.docs if obj["kind"] == "Deployment")
        self.pod = self.deployment["spec"]["template"]["spec"]
        self.container = self.pod["containers"][0]
        self.mounts = {item["name"]: item for item in self.container["volumeMounts"]}
        self.volumes = {item["name"]: item for item in self.pod["volumes"]}
        self.config = next(obj for obj in self.docs if obj["kind"] == "ConfigMap"
                           and "server.toml" in obj["data"])
        self.controller_config = next(obj for obj in self.docs if obj["kind"] == "ConfigMap"
                                      and "kubeconfig" in obj["data"])

    def rejected(self):
        with self.assertRaises(ValidationError):
            validate_ingestion(self.docs, self.retained)

    def test_opt_in_template_and_distinct_admin_configuration(self):
        validate_ingestion(self.docs, self.retained)
        validate_ingestion_rbac(self.binding)
        with self.assertRaises(ValidationError):
            validate(self.docs)
        for operation in ("migrate", "maintain", "rebuild"):
            admin = load_documents((Path(os.environ["OPENLEGAL_RENDERED_ADMIN_DIR"])
                                    / f"{operation}.yaml").read_text())
            validate_admin(admin, self.retained, operation)
            with self.assertRaises(ValidationError):
                validate_admin(admin, self.docs, operation)

    def test_projected_identity_and_bounded_writable_volume(self):
        account = next(obj for obj in self.docs if obj["kind"] == "ServiceAccount")
        projection = self.volumes["controller-identity"]["projected"]
        token = projection["sources"][0]["serviceAccountToken"]
        missing = object()
        for mapping, key, value in (
            (account, "automountServiceAccountToken", True),
            (self.pod, "automountServiceAccountToken", True),
            (self.pod, "serviceAccountName", "default"),
            (projection, "defaultMode", 0o444),
            (token, "expirationSeconds", 86400), (token, "audience", "other"),
            (projection["sources"][1]["configMap"], "name", "untrusted-ca"),
            (self.mounts["controller-identity"], "subPath", "token"),
            (self.mounts["controller-config"], "readOnly", False),
            (self.mounts["controller-identity"], "readOnly", False),
            (self.volumes["kubectl-tmp"]["emptyDir"], "medium", "Memory"),
            (self.volumes["kubectl-tmp"]["emptyDir"], "sizeLimit", "1Gi"),
            (self.container["resources"]["limits"], "ephemeral-storage", "1Gi"),
            (self.deployment["spec"], "replicas", 2),
        ):
            before = mapping.get(key, missing)
            with self.subTest(key=key, value=value):
                mapping[key] = value
                self.rejected()
            if before is missing:
                del mapping[key]
            else:
                mapping[key] = before

    def test_provider_secret_and_extra_resources(self):
        env = next(item for item in self.container["env"] if item["name"] == "OPENLEGAL_LAW_PROVIDER_CREDENTIAL")
        original = copy.deepcopy(env)
        for replacement in (
            {"name": env["name"], "value": "inline-provider-secret"},
            {"name": env["name"], "valueFrom": {"secretKeyRef": {"name": "other", "key": env["name"]}}},
        ):
            env.clear()
            env.update(replacement)
            self.rejected()
        env.clear()
        env.update(original)
        self.docs.append({"apiVersion": "v1", "kind": "Secret", "metadata": {
            "name": "openlegal-law-provider", "namespace": "openlegal-serving"}, "stringData": {"token": "inline"}})
        self.rejected()

    def test_kubeconfig_rejects_ambient_or_embedded_authentication_and_tls_downgrade(self):
        data = self.controller_config["data"]
        original = data["kubeconfig"]
        for old, new in (
            ("tokenFile:", "token:"), ("tokenFile:", "exec:"),
            ("https://kubernetes.default.svc:443", "http://kubernetes.default.svc:443"),
            ("certificate-authority:", "insecure-skip-tls-verify:"),
            ("current-context: openlegal-document-controller", "current-context: ambient"),
            ("namespace: openlegal-documents", "namespace: default"),
        ):
            with self.subTest(new=new):
                self.assertIn(old, original)
                data["kubeconfig"] = original.replace(old, new)
                self.rejected()
        data["kubeconfig"] = original + "preferences: {}\n"
        self.rejected()

    def test_configuration_parity_image_digests_and_explicit_context(self):
        data = self.config["data"]
        original = data["server.toml"]
        for old, new in (
            ('enabled = true', 'enabled = false'),
            ('retain_history_bodies = false', 'retain_history_bodies = true'),
            ('context = "openlegal-document-controller"', 'context = "ambient"'),
            ('namespace = "openlegal-documents"', 'namespace = "default"'),
            ('/usr/local/bin/kubectl', 'kubectl'),
            ('/run/secrets/document-controller/config/kubeconfig', '/root/.kube/config'),
            ('@sha256:' + '0' * 64, ':latest'),
            ('tls_mode = "verify-full"', 'tls_mode = "plaintext"'),
            ('OPENLEGAL_LAW_PROVIDER_CREDENTIAL', 'OPENLEGAL_DATABASE_URL'),
        ):
            with self.subTest(new=new):
                self.assertIn(old, original)
                data["server.toml"] = original.replace(old, new)
                self.rejected()
        data["server.toml"] = original
        for image in ('example/server:latest', 'example/server@sha256:' + '0' * 64,
                      'example/server:tag@sha256:' + 'a' * 64):
            self.container["image"] = image
            self.rejected()
        self.container["image"] = 'example/server@sha256:' + 'a' * 64
        data["server.toml"] = original.replace('registry.example/openlegal-document-worker@sha256:' + '0' * 64,
                                               'example/worker@sha256:' + 'b' * 64)
        validate_ingestion(self.docs, self.retained)

    def test_existing_role_rejects_broader_controller_permissions(self):
        role = load_documents(Path(os.environ["OPENLEGAL_DOCUMENT_CONTROLLER_ROLE"]).read_text())
        validate_document_controller_role(role)
        for path, value in ((('kind',), 'ClusterRole'),
                            (('metadata', 'namespace'), 'openlegal-serving'),
                            (('rules', 0, 'resources'), ['*']),
                            (('rules', 1, 'resources'), ['secrets']),
                            (('rules', 2, 'verbs'), ['get', 'update']),
                            (('rules', 2, 'resourceNames'), [])):
            changed = copy.deepcopy(role)
            mapping = changed[0]
            for key in path[:-1]:
                mapping = mapping[key]
            mapping[path[-1]] = value
            with self.subTest(path=path), self.assertRaises(ValidationError):
                validate_document_controller_role(changed)

    def test_binding_preserves_cross_namespace_subject_and_narrow_role(self):
        for path, value in ((('metadata', 'namespace'), 'openlegal-serving'),
                            (('subjects', 0, 'namespace'), 'openlegal-documents'),
                            (('subjects', 0, 'name'), 'default'),
                            (('roleRef', 'kind'), 'ClusterRole'),
                            (('roleRef', 'name'), 'cluster-admin')):
            changed = copy.deepcopy(self.binding)
            mapping = changed[0]
            for key in path[:-1]:
                mapping = mapping[key]
            mapping[path[-1]] = value
            with self.subTest(path=path), self.assertRaises(ValidationError):
                validate_ingestion_rbac(changed)


class NetworkValidationTests(unittest.TestCase):
    """Check declared traffic intent, not CNI execution, NAT or host exemptions."""

    @classmethod
    def setUpClass(cls):
        root = Path(os.environ["OPENLEGAL_RENDERED_NETWORK_DIR"])
        cls.examples = {variant: load_documents((root / f"{variant}.yaml").read_text())
                        for variant in NETWORK_VARIANTS}
        cls.serving = load_documents(Path(os.environ["OPENLEGAL_RENDERED_SERVING"]).read_text())
        cls.sandbox = load_documents(Path(os.environ["OPENLEGAL_DOCUMENT_BOUNDARY"]).read_text())
        cls.workloads = [next(obj for obj in cls.serving if obj["kind"] == "Deployment")
                         ["spec"]["template"]["metadata"]["labels"]]
        for operation in ("migrate", "maintain", "rebuild"):
            docs = load_documents((Path(os.environ["OPENLEGAL_RENDERED_ADMIN_DIR"])
                                   / f"{operation}.yaml").read_text())
            cls.workloads.append(next(obj for obj in docs if obj["kind"] == "Job")
                                 ["spec"]["template"]["metadata"]["labels"])

    @staticmethod
    def selects(selector, labels):
        if any(labels.get(key) != value for key, value in selector.get("matchLabels", {}).items()):
            return False
        for expression in selector.get("matchExpressions", []):
            if expression["operator"] != "In":
                raise AssertionError("test evaluator supports only the admitted In selector")
            if labels.get(expression["key"]) not in expression["values"]:
                return False
        return True

    def permits(self, policies, workload, direction, protocol, port, *, address="198.51.100.99",
                namespace="unrelated", labels=None):
        """Evaluate only explicit allows in these validated, isolated examples."""
        for policy in policies:
            spec = policy["spec"]
            if not self.selects(spec["podSelector"], workload):
                continue
            for rule in spec.get(direction, []):
                if not any(item["port"] == port and item["protocol"] == protocol
                           for item in rule["ports"]):
                    continue
                for peer in rule["to" if direction == "egress" else "from"]:
                    if "ipBlock" in peer:
                        if ipaddress.ip_address(address) in ipaddress.ip_network(peer["ipBlock"]["cidr"]):
                            return True
                    elif (self.selects(peer["namespaceSelector"], {"kubernetes.io/metadata.name": namespace})
                          and self.selects(peer["podSelector"], labels or {})):
                        return True
        return False

    def combination(self, database, dns, monitoring):
        variants = ["base", "edge", database] + ([dns] if dns else [])
        if monitoring:
            variants.append("monitoring")
        docs = [copy.deepcopy(obj) for variant in variants for obj in self.examples[variant]]
        validate_network(docs, variants)
        return docs

    def test_complete_matrix_and_actual_workload_selectors(self):
        for database, dns, monitoring in itertools.product(
                ("postgres-in-cluster", "postgres-external"), (None, "dns-cluster", "dns-fixed"),
                (False, True)):
            with self.subTest(database=database, dns=dns, monitoring=monitoring):
                docs = self.combination(database, dns, monitoring)
                db_peer = ({"namespace": "replace-with-postgres-namespace",
                            "labels": {"app.kubernetes.io/name": "replace-with-postgres-app"}}
                           if database == "postgres-in-cluster" else {"address": "192.0.2.20"})
                dns_peer = ({"namespace": "kube-system", "labels": {"k8s-app": "kube-dns"}}
                            if dns == "dns-cluster" else {"address": "192.0.2.53"})
                monitor_peer = {"namespace": "replace-with-monitoring-namespace",
                                "labels": {"app.kubernetes.io/name": "replace-with-monitoring-app"}}
                for index, workload in enumerate(self.workloads + [{"app.kubernetes.io/name": "other"}]):
                    supported = index < 4
                    self.assertEqual(self.permits(docs, workload, "egress", "TCP", 5432, **db_peer), supported)
                    for protocol in ("TCP", "UDP"):
                        self.assertEqual(self.permits(docs, workload, "egress", protocol, 53, **dns_peer),
                                         supported and dns is not None)
                    for protocol, port in (("TCP", 8080), ("UDP", 4433)):
                        self.assertEqual(self.permits(docs, workload, "ingress", protocol, port,
                                                      address="192.0.2.10"), index == 0)
                    self.assertEqual(self.permits(docs, workload, "ingress", "TCP", 9090, **monitor_peer),
                                     index == 0 and monitoring)
                    for port in (443, 6443, 5432):
                        self.assertFalse(self.permits(docs, workload, "egress", "TCP", port))
                    self.assertFalse(self.permits(docs, workload, "ingress", "TCP", 9090,
                                                  address="192.0.2.10"))
                    self.assertFalse(self.permits(docs, workload, "ingress", "TCP", 8080))

    def test_ingestion_allows_only_opted_in_serving_and_explicit_destinations(self):
        docs = self.combination("postgres-external", "dns-cluster", False)
        for variant in ("ingestion-api", "ingestion-provider"):
            docs.extend(self.examples[variant])
        ingestion = load_documents(Path(os.environ["OPENLEGAL_RENDERED_INGESTION"]).read_text())
        labels = next(obj for obj in ingestion if obj["kind"] == "Deployment")["spec"]["template"]["metadata"]["labels"]
        for workload in self.workloads + [labels, {"openlegal.ingestion/enabled": "true"},
                                          {"app.kubernetes.io/name": "openlegal-document-worker"}]:
            for address in ("192.0.2.40", "192.0.2.50"):
                self.assertEqual(self.permits(docs, workload, "egress", "TCP", 443, address=address), workload == labels)
                self.assertFalse(self.permits(docs, workload, "egress", "UDP", 443, address=address))
                self.assertFalse(self.permits(docs, workload, "egress", "TCP", 6443, address=address))
            for address in ("192.0.2.41", "192.0.2.51", "198.51.100.1"):
                self.assertFalse(self.permits(docs, workload, "egress", "TCP", 443, address=address))

    def test_peer_conjunction_ports_and_destination_boundaries(self):
        docs = self.combination("postgres-in-cluster", "dns-cluster", True)
        serving = self.workloads[0]
        for direction, port, namespace, labels in (
            ("egress", 5432, "replace-with-postgres-namespace",
             {"app.kubernetes.io/name": "replace-with-postgres-app"}),
            ("egress", 53, "kube-system", {"k8s-app": "kube-dns"}),
            ("ingress", 9090, "replace-with-monitoring-namespace",
             {"app.kubernetes.io/name": "replace-with-monitoring-app"}),
        ):
            self.assertTrue(self.permits(docs, serving, direction, "TCP", port, namespace=namespace, labels=labels))
            self.assertFalse(self.permits(docs, serving, direction, "TCP", port, namespace="other", labels=labels))
            self.assertFalse(self.permits(docs, serving, direction, "TCP", port, namespace=namespace, labels={}))
            self.assertFalse(self.permits(docs, serving, direction, "TCP", port + 1, namespace=namespace, labels=labels))
        for protocol, port in (("UDP", 8080), ("TCP", 4433), ("TCP", 30080), ("UDP", 30433)):
            self.assertFalse(self.permits(docs, serving, "ingress", protocol, port, address="192.0.2.10"))
        docs = self.combination("postgres-external", "dns-fixed", False)
        for address in ("192.0.2.19", "192.0.2.21", "198.51.100.20"):
            self.assertFalse(self.permits(docs, serving, "egress", "TCP", 5432, address=address))

    def test_missing_duplicate_extra_and_exclusive_policies(self):
        docs = self.combination("postgres-external", None, False)
        variants = ["base", "edge", "postgres-external"]
        for changed in (docs[1:], docs + [docs[0]], docs + self.examples["monitoring"]):
            with self.assertRaises(ValidationError):
                validate_network(changed, variants)
        for first, second in (("postgres-in-cluster", "postgres-external"), ("dns-cluster", "dns-fixed")):
            self.assertEqual(self.examples[first][0]["metadata"], self.examples[second][0]["metadata"])
            with self.assertRaises(ValidationError):
                validate_network(self.examples[first] + self.examples[second], [first, second])
        baseline = next(obj for obj in self.serving if obj["kind"] == "NetworkPolicy")
        self.assertEqual(baseline, self.examples["base"][0])
        with self.assertRaises(ValidationError):
            validate([obj for obj in self.serving if obj is not baseline])

    def test_rejects_weakened_baseline_and_allow_rules(self):
        for variant, example in self.examples.items():
            cases = [(('metadata', 'namespace'), 'other'), (('spec', 'podSelector'), {}),
                     (('spec', 'policyTypes'), [])]
            if variant == "base":
                cases = [(('spec', 'podSelector'), {"matchLabels": {"app": "one"}}),
                         (('spec', 'policyTypes'), ["Ingress"]),
                         (('spec', 'ingress'), [{}]), (('spec', 'egress'), [{}])]
            else:
                direction = "ingress" if variant in ("edge", "monitoring") else "egress"
                peers = "from" if direction == "ingress" else "to"
                rule = ("spec", direction, 0)
                cases += [(rule + (peers,), [{}]), (rule + (peers,), []),
                          (rule + ("ports",), []), (rule + ("ports", 0, "port"), True),
                          (rule + ("ports", 0, "protocol"), "SCTP"),
                          (rule + (peers,), [{"ipBlock": {"cidr": "0.0.0.0/0"}}]),
                          (rule + (peers,), [{"ipBlock": {"cidr": "::/0"}}])]
                if variant in ("postgres-in-cluster", "dns-cluster", "monitoring"):
                    peer = example[0]["spec"][direction][0][peers][0]
                    cases.append((rule + (peers,), [{key: value} for key, value in peer.items()]))
                else:
                    cases.append((rule + (peers, 0, "ipBlock", "cidr"), "192.0.2.0/24"))
                if variant.startswith("ingestion-"):
                    cases.append((("spec", "podSelector", "matchLabels"), {"app.kubernetes.io/name": "openlegal-server"}))
                elif direction == "egress":
                    cases.append((("spec", "podSelector", "matchExpressions", 0, "values"), ["openlegal-server"]))
                else:
                    cases.append((("spec", "podSelector"), {"matchLabels": {"app.kubernetes.io/name": "openlegal-admin"}}))
                for omitted in ("ports", peers):
                    changed = copy.deepcopy(example)
                    del changed[0]["spec"][direction][0][omitted]
                    with self.assertRaises(ValidationError):
                        validate_network(changed, [variant])
            for path, value in cases:
                with self.subTest(variant=variant, path=path, value=value):
                    changed = copy.deepcopy(example)
                    mapping = changed[0]
                    for key in path[:-1]:
                        mapping = mapping[key]
                    mapping[path[-1]] = value
                    with self.assertRaises(ValidationError):
                        validate_network(changed, [variant])

    def test_document_namespace_remains_separate(self):
        validate_document_boundary(self.sandbox)
        for kind, path, value in (
            ("NetworkPolicy", ("spec", "egress"), [{}]),
            ("NetworkPolicy", ("spec", "ingress"), [{}]),
            ("ResourceQuota", ("spec", "hard", "pods"), "3"),
            ("RuntimeClass", ("handler",), "runc"),
            ("RuntimeClass", ("scheduling",), {}),
            ("Namespace", ("metadata", "labels", "pod-security.kubernetes.io/enforce"), "privileged"),
        ):
            changed = copy.deepcopy(self.sandbox)
            mapping = next(obj for obj in changed if obj["kind"] == kind)
            for key in path[:-1]:
                mapping = mapping[key]
            mapping[path[-1]] = value
            with self.subTest(kind=kind), self.assertRaises(ValidationError):
                validate_document_boundary(changed)
        for changed in (self.sandbox[:-1], self.sandbox + self.examples["edge"]):
            with self.assertRaises(ValidationError):
                validate_document_boundary(changed)


if __name__ == "__main__":
    unittest.main()
