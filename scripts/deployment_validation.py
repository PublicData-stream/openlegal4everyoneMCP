#!/usr/bin/env python3
"""Offline serving/admin invariants; not Kubernetes schema/cluster validation."""

import argparse
import copy
import json
import re
import sys
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

import yaml


class ValidationError(ValueError):
    """The rendered serving example violates its supported profile."""


class UniqueSafeLoader(yaml.SafeLoader):
    """Reject ambiguous duplicate keys, including YAML merge keys."""


def unique_mapping(loader, node):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node)
        if not isinstance(key, str) or key in result:
            raise ValidationError("YAML mapping keys must be unique strings")
        result[key] = loader.construct_object(value_node)
    return result


UniqueSafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping
)


def require(condition, message):
    if not condition:
        raise ValidationError(message)


def keys(value, expected, where):
    require(isinstance(value, dict) and set(value) == set(expected),
            f"{where}: unexpected or missing fields")


def equal(value, expected, where):
    # bool is a subclass of int in Python; do not silently admit YAML true as 1.
    require(type(value) is type(expected), f"{where}: incorrect type")
    if isinstance(expected, dict):
        keys(value, expected, where)
        for key, item in expected.items():
            equal(value[key], item, f"{where}.{key}")
    elif isinstance(expected, list):
        require(len(value) == len(expected), f"{where}: incorrect item count")
        for index, item in enumerate(expected):
            equal(value[index], item, f"{where}[{index}]")
    else:
        require(value == expected, f"{where}: incorrect value")


def load_documents(text):
    require(len(text.encode()) <= 256 * 1024, "rendered template exceeds 256 KiB")
    return list(yaml.load_all(text, Loader=UniqueSafeLoader))


STORAGE = ("cache-blobs", "corpus-blobs", "corpus-index", "mecab-ko-dictionary")
ADMIN = {
    "migrate": ("--migrate", 600, ()),
    "maintain": ("--maintain", 300, ("cache-blobs",)),
    "rebuild": ("--rebuild-corpus-index", 86400, STORAGE),
}

NETWORK_VARIANTS = ("base", "edge", "postgres-in-cluster", "postgres-external",
                    "dns-cluster", "dns-fixed", "monitoring", "ingestion-api", "ingestion-provider")


def resource_index(documents):
    """Index by resource identity without silently overwriting repeated kinds."""
    require(isinstance(documents, list), "manifest must be a document list")
    result = {}
    for obj in documents:
        require(isinstance(obj, dict), "manifest document must be a mapping")
        metadata = obj.get("metadata")
        require(isinstance(metadata, dict), "resource metadata must be a mapping")
        identity = (obj.get("apiVersion"), obj.get("kind"),
                    metadata.get("namespace", ""), metadata.get("name"))
        require(all(isinstance(value, str) for value in identity)
                and all(identity[i] for i in (0, 1, 3)), "missing resource identity")
        require(identity not in result, "duplicate resource identity")
        result[identity] = obj
    return result


def network_example(variant):
    """Contract for committed examples, not admission of production overlays."""
    require(variant in NETWORK_VARIANTS, "unknown network example")
    serving = {"matchLabels": {"app.kubernetes.io/name": "openlegal-server"}}
    database_clients = {"matchExpressions": [{"key": "app.kubernetes.io/name",
                        "operator": "In", "values": ["openlegal-server", "openlegal-admin"]}]}

    def selected_peer(namespace, key, value):
        return {"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": namespace}},
                "podSelector": {"matchLabels": {key: value}}}

    def port(protocol, number):
        return {"protocol": protocol, "port": number}

    if variant == "base":
        name = "openlegal-default-deny"
        spec = {"podSelector": {}, "policyTypes": ["Ingress", "Egress"],
                "ingress": [], "egress": []}
    else:
        ingress = variant in ("edge", "monitoring")
        direction = "ingress" if ingress else "egress"
        if variant == "edge":
            name, peer = "openlegal-allow-edge", {"ipBlock": {"cidr": "192.0.2.10/32"}}
            ports = [port("TCP", 8080), port("UDP", 4433)]
        elif variant.startswith("postgres-"):
            name = "openlegal-allow-postgres"
            peer = (selected_peer("replace-with-postgres-namespace", "app.kubernetes.io/name",
                                  "replace-with-postgres-app") if variant == "postgres-in-cluster"
                    else {"ipBlock": {"cidr": "192.0.2.20/32"}})
            ports = [port("TCP", 5432)]
        elif variant.startswith("dns-"):
            name = "openlegal-allow-dns"
            peer = (selected_peer("kube-system", "k8s-app", "kube-dns") if variant == "dns-cluster"
                    else {"ipBlock": {"cidr": "192.0.2.53/32"}})
            ports = [port("TCP", 53), port("UDP", 53)]
        elif variant.startswith("ingestion-"):
            name = "openlegal-allow-" + variant
            address = "192.0.2.40" if variant == "ingestion-api" else "192.0.2.50"
            peer = {"ipBlock": {"cidr": address + "/32"}}
            ports = [port("TCP", 443)]
        else:
            name = "openlegal-allow-monitoring"
            peer = selected_peer("replace-with-monitoring-namespace", "app.kubernetes.io/name",
                                 "replace-with-monitoring-app")
            ports = [port("TCP", 9090)]
        selector = (serving if ingress else database_clients)
        if variant.startswith("ingestion-"):
            selector = {"matchLabels": {"app.kubernetes.io/name": "openlegal-server",
                                        "openlegal.ingestion/enabled": "true"}}
        spec = {"podSelector": selector,
                "policyTypes": ["Ingress" if ingress else "Egress"],
                direction: [{"from" if ingress else "to": [peer], "ports": ports}]}
    return {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
            "metadata": {"name": name, "namespace": "openlegal-serving"}, "spec": spec}


def validate_network(documents, variants):
    """Validate an exact selection, rejecting combined mutually exclusive variants."""
    expected = resource_index([network_example(variant) for variant in variants])
    actual = resource_index(documents)
    require(actual.keys() == expected.keys(), "unexpected or missing network policy")
    for identity, policy in expected.items():
        equal(actual[identity], policy, f"NetworkPolicy {identity[-1]}")


def validate_document_boundary(documents):
    """Guard the existing document trust domain without altering its resources."""
    objects = resource_index(documents)
    expected = {
        ("v1", "Namespace", "", "openlegal-documents"),
        ("v1", "ResourceQuota", "openlegal-documents", "document-budget"),
        ("networking.k8s.io/v1", "NetworkPolicy", "openlegal-documents", "deny-all"),
        ("node.k8s.io/v1", "RuntimeClass", "", "openlegal-document"),
    }
    require(objects.keys() == expected, "unexpected document boundary resource")
    by_kind = {obj["kind"]: obj for obj in objects.values()}

    def field(kind, *path):
        value = by_kind[kind]
        for key in path:
            require(isinstance(value, dict) and key in value, f"missing document {kind}.{key}")
            value = value[key]
        return value

    equal(field("NetworkPolicy", "spec"), network_example("base")["spec"], "document denial")
    equal(field("ResourceQuota", "spec", "hard", "pods"), "2", "document Pod quota")
    equal(field("Namespace", "metadata", "labels", "pod-security.kubernetes.io/enforce"),
          "restricted", "document Pod Security")
    equal(field("RuntimeClass", "handler"), "runc", "document runtime handler")
    equal(field("RuntimeClass", "scheduling"), {"nodeSelector": {"openlegal.document-sandbox/ready": "true"}},
          "document prepared-node scheduling")


def claim_name(volume):
    return "openlegal-" + ("mecab-dictionary" if volume == "mecab-ko-dictionary" else volume)


def validate_config(raw, profile="retained"):
    require(profile in ("retained", "text-only"), "unsupported profile")
    require(isinstance(raw, str), "server.toml must be text")
    config = tomllib.loads(raw)
    sections = ("source", "http", "webtransport", "health", "limits", "text_diff")
    if profile == "retained":
        sections += ("cache", "database")
    keys(config, sections, f"{profile} server configuration")
    keys(config["source"], ("url",), "source")
    url = config["source"]["url"]
    require(isinstance(url, str), "source URL must be text")
    parsed = urlsplit(url)
    require(parsed.scheme == "https" and bool(parsed.hostname)
            and not parsed.username and not parsed.password, "source URL must be HTTPS without credentials")
    if profile == "retained":
        require(re.fullmatch(
            r"https://github\.com/PublicData-stream/openlegal4everyoneMCP/tree/"
            r"(?:REPLACE_WITH_RELEASE_REVISION|[a-f0-9]{40})", url),
            "retained source URL must identify the exact release revision")
    for transport, port in (("http", 8080), ("webtransport", 4433)):
        expected = {
            "bind": f"0.0.0.0:{port}",
            "allowed_hosts": [f"localhost:{port}", f"127.0.0.1:{port}"],
            "allowed_origins": [],
        }
        if profile == "retained":
            authority = ("REPLACE_WITH_OXIBELT_BACKEND_AUTHORITY" if transport == "http"
                         else "REPLACE_WITH_OXIBELT_WEBTRANSPORT_AUTHORITY")
            expected.update(allowed_hosts=[authority],
                            allowed_origins=["https://openlegal4everyone.stream"])
        if transport == "webtransport":
            expected.update(certificate="/run/secrets/backend-tls/tls.crt",
                            private_key="/run/secrets/backend-tls/tls.key")
        equal(config[transport], expected, transport)
    equal(config["health"], {"bind": "0.0.0.0:9090"}, "health")
    equal(config["limits"], {"max_message_bytes": 16 * 1024 * 1024,
                             "max_buffer_bytes": 256 * 1024 * 1024,
                             "shutdown_timeout_secs": 15}, "limits")
    equal(config["text_diff"], {"widget_html": "/opt/openlegal/widgets/text-diff.html"}, "text_diff")
    if profile == "retained":
        equal(config["cache"], {
            "mode": "persistent",
            "postgres": {"url_env": "OPENLEGAL_DATABASE_URL",
                         "migration_url_env": "OPENLEGAL_MIGRATION_DATABASE_URL",
                         "tls_mode": "verify-full", "ca_file": "/run/secrets/postgres-ca/ca.crt"},
            "blob": {"kind": "filesystem", "path": "/var/lib/openlegal/cache-blobs/data"},
        }, "persistent cache")
        equal(config["database"], {
            "blob_path": "/var/lib/openlegal/corpus-blobs/data",
            "index_path": "/var/lib/openlegal/corpus-index/data",
            "widget_html": "/opt/openlegal/widgets/database.html",
            "mecab_dictionary_path": "/var/lib/openlegal/mecab-ko-dictionary/data",
        }, "retained corpus")
    return raw


def validate(documents, profile="retained"):
    require(profile in ("retained", "text-only"), "unsupported profile")
    kinds = ("Namespace", "ConfigMap", "Deployment")
    if profile == "retained":
        kinds += ("Service", "NetworkPolicy")
    require(isinstance(documents, list) and len(documents) == len(kinds),
            f"expected only {', '.join(kinds)}")
    resource_index(documents)
    objects = {}
    for obj in documents:
        require(isinstance(obj, dict), "manifest document must be a mapping")
        kind = obj.get("kind")
        require(isinstance(kind, str) and kind not in objects, "duplicate/missing kind")
        objects[kind] = obj
    keys(objects, kinds, "rendered objects")
    if profile == "retained":
        validate_service(objects["Service"])
        validate_network([objects["NetworkPolicy"]], ["base"])
    namespace, cm, deployment = (objects[k] for k in ("Namespace", "ConfigMap", "Deployment"))
    labels = {f"pod-security.kubernetes.io/{mode}{suffix}": value
              for mode in ("enforce", "audit", "warn")
              for suffix, value in (("", "restricted"), ("-version", "latest"))}
    equal(namespace, {"apiVersion": "v1", "kind": "Namespace",
                      "metadata": {"name": "openlegal-serving", "labels": labels}}, "Namespace")
    keys(cm, ("apiVersion", "kind", "metadata", "data"), "ConfigMap")
    equal(cm["apiVersion"], "v1", "ConfigMap.apiVersion")
    keys(cm["metadata"], ("name", "namespace"), "ConfigMap.metadata")
    equal(cm["metadata"]["namespace"], "openlegal-serving", "ConfigMap namespace")
    name = cm["metadata"]["name"]
    require(isinstance(name, str) and re.fullmatch(r"openlegal-server-config-[a-z0-9]{10}", name),
            "ConfigMap must retain Kustomize's content hash")
    keys(cm["data"], ("server.toml",), "ConfigMap data")
    raw = validate_config(cm["data"]["server.toml"], profile)
    keys(deployment, ("apiVersion", "kind", "metadata", "spec"), "Deployment")
    equal(deployment["apiVersion"], "apps/v1", "Deployment.apiVersion")
    equal(deployment["metadata"], {"name": "openlegal-server", "namespace": "openlegal-serving"},
          "Deployment.metadata")
    spec = deployment["spec"]
    keys(spec, ("replicas", "revisionHistoryLimit", "strategy", "selector", "template"), "Deployment.spec")
    equal(spec["replicas"], 1, "replicas")
    equal(spec["revisionHistoryLimit"], 2, "revision history")
    equal(spec["strategy"], {"type": "Recreate"}, "update strategy")
    app_label = {"app.kubernetes.io/name": "openlegal-server"}
    equal(spec["selector"], {"matchLabels": app_label}, "selector")
    keys(spec["template"], ("metadata", "spec"), "Pod template")
    equal(spec["template"]["metadata"], {"labels": app_label}, "Pod metadata")
    pod = spec["template"]["spec"]
    keys(pod, ("nodeSelector", "affinity", "securityContext", "automountServiceAccountToken",
               "enableServiceLinks", "terminationGracePeriodSeconds", "containers", "volumes"), "Pod spec")
    equal(pod["nodeSelector"], {"kubernetes.io/os": "linux", "openlegal.server/ready": "true"}, "node selection")
    equal(pod["affinity"], {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
        "nodeSelectorTerms": [{"matchExpressions": [{"key": "kubernetes.io/arch", "operator": "In",
                                                    "values": ["amd64", "arm64"]}]}]}}}, "architecture affinity")
    security = {"runAsNonRoot": True, "runAsUser": 10004, "runAsGroup": 10004,
                "fsGroup": 10004, "seccompProfile": {"type": "RuntimeDefault"}}
    if profile == "retained":
        security["fsGroupChangePolicy"] = "OnRootMismatch"
    equal(pod["securityContext"], security, "Pod security")
    equal(pod["automountServiceAccountToken"], False, "service account token")
    equal(pod["enableServiceLinks"], False, "service environment injection")
    equal(pod["terminationGracePeriodSeconds"], 30, "termination grace")
    require(isinstance(pod["containers"], list) and len(pod["containers"]) == 1, "exactly one serving container required")
    container = pod["containers"][0]
    fields = ("name", "image", "imagePullPolicy", "ports", "securityContext", "resources",
              "livenessProbe", "readinessProbe", "volumeMounts")
    if profile == "retained":
        fields += ("env", "startupProbe")
    keys(container, fields, "container")
    if profile == "retained":
        equal(container["env"], [{"name": "OPENLEGAL_DATABASE_URL", "valueFrom": {"secretKeyRef": {
            "name": "openlegal-runtime-db", "key": "OPENLEGAL_DATABASE_URL"}}}], "runtime credential")
        equal(container["startupProbe"], {"httpGet": {"path": "/live", "port": "health"},
              "periodSeconds": 5, "timeoutSeconds": 2, "failureThreshold": 60,
              "successThreshold": 1}, "startup probe")
    equal(container["name"], "server", "container name")
    digest_image(container["image"], "openlegal-server")
    equal(container["imagePullPolicy"], "IfNotPresent", "image pull policy")
    equal(container["ports"], [{"name": n, "containerPort": p, "protocol": protocol}
          for n, p, protocol in (("http", 8080, "TCP"), ("webtransport", 4433, "UDP"), ("health", 9090, "TCP"))], "ports")
    equal(container["securityContext"], {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                        "capabilities": {"drop": ["ALL"]}}, "container security")
    resources = ({"requests": {"cpu": "1", "memory": "2Gi"},
                  "limits": {"cpu": "2", "memory": "4Gi"}} if profile == "retained" else
                 {"requests": {"cpu": "500m", "memory": "512Mi"},
                  "limits": {"cpu": "2", "memory": "2Gi"}})
    equal(container["resources"], resources, "provisional resources")
    for field, path, delay, period in (("livenessProbe", "/live", 30, 10), ("readinessProbe", "/ready", 0, 5)):
        equal(container[field], {"httpGet": {"path": path, "port": "health"}, "initialDelaySeconds": delay,
                                 "periodSeconds": period, "timeoutSeconds": 2, "failureThreshold": 3,
                                 "successThreshold": 1}, field)
    mounts = [{"name": "config", "mountPath": "/etc/openlegal", "readOnly": True},
              {"name": "backend-tls", "mountPath": "/run/secrets/backend-tls", "readOnly": True}]
    volumes = [{"name": "config", "configMap": {"name": name, "defaultMode": 0o444}},
               {"name": "backend-tls", "secret": {"secretName": "openlegal-backend-tls", "defaultMode": 0o440,
                "items": [{"key": "tls.crt", "path": "tls.crt"}, {"key": "tls.key", "path": "tls.key"}]}}]
    if profile == "retained":
        mounts.append({"name": "postgres-ca", "mountPath": "/run/secrets/postgres-ca", "readOnly": True})
        volumes.append({"name": "postgres-ca", "secret": {"secretName": "openlegal-postgres-ca",
                        "defaultMode": 0o440, "items": [{"key": "ca.crt", "path": "ca.crt"}]}})
        for volume in STORAGE:
            readonly = volume == "mecab-ko-dictionary"
            mounts.append({"name": volume, "mountPath": f"/var/lib/openlegal/{volume}", "readOnly": readonly})
            volumes.append({"name": volume, "persistentVolumeClaim": {
                "claimName": claim_name(volume), "readOnly": readonly}})
    equal(container["volumeMounts"], mounts, "mounts")
    equal(pod["volumes"], volumes, "volumes")
    return raw


def digest_image(image, placeholder):
    require(isinstance(image, str) and re.fullmatch(
        r"[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}", image),
        "image must use a sha256 digest reference")
    require(":" not in image.split("@", 1)[0].rsplit("/", 1)[-1], "image tags are not supported")
    if image.endswith("@sha256:" + "0" * 64):
        equal(image, f"registry.example/{placeholder}@sha256:" + "0" * 64, "image placeholder")


def validate_ingestion(documents, retained_documents):
    """Require the explicit opt-in additions and exact retained behavior parity."""
    retained_raw = validate(retained_documents)
    actual = resource_index(documents)
    expected_documents = copy.deepcopy(retained_documents)

    def one(kind, prefix=None):
        matches = [obj for obj in documents if obj["kind"] == kind
                   and (prefix is None or obj["metadata"]["name"].startswith(prefix))]
        require(len(matches) == 1, f"expected one ingestion {kind} {prefix or ''}")
        return matches[0]

    server_cm = one("ConfigMap", "openlegal-server-config-")
    controller_cm = one("ConfigMap", "openlegal-document-controller-config-")
    for cm, prefix in ((server_cm, "openlegal-server-config"),
                       (controller_cm, "openlegal-document-controller-config")):
        require(re.fullmatch(prefix + r"-[a-z0-9]{10}", cm["metadata"]["name"]),
                "ingestion ConfigMap must retain content hash")
        keys(cm.get("data"), ("server.toml", "pilot-candidates.json") if cm is server_cm else ("kubeconfig",), "ingestion ConfigMap data")
    raw = server_cm["data"]["server.toml"]
    require(isinstance(raw, str), "ingestion server.toml must be text")
    candidate_raw = server_cm["data"]["pilot-candidates.json"]
    require(isinstance(candidate_raw, str) and len(candidate_raw.encode()) <= 32 * 1024,
            "ingestion pilot candidate manifest size")
    try:
        candidate_manifest = json.loads(candidate_raw)
    except (ValueError, TypeError):
        raise ValidationError("ingestion pilot candidate manifest syntax") from None
    require(isinstance(candidate_manifest, dict) and candidate_manifest.get("version") == 1
            and isinstance(candidate_manifest.get("candidates"), list)
            and len(candidate_manifest["candidates"]) <= 18,
            "ingestion pilot candidate manifest shape")
    config = tomllib.loads(raw)
    require(isinstance(config.get("database"), dict), "ingestion database configuration required")
    ingestion = config["database"].pop("ingestion", None)
    require(isinstance(ingestion, dict), "ingestion configuration required")
    equal(config, tomllib.loads(retained_raw), "ingestion retained configuration parity")
    worker = ingestion.get("worker_image")
    digest_image(worker, "openlegal-document-worker")
    equal(ingestion, {
        "credential_env": "OPENLEGAL_LAW_PROVIDER_CREDENTIAL", "kubectl": "/usr/local/bin/kubectl",
        "kubeconfig": "/run/secrets/document-controller/config/kubeconfig",
        "context": "openlegal-document-controller", "namespace": "openlegal-documents",
        "worker_image": worker, "enabled": True, "mode": "pilot",
        "manual_candidates_path": "/etc/openlegal/pilot-candidates.json",
        "retain_history_bodies": False,
    }, "ingestion configuration")
    identity = "/run/secrets/document-controller/identity"
    kubeconfig = {
        "apiVersion": "v1", "kind": "Config",
        "clusters": [{"name": "document-cluster", "cluster": {
            "server": "https://kubernetes.default.svc:443", "certificate-authority": identity + "/ca.crt"}}],
        "users": [{"name": "openlegal-document-controller", "user": {"tokenFile": identity + "/token"}}],
        "contexts": [{"name": "openlegal-document-controller", "context": {
            "cluster": "document-cluster", "user": "openlegal-document-controller", "namespace": "openlegal-documents"}}],
        "current-context": "openlegal-document-controller",
    }
    require(isinstance(controller_cm["data"]["kubeconfig"], str), "kubeconfig must be text")
    parsed_kubeconfig = load_documents(controller_cm["data"]["kubeconfig"])
    equal(parsed_kubeconfig, [kubeconfig], "dedicated kubeconfig")
    expected_controller = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {
        "name": controller_cm["metadata"]["name"], "namespace": "openlegal-serving"},
        "data": {"kubeconfig": controller_cm["data"]["kubeconfig"]}}
    expected_documents.extend([expected_controller, {
        "apiVersion": "v1", "kind": "ServiceAccount", "metadata": {
            "name": "openlegal-document-controller", "namespace": "openlegal-serving"},
        "automountServiceAccountToken": False}])
    base_cm = next(obj for obj in expected_documents if obj["kind"] == "ConfigMap"
                   and obj["metadata"]["name"].startswith("openlegal-server-config-"))
    require(base_cm["metadata"]["name"] != server_cm["metadata"]["name"], "ingestion needs a distinct configuration hash")
    base_cm["metadata"]["name"] = server_cm["metadata"]["name"]
    base_cm["data"]["server.toml"] = raw
    base_cm["data"]["pilot-candidates.json"] = candidate_raw
    template = next(obj for obj in expected_documents if obj["kind"] == "Deployment")["spec"]["template"]
    template["metadata"]["labels"]["openlegal.ingestion/enabled"] = "true"
    pod = template["spec"]
    pod["serviceAccountName"] = "openlegal-document-controller"
    container = pod["containers"][0]
    image = one("Deployment")["spec"]["template"]["spec"]["containers"][0].get("image")
    digest_image(image, "openlegal-server-ingestion")
    container["image"] = image
    container["env"].append({"name": "OPENLEGAL_LAW_PROVIDER_CREDENTIAL", "valueFrom": {
        "secretKeyRef": {"name": "openlegal-law-provider", "key": "OPENLEGAL_LAW_PROVIDER_CREDENTIAL"}}})
    container["resources"]["requests"]["ephemeral-storage"] = "64Mi"
    container["resources"]["limits"]["ephemeral-storage"] = "128Mi"
    container["volumeMounts"].extend([
        {"name": "controller-config", "mountPath": "/run/secrets/document-controller/config", "readOnly": True},
        {"name": "controller-identity", "mountPath": identity, "readOnly": True},
        {"name": "kubectl-tmp", "mountPath": "/tmp", "readOnly": False}])
    next(volume for volume in pod["volumes"] if volume["name"] == "config")["configMap"]["name"] = server_cm["metadata"]["name"]
    pod["volumes"].extend([
        {"name": "controller-config", "configMap": {"name": controller_cm["metadata"]["name"], "defaultMode": 0o444}},
        {"name": "controller-identity", "projected": {"defaultMode": 0o440, "sources": [
            {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}},
            {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}}]}},
        {"name": "kubectl-tmp", "emptyDir": {"sizeLimit": "64Mi"}}])
    # Kustomize prepends strategic-merge entries. Order is irrelevant for these
    # unique named items; duplicate names remain rejected by exact list equality.
    actual = copy.deepcopy(actual)
    expected = resource_index(expected_documents)
    for indexed in (actual, expected):
        deployment = next(obj for obj in indexed.values() if obj["kind"] == "Deployment")
        spec = deployment["spec"]["template"]["spec"]
        for items in (spec["volumes"], spec["containers"][0]["volumeMounts"], spec["containers"][0]["env"]):
            items.sort(key=lambda item: item.get("name", ""))
    require(actual.keys() == expected.keys(), "unexpected or missing ingestion resources")
    for identity_key, obj in expected.items():
        equal(actual[identity_key], obj, f"ingestion {identity_key[-1]}")
    return raw


def validate_document_controller_role(documents):
    equal(documents, [{
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
        "metadata": {"name": "document-controller", "namespace": "openlegal-documents"},
        "rules": [
            {"apiGroups": [""], "resources": ["pods"], "verbs": ["create", "get", "list", "watch", "delete"]},
            {"apiGroups": [""], "resources": ["pods/exec"], "verbs": ["get", "create"]},
            {"apiGroups": [""], "resources": ["resourcequotas"], "resourceNames": ["document-budget"], "verbs": ["get"]},
        ],
    }], "existing document controller Role")


def validate_ingestion_rbac(documents):
    equal(documents, [{
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
        "metadata": {"name": "openlegal-document-controller", "namespace": "openlegal-documents"},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "document-controller"},
        "subjects": [{"kind": "ServiceAccount", "name": "openlegal-document-controller", "namespace": "openlegal-serving"}],
    }], "controller namespace-scoped binding")


def validate_admin(documents, serving_documents, operation):
    """Check a separately rendered Job against the validated serving revision."""
    require(operation in ADMIN, "unsupported administration operation")
    validate(serving_documents)
    require(isinstance(documents, list) and len(documents) == 2,
            "administration root must contain only its Job and shared ConfigMap")
    objects = {}
    for obj in documents:
        require(isinstance(obj, dict), "administration document must be a mapping")
        kind = obj.get("kind")
        require(isinstance(kind, str) and kind not in objects, "duplicate/missing admin kind")
        objects[kind] = obj
    keys(objects, ("ConfigMap", "Job"), "administration objects")
    serving = {obj["kind"]: obj for obj in serving_documents}
    equal(objects["ConfigMap"], serving["ConfigMap"], "shared admin ConfigMap")
    command, deadline, storage = ADMIN[operation]
    pod = copy.deepcopy(serving["Deployment"]["spec"]["template"]["spec"])
    pod["restartPolicy"] = "Never"
    container = pod["containers"][0]
    for field in ("ports", "startupProbe", "livenessProbe", "readinessProbe"):
        del container[field]
    container["args"] = [command, "/etc/openlegal/server.toml"]
    if operation == "migrate":
        container["env"] = [{"name": "OPENLEGAL_MIGRATION_DATABASE_URL", "valueFrom": {
            "secretKeyRef": {"name": "openlegal-migration-db", "key": "OPENLEGAL_MIGRATION_DATABASE_URL"}}}]
    if operation != "rebuild":
        container["resources"] = {"requests": {"cpu": "100m", "memory": "128Mi"},
                                  "limits": {"cpu": "1", "memory": "512Mi"}}
    allowed = ("config", "postgres-ca", *storage)
    container["volumeMounts"] = [mount for mount in container["volumeMounts"] if mount["name"] in allowed]
    pod["volumes"] = [volume for volume in pod["volumes"] if volume["name"] in allowed]
    if operation == "rebuild":
        index = next(volume for volume in pod["volumes"] if volume["name"] == "corpus-index")
        index["persistentVolumeClaim"]["claimName"] = "openlegal-corpus-index-rebuild"
    expected = {
        "apiVersion": "batch/v1", "kind": "Job",
        "metadata": {"name": f"openlegal-{operation}", "namespace": "openlegal-serving"},
        "spec": {"suspend": True, "parallelism": 1, "completions": 1, "backoffLimit": 0,
                 "podReplacementPolicy": "Failed", "activeDeadlineSeconds": deadline,
                 "template": {"metadata": {"labels": {"app.kubernetes.io/name": "openlegal-admin",
                                                        "app.kubernetes.io/component": operation}},
                              "spec": pod}},
    }
    equal(objects["Job"], expected, f"{operation} Job")
    return objects["ConfigMap"]["data"]["server.toml"]


def validate_storage(documents):
    """Validate unconfigured operator examples, never actual cluster state."""
    require(isinstance(documents, list) and len(documents) == 11,
            "expected one StorageClass, five reserved PVs and five PVCs including fresh rebuild storage")
    objects = {}
    for obj in documents:
        require(isinstance(obj, dict) and isinstance(obj.get("metadata"), dict), "storage object must have metadata")
        identity = (obj.get("kind"), obj["metadata"].get("name"))
        require(all(isinstance(part, str) for part in identity), "storage identity must be text")
        require(identity not in objects, "duplicate storage object")
        objects[identity] = obj
    expected = {
        ("StorageClass", "openlegal-local"): {
            "apiVersion": "storage.k8s.io/v1", "kind": "StorageClass",
            "metadata": {"name": "openlegal-local", "annotations": {
                "storageclass.kubernetes.io/is-default-class": "false"}},
            "provisioner": "kubernetes.io/no-provisioner", "volumeBindingMode": "WaitForFirstConsumer",
            "reclaimPolicy": "Retain",
        }
    }
    for volume in (*STORAGE, "corpus-index-rebuild"):
        name = claim_name(volume)
        placeholder = name.removeprefix("openlegal-").upper().replace("-", "_")
        capacity = f"REPLACE_WITH_{placeholder}_CAPACITY"
        expected[("PersistentVolume", name)] = {
            "apiVersion": "v1", "kind": "PersistentVolume", "metadata": {"name": name},
            "spec": {"capacity": {"storage": capacity}, "volumeMode": "Filesystem",
                     "accessModes": ["ReadWriteOnce"], "persistentVolumeReclaimPolicy": "Retain",
                     "storageClassName": "openlegal-local",
                     "claimRef": {"namespace": "openlegal-serving", "name": name},
                     "local": {"path": f"/REPLACE_WITH_{placeholder}_HOST_PATH"},
                     "nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [{
                         "key": "kubernetes.io/hostname", "operator": "In",
                         "values": ["REPLACE_WITH_STORAGE_NODE"]}]}]}}},
        }
        expected[("PersistentVolumeClaim", name)] = {
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"namespace": "openlegal-serving", "name": name},
            "spec": {"accessModes": ["ReadWriteOnce"], "volumeMode": "Filesystem",
                     "storageClassName": "openlegal-local", "volumeName": name,
                     "resources": {"requests": {"storage": capacity}}},
        }
    require(set(objects) == set(expected), "unexpected storage objects")
    for identity, obj in expected.items():
        equal(objects[identity], obj, f"{identity[0]}/{identity[1]}")


def validate_service(service):
    equal(service, {
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": "openlegal-server", "namespace": "openlegal-serving"},
        "spec": {
            "type": "NodePort", "externalTrafficPolicy": "Cluster",
            "selector": {"app.kubernetes.io/name": "openlegal-server"},
            "ports": [
                {"name": "mcp-http", "protocol": "TCP", "port": 8080,
                 "targetPort": 8080, "nodePort": 30080},
                {"name": "mcp-webtransport", "protocol": "UDP", "port": 4433,
                 "targetPort": 4433, "nodePort": 30433},
            ],
        },
    }, "Service")


def validate_oxibelt(raw, service):
    """Check the committed handoff, not arbitrary operator OxiBelt configurations."""
    validate_service(service)
    config = tomllib.loads(raw)
    keys(config, ("config", "logging", "runtime", "quic", "listeners", "tls", "proxy",
                  "compression", "cache", "waf", "upstreams", "routes"), "OxiBelt example")
    equal(config["config"], {"strict_unknown_fields": True}, "OxiBelt schema policy")
    equal(config["listeners"], {"https_bind": "0.0.0.0:8443", "http1": True,
                                "http2": True, "http3": True}, "edge listeners")
    equal(config["tls"], {"cert_chain": "edge.pem", "private_key": "edge-key.pem",
                          "ocsp": {"mode": "disabled"}}, "edge TLS")
    equal(config["proxy"], {
        "trusted_ca_certs": ["backend-ca.pem"],
        "auto_upgrade": {"enabled": True, "max_http_version": "h3"},
        "buffering": {"request": "streaming", "response": "streaming"},
    }, "backend trust and streaming")
    for section in ("compression", "cache"):
        equal(config[section], {"enabled": False}, section)
    upstreams = []
    for port in service["spec"]["ports"]:
        webtransport = port["protocol"] == "UDP"
        scheme = "https" if webtransport else "http"
        upstream = {
            "name": port["name"],
            "origin": f"{scheme}://replace-with-private-node.invalid:{port['nodePort']}",
            "max_http_version": "h3" if webtransport else "h1",
            "preserve_host": False, "connect_timeout_ms": 3000, "request_timeout_ms": 40000,
        }
        if webtransport:
            upstream.update(webtransport=True, idle_timeout_ms=65000,
                            tls={"ech": {"mode": "disabled"}})
        else:
            upstream["pool_max_idle_per_host"] = 0
        upstreams.append(upstream)
    equal(config["upstreams"], upstreams, "NodePort upstreams")
    equal(config["routes"], [
        {"name": "mcp-http", "hosts": ["openlegal4everyone.stream"],
         "upstream": "mcp-http", "compression": "off",
         "limits": {"max_request_body_bytes": 16 * 1024 * 1024},
         "match": {"path": {"exact": "/mcp"}}},
        {"name": "mcp-webtransport", "hosts": ["openlegal4everyone.stream"],
         "upstream": "mcp-webtransport",
         "match": {"methods": ["CONNECT"], "protocols": ["webtransport"],
                   "path": {"exact": "/mcp-wt/v1"}}},
    ], "public MCP routes")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--config-output", type=Path)
    parser.add_argument("--profile", choices=("retained", "text-only"), default="retained")
    parser.add_argument("--storage-manifest", type=Path)
    parser.add_argument("--oxibelt-config", type=Path)
    parser.add_argument("--admin-manifest", action="append", default=[], metavar="OPERATION=FILE")
    parser.add_argument("--network-dir", type=Path,
                        help="directory of all separately rendered network examples")
    parser.add_argument("--document-boundary", type=Path)
    parser.add_argument("--ingestion-manifest", type=Path)
    parser.add_argument("--ingestion-rbac", type=Path)
    parser.add_argument("--document-controller-role", type=Path)
    args = parser.parse_args()
    try:
        documents = load_documents(args.manifest.read_text())
        raw = validate(documents, args.profile)
        if args.ingestion_manifest:
            require(args.profile == "retained", "ingestion requires retained baseline")
            validate_ingestion(load_documents(args.ingestion_manifest.read_text()), documents)
        if args.document_controller_role:
            validate_document_controller_role(load_documents(args.document_controller_role.read_text()))
        if args.ingestion_rbac:
            validate_ingestion_rbac(load_documents(args.ingestion_rbac.read_text()))
        if args.network_dir:
            for variant in NETWORK_VARIANTS:
                validate_network(load_documents((args.network_dir / f"{variant}.yaml").read_text()),
                                 [variant])
        if args.document_boundary:
            validate_document_boundary(load_documents(args.document_boundary.read_text()))
        seen = set()
        for admin in args.admin_manifest:
            require(args.profile == "retained", "administration requires retained serving profile")
            operation, separator, path = admin.partition("=")
            require(separator and path and operation in ADMIN and operation not in seen,
                    "admin manifest must be a unique migrate|maintain|rebuild=FILE")
            seen.add(operation)
            validate_admin(load_documents(Path(path).read_text()), documents, operation)
        if args.oxibelt_config:
            require(args.profile == "retained", "OxiBelt handoff requires retained profile")
            service = next(obj for obj in documents if obj["kind"] == "Service")
            validate_oxibelt(args.oxibelt_config.read_text(), service)
        if args.storage_manifest:
            validate_storage(load_documents(args.storage_manifest.read_text()))
        if args.config_output:
            args.config_output.write_text(raw)
    except (ValidationError, yaml.YAMLError, tomllib.TOMLDecodeError, OSError,
            ValueError, KeyError, TypeError, RecursionError) as error:
        # Parser exceptions can quote credential-bearing source text. Invariant
        # messages can also contain input-derived resource names. Never forward
        # either through the deployment gate's diagnostics.
        parser.exit(1, f"Deployment template validation failed ({type(error).__name__}); "
                    "check the selected template against its documented contract.\n")
    print("Deployment template invariants passed (offline; no Kubernetes API admission or cluster acceptance).")


if __name__ == "__main__":
    main()
