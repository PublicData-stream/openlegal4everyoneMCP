#!/usr/bin/env python3
"""Offline serving/admin invariants; not Kubernetes schema/cluster validation."""

import argparse
import copy
from fractions import Fraction
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

DOCUMENT_WORKER_DEFAULTS = {"cpu": "2", "memory": "4Gi", "scratch": "2Gi", "pool_limit": 16}
QUANTITY_SCALE = {
    "": 1, "n": Fraction(1, 10**9), "u": Fraction(1, 10**6),
    "m": Fraction(1, 1000), "k": 10**3, "M": 10**6, "G": 10**9,
    "T": 10**12, "P": 10**15, "E": 10**18,
    "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40,
    "Pi": 2**50, "Ei": 2**60,
}


def normalize_quantity(value, resource):
    """Convert a positive Kubernetes Quantity to rounded milli base units."""
    require(isinstance(value, str) and len(value) <= 128,
            f"{resource}: expected bounded Quantity text")
    match = re.fullmatch(r"([+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))"
                         r"(Ki|Mi|Gi|Ti|Pi|Ei|[numkMGTPE]|[eE][+-]?[0-9]+)?", value)
    require(match is not None, f"{resource}: invalid Quantity")
    number, suffix = match.groups()
    digits = number.lstrip("+")
    numerator = Fraction(digits)
    if (suffix or "") in QUANTITY_SCALE:
        scale = QUANTITY_SCALE[suffix or ""]
    else:
        exponent = int(suffix[1:])
        require(-(2**31) <= exponent <= 2**31 - 1,
                f"{resource}: Quantity exponent out of range")
        if exponent < -200:
            require(numerator > 0, f"{resource}: Quantity must be positive")
            return 1
        require(exponent <= 200, f"{resource}: Quantity exceeds supported range")
        scale = Fraction(10) ** exponent
    amount = numerator * scale * 1000
    require(amount > 0, f"{resource}: Quantity must be positive")
    rounded = -(-amount.numerator // amount.denominator)
    require(rounded <= (2**63 - 1) * 1000,
            f"{resource}: Quantity exceeds supported range")
    return rounded


def document_worker_settings(ingestion):
    """Validate optional operator settings without loosening other ingestion fields."""
    supplied = ingestion.get("document_worker", {})
    require(isinstance(supplied, dict), "document_worker must be a table")
    require(set(supplied) <= set(DOCUMENT_WORKER_DEFAULTS),
            "document_worker has unknown fields")
    settings = DOCUMENT_WORKER_DEFAULTS | supplied
    for resource in ("cpu", "memory", "scratch"):
        normalize_quantity(settings[resource], resource)
    pool = settings["pool_limit"]
    require(type(pool) is int and 0 < pool <= 2**32 - 1,
            "document_worker.pool_limit must be a positive integer")
    for resource in ("cpu", "memory", "scratch"):
        require(normalize_quantity(settings[resource], resource) * pool <= (2**63 - 1) * 1000,
                f"document_worker aggregate {resource} exceeds quota range")
    return settings


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
                        "operator": "In", "values": ["openlegal-server", "openlegal-admin", "openlegal-collection-scheduler", "openlegal-collection-job"]}]}

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
            selector = {"matchLabels": {"openlegal.ingestion/enabled": "true"}}
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


def validate_document_boundary(documents, worker_settings=None):
    """Guard the document trust domain and the selected exact pool budget."""
    settings = document_worker_settings({"document_worker": worker_settings or {}})
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

    quota = {"pods": settings["pool_limit"] * 1000}
    for resource, quota_resource in (("cpu", "cpu"), ("memory", "memory"),
                                     ("scratch", "ephemeral-storage")):
        total = normalize_quantity(settings[resource], resource) * settings["pool_limit"]
        for direction in ("requests", "limits"):
            quota[f"{direction}.{quota_resource}"] = total
    equal(by_kind["Namespace"], {"apiVersion": "v1", "kind": "Namespace", "metadata": {
        "name": "openlegal-documents", "labels": {
            "pod-security.kubernetes.io/enforce": "restricted",
            "pod-security.kubernetes.io/enforce-version": "latest"}}}, "document Namespace")
    equal(by_kind["NetworkPolicy"], network_example("base") | {"metadata": {
        "name": "deny-all", "namespace": "openlegal-documents"}}, "document denial")
    equal(by_kind["RuntimeClass"], {"apiVersion": "node.k8s.io/v1", "kind": "RuntimeClass",
        "metadata": {"name": "openlegal-document"}, "handler": "runc", "scheduling": {
            "nodeSelector": {"openlegal.document-sandbox/ready": "true"}}},
          "document runtime")
    hard = field("ResourceQuota", "spec", "hard")
    keys(hard, quota, "document quota hard resources")
    for key, expected in quota.items():
        if key == "pods":
            require(normalize_quantity(hard[key], "pods") == expected,
                    "document Pod quota differs from selected pool limit")
        else:
            resource = key.rsplit(".", 1)[1]
            kind = "scratch" if resource == "ephemeral-storage" else resource
            require(normalize_quantity(hard[key], kind) == expected,
                    f"document quota {key} differs from selected worker budget")
    equal({key: value for key, value in by_kind["ResourceQuota"].items() if key != "spec"},
          {"apiVersion": "v1", "kind": "ResourceQuota", "metadata": {
              "name": "document-budget", "namespace": "openlegal-documents"}}, "document quota identity")
    equal(by_kind["ResourceQuota"]["spec"], {"hard": hard}, "document unscoped quota")


def claim_name(volume):
    return "openlegal-" + ("mecab-dictionary" if volume == "mecab-ko-dictionary" else volume)


def validate_config(raw, profile="retained"):
    require(profile in ("retained", "text-only"), "unsupported profile")
    require(isinstance(raw, str), "server.toml must be text")
    config = tomllib.loads(raw)
    sections = ("source", "http", "webtransport", "health", "limits", "text_diff")
    if profile == "retained":
        sections += ("edge_mtls", "cache", "database")
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
                            allowed_origins=["https://openlegal4everyone.mcp.publicdata.stream",
                                             "https://openlegal4everyone.api.publicdata.stream"])
            if transport == "http":
                expected["tls"] = {"certificate": "/run/secrets/backend-tls/tls.crt",
                                   "private_key": "/run/secrets/backend-tls/tls.key"}
        if transport == "webtransport":
            expected.update(certificate="/run/secrets/backend-tls/tls.crt",
                            private_key="/run/secrets/backend-tls/tls.key")
        equal(config[transport], expected, transport)
    if profile == "retained":
        equal(config["edge_mtls"], {
            "client_ca_file": "/run/secrets/edge-client-ca/ca.crt",
            "required_client_dns_san": "oxibelt.openlegal.internal",
        }, "edge mTLS")
    equal(config["health"], {"bind": "0.0.0.0:9090"}, "health")
    expected_limits = {"max_message_bytes": 16 * 1024 * 1024,
                       "max_buffer_bytes": 256 * 1024 * 1024,
                       "shutdown_timeout_secs": 15}
    if profile == "retained":
        expected_limits["rate_limit"] = {"verified_tunnel": {
            "calls_per_second": 1000, "burst": 1000}}
    equal(config["limits"], expected_limits, "limits")
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
        mounts.append({"name": "edge-client-ca", "mountPath": "/run/secrets/edge-client-ca",
                       "readOnly": True})
        volumes.append({"name": "edge-client-ca", "secret": {
            "secretName": "openlegal-edge-client-ca", "defaultMode": 0o440,
            "items": [{"key": "ca.crt", "path": "ca.crt"}]}})
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
    """Keep serving immutable and inspect the isolated scheduler and Job contract."""
    retained_raw = validate(retained_documents)
    actual = resource_index(documents)
    retained = resource_index(retained_documents)
    require(set(retained).issubset(actual), "ingestion overlay removed retained resources")
    for identity, expected in retained.items():
        equal(actual[identity], expected, f"retained {identity[-1]}")
    additions = [obj for identity, obj in actual.items() if identity not in retained]
    equal({(obj["kind"], obj["metadata"]["name"]) for obj in additions}, {
        ("ConfigMap", "openlegal-collection-config"),
        ("ConfigMap", "openlegal-document-controller-config"),
        ("ServiceAccount", "openlegal-collection-controller"),
        ("ServiceAccount", "openlegal-collection-scheduler"),
        ("Deployment", "openlegal-collection-scheduler"),
        ("Role", "collection-scheduler"),
        ("RoleBinding", "collection-scheduler"),
        ("ResourceQuota", "collection-pod-budget"),
    }, "ingestion resource set")
    def added(kind, name):
        return next(obj for obj in additions if obj["kind"] == kind and obj["metadata"]["name"] == name)
    cm = added("ConfigMap", "openlegal-collection-config")
    keys(cm["data"], ("server.toml", "collection-job.json"), "collection configuration")
    raw = cm["data"]["server.toml"]
    config = tomllib.loads(raw)
    ingestion = config["database"].pop("ingestion", None)
    require(isinstance(ingestion, dict), "ingestion configuration required")
    equal(config, tomllib.loads(retained_raw), "retained configuration parity")
    document_worker_settings(ingestion)
    worker = ingestion.pop("worker_image", None)
    digest_image(worker, "openlegal-document-worker")
    ingestion.pop("document_worker", None)
    equal(ingestion, {
        "credential_env": "OPENLEGAL_LAW_PROVIDER_CREDENTIAL",
        "kubectl": "/usr/local/bin/kubectl",
        "kubeconfig": "/run/secrets/document-controller/config/kubeconfig",
        "context": "openlegal-document-controller", "namespace": "openlegal-documents",
        "collection_namespace": "openlegal-serving",
        "collection_job_template_path": "/etc/openlegal/collection-job.json",
        "enabled": True, "mode": "continuous", "retain_history_bodies": False,
        "detail_timeout_secs": 3600, "detail_job_workers": 4,
    }, "continuous collection configuration")
    equal(added("ServiceAccount", "openlegal-collection-controller"), {
        "apiVersion": "v1", "kind": "ServiceAccount", "metadata": {
            "name": "openlegal-collection-controller", "namespace": "openlegal-serving"},
        "automountServiceAccountToken": False,
    }, "request Job ServiceAccount")
    equal(added("ServiceAccount", "openlegal-collection-scheduler"), {
        "apiVersion": "v1", "kind": "ServiceAccount", "metadata": {
            "name": "openlegal-collection-scheduler", "namespace": "openlegal-serving"},
        "automountServiceAccountToken": False,
    }, "scheduler ServiceAccount")
    equal(added("Role", "collection-scheduler"), {
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
        "metadata": {"name": "collection-scheduler", "namespace": "openlegal-serving"},
        "rules": [{"apiGroups": ["batch"], "resources": ["jobs"],
                   "verbs": ["create", "get", "list", "watch"]}],
    }, "scheduler Job permissions")
    equal(added("RoleBinding", "collection-scheduler"), {
        "apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
        "metadata": {"name": "collection-scheduler", "namespace": "openlegal-serving"},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role",
                    "name": "collection-scheduler"},
        "subjects": [{"kind": "ServiceAccount", "name": "openlegal-collection-scheduler",
                      "namespace": "openlegal-serving"}],
    }, "scheduler Job RoleBinding")
    kubeconfig = {
        "apiVersion": "v1", "kind": "Config",
        "clusters": [{"name": "document-cluster", "cluster": {
            "server": "https://kubernetes.default.svc:443",
            "certificate-authority": "/run/secrets/document-controller/identity/ca.crt"}}],
        "users": [{"name": "openlegal-document-controller", "user": {
            "tokenFile": "/run/secrets/document-controller/identity/token"}}],
        "contexts": [{"name": "openlegal-document-controller", "context": {
            "cluster": "document-cluster", "user": "openlegal-document-controller",
            "namespace": "openlegal-documents"}}],
        "current-context": "openlegal-document-controller",
    }
    controller_cm = added("ConfigMap", "openlegal-document-controller-config")
    keys(controller_cm["data"], ("kubeconfig",), "controller ConfigMap")
    equal(load_documents(controller_cm["data"]["kubeconfig"]), [kubeconfig], "dedicated kubeconfig")
    job = json.loads(cm["data"]["collection-job.json"])
    require(job.get("kind") == "Job" and job["spec"].get("activeDeadlineSeconds") == 7500
            and job["spec"].get("backoffLimit") == 0, "bounded collection Job")
    job_pod = job["spec"]["template"]["spec"]
    digest_image(job_pod["containers"][0]["image"], "openlegal-server-ingestion")
    require(job_pod.get("serviceAccountName") == "openlegal-collection-controller"
            and job_pod.get("automountServiceAccountToken") is False,
            "collection Job identity")
    scheduler = added("Deployment", "openlegal-collection-scheduler")
    scheduler_pod = scheduler["spec"]["template"]["spec"]
    digest_image(scheduler_pod["containers"][0]["image"], "openlegal-server-ingestion")
    require(scheduler_pod["containers"][0]["args"] == ["--collection-scheduler", "/etc/openlegal/server.toml"]
            and scheduler_pod.get("serviceAccountName") == "openlegal-collection-scheduler"
            and scheduler_pod.get("automountServiceAccountToken") is False,
            "scheduler command and identity")
    require(scheduler["spec"].get("replicas") == 1, "one collection scheduler")
    identity = "/run/secrets/document-controller/identity"
    for pod in (job_pod, scheduler_pod):
        require(set(pod) == {"automountServiceAccountToken", "containers", "enableServiceLinks",
                             "nodeSelector", "securityContext", "serviceAccountName",
                             "terminationGracePeriodSeconds", "volumes"}
                | ({"restartPolicy"} if pod is job_pod else set()), "collector Pod fields")
        require(len(pod["containers"]) == 1 and pod.get("enableServiceLinks") is False
                and pod.get("terminationGracePeriodSeconds") == 30,
                "isolated collector Pod")
        require(pod.get("nodeSelector") == {"kubernetes.io/os": "linux",
                                             "openlegal.server/ready": "true"},
                "collector node placement")
        equal(pod["securityContext"], {"runAsNonRoot": True, "runAsUser": 10004,
              "runAsGroup": 10004, "fsGroup": 10004,
              "seccompProfile": {"type": "RuntimeDefault"}}, "collector Pod security")
        container = pod["containers"][0]
        require(set(container) == {"name", "image", "imagePullPolicy", "args", "env",
                                   "resources", "securityContext", "volumeMounts"},
                "collector container fields")
        equal(container["securityContext"], {"allowPrivilegeEscalation": False,
              "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
              "collector container security")
        volumes = {item["name"]: item for item in pod["volumes"]}
        mounts = {item["name"]: item for item in container["volumeMounts"]}
        require(len(volumes) == len(pod["volumes"]) == 8 and
                len(mounts) == len(container["volumeMounts"]) == 8 and
                set(volumes) == set(mounts) == {"config", "postgres-ca", "controller-config",
                                             "controller-identity", "cache-blobs", "corpus-blobs",
                                             "mecab-ko-dictionary", "scratch"},
                "collector volumes and mounts")
        equal(volumes["config"], {"name": "config", "configMap": {
            "name": "openlegal-collection-config", "defaultMode": 0o444}},
            "collector configuration volume")
        equal(volumes["postgres-ca"], {"name": "postgres-ca", "secret": {
            "secretName": "openlegal-postgres-ca", "defaultMode": 0o440}},
            "collector database CA")
        equal(volumes["controller-config"], {"name": "controller-config", "configMap": {
            "name": "openlegal-document-controller-config", "defaultMode": 0o444}},
            "controller configuration volume")
        for name, claim, read_only in (("cache-blobs", "openlegal-cache-blobs", False),
                                       ("corpus-blobs", "openlegal-corpus-blobs", False),
                                       ("mecab-ko-dictionary", "openlegal-mecab-dictionary", True)):
            expected_claim = {"claimName": claim}
            if read_only:
                expected_claim["readOnly"] = True
            equal(volumes[name], {"name": name, "persistentVolumeClaim": expected_claim},
                  f"collector {name} PVC")
        equal(volumes["controller-identity"], {"name": "controller-identity", "projected": {
            "defaultMode": 0o440, "sources": [
                {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}},
                {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}}]}},
            "projected controller identity")
        equal(mounts["controller-identity"], {"name": "controller-identity", "mountPath": identity,
              "readOnly": True}, "controller identity mount")
        equal(mounts["controller-config"], {"name": "controller-config",
              "mountPath": "/run/secrets/document-controller/config", "readOnly": True},
              "controller kubeconfig mount")
        equal(volumes["scratch"], {"name": "scratch", "emptyDir": {"sizeLimit": "2Gi"}},
              "bounded scratch")
        equal(container["resources"], {"requests": {"cpu": "500m" if pod is scheduler_pod else "250m",
            "memory": "2Gi" if pod is scheduler_pod else "1Gi", "ephemeral-storage": "128Mi"},
            "limits": {"cpu": "2", "memory": "4Gi", "ephemeral-storage": "2Gi"}},
            "collector resources")
    for pod in (job_pod, scheduler_pod):
        env = {item["name"]: item for item in pod["containers"][0]["env"]}
        equal(env, {name: {"name": name, "valueFrom": {"secretKeyRef": {"name": secret, "key": name}}}
                    for name, secret in (("OPENLEGAL_DATABASE_URL", "openlegal-runtime-db"),
                                         ("OPENLEGAL_LAW_PROVIDER_CREDENTIAL", "openlegal-law-provider"))},
              "collector credential set")
    quota = added("ResourceQuota", "collection-pod-budget")
    equal(quota["spec"], {"hard": {"pods": "18"}}, "separate on-demand Pod budget")
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
        "subjects": [
            {"kind": "ServiceAccount", "name": "openlegal-collection-controller", "namespace": "openlegal-serving"},
            {"kind": "ServiceAccount", "name": "openlegal-collection-scheduler", "namespace": "openlegal-serving"},
        ],
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
                  "compression", "cache", "waf", "rate_limits", "upstreams", "routes"), "OxiBelt example")
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
    equal(config["waf"], {
        "enabled": True, "mode": "enforcing", "rules": [{
            "name": "mcp-webtransport-handshake-budget",
            "id": "ol-mcp-wt-handshake-rate", "phase": "request", "priority": 100,
            "when": "Request.Http.Path == '/mcp-wt/v1'",
            "actions": [{"type": "rate_limit", "name": "mcp-webtransport-handshakes",
                         "key": "route", "rate": "100r/s", "burst": 100,
                         "max_buckets": 1, "status": 429}],
        }],
    }, "WebTransport handshake WAF rate limit")
    equal(config["rate_limits"], [
        {"name": "mcp-http-edge-budget", "key": "route", "routes": ["mcp-http"],
         "rate": "1000r/s", "burst": 1000, "max_buckets": 1, "status": 429},
    ], "HTTP edge route rate limit")
    upstreams = []
    for port in service["spec"]["ports"]:
        webtransport = port["protocol"] == "UDP"
        upstream = {
            "name": port["name"],
            "origin": f"https://replace-with-private-node.invalid:{port['nodePort']}",
            "max_http_version": "h3" if webtransport else "h1",
            "preserve_host": False, "connect_timeout_ms": 3000, "request_timeout_ms": 40000,
        }
        client_identity = {"client_identity": {"cert_chain": "openlegal-client.pem",
                                               "private_key": "openlegal-client-key.pem"},
                           "ech": {"mode": "disabled"}}
        if webtransport:
            upstream.update(webtransport=True, idle_timeout_ms=65000,
                            tls=client_identity)
        else:
            upstream["pool_max_idle_per_host"] = 0
            upstream["tls"] = client_identity
        upstreams.append(upstream)
    equal(config["upstreams"], upstreams, "NodePort upstreams")
    equal(config["routes"], [
        {"name": "mcp-http", "hosts": ["openlegal4everyone.mcp.publicdata.stream",
                                      "openlegal4everyone.api.publicdata.stream"],
         "upstream": "mcp-http", "compression": "off",
         "limits": {"max_request_body_bytes": 16 * 1024 * 1024},
         "match": {"path": {"exact": "/mcp"}}},
        {"name": "mcp-webtransport", "hosts": ["openlegal4everyone.mcp.publicdata.stream",
                                              "openlegal4everyone.api.publicdata.stream"],
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
            ingestion_raw = validate_ingestion(load_documents(args.ingestion_manifest.read_text()), documents)
            ingestion_settings = document_worker_settings(tomllib.loads(ingestion_raw)["database"]["ingestion"])
        else:
            ingestion_settings = None
        if args.document_controller_role:
            validate_document_controller_role(load_documents(args.document_controller_role.read_text()))
        if args.ingestion_rbac:
            validate_ingestion_rbac(load_documents(args.ingestion_rbac.read_text()))
        if args.network_dir:
            for variant in NETWORK_VARIANTS:
                validate_network(load_documents((args.network_dir / f"{variant}.yaml").read_text()),
                                 [variant])
        if args.document_boundary:
            validate_document_boundary(load_documents(args.document_boundary.read_text()), ingestion_settings)
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
