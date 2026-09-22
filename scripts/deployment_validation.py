#!/usr/bin/env python3
"""Offline serving/admin invariants; not Kubernetes schema/cluster validation."""

import argparse
import copy
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
        kinds += ("Service",)
    require(isinstance(documents, list) and len(documents) == len(kinds),
            f"expected only {', '.join(kinds)}")
    objects = {}
    for obj in documents:
        require(isinstance(obj, dict), "manifest document must be a mapping")
        kind = obj.get("kind")
        require(isinstance(kind, str) and kind not in objects, "duplicate/missing kind")
        objects[kind] = obj
    keys(objects, kinds, "rendered objects")
    if profile == "retained":
        validate_service(objects["Service"])
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
    image = container["image"]
    require(isinstance(image, str) and re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}", image),
            "image must use a sha256 digest reference")
    require(":" not in image.split("@", 1)[0].rsplit("/", 1)[-1], "image tags are not supported")
    if image.endswith("@sha256:" + "0" * 64):
        require(image == "registry.example/openlegal-server@sha256:" + "0" * 64,
                "only the documented image placeholder may use a zero digest")
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
    args = parser.parse_args()
    try:
        documents = load_documents(args.manifest.read_text())
        raw = validate(documents, args.profile)
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
    except (ValidationError, yaml.YAMLError, tomllib.TOMLDecodeError, OSError, ValueError) as error:
        parser.exit(1, f"Deployment template validation failed: {error}\n")
    print("Deployment template invariants passed (offline; no Kubernetes API admission or cluster acceptance).")


if __name__ == "__main__":
    main()
