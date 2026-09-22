#!/usr/bin/env python3
"""Offline serving-template invariants; not Kubernetes schema/cluster validation."""

import argparse
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


def validate_config(raw):
    require(isinstance(raw, str), "server.toml must be text")
    config = tomllib.loads(raw)
    keys(config, ("source", "http", "webtransport", "health", "limits", "text_diff"),
         "text-only server configuration")
    keys(config["source"], ("url",), "source")
    url = config["source"]["url"]
    require(isinstance(url, str), "source URL must be text")
    parsed = urlsplit(url)
    require(parsed.scheme == "https" and bool(parsed.hostname)
            and not parsed.username and not parsed.password, "source URL must be HTTPS without credentials")
    for transport, port in (("http", 8080), ("webtransport", 4433)):
        expected = {
            "bind": f"0.0.0.0:{port}",
            "allowed_hosts": [f"localhost:{port}", f"127.0.0.1:{port}"],
            "allowed_origins": [],
        }
        if transport == "webtransport":
            expected.update(certificate="/run/secrets/backend-tls/tls.crt",
                            private_key="/run/secrets/backend-tls/tls.key")
        equal(config[transport], expected, transport)
    equal(config["health"], {"bind": "0.0.0.0:9090"}, "health")
    equal(config["limits"], {"max_message_bytes": 16 * 1024 * 1024,
                             "max_buffer_bytes": 256 * 1024 * 1024,
                             "shutdown_timeout_secs": 15}, "limits")
    equal(config["text_diff"], {"widget_html": "/opt/openlegal/widgets/text-diff.html"}, "text_diff")
    return raw


def validate(documents):
    require(isinstance(documents, list) and len(documents) == 3,
            "expected only Namespace, ConfigMap and Deployment")
    objects = {}
    for obj in documents:
        require(isinstance(obj, dict), "manifest document must be a mapping")
        kind = obj.get("kind")
        require(isinstance(kind, str) and kind not in objects, "duplicate/missing kind")
        objects[kind] = obj
    keys(objects, ("Namespace", "ConfigMap", "Deployment"), "rendered objects")
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
    raw = validate_config(cm["data"]["server.toml"])
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
    equal(pod["securityContext"], {"runAsNonRoot": True, "runAsUser": 10004, "runAsGroup": 10004,
                                  "fsGroup": 10004, "seccompProfile": {"type": "RuntimeDefault"}}, "Pod security")
    equal(pod["automountServiceAccountToken"], False, "service account token")
    equal(pod["enableServiceLinks"], False, "service environment injection")
    equal(pod["terminationGracePeriodSeconds"], 30, "termination grace")
    require(isinstance(pod["containers"], list) and len(pod["containers"]) == 1, "exactly one serving container required")
    container = pod["containers"][0]
    keys(container, ("name", "image", "imagePullPolicy", "ports", "securityContext", "resources",
                     "livenessProbe", "readinessProbe", "volumeMounts"), "container")
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
    equal(container["resources"], {"requests": {"cpu": "500m", "memory": "512Mi"},
                                   "limits": {"cpu": "2", "memory": "2Gi"}}, "provisional resources")
    for field, path, delay, period in (("livenessProbe", "/live", 30, 10), ("readinessProbe", "/ready", 0, 5)):
        equal(container[field], {"httpGet": {"path": path, "port": "health"}, "initialDelaySeconds": delay,
                                 "periodSeconds": period, "timeoutSeconds": 2, "failureThreshold": 3,
                                 "successThreshold": 1}, field)
    equal(container["volumeMounts"], [{"name": "config", "mountPath": "/etc/openlegal", "readOnly": True},
                                    {"name": "backend-tls", "mountPath": "/run/secrets/backend-tls", "readOnly": True}], "mounts")
    equal(pod["volumes"], [{"name": "config", "configMap": {"name": name, "defaultMode": 0o444}},
                          {"name": "backend-tls", "secret": {"secretName": "openlegal-backend-tls", "defaultMode": 0o440,
                           "items": [{"key": "tls.crt", "path": "tls.crt"}, {"key": "tls.key", "path": "tls.key"}]}}], "volumes")
    return raw


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--config-output", type=Path)
    args = parser.parse_args()
    try:
        raw = validate(load_documents(args.manifest.read_text()))
        if args.config_output:
            args.config_output.write_text(raw)
    except (ValidationError, yaml.YAMLError, tomllib.TOMLDecodeError, OSError, ValueError) as error:
        parser.exit(1, f"Serving template validation failed: {error}\n")
    print("Serving template invariants passed (offline; no Kubernetes API admission or cluster acceptance).")


if __name__ == "__main__":
    main()
