#!/usr/bin/env python3
"""Opt-in real-cluster acceptance. Only synthetic content leaves this process."""
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import time
import uuid

if os.environ.get("RUN_DOCUMENT_SANDBOX_TESTS") != "1":
    raise SystemExit("explicit sandbox test opt-in required")
namespace = os.environ.get("DOCUMENT_NAMESPACE", "openlegal-documents")
image = os.environ["DOCUMENT_IMAGE"]
if "@sha256:" not in image or len(image.rsplit("@sha256:", 1)[1]) != 64:
    raise SystemExit("an immutable image digest is required")
base = [os.environ.get("KUBECTL", "kubectl"), "--kubeconfig", os.environ["DOCUMENT_KUBECONFIG"],
        "--context", os.environ["DOCUMENT_CONTEXT"], "--namespace", namespace, "--request-timeout=30s"]
name = "document-acceptance-" + uuid.uuid4().hex[:12]


def call(args, data=None, timeout=90, check=True):
    return subprocess.run(base + args, input=data, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout, check=check)


def manifest(deadline=300):
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name,
        "labels": {"app.kubernetes.io/name": "openlegal-document-worker"}}, "spec": {
        "runtimeClassName": "openlegal-document", "hostUsers": False,
        "nodeSelector": {"openlegal.document-sandbox/ready": "true"},
        "automountServiceAccountToken": False, "enableServiceLinks": False,
        "restartPolicy": "Never", "activeDeadlineSeconds": deadline, "terminationGracePeriodSeconds": 1,
        "dnsPolicy": "None", "dnsConfig": {"nameservers": ["127.0.0.1"]},
        "securityContext": {"runAsNonRoot": True, "runAsUser": 65532, "runAsGroup": 65532,
            "fsGroup": 65532, "seccompProfile": {"type": "Localhost", "localhostProfile": "openlegal-document.json"}},
        "containers": [{"name": "worker", "image": image,
            "command": ["/usr/local/bin/openlegal-document-worker", "--idle"],
            "env": [{"name": "TMPDIR", "value": "/scratch"}, {"name": "TESSDATA_PREFIX", "value": "/opt/tessdata"},
                    {"name": "OMP_THREAD_LIMIT", "value": "2"}, {"name": "RAYON_NUM_THREADS", "value": "2"}],
            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                "capabilities": {"drop": ["ALL"]}, "appArmorProfile": {"type": "Localhost", "localhostProfile": "openlegal-document"}},
            "resources": {"requests": {"cpu": "2", "memory": "4Gi", "ephemeral-storage": "2Gi"},
                          "limits": {"cpu": "2", "memory": "4Gi", "ephemeral-storage": "2Gi"}},
            "volumeMounts": [{"name": "scratch", "mountPath": "/scratch"}]}],
        "volumes": [{"name": "scratch", "emptyDir": {"sizeLimit": "2Gi"}}]}}


def create(deadline=300):
    call(["create", "-f", "-"], json.dumps(manifest(deadline)).encode())
    call(["wait", "--for=condition=Ready", "pod/" + name, "--timeout=60s"])


def delete():
    call(["delete", "pod", name, "--ignore-not-found", "--wait=true", "--timeout=30s"])


def execute(mode, data=None, check=True):
    return call(["exec", "-i", name, "--container=worker", "--", "/usr/local/bin/openlegal-document-worker", mode], data, 310, check)


try:
    policy = json.loads(call(["get", "networkpolicy", "deny-all", "-o", "json"]).stdout)["spec"]
    assert set(policy["policyTypes"]) == {"Ingress", "Egress"} and not policy.get("ingress") and not policy.get("egress")
    quota = json.loads(call(["get", "resourcequota", "document-budget", "-o", "json"]).stdout)
    assert quota["spec"]["hard"]["pods"] == "2"
    assert not quota["spec"].get("scopes") and quota["spec"].get("scopeSelector") is None
    create()
    report = json.loads(execute("--probe").stdout)
    for key in ["nonroot", "no_new_privileges", "capabilities_dropped", "seccomp_filter", "user_namespace", "network_denied", "root_readonly", "no_token"]:
        assert report[key], key
    assert "openlegal-document" in report["apparmor"] and "enforce" in report["apparmor"]
    # A user-namespaced runc container can expose a wider local pids.max even
    # when the parent Pod cgroup enforces the kubelet limit. The exhaustion
    # probe below tests the effective boundary.
    assert report["memory_max"] == str(4 * 1024**3)
    quota_us, period_us = map(int, report["cpu_max"].split())
    assert quota_us / period_us == 2
    raw = Path("apps/document-worker/tests/fixtures/law.xml").read_bytes()
    header = json.dumps({"format": "xml", "source_sha256": hashlib.sha256(raw).hexdigest(), "ocr": False, "bytes_len": len(raw)}).encode()
    response = execute("--process", struct.pack(">I", len(header)) + header + raw).stdout
    length = struct.unpack(">I", response[:4])[0]
    assert length <= 16 * 1024**2 and len(response) == length + 4
    output = json.loads(response[4:])
    assert output["status"] == "success" and "가상 조문" in output["value"]["text"]
    assert not call(["logs", name, "--container=worker"]).stdout, "document bytes reached Pod logs"
    pid_probe = json.loads(execute("--probe-exhaust-pids").stdout)
    assert pid_probe["denied"] and 0 < pid_probe["created"] < 128
    delete()
    create()
    memory = execute("--probe-exhaust-memory", check=False)
    assert memory.returncode == 137, "expected cgroup OOM termination"
    delete()
    # A shortened deadline verifies the same kubelet enforcement mechanism;
    # the controller always emits 300 seconds for real jobs.
    create(10)
    status = {}
    for _ in range(20):
        status = json.loads(call(["get", "pod", name, "-o", "json"]).stdout)["status"]
        if status.get("reason") == "DeadlineExceeded":
            break
        time.sleep(2)
    assert status.get("reason") == "DeadlineExceeded"
    print("PASS: real runtime isolation, cgroups, XML exec framing, no content logs, PID and memory exhaustion, deadline, cleanup")
finally:
    delete()
