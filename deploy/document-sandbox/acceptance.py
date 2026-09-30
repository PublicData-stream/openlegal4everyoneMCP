#!/usr/bin/env python3
"""Opt-in real-cluster acceptance. Only synthetic content leaves this process."""
import hashlib
import json
import os
import re
import tomllib
from fractions import Fraction
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

DEFAULT_WORKER = {"cpu": "2", "memory": "4Gi", "scratch": "2Gi", "pool_limit": 2}
QUANTITY = re.compile(r"([+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))"
                      r"(n|u|m|k|M|G|T|P|E|Ki|Mi|Gi|Ti|Pi|Ei|[eE][+-]?[0-9]+)?\Z")
QUANTITY_SCALE = {
    "": 1, "n": Fraction(1, 10**9), "u": Fraction(1, 10**6),
    "m": Fraction(1, 1000), "k": 10**3, "M": 10**6, "G": 10**9,
    "T": 10**12, "P": 10**15, "E": 10**18,
    "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40,
    "Pi": 2**50, "Ei": 2**60,
}


def quantity(value):
    """Return a Kubernetes Quantity as exact, rounded milli base units."""
    if not isinstance(value, str) or len(value) > 128:
        raise ValueError("resource quantity must be a bounded string")
    match = QUANTITY.fullmatch(value)
    if not match:
        raise ValueError(f"invalid resource quantity: {value!r}")
    number = Fraction(match.group(1))
    if number <= 0:
        raise ValueError("resource quantity must be positive")
    suffix = match.group(2) or ""
    if suffix in QUANTITY_SCALE:
        scale = QUANTITY_SCALE[suffix]
    else:
        exponent = int(suffix[1:])
        if not -(2**31) <= exponent <= 2**31 - 1:
            raise ValueError("resource quantity exponent is out of range")
        if exponent < -200:
            return 1
        if exponent > 200:
            raise ValueError("resource quantity exceeds representable range")
        scale = Fraction(10) ** exponent
    amount = number * scale * 1000
    rounded = -(-amount.numerator // amount.denominator)
    if rounded > (2**63 - 1) * 1000:
        raise ValueError("resource quantity exceeds representable range")
    return rounded


def worker_settings():
    selected = DEFAULT_WORKER.copy()
    config_path = os.environ.get("DOCUMENT_SERVER_CONFIG")
    if config_path is not None:
        if not config_path:
            raise ValueError("DOCUMENT_SERVER_CONFIG must name an ingestion TOML file")
        config = tomllib.loads(Path(config_path).read_text())
        ingestion = config["database"]["ingestion"]
        if ingestion.get("worker_image") != image:
            raise ValueError("DOCUMENT_IMAGE must match ingestion.worker_image")
        overrides = ingestion.get("document_worker", {})
        if not isinstance(overrides, dict) or set(overrides) - set(selected):
            raise ValueError("invalid document_worker settings")
        selected.update(overrides)
    for key in ("cpu", "memory", "scratch"):
        quantity(selected[key])
    limit = selected["pool_limit"]
    if isinstance(limit, bool) or not isinstance(limit, int) or not 0 < limit <= 2**32 - 1:
        raise ValueError("document_worker.pool_limit must be a positive representable integer")
    for key in ("cpu", "memory", "scratch"):
        if quantity(selected[key]) * limit > (2**63 - 1) * 1000:
            raise ValueError(f"document_worker pool {key} total is not representable")
    return selected


worker = worker_settings()


def assert_quota(quota):
    spec = quota["spec"]
    hard = spec["hard"]
    assert set(spec) == {"hard"}, "quota must be unscoped"
    assert set(hard) == {"pods", "requests.cpu", "limits.cpu", "requests.memory",
                         "limits.memory", "requests.ephemeral-storage",
                         "limits.ephemeral-storage"}, "quota resource set"
    assert quantity(hard["pods"]) == worker["pool_limit"] * 1000, "Pod quota"
    for resource, selected in (("cpu", worker["cpu"]), ("memory", worker["memory"]),
                               ("ephemeral-storage", worker["scratch"])):
        expected = quantity(selected) * worker["pool_limit"]
        for bound in ("requests", "limits"):
            assert quantity(hard[f"{bound}.{resource}"]) == expected, f"{bound}.{resource} quota"


def assert_created_pod(pod):
    """Detect admission mutations of the resource profile under qualification."""
    spec = pod["spec"]
    assert len(spec["containers"]) == 1 and not spec.get("initContainers")
    assert not spec.get("ephemeralContainers")
    resources = spec["containers"][0]["resources"]
    for resource, selected in (("cpu", worker["cpu"]), ("memory", worker["memory"]),
                               ("ephemeral-storage", worker["scratch"])):
        for bound in ("requests", "limits"):
            assert quantity(resources[bound][resource]) == quantity(selected), f"Pod {bound}.{resource}"
    scratch = next(volume["emptyDir"] for volume in spec["volumes"]
                   if volume["name"] == "scratch")
    assert scratch.get("medium") == "Memory", "Pod scratch must use tmpfs"
    assert quantity(scratch["sizeLimit"]) == quantity(worker["scratch"]), "Pod scratch size"


def call(args, data=None, timeout=90, check=True):
    return subprocess.run(base + args, input=data, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout, check=check)


def manifest(deadline=300):
    resources = {"cpu": worker["cpu"], "memory": worker["memory"],
                 "ephemeral-storage": worker["scratch"]}
    threads = str(min(64, (quantity(worker["cpu"]) + 999) // 1000))
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
                    {"name": "OMP_THREAD_LIMIT", "value": threads}, {"name": "RAYON_NUM_THREADS", "value": threads}],
            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                "capabilities": {"drop": ["ALL"]}, "appArmorProfile": {"type": "Localhost", "localhostProfile": "openlegal-document"}},
            "resources": {"requests": resources, "limits": resources.copy()},
            "volumeMounts": [{"name": "scratch", "mountPath": "/scratch"}]}],
        "volumes": [{"name": "scratch", "emptyDir": {"medium": "Memory", "sizeLimit": worker["scratch"]}}]}}


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
    assert_quota(quota)
    create()
    assert_created_pod(json.loads(call(["get", "pod", name, "-o", "json"]).stdout))
    report = json.loads(execute("--probe").stdout)
    for key in ["nonroot", "no_new_privileges", "capabilities_dropped", "seccomp_filter", "user_namespace", "network_denied", "root_readonly", "no_token", "scratch_tmpfs"]:
        assert report[key], key
    assert "openlegal-document" in report["apparmor"] and "enforce" in report["apparmor"]
    # A user-namespaced runc container can expose a wider local pids.max even
    # when the parent Pod cgroup enforces the kubelet limit. The exhaustion
    # probe below tests the effective boundary.
    expected_memory = (quantity(worker["memory"]) + 999) // 1000
    assert report["memory_max"] == str(expected_memory)
    quota_us, period_us = map(int, report["cpu_max"].split())
    assert quota_us > 0 and period_us > 0
    requested_cpu_milli = quantity(worker["cpu"])
    # Kubelet converts millicores to whole microseconds with integer division
    # and enforces a 1 ms minimum; compare without floating point.
    expected_quota = max(1000, requested_cpu_milli * period_us // 1000)
    assert quota_us == expected_quota, "CPU cgroup quota"
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
    # The worker's fixed probe touches 5 GiB. Above that selected limit it
    # cannot establish OOM enforcement, so report the narrower evidence.
    memory_exhaustion = quantity(worker["memory"]) < 5 * 1024**3 * 1000
    if memory_exhaustion:
        create()
        memory = execute("--probe-exhaust-memory", check=False)
        assert memory.returncode == 137, "expected cgroup OOM termination"
        delete()
    else:
        print("SKIP: memory exhaustion probe (selected limit is at least 5 GiB; memory.max was checked)")
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
    evidence = "memory exhaustion" if memory_exhaustion else "memory.max only (exhaustion untested)"
    print(f"PASS: real runtime isolation, scratch tmpfs, cgroups, XML exec framing, no content logs, PID, {evidence}, deadline, cleanup")
finally:
    delete()
