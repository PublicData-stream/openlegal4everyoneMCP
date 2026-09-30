"""Exercise sandbox acceptance against synthetic admitted Pods and runtime reports."""
import copy
import io
import json
import os
from pathlib import Path
import runpy
import struct
import subprocess
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]


class DocumentSandboxAcceptanceTests(unittest.TestCase):
    def run_acceptance(self, medium="Memory", scratch_tmpfs=True):
        created = []
        deleted = []
        current = None

        def run(command, input=None, **kwargs):
            nonlocal current
            args = command[8:]
            operation = args[0]
            output = b""
            returncode = 0
            if operation == "create":
                current = json.loads(input)
                created.append(copy.deepcopy(current))
            elif operation == "get" and args[1] == "networkpolicy":
                output = json.dumps({"spec": {"policyTypes": ["Ingress", "Egress"]}}).encode()
            elif operation == "get" and args[1] == "resourcequota":
                output = json.dumps({"spec": {"hard": {
                    "pods": "2", "requests.cpu": "4", "limits.cpu": "4",
                    "requests.memory": "8Gi", "limits.memory": "8Gi",
                    "requests.ephemeral-storage": "4Gi", "limits.ephemeral-storage": "4Gi",
                }}}).encode()
            elif operation == "get" and args[1] == "pod":
                admitted = copy.deepcopy(current)
                scratch = admitted["spec"]["volumes"][0]["emptyDir"]
                if medium is None:
                    scratch.pop("medium", None)
                else:
                    scratch["medium"] = medium
                if current["spec"]["activeDeadlineSeconds"] == 10:
                    admitted["status"] = {"reason": "DeadlineExceeded"}
                output = json.dumps(admitted).encode()
            elif operation == "exec":
                mode = args[-1]
                if mode == "--probe":
                    report = dict.fromkeys([
                        "nonroot", "no_new_privileges", "capabilities_dropped",
                        "seccomp_filter", "user_namespace", "network_denied",
                        "root_readonly", "no_token",
                    ], True)
                    report.update(scratch_tmpfs=scratch_tmpfs,
                                  apparmor="openlegal-document (enforce)",
                                  memory_max=str(4 * 1024**3), cpu_max="200000 100000")
                    output = json.dumps(report).encode()
                elif mode == "--process":
                    result = json.dumps({"status": "success", "value": {"text": "가상 조문"}}).encode()
                    output = struct.pack(">I", len(result)) + result
                elif mode == "--probe-exhaust-pids":
                    output = b'{"denied": true, "created": 127}'
                elif mode == "--probe-exhaust-memory":
                    returncode = 137
                else:
                    raise AssertionError(f"unexpected exec mode: {mode}")
            elif operation == "delete":
                deleted.append(args[2])
            elif operation not in ("wait", "logs"):
                raise AssertionError(f"unexpected command: {args}")
            return subprocess.CompletedProcess(command, returncode, output, b"")

        environment = {
            "RUN_DOCUMENT_SANDBOX_TESTS": "1", "DOCUMENT_KUBECONFIG": "/synthetic/kubeconfig",
            "DOCUMENT_CONTEXT": "synthetic", "DOCUMENT_NAMESPACE": "openlegal-documents",
            "DOCUMENT_IMAGE": "worker@sha256:" + "a" * 64,
        }
        try:
            with patch.dict(os.environ, environment, clear=True), patch("subprocess.run", side_effect=run), \
                    patch("sys.stdout", new_callable=io.StringIO):
                runpy.run_path(str(ROOT / "deploy/document-sandbox/acceptance.py"), run_name="__main__")
        finally:
            self.assertTrue(deleted, "acceptance must delete its synthetic Pod even on failure")
        return created

    def test_tmpfs_manifest_and_runtime_complete_acceptance(self):
        created = self.run_acceptance()
        self.assertEqual(len(created), 3)
        for pod in created:
            self.assertEqual(pod["spec"]["volumes"], [{"name": "scratch", "emptyDir": {
                "medium": "Memory", "sizeLimit": "2Gi",
            }}])

    def test_admission_cannot_remove_or_change_memory_medium(self):
        for medium in (None, ""):
            with self.subTest(medium=medium), self.assertRaisesRegex(AssertionError, "must use tmpfs"):
                self.run_acceptance(medium=medium)

    def test_runtime_disk_scratch_fails_even_with_memory_manifest(self):
        with self.assertRaisesRegex(AssertionError, "scratch_tmpfs"):
            self.run_acceptance(scratch_tmpfs=False)


if __name__ == "__main__":
    unittest.main()
