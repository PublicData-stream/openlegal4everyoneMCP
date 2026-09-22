#!/usr/bin/env python3
"""Exercise the alpine gate's startup control flow without Docker or networking."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
HARNESS = ROOT / "scripts/test-alpine-packages.sh"

# Each command runs in a separate process, as it does in the real shell gate.
# Only preparation, Docker and sleep are replaced; tar, shell cleanup and the
# statistics assertions execute normally. Unexpected stub calls fail closed.
STUB = r'''
import io
import json
import os
from pathlib import Path
import sys
import tarfile

command = Path(sys.argv[0]).name
args = sys.argv[1:]
events_path = Path(os.environ["ALPINE_TEST_EVENTS"])
mode = os.environ["ALPINE_TEST_MODE"]

def record(kind, **values):
    with events_path.open("a") as stream:
        stream.write(json.dumps({"kind": kind, **values}) + "\n")

def unexpected():
    print(f"Unexpected stub invocation: {command} {args}", file=sys.stderr)
    sys.exit(97)

if command == "python3":
    if args and args[0] == "test-support/alpine-packages/prepare.py":
        fixture = Path(args[1])
        (fixture / "repository").mkdir()
        for name in ("server.key", "ca.crt", "fixture.rsa.pub"):
            (fixture / name).write_text("synthetic fixture\n")
        (fixture / "server.key").chmod(0o600)
        record("prepare", fixture=str(fixture))
        sys.exit(0)
    if args and args[0] == "-":
        os.execv(sys.executable, [sys.executable, *args])
    unexpected()
elif command == "sleep":
    record("sleep", args=args)
    sys.exit(0)
elif command != "docker":
    unexpected()

record("docker", args=args)
if args[0] == "pull" or args[:2] in (["network", "create"], ["network", "rm"],
                                     ["volume", "create"], ["volume", "rm"]):
    sys.exit(0)
if args[0] == "rm":
    sys.exit(0)
if args[0] == "run":
    if "-i" in args:
        # Consume the actual pipeline so tar failures cannot be hidden by a stub
        # that exits before reading the archive.
        archive = sys.stdin.buffer.read()
        with tarfile.open(fileobj=io.BytesIO(archive)) as stream:
            key = stream.getmember("server.key")
            record("archive", uid=key.uid, gid=key.gid, mode=key.mode)
        sys.exit(0)
    if "/fixture/server.mjs" in args:
        record("origin")
        sys.exit(0)
    if "/fixture/client.sh" in args:
        record("client")
        print("synthetic client passed")
        sys.exit(0)
    unexpected()
if args[0] == "exec":
    if args[2:] == ["test", "-f", "/tmp/ready"]:
        events = [json.loads(line) for line in events_path.read_text().splitlines()]
        attempts = sum(event["kind"] == "probe" for event in events) + 1
        record("probe", attempt=attempts)
        if mode == "ready" or (mode == "delayed" and attempts >= 3):
            sys.exit(0)
        print("synthetic readiness probe noise", file=sys.stderr)
        sys.exit(1)
    if args[2:] == ["cat", "/tmp/stats.json"]:
        print(json.dumps({"packageRequests": 1, "metadataRequests": 2}))
        sys.exit(0)
    unexpected()
if args[0] == "inspect":
    if mode == "inspect-failed":
        print("synthetic inspect unavailable", file=sys.stderr)
        sys.exit(71)
    if "{{.State.Running}}" in args:
        if mode != "inspect-empty":
            print("false" if mode in ("exited", "logs-failed") else "true")
    else:
        print("synthetic state: status=exited exit=1 oom=false")
    sys.exit(0)
if args[0] == "logs":
    if mode == "logs-failed":
        print("synthetic logs unavailable", file=sys.stderr)
        sys.exit(72)
    print("synthetic origin log")
    sys.exit(0)
unexpected()
'''


class AlpineStartupTests(unittest.TestCase):
    def run_gate(self, mode):
        with tempfile.TemporaryDirectory(prefix="alpine-harness-test-") as directory:
            directory = Path(directory)
            commands = directory / "bin"
            commands.mkdir()
            stub = commands / "stub"
            stub.write_text(f"#!{sys.executable}\n" + STUB)
            stub.chmod(0o755)
            for name in ("docker", "python3", "sleep", "openssl"):
                (commands / name).symlink_to(stub)
            events_path = directory / "events.jsonl"
            result = subprocess.run(
                ["bash", str(HARNESS), "baseline"], cwd=ROOT,
                env={**os.environ, "PATH": f"{commands}{os.pathsep}{os.environ['PATH']}",
                     "ALPINE_TEST_EVENTS": str(events_path), "ALPINE_TEST_MODE": mode},
                text=True, capture_output=True, timeout=20,
            )
            events = [json.loads(line) for line in events_path.read_text().splitlines()]
        self.assertNotIn("Unexpected stub invocation", result.stdout + result.stderr)
        self.assertNotIn("synthetic readiness probe noise", result.stderr)
        self.assert_cleanup(events)
        return result, events

    def assert_cleanup(self, events):
        commands = [event["args"] for event in events if event["kind"] == "docker"]
        for resource in ("network", "volume"):
            created = next(args[-1] for args in commands if args[:2] == [resource, "create"])
            self.assertIn([resource, "rm", created], commands)
        origin = next(args[args.index("--name") + 1] for args in commands
                      if args[0] == "run" and "/fixture/server.mjs" in args)
        cleanup = next(args for args in commands if args[:2] == ["rm", "-f"] and len(args) == 4)
        self.assertIn(origin, cleanup)
        self.assertTrue(any(name.startswith("openlegal-alpine-client-") for name in cleanup[2:]))
        fixture = next(event["fixture"] for event in events if event["kind"] == "prepare")
        self.assertFalse(Path(fixture).exists(), "temporary fixture was not removed")

    def assert_failed(self, mode, probes=1):
        result, events = self.run_gate(mode)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("Fixture origin failed to start: scenario=baseline:", result.stderr)
        self.assertEqual(sum(event["kind"] == "probe" for event in events), probes)
        self.assertFalse(any(event["kind"] == "client" for event in events))
        self.assertTrue(any(event["kind"] == "docker" and event["args"][0] == "logs"
                            for event in events))
        self.assertNotIn("Alpine package fixtures passed", result.stdout)
        return result, events

    def test_immediate_readiness_runs_client_and_statistics(self):
        result, events = self.run_gate("ready")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("PASS baseline:", result.stdout)
        self.assertIn("Alpine package fixtures passed: baseline", result.stdout)
        self.assertEqual(sum(event["kind"] == "client" for event in events), 1)
        self.assertEqual(sum(event["kind"] == "probe" for event in events), 1)
        self.assertFalse(any(event["kind"] == "sleep" for event in events))
        archive = next(event for event in events if event["kind"] == "archive")
        self.assertEqual((archive["uid"], archive["gid"], archive["mode"]), (1001, 1001, 0o600))

    def test_running_origin_can_become_ready(self):
        result, events = self.run_gate("delayed")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(sum(event["kind"] == "probe" for event in events), 3)
        self.assertEqual(sum(event["kind"] == "sleep" for event in events), 2)
        self.assertEqual(sum(event["kind"] == "client" for event in events), 1)

    def test_exited_origin_fails_immediately_with_state_and_logs(self):
        result, events = self.assert_failed("exited")
        self.assertIn("synthetic state:", result.stdout + result.stderr)
        self.assertIn("synthetic origin log", result.stdout + result.stderr)
        self.assertFalse(any(event["kind"] == "sleep" for event in events))

    def test_inspection_failure_preserves_primary_failure_and_cleanup(self):
        result, events = self.assert_failed("inspect-failed")
        self.assertIn("synthetic origin log", result.stdout + result.stderr)
        self.assertFalse(any(event["kind"] == "sleep" for event in events))

    def test_empty_inspection_does_not_allow_client_startup(self):
        _, events = self.assert_failed("inspect-empty")
        self.assertFalse(any(event["kind"] == "sleep" for event in events))

    def test_running_unready_origin_has_bounded_timeout(self):
        result, events = self.assert_failed("timeout", probes=30)
        self.assertIn("readiness timeout", result.stderr)
        self.assertLessEqual(sum(event["kind"] == "sleep" for event in events), 30)

    def test_unavailable_logs_preserve_primary_failure_and_cleanup(self):
        result, events = self.assert_failed("logs-failed")
        self.assertIn("synthetic state:", result.stdout + result.stderr)
        self.assertFalse(any(event["kind"] == "sleep" for event in events))


RETRY_STUB = r"""
import json
import os
from pathlib import Path
import sys

command = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["APK_TEST_EVENTS"], "a") as stream:
    stream.write(json.dumps({"command": command, "args": args}) + "\n")
if command == "cp":
    assert args[-1] == "/etc/apk/repositories"
    sys.exit(0)  # Never change the host repository configuration.
if command == "sleep":
    sys.exit(0)
if command == "timeout":
    assert args[:3] == ["-k", "10s", "120s"]
    os.execvp(args[3], args[3:])
assert command == "apk", (command, args)
if args[-1] == "update":
    print("OK: synthetic fresh index")
    sys.exit(0)
assert "download" in args and "add" not in args, args
print(os.environ["APK_TEST_DIAGNOSTIC"])
sys.exit(int(os.environ["APK_TEST_STATUS"]))
"""


class AlpineRetryTests(unittest.TestCase):
    def run_failure(self, status, diagnostic, attempts):
        with tempfile.TemporaryDirectory(prefix="alpine-retry-test-") as directory:
            directory = Path(directory)
            commands = directory / "bin"
            commands.mkdir()
            stub = commands / "stub"
            stub.write_text(f"#!{sys.executable}\n" + RETRY_STUB)
            stub.chmod(0o755)
            for name in ("cp", "timeout", "apk", "sleep"):
                (commands / name).symlink_to(stub)
            event_path = directory / "events"
            result = subprocess.run(
                ["sh", str(ROOT / "scripts/install-alpine-packages.sh"), "example"],
                env={**os.environ, "PATH": f"{commands}{os.pathsep}{os.environ['PATH']}",
                     "TMPDIR": str(directory), "APK_TEST_EVENTS": str(event_path),
                     "APK_TEST_STATUS": str(status), "APK_TEST_DIAGNOSTIC": diagnostic},
                text=True, capture_output=True, timeout=10,
            )
            events = [json.loads(line) for line in event_path.read_text().splitlines()]
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertNotIn("Traceback", result.stdout + result.stderr)
            requests = [event for event in events if event["command"] == "apk"
                        and "download" in event["args"]]
            self.assertEqual(len(requests), attempts, result.stdout + result.stderr)
            sleeps = [event["args"] for event in events if event["command"] == "sleep"]
            self.assertEqual(sleeps, [[str(2**i)] for i in range(attempts - 1)])
            self.assertFalse(any(path.name.startswith("tmp.") for path in directory.iterdir()))

    def test_transient_failures_have_six_attempts_and_exact_backoffs(self):
        self.run_failure(99, "ERROR: example: HTTP 503: Service Unavailable", 6)

    def test_busybox_watchdog_retries_acquisition_only(self):
        self.run_failure(143, "", 6)

    def test_gnu_watchdog_retries_acquisition_only(self):
        self.run_failure(124, "", 6)

    def test_sigkill_is_not_assumed_to_be_a_network_timeout(self):
        self.run_failure(137, "", 1)

    def test_sigkill_after_a_transient_diagnostic_is_still_terminal(self):
        self.run_failure(137, "WARNING: example: HTTP 503: Service Unavailable", 1)

    def test_unknown_error_is_terminal(self):
        self.run_failure(1, "ERROR: example: unexpected failure", 1)

    def test_mixed_trust_and_transient_diagnostics_are_terminal(self):
        self.run_failure(2, "ERROR: main: UNTRUSTED signature\n"
                         "WARNING: community: HTTP 503: Service Unavailable", 1)

    def test_watchdog_cannot_override_a_trust_failure(self):
        self.run_failure(143, "ERROR: main: UNTRUSTED signature", 1)


class AlpineArgumentTests(unittest.TestCase):
    def test_rejects_options_archives_selectors_and_empty_input_before_mutation(self):
        helper = ROOT / "scripts/install-alpine-packages.sh"
        for arguments in ([], [""], ["--allow-untrusted"], ["/tmp/package.apk"],
                          ["https://example.invalid/p.apk"], ["name@edge"],
                          ["name=1.0"], ["name\nother"], ["UPPER"]):
            with self.subTest(arguments=arguments):
                result = subprocess.run(["sh", str(helper), *arguments],
                                        text=True, capture_output=True, timeout=5)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertNotIn("Retrying", result.stderr)


if __name__ == "__main__":
    unittest.main()
