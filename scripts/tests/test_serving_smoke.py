#!/usr/bin/env python3
"""Deterministic synthetic serving-smoke tests; no legal providers or clusters."""
import contextlib
import http.server
import importlib.util
import io
import json
from pathlib import Path
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("serving_smoke", Path(__file__).parents[1] / "serving_smoke.py")
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)
REQUEST = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"_meta": {"progressToken": "test"}}}
FINAL = {"jsonrpc": "2.0", "id": 1, "result": {"ok": True}}
PROGRESS = {"jsonrpc": "2.0", "method": "notifications/progress", "params": {"progressToken": "test", "progress": 1, "total": 2}}
PATCH = "--- before\n+++ after\n@@ -1,2 +1,2 @@\n first\n-old\n+new\n"


def event(value):
    return b"data: " + json.dumps(value).encode() + b"\n\n"


class Response(io.BytesIO):
    def __init__(self, data, mime="text/event-stream"):
        super().__init__(data)
        self.mime = mime

    def getheader(self, _name, _default=""):
        return self.mime


class ParsingTests(unittest.TestCase):
    def failed(self, reason, function, *args):
        with self.assertRaises(smoke.Failure) as caught:
            function(*args)
        self.assertEqual(caught.exception.reason, reason)

    def test_explicit_endpoint_contract(self):
        self.assertEqual(smoke.endpoint("https://example.test:443/mcp", "/mcp").hostname, "example.test")
        smoke.endpoint("https://[::1]:443/mcp", "/mcp")
        smoke.endpoint("http://127.0.0.1:9090/live", "/live", private=True)
        smoke.endpoint("https://client.test", origin=True)
        for value in ("http://example.test/mcp", "https://user:secret@example.test/mcp", "https://example.test/mcp?secret",
                      "https://example.test/mcp#", "https://example.test/mcp?", "https://example.test\\@other/mcp",
                      "https://example.test/mcp\n", "https://example.test:0/mcp", "https://example.test:65536/mcp",
                      "https://%65xample.test/mcp", "https://example..test/mcp", "https://example.test/mcp/",
                      "https://-example.test/mcp"):
            with self.subTest(value=value):
                self.failed("invalid_arguments", smoke.endpoint, value, "/mcp")
        self.failed("invalid_arguments", lambda: smoke.endpoint("https://example.test/", origin=True))
        self.failed("invalid_arguments", lambda: smoke.endpoint("http://example.test", origin=True))

    def test_json_duplicate_invalid_utf8_nonfinite_and_nesting(self):
        for raw in (b'{"a":1,"a":2}', b'"\xff"', b'{"a":NaN}', b'[' * 2000):
            self.failed("invalid_response", smoke.decode_json, raw)
        with patch.object(smoke, "MAX_BODY", 4):
            self.failed("response_limit", smoke.decode_json, b'12345')

    def test_sse_final_does_not_read_eof(self):
        response = Response(event(PROGRESS) + event(FINAL) + b"unread trailing bytes")
        self.assertEqual(smoke.read_rpc(response, REQUEST), {"ok": True})
        self.assertEqual(response.read(), b"unread trailing bytes")

    def test_sse_multiline_comments_crlf(self):
        raw = b": heartbeat\r\nevent: message\r\n" + event(PROGRESS)
        raw += b'data: {"jsonrpc":"2.0",\r\ndata: "id":1,"result":{"ok":true}}\r\n\r\n'
        self.assertEqual(smoke.read_rpc(Response(raw), REQUEST), {"ok": True})

    def test_sse_requires_progress(self):
        self.failed("missing_progress", smoke.read_rpc, Response(event(FINAL)), REQUEST)
        self.failed("missing_progress", smoke.read_rpc, Response(json.dumps(FINAL).encode(), "application/json"), REQUEST)

    def test_progress_token_increasing_finite_and_count(self):
        for change in ({"progressToken": "other"}, {"progress": 0}, {"progress": True}, {"progress": -1}, {"total": 0}):
            progress = {**PROGRESS, "params": {**PROGRESS["params"], **change}}
            self.failed("invalid_progress", smoke.read_rpc, Response(event(progress) + event(FINAL)), REQUEST)
        self.failed("invalid_progress", smoke.read_rpc, Response(event(PROGRESS) * 2 + event(FINAL)), REQUEST)
        notifications = b"".join(event({**PROGRESS, "params": {"progressToken": "test", "progress": n}}) for n in range(1, 7))
        self.failed("invalid_progress", smoke.read_rpc, Response(notifications + event(FINAL)), REQUEST)

    def test_unmatched_ids_and_tool_errors(self):
        for final in ({**FINAL, "id": 2}, {**FINAL, "id": True}, {**FINAL, "error": {}},
                      {**FINAL, "result": {"isError": True}}, {**FINAL, "jsonrpc": "1.0"}):
            self.failed("invalid_response", smoke.read_rpc, Response(event(PROGRESS) + event(final)), REQUEST)

    def test_body_and_sse_comments_bounded(self):
        with patch.object(smoke, "MAX_BODY", 32):
            self.failed("response_limit", smoke.read_rpc, Response(b":" + b"x" * 100), REQUEST)
            self.failed("response_limit", smoke.read_rpc, Response(b" " * 33, "application/json"), REQUEST)

    def test_eof_and_unknown_mime_fail(self):
        self.failed("invalid_response", smoke.read_rpc, Response(b""), REQUEST)
        self.failed("invalid_response", smoke.read_rpc, Response(b"{}", "text/html"), REQUEST)

    def test_http_source_offer_matches_discovery(self):
        info = {"license": "AGPL-3.0-only", "licenseUrl": "https://www.gnu.org/licenses/agpl-3.0.html",
                "sourceUrl": "https://example.test/source?revision=fixture#download"}
        instructions = " ".join(info.values())
        smoke.validate_source_info(info, instructions)
        for source in ("", "http://example.test/source", "https://secret@example.test/source", "https://", "https://example.test/source\n"):
            self.failed("content_mismatch", smoke.validate_source_info, {**info, "sourceUrl": source}, instructions)
        self.failed("content_mismatch", smoke.validate_source_info, {**info, "licenseUrl": "https://wrong.test"}, instructions)
        self.failed("content_mismatch", smoke.validate_source_info, info, "AGPL-3.0-only")

    def test_absolute_deadline(self):
        start = time.monotonic()
        with self.assertRaises(smoke.Failure) as caught:
            with smoke.deadline(0.03):
                time.sleep(1)
        self.assertEqual(caught.exception.reason, "deadline")
        self.assertLess(time.monotonic() - start, 0.3)


class LifecycleClient:
    def __init__(self, corrupt=False, fail_delete=False, malformed_patch=False):
        self.calls = []
        self.corrupt, self.fail_delete, self.malformed_patch = corrupt, fail_delete, malformed_patch

    def call(self, _revision, name, arguments, _progress=False):
        self.calls.append((name, arguments))
        if name == "text.diff":
            return {"schema_version": 1, "comparison": {"comparison_id": "a" * 64, "equal": self.corrupt,
                    "additions": 1, "deletions": 1}, "patch": {"attachment_id": "bad" if self.malformed_patch else "b" * 64,
                    "sealed": True, "kind": "patch", "total_bytes": len(PATCH)}, "explanation": "synthetic"}
        if name == "text.diff.page":
            return {**arguments, "total_pages": 1, "text": "first\n" + ("old\n" if arguments["view"] == "before" else "new\n")}
        if name == "text.attachment.read":
            return {"complete": True, "offset": 0, "attachment": {"attachment_id": "b" * 64}, "text": PATCH, "next_offset": len(PATCH)}
        if name.endswith("delete"):
            if self.fail_delete and name == "text.diff.delete":
                raise RuntimeError("sensitive diagnostic")
            return {"schema_version": 1, "deleted": True}
        raise AssertionError("unexpected operation")


class LifecycleTests(unittest.TestCase):
    def test_exact_content_and_only_owned_cleanup(self):
        client, cleanup = LifecycleClient(), []
        smoke.check_diff(client, smoke.REVISIONS[0], cleanup)
        self.assertEqual([c["status"] for c in cleanup], ["passed", "passed"])
        self.assertEqual(client.calls[-2:], [("text.diff.delete", {"comparison_id": "a" * 64}),
                                           ("text.attachment.delete", {"attachment_id": "b" * 64})])
        self.assertNotIn("a" * 64, json.dumps(cleanup))

    def test_content_failure_still_cleans_both(self):
        client, cleanup = LifecycleClient(corrupt=True), []
        with self.assertRaises(smoke.Failure):
            smoke.check_diff(client, smoke.REVISIONS[0], cleanup)
        self.assertEqual(len(cleanup), 2)

    def test_cleanup_failure_does_not_skip_second_handle(self):
        client, cleanup = LifecycleClient(fail_delete=True), []
        smoke.check_diff(client, smoke.REVISIONS[0], cleanup)
        self.assertEqual([c["status"] for c in cleanup], ["failed", "passed"])
        self.assertNotIn("sensitive", json.dumps(cleanup))

    def test_malformed_sibling_still_cleans_valid_handle(self):
        client, cleanup = LifecycleClient(malformed_patch=True), []
        with self.assertRaises(smoke.Failure):
            smoke.check_diff(client, smoke.REVISIONS[0], cleanup)
        self.assertEqual(len(cleanup), 1)
        self.assertEqual(client.calls[-1], ("text.diff.delete", {"comparison_id": "a" * 64}))


class ProcessTests(unittest.TestCase):
    def test_output_cap_and_failure_redaction(self):
        with self.assertRaises(smoke.Failure) as caught:
            smoke.subprocess_output([sys.executable, "-c", "print('secret'*1000)"], 2, 100)
        self.assertEqual(caught.exception.reason, "response_limit")
        with self.assertRaises(smoke.Failure) as caught:
            smoke.subprocess_output([sys.executable, "-c", "import sys;sys.stderr.write('secret');sys.exit(1)"], 2)
        self.assertEqual(str(caught.exception), "client_failed")

    def test_subprocess_deadline(self):
        with self.assertRaises(smoke.Failure) as caught:
            smoke.subprocess_output([sys.executable, "-c", "import time;time.sleep(10)"], .05)
        self.assertEqual(caught.exception.reason, "deadline")

    def test_webtransport_requires_complete_report(self):
        args = types.SimpleNamespace(wt_client="/unused", webtransport_url="https://example.test/mcp-wt/v1",
                                     ca_file="/unused", origin="https://client.test")
        with patch.object(smoke, "subprocess_output", return_value=(0, b'{"schema_version":1,"checks":[{"id":"connect","status":"passed"}]}')):
            with self.assertRaises(smoke.Failure):
                smoke.webtransport(args, smoke.REVISIONS[0], time.monotonic() + 1, [])

    def test_native_cleanup_failure_survives_sanitized_report(self):
        args = types.SimpleNamespace(wt_client="/unused", webtransport_url="https://example.test/mcp-wt/v1",
                                     ca_file="/unused", origin="https://client.test")
        ids = ("configuration", "connect", "discovery", "tools_list", "server_info", "text_diff",
               "comparison_content", "patch_content", "comparison_cleanup", "attachment_cleanup", "invalid_origin")
        value = {"schema_version": 1, "secret": "never-copy-this", "checks": [
            {"id": name, "status": "failed" if name == "comparison_cleanup" else "passed",
             "reason_code": "private-diagnostic"} for name in ids]}
        details = []
        with patch.object(smoke, "subprocess_output", return_value=(1, json.dumps(value).encode())):
            with self.assertRaises(smoke.Failure) as caught:
                smoke.webtransport(args, smoke.REVISIONS[0], time.monotonic() + 1, details)
        self.assertEqual(caught.exception.reason, "client_failed")
        self.assertEqual(next(c for c in details if c["id"].endswith("comparison_cleanup"))["status"], "failed")
        self.assertNotIn("private-diagnostic", json.dumps(details))
        self.assertNotIn("never-copy-this", json.dumps(details))

    def test_report_exclusive_creation_and_safe_output(self):
        report = {"schema_version": 1, "status": "passed", "evidence_scope": "configured_endpoint", "checks": []}
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "report.json"
            args = types.SimpleNamespace(report=str(target))
            with patch.object(smoke, "arguments", return_value=args), patch.object(smoke, "run", return_value=report):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(smoke.main([]), 0)
                self.assertEqual(json.loads(target.read_text()), report)
                err = io.StringIO()
                with contextlib.redirect_stderr(err):
                    self.assertEqual(smoke.main([]), 1)
                self.assertEqual(err.getvalue(), "serving_smoke: failed (io_failure)\n")
                self.assertEqual(json.loads(target.read_text()), report)

    def test_argument_errors_never_echo_credentials(self):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertEqual(smoke.main(["--secret-private-value"]), 1)
        self.assertEqual(err.getvalue(), "serving_smoke: failed (invalid_arguments)\n")

    def test_private_kube_uses_selected_fields(self):
        args = types.SimpleNamespace(kubectl="/kubectl", kubeconfig="/fixture/config", context="fixture", namespace="fixture", pod="serving")
        with patch.object(smoke, "subprocess_output", return_value=b"fixture-uid\nRunning\n\nTrue\n") as call, patch.object(smoke.time, "sleep"):
            smoke.pod_readiness(args)
        self.assertEqual(call.call_count, 2)
        command = call.call_args.args[0]
        self.assertIn("pod/serving", command)
        self.assertIn("get", command)
        self.assertEqual(command[-1], 'jsonpath={.metadata.uid}{"\\n"}{.status.phase}{"\\n"}'
                         '{.metadata.deletionTimestamp}{"\\n"}'
                         '{.status.conditions[?(@.type=="Ready")].status}{"\\n"}')
        self.assertNotIn("secrets", command)
        self.assertLessEqual(call.call_args.args[1], smoke.OP_SECONDS)

    def test_private_kube_waits_for_running_and_ready(self):
        args = types.SimpleNamespace(kubectl="/kubectl", kubeconfig="/fixture/config", context="fixture", namespace="fixture", pod="serving")
        observations = [b"fixture-uid\nPending\n\nTrue\n", b"fixture-uid\nRunning\n\nFalse\n",
                        b"fixture-uid\nRunning\n\nTrue\n"]
        with patch.object(smoke, "subprocess_output", side_effect=observations) as call, patch.object(smoke.time, "sleep"):
            smoke.pod_readiness(args)
        self.assertEqual(call.call_count, 3)

    def test_private_kube_rejects_termination_replacement_and_malformed_fields(self):
        args = types.SimpleNamespace(kubectl="/kubectl", kubeconfig="/fixture/config", context="fixture", namespace="fixture", pod="serving")
        for bad in (b"fixture-uid\nRunning\n2026-09-22T00:00:00Z\nTrue\n", b"replacement\nRunning\n\nTrue\n",
                    b"fixture-uid\nFailed\n\nTrue\n", b"fixture-uid\nSucceeded\n\nTrue\n", b"\nRunning\n\nTrue\n",
                    b"fixture-uid\nRunning\n\nTrue False\n", b"True", b"fixture-uid\nRunning\n\nTrue\nextra"):
            with self.subTest(bad=bad), patch.object(smoke, "subprocess_output", side_effect=[b"fixture-uid\nPending\n\nFalse\n", bad]), patch.object(smoke.time, "sleep"):
                with self.assertRaises(smoke.Failure) as caught:
                    smoke.pod_readiness(args)
                self.assertEqual(caught.exception.reason, "invalid_response")

    def test_private_kube_has_one_absolute_polling_budget(self):
        args = types.SimpleNamespace(kubectl="/kubectl", kubeconfig="/fixture/config", context="fixture", namespace="fixture", pod="serving")
        with patch.object(smoke, "subprocess_output", return_value=b"fixture-uid\nPending\n\nFalse\n") as call, patch.object(smoke.time, "monotonic", side_effect=[0, 359, 361]):
            with self.assertRaises(smoke.Failure) as caught:
                smoke.pod_readiness(args)
        self.assertEqual(caught.exception.reason, "deadline")
        self.assertEqual(call.call_args.args[1], 1)


class TlsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix="serving-smoke-test-")
        path = Path(cls.directory.name)
        cls.cert, cls.key = path / "cert.pem", path / "key.pem"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
                        "-keyout", str(cls.key), "-out", str(cls.cert)], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        cls.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        cls.context.load_verify_locations(cafile=cls.cert)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    @contextlib.contextmanager
    def server(self, mode):
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                self.do_GET()

            def do_GET(self):
                try:
                    if mode == "trickle_headers":
                        self.wfile.write(b"HTTP/1.1 200 OK\r\nX-Test: ")
                        for _ in range(20):
                            self.wfile.write(b"x")
                            self.wfile.flush()
                            time.sleep(.02)
                        return
                    status = 302 if mode == "redirect" else 403 if mode == "denied" else 200
                    self.send_response(status)
                    self.send_header("Content-Type", "text/event-stream")
                    self.end_headers()
                    if mode == "sse":
                        self.wfile.write(event(PROGRESS) + event(FINAL))
                        self.wfile.flush()
                        time.sleep(.3)
                    elif mode == "trickle_body":
                        for _ in range(20):
                            self.wfile.write(b":x\n")
                            self.wfile.flush()
                            time.sleep(.02)
                except (OSError, ssl.SSLError):
                    pass
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(self.cert, self.key)
        server.socket = tls.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield smoke.endpoint(f"https://localhost:{server.server_port}/mcp", "/mcp")
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def client(self, url, context=None):
        return smoke.HttpClient(url, "https://client.test", context or self.context, time.monotonic() + 5)

    def test_verified_tls_and_sse_without_eof(self):
        with self.server("sse") as url:
            start = time.monotonic()
            result = self.client(url).exchange(REQUEST, smoke.REVISIONS[0])
            self.assertEqual(result, {"ok": True})
            self.assertLess(time.monotonic() - start, .25)

    def test_ca_bundle_requires_only_valid_certificates(self):
        cert = self.cert.read_bytes()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bundle.pem"
            path.write_bytes(cert + b"\n" + cert)
            context = smoke.certificate_context(path)
            self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
            self.assertTrue(context.check_hostname)
            for bad in (b"", b"garbage", b"x" * (1024 * 1024 + 1), b"\xff",
                        b"-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----",
                        b"-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----",
                        cert + b"trailing garbage", b"garbage" + cert, cert + self.key.read_bytes()):
                path.write_bytes(bad)
                with self.subTest(size=len(bad)), self.assertRaises(smoke.Failure) as caught:
                    smoke.certificate_context(path)
                self.assertEqual(caught.exception.reason, "invalid_arguments")

    def test_tls_failure_is_never_policy_denial(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with self.server("denied") as url:
            with self.assertRaises(smoke.Failure) as caught:
                self.client(url, context).exchange(expected=403)
            self.assertEqual(caught.exception.reason, "tls_failure")

    def test_hostname_verification(self):
        with self.server("denied") as url:
            bad = url._replace(netloc=f"127.0.0.1:{url.port}")
            with self.assertRaises(smoke.Failure) as caught:
                self.client(bad).exchange(expected=403)
            self.assertEqual(caught.exception.reason, "tls_failure")

    def test_redirect_not_followed(self):
        with self.server("redirect") as url:
            with self.assertRaises(smoke.Failure) as caught:
                self.client(url).exchange()
            self.assertEqual(caught.exception.reason, "unexpected_status")

    def test_specific_denial_status(self):
        with self.server("denied") as url:
            self.assertIsNone(self.client(url).exchange(expected=403))
            with self.assertRaises(smoke.Failure) as caught:
                self.client(url).exchange(expected=404)
            self.assertEqual(caught.exception.reason, "unexpected_status")

    def test_absolute_deadline_stops_header_and_body_trickle(self):
        for mode in ("trickle_headers", "trickle_body"):
            with self.subTest(mode=mode), self.server(mode) as url, patch.object(smoke, "OP_SECONDS", .08):
                start = time.monotonic()
                with self.assertRaises(smoke.Failure) as caught:
                    self.client(url).exchange(REQUEST, smoke.REVISIONS[0])
                self.assertEqual(caught.exception.reason, "deadline")
                self.assertLess(time.monotonic() - start, .3)


if __name__ == "__main__":
    unittest.main()
