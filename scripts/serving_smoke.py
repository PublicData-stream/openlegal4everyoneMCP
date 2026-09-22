#!/usr/bin/env python3
"""Bounded serving checks. Output contains fixed identifiers, never remote bodies.

Linux/POSIX only: an absolute SIGALRM budget covers DNS, TLS, headers and slow
response bodies. No proxies, redirects, retries, discovery or runtime downloads.
"""
import argparse
from contextlib import contextmanager
import http.client
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import ssl
import subprocess
import sys
import time
from urllib.parse import urlsplit

REVISIONS = ("2025-11-25", "2026-07-28")
MAX_BODY = 16 * 1024 * 1024
OP_SECONDS = 10
TRANSPORT_SECONDS = 180
READINESS_SECONDS = 360
REASONS = {"invalid_arguments", "deadline", "tls_failure", "connection_failure",
           "invalid_response", "response_limit", "unexpected_status", "invalid_progress",
           "missing_progress", "content_mismatch", "cleanup_failed", "client_failed",
           "invalid_client_report", "not_selected", "prior_check_failed", "io_failure"}


class Failure(Exception):
    def __init__(self, reason):
        self.reason = reason if reason in REASONS else "invalid_response"
        super().__init__(self.reason)


def require(condition, reason="invalid_response"):
    if not condition:
        raise Failure(reason)


@contextmanager
def deadline(seconds):
    require(seconds > 0, "deadline")
    def expired(_signum, _frame):
        raise Failure("deadline")
    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def endpoint(value, path=None, private=False, origin=False):
    require(isinstance(value, str) and 0 < len(value) <= 2048, "invalid_arguments")
    require(not any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value)
            and not any(c in value for c in "\\?#"), "invalid_arguments")
    try:
        parsed = urlsplit(value)
        host, port = parsed.hostname, parsed.port
        require(parsed.scheme in (("http", "https") if private else ("https",))
                and host and not parsed.username and not parsed.password
                and "@" not in parsed.netloc and "%" not in parsed.netloc,
                "invalid_arguments")
        require(port is None or 1 <= port <= 65535, "invalid_arguments")
        try:
            ipaddress.ip_address(host)
        except ValueError:
            require(len(host) <= 253 and all(re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label)
                    for label in host.split(".")), "invalid_arguments")
        require(parsed.path == ("" if origin else path), "invalid_arguments")
        return parsed
    except (ValueError, UnicodeError):
        raise Failure("invalid_arguments") from None


def decode_json(raw):
    require(len(raw) <= MAX_BODY, "response_limit")
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result)
            result[key] = value
        return result
    def constant(_value):
        raise Failure("invalid_response")
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
    except (ValueError, UnicodeError, RecursionError):
        raise Failure("invalid_response") from None


class RpcResponse:
    def __init__(self, request):
        self.request = request
        self.count = 0
        self.progress = 0
        self.token = request.get("params", {}).get("_meta", {}).get("progressToken")

    def consume(self, value):
        require(isinstance(value, dict) and value.get("jsonrpc") == "2.0")
        if value.get("method") == "notifications/progress":
            params = value.get("params", {})
            require(isinstance(params, dict), "invalid_progress")
            progress = params.get("progress")
            total = params.get("total")
            self.count += 1
            require(self.token is not None and "id" not in value and self.count <= 5
                    and params.get("progressToken") == self.token
                    and type(progress) in (int, float) and math.isfinite(progress)
                    and progress > self.progress, "invalid_progress")
            if total is not None:
                require(type(total) in (int, float) and math.isfinite(total)
                        and total >= progress, "invalid_progress")
            self.progress = progress
            return None
        require("method" not in value and type(value.get("id")) is int
                and value["id"] == self.request["id"] and "error" not in value
                and isinstance(value.get("result"), dict)
                and not value["result"].get("isError", False))
        require(self.token is None or self.count > 0, "missing_progress")
        return value["result"]


def read_rpc(response, request):
    collector = RpcResponse(request)
    mime = response.getheader("Content-Type", "").split(";", 1)[0].strip().lower()
    if mime == "application/json":
        result = collector.consume(decode_json(response.read(MAX_BODY + 1)))
        require(result is not None)
        return result
    require(mime == "text/event-stream")
    used, data = 0, []
    while True:
        line = response.readline(MAX_BODY + 1 - used)
        used += len(line)
        require(used <= MAX_BODY, "response_limit")
        require(bool(line))
        line = line.rstrip(b"\r\n")
        if not line:
            if data:
                result = collector.consume(decode_json(b"\n".join(data)))
                data = []
                if result is not None:
                    return result  # SSE may remain open indefinitely after the final event.
        elif line.startswith(b"data:"):
            item = line[5:]
            data.append(item[1:] if item.startswith(b" ") else item)
        elif line.startswith(b":") or line.startswith((b"event:", b"id:", b"retry:")):
            continue
        else:
            raise Failure("invalid_response")


class HttpClient:
    def __init__(self, url, origin, context, end):
        self.url, self.origin, self.context, self.end = url, origin, context, end
        self.ident = 0

    def exchange(self, body=None, revision=None, *, url=None, host=None, origin=None,
                 expected=200):
        target = url or self.url
        timeout = min(OP_SECONDS, self.end - time.monotonic())
        with deadline(timeout):
            connection_class = (http.client.HTTPSConnection if target.scheme == "https"
                                else http.client.HTTPConnection)
            kwargs = {"timeout": timeout}
            if target.scheme == "https":
                kwargs["context"] = self.context
            connection = connection_class(target.hostname, target.port, **kwargs)
            headers = {"Origin": origin or self.origin, "Accept": "application/json, text/event-stream"}
            if host:
                headers["Host"] = host
            if body:
                headers.update({"Content-Type": "application/json", "MCP-Protocol-Version": revision})
                if revision == REVISIONS[1]:
                    headers["Mcp-Method"] = body["method"]
                    if "name" in body.get("params", {}):
                        headers["Mcp-Name"] = body["params"]["name"]
            try:
                connection.request("POST" if body else "GET", target.path,
                                   json.dumps(body) if body else None, headers)
                response = connection.getresponse()
                require(response.status == expected, "unexpected_status")
                if body and "id" in body and expected == 200:
                    return read_rpc(response, body)
                # Negative checks are specific HTTP statuses after successful TLS.
                # Do not download error bodies, which may include credentials.
                return None
            except ssl.SSLError:
                raise Failure("tls_failure") from None
            except (OSError, http.client.HTTPException):
                raise Failure("connection_failure") from None
            finally:
                connection.close()

    def rpc(self, revision, method, params=None, notification=False):
        params = dict(params or {})
        if revision == REVISIONS[1]:
            params.setdefault("_meta", {}).update({
                "io.modelcontextprotocol/protocolVersion": revision,
                "io.modelcontextprotocol/clientInfo": {"name": "serving-smoke", "version": "1"},
                "io.modelcontextprotocol/clientCapabilities": {}})
        body = {"jsonrpc": "2.0", "method": method, "params": params}
        if not notification:
            self.ident += 1
            body["id"] = self.ident
        return self.exchange(body, revision, expected=202 if notification else 200)

    def call(self, revision, name, arguments, progress=False):
        params = {"name": name, "arguments": arguments}
        if progress:
            params["_meta"] = {"progressToken": "serving-smoke-diff"}
        result = self.rpc(revision, "tools/call", params)
        value = result.get("structuredContent")
        require(isinstance(value, dict))
        return value


def check_diff(client, revision, cleanup_results):
    owned = []
    try:
        value = client.call(revision, "text.diff", {"before": "first\nold\n", "after": "first\nnew\n"}, True)
        comparison, patch = value.get("comparison", {}), value.get("patch", {})
        # Capture each valid returned handle before inspecting other content so a
        # malformed sibling does not prevent cleanup of a known allocation.
        for part, field, operation in ((comparison, "comparison_id", "text.diff.delete"),
                                       (patch, "attachment_id", "text.attachment.delete")):
            if isinstance(part, dict) and isinstance(part.get(field), str) and re.fullmatch(r"[0-9a-f]{64}", part[field]):
                owned.append((operation, field, part[field]))
        require(len(owned) == 2, "content_mismatch")
        require(value.get("schema_version") == 1 and comparison.get("equal") is False
                and comparison.get("additions") == comparison.get("deletions") == 1
                and patch.get("sealed") is True and patch.get("kind") == "patch"
                and isinstance(value.get("explanation"), str) and value["explanation"], "content_mismatch")
        for view, expected in (("before", "first\nold\n"), ("after", "first\nnew\n")):
            page = client.call(revision, "text.diff.page", {"comparison_id": comparison["comparison_id"], "view": view, "page": 0})
            require(page.get("comparison_id") == comparison["comparison_id"]
                    and page.get("view") == view and page.get("page") == 0
                    and page.get("total_pages") == 1 and page.get("text") == expected, "content_mismatch")
        page = client.call(revision, "text.attachment.read", {"attachment_id": patch["attachment_id"]})
        text = page.get("text", "")
        # Rust emits explicit line counts in canonical unified patches.
        require(page.get("complete") is True and page.get("offset") == 0
                and page.get("attachment", {}).get("attachment_id") == patch["attachment_id"]
                and text == "--- before\n+++ after\n@@ -1,2 +1,2 @@\n first\n-old\n+new\n"
                and page.get("next_offset") == patch.get("total_bytes") == len(text.encode()), "content_mismatch")
    finally:
        for operation, field, handle in owned:
            started = time.monotonic()
            try:
                result = client.call(revision, operation, {field: handle})
                require(result.get("schema_version") == 1 and result.get("deleted") is True, "cleanup_failed")
                cleanup_results.append(entry(operation, "passed", started))
            except Exception:
                cleanup_results.append(entry(operation, "failed", started, "cleanup_failed"))


def entry(ident, status, started, reason=None):
    result = {"id": ident, "status": status, "duration_ms": round((time.monotonic() - started) * 1000)}
    if reason:
        result["reason_code"] = reason
    return result


def record(checks, ident, operation):
    started = time.monotonic()
    try:
        operation()
        checks.append(entry(ident, "passed", started))
        return True
    except Failure as error:
        checks.append(entry(ident, "failed", started, error.reason))
    except Exception:
        checks.append(entry(ident, "failed", started, "invalid_response"))
    return False


def subprocess_output(command, seconds, limit=MAX_BODY, allow_failure=False):
    require(seconds > 0, "deadline")
    end = time.monotonic() + seconds
    # Bound both pipes while draining concurrently. communicate() alone would
    # retain unbounded output from a broken or compromised helper.
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          stdin=subprocess.DEVNULL, start_new_session=True) as process:
        output = bytearray()
        total = 0
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                selector.register(process.stderr, selectors.EVENT_READ)
                while selector.get_map():
                    remaining = end - time.monotonic()
                    require(remaining > 0, "deadline")
                    events = selector.select(remaining)
                    require(events, "deadline")
                    for key, _mask in events:
                        chunk = os.read(key.fileobj.fileno(), 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        total += len(chunk)
                        require(total <= limit, "response_limit")
                        if key.fileobj is process.stdout:
                            output.extend(chunk)
                try:
                    code = process.wait(timeout=max(0.001, end - time.monotonic()))
                except subprocess.TimeoutExpired:
                    raise Failure("deadline") from None
                if allow_failure:
                    return code, bytes(output)
                require(code == 0, "client_failed")
                return bytes(output)
        finally:
            # Also reap helper descendants that retain pipes after parent exit.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()


def webtransport(args, revision, end, details):
    code, raw = subprocess_output([args.wt_client, args.webtransport_url, args.ca_file,
                             revision, args.origin, "--serving-smoke"], end - time.monotonic(),
                             allow_failure=True)
    value = decode_json(raw)
    require(isinstance(value, dict) and value.get("schema_version") == 1, "invalid_client_report")
    checks = value.get("checks")
    require(isinstance(checks, list) and 1 <= len(checks) <= 32, "invalid_client_report")
    required = {"configuration", "connect", "discovery", "tools_list", "server_info",
                "text_diff", "comparison_content", "patch_content", "comparison_cleanup",
                "attachment_cleanup", "invalid_origin"}
    seen = set()
    for check in checks:
        require(isinstance(check, dict) and isinstance(check.get("id"), str)
                and check["id"] in required | {"positive", "deadline"}
                and check["id"] not in seen
                and check.get("status") in {"passed", "failed", "not_run"}, "invalid_client_report")
        seen.add(check["id"])
    require(required <= seen, "invalid_client_report")
    # Copy only admitted fields; arbitrary helper diagnostics never enter reports.
    for check in checks:
        reason = check.get("reason_code")
        if reason not in REASONS | {"transport_session_rejected"}:
            reason = "client_failed" if check["status"] == "failed" else None
        details.append(entry("webtransport." + revision + "." + check["id"],
                             check["status"], time.monotonic(), reason))
    require(code == 0 and all(c["status"] == "passed" for c in checks), "client_failed")


class SafeParser(argparse.ArgumentParser):
    def error(self, _message):
        raise Failure("invalid_arguments")


def certificate_context(path):
    # Match the native client's certificate-only, 1 MiB bundle admission. OpenSSL
    # alone silently ignores unrelated PEM sections and text around certificates.
    try:
        with open(path, "rb") as stream:
            raw = stream.read(1024 * 1024 + 1)
        require(len(raw) <= 1024 * 1024, "invalid_arguments")
        remaining = raw.decode("utf-8").strip()
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        count = 0
        while remaining:
            require(remaining.startswith("-----BEGIN CERTIFICATE-----"), "invalid_arguments")
            marker = "-----END CERTIFICATE-----"
            end = remaining.find(marker)
            require(end >= 0, "invalid_arguments")
            end += len(marker)
            context.load_verify_locations(cadata=remaining[:end])
            count += 1
            remaining = remaining[end:].strip()
        require(count > 0, "invalid_arguments")
        return context
    except (OSError, ssl.SSLError, UnicodeError):
        raise Failure("invalid_arguments") from None


def arguments(argv):
    parser = SafeParser(description=__doc__)
    for name in ("http-url", "webtransport-url", "origin", "ca-file", "wt-client"):
        parser.add_argument("--" + name, required=True)
    for name in ("report", "live-url", "ready-url", "kubeconfig", "context", "namespace", "pod", "kubectl"):
        parser.add_argument("--" + name)
    args = parser.parse_args(argv)
    args.http = endpoint(args.http_url, "/mcp")
    endpoint(args.webtransport_url, "/mcp-wt/v1")
    endpoint(args.origin, origin=True)
    for name in ("live", "ready"):
        value = getattr(args, name + "_url")
        setattr(args, name, endpoint(value, "/" + name, private=True) if value else None)
    kube = [args.kubeconfig, args.context, args.namespace, args.pod, args.kubectl]
    require(not any(kube) or all(kube), "invalid_arguments")
    if all(kube):
        require(Path(args.kubeconfig).is_file() and Path(args.kubectl).is_file()
                and os.access(args.kubectl, os.X_OK), "invalid_arguments")
        require(0 < len(args.context) <= 253 and not any(ord(c) < 32 for c in args.context), "invalid_arguments")
        for name in (args.namespace, args.pod):
            require(len(name) <= 253 and re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", name), "invalid_arguments")
    require(Path(args.ca_file).is_file() and Path(args.wt_client).is_file()
            and os.access(args.wt_client, os.X_OK), "invalid_arguments")
    # Paths are explicit: never discover a helper through PATH or shell evaluation.
    args.wt_client = str(Path(args.wt_client).resolve())
    if args.kubectl:
        args.kubectl = str(Path(args.kubectl).resolve())
    args.tls = certificate_context(args.ca_file)
    return args


def pod_readiness(args):
    # Poll only the named Pod's selected fields. A lingering Ready condition is
    # insufficient during termination, and a same-name replacement is a failure.
    selected = ('jsonpath={.metadata.uid}{"\\n"}{.status.phase}{"\\n"}'
                '{.metadata.deletionTimestamp}{"\\n"}'
                '{.status.conditions[?(@.type=="Ready")].status}{"\\n"}')
    command = [args.kubectl, "--kubeconfig", args.kubeconfig,
            "--context", args.context, "--namespace", args.namespace,
            "get", "pod/" + args.pod, "--request-timeout=10s", "-o", selected]
    end, previous_uid = time.monotonic() + READINESS_SECONDS, None
    while True:
        remaining = end - time.monotonic()
        raw = subprocess_output(command, min(OP_SECONDS, remaining), 65536)
        fields = raw.split(b"\n")
        require(len(fields) == 5 and fields[-1] == b"")
        uid, phase, deletion, ready = fields[:4]
        require(0 < len(uid) <= 128 and re.fullmatch(rb"[A-Za-z0-9-]+", uid)
                and phase in (b"Pending", b"Running", b"Unknown")
                and not deletion and ready in (b"", b"True", b"False", b"Unknown"))
        require(previous_uid is None or uid == previous_uid)
        if previous_uid is not None and phase == b"Running" and ready == b"True":
            return
        previous_uid = uid
        remaining = end - time.monotonic()
        require(remaining > 0, "deadline")
        time.sleep(min(1, remaining))


def validate_source_info(info, instructions):
    source = info.get("sourceUrl")
    license_name = "AGPL-3.0-only"
    license_url = "https://www.gnu.org/licenses/agpl-3.0.html"
    require(isinstance(source, str) and 0 < len(source.encode("utf-8")) <= 2048
            and source == source.strip() and "\\" not in source
            and not any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in source), "content_mismatch")
    try:
        parsed = urlsplit(source)
        require(parsed.scheme == "https" and parsed.hostname
                and "@" not in parsed.netloc and not parsed.username and not parsed.password,
                "content_mismatch")
        # Accessing port validates malformed authorities without fetching source.
        _port = parsed.port
    except ValueError:
        raise Failure("content_mismatch") from None
    require(info.get("license") == license_name and info.get("licenseUrl") == license_url
            and isinstance(instructions, str)
            and all(value in instructions for value in (source, license_name, license_url)),
            "content_mismatch")


def run(args):
    checks = []
    started = time.monotonic()
    end = started + TRANSPORT_SECONDS
    client = HttpClient(args.http, args.origin, args.tls, end)
    for revision in REVISIONS:
        prefix = "http." + revision + "."
        discovered = {}
        def discovery():
            if revision == REVISIONS[0]:
                result = client.rpc(revision, "initialize", {"protocolVersion": revision,
                    "capabilities": {}, "clientInfo": {"name": "serving-smoke", "version": "1"}})
                require(result.get("protocolVersion") == revision)
                client.rpc(revision, "notifications/initialized", notification=True)
            else:
                result = client.rpc(revision, "server/discover")
                require(revision in result.get("supportedVersions", []))
            require(isinstance(result.get("instructions"), str) and result["instructions"])
            discovered["instructions"] = result["instructions"]
        connected = record(checks, prefix + "discovery", discovery)
        def listing():
            result = client.rpc(revision, "tools/list")
            names = {tool["name"] for tool in result.get("tools", [])}
            require({"server_info", "text.diff", "text.diff.page", "text.diff.delete",
                     "text.attachment.read", "text.attachment.delete"} <= names)
        def info():
            result = client.call(revision, "server_info", {})
            validate_source_info(result, discovered.get("instructions"))
        for name, operation in (("tools_list", listing), ("server_info", info)):
            if connected:
                record(checks, prefix + name, operation)
            else:
                checks.append(entry(prefix + name, "not_run", time.monotonic(), "prior_check_failed"))
        cleanup = []
        if connected:
            record(checks, prefix + "text_diff", lambda: check_diff(client, revision, cleanup))
        else:
            checks.append(entry(prefix + "text_diff", "not_run", time.monotonic(), "prior_check_failed"))
        for check in cleanup:
            check["id"] = prefix + check["id"]
            checks.append(check)
        # Only classify policy rejection after a positive TLS/MCP control.
        for name, kwargs in (("invalid_host", {"host": "smoke-denied.invalid", "expected": 404}),
                             ("invalid_origin", {"origin": "https://smoke-denied.invalid", "expected": 403})):
            if connected:
                record(checks, prefix + name, lambda kwargs=kwargs: client.exchange(
                    {"jsonrpc": "2.0", "id": 999, "method": "tools/list", "params": {}}, revision, **kwargs))
            else:
                checks.append(entry(prefix + name, "not_run", time.monotonic(), "prior_check_failed"))
    positive = any(c["id"].endswith("discovery") and c["status"] == "passed" for c in checks)
    for path in ("live", "ready", "metrics"):
        if positive:
            record(checks, "http.public_" + path + "_excluded", lambda path=path: client.exchange(
                url=args.http._replace(path="/" + path), expected=404))
        else:
            checks.append(entry("http.public_" + path + "_excluded", "not_run", time.monotonic(), "prior_check_failed"))
    for revision in REVISIONS:
        details = []
        record(checks, "webtransport." + revision, lambda revision=revision: webtransport(args, revision, end, details))
        checks.extend(details)
    for name in ("live", "ready"):
        target = getattr(args, name)
        if target:
            record(checks, "private." + name, lambda target=target: client.exchange(url=target))
        else:
            checks.append(entry("private." + name, "not_run", time.monotonic(), "not_selected"))
    if args.pod:
        record(checks, "kubernetes.pod_ready", lambda: pod_readiness(args))
    else:
        checks.append(entry("kubernetes.pod_ready", "not_run", time.monotonic(), "not_selected"))
    passed = all(c["status"] == "passed" or c.get("reason_code") == "not_selected" for c in checks)
    return {"schema_version": 1, "evidence_scope": "configured_endpoint",
            "status": "passed" if passed else "failed", "checks": checks,
            "duration_ms": round((time.monotonic() - started) * 1000)}


def main(argv=None):
    try:
        args = arguments(argv)
        report = run(args)
        if args.report:
            # Exclusive creation prevents following an existing report symlink or
            # overwriting credentials. Operators select a fresh report path.
            with open(args.report, "x", encoding="utf-8") as stream:
                json.dump(report, stream, indent=2)
                stream.write("\n")
        for check in report["checks"]:
            print(check["id"] + ": " + check["status"] + (
                " (" + check["reason_code"] + ")" if "reason_code" in check else ""))
        print("configured_endpoint: " + report["status"])
        return 0 if report["status"] == "passed" else 1
    except Failure as error:
        print("serving_smoke: failed (" + error.reason + ")", file=sys.stderr)
    except Exception:
        print("serving_smoke: failed (io_failure)", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
