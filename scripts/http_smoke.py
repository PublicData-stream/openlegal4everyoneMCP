#!/usr/bin/env python3
"""Bounded synthetic MCP checks against the disposable OxiBelt network only."""
import http.client
import json
import ssl
import sys
import time

CA = sys.argv[1] if len(sys.argv) > 1 else "/fixture/cert/ca.pem"
CONTEXT = ssl.create_default_context(cafile=CA)
MODERN = "2026-07-28"
LEGACY = "2025-11-25"
MAX_BODY = 16 * 1024 * 1024
MAX_PROGRESS = 5
SOURCE_URL = "https://example.org/openlegal/source"


def exchange(body, revision=MODERN, session=None, origin=None, path="/mcp", host=None, timeout=10):
    connection = http.client.HTTPSConnection("edge", 8443, context=CONTEXT, timeout=timeout)
    headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
               "MCP-Protocol-Version": revision}
    if revision == MODERN:
        headers["Mcp-Method"] = body["method"]
        name = body.get("params", {}).get("name") or body.get("params", {}).get("uri")
        if name:
            headers["Mcp-Name"] = name
    if session:
        headers["Mcp-Session-Id"] = session
    if origin:
        headers["Origin"] = origin
    if host:
        headers["Host"] = host
    try:
        connection.request("POST", path, json.dumps(body), headers)
        response = connection.getresponse()
        status, response_headers = response.status, dict(response.getheaders())
        result = None
        progress_count = 0
        if response.getheader("content-type", "").startswith("text/event-stream"):
            consumed = 0
            progress_value = 0
            # Read until this RPC's response, without waiting for SSE connection EOF.
            while consumed <= MAX_BODY:
                line = response.readline(MAX_BODY + 1 - consumed)
                consumed += len(line)
                if not line:
                    break
                if line.startswith(b"data:"):
                    value = json.loads(line[5:].strip())
                    if value.get("method") == "notifications/progress":
                        progress_count += 1
                        assert progress_count <= MAX_PROGRESS
                        assert value["params"]["progressToken"] == body["params"]["_meta"]["progressToken"]
                        assert value["params"]["progress"] > progress_value
                        progress_value = value["params"]["progress"]
                    if value.get("id") == body.get("id"):
                        result = value
                        break
            if consumed > MAX_BODY:
                raise AssertionError("SSE response exceeded bound")
        else:
            raw = response.read(MAX_BODY + 1)
            assert len(raw) <= MAX_BODY, "response exceeded bound"
            if raw and response.getheader("content-type", "").startswith("application/json"):
                result = json.loads(raw)
        if (body.get("method") == "tools/call"
                and "progressToken" in body.get("params", {}).get("_meta", {})
                and status == 200 and result and "result" in result
                and not result["result"].get("isError", False)):
            assert progress_count > 0, "successful token-bearing tool call emitted no progress"
        return status, {k.lower(): v for k, v in response_headers.items()}, result
    finally:
        connection.close()


def request(method, revision, ident=1, **params):
    if revision == MODERN:
        params.setdefault("_meta", {}).update({
            "io.modelcontextprotocol/protocolVersion": revision,
            "io.modelcontextprotocol/clientInfo": {"name": "edge-smoke", "version": "1"},
            "io.modelcontextprotocol/clientCapabilities": {},
        })
    return {"jsonrpc": "2.0", "id": ident, "method": method, "params": params}


def success(body, revision, session=None, origin=None):
    status, headers, reply = exchange(body, revision, session, origin)
    assert status == 200, (status, reply)
    assert reply and reply.get("id") == body["id"] and "error" not in reply, reply
    return headers, reply["result"]



def text_diff_smoke(revision, session, tools):
    by_name = {tool["name"]: tool for tool in tools}
    for name in ("compare_texts", "show_text_diff", "get_text_diff_page", "delete_text_diff"):
        assert name in by_name, f"{name} missing"
    deletion = by_name["delete_text_diff"]["annotations"]
    assert deletion["readOnlyHint"] is False, deletion
    assert deletion["destructiveHint"] is True, deletion
    assert deletion["idempotentHint"] is True, deletion

    def call(name, **arguments):
        _, result = success(request("tools/call", revision, 20, name=name, arguments=arguments),
                            revision, session)
        assert not result.get("isError", False), result
        value = result["structuredContent"]
        assert value["schema_version"] == 1, value
        return value

    blank = call("show_text_diff")
    assert blank["comparison"] is None, blank
    summary = call("compare_texts", before="first\nold\n", after="first\nnew\n",
                   before_label="Before", after_label="After")
    assert summary["equal"] is False and summary["additions"] == summary["deletions"] == 1, summary
    handle = summary["comparison_id"]
    shown = call("show_text_diff", comparison_id=handle)
    assert shown["comparison"]["comparison_id"] == handle, shown
    for view in ("changes", "before", "after"):
        page = call("get_text_diff_page", comparison_id=handle, view=view, page=0)
        assert page["comparison_id"] == handle and page["view"] == view, page
        assert page["page"] == 0 and page["total_pages"] == 1, page
        if view == "changes":
            assert page["fragments"] and "-old" in page["fragments"][0]["patch"], page
            assert "+new" in page["fragments"][0]["patch"], page
            assert page["fragments"][0]["inline_changes"] == [
                {"row_index": 1, "ranges": [[0, 3]]},
                {"row_index": 2, "ranges": [[0, 3]]},
            ], page
        else:
            assert page["text"] == ("first\nold\n" if view == "before" else "first\nnew\n"), page
        assert len(json.dumps(page, ensure_ascii=False, separators=(",", ":")).encode()) <= 256 * 1024
    for _ in range(2):
        assert call("delete_text_diff", comparison_id=handle)["deleted"] is True
    _, missing = success(request("tools/call", revision, 21, name="get_text_diff_page",
                                arguments={"comparison_id": handle, "view": "changes", "page": 0}),
                         revision, session)
    assert missing.get("isError") is True, missing

    # Each input is exactly 1 MiB, within line limits. Control-character JSON
    # escaping exercises nearly 12 MiB of request body without oversized lines.
    large = ("\x01" * 1023 + "\n") * 1024
    assert len(large.encode()) == 1024 * 1024
    maximum = call("compare_texts", before=large, after=large)
    assert maximum["equal"] is True, maximum
    assert maximum["before"]["bytes"] == maximum["after"]["bytes"] == 1024 * 1024
    assert maximum["additions"] == maximum["deletions"] == 0, maximum
    assert call("delete_text_diff", comparison_id=maximum["comparison_id"])["deleted"] is True

    _, resource = success(request("resources/read", revision, 22,
                                  uri="ui://openlegal/text-diff-v1.html"), revision, session)
    contents = resource["contents"][0]
    assert contents["mimeType"] == "text/html;profile=mcp-app", contents["mimeType"]
    assert SOURCE_URL in contents["text"]
    assert "__OPENLEGAL_SOURCE_URL__" not in contents["text"]
    print(f"HTTP {revision}: text comparison lifecycle, 1 MiB inputs, resource read", flush=True)


def smoke():
    for revision in (MODERN, LEGACY):
        session = None
        if revision == LEGACY:
            headers, result = success(request("initialize", revision, protocolVersion=revision,
                                              capabilities={}, clientInfo={"name": "edge-smoke", "version": "1"}), revision)
            assert result["protocolVersion"] == revision
            session = headers.get("mcp-session-id")
            status, _, _ = exchange({"jsonrpc": "2.0", "method": "notifications/initialized"}, revision, session)
            assert status in (200, 202, 204), status
        else:
            _, result = success(request("server/discover", revision), revision)
            assert result["resultType"] == "complete", result
            assert set(result["supportedVersions"]) == {MODERN, LEGACY}, result
            assert result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"] == "openlegal4everyone.stream", result
        assert SOURCE_URL in result["instructions"], result
        assert "AGPL-3.0-only" in result["instructions"], result
        _, result = success(request("tools/list", revision, 2), revision, session)
        assert any(tool["name"] == "server_info" for tool in result["tools"]), result
        text_diff_smoke(revision, session, result["tools"])
        for origin in (None, "https://example.test"):
            _, result = success(request("tools/call", revision, 3, name="server_info", arguments={}),
                                revision, session, origin)
            assert not result.get("isError", False), result
            assert result.get("content"), result
            info = result.get("structuredContent") or json.loads(result["content"][0]["text"])
            assert info["license"] == "AGPL-3.0-only", info
            assert info["sourceUrl"] == SOURCE_URL, info
            assert info["licenseUrl"] == "https://www.gnu.org/licenses/agpl-3.0.html", info
        print(f"HTTP {revision}: discovery, tools/list, server_info, Origin accepted", flush=True)
        for ident, tool, arguments in (
            (4, "demo_search_records", {"source": "layout_a"}),
            (5, "demo_get_record", {"source": "layout_b", "id": "001"}),
            (6, "demo_show_records", {"records": [{"source": "layout_a", "id": "001"}]}),
        ):
            _, result = success(request("tools/call", revision, ident, name=tool,
                                        arguments=arguments, _meta={"progressToken": f"http-{ident}"}), revision, session)
            assert not result.get("isError", False), result
            assert result["structuredContent"]["synthetic"] is True, result
        history_query = {"operation": "get", "source": "layout_b", "id": "001"}
        def history_call(name, **arguments):
            _, result = success(request("tools/call", revision, 24, name=name,
                arguments=arguments, _meta={"progressToken": "history"}), revision, session)
            assert not result.get("isError", False), result
            return result["structuredContent"]
        history = history_call("demo_list_snapshots", query=history_query)
        capture = history["snapshots"][0]["snapshot_id"]
        exact = history_call("demo_get_snapshot", query=history_query, snapshot_id=capture)
        assert exact["historical"] is True and "freshness" not in exact
        assert exact["data"]["id"] == "001"
        compared = history_call("demo_compare_record_snapshots", source="layout_b", id="001",
            before_snapshot_id=capture, after_snapshot_id=capture)
        assert compared["equal"] is True
        assert compared["origin"]["before"]["snapshot_id"] == capture
        _, removed = success(request("tools/call", revision, 25, name="delete_text_diff",
            arguments={"comparison_id": compared["comparison_id"]}), revision, session)
        assert removed["structuredContent"]["deleted"] is True
        print(f"HTTP {revision}: exact retained history and snapshot comparison verified", flush=True)
        _, result = success(request("resources/read", revision, 7,
                                    uri="ui://openlegal-demo/records-v1.html"), revision, session)
        assert result["contents"][0]["mimeType"] == "text/html;profile=mcp-app", result
        assert "Synthetic" in result["contents"][0]["text"] or "synthetic" in result["contents"][0]["text"]
        assert SOURCE_URL in result["contents"][0]["text"]
        assert "__OPENLEGAL_SOURCE_URL__" not in result["contents"][0]["text"]
        print(f"HTTP {revision}: synthetic search/detail/render, progress, resource read", flush=True)
    probe = request("tools/list", MODERN)
    assert exchange(probe, origin="https://rejected.test")[0] == 403
    assert exchange(probe, path="/mcp-other")[0] == 404
    assert exchange(probe, host="rejected.test")[0] == 404
    for path in ("/live", "/ready", "/metrics"):
        connection = http.client.HTTPSConnection("edge", 8443, context=CONTEXT, timeout=10)
        try:
            connection.request("GET", path)
            response = connection.getresponse()
            assert response.status == 404, (path, response.status)
            assert len(response.read(MAX_BODY + 1)) <= MAX_BODY, "response exceeded bound"
        finally:
            connection.close()
    untrusted = http.client.HTTPSConnection("edge", 8443, context=ssl.create_default_context(), timeout=10)
    try:
        untrusted.connect()
    except ssl.SSLCertVerificationError:
        pass
    else:
        raise AssertionError("untrusted edge certificate accepted")
    finally:
        untrusted.close()
    print("HTTP: wrong Origin/path/authority and untrusted certificate rejected; health/metrics unavailable", flush=True)


if __name__ == "__main__":
    if "--ready" in sys.argv:
        deadline = time.monotonic() + 10
        last_failure = "no response"
        while time.monotonic() < deadline:
            try:
                status, _, reply = exchange(request("tools/list", MODERN), timeout=min(1, deadline - time.monotonic()))
                if status == 200:
                    sys.exit(0)
                last_failure = f"HTTP {status}: {reply}"
            except (OSError, http.client.HTTPException) as error:
                last_failure = f"{type(error).__name__}: {error}"
            time.sleep(min(0.25, max(0, deadline - time.monotonic())))
        sys.exit(f"edge did not become ready within 10 seconds ({last_failure})")
    smoke()
