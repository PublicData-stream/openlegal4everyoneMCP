"""Pure operator evidence gate for preserving serving after collector isolation.

``assess(observation, now)`` never connects, stops, starts or repairs anything.
The site observer first quarantines collectors, then supplies fresh evidence. It
must obtain ``diagnostic`` from the protected provider_diagnostic() function,
enumerate scheduler/request/detail Pods and Jobs and all configured external
initiators, and collect storage and serving checks from the selected deployment.
Do not substitute an exception handler's assumption for an observed field.

Input contract (all timestamps are integer UTC epoch seconds):
* operation: id, revision (full Git SHA), started_at, intentional_maintenance.
* expected: storage_id, schema_checksums ({version: SHA384 hex}), index_format,
  analyzer, capture_id, capture_digest (SHA256 hex), serving_image, config_digest,
  external_initiator_ids, public_hosts, protocol_revisions.
* collectors/storage/serving: each has operation_id, revision, observed_at and
  the fields read below. Observations may be at most MAX_AGE seconds old.

``public_checks`` are normalized successful/failed explicit-endpoint checks,
not arbitrary user supplied URLs. Their revision is the server's observed source
revision. Private health alone does not prove retained storage or public routing.
Expected values come from the reviewed operation baseline, not the observations
being validated. The caller bounds file/command inputs before decoding them and
retains original private evidence; results contain fixed reason codes only.
Observed schema_checksums must include every migration row: use None instead of
the checksum for an unsuccessful row so failure cannot normalize into success.

Disposition is a recommendation for that observer, never deployment acceptance
or permission to clear uncertainty. Abandoned slots after confirmed shutdown are
preserved for separate audited recovery; they do not alone require serving to
stop. Missing evidence fails closed. Explicit maintenance always stays stopped.
"""

import re


MAX_AGE = 120
MAX_RECORDS = 4096
_REVISION = re.compile(r"[0-9a-f]{40}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SHA384 = re.compile(r"[0-9a-f]{96}\Z")
_PINNED_IMAGE = re.compile(r"[a-z0-9][a-z0-9./:_-]*@sha256:[0-9a-f]{64}\Z")


def _integer(value):
    return type(value) is int and value >= 0


def _text(value, maximum=1024):
    return (isinstance(value, str) and 0 < len(value) <= maximum
            and not any(ord(c) < 32 or ord(c) == 127 for c in value))


def _strings(value, maximum=MAX_RECORDS, allow_empty=False):
    return (isinstance(value, list) and len(value) <= maximum
            and (allow_empty or bool(value)) and all(_text(x) for x in value)
            and len(set(value)) == len(value))


def _schema(value):
    return (isinstance(value, dict) and 0 < len(value) <= 256
            and all(isinstance(k, str) and 0 < len(k) <= 3 and k.isascii() and k.isdigit()
                    and str(int(k)) == k and 0 < int(k) <= 256
                    and isinstance(v, str) and _SHA384.fullmatch(v)
                    for k, v in value.items()))


def assess(observation, now):
    """Return disposition, collection quarantine and fixed, actionable gate codes.

    Collector failure is the caller's trigger. This gate never resumes collection,
    refunds usage, clears owners or claims source completeness. A preserve result
    is valid only for this sampled revision and freshness interval.
    """
    blockers = []
    notes = []

    def fail(code):
        blockers.append(code)

    def result(maintenance=False):
        return {
            "disposition": ("maintenance" if maintenance else
                            "stop_serving" if blockers else "preserve_serving"),
            "collection": "quarantine",
            "reasons": sorted(set(blockers + notes)),
            "provider_review_required": bool(notes or blockers),
        }

    if not isinstance(observation, dict) or not _integer(now):
        fail("invalid_observation")
        return result()
    operation = observation.get("operation")
    expected = observation.get("expected")
    if (not isinstance(operation, dict) or not _text(operation.get("id"), 128)
            or not isinstance(operation.get("revision"), str)
            or not _REVISION.fullmatch(operation["revision"])
            or not _integer(operation.get("started_at"))
            or operation["started_at"] > now
            or type(operation.get("intentional_maintenance")) is not bool):
        fail("invalid_operation")
        return result()
    if operation["intentional_maintenance"]:
        notes.append("intentional_maintenance")
        return result(maintenance=True)
    if (not isinstance(expected, dict)
            or not all(_text(expected.get(k)) for k in
                       ("storage_id", "analyzer", "capture_id", "serving_image"))
            or not _PINNED_IMAGE.fullmatch(expected["serving_image"])
            or not _schema(expected.get("schema_checksums"))
            or not _integer(expected.get("index_format"))
            or expected["index_format"] == 0
            or not all(isinstance(expected.get(k), str)
                       and _SHA256.fullmatch(expected[k])
                       for k in ("capture_digest", "config_digest"))
            or not _strings(expected.get("external_initiator_ids"), allow_empty=True)
            or not _strings(expected.get("public_hosts"), maximum=16)
            or not _strings(expected.get("protocol_revisions"), maximum=16)):
        fail("invalid_baseline")
        return result()

    sections = {}
    for name in ("collectors", "storage", "serving"):
        data = observation.get(name)
        if not isinstance(data, dict):
            fail(name + "_evidence_missing")
            continue
        if (data.get("operation_id") != operation["id"]
                or data.get("revision") != operation["revision"]):
            fail(name + "_identity_mismatch")
            continue
        stamp = data.get("observed_at")
        if (not _integer(stamp) or stamp > now or now - stamp > MAX_AGE
                or stamp < operation["started_at"]):
            fail(name + "_evidence_stale")
            continue
        sections[name] = data

    data = sections.get("collectors")
    if data is not None:
        if type(data.get("scheduler_replicas")) is not int or data["scheduler_replicas"] != 0:
            fail("scheduler_not_stopped")
        pods = data.get("pods")
        if (not isinstance(pods, list) or len(pods) > MAX_RECORDS
                or any(not isinstance(p, dict) or not _text(p.get("uid"))
                       or p.get("kind") not in ("scheduler", "request", "detail")
                       or p.get("phase") not in ("Succeeded", "Failed") for p in pods)):
            fail("collector_pods_not_stopped")
        jobs = data.get("jobs")
        if (not isinstance(jobs, list) or len(jobs) > MAX_RECORDS
                or any(not isinstance(j, dict) or not _text(j.get("uid"))
                       or type(j.get("suspended")) is not bool
                       or type(j.get("terminal")) is not bool
                       or not (j["suspended"] or j["terminal"]) for j in jobs)):
            fail("collector_jobs_not_quarantined")
        initiators = data.get("external_initiators")
        if (not isinstance(initiators, list) or len(initiators) > MAX_RECORDS
                or any(not isinstance(x, dict) or not _text(x.get("id"))
                       or x.get("state") != "stopped" or not _integer(x.get("drained_at"))
                       or not operation["started_at"] <= x["drained_at"] <= data["observed_at"]
                       for x in initiators)
                or sorted(x["id"] for x in initiators) != sorted(expected["external_initiator_ids"])):
            fail("external_initiators_not_drained")
        diagnostic = data.get("diagnostic")
        if (not isinstance(diagnostic, dict) or type(diagnostic.get("version")) is not int
                or diagnostic["version"] != 1
                or not _integer(diagnostic.get("observed_at"))
                or not operation["started_at"] <= diagnostic["observed_at"] <= data["observed_at"]
                or now - diagnostic["observed_at"] > MAX_AGE
                or not all(_integer(diagnostic.get(k)) for k in ("active_slots", "abandoned_slots"))
                or not all(type(diagnostic.get(k)) is bool for k in ("legacy_uncertain", "recovery_hold"))
                or type(data.get("legacy_lock_held")) is not bool):
            fail("provider_diagnostic_invalid")
        else:
            if diagnostic["active_slots"] != 0 or data["legacy_lock_held"]:
                fail("provider_owner_still_active")
            if diagnostic["abandoned_slots"] or diagnostic["legacy_uncertain"]:
                notes.append("provider_uncertainty_preserved")
            if diagnostic["recovery_hold"]:
                notes.append("provider_recovery_hold_preserved")

    data = sections.get("storage")
    if data is not None:
        if data.get("storage_id") != expected["storage_id"]:
            fail("storage_identity_mismatch")
        if (not _schema(data.get("schema_checksums"))
                or data["schema_checksums"] != expected["schema_checksums"]):
            fail("schema_identity_mismatch")
        index = data.get("index")
        if (not isinstance(index, dict) or type(index.get("complete")) is not bool
                or not index["complete"] or type(index.get("format")) is not int
                or index["format"] != expected["index_format"]
                or index.get("analyzer") != expected["analyzer"]
                or not all(_integer(index.get(k)) for k in ("generation", "index_ack", "next_event"))
                or index["generation"] != index["index_ack"]
                or index["index_ack"] != index["next_event"] - 1):
            fail("index_integrity_unverified")
        capture = data.get("retained_capture")
        if (not isinstance(capture, dict)
                or capture.get("id") != expected["capture_id"]
                or capture.get("digest") != expected["capture_digest"]
                or capture.get("storage_id") != expected["storage_id"]):
            fail("retained_capture_unverified")

    data = sections.get("serving")
    if data is not None:
        if (type(data.get("desired_replicas")) is not int or data["desired_replicas"] != 1
                or type(data.get("ready_replicas")) is not int or data["ready_replicas"] != 1
                or type(data.get("ready_endpoints")) is not int or data["ready_endpoints"] != 1
                or data.get("running_revision") != operation["revision"]
                or data.get("image") != expected["serving_image"]
                or data.get("config_digest") != expected["config_digest"]):
            fail("serving_identity_or_readiness_invalid")
        private = data.get("private_health")
        if (not isinstance(private, dict) or type(private.get("live_status")) is not int
                or private["live_status"] != 200 or type(private.get("ready_status")) is not int
                or private["ready_status"] != 200):
            fail("private_health_failed")
        checks = data.get("public_checks")
        required = {(host, revision, transport) for host in expected["public_hosts"]
                    for revision in expected["protocol_revisions"] for transport in ("http", "webtransport")}
        found = set()
        valid = isinstance(checks, list) and len(checks) == len(required)
        if valid:
            for check in checks:
                if not isinstance(check, dict):
                    valid = False
                    break
                key = tuple(check.get(k) for k in ("host", "protocol_revision", "transport"))
                if (not all(isinstance(k, str) for k in key) or key not in required
                        or key in found or check.get("status") != "passed"
                        or check.get("running_revision") != operation["revision"]):
                    valid = False
                    break
                found.add(key)
        if not valid or found != required:
            fail("public_transport_failed_or_missing")
    return result()
