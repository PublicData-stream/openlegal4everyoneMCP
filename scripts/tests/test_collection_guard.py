"""Synthetic collector-isolation evidence checks; no cluster or provider calls."""

from copy import deepcopy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import collection_guard as guard  # noqa: E402


NOW = 1000
REVISION = "a" * 40


def evidence():
    expected = {
        "storage_id": "retained-store", "schema_checksums": {"1": "b" * 96},
        "index_format": 3, "analyzer": "reviewed-korean-analyzer",
        "capture_id": "fixed-capture", "capture_digest": "c" * 64,
        "serving_image": "registry.example/server@sha256:" + "d" * 64,
        "config_digest": "e" * 64, "external_initiator_ids": ["operator-collector"],
        "public_hosts": ["mcp.example.test", "api.example.test"],
        "protocol_revisions": ["2025-11-25", "2026-07-28"],
    }
    stamp = {"operation_id": "operation-1", "revision": REVISION, "observed_at": NOW}
    return {
        "operation": {"id": "operation-1", "revision": REVISION,
                      "started_at": NOW - 10, "intentional_maintenance": False},
        "expected": expected,
        "collectors": {
            **stamp, "scheduler_replicas": 0, "pods": [],
            "jobs": [{"uid": "suspended-job", "suspended": True, "terminal": False}],
            "external_initiators": [{"id": "operator-collector", "state": "stopped", "drained_at": NOW}],
            "legacy_lock_held": False,
            "diagnostic": {"version": 1, "observed_at": NOW, "active_slots": 0,
                           "abandoned_slots": 0, "legacy_uncertain": False, "recovery_hold": False},
        },
        "storage": {
            **stamp, "storage_id": expected["storage_id"],
            "schema_checksums": deepcopy(expected["schema_checksums"]),
            "index": {"format": 3, "analyzer": expected["analyzer"], "complete": True,
                      "generation": 18216, "index_ack": 18216, "next_event": 18217},
            "retained_capture": {"id": expected["capture_id"], "digest": expected["capture_digest"],
                                 "storage_id": expected["storage_id"]},
        },
        "serving": {
            **stamp, "desired_replicas": 1, "ready_replicas": 1, "ready_endpoints": 1,
            "running_revision": REVISION, "image": expected["serving_image"],
            "config_digest": expected["config_digest"],
            "private_health": {"live_status": 200, "ready_status": 200},
            "public_checks": [
                {"host": host, "protocol_revision": revision, "transport": transport,
                 "status": "passed", "running_revision": REVISION}
                for host in expected["public_hosts"] for revision in expected["protocol_revisions"]
                for transport in ("http", "webtransport")
            ],
        },
    }


class CollectionGuardTests(unittest.TestCase):
    def assess(self, sample):
        before = deepcopy(sample)
        result = guard.assess(sample, NOW)
        self.assertEqual(sample, before, "assessment must not mutate operational evidence")
        self.assertEqual(result["collection"], "quarantine")
        return result

    def reject(self, sample, reason):
        result = self.assess(sample)
        self.assertEqual(result["disposition"], "stop_serving")
        self.assertIn(reason, result["reasons"])

    def test_preserves_verified_serving_without_resuming_collection(self):
        result = self.assess(evidence())
        self.assertEqual(result, {"disposition": "preserve_serving", "collection": "quarantine",
                                  "reasons": [], "provider_review_required": False})

    def test_original_stop_sequence_keeps_uncertainty_without_stopping_healthy_serving(self):
        sample = evidence()
        sample["collectors"]["diagnostic"]["active_slots"] = 4
        self.reject(sample, "provider_owner_still_active")
        sample["collectors"]["diagnostic"].update(active_slots=0, abandoned_slots=2)
        result = self.assess(sample)
        self.assertEqual(result["disposition"], "preserve_serving")
        self.assertEqual(result["reasons"], ["provider_uncertainty_preserved"])
        self.assertTrue(result["provider_review_required"])
        self.assertEqual(sample["collectors"]["diagnostic"]["abandoned_slots"], 2)

    def test_abandoned_slots_do_not_substitute_for_shutdown_proof(self):
        sample = evidence()
        sample["collectors"]["diagnostic"]["abandoned_slots"] = 2
        sample["collectors"]["pods"] = [{"uid": "request-pod", "kind": "request", "phase": "Running"}]
        self.reject(sample, "collector_pods_not_stopped")

    def test_live_legacy_owner_blocks_preservation(self):
        sample = evidence()
        sample["collectors"]["legacy_lock_held"] = True
        self.reject(sample, "provider_owner_still_active")

    def test_hold_and_legacy_uncertainty_remain_visible(self):
        sample = evidence()
        sample["collectors"]["diagnostic"].update(legacy_uncertain=True, recovery_hold=True)
        result = self.assess(sample)
        self.assertEqual(result["disposition"], "preserve_serving")
        self.assertEqual(result["reasons"], ["provider_recovery_hold_preserved", "provider_uncertainty_preserved"])

    def test_scheduler_job_and_external_initiator_must_all_be_stopped(self):
        cases = [
            ("scheduler_replicas", 1, "scheduler_not_stopped"),
            ("jobs", [{"uid": "pending-job", "suspended": False, "terminal": False}], "collector_jobs_not_quarantined"),
            ("external_initiators", [], "external_initiators_not_drained"),
        ]
        for key, value, reason in cases:
            with self.subTest(key=key):
                sample = evidence()
                sample["collectors"][key] = value
                self.reject(sample, reason)

    def test_private_health_does_not_establish_public_recovery(self):
        sample = evidence()
        sample["serving"]["public_checks"][0]["status"] = "failed"
        self.reject(sample, "public_transport_failed_or_missing")

    def test_public_matrix_is_complete_unique_and_same_release(self):
        for change in ("missing", "duplicate", "wrong_revision", "malformed"):
            with self.subTest(change=change):
                sample = evidence()
                checks = sample["serving"]["public_checks"]
                if change == "missing":
                    checks.pop()
                elif change == "duplicate":
                    checks[-1] = deepcopy(checks[0])
                elif change == "wrong_revision":
                    checks[-1]["running_revision"] = "f" * 40
                else:
                    checks[-1]["host"] = []
                self.reject(sample, "public_transport_failed_or_missing")

    def test_unavailable_private_health_or_zero_replicas_fail_closed(self):
        sample = evidence()
        sample["serving"]["private_health"]["ready_status"] = 503
        self.reject(sample, "private_health_failed")
        sample = evidence()
        sample["serving"].update(desired_replicas=0, ready_replicas=0, ready_endpoints=0)
        self.reject(sample, "serving_identity_or_readiness_invalid")

    def test_storage_identity_schema_and_retained_capture_are_independent_gates(self):
        for key, value, reason in (
            ("storage_id", "different-store", "storage_identity_mismatch"),
            ("schema_checksums", {"1": "f" * 96}, "schema_identity_mismatch"),
            ("retained_capture", {}, "retained_capture_unverified"),
        ):
            with self.subTest(key=key):
                sample = evidence()
                sample["storage"][key] = value
                self.reject(sample, reason)

    def test_old_incomplete_wrong_analyzer_or_lagging_index_is_not_servable(self):
        for key, value in (("format", 2), ("complete", False), ("analyzer", "other"),
                           ("generation", 18215), ("index_ack", 18215), ("next_event", 18218)):
            with self.subTest(key=key):
                sample = evidence()
                sample["storage"]["index"][key] = value
                self.reject(sample, "index_integrity_unverified")

    def test_missing_stale_future_preoperation_and_wrong_identity_evidence(self):
        for section in ("collectors", "storage", "serving"):
            with self.subTest(section=section, case="missing"):
                sample = evidence()
                del sample[section]
                self.reject(sample, section + "_evidence_missing")
            for stamp in (NOW - guard.MAX_AGE - 1, NOW + 1, NOW - 11):
                with self.subTest(section=section, stamp=stamp):
                    sample = evidence()
                    sample[section]["observed_at"] = stamp
                    self.reject(sample, section + "_evidence_stale")
            for key, value in (("operation_id", "different-operation"), ("revision", "f" * 40)):
                with self.subTest(section=section, key=key):
                    sample = evidence()
                    sample[section][key] = value
                    self.reject(sample, section + "_identity_mismatch")

    def test_invalid_or_old_protected_diagnostic_is_not_zero_active_owners(self):
        for value in ({}, {"healthy": True}, None):
            sample = evidence()
            sample["collectors"]["diagnostic"] = value
            self.reject(sample, "provider_diagnostic_invalid")
        sample = evidence()
        sample["collectors"]["diagnostic"]["observed_at"] = NOW - 11
        self.reject(sample, "provider_diagnostic_invalid")

    def test_intentional_maintenance_never_starts_serving(self):
        sample = evidence()
        sample["operation"]["intentional_maintenance"] = True
        del sample["serving"]
        result = self.assess(sample)
        self.assertEqual(result["disposition"], "maintenance")
        self.assertEqual(result["reasons"], ["intentional_maintenance"])

    def test_invalid_shapes_and_boolean_counts_do_not_throw_or_pass(self):
        for sample in (None, [], {}, {"operation": {"id": "secret\nvalue"}}):
            self.assertEqual(guard.assess(sample, NOW)["disposition"], "stop_serving")
        sample = evidence()
        sample["serving"]["desired_replicas"] = True
        self.reject(sample, "serving_identity_or_readiness_invalid")
        sample = evidence()
        sample["expected"]["schema_checksums"] = {"1" * 5000: "b" * 96}
        self.reject(sample, "invalid_baseline")

    def test_reason_codes_do_not_echo_error_bodies_or_identities(self):
        sample = evidence()
        sample["serving"]["public_checks"][0]["status"] = "credential=private-response"
        result = self.assess(sample)
        self.assertNotIn("credential", repr(result))
        self.assertNotIn("mcp.example.test", repr(result))

    def test_failed_migration_and_unpinned_release_do_not_pass(self):
        sample = evidence()
        sample["storage"]["schema_checksums"]["1"] = None
        self.reject(sample, "schema_identity_mismatch")
        sample = evidence()
        sample["expected"]["serving_image"] = "registry.example/server:latest"
        self.reject(sample, "invalid_baseline")

    def test_malformed_nested_fields_return_decisions_without_throwing(self):
        sample = evidence()
        paths = []

        def visit(value, path=()):
            if isinstance(value, dict):
                children = value.items()
            elif isinstance(value, list):
                children = enumerate(value)
            else:
                return
            for key, child in children:
                paths.append(path + (key,))
                visit(child, path + (key,))

        visit(sample)
        for path in paths:
            for replacement in (None, [], {}, True, -1, "", float("nan"), [["nested"]]):
                with self.subTest(path=path, replacement=type(replacement).__name__):
                    changed = deepcopy(sample)
                    parent = changed
                    for key in path[:-1]:
                        parent = parent[key]
                    parent[path[-1]] = replacement
                    result = guard.assess(changed, NOW)
                    self.assertIn(result["disposition"], ("stop_serving", "preserve_serving", "maintenance"))
                    self.assertEqual(result["collection"], "quarantine")


if __name__ == "__main__":
    unittest.main()
