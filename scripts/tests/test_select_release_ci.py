"""Offline checks for release reuse of a complete canonical CI attempt."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import URLError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import select_release_ci as reuse  # noqa: E402


REVISION = "a" * 40
NOW = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
START = (NOW - timedelta(hours=1)).isoformat().replace("+00:00", "Z")


def run(*, run_id=42, attempt=1, started=START):
    return {
        "id": run_id,
        "run_attempt": attempt,
        "workflow_id": 9,
        "path": reuse.WORKFLOW_PATH,
        "event": "push",
        "head_branch": "main",
        "head_sha": REVISION,
        "repository": {"full_name": reuse.REPOSITORY},
        "head_repository": {"full_name": reuse.REPOSITORY},
        "status": "completed",
        "conclusion": "success",
        "run_started_at": started,
    }


def jobs(*, run_id=42, attempt=1):
    return [
        {
            "name": name,
            "run_id": run_id,
            "run_attempt": attempt,
            "head_sha": REVISION,
            "status": "completed",
            "conclusion": "success",
        }
        for name in sorted(reuse.EXPECTED_JOBS)
    ]


class FakeApi:
    def __init__(self, *, runs=None, attempt_jobs=None):
        self.runs = deepcopy([run()] if runs is None else runs)
        self.jobs = deepcopy(jobs() if attempt_jobs is None else attempt_jobs)
        self.latest = deepcopy(self.runs[0]) if self.runs else None
        self.calls = []

    def __call__(self, path):
        self.calls.append(path)
        if path == "/actions/workflows/ci.yml":
            return {"id": 9, "path": reuse.WORKFLOW_PATH}
        if path.startswith("/actions/workflows/ci.yml/runs?"):
            return {"total_count": len(self.runs), "workflow_runs": deepcopy(self.runs)}
        if path == "/actions/runs/42/attempts/1":
            return deepcopy(self.runs[0])
        if path == "/actions/runs/42/attempts/2":
            return deepcopy(self.runs[0])
        if path.startswith("/actions/runs/42/attempts/") and "/jobs?" in path:
            return {"total_count": len(self.jobs), "jobs": deepcopy(self.jobs)}
        if path == "/actions/runs/42":
            return deepcopy(self.latest)
        raise AssertionError(f"unexpected API path: {path}")


class ReleaseCIReuseTests(unittest.TestCase):
    def test_accepts_complete_first_attempt_and_complete_rerun(self):
        first = FakeApi()
        self.assertEqual(reuse.select_reusable_ci(first, REVISION, NOW), (42, 1))
        rerun = FakeApi(runs=[run(attempt=2)], attempt_jobs=jobs(attempt=2))
        self.assertEqual(reuse.select_reusable_ci(rerun, REVISION, NOW), (42, 2))
        self.assertIn("/actions/runs/42/attempts/2/jobs?per_page=100&page=1", rerun.calls)

    def test_partial_rerun_cannot_borrow_jobs_from_earlier_attempt(self):
        partial = FakeApi(runs=[run(attempt=2)], attempt_jobs=jobs(attempt=2)[:-1])
        self.assertIsNone(reuse.select_reusable_ci(partial, REVISION, NOW))

    def test_newer_failure_cannot_be_hidden_by_older_success(self):
        newest = run(started=(NOW - timedelta(minutes=5)).isoformat())
        newest["conclusion"] = "failure"
        older = run(run_id=41)
        api = FakeApi(runs=[older, newest])
        self.assertIsNone(reuse.select_reusable_ci(api, REVISION, NOW))
        self.assertFalse(any("/jobs?" in path for path in api.calls))

    def test_age_and_identity_are_required(self):
        stale = FakeApi(runs=[run(started=(NOW - timedelta(hours=25)).isoformat())])
        self.assertIsNone(reuse.select_reusable_ci(stale, REVISION, NOW))
        future = FakeApi(runs=[run(started=(NOW + timedelta(seconds=1)).isoformat())])
        self.assertIsNone(reuse.select_reusable_ci(future, REVISION, NOW))
        for field, value in (
            ("path", ".github/workflows/other.yml"),
            ("head_repository", {"full_name": "someone/fork"}),
            ("status", "in_progress"),
        ):
            with self.subTest(field=field):
                bad = run()
                bad[field] = value
                self.assertIsNone(reuse.select_reusable_ci(FakeApi(runs=[bad]), REVISION, NOW))
        for field, value in (("head_sha", "b" * 40), ("event", "pull_request"),
                             ("head_branch", "other")):
            with self.subTest(field=field):
                bad = run()
                bad[field] = value
                with self.assertRaises(reuse.EvidenceUnavailable):
                    reuse.select_reusable_ci(FakeApi(runs=[bad]), REVISION, NOW)

    def test_missing_duplicate_failed_or_wrong_attempt_job_rejects_reuse(self):
        variants = []
        variants.append(jobs()[:-1])
        duplicate = jobs()
        duplicate[-1]["name"] = duplicate[0]["name"]
        variants.append(duplicate)
        failed = jobs()
        failed[-1]["conclusion"] = "skipped"
        variants.append(failed)
        wrong_attempt = jobs()
        wrong_attempt[-1]["run_attempt"] = 2
        variants.append(wrong_attempt)
        for attempt_jobs in variants:
            with self.subTest(last=attempt_jobs[-1]):
                self.assertIsNone(reuse.select_reusable_ci(
                    FakeApi(attempt_jobs=attempt_jobs), REVISION, NOW
                ))

    def test_rerun_started_during_job_read_rejects_reuse(self):
        api = FakeApi()
        api.latest["run_attempt"] = 2
        self.assertIsNone(reuse.select_reusable_ci(api, REVISION, NOW))

    def test_malformed_or_incomplete_pagination_is_unavailable(self):
        def missing_page(path):
            if path == "/actions/workflows/ci.yml":
                return {"id": 9, "path": reuse.WORKFLOW_PATH}
            return {"total_count": 101, "workflow_runs": []}

        with self.assertRaises(reuse.EvidenceUnavailable):
            reuse.select_reusable_ci(missing_page, REVISION, NOW)

        extra = FakeApi(attempt_jobs=jobs() + jobs()[:1])
        with self.assertRaises(reuse.EvidenceUnavailable):
            reuse.select_reusable_ci(extra, REVISION, NOW)

        pages = {
            1: {"total_count": 101, "items": [{"id": index} for index in range(100)]},
            2: {"total_count": 101, "items": [{"id": 100}]},
        }

        def fetch_page(path):
            return pages[int(path.rsplit("page=", 1)[1])]

        self.assertEqual(len(reuse.paged_items(fetch_page, "/items", "items", 200)), 101)

    def test_api_failure_falls_back_to_fresh_ci(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            environment = {
                "GITHUB_REPOSITORY": reuse.REPOSITORY,
                "GITHUB_TOKEN": "test-only-token",
                "GITHUB_OUTPUT": str(output),
                "RELEASE_REVISION": REVISION,
            }
            with patch.dict(os.environ, environment), patch.object(
                reuse, "request_json", side_effect=URLError("unavailable")
            ):
                self.assertEqual(reuse.main(), 0)
            self.assertEqual(output.read_text(), "reuse=false\n")


if __name__ == "__main__":
    unittest.main()
