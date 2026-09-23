#!/usr/bin/env python3
"""Reuse only a recent, complete main-push CI attempt for a release commit."""

from datetime import datetime, timedelta, timezone
import json
import os
import re
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener


REPOSITORY = "PublicData-stream/openlegal4everyoneMCP"
WORKFLOW_PATH = ".github/workflows/ci.yml"
API_ROOT = f"https://api.github.com/repos/{REPOSITORY}"
MAX_AGE = timedelta(hours=24)
PAGE_SIZE = 100
MAX_RUNS = 1000
MAX_RESPONSE_BYTES = 2_000_000
# Keep this set aligned with all expanded jobs in .github/workflows/ci.yml.
EXPECTED_JOBS = frozenset({
    "alpine-packages",
    "ci-summary",
    "dependencies",
    "document-worker",
    "korean-tokenization",
    "kubernetes-serving",
    "oxibelt",
    "postgres",
    "release-contract",
    "rust (ubuntu-26.04)",
    "rust (ubuntu-26.04-arm)",
    "server-image (ubuntu-26.04, linux/amd64, runtime)",
    "server-image (ubuntu-26.04, linux/amd64, runtime-ingestion)",
    "server-image (ubuntu-26.04-arm, linux/arm64, runtime)",
    "server-image (ubuntu-26.04-arm, linux/arm64, runtime-ingestion)",
    "widget",
})


class EvidenceUnavailable(ValueError):
    """The Actions response cannot establish a reusable CI attempt."""


class RejectRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        raise EvidenceUnavailable("Actions API redirect")


def request_json(path: str, token: str) -> dict:
    request = Request(
        API_ROOT + path,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "openlegal-release-ci-gate",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with build_opener(RejectRedirect).open(request, timeout=10) as response:
        if response.status != 200:
            raise EvidenceUnavailable("Actions API status")
        body = response.read(MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES:
        raise EvidenceUnavailable("Actions API response too large")
    result = json.loads(body)
    if not isinstance(result, dict):
        raise EvidenceUnavailable("Actions API response shape")
    return result


def paged_items(fetch, path: str, key: str, limit: int) -> list[dict]:
    items = []
    total = None
    for page in range(1, (limit + PAGE_SIZE - 1) // PAGE_SIZE + 1):
        separator = "&" if "?" in path else "?"
        result = fetch(f"{path}{separator}per_page={PAGE_SIZE}&page={page}")
        if not isinstance(result, dict):
            raise EvidenceUnavailable("Actions API page shape")
        count = result.get("total_count")
        batch = result.get(key)
        if type(count) is not int or count < 0 or count > limit or not isinstance(batch, list):
            raise EvidenceUnavailable("Actions API pagination metadata")
        if total is None:
            total = count
        if count != total or len(batch) > PAGE_SIZE or len(items) + len(batch) > count:
            raise EvidenceUnavailable("Actions API pagination changed")
        if any(not isinstance(item, dict) for item in batch):
            raise EvidenceUnavailable("Actions API item shape")
        items.extend(batch)
        if len(items) == total:
            return items
        if not batch:
            raise EvidenceUnavailable("Actions API incomplete page")
    raise EvidenceUnavailable("Actions API pagination limit")


def timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise EvidenceUnavailable("Actions API timestamp")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise EvidenceUnavailable("Actions API timestamp") from error
    if result.tzinfo is None:
        raise EvidenceUnavailable("Actions API timestamp timezone")
    return result.astimezone(timezone.utc)


def run_identity(run: dict, revision: str, workflow_id: int, run_id: int, attempt: int) -> bool:
    return (
        isinstance(run, dict)
        and run.get("id") == run_id
        and type(run.get("run_attempt")) is int
        and run.get("run_attempt") == attempt
        and run.get("workflow_id") == workflow_id
        and run.get("path") == WORKFLOW_PATH
        and run.get("event") == "push"
        and run.get("head_branch") == "main"
        and run.get("head_sha") == revision
        and isinstance(run.get("repository"), dict)
        and run["repository"].get("full_name") == REPOSITORY
        and isinstance(run.get("head_repository"), dict)
        and run["head_repository"].get("full_name") == REPOSITORY
        and run.get("status") == "completed"
        and run.get("conclusion") == "success"
    )


def complete_jobs(jobs: list[dict], revision: str, run_id: int, attempt: int) -> bool:
    names = [job.get("name") for job in jobs]
    return (
        len(jobs) == len(EXPECTED_JOBS)
        and all(isinstance(name, str) for name in names)
        and len(set(names)) == len(names)
        and set(names) == EXPECTED_JOBS
        and all(
            job.get("run_id") == run_id
            and type(job.get("run_attempt")) is int
            and job.get("run_attempt") == attempt
            and job.get("head_sha") == revision
            and job.get("status") == "completed"
            and job.get("conclusion") == "success"
            for job in jobs
        )
    )


def select_reusable_ci(fetch, revision: str, now: datetime) -> tuple[int, int] | None:
    """Return run and attempt IDs only when one whole CI attempt is proven successful."""
    if not re.fullmatch(r"[0-9a-f]{40}", revision) or now.tzinfo is None:
        raise EvidenceUnavailable("release revision or clock")
    workflow = fetch("/actions/workflows/ci.yml")
    if not isinstance(workflow, dict):
        raise EvidenceUnavailable("CI workflow response shape")
    workflow_id = workflow.get("id")
    if type(workflow_id) is not int or workflow_id <= 0 or workflow.get("path") != WORKFLOW_PATH:
        raise EvidenceUnavailable("CI workflow identity")
    query = urlencode({"head_sha": revision, "event": "push", "branch": "main"})
    runs = paged_items(fetch, f"/actions/workflows/ci.yml/runs?{query}", "workflow_runs", MAX_RUNS)
    if not runs:
        return None
    run_ids = [run.get("id") for run in runs]
    if any(type(run_id) is not int for run_id in run_ids) or len(set(run_ids)) != len(runs):
        raise EvidenceUnavailable("duplicate or malformed CI run")
    # Never filter to successful runs before selecting: a newer failure must win.
    if any(run.get("head_sha") != revision or run.get("event") != "push"
           or run.get("head_branch") != "main" for run in runs):
        raise EvidenceUnavailable("CI run query mismatch")
    started = [(timestamp(run.get("run_started_at")), run) for run in runs]
    latest_time = max(when for when, _ in started)
    latest = [run for when, run in started if when == latest_time]
    if len(latest) != 1:
        raise EvidenceUnavailable("ambiguous latest CI attempt")
    run = latest[0]
    age = now.astimezone(timezone.utc) - latest_time
    if age < timedelta(0) or age > MAX_AGE:
        return None
    run_id = run.get("id")
    attempt = run.get("run_attempt")
    if type(run_id) is not int or run_id <= 0 or type(attempt) is not int or attempt <= 0:
        raise EvidenceUnavailable("CI attempt identity")
    if not run_identity(run, revision, workflow_id, run_id, attempt):
        return None
    attempt_path = f"/actions/runs/{run_id}/attempts/{attempt}"
    if not run_identity(fetch(attempt_path), revision, workflow_id, run_id, attempt):
        return None
    jobs = paged_items(fetch, attempt_path + "/jobs", "jobs", len(EXPECTED_JOBS))
    if not complete_jobs(jobs, revision, run_id, attempt):
        return None
    # Reject a rerun that began while the attempt-specific jobs were being read.
    if not run_identity(fetch(f"/actions/runs/{run_id}"), revision, workflow_id, run_id, attempt):
        return None
    return run_id, attempt


def main() -> int:
    revision = os.environ.get("RELEASE_REVISION", "")
    token = os.environ.get("GITHUB_TOKEN", "")
    output = os.environ["GITHUB_OUTPUT"]
    selected = None
    if os.environ.get("GITHUB_REPOSITORY") == REPOSITORY and token:
        try:
            selected = select_reusable_ci(
                lambda path: request_json(path, token), revision, datetime.now(timezone.utc)
            )
        except (EvidenceUnavailable, HTTPError, URLError, OSError, ValueError,
                TypeError, KeyError, json.JSONDecodeError):
            print("CI evidence unavailable; running fresh complete CI", file=sys.stderr)
    with open(output, "a", encoding="utf-8") as stream:
        if selected is None:
            stream.write("reuse=false\n")
        else:
            run_id, attempt = selected
            stream.write(f"reuse=true\nrun_id={run_id}\nattempt={attempt}\n")
            print(f"Reusing complete CI run {run_id} attempt {attempt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
