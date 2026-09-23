#!/usr/bin/env python3
"""Fail-closed source and event contract for GHCR image releases."""

import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib


REPOSITORY = "PublicData-stream/openlegal4everyoneMCP"
TAG = re.compile(
    r"(?P<base>(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*))"
    r"(?:(?P<beta>-beta\.[1-9][0-9]*)|(?P<build>-build\.[0-9a-f]{8}))?\Z"
)


def git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args], capture_output=True, text=True, check=False
    )
    if result.returncode:
        raise ValueError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def validate(root: Path, env: dict[str, str]) -> dict[str, str]:
    if env.get("GITHUB_REPOSITORY") != REPOSITORY:
        raise ValueError("release must run in the canonical repository")
    ref = env.get("GITHUB_REF", "")
    if not ref.startswith("refs/tags/"):
        raise ValueError("release must run from a tag ref")
    version = ref.removeprefix("refs/tags/")
    match = TAG.fullmatch(version)
    if match is None:
        raise ValueError("release tag has an unsupported format")
    kind = "beta" if match.group("beta") else "build" if match.group("build") else "stable"
    event = env.get("GITHUB_EVENT_NAME")
    if kind == "build":
        if event != "push" or version[-8:] != env.get("GITHUB_SHA", "")[:8]:
            raise ValueError("build candidate requires a matching tag push and commit prefix")
    else:
        if (
            event != "release"
            or env.get("RELEASE_TAG") != version
            or env.get("RELEASE_DRAFT") != "false"
            or env.get("RELEASE_PRERELEASE") != ("true" if kind == "beta" else "false")
        ):
            raise ValueError("stable and beta images require a matching published release")
    if git(root, "cat-file", "-t", ref) != "tag":
        raise ValueError("release tag must be annotated")
    revision = git(root, "rev-parse", f"{ref}^{{commit}}")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("release revision is not a full commit SHA")
    if revision != git(root, "rev-parse", "HEAD") or revision != env.get("GITHUB_SHA"):
        raise ValueError("tag, checkout, and workflow commit do not match")
    if git(root, "rev-parse", "refs/remotes/origin/main") == "":
        raise ValueError("origin/main is missing")
    ancestor = subprocess.run(
        ["git", "-C", str(root), "merge-base", "--is-ancestor", revision, "origin/main"],
        capture_output=True, check=False,
    )
    if ancestor.returncode:
        raise ValueError("release commit is not on main")
    with (root / "Cargo.toml").open("rb") as stream:
        server_version = tomllib.load(stream)["workspace"]["package"]["version"]
    with (root / "apps/document-worker/Cargo.toml").open("rb") as stream:
        worker_version = tomllib.load(stream)["package"]["version"]
    if match.group("base") != server_version or match.group("base") != worker_version:
        raise ValueError("release base version differs from Rust package versions")
    return {
        "version": version,
        "revision": revision,
        "kind": kind,
        "source_url": f"https://github.com/{REPOSITORY}/archive/{revision}.tar.gz",
    }


def main() -> int:
    try:
        result = validate(Path.cwd(), dict(os.environ))
        output = os.environ["GITHUB_OUTPUT"]
        with open(output, "a", encoding="utf-8") as stream:
            for key, value in result.items():
                stream.write(f"{key}={value}\n")
    except (KeyError, OSError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"release contract failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
