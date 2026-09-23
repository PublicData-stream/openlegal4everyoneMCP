#!/usr/bin/env python3
"""Stamp a validated release tag into first-party Rust version definitions."""

import argparse
from pathlib import Path
import re
import sys
import tomllib


PLACEHOLDER = "0.0.0"
TAG = re.compile(
    r"(?P<base>(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*))"
    r"(?:(?P<beta>-beta\.[1-9][0-9]*)|(?P<build>-build\.[0-9a-f]{8}))?\Z"
)
PACKAGES = {
    "openlegal-server",
    "openlegal-domain",
    "openlegal-normalization",
    "openlegal-application",
    "openlegal-adapters",
    "openlegal-document-worker",
}
ROOT_PACKAGES = PACKAGES - {"openlegal-document-worker"}
WORKER_PACKAGES = {"openlegal-document-worker", "openlegal-domain", "openlegal-application"}
MANIFESTS = (
    "Cargo.toml",
    "apps/server/Cargo.toml",
    "crates/domain/Cargo.toml",
    "crates/normalization/Cargo.toml",
    "crates/application/Cargo.toml",
    "crates/adapters/Cargo.toml",
    "apps/document-worker/Cargo.toml",
)
LOCKS = {"Cargo.lock": ROOT_PACKAGES, "apps/document-worker/Cargo.lock": WORKER_PACKAGES}
DENY = {"deny.toml": ROOT_PACKAGES, "apps/document-worker/deny.toml": WORKER_PACKAGES}
FILES = (*MANIFESTS, *LOCKS, *DENY)


def _named_versions(items, expected_names, version, label):
    found = {}
    for name, actual in items:
        if not name.startswith("openlegal-"):
            continue
        if name in found:
            raise ValueError(f"{label}: duplicate first-party package {name}")
        found[name] = actual
    if found.keys() != expected_names or any(value != version for value in found.values()):
        raise ValueError(f"{label}: first-party versions differ from {version}")


def validate_documents(documents: dict[str, str], version: str) -> None:
    """Check every first-party version surface before or after stamping."""
    parsed = {path: tomllib.loads(content) for path, content in documents.items()}
    if parsed["Cargo.toml"]["workspace"]["package"]["version"] != version:
        raise ValueError("Cargo.toml: workspace version differs")
    package_versions = []
    for path in MANIFESTS[1:]:
        manifest = parsed[path]
        package = manifest["package"]
        declared = package.get("version")
        expected = version if path == "apps/document-worker/Cargo.toml" else {"workspace": True}
        if declared != expected:
            raise ValueError(f"{path}: package version differs")
        package_versions.append((package["name"], version))
        for name, dependency in manifest.get("dependencies", {}).items():
            if name.startswith("openlegal-"):
                if (
                    not isinstance(dependency, dict)
                    or "path" not in dependency
                    or dependency.get("version") != version
                ):
                    raise ValueError(f"{path}: first-party path dependency {name} differs")
    _named_versions(package_versions, PACKAGES, version, "manifests")
    for path, expected in LOCKS.items():
        packages = parsed[path]["package"]
        _named_versions(
            ((entry["name"], entry["version"]) for entry in packages), expected, version, path
        )
    for path, expected in DENY.items():
        exceptions = parsed[path]["licenses"]["exceptions"]
        items = []
        for entry in exceptions:
            name, separator, actual = entry["crate"].partition("@")
            if name.startswith("openlegal-"):
                if not separator or entry["allow"] != ["AGPL-3.0-only"]:
                    raise ValueError(f"{path}: invalid first-party license exception")
                items.append((name, actual))
        _named_versions(items, expected, version, path)


def _replace_exact(content: str, pattern: str, replacement: str, count: int, path: str) -> str:
    changed, actual = re.subn(pattern, replacement, content, flags=re.MULTILINE)
    if actual != count:
        raise ValueError(f"{path}: expected {count} version edits, found {actual}")
    return changed


def render(documents: dict[str, str], version: str) -> dict[str, str]:
    if TAG.fullmatch(version) is None or version == PLACEHOLDER:
        raise ValueError("release version has an unsupported format")
    validate_documents(documents, PLACEHOLDER)
    result = documents.copy()
    escaped = re.escape(PLACEHOLDER)
    for path in MANIFESTS:
        source = result[path]
        if path in ("Cargo.toml", "apps/document-worker/Cargo.toml"):
            source = _replace_exact(
                source, rf'^version = "{escaped}"$', f'version = "{version}"', 1, path
            )
        dependency_count = sum(
            name.startswith("openlegal-") for name in tomllib.loads(source).get("dependencies", {})
        )
        if dependency_count:
            source = _replace_exact(
                source,
                rf'(openlegal-[a-z-]+ = \{{[^\n]*\bversion = "){escaped}("[^\n]*\}})',
                rf'\g<1>{version}\2', dependency_count, path,
            )
        result[path] = source
    for path, expected in LOCKS.items():
        result[path] = _replace_exact(
            result[path],
            rf'(^\[\[package\]\]\nname = "openlegal-[a-z-]+"\nversion = "){escaped}("$)',
            rf'\g<1>{version}\2', len(expected), path,
        )
    for path, expected in DENY.items():
        result[path] = _replace_exact(
            result[path], rf'(crate = "openlegal-[a-z-]+@){escaped}(")',
            rf'\g<1>{version}\2', len(expected), path,
        )
    validate_documents(result, version)
    return result


def load_documents(root: Path) -> dict[str, str]:
    return {path: (root / path).read_text(encoding="utf-8") for path in FILES}


def sync(root: Path, version: str) -> None:
    rendered = render(load_documents(root), version)
    for path, content in rendered.items():
        (root / path).write_text(content, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="validated full release tag")
    args = parser.parse_args()
    try:
        sync(Path.cwd(), args.version)
    except (KeyError, OSError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"release version sync failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
