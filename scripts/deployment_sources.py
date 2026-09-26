#!/usr/bin/env python3
"""Bounded, offline admission of inventoried deployment source files.

This is a high-confidence credential guard, not a general secret scanner. The
rendered semantic and Kubernetes schema gates remain responsible for behavior.
Errors deliberately contain only a source path, a fixed rule and a location.
"""

import argparse
import json
import os
import re
import stat
import sys
import tomllib
from pathlib import Path, PurePosixPath
from urllib.parse import parse_qsl, urlsplit

import yaml

from deployment_validation import UniqueSafeLoader, ValidationError, digest_image


INVENTORY = "scripts/deployment-sources.json"
ROOTS = ("deploy/kubernetes", "test-support/deployment/text-only")
RENDER_ROOTS = {
    "serving": "deploy/kubernetes/serving",
    "text-only": "test-support/deployment/text-only",
    "ingestion": "deploy/kubernetes/ingestion",
    "ingestion-rbac": "deploy/kubernetes/ingestion/rbac",
    **{f"admin-{name}": f"deploy/kubernetes/admin/{name}"
       for name in ("migrate", "maintain", "rebuild")},
    **{f"network-{name}": f"deploy/kubernetes/network/{name}"
       for name in ("base", "edge", "postgres-in-cluster", "postgres-external",
                    "dns-cluster", "dns-fixed", "monitoring", "ingestion-api",
                    "ingestion-provider")},
}
STANDALONE = {
    **{f"deploy/kubernetes/storage/{name}.example.yaml": "storage"
       for name in ("storage-class", "local-pv", "local-pvc", "local-rebuild-pv",
                    "local-rebuild-pvc")},
    "deploy/document-sandbox/namespace.yaml": "document-boundary",
    "deploy/document-sandbox/controller-role.yaml": "document-controller-role",
    "deploy/oxibelt/kubernetes-upstream.example.toml": "oxibelt",
}
EXTERNAL = tuple(path for path in STANDALONE if not path.startswith(ROOTS[0] + "/"))
ROLES = {"kustomization", "resource", "patch", "configuration", "standalone"}
MAX_FILE_BYTES = 256 * 1024
MAX_TOTAL_BYTES = 2 * 1024 * 1024
MAX_ENTRIES = 512
PATH_PATTERN = re.compile(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*")
SENSITIVE_ENV = {"OPENLEGAL_DATABASE_URL", "OPENLEGAL_MIGRATION_DATABASE_URL",
                 "OPENLEGAL_LAW_PROVIDER_CREDENTIAL"}
SENSITIVE_NAMES = ("password", "passwd", "token", "access_token", "refresh_token",
                   "api_key", "credential", "credentials", "client_key_data",
                   "private_key_data", "authorization", "provider_credential")
SENSITIVE_KEYS = {name.replace("_", "") for name in SENSITIVE_NAMES}
RAW_SENSITIVE_NAMES = "|".join(
    [name.replace("_", "[-_]?") for name in SENSITIVE_NAMES] + sorted(SENSITIVE_ENV)
)
URL_PATTERN = re.compile(r"\b(?:postgres(?:ql)?|https?)://[^\s<>\"']+", re.I)
RAW_RULES = (
    ("private-key-material", re.compile(
        r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----")),
    ("credential-url", re.compile(r"\b(?:postgres(?:ql)?|https?)://[^\s/@]+:[^\s/@]+@", re.I)),
    ("credential-assignment", re.compile(
        r"^[ \t]*(?:#[ \t]*)?[\"']?(?:" + RAW_SENSITIVE_NAMES + r")"
        r"[\"']?[ \t]*[:=][ \t]*\S", re.I | re.M)),
    ("credential-token", re.compile(
        r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{40,}|AKIA[A-Z0-9]{16})\b")),
)


class SourceValidationError(ValueError):
    """An error whose public string never includes source values or parser text."""

    def __init__(self, path, rule, location="file"):
        self.path, self.rule, self.location = path, rule, location
        super().__init__(f"{json.dumps(str(path), ensure_ascii=True)}: {rule}: {location}")


def require(condition, path, rule, location="file"):
    if not condition:
        raise SourceValidationError(path, rule, location)


def _regular_path(repo, relative):
    """Check every component without following a symlink, including ancestors."""
    current = repo
    try:
        for part in PurePosixPath(relative).parts:
            current = current / part
            mode = current.lstat().st_mode
            require(not stat.S_ISLNK(mode), relative, "symlink")
        return mode
    except OSError:
        raise SourceValidationError(relative, "missing-or-unreadable") from None


def _read(repo, relative):
    mode = _regular_path(repo, relative)
    require(stat.S_ISREG(mode), relative, "regular-file-required")
    try:
        with (repo / relative).open("rb") as source:
            raw = source.read(MAX_FILE_BYTES + 1)
        require(len(raw) <= MAX_FILE_BYTES, relative, "file-size-limit")
        return raw.decode("utf-8"), len(raw)
    except (OSError, UnicodeError):
        raise SourceValidationError(relative, "unreadable-utf8") from None


def _json_pairs(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, INVENTORY, "duplicate-inventory-key")
        result[key] = value
    return result


def _inventory(repo):
    raw, _ = _read(repo, INVENTORY)
    try:
        data = json.loads(raw, object_pairs_hook=_json_pairs)
    except (ValueError, RecursionError):
        raise SourceValidationError(INVENTORY, "invalid-inventory") from None
    require(isinstance(data, dict) and set(data) == {"version", "sources"},
            INVENTORY, "inventory-fields")
    require(type(data["version"]) is int and data["version"] == 1,
            INVENTORY, "inventory-version")
    require(isinstance(data["sources"], list) and 0 < len(data["sources"]) <= MAX_ENTRIES,
            INVENTORY, "inventory-size")
    sources = {}
    owners = set(RENDER_ROOTS) | set(STANDALONE.values())
    for entry in data["sources"]:
        require(isinstance(entry, dict) and set(entry) == {"path", "role", "owners"},
                INVENTORY, "inventory-entry")
        path = entry["path"]
        require(isinstance(path, str) and PATH_PATTERN.fullmatch(path)
                and all(part not in (".", "..") for part in path.split("/")),
                INVENTORY, "inventory-path")
        require(path in EXTERNAL or any(path.startswith(root + "/") for root in ROOTS),
                path, "inventory-scope")
        require(path not in sources, path, "duplicate-source")
        require(isinstance(entry["role"], str) and entry["role"] in ROLES,
                path, "inventory-role")
        declared = entry["owners"]
        require(isinstance(declared, list) and bool(declared)
                and all(isinstance(owner, str) and owner in owners for owner in declared)
                and len(set(declared)) == len(declared), path, "inventory-owners")
        sources[path] = entry
    require(set(STANDALONE) <= sources.keys(), INVENTORY, "missing-standalone-source")
    return sources


def _discover(repo):
    found = set()
    visited = 0

    def walk(relative, depth=0):
        nonlocal visited
        visited += 1
        require(visited <= MAX_ENTRIES and depth <= 16, relative, "directory-limit")
        mode = _regular_path(repo, relative)
        if stat.S_ISDIR(mode):
            try:
                with os.scandir(repo / relative) as entries:
                    names = []
                    for entry in entries:
                        names.append(entry.name)
                        require(len(names) + visited <= MAX_ENTRIES, relative, "directory-limit")
            except OSError:
                raise SourceValidationError(relative, "unreadable-directory") from None
            for name in sorted(names):
                walk(relative + "/" + name, depth + 1)
        else:
            require(stat.S_ISREG(mode), relative, "regular-file-required")
            found.add(relative)

    for root in ROOTS:
        require(stat.S_ISDIR(_regular_path(repo, root)), root, "directory-required")
        walk(root)
    for path in EXTERNAL:
        require(stat.S_ISREG(_regular_path(repo, path)), path, "regular-file-required")
        found.add(path)
    return found


def _parse(raw, path):
    try:
        if path.endswith(".toml"):
            return tomllib.loads(raw)
        # Aliases are unnecessary in these committed inputs. Reject them before
        # constructing objects so recursive/shared graphs cannot expand our walk.
        for index, token in enumerate(yaml.scan(raw, Loader=UniqueSafeLoader)):
            require(index < 65536, path, "yaml-token-limit")
            require(not isinstance(token, (yaml.tokens.AliasToken, yaml.tokens.AnchorToken)),
                    path, "yaml-alias", f"line {token.start_mark.line + 1}")
        documents = list(yaml.load_all(raw, Loader=UniqueSafeLoader))
        require(bool(documents) and all(isinstance(doc, dict) for doc in documents),
                path, "yaml-document-mapping")
        return documents
    except SourceValidationError:
        raise
    except (yaml.YAMLError, tomllib.TOMLDecodeError, ValidationError, ValueError,
            RecursionError, TypeError):
        raise SourceValidationError(path, "invalid-source-syntax") from None


def _guard(raw, parsed, path):
    for rule, pattern in RAW_RULES:
        match = pattern.search(raw)
        if match:
            raise SourceValidationError(path, rule, f"line {raw.count(chr(10), 0, match.start()) + 1}")
    for match in URL_PATTERN.finditer(raw):
        location = f"line {raw.count(chr(10), 0, match.start()) + 1}"
        try:
            query = parse_qsl(urlsplit(match.group()).query, keep_blank_values=True,
                              errors="strict", max_num_fields=128)
        except (ValueError, UnicodeError):
            raise SourceValidationError(path, "invalid-url-query", location) from None
        for key, _ in query:
            normalized = key.replace("-", "").replace("_", "").lower()
            require(normalized not in SENSITIVE_KEYS, path, "credential-url-query", location)
    remaining = 20000

    def walk(value, depth=0, parent=""):
        nonlocal remaining
        remaining -= 1
        require(remaining >= 0 and depth <= 32, path, "structure-limit")
        if isinstance(value, dict):
            require(value.get("kind") != "Secret", path, "secret-resource")
            require("secretGenerator" not in value, path, "secret-generator")
            if isinstance(value.get("name"), str) and value["name"] in SENSITIVE_ENV:
                require("value" not in value, path, "inline-credential-environment")
            for key, item in value.items():
                normalized = key.replace("-", "").replace("_", "").lower()
                require(normalized not in SENSITIVE_KEYS, path, "inline-credential-field")
                if parent == "postgres":
                    require(normalized not in ("url", "migrationurl"), path, "inline-database-url")
                if key in ("image", "worker_image"):
                    placeholder = ("openlegal-document-worker" if key == "worker_image" else
                                   "openlegal-server-ingestion" if path ==
                                   "deploy/kubernetes/ingestion/deployment-patch.yaml" else "openlegal-server")
                    try:
                        digest_image(item, placeholder)
                    except ValidationError:
                        raise SourceValidationError(path, "image-digest") from None
                walk(item, depth + 1, normalized)
        elif isinstance(value, list):
            for item in value:
                walk(item, depth + 1, parent)

    walk(parsed)


def _local_target(repo, source, reference, sources):
    require(isinstance(reference, str) and PATH_PATTERN.fullmatch(reference)
            and not reference.startswith("/"), source, "local-reference-required")
    try:
        candidate = (repo / source).parent / reference
        target = candidate.resolve(strict=True).relative_to(repo).as_posix()
    except (OSError, ValueError, RuntimeError):
        raise SourceValidationError(source, "reference-outside-inventory") from None
    # Also check the lexical path: resolve alone would follow a symlink.
    relative = candidate.relative_to(repo).as_posix()
    mode = _regular_path(repo, relative)
    if stat.S_ISDIR(mode):
        target += "/kustomization.yaml"
    require(target in sources, source, "reference-outside-inventory")
    return target


def _edges(repo, path, parsed, sources):
    require(isinstance(parsed, list) and len(parsed) == 1, path, "kustomization-document")
    config = parsed[0]
    allowed = {"apiVersion", "kind", "namespace", "resources", "configMapGenerator", "patches"}
    require(set(config) <= allowed and config.get("apiVersion") == "kustomize.config.k8s.io/v1beta1"
            and config.get("kind") == "Kustomization", path, "unsupported-kustomization")
    if "namespace" in config:
        require(config["namespace"] == "openlegal-serving", path, "kustomization-namespace")
    edges = set()

    def add(reference, roles):
        target = _local_target(repo, path, reference, sources)
        require(sources[target]["role"] in roles, path, "reference-role")
        require(target not in edges, path, "duplicate-reference")
        edges.add(target)

    resources = config.get("resources", [])
    require(isinstance(resources, list), path, "resource-list")
    for reference in resources:
        add(reference, {"resource", "kustomization"})
    generators = config.get("configMapGenerator", [])
    require(isinstance(generators, list), path, "generator-list")
    names = set()
    for generator in generators:
        require(isinstance(generator, dict) and set(generator) <= {"name", "behavior", "files"}
                and {"name", "files"} <= set(generator), path, "unsupported-generator")
        name = generator["name"]
        require(isinstance(name, str) and name in
                ("openlegal-server-config", "openlegal-document-controller-config")
                and name not in names, path, "generator-name")
        names.add(name)
        if "behavior" in generator:
            require(generator["behavior"] == "replace", path, "generator-behavior")
        files = generator["files"]
        require(isinstance(files, list) and bool(files), path, "generator-files")
        for reference in files:
            add(reference, {"configuration"})
    patches = config.get("patches", [])
    require(isinstance(patches, list), path, "patch-list")
    for patch in patches:
        require(isinstance(patch, dict) and set(patch) == {"path"}, path, "unsupported-patch")
        add(patch["path"], {"patch"})
    return edges


def validate_sources(repo: Path):
    """Validate the full source inventory before any Kustomize invocation."""
    repo = repo.resolve()
    sources = _inventory(repo)
    found = _discover(repo)
    for path in sorted(found - sources.keys()):
        raise SourceValidationError(path, "unexpected-source")
    for path in sorted(sources.keys() - found):
        raise SourceValidationError(path, "missing-source")
    parsed, total = {}, 0
    for path, entry in sources.items():
        raw, size = _read(repo, path)
        total += size
        require(total <= MAX_TOTAL_BYTES, path, "total-size-limit")
        parsed[path] = _parse(raw, path)
        _guard(raw, parsed[path], path)
        role = entry["role"]
        if path in STANDALONE:
            require(role == "standalone", path, "standalone-role")
        elif path.endswith("/kustomization.yaml"):
            require(role == "kustomization", path, "kustomization-role")
        elif path.endswith((".toml", ".json")) or path.endswith("/kubeconfig"):
            require(role == "configuration", path, "configuration-role")
        else:
            require(role in ("resource", "patch"), path, "resource-role")
        if isinstance(parsed[path], list) and role != "kustomization":
            require(all(doc.get("kind") != "Kustomization" for doc in parsed[path]),
                    path, "unclassified-kustomization")
    edges = {path: _edges(repo, path, parsed[path], sources)
             for path, entry in sources.items() if entry["role"] == "kustomization"}
    owners = {path: set() for path in sources}

    def visit(path, owner, active):
        require(path not in active, path, "kustomization-cycle")
        if owner in owners[path]:
            return
        owners[path].add(owner)
        for target in sorted(edges.get(path, ())):
            visit(target, owner, active | {path})

    for owner, directory in RENDER_ROOTS.items():
        root = directory + "/kustomization.yaml"
        require(root in edges, root, "missing-render-root")
        visit(root, owner, set())
    for path, owner in STANDALONE.items():
        owners[path].add(owner)
    for path, entry in sources.items():
        require(bool(owners[path]), path, "unreachable-source")
        require(owners[path] == set(entry["owners"]), path, "validation-owner-drift")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", nargs="?", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    try:
        validate_sources(args.repo)
    except SourceValidationError as error:
        parser.exit(1, f"Deployment source validation failed: {error}\n")
    except (OSError, ValueError, TypeError, RecursionError, RuntimeError):
        parser.exit(1, 'Deployment source validation failed: "repository": invalid-source: file\n')
    print("Deployment source inventory and credential guards passed (offline).")


if __name__ == "__main__":
    main()
