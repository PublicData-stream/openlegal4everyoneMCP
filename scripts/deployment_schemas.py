#!/usr/bin/env python3
"""Provision pinned deployment schemas explicitly; validate without network access."""

import argparse
import copy
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


LOCK = Path(__file__).with_name("deployment-schema-lock.json")
BUNDLE = "schema-validation"
INPUTS = (
    "retained.yaml", "text-only.yaml", "ingestion.yaml", "ingestion-rbac.yaml",
    "migrate.yaml", "maintain.yaml", "rebuild.yaml", "storage.yaml",
    *(f"network/{name}.yaml" for name in (
        "base", "edge", "postgres-in-cluster", "postgres-external", "dns-cluster",
        "dns-fixed", "monitoring", "ingestion-api", "ingestion-provider")),
)
DOCUMENT_INPUTS = ("namespace.yaml", "controller-role.yaml")
KINDS = {
    ("v1", name): f"{name.lower()}-v1.json" for name in (
        "Namespace", "ConfigMap", "Service", "ServiceAccount", "PersistentVolume",
        "PersistentVolumeClaim", "ResourceQuota")
} | {
    (group, kind): f"{kind.lower()}-{short}-v1.json"
    for group, kind, short in (
        ("apps/v1", "Deployment", "apps"), ("batch/v1", "Job", "batch"),
        ("networking.k8s.io/v1", "NetworkPolicy", "networking"),
        ("node.k8s.io/v1", "RuntimeClass", "node"),
        ("rbac.authorization.k8s.io/v1", "Role", "rbac"),
        ("rbac.authorization.k8s.io/v1", "RoleBinding", "rbac"),
        ("storage.k8s.io/v1", "StorageClass", "storage"),
    )
}
STORAGE_NAMES = (
    "cache-blobs", "corpus-blobs", "corpus-index", "mecab-dictionary",
    "corpus-index-rebuild",
)
SETUP_HINT = ("Remove the schema-validation bundle and run "
              "scripts/setup-deployment-tools.sh to provision verified assets.")


class SchemaError(ValueError):
    """A fixed, value-free diagnostic safe to display with untrusted manifests."""


def require(condition, message):
    if not condition:
        raise SchemaError(message)


def host_arch():
    require(platform.system() == "Linux", "Schema tools require Linux amd64 or arm64.")
    arch = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(platform.machine())
    require(arch is not None, "Schema tools require Linux amd64 or arm64.")
    return arch


def load_lock():
    lock = json.loads(LOCK.read_bytes())
    require(lock["format"] == 1, "Unsupported deployment schema lock format.")
    require(lock["schemas"]["versions"] == ["1.36.0", "1.37.0"], "Unexpected schema versions.")
    expected = {f"v{version}-standalone-strict/{name}"
                for version in lock["schemas"]["versions"] for name in KINDS.values()}
    require(set(lock["schemas"]["files"]) == expected, "Incomplete deployment schema lock.")
    return lock


def checked_bytes(data, asset):
    require(len(data) == asset["size"] and hashlib.sha256(data).hexdigest() == asset["sha256"],
            "Deployment asset size or SHA-256 mismatch. " + SETUP_HINT)
    return data


def reject_external_refs(schema):
    """Descriptions and $schema URIs are metadata; only local $ref targets are allowed."""
    if isinstance(schema, dict):
        if "$ref" in schema:
            ref = schema["$ref"]
            require(isinstance(ref, str) and (ref == "#" or ref.startswith("#/")),
                    "External schema reference is forbidden.")
        for value in schema.values():
            reject_external_refs(value)
    elif isinstance(schema, list):
        for value in schema:
            reject_external_refs(value)


def installed_assets(lock, arch):
    return {
        "bin/kubeconform": lock["kubeconform"]["archives"][arch]["binary"],
        "licenses/kubeconform-LICENSE": lock["kubeconform"]["license_file"],
        "licenses/kubernetes-json-schema-LICENSE": lock["schemas"]["license_file"],
        **{f"schemas/{name}": asset for name, asset in lock["schemas"]["files"].items()},
    }


def verify_bundle(bundle, lock, arch):
    require(bundle.is_dir() and not bundle.is_symlink(), "Missing or symlinked schema bundle. " + SETUP_HINT)
    assets = installed_assets(lock, arch)
    actual = set()
    for path in bundle.rglob("*"):
        require(not path.is_symlink(), "Symlinked schema asset is forbidden. " + SETUP_HINT)
        if not path.is_dir():
            require(path.is_file(), "Non-regular schema asset is forbidden. " + SETUP_HINT)
            actual.add(path.relative_to(bundle).as_posix())
    require(actual == set(assets), "Missing or unexpected schema assets. " + SETUP_HINT)
    for name, asset in assets.items():
        path = bundle / name
        require(path.stat().st_size == asset["size"], "Deployment asset size mismatch. " + SETUP_HINT)
        data = checked_bytes(path.read_bytes(), asset)
        if name.startswith("schemas/"):
            reject_external_refs(json.loads(data))
    require(os.access(bundle / "bin/kubeconform", os.X_OK), "Schema validator is not executable. " + SETUP_HINT)


def verify(tools_dir, lock=None, arch=None):
    lock = load_lock() if lock is None else lock
    arch = host_arch() if arch is None else arch
    verify_bundle(tools_dir / BUNDLE, lock, arch)
    return tools_dir / BUNDLE


class PinnedRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        check_download_url(newurl)
        return super().redirect_request(request, fp, code, msg, headers, newurl)


def check_download_url(url):
    parsed = urllib.parse.urlsplit(url)
    require(parsed.scheme == "https" and parsed.hostname in {
        "github.com", "raw.githubusercontent.com", "release-assets.githubusercontent.com",
    } and parsed.port in (None, 443) and parsed.username is None and parsed.password is None,
            "Deployment asset destination is not allowed.")


def download(asset, deadline):
    """Exact byte limits, TLS, bounded redirects/time and no error-body diagnostics."""
    check_download_url(asset["url"])
    require(0 < asset["size"] <= 16 * 1024 * 1024, "Deployment asset exceeds download limit.")
    chunks = []
    remaining = asset["size"] + 1
    try:
        require(time.monotonic() < deadline, "Deployment asset provisioning deadline exceeded.")
        with urllib.request.build_opener(PinnedRedirects()).open(asset["url"], timeout=30) as response:
            while remaining:
                require(time.monotonic() < deadline, "Deployment asset provisioning deadline exceeded.")
                # read1 performs one buffered/socket read so trickled bytes cannot
                # keep a read(size) filling indefinitely beyond the shared deadline.
                chunk = response.read1(min(65536, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
    except (urllib.error.URLError, http.client.HTTPException, OSError, TimeoutError):
        raise SchemaError("Pinned deployment asset download failed; no bundle was published.") from None
    return checked_bytes(b"".join(chunks), asset)


def setup(tools_dir, lock=None, arch=None):
    lock = load_lock() if lock is None else lock
    arch = host_arch() if arch is None else arch
    bundle = tools_dir / BUNDLE
    if bundle.exists() or bundle.is_symlink():
        verify_bundle(bundle, lock, arch)
        return
    tools_dir.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + 600
    with tempfile.TemporaryDirectory(prefix=".schema-staging-", dir=tools_dir) as scratch:
        staging = Path(scratch) / BUNDLE
        for directory in ("bin", "licenses", "schemas"):
            (staging / directory).mkdir(parents=True)
        archive = lock["kubeconform"]["archives"][arch]
        with tarfile.open(fileobj=io.BytesIO(download(archive, deadline)), mode="r:gz") as release:
            members = release.getmembers()
            require({item.name for item in members} == {"kubeconform", "LICENSE"}
                    and len(members) == 2 and all(item.isfile() for item in members),
                    "Unexpected kubeconform archive members.")
            for name, destination, asset in (
                ("kubeconform", "bin/kubeconform", archive["binary"]),
                ("LICENSE", "licenses/kubeconform-LICENSE", lock["kubeconform"]["license_file"]),
            ):
                require(release.getmember(name).size == asset["size"], "Unexpected extracted asset size.")
                data = checked_bytes(release.extractfile(name).read(asset["size"] + 1), asset)
                (staging / destination).write_bytes(data)
        (staging / "bin/kubeconform").chmod(0o755)
        license_file = lock["schemas"]["license_file"]
        (staging / "licenses/kubernetes-json-schema-LICENSE").write_bytes(download(license_file, deadline))
        for name, asset in lock["schemas"]["files"].items():
            destination = staging / "schemas" / name
            destination.parent.mkdir(exist_ok=True)
            data = download(asset, deadline)
            reject_external_refs(json.loads(data))
            destination.write_bytes(data)
        verify_bundle(staging, lock, arch)
        # Same-filesystem rename publishes only a fully verified bundle. An existing
        # nonempty bundle is never overwritten, including concurrent provisioning.
        staging.rename(bundle)


def storage_schema_copy(documents):
    """Replace only reviewed operator placeholders in a disposable deep copy."""
    result = copy.deepcopy(documents)
    expected = {(kind, f"openlegal-{suffix}") for kind in ("PersistentVolume", "PersistentVolumeClaim")
                for suffix in STORAGE_NAMES} | {("StorageClass", "openlegal-local")}
    require(all(isinstance(item, dict) and isinstance(item.get("metadata"), dict)
                and isinstance(item.get("kind"), str) and isinstance(item["metadata"].get("name"), str)
                for item in result), "storage.yaml: schema fixture resource identities changed.")
    identities = [(item.get("kind"), item.get("metadata", {}).get("name")) for item in result]
    require(len(identities) == len(expected) and set(identities) == expected,
            "storage.yaml: schema fixture resource identities changed.")
    try:
        for item in result:
            if item["kind"] == "StorageClass":
                require(item["apiVersion"] == "storage.k8s.io/v1" and "namespace" not in item["metadata"],
                        "storage.yaml: schema fixture StorageClass identity changed.")
                continue
            require(item["apiVersion"] == "v1", "storage.yaml: schema fixture API changed.")
            suffix = item["metadata"]["name"].removeprefix("openlegal-")
            placeholder = suffix.upper().replace("-", "_")
            spec = item["spec"]
            if item["kind"] == "PersistentVolume":
                require("namespace" not in item["metadata"], "storage.yaml: PV namespace changed.")
                require(spec["capacity"]["storage"] == f"REPLACE_WITH_{placeholder}_CAPACITY"
                        and spec["local"]["path"] == f"/REPLACE_WITH_{placeholder}_HOST_PATH",
                        "storage.yaml: schema fixture PV placeholders changed.")
                terms = spec["nodeAffinity"]["required"]["nodeSelectorTerms"]
                require(terms == [{"matchExpressions": [{"key": "kubernetes.io/hostname",
                        "operator": "In", "values": ["REPLACE_WITH_STORAGE_NODE"]}]}],
                        "storage.yaml: schema fixture node placeholder changed.")
                spec["capacity"]["storage"] = "1Gi"
                spec["local"]["path"] = f"/var/lib/openlegal/schema-fixture/{suffix}"
                terms[0]["matchExpressions"][0]["values"] = ["schema-fixture-node"]
            else:
                require(item["metadata"]["namespace"] == "openlegal-serving"
                        and spec["resources"]["requests"]["storage"] == f"REPLACE_WITH_{placeholder}_CAPACITY",
                        "storage.yaml: schema fixture PVC identity or placeholder changed.")
                spec["resources"]["requests"]["storage"] = "1Gi"
    except (KeyError, TypeError, IndexError):
        raise SchemaError("storage.yaml: schema fixture field paths changed.") from None
    return result


def read_documents(path, label):
    # Imported only for validation: setup/verify work with the Python standard library.
    import yaml
    from deployment_validation import load_documents

    try:
        require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 256 * 1024,
                f"{label}: missing, symlinked or oversized schema input.")
        text = path.read_text()
        require(not any(isinstance(token, yaml.tokens.AliasToken) for token in yaml.scan(text)),
                f"{label}: YAML aliases are forbidden in schema inputs.")
        documents = [item for item in load_documents(text) if item is not None]
        require(documents and all(isinstance(item, dict) for item in documents),
                f"{label}: schema input must contain resource mappings.")
        for item in documents:
            require((item.get("apiVersion"), item.get("kind")) in KINDS,
                    f"{label}: resource type has no admitted offline schema.")
        return documents
    except (ValueError, yaml.YAMLError, OSError, TypeError, RecursionError):
        raise SchemaError(f"{label}: malformed or unsupported schema input.") from None


def validate_documents(bundle, version, documents, label):
    location = str(bundle.absolute() / "schemas" /
                   "{{.NormalizedKubernetesVersion}}-standalone{{.StrictSuffix}}" /
                   "{{.ResourceKind}}{{.KindSuffix}}.json")
    command = [str(bundle.absolute() / "bin/kubeconform"), "-strict", "-summary",
               "-output", "json", "-n", "1", "-kubernetes-version", version,
               "-schema-location", location]
    payload = "\n---\n".join(json.dumps(item) for item in documents).encode()
    with tempfile.TemporaryDirectory(prefix="openlegal-schema-check-") as scratch:
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
            try:
                run = subprocess.run(command, input=payload, stdout=output, stderr=errors,
                                     timeout=30, check=False, env={"HOME": scratch, "TMPDIR": scratch})
                require(output.tell() <= 1024 * 1024, f"{label}: schema diagnostic limit exceeded.")
                output.seek(0)
                summary = json.load(output).get("summary", {})
            except (OSError, subprocess.TimeoutExpired, ValueError, AttributeError):
                raise SchemaError(f"{label}: Kubernetes {version} schema validator failed.") from None
    require(run.returncode == 0 and summary == {
        "valid": len(documents), "invalid": 0, "errors": 0, "skipped": 0,
    }, f"{label}: Kubernetes {version} strict schema validation failed; "
       "resource values and validator diagnostics withheld.")


def validate(tools_dir, rendered_dir, repo):
    lock = load_lock()
    bundle = verify(tools_dir, lock)
    inputs = [(rendered_dir / name, f"rendered/{name}") for name in INPUTS]
    inputs += [(repo / "deploy/document-sandbox" / name, f"deploy/document-sandbox/{name}")
               for name in DOCUMENT_INPUTS]
    total = 0
    for path, label in inputs:
        documents = read_documents(path, label)
        if label == "rendered/storage.yaml":
            documents = storage_schema_copy(documents)
        for version in lock["schemas"]["versions"]:
            validate_documents(bundle, version, documents, label)
        total += len(documents)
    return total


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("setup", "verify", "validate"))
    parser.add_argument("--tools-dir", type=Path, required=True)
    parser.add_argument("--rendered-dir", type=Path)
    parser.add_argument("--repo", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "setup":
            setup(args.tools_dir)
            print("Pinned schema validator and Kubernetes schemas provisioned.")
        elif args.command == "verify":
            verify(args.tools_dir)
        else:
            require(args.rendered_dir is not None and args.repo is not None,
                    "validate requires --rendered-dir and --repo.")
            count = validate(args.tools_dir, args.rendered_dir, args.repo)
            print(f"Strict offline schemas: {count} resources each passed Kubernetes 1.36.0 and 1.37.0; no skips.")
    except SchemaError as error:
        print(f"Deployment schemas: {error}", file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, TypeError, RecursionError, tarfile.TarError):
        # Never display parser/download/subprocess details: they may contain values.
        print("Deployment schemas: asset or input processing failed; diagnostics withheld.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
