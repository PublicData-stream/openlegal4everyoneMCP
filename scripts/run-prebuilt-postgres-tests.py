#!/usr/bin/env python3
"""Execute trusted, hash-verified Rust test artifacts on a separate native host.

Compilation and execution are separate evidence levels. This runner never builds
or fetches dependencies. Only use operator-provided test artifacts and manifest.
"""
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


def main():
    if len(sys.argv) != 2:
        raise ValueError("expected artifact directory")
    root = Path(sys.argv[1]).resolve(strict=True)
    items = json.loads((root / "manifest.json").read_text())
    if not isinstance(items, list) or not 1 <= len(items) <= 128:
        raise ValueError("invalid artifact manifest")
    checked = []
    for item in items:
        name = item["executable"]
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", name):
            raise ValueError("invalid artifact name")
        executable = root / name
        if executable.is_symlink() or not executable.is_file():
            raise ValueError("invalid artifact file")
        with executable.open("rb") as source:
            actual = hashlib.file_digest(source, "sha256").hexdigest()
        if actual != item["sha256"]:
            raise ValueError("artifact digest mismatch")
        checked.append(executable)
    for executable in checked:
        print("Native PostgreSQL test artifact: " + executable.name, flush=True)
        subprocess.run([str(executable), "--ignored", "--test-threads=1"],
                       check=True, timeout=900)
    print("Native execution passed; artifacts were compiled separately.", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        print("Prebuilt PostgreSQL test execution failed.", file=sys.stderr)
        sys.exit(1)
