#!/usr/bin/env python3
"""Verify the upstream manifest before replacing its exact admitted image fields."""
import argparse
import hashlib
import json
from pathlib import Path
import re

LOCK = Path(__file__).with_name("assets.lock.json")


def pin_manifest(raw: bytes, lock: dict) -> str:
    if hashlib.sha256(raw).hexdigest() != lock["calico_sha256"]:
        raise ValueError("Calico manifest checksum mismatch")
    text = raw.decode("utf-8")
    seen = set()

    def replace(match):
        original = match[2]
        if original not in lock["calico_images"]:
            raise ValueError("Unadmitted Calico image")
        seen.add(original)
        pinned = lock["calico_images"][original]
        if not re.fullmatch(re.escape(original) + r"@sha256:[0-9a-f]{64}", pinned):
            raise ValueError("Invalid Calico image pin")
        return match[1] + pinned

    text = re.sub(r"(?m)^([ \t]*(?:-[ \t]+)?image:[ \t]*)(\S+)[ \t]*$", replace, text)
    if seen != set(lock["calico_images"]):
        raise ValueError("Calico image inventory mismatch")
    # Use preloaded images only; an offline fixture must never mask a missing
    # admitted image by pulling a mutable tag or contacting registries at runtime.
    text = re.sub(r"(?m)^([ \t]*imagePullPolicy:)[ \t]*\S+", r"\1 Never", text)
    text, count = re.subn(
        r'(?m)^( +)# - name: CALICO_IPV4POOL_CIDR\n +#   value: "192\.168\.0\.0/16"$',
        r'\1- name: CALICO_IPV4POOL_CIDR\n\1  value: "10.244.0.0/16"',
        text,
    )
    if count != 1:
        raise ValueError("Calico default pool declaration mismatch")
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    lock = json.loads(LOCK.read_text())
    args.destination.write_text(pin_manifest(args.source.read_bytes(), lock))


if __name__ == "__main__":
    main()
