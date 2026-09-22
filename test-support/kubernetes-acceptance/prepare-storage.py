#!/usr/bin/env python3
"""Render the canonical Local PV templates for one disposable fixture node."""
import argparse
from pathlib import Path
import re

REPO = Path(__file__).resolve().parents[2]


def render(node: str) -> str:
    if not re.fullmatch(r"openlegal-accept-[0-9a-f]{16}-control-plane", node):
        raise ValueError("Expected an owned disposable acceptance node")
    substitutions = {"REPLACE_WITH_STORAGE_NODE": node}
    for placeholder, directory in (
        ("CACHE_BLOBS", "cache-blobs"),
        ("CORPUS_BLOBS", "corpus-blobs"),
        ("CORPUS_INDEX", "corpus-index"),
        ("MECAB_DICTIONARY", "mecab-ko-dictionary"),
    ):
        # Local PV capacity is scheduling metadata, not a filesystem quota.
        substitutions[f"REPLACE_WITH_{placeholder}_CAPACITY"] = "4Gi"
        substitutions[f"/REPLACE_WITH_{placeholder}_HOST_PATH"] = f"/var/local/openlegal/{directory}"
    parts = []
    for name in ("storage-class", "local-pv", "local-pvc"):
        text = (REPO / f"deploy/kubernetes/storage/{name}.example.yaml").read_text()
        for old, new in substitutions.items():
            text = text.replace(old, new)
        if "REPLACE_WITH_" in text:
            raise ValueError("Canonical storage template has an unresolved placeholder")
        parts.append(text)
    return "\n---\n".join(parts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with args.output.open("x") as output:
        output.write(render(args.node))


if __name__ == "__main__":
    main()
