"""Collect notices from the resolved locked graph, not unrelated cache entries.

Build-only helper: preserves nested native-library notices and Lindera's embedded
Korean dictionary notice. Package source and compiler caches are not distributed.
"""

import json
import hashlib
from pathlib import Path
import shutil
import sys

metadata = json.loads(Path(sys.argv[1]).read_text())
destination = Path(sys.argv[2])
supplements = Path(__file__).parent / "notices"
manifest = json.loads((supplements / "manifest.json").read_text())
if manifest["version"] != 1:
    raise SystemExit("Unsupported supplemental notice manifest")
nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
pending = [p["id"] for p in metadata["packages"] if p["name"] == "openlegal-server"]
if len(pending) != 1:
    raise SystemExit("Expected exactly one server package")
resolved = set()
while pending:
    package_id = pending.pop()
    if package_id in resolved:
        continue
    resolved.add(package_id)
    pending.extend(dep["pkg"] for dep in nodes[package_id]["deps"]
                   if any(kind["kind"] != "dev" for kind in dep["dep_kinds"]))
inventory = []
for package in metadata["packages"]:
    if package["id"] not in resolved or package["source"] is None:
        continue
    root = Path(package["manifest_path"]).parent
    output = destination / f'{package["name"]}-{package["version"]}'
    notices = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or not path.is_file():
            continue
        if not any(part.lower().startswith(("license", "copying", "notice", "copyright", "authors"))
                   for part in path.relative_to(root).parts):
            continue
        target = output / path.relative_to(root)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
        notices.append({"path": str(target.relative_to(destination)),
                        "source": "published crate: " + str(path.relative_to(root))})

    supplement = manifest["packages"].get(f'{package["name"]}@{package["version"]}')
    if supplement:
        vcs_path = root / ".cargo_vcs_info.json"
        if vcs_path.exists() and json.loads(vcs_path.read_text())["git"]["sha1"] != supplement["revision"]:
            raise SystemExit(f'Notice revision mismatch: {package["name"]}')
        for notice in supplement["files"]:
            relative = Path(notice["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise SystemExit("Invalid supplemental notice path")
            data = (supplements / relative).read_bytes()
            if not data or hashlib.sha256(data).hexdigest() != notice["sha256"]:
                raise SystemExit(f'Notice digest mismatch: {relative}')
            target = output / "supplemental" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            notices.append({"path": str(target.relative_to(destination)),
                            "source": notice["source"], "sha256": notice["sha256"]})
    if not notices:
        raise SystemExit(f'Missing dependency notices: {package["name"]}@{package["version"]}')
    inventory.append({"name": package["name"], "version": package["version"],
                      "license": package["license"], "repository": package["repository"],
                      "notices": notices})

# This dictionary is embedded in the server rather than mounted by the operator.
dictionary_notice = list(destination.glob("lindera-ko-dic-*/NOTICE.txt"))
if len(dictionary_notice) != 1:
    raise SystemExit("Expected the locked embedded Korean dictionary notice")
(destination / "inventory.json").write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n")
shutil.copyfile(supplements / "manifest.json", destination / "supplemental-sources.json")
