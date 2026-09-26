#!/usr/bin/env python3
"""Select bounded, untrusted pilot candidates from operator-downloaded lists.

The output is an identity hint only. The running adapter must fetch and verify
each detail from LAW OPEN DATA before any record can be published.
"""
import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from xml.etree import ElementTree as ET


SOURCES = (
    ("법령검색목록.xml", "national_statute", "법령ID", "법령MST", "법령명", "시행일자", "공포일자", None, None),
    ("자치법규검색목록.xml", "ordinance", "ordinId", "ordinMST", "ordinNm", "efYd", "ancYd", None, None),
    ("양자조약검색목록.xml", "treaty", "trtySeq", "trtySeq", "trtyNm", "eftYd", None, None, "440101"),
    ("다자조약검색목록.xml", "treaty", "trtySeq", "trtySeq", "trtyNm", "eftYd", None, None, "440102"),
    ("판례검색목록.xml", "precedent", "precSeq", "precSeq", "evtNm", None, None, "evtNo", None),
    ("헌재결정례검색목록.xml", "constitutional_decision", "detcSeq", "detcSeq", "evtNm", None, None, "evtNo", None),
    ("법령해석검색목록.xml", "legal_interpretation", "expcSeq", "expcSeq", "itmNm", None, None, "itmNo", None),
    ("행정심판례검색목록.xml", "administrative_appeal", "deccSeq", "deccSeq", "evtNm", None, None, "evtNo", None),
)


def field(row, tag):
    return (row.findtext(tag) or "").strip() if tag else ""


def candidate(row, spec):
    _, dataset, id_tag, rev_tag, title_tag, effective_tag, publication_tag, case_tag, treaty_class = spec
    identifier, revision = field(row, id_tag), field(row, rev_tag)
    if not re.fullmatch(r"[0-9]{1,128}", identifier) or identifier == "0":
        return None
    if not re.fullmatch(r"[0-9]{1,128}", revision) or revision == "0":
        return None
    effective, publication = field(row, effective_tag), field(row, publication_tag)
    if effective and not re.fullmatch(r"[0-9]{8}", effective):
        return None
    if publication and not re.fullmatch(r"[0-9]{8}", publication):
        return None
    if dataset == "national_statute":
        if not effective:
            return None
        revision += ":" + effective
    if dataset == "treaty" and field(row, "trtyClsCd") != treaty_class:
        return None
    title = field(row, title_tag)
    if len(title.encode()) > 512:
        return None
    return {
        "object": {"jurisdiction": "kr", "provider": "law_go_kr", "dataset": dataset, "id": identifier},
        "revision_id": revision,
        "effective_date": effective or None,
        "publication_date": publication or None,
        "title": title,
        "data_source": None,
        "case_number": field(row, case_tag) or None,
        "treaty_class_code": treaty_class,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()
    candidates, failures = [], []
    for spec in SOURCES:
        path = args.input_dir / spec[0]
        try:
            with path.open("rb") as source:
                data = source.read(64 * 1024 * 1024 + 1)
            if len(data) > 64 * 1024 * 1024:
                raise ValueError("file exceeds 64 MiB")
            text = data.decode("utf-8-sig")
            if "\x00" in text or "<!DOCTYPE" in text.upper() or "<!ENTITY" in text.upper():
                raise ValueError("DTD and entities are not admitted")
            root = ET.fromstring(text)
            if root.tag not in {"cdList", "현행법령목록"}:
                raise ValueError("unexpected root element")
            selected, seen = [], set()
            for row in root:
                item = candidate(row, spec)
                if item is None:
                    continue
                key = (item["object"]["id"], item["revision_id"])
                if key not in seen:
                    seen.add(key)
                    selected.append(item)
                if len(selected) == 2:
                    break
            if not selected:
                raise ValueError("no usable nonzero candidate IDs")
            candidates.extend(selected)
            print(f"{path.name}: {len(selected)} candidates, sha256={hashlib.sha256(data).hexdigest()}", file=sys.stderr)
        except (OSError, UnicodeError, ET.ParseError, ValueError) as exc:
            failures.append(path.name)
            print(f"{path.name}: skipped ({exc})", file=sys.stderr)
    if failures and not args.allow_incomplete:
        parser.error("incomplete exports; rerun with --allow-incomplete for live-list fallback: " + ", ".join(failures))
    print("행정규칙검색목록.xml: omitted (manual ID/LID relation awaiting verification); pilot uses the live list", file=sys.stderr)
    args.output.write_text(json.dumps({"version": 1, "candidates": candidates}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {len(candidates)} identity hints to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
