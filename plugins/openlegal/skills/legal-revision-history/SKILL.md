---
name: legal-revision-history
description: Trace how a Korean statute or ordinance changed over time, compare two versions and watch laws for amendments from the OpenLegal corpus. Use when the user asks about 개정 이력, 연혁, 신구조문 비교, 개정 여부 확인, 폐지·개명, 시행 예정, what changed in a law, or the text of a past version.
license: AGPL-3.0-only
metadata:
  version: "0.3.1"
---

# Legal revision history

Use the OpenLegal MCP server to list retained versions of one legal object and
compare them line by line. First identify the object with the
`korean-law-research` skill if the user did not give an exact ID.

## Ground rules

- Report only returned text and metadata. A diff shows textual change, not
  legal equivalence, applicability or effect.
- Revisions are listed newest first by effective date, using the publication
  date only when the effective date is absent; undated revisions come last and
  equal dates are ordered by revision ID. This is a presentation order, not a
  statement of which version applied when.
- Precedents have no provider revision history. For them, only `captures`
  history (local observations) exists.

## Workflow

1. **List versions.** Call `database.history` with the `object`,
   `kind: "revisions"` and an optional `limit` (default 20). Continue with
   `cursor`. Use `kind: "captures"` for local observations instead.
   A `snapshot_invalidated` error means restart from the first page.
2. **Pick two versions.** Show the user the candidates with their revision IDs
   and dates. If they asked for "before and after amendment X", match the
   revision by its recorded dates and say which ones you chose. When they
   give two dates ("2020년과 지금"), call `law.in_force_at` with `date`,
   `compare_date` and optionally `article`; its `diff_before` and `diff_after`
   are the selectors for step 3.
3. **Compare.** Call `database.diff` with the `object`, `before` and `after`
   selectors (`{"kind":"revision","id":...}` or `{"kind":"capture","id":...}`).
   `include_ocr` is false by default.
4. **Read the changes.** The result carries both versions' metadata and a
   `comparison` summary with a `comparison_id`. Call `text.diff.page` with that
   `comparison_id`, `view: "changes"` and `page` from 0 to `total_pages - 1`.
   A summary is not the full patch; read every page when completeness matters.
5. **Show (optional).** `text.diff.show` with the `comparison_id` opens the
   comparison widget when the host renders apps.
6. **Clean up.** Call `text.diff.delete` with the `comparison_id` when done.
   Handles expire after ten minutes anyway.

## Watching many laws

When the user wants to know which of several laws changed, call `law.watch`
with up to 100 `laws` (names or objects). Pass the `snapshot` from the user's
last check as `previous`, and give the new `snapshot` back so they can keep it
for next time; you cannot schedule the check yourself. "Today" defaults to the
date in `Asia/Seoul`; pass `timezone` (an IANA name such as `America/New_York`)
to `law.watch` or `law.lineage` when the user asks relative to another zone, and
report the returned `today`. Report `changed` entries
first, then entries whose `repeal_status` is `repealed` or `repeal_scheduled`,
then entries with `upcoming` revisions (promulgated, not yet in force), then
`not_observed` or `ambiguous` names to fix. Without `previous`, every
found entry is `new`: report its HEAD revision and dates instead of a change.

## Renames, repeal and upcoming changes

For one law, call `law.lineage`. `titles` lists title periods (`renamed` is
true when the title changed), `upcoming` lists revisions taking effect after
today, and `head_state` other than `published` (for example `withdrawn`) means
the current text is not readable.

`repeal_status` comes from the provider's amendment type (`제개정구분명`) on
the latest retained revision. `repealed` or `repeal_scheduled` with a `repeal`
record is the provider's own record: state it with the repealing revision, its
amendment type (`폐지`, `타법폐지`, `일괄폐지`) and its effective date.
`no_repeal_recorded` means the latest retained revision is not a repeal; say
that a repeal not yet collected would not show. `unknown` means no amendment
type was recorded. `repeal_mentions` are lines in other laws that mention the
title with `폐지`: quote them as leads, never as the repeal record. If `law.watch` or `law.lineage` is
missing, use `database.history` and `database.get_metadata` instead.

## Large documents

Each composed text is limited to 1 MiB. For larger documents, read the two
versions section by section with `database.get` and compare the matching
sections with `text.diff` (see the `legal-text-comparison` skill).

## Answer shape

Summarize the changed articles first, quote the changed lines exactly, and end
with both versions' citations (revision ID, capture ID and dates).
Use each version's returned capture reference beside the corresponding claim,
following the research skill's
[citation guidance](../korean-law-research/references/citation-and-provenance.md).
Do not use current-capture `search` to recreate a historical citation. Missing
body evidence stays explicit; a metadata-only link does not prove changed text.
