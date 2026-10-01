---
name: legal-revision-history
description: Trace how a Korean statute or ordinance changed over time and compare two versions from the OpenLegal corpus. Use when the user asks about 개정 이력, 연혁, 신구조문 비교, what changed in a law, or the text of a past version.
license: AGPL-3.0-only
metadata:
  version: "0.1.0"
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
   revision by its recorded dates and say which ones you chose.
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

## Large documents

Each composed text is limited to 1 MiB. For larger documents, read the two
versions section by section with `database.get` and compare the matching
sections with `text.diff` (see the `legal-text-comparison` skill).

## Answer shape

Summarize the changed articles first, quote the changed lines exactly, and end
with both versions' citations (revision ID, capture ID and dates).
