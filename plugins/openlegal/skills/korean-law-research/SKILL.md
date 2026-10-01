---
name: korean-law-research
description: Find and quote Korean statutes, ordinances and court precedents from the OpenLegal corpus with exact provenance. Use when the user asks for a law article, 법령 조문, 판례, 사건번호, 자치법규, or wants legal source text found, read or cited.
license: AGPL-3.0-only
metadata:
  version: "0.1.0"
---

# Korean law research

Retrieve authoritative Korean legal text through the OpenLegal MCP server and
quote it with provenance. Clients may prefix tool names with the connector name;
the server names are used below.

## Ground rules

- Report only text and metadata returned by the tools. Never invent an article,
  date, case number or holding, and label any summary or translation as derived.
- This is legal information, not legal advice. Do not predict outcomes or tell
  the user what they should do in their own matter.
- Cite every quoted passage with the fields in
  [citation-and-provenance.md](references/citation-and-provenance.md).
- A selector date means an exact recorded date, not "the law in force on that
  day". Read [selectors-and-dates.md](references/selectors-and-dates.md) before
  answering a point-in-time question.

## Workflow

1. **Search.** Call `database.query` with the query DSL in
   [query-syntax.md](references/query-syntax.md). Narrow with `filters.datasets`
   (`national_statute`, `ordinance`, `precedent`, …) and typed date bounds. Use
   `literal: true` or `database.rg` when the user gives an exact phrase or a
   pattern such as `제750조`.
2. **Check coverage.** Read `corpus_complete`, `index_lag` and any
   `collection_notices` in the result. A page with zero `hits` and a
   `next_cursor` is not "no results"; pass it back as `cursor` before
   concluding.
3. **Read.** Call `database.get` with the hit's `object` and `selector`
   `{"kind":"head"}`, or `{"kind":"capture","id":<hit capture_id>}` for exactly
   the matched capture. To continue a long document, pass
   `{"kind":"capture","id":<metadata.capture_id>}`, the returned `session`, the
   same `section` and `next_offset`. Use `sections` to jump to an article.
4. **Report freshness.** State `retrieved_at`, `validated_at` and `freshness`
   from `metadata`. If the result is stale, say so; set `fresh_only: true` when the
   user needs current text only.
5. **Show (optional).** Call `database.show` to open the corpus browser when the
   host renders apps and the user wants to browse.

## When the corpus does not have it

- `not_observed` means this corpus has not seen the object; it does not mean the
  law does not exist. `processing_pending` and `collection_incomplete` mean the
  object is not yet readable.
- Call `database.object_status` for one object's state.
- To ask for collection, call `database.request_collection` with one target:
  `{"kind":"object","object":{...}}` for a national statute ID,
  `{"kind":"precedent_case","case_number":"..."}` for a precedent, or
  `{"kind":"search","mode":"query","term":"...","datasets":[...]}`.
  This queues background work and does not return legal text. Tell the user
  it was requested, then check `database.collection_status` with the returned
  `request_id` and search again once it is `done`.
- Equivalent requests coalesce for 24 hours. Do not resubmit in a loop.

## Answer shape

Lead with the quoted text or the list of matching instruments, then the
citation block, then any freshness or coverage caveat. Keep the user's language
(Korean or English) and keep official names in their original Korean.
