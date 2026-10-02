---
name: korean-law-research
description: Find and quote Korean statutes, ordinances and court precedents from the OpenLegal corpus with exact provenance. Use when the user asks for a law article, 법령 조문, 판례, 사건번호, 자치법규, or wants legal source text found, read or cited.
license: AGPL-3.0-only
metadata:
  version: "0.3.1"
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

For a source search or a request for inline citations, prefer `search` then
`fetch` when both tools are advertised. Pass the same query DSL described below
as `search({"query":"..."})`, then pass a returned `results[].id` verbatim to
`fetch({"id":"..."})` before relying on its text. Keep the returned `id`, `url`
and capture metadata together. Search covers current retained non-OCR text;
it does not select historical versions or establish complete coverage. Read
the additional qualification text as well as the JSON result.

Use the native workflow below for typed filters, pagination, regex, historical
selection, article navigation or freshness controls. A native result's
`references` may identify the exact evidence; use the returned ID and URL
according to [citation-and-provenance.md](references/citation-and-provenance.md).
Do not replace a selected historical capture with a new `search` result.

0. **Resolve names.** When the user names a law, possibly by abbreviation
   (`산안법`, `중처법 시행령`), call `law.resolve_name` to get its object. The
   `resolution` field shows any alias expansion; mention it to the user.
   `law.resolve_name`, `citation.verify` and `law.in_force_at` take an optional
   `jurisdiction` (ISO 3166-1 alpha-3); it defaults to `KOR`, which is what this
   skill needs. `unsupported_jurisdiction` lists the codes the server supports.
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
   For a long statute, administrative rule or ordinance, prefer `law.article`:
   pass `article` (with `context` for neighbours), `chapter` (`제2장`),
   `keyword` or `annex` (`별표 1`), or nothing to get the outline first. Add
   `date` for the version in force on a day. An `annex_text_sparse` warning
   means the annex table may exist only as an image; say so instead of
   guessing its values.
4. **Report freshness.** State `retrieved_at`, `validated_at` and `freshness`
   from `metadata`. If the result is stale, say so; set `fresh_only: true` when the
   user needs current text only.
5. **Show (optional).** Call `database.show` to open the corpus browser when the
   host renders apps and the user wants to browse.

## Checking citations

When the user supplies text with citations, or before you send an answer that
cites articles or case numbers, call `citation.verify` with that text. Report
each citation's `status` as returned. `title_mismatch`, `article_not_found`,
`paragraph_not_found` and `article_deleted` mean the citation does not match the
checked capture; `law_not_observed` and `not_observed` only mean the corpus has
no record. Correct or flag your own citations that do not verify.

## Precedent status and article impact

- Before relying on a precedent, call `precedent.citing` with its case number.
  `overruling_language_found` means a later retained decision says it changes or
  no longer follows the cited view: quote that line and the decision's case
  number and date. `none_found` only means no recognized phrase was found in the
  retained decisions; never present it as confirmation that the precedent is
  still good law.
- When the user asks what an article affects or what cites it, call
  `article.impact`. Report counts per dataset with a few example lines, and say
  that abbreviated references (`법 제5조`) and references under former titles
  are not counted. The `mermaid` field can be rendered as a diagram where the
  host supports Mermaid.

If `law.resolve_name`, `citation.verify`, `law.article`, `precedent.citing` or
`article.impact` is not in the server's tool list, fall back to `database.rg`
(on the `title` section for names, on `body` for a case number or
`「법령명」 제N조`) and `database.get`.

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
source citation beside each supported claim, then any freshness or coverage
caveat. Use the host's citation metadata when available; otherwise use the
returned source URL as a clearly labeled link. Keep compact provenance nearby
when needed to distinguish versions. A separate source list is not proof of
native citation rendering. Keep the user's language (Korean or English) and
keep official names in their original Korean.
