# Legal analysis tools

When the [legal corpus](database.md) is configured, the server registers five
read-only analysis tools next to the [legal reference tools](legal-reference.md):

| Tool | Purpose |
| --- | --- |
| `law.watch` | Check up to 100 laws for HEAD changes and upcoming revisions in one call |
| `law.lineage` | Trace title changes, upcoming revisions, HEAD state and repeal mentions of one law |
| `precedent.citing` | List retained decisions that cite a case number and flag overruling language |
| `article.impact` | Map retained texts that cite one article and the citations inside it |
| `law.article` | Read single articles, a chapter, keyword matches or an annex from a long text |

Like the reference tools, they read only the managed corpus and never contact the
provider. A missing match means the corpus has no retained observation, never
that a law, article or decision does not exist. Results carry `corpus_complete`
or `inventory_complete` and the object, revision and capture identities of the
evidence.

Parsing lives in `openlegal_normalization::kr_legal_reference`, result contracts in
`openlegal_domain::legal_analysis`, and the MCP adapters in
`apps/server/src/legal_analysis.rs`.

## Names and dates

Tools that accept `law_name` look it up like `law.resolve_name`, across national
statutes, administrative rules and ordinances. When several objects carry the
title, a single national statute wins; otherwise the call fails as ambiguous and
the caller should pass `object`. "Today" is the calendar date in Korea (UTC+9)
when the call runs; a revision is upcoming when its effective date is later.

## Amendment types and repeal

Collection records the provider's `제개정구분명` from each statute,
administrative-rule and ordinance list row as the capture metadata
`amendment_type`, exactly as written (for example `제정`, `일부개정`, `타법개정`,
`폐지`, `타법폐지`). The official effective-date list guide documents this field
and the amendment classes `폐지`, `폐지제정`, `일괄폐지` and `타법폐지`; the
administrative-rule and ordinance rows are read when they carry the same field.

`law.watch` and `law.lineage` read the amendment type of the latest retained
revision, the one with the latest effective date:

| `repeal_status` | Meaning |
| --- | --- |
| `repealed` | The latest revision is `폐지`, `타법폐지` or `일괄폐지` and its effective date is today or earlier |
| `repeal_scheduled` | The latest revision is such a repeal and takes effect after today |
| `no_repeal_recorded` | The latest revision records another amendment type |
| `unknown` | The latest revision records no amendment type or could not be read |

`repeal` gives the repealing revision, its recorded amendment type, its kind and
dates. `폐지제정` (repealed and re-enacted) is not reported as a repeal. A repeal
that the corpus has not collected yet is not reflected, so `no_repeal_recorded`
is evidence about retained revisions only. Captures made before this field was
collected have no amendment type and report `unknown` until the revision is
revalidated or collected again; whether revalidation records the field for an
unchanged provider body has not been verified against a live corpus.

## Batch watching

`law.watch` accepts `laws` (1 to 100 items, each a `law_name` or an `object`),
`previous`, `include_upcoming` (default true) and `changes_only`. `previous` is
the `snapshot` returned by an earlier call: a map from `dataset:id` (for example
`national_statute:1234`) to the HEAD revision ID then observed.

Each entry reports one status:

| Status | Meaning |
| --- | --- |
| `changed` | HEAD differs from the revision in `previous` |
| `unchanged` | HEAD equals the revision in `previous` |
| `new` | `previous` has no revision for the object |
| `not_observed` | No retained object has the name, or the object has no retained observation |
| `ambiguous` | Several retained objects have the name; `detail` gives the count |
| `unavailable` | HEAD could not be read; `detail` gives the corpus error code |

Entries also carry the HEAD title, dates and amendment type and, for datasets
with provider revisions, the retained revisions whose effective date is after
today (with the amendment types of the first five) and the repeal status above.
The catalog is read even when HEAD is unavailable, because a repealed law can
leave the provider's current list. `repealed` counts entries whose status is
`repealed` or `repeal_scheduled`. The new
`snapshot` keeps the previous revision for objects whose HEAD could not be read,
so a failed read does not reset the watch. `changes_only` omits unchanged entries
without upcoming revisions or a repeal; the snapshot still covers every object.

Scheduling and notification belong to the client. A client can store the
snapshot and call the tool on its own schedule.

## Lineage

`law.lineage` accepts `object` or `law_name` for a dataset with provider
revisions. It reports:

- `titles`: runs of consecutive retained revisions (oldest first) that carry the
  same title, compared without spacing. `renamed` is true when there is more than
  one run. `revisions_without_title` counts catalog revisions with no retained
  capture.
- `upcoming`: retained revisions whose effective date is after today, with the
  amendment types of the first five.
- `repeal_status`, `repeal` and `latest_amendment_type`, as described above.
- `head_state`: `published`, or the corpus error code for HEAD, such as
  `withdrawn`.
- `repeal_mentions`: up to 20 lines in current national statutes and
  administrative rules that contain the current title or a former title together
  with `폐지`, with the title not preceded by another Hangul syllable.

Repeal mentions are leads to read, such as a supplementary provision stating
that an older act is repealed. Unlike `repeal_status`, they are not a provider
record of this object's repeal. A title that is a prefix of a longer title
(`민법` in `민법 시행령`) can also produce a mention.

## Citing decisions

`precedent.citing` accepts one `case_number` in the form recognized by
`citation.verify`, such as `2007다27670`. Targets are the retained precedents and
Constitutional Court decisions whose `case_number` section contains the number.
Citing decisions are other such records whose text has a line containing the
number with no adjacent digits; at most 50 decisions are returned, with up to
three lines of at most 400 characters each. Metadata adds the decision's own case
number, judgment date and court, and decisions are ordered newest judgment first.

A citing line that contains one of the phrases `변경하기로 한다`,
`변경하기로 하며`, `모두 변경`, `견해를 변경`, `더 이상 유지할 수 없` or
`폐기하기로`, ignoring spaces, is reported first with `overruling_phrase`, and the
result signal becomes `overruling_language_found`. `en_banc` is true when the
title or that line mentions `전원합의체`.

`none_found` does not establish that a decision is still good law. A decision can
be overruled or limited without quoting the number, in wording outside the
list, or by a decision the corpus has not collected. The `basis` field says so.

## Article impact

`article.impact` accepts `object` or `law_name` and an `article` (`제750조`,
`44의2`, `제9-5조`). It reads HEAD, locates the article and fails with
`not_found` when HEAD has no such article.

Inbound references are lines in other retained texts that contain the current
title (optionally in `「」`) followed by the article locator. A locator for
`제5조` does not match `제50조` or `제5조의2`, and a title preceded by another
Hangul syllable is not matched. Precedents, Constitutional Court decisions, legal
interpretations, national statutes, administrative rules, ordinances and
administrative appeals are searched. Each dataset reports the number of distinct
objects and up to 20 example lines. Abbreviated references, such as `법 제5조` in
a decree, and references under former titles are not counted.

Outbound references come from the article's own text. Named laws are resolved
like `citation.verify`; bare locators such as `제3조` and `이 법 제3조` are
reported without a law name, meaning the same law. At most 30 are returned.
`mermaid` holds a `graph LR` source that links dataset counts to the article and
the article to its outbound references.

## Article reads

`law.article` accepts `object` or `law_name`, and either a `selector` or a
`date` (`YYYYMMDD`, selected like `law.in_force_at`; a date before every retained
revision fails with `not_found`). Without either it reads HEAD. It then returns
any combination of:

| Input | Returns |
| --- | --- |
| `article`, `context` (0 to 3) | The article and up to three neighbours on each side |
| `chapter` (`제2장`, `3절`) | Every article under the first matching heading |
| `keyword` | Every article containing the text, ignoring spacing, and their labels in `keyword_matches` |
| `annex` (`별표 1`, `1의2`) | Annex texts with that exact label |

With none of these it returns `outline`, the headings (`편`, `장`, `절`, `관`) and
articles in order, up to 600 entries. A missing article or heading also returns
the outline. Every article carries its enclosing headings in `path`.
`annex_index` always lists the annex labels found.

National statutes keep one article per `article:` section. Administrative rules
and ordinances often keep many articles in one provider block (`조문내용`,
`조내용`, `전문`); such blocks are split at lines that start with an article
locator followed by a title, `삭제` or nothing. Locators of the form `제9-5조`,
common in administrative rules, are supported; when a capture has no `제9-5조`, a
request for it falls back to `제9조의5`. Supplementary provisions are not part of
the outline.

Annexes come from provider `별표내용` sections and extracted attachment text,
labelled by the first `별표 N` or `별표 N의M` marker. `별표 1` never matches
`별표 1의2`. An annex with fewer than 20 Hangul syllables or an image tag is
marked `sparse` and adds `annex_text_sparse`: the table may exist only as an
image, which the document sandbox may OCR separately.

Text is bounded to 16 KiB per article, 32 KiB per annex and 64 KiB per call;
`truncated` reports any cut. Warnings are `article_not_found`,
`heading_not_found`, `keyword_not_found`, `annex_not_found`,
`annex_text_sparse` and `no_article_structure`.

## Validation

Parsing helpers are covered by unit tests in the normalization crate. The server
tests run all five tools against an in-memory corpus of fictional records whose
search backend evaluates the generated patterns with the same ripgrep regex
engine as the corpus adapter. These tools have not yet been exercised against a
provider-collected corpus or the PostgreSQL integration fixture.

## Provenance

The feature set follows a review of korean-law-mcp
(<https://github.com/chrisryugj/korean-law-mcp>, MIT License, Copyright (c) 2025
Chris). The Rust implementation, phrase lists and tests were written for this
repository; no code or data was copied from that project.
