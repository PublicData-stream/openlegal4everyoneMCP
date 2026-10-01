# Legal reference tools

When the [legal corpus](database.md) is configured, the server also registers
three read-only tools that build on its search, history and checkpoint reads:

| Tool | Purpose |
| --- | --- |
| `law.resolve_name` | Resolve a Korean law name or abbreviation to retained objects |
| `citation.verify` | Check statute citations and court case numbers in supplied text |
| `law.in_force_at` | Select the retained revision whose effective date is the latest on or before a date |

They read only the managed corpus and never contact the provider. A missing match
means the corpus has no retained observation. It never means that a law, article
or decision does not exist. Every result carries `corpus_complete` or an
inventory flag, and object, revision and capture identities for the evidence used.

Parsing lives in `openlegal_normalization::kr_legal_reference`, corpus lookups and
the selection policy in `openlegal_application::legal_reference`, and the MCP
adapters in `apps/server/src/legal_reference.rs`.

## Name resolution

`law.resolve_name` accepts `name` (up to 512 bytes, one line) and optional
`datasets` (default `["national_statute"]`). The name is normalized first:
surrounding `「」`, quotes and repeated spaces are removed and middle-dot variants
(`·`, `‧`, `•`, `・` and others) become the provider's `ㆍ`. A built-in table then
expands common abbreviations, including `시행령` and `시행규칙` forms: `산안법`
becomes `산업안전보건법` and `중처법 시행령` becomes
`중대재해 처벌 등에 관한 법률 시행령`. The `resolution` field reports the
normalized input, the name looked up and the alias used, so no rewrite is silent.

Lookup is an anchored `database.rg` search of the `title` section that ignores
spacing between characters. Current titles are searched first. Names with no
current match are searched again in retained historical captures; such matches
report `title_status: "former"` and, when HEAD metadata is readable, the current
title.

The abbreviation table was compiled for this repository from official titles.
It covers frequently cited labor, privacy, competition, finance, real-estate,
criminal and procedure laws. It is not exhaustive. Add an entry only when the
official title and the abbreviation's common use are both clear; the unit tests
reject duplicate keys and non-normalized titles.

## Citation checks

`citation.verify` accepts `text` up to 50,000 bytes and checks at most 50 statute
citations and 30 distinct case numbers; `truncated` reports any excess.

Recognized statute forms include `「법령명」 제N조`, an unbracketed name before the
article (`형법 제329조`, `중대재해 처벌 등에 관한 법률 제4조`), abbreviations,
`제N조의M`, a following `제K항` and `제J호`, and a parenthesized article title
such as `제750조(불법행위의 내용)`. `같은 법`, `동법` and `같은 법 시행령` inherit
the previous citation's law; a bare `제N조` joined to the previous citation by
`및`, `,`, `또는`, `부터` or `까지` does too. A blank line ends inheritance. For an
unbracketed name, the longest run of up to eight preceding words that matches a
retained title is used. Bare articles with no law, such as `제3조에 따라`, are not
reported.

Each statute citation receives one status:

| Status | Meaning |
| --- | --- |
| `verified` | The article exists, cited paragraphs and subparagraphs exist, and any cited title is consistent |
| `article_not_found` | No such article in the checked capture; `article_range` gives the first and last article |
| `article_deleted` | The article text begins with `삭제` |
| `paragraph_not_found` / `subparagraph_not_found` | The article exists without the cited paragraph or subparagraph |
| `title_mismatch` | The article exists, but the cited title's similarity to the retained title is below 50 |
| `law_not_observed` | No retained object has the name; `law_name_candidates` lists the names tried |
| `law_ambiguous` | More than one retained object has the name |
| `law_name_unresolved` | The citation refers to an unnamed law, such as `이 법 제2조` |
| `unavailable` | The object could not be read; `detail` gives the corpus error code |

Current titles are checked against HEAD; a former title is checked against the
capture that carried it. Articles are located in `article:` provider sections by
their leading `제N조` or `제N조의M`; supplementary-provision (`부칙`) sections are
not used. Paragraphs are the circled numbers ① to ㊿ at line starts; an article
without them accepts only `제1항`. Subparagraphs are `N.` lines inside the cited
paragraph. Title similarity is 100 when one normalized title contains the other,
otherwise the character-bigram Jaccard index.

Case numbers are recognized as a two-digit or `19xx`/`20xx` year, a known court
case-type code (for example `다`, `도`, `두`, `누`, `헌마`, `헌바`) and a serial
number, which excludes dates such as `2020년3월`. They are matched against the
`case_number` section of precedents and constitutional decisions, including
records listing several numbers. The result is `observed` with the matching
records, or `not_observed`.

## Date-based selection

`law.in_force_at` accepts either `object` or a national-statute `law_name`, a
`date` (`YYYYMMDD`), an optional `article` (`제44조`, `44의2`, `44-2`) and an optional
`compare_date`. Only datasets with provider revisions are supported.

The tool reads up to 2,000 revisions from `database.history`. It selects the
revision with the latest effective date on or before the date; among revisions
with that effective date, the latest publication date (then revision ID) is
selected and the others are listed in `same_effective_date`. National-statute
revisions are provider effective-date views (`MST:efYd`), so this selects the
view that took effect most recently by that date.

| Status | Meaning |
| --- | --- |
| `determined` | The retained inventory is complete and a revision was selected |
| `provisional` | A revision was selected, but the inventory is incomplete |
| `not_yet_effective` | Every dated retained revision takes effect after the date |
| `undetermined` | No retained revision carries an effective date |

`next_change` gives the earliest revision effective after the date. When the
selected revision's `provision_effective_dates` metadata contains dates after the
date, they are listed in `later_provision_dates` with a warning. With `article`,
the result includes that article at the selected revision and at HEAD and whether
the text changed. With `compare_date`, it includes a second selection and
`diff_before`/`diff_after` selectors for `database.diff`.

This is a selection over retained provider views, not a ruling on which law
applies to particular facts. Supplementary provisions, transitional rules,
retroactivity and provision-level effective dates can change the applicable text;
the `basis` field says so in every result. Exact date selectors in
`database.get` keep their existing meaning.

## Validation

Pure parsing and selection are covered by unit tests in the normalization and
application crates. The server tests run all three tools against an in-memory
corpus and search backend with fictional records. These tools have not yet been
exercised against a provider-collected corpus or the PostgreSQL integration
fixture.

## Provenance

The feature set was chosen after reviewing korean-law-mcp
(<https://github.com/chrisryugj/korean-law-mcp>, MIT License, Copyright (c) 2025
Chris). The Rust implementation, the abbreviation table and the tests were
written for this repository; no code or data was copied from that project.
