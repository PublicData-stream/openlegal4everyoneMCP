# Objects, selectors and dates

An object is `{"jurisdiction":"kr","provider":"law_go_kr","dataset":"<dataset>","id":"<provider id>"}`.
Keep `id` exactly as returned, including leading zeros.

| Selector | Meaning |
| --- | --- |
| `{"kind":"head"}` | Current view. For national statutes, the provider's current effective-date view |
| `{"kind":"revision","id":"..."}` | One provider checkpoint (for national statutes, MST plus effective date) |
| `{"kind":"capture","id":"<64 hex>"}` | One retained observation; use it to page through a document |
| `{"kind":"publication_date","date":"YYYYMMDD"}` | The unique revision with exactly that publication date |
| `{"kind":"effective_date","date":"YYYYMMDD"}` | The unique revision with exactly that effective date |

## What dates do not mean

- A date selector matches a recorded date exactly. It is **not** "the version
  legally applicable on that day". Missing or ambiguous evidence returns an error
  rather than a guess.
- To answer "what did the law say on day X", list revisions with
  `database.history` (`kind: "revisions"`), show the user the candidate
  revisions with their dates, and say that applicability needs legal judgment.
- `database.history` lists revisions newest first by effective date, using the
  publication date only when the effective date is absent; undated revisions
  come last and equal dates are ordered by revision ID. This presentation order
  is not a statement of legal applicability.
- Precedents have no provider revision history; only capture history exists.

## Freshness

HEAD is fresh for one hour after validation and can be served stale, with
disclosure, for up to 24 hours. After that `database.get` returns
`freshness_unavailable`. Historical revisions and captures carry no current TTL.
