# Retained-source citations

Enable citation support alongside the retained database before serving:

```toml
[citations]
base_url = "https://openlegal4everyone.reference.publicdata.stream"
```

The base is an explicit public HTTPS origin without credentials, path, query or
fragment. Provision DNS and a matching certificate before advertising it. The
existing HTTP listener serves `/source`; OxiBelt routes GET/HEAD on the reference
host to the same authenticated backend. Its separate reference bucket admits
100 requests/second with burst 100. Operational paths remain private.

## ChatGPT compatibility

`search({"query":"..."})` uses the existing corpus query language, current
captures and non-OCR text. It returns at most 20 items in
`{"results":[{"id":"...","title":"...","url":"https://..."}]}`.
`fetch({"id":"..."})` returns `id`, `title`, complete item `text`, `url`, and
compact provenance `metadata`. Identical JSON is returned in `structuredContent`
and the first JSON text block. Additional text qualifies partial search,
incomplete corpus, index lag and collection gaps. A bounded empty result does not
establish legal nonexistence. Use `database.query` for rich controls and pagination.

An item is a complete short document, an explicit source article/section, or a
fixed UTF-8-safe passage of at most 8,192 bytes. Long articles have separately
identified passages. An insufficient result budget returns `resource_limit`;
it does not shorten the same item or change its identity. JSON escaping, provenance,
references and duplicated compatibility text count toward output limits.

These shapes follow [OpenAI MCP compatibility](https://developers.openai.com/api/docs/mcp)
and its [user-openable URL guidance](https://developers.openai.com/plugins/build/mcp-server#company-knowledge-compatibility).
Protocol tests do not establish actual ChatGPT citation rendering.

For an inline citation request, use `search` followed by `fetch` on a returned ID
before citing its text. Native `database.*` results remain useful for richer
queries, historical selection and provenance; their `references` and resource
links alone do not establish ChatGPT citation metadata. The bundled research
skill describes this choice. Never replace selected historical evidence with
current search results, invent citation tokens or treat a Markdown source link
as a successful native citation UI test.

## MCP references and evidence

Canonical `v1` IDs preserve jurisdiction, provider, dataset, provider object ID,
capture and projection; section locators are percent escaped. MCP URIs use
`openlegal://source/<id>` and public URLs use `/source/<id>`. No caller URL causes
network or filesystem retrieval. HEAD and transient sessions are not citation IDs.

Native document/search/history/diff and legal-analysis results add `references`
and standard `resource_link` content. They use established result capture identities
without additional HEAD reads. Metadata-only references are qualified; revisions
without a capture do not get invented references. Links are deduplicated and
bounded to 20; warnings report missing or omitted references.

`resources/templates/list` exposes a source template without corpus enumeration.
`resources/read` returns an exact body unit or metadata descriptor. Dynamic reads
share call admission, rate, cancellation and deadline policies. Modern responses
use public cache scope and zero TTL. Static widget resources remain separate.

Historical body retention remains 30 days with existing HEAD/session protection
and a renewable 600-second citation lease. Search establishes leases before
releasing generation protection. One durable row covers all projections of a
capture; repeated clicks do not accumulate read sessions. At most 2,048 captures
may have live citation leases; existing storage byte limits still apply. Frequent
access can extend availability beyond 30 days. This is not a permanent archive.

Confirmed official links identify provider records, whose pages may change after
capture. Unsupported mappings, including ordinances without sufficient routing
evidence, use a qualified official lookup. The credential-free API `source_url`
remains provenance rather than proof of a browser-accessible historical copy.

## Browser pages

Pages default to English; `?lang=ko` selects Korean labels. Source text is not
translated. Metadata/document overviews show the first retained body unit and
navigation. HTML is escaped, scripts are disabled and source links are click-only.
Provider text, extracted text and OCR carry distinct provenance.

Expired pages return 410 with retained document metadata and unavailable text.
Without a body, a requested section/range cannot be verified. Unknown references
return 404; malformed IDs return 400. Withdrawal overrides leases. `fetch` and
body resource reads fail instead of substituting a new capture. Metadata resource
reads do not acquire a body lease.

## Deployment and platform acceptance

Run the Rust baseline, PostgreSQL, Korean tokenization, both OxiBelt profiles,
Kubernetes serving, both platforms of runtime/runtime-ingestion image gates and
applicable worker/smoke checks. Fixture results, emulated ARM64, hosted native CI,
public endpoint checks and ChatGPT UI observations are separate evidence.
Independently review citation identity, retention and public trust boundaries.

Run the registered citation-lease migration before the new server; grant the
restricted runtime role rights on the new table through the existing migration
arrangement. Snapshot before activation. Update serving, scheduler, collection
Job template and worker references together while preserving collection state.
Verify release digests, source identity, reference routing and TLS.

After public smoke succeeds, connect the public `/mcp` endpoint in ChatGPT
developer mode and check:

1. Refresh the connection's advertised tools and use the updated research skill
   in a new conversation. Explicitly call `search`, fetch one returned item and
   ask for a source citation beside the supported claim. Confirm citation
   rendering and that its click opens the same capture and text unit.
2. Read a long article/passage. Confirm explicit passage labels and navigation
   within the same capture.
3. Follow a native `law.article` or history reference. Confirm exact provenance
   and body access rather than selection of another HEAD.
4. Query unobserved material. Confirm incomplete coverage is qualified.
5. Check both page languages and an expired fixture. Confirm explicit unavailable
   evidence without text replacement.

Record deployment revision/digests and the user's citation/click observations
separately. ChatGPT UI acceptance remains pending until those observations arrive.
