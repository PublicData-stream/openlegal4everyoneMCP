# Legal corpus tools

The optional `[database]` configuration registers `database.query`, `database.rg`,
`database.get`, `database.get_metadata`, `database.history`, `database.diff`, and
`database.show` on both existing MCP transports. It requires PostgreSQL persistence
and the text comparison service. The synthetic demo remains a separate dataset.

The implementation has offline fixtures and local integration gates. An enabled
provider adapter or a successful build does not establish a complete national
corpus, successful authenticated upstream integration, or production deployment.
The document sandbox has a separate operator acceptance gate.

## Identity, revisions and evidence

An object is `{jurisdiction, provider, dataset, id}`. Initial datasets are
`national_statute`, `ordinance`, and `precedent`, with `law_go_kr` as provider.
A revision is a provider checkpoint; a capture is a particular retained observation.
They are separate identifiers. National effective-view revisions include both MST
and efYd. Identical observations can refresh validation timing without creating an
artificial legal amendment. Changed source evidence or processor output produces
another immutable capture, including reversions.

Selectors use a tagged object:

```json
{"kind":"head"}
{"kind":"revision","id":"provider-checkpoint"}
{"kind":"capture","id":"64-lowercase-hex-capture-id"}
{"kind":"publication_date","date":"20260916"}
{"kind":"effective_date","date":"20260916"}
```

Dates mean exact typed dates. They do not mean “the legally applicable text as of
this day.” A date requires an adequate inventory and a unique matching revision;
missing or ambiguous evidence returns a distinct error. National HEAD is the
provider's current effective-date view, ordinance HEAD its current view, and
precedent HEAD its current record. Precedent provider revision history is explicitly
unsupported; capture observations remain available.

The catalog can outlive body retention. An unavailable checkpoint never silently
becomes HEAD. Raw primary evidence and referenced attachment evidence remain linked
by SHA-256; failures to read or verify retained evidence fail closed. SHA-256 links
bytes and representations; it does not authenticate legal authority.

## Content and metadata

`database.get` accepts object, selector, `fresh_only`, optional section, offset,
section-catalog offset, and content-session bearer. Its result contains a bounded
text page, metadata, section catalog page, and session. Continue with the returned
capture ID, session, same section, and `next_offset`. Section catalogs also have a
continuation offset. Sessions last ten minutes and do not extend on access. They
pin exact content against ordinary retention; withdrawal invalidates sessions.
`database.get_metadata` returns provenance and timing without body text.

HEAD includes retrieval time, last validation time, publication-transaction time (returned only after durable commit),
served time, age, freshness state, expiry times and remaining TTLs. Freshness lasts
one hour; stale content can be served only within 24 hours of validation.
`fresh_only` rejects stale data. A newly observed replacement awaiting processing
cannot make the previous HEAD fresh. Historical results do not carry current TTL.

Provider text, extracted attachment text, and OCR are distinct sections. OCR has
source digest/page provenance and is excluded from search/comparison by default.
The page excerpts do not alter stored evidence.

`database.diff` resolves both selectors for one object and returns the exact capture
metadata with a text-comparison handle. Its line and character comparison is the
same algorithm documented in [text comparison](text-diff.md). Each composed text is
limited to 1 MiB. Large documents can be read section by section and compared with
`text.diff`; comparison does not establish legal equivalence or applicability.

## Search

Both search methods operate on the managed corpus, never arbitrary paths or caller
SQL. Filters include dataset, authority, object ID, document type and typed date
bounds. Default page size is 20; maximum is 100. Results use stable object/revision/
capture/section ordering and retain one index reader for ten minutes.

`database.query` uses the existing [query DSL](search-query.md): words, prefixes,
Boolean expressions, grouping, and title/body fields. Lindera and the Rust MeCab-Ko
engine analyze NFC/ASCII-lowercase search surfaces independently, without stopword,
stemming, or POS filters. Original legal text is preserved. Double-quoted strings
remain exact source substrings, including under NOT.

Terms, prefixes, AND and OR are evaluated separately for each engine. At every NOT
node, the union of its child's two engine results is negated and shared by both
engines; the final root results are unioned. Thus `A AND B` requires one engine to
satisfy both operands, while `A AND NOT B` is excluded if either engine matches B.
This rule also applies recursively to nested NOT and field scopes. Query analysis
is compiled once per search session. Explicitly selected sections, including OCR,
use the same analyzers as indexed text. Analyzer failure is an explicit error;
there is no silent single-engine fallback.

Boolean results have whole-object scope; an excerpt is not a claimed matching span.
Excerpt section and OCR inclusion are reported separately. Combining results is not
a claim of improved legal-search accuracy; evaluate recall and false positives on
appropriate evidence before making that claim.

`database.rg` uses ripgrep's Rust regex engine. Matching is line oriented and case
sensitive unless explicitly changed with typed `literal`, `ignore_case`, or
`context_lines` options. There are no CLI flags, PCRE2 execution, or filesystem
inputs. Match offsets are UTF-8 byte offsets in the identified original section.

Managed publication preflights indexability: at most 262144 analyzed tokens combined across both
engines per text field and a 48 MiB serialized index envelope, with 8 KiB reserved for capture
metadata. Each analyzed line is limited to 64 KiB and 4096 Unicode scalars after
normalization; a whitespace-delimited run is limited to 128 scalars. MeCab-Ko's
unknown-word lattice expands superlinearly, so these additional limits reject
pathological inputs before analysis rather than splitting text and changing its
segmentation. They apply to queries and indexed or selected text alike. A source exceeding these bounds remains unprocessed; it cannot publish a
new HEAD and then block the ordered index stream.

Each call has a ten-second deadline and 64 MiB scan budget. A page can have zero
hits and a continuation when a scan budget is exhausted. A continuation does not
claim that the whole corpus was searched. Responses expose generation, index lag,
analyzer version, and observed current-primary-text corpus coverage (historical,
OCR, and selected attachment searches conservatively report incomplete coverage); retained-reader expiry and withdrawal have
explicit errors. PostgreSQL is authoritative. The index consumes an ordered durable
outbox, commits before acknowledging events, and preserves old readers until their
sessions expire. Referenced missing/corrupt evidence is a storage failure.

## Operator configuration

Build the widgets first. Add to a configuration that already supplies persistent
cache storage and the text-diff large-message profile:

```toml
[database]
blob_path = "/var/lib/openlegal/corpus-blobs"
index_path = "/var/lib/openlegal/corpus-index"
widget_html = "apps/widget/dist/database.html"
mecab_dictionary_path = "/var/lib/openlegal/mecab-ko-dictionary"
```

Provision the pinned standard dictionary before startup:

```sh
scripts/prepare-korean-dictionary.sh /var/lib/openlegal/mecab-ko-dictionary
```

The destination must not already exist. Set `MECAB_SOURCE_ARCHIVE` to reuse a
local source archive; its checksum is still verified. The helper builds from the
exact archive selected by Lindera, verifies SHA-256,
and retains upstream notices. Its manifest records source and output digests,
builder version and validation counts. The runtime validates the manifest and the
uncompressed `sys.dic`, `matrix.bin`, and release-compatible `entries.bin` artifacts;
missing, miniature, empty, corrupt or incompatible dictionaries fail startup.
The server does not download dictionaries or discover a default dictionary.

The mutable MeCab-Ko engine uses four bounded tokenizer slots, each with an eagerly
loaded dictionary copy. Budget memory for all four copies in addition to Lindera,
index readers and server work. Excess admission fails explicitly; no unbounded
queue is created. Cancellation retains its slot until blocking analysis actually
finishes. Dictionary replacement requires a restart and compatible index rebuild.

Use distinct, non-nested directories for synthetic cache blobs, corpus blobs and
the index. Do not share a corpus index between concurrent server processes. Run
`openlegal-server --migrate CONFIG.toml` with the migration credential before serving.
The runtime credential does not create schema objects.

Serving retained data requires neither provider credentials nor Kubernetes.
Coverage is reported complete only after all three current datasets have matching
consecutive full inventory traversals, their desired HEADs are available within the
24-hour policy, and the selected index generation has caught up. This is observed
provider coverage, not an upstream atomic-snapshot guarantee. Restart clears this
claim until inventories are verified again.

Background ingestion requires explicit `[database.ingestion]` configuration with
`enabled = true`, a credential environment variable name, absolute kubectl and
kubeconfig paths, an explicit context/namespace, and a worker image pinned by digest.
A claimed ingestion attempt allows at most 500 seconds for provider/detail processing,
10 seconds for index admission, and 40 seconds for publication, within its 600-second
lease. Deterministic source/access/format failures are terminal for that attempt;
transient failures have at most three attempts and honor provider Retry-After.
No live provider calls occur in ordinary tests. Refer to the
[Korean provider profile](providers/kr-law-go-kr.md) and
[document sandbox](document-sandbox.md) for evidence and deployment gates.
The [Kubernetes ingestion overlay](deployment-kubernetes.md#optional-ingestion-integration)
supplies the optional image, projected identity and network templates. Its enabled
startup initiates background traffic; applying it requires separate operator
authorization after sandbox acceptance. Retained serving remains independent.

Current HEAD data is retained independently of ordinary historical retention.
Historical bodies are retained for 30 days, with bounded session extensions;
revision catalog metadata survives body eviction. Current corpus raw evidence has a
1 TiB ledger cap and staging a 16 GiB cap. Reserve additional space for PostgreSQL,
index generations/rebuilds, history and filesystem overhead within the deployment's
1.5 TiB managed-storage ceiling. These software bounds are not ZFS configuration.

## Offline index rebuild

For Kubernetes, follow the [administrative Job sequence](deployment-kubernetes.md#administrative-jobs)
and [rollback procedure](deployment-kubernetes.md#rollback-and-recovery) alongside
this index compatibility contract.

Indexes persist format version, analyzer identity, generation and completion state.
Analyzer identity includes both engine versions, dictionary digests, normalization
and matching policy. Legacy, mismatched, malformed or incomplete indexes cannot be
served. Responses report the identity from the validated retained index snapshot.

To upgrade an existing index:

1. Stop the sole backend, including its ingestion and retention maintenance.
2. Keep the old index directory. Configure a fresh `database.index_path` and the
   new provisioned dictionary in an operator configuration using retained storage.
3. Run `openlegal-server --rebuild-corpus-index CONFIG.toml`.
4. Start the backend only after the rebuild succeeds.

The rebuild requires no provider credential, document worker or listeners. It
replays the durable outbox in bounded batches through a captured watermark,
including capture removals. Missing captures are skipped only when durable retirement
evidence permits it; otherwise missing or corrupt evidence fails the rebuild.
PostgreSQL acknowledgment is never rewound and is advanced only after the complete
target generation is durable and the watermark remains unchanged. Interrupted replay
leaves an incomplete, unservable destination; start again with a fresh one. If the
final acknowledgment fails after index completion, resolve the failure before
serving; rebuild again into a fresh directory to retry the complete operation.
Rebuilding does not rewrite retained legal evidence.

Restart expires search cursors. Rollback requires the previous binary, its dictionary
and a compatible complete index generation. If acknowledgment advanced beyond the
old index, simply pointing at that directory is insufficient. This command writes
only the new dual-engine format, not the legacy Lindera-only format. Such a downgrade
requires version-specific rebuilding tooling; otherwise repair forward with the
new analyzer. Never rewind acknowledgment or discard corpus evidence to downgrade.

## Validation

The full standard-dictionary gate is `scripts/test-korean-tokenization.sh`. It
provisions its own artifact unless `OPENLEGAL_TEST_MECAB_DICTIONARY` is supplied.
It exercises both engines and reports reproducible synthetic-input measurements;
these are not legal accuracy or production capacity claims. The PostgreSQL gate
also provisions a dictionary unless that variable names an existing artifact.
Ordinary unit tests do not require the external MeCab-Ko artifact.
The [implementation review](korean-tokenization-review.md) records independent
review, local validation, dictionary identity and measured resource use.

Required baseline and PostgreSQL/OxiBelt gates are in [CONTRIBUTING](../CONTRIBUTING.md).
Fixtures label fictional legal records explicitly. Tests cover exact/ambiguous date
selection, capture lineage, publication fencing, raw evidence verification, retained
reader/session behavior, withdrawal, patch round trips and bounded MCP results.
Live upstream coverage and hardened-cluster acceptance are separate opt-in checks.
The [endpoint review](mcp-endpoints-review.md) records compatibility, independent
review scope, local verification and remaining deployment gates.
