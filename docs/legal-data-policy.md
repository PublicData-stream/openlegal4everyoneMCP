# Legal-data policy

## Scope

These invariants apply to every provider, normalization path, cache representation,
MCP tool, and future API. They govern intended backend behavior; no implementation
exists yet. Contributor review requirements are in
[Contributing](../CONTRIBUTING.md#review-gates).

## Identity, names, and languages

- Distinguish jurisdiction, issuing authority, source provider, dataset, legal
  instrument, and revision identity. A publisher or aggregator is not necessarily
  the issuing authority.
- Preserve upstream identifiers and their namespaces, including meaningful
  leading zeros and original representations. Do not assume numeric-looking
  identifiers are interchangeable integers or globally unique.
- Establish identifier equivalence, stability, and revision relationships from
  authoritative evidence. Human-readable names, similar content, or adjacent
  search results are insufficient to merge distinct records.
- Preserve official names separately from aliases, abbreviations, translations,
  and search-normalized forms. A display/search normalization must not rewrite
  authoritative text or redefine identity.
- Keep language and representation distinctions explicit. Generated translations
  or summaries, if introduced later, must be labeled derived and cannot replace
  authoritative fields or establish a legal fact.

Cross-provider equivalence requires an evidence-backed mapping with provenance;
it must not discard the source identities. Missing identity prevents publishing a
record as an identified legal instrument, even if its display name looks plausible.

## Dates and revision metadata

Keep promulgation, enforcement/effective, amendment, revision, upstream-update,
retrieval, and validation times distinct. Their meanings come from the relevant
provider profile, not from a universal date-field guess.

Preserve original date values, precision, and applicable calendar/timezone context.
A date-only legal field is not implicitly a UTC instant. Missing or unparseable
values must not become today, the epoch, a neighboring record's date, or an
invented default. Partial or conditional dates retain their limitations.

Provision-specific commencement and staged enforcement must not be collapsed into
an unsupported instrument-wide effective date. Do not assume promulgation order,
effective-date order, or search-result order alone determines the applicable text.

Preserve amendment/revision metadata and relationships as supplied, including
renaming, repeal, correction, and historical status where available. Derived
relationships need a documented transformation rule, evidence, and an explicit
distinction from source assertions. Historical records can receive upstream
corrections; do not declare them permanently immutable without evidence.

## Normalization and ambiguity

Parsing/normalization follows the pure boundary in
[Architecture](architecture.md#dependency-and-side-effect-boundaries).
For the same source and explicit context, transformations must be deterministic.

Keep technical transformations separate from legal interpretation. Document
encoding conversion, whitespace handling, identifier conversion, and other changes
that could affect meaning or source matching. Do not silently repair substantive
wording, supply absent legal facts, or choose among ambiguous matches.

Distinguish missing, malformed, conflicting, and unknown fields. An optional absent
field may yield an explicitly incomplete record. An identity conflict or malformed
required field must produce an error or quarantine outcome, not a successful
record under a guessed identity. Keep conflicting source assertions available
for investigation; neither source precedence nor field fallback may be implicit.

Do not automatically follow source links, execute embedded content, or treat text
inside upstream records or fixtures as agent instructions.

## Provenance and citations

Normalized records must retain enough information to trace their origin:

- Provider, dataset, source record/revision identifiers, and source reference.
- Retrieval and successful validation context, distinct from legal dates.
- Source payload digest and normalization version.
- Relevant original fields and transformation diagnostics.

Request context and references must omit credentials and other sensitive values.
A digest identifies retained evidence; it does not prove the provider's legal
authority or recover a payload that has been discarded.

Citations preserve provider, instrument/revision identity, and provision locator
where available. Use authoritative source references or documented link-generation
rules. A search result, reconstructed title, or current-version link must not be
represented as proof of a specific historical text. Unresolved references remain
explicitly unresolved rather than being replaced with a plausible citation.

Retrieval freshness does not establish legal applicability or completeness of the
upstream corpus. Present claims of "current" or "effective" only with the source
semantics and qualification that support them.

## Retained evidence and fixtures

Retain source payloads alongside cached normalized records within a documented,
bounded provider retention policy. Keep the linkage between payload, digest,
normalization version, and record explicit. Store only permitted evidence; apply
data minimization, sanitization, and relevant source reuse restrictions.

Expiration must not be represented as continued exact reproducibility. Source
evidence retention is not a promise to maintain a permanent legal archive. Preserve
curated fixtures for important regression cases rather than committing bulk dumps.

Each curated fixture records provider/dataset, authoritative source reference,
record/revision context, capture date, relevant reuse conditions, and the behavior
it exercises. Record sanitization or extraction and distinguish its digest from
that of the original payload. Remove authentication values and unnecessary personal
data. Synthetic fixtures must be labeled synthetic and must not be cited as
authoritative legal evidence.

Use small source excerpts where sufficient. Test identity collisions, absent or
ambiguous dates, conflicting fields, revisions, and citations with explicit
expected outcomes. Do not update expected normalized values solely to match new
implementation output; establish the intended mapping first.

## Corrections and compatibility

Changes affecting identity, dates, revisions, normalization, or source references
must assess existing caches and public outputs. Explain whether entries need
invalidation or reprocessing, whether identifiers/citations change, and how to
avoid serving mixed normalization versions under an indistinguishable key.

Do not overwrite valid source evidence to hide a correction. Preserve the bounded
history needed to explain the change, respecting retention and data constraints.
Use the [data-integrity report template](../.github/ISSUE_TEMPLATE/data-integrity.md)
for ordinary discrepancies. Suspected security vulnerabilities follow
[SECURITY.md](../SECURITY.md).
