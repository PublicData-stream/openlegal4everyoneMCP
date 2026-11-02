# Review and validation records

These records are point-in-time evidence of reviews, validations and acceptance
runs. They are not maintained after recording; each begins with a header naming
its recording commit, its current-behavior document and later changes known when
the header was added. The living documents linked below own current behavior.
[Review gates](../CONTRIBUTING.md#review-gates) govern which review a change requires.
Earlier record bodies keep their original reviewer labels and category wording.

| Date | Record | Subject | Current behavior |
| --- | --- | --- | --- |
| 2026-09-11 | [Server foundation validation](implementation-review.md) | MCP handler, Streamable HTTP and WebTransport foundation | [Server and extension contract](server.md) |
| 2026-09-12 | [Retrieval and MCP Apps validation](retrieval-review.md) | Synthetic retrieval, cache adapters and React browser | [Upstream retrieval](retrieval.md) |
| 2026-09-12 | [AGPL migration validation](licensing-review.md) | `AGPL-3.0-only` relicensing and source offers | [License](../README.md#license), [source offers](server.md#source-offers-and-migration) |
| 2026-09-13 | [Original Git text comparison review](text-diff-review.md) | Git-based supplied-text comparison | [Supplied-text comparison](text-diff.md) |
| 2026-09-13 | [Rust line and character comparison migration](similar-review.md) | `similar` workers and character highlights | [Supplied-text comparison](text-diff.md) |
| 2026-09-13 | [PostgreSQL and BlobStore review](persistence-review.md) | PostgreSQL 18 and immutable blob persistence | [Persistence](persistence.md) |
| 2026-09-15 | [Rustls 0.23.45 maintenance review](rustls-review.md) | RUSTSEC-2026-0285 Rustls upgrade | [Dependency admission](dependencies.md) |
| 2026-09-15 | [Search-query processor review](search-query-review.md) | Bounded search-query syntax processor | [Search-query syntax](search-query.md) |
| 2026-09-16 | [MCP corpus and text endpoint review](mcp-endpoints-review.md) | Legal corpus tools, text attachments and patches | [Legal corpus tools](database.md), [text comparison](text-diff.md) |
| 2026-09-19 | [Korean tokenization review](korean-tokenization-review.md) | MeCab-Ko alongside Lindera in corpus search | [Legal corpus tools](database.md#search) |
| 2026-09-22 | [Kubernetes ingestion integration review](kubernetes-ingestion-review.md) | Opt-in ingestion runtime and deployment overlay | [Optional ingestion integration](deployment-kubernetes.md#optional-ingestion-integration) |
| 2026-09-22 | [Phase 9 serving acceptance](deployment-acceptance.md) | Disposable Kubernetes serving acceptance | [Kubernetes operator runbook](deployment-kubernetes.md) |
| 2026-09-22 | [Alpine cluster qualification status](alpine-cluster-qualification.md) | gVisor AppArmor incompatibility and resource preflight | [Kubernetes operator runbook](deployment-kubernetes.md), [sandbox status](document-sandbox.md#current-status) |
| 2026-09-23 | [Alpine 3.24 migration acceptance](alpine-image-acceptance.md) | Alpine production images | [Kubernetes operator runbook](deployment-kubernetes.md) |
| 2026-10-04 | [Permanent LAW clone validation](../test-support/fixtures/law-go-kr/ACCEPTANCE.md) | LAW clone implementation and bounded provider probe | [LAW OPEN DATA profile](providers/kr-law-go-kr.md) |
| 2026-10-05 | [Collection deferral and scheduler contention validation](../test-support/fixtures/law-go-kr/COLLECTION-RECOVERY.md) | Collection deferral reasons and scheduler retries | [LAW OPEN DATA profile](providers/kr-law-go-kr.md) |
| 2026-10-28 | [MeCab zstd security maintenance](zstd-review.md) | Fixed compression dependencies, local-source notices and dictionary compatibility | [Dependency admission](dependencies.md) |
| 2026-10-29 | [MCP recovery progress](recovery-2026-10-29.md) | Unintended full fence, schema/index recovery, guarded collection isolation and pending operational acceptance | [Kubernetes operator runbook](deployment-kubernetes.md), [provider recovery](persistence.md#offline-provider-recovery) |
| 2026-10-31 | [Collection cancellation-drain review](reviews-collection-drain-2026-10-31.md) | Bounded source and fixture review of started-supervisor drainage; deployment and operating acceptance remain separate | [Persistence](persistence.md), [Kubernetes operator runbook](deployment-kubernetes.md) |
| 2026-11-01 | [Storage health diagnostics review](reviews-storage-health-2026-11-01.md) | Failure-origin logging and bounded fixture review; storage cause and operating acceptance remain unresolved | [Persistence](persistence.md), [Kubernetes operator runbook](deployment-kubernetes.md) |
| 2026-11-01 | [Diagnostic lookup deployment](deployment-diagnostic-2026-11-01.md) | Signed diagnostic release, coordinated snapshot and restore, actual native/public lookup acceptance; collection remains quarantined | [Persistence](persistence.md), [Kubernetes operator runbook](deployment-kubernetes.md) |
| 2026-11-01 | [Corpus Blob SSD lookup cutover](deployment-ssd-lookup-2026-11-01.md) | Preserved HDD originals, verified SSD copy, native/external lookup acceptance and actual two-hour observation; collection remains off | [Persistence](persistence.md), [Kubernetes operator runbook](deployment-kubernetes.md) |
| 2026-11-02 | [Initial epoch producer source review](reviews-initial-epoch-producer-2026-11-02.md) | Reviewed initial certificate producer, actual child-ownership regressions and 875 frozen operator tests; deployment and lifecycle integration remain gated | [Persistence](persistence.md), [operator implementation](../scripts/storage-stability/README.md) |
| 2026-11-02 | [Storage stability preparation review](reviews-storage-stability-preparation-2026-11-02.md) | Source-pinned operator and epoch read-port reviews, temporary-file/process checks and failed bounded copy calibration; operating acceptance remains gated | [Persistence](persistence.md), [operator implementation](../scripts/storage-stability/README.md) |
