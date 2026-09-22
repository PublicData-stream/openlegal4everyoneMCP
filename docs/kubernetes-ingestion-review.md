# Kubernetes ingestion integration review

## Result and boundary

Phase 7 builds on `fab54a6e3241c04c68def42606fde2617cb0df5f` on 2026-09-22.
It adds an explicitly selected ingestion runtime and deployment overlay, rotating
file-backed controller credentials, unchanged namespaced RBAC, separate API/provider
egress examples and an isolated kubectl subprocess environment. The default image,
retained deployment and administrative Jobs remain independent of ingestion.
The [deployment guide](deployment-kubernetes.md#optional-ingestion-integration)
owns activation, rollback and external acceptance requirements.

No image publication, Git push, cluster deployment or legal-provider request was
performed. Local ARM64 execution uses emulation; native ARM64 hosted acceptance
is a separate CI result and was not watched or claimed here.

## Independent Security Review

Reviewer `security_review` was a separate agent, independent of implementation,
following the [delegation skill](../.agents/skills/subagent-delegation/SKILL.md).
It inspected the actual 31-file Phase 7 patch and surrounding contracts, including
image/default-target behavior, binary admission and notices, subprocess environment
and cleanup, projected identity/TLS, cross-namespace binding, restrictive egress,
negative tests, CI and activation/rollback instructions. It found no blocking
security or correctness issues. This is focused agent review, not a human GitHub
approval or a repository-wide security scan.

The reviewed snapshot fingerprint is
`9ba12d83a8bdcbe30c5bbce186399cf5fbb301ca91c9ee79d28fac04c7c37136`.
It is SHA-256 of sorted changed/new repository-relative paths in the Phase 7 diff
against the base above, each followed by NUL, final file bytes and NUL. This evidence
report is excluded. The reviewer independently ran both deployment profiles; the
final invocation passed 33 tests, and whitespace checking passed.

The regression for inherited controller environment variables failed before the
fix and passed afterward. It launches an isolated child test process with synthetic
credential/proxy/configuration values and verifies successful quota/create/wait/
exec/delete operations without passing them to kubectl or document-worker Pods.

## Validation

Commands run from the repository root. Cargo uses the pinned Rust toolchain and
cleared Rust flag overrides; shared-cache commands use the privileged channel.
`OPENLEGAL_TEST_MECAB_DICTIONARY` may point to an explicitly provisioned dictionary
to avoid redundant downloads; its admission checks still run.

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed |
| `cargo test --workspace --locked` | Passed |
| `cargo audit` | Passed with existing documented policy exceptions |
| `cargo deny check` | Passed; existing duplicate-version warnings remain |
| `cargo test -p openlegal-adapters --lib --locked document_jobs::tests` | 5 passed |
| `cargo test -p openlegal-server --test deployment_config --locked` | 5 passed |
| `scripts/test-kubernetes-serving.sh` | Both profile invariants and 33 tests passed |
| `scripts/test-kubernetes-serving.sh --profile text-only` | Both profile invariants and 33 tests passed |
| Default amd64 Docker build without `--target` | Passed; kubectl absent |
| `scripts/test-server-image.sh --platform linux/amd64 --target runtime` | Passed |
| `scripts/test-server-image.sh --platform linux/arm64 --target runtime` | Passed under local emulation |
| `scripts/test-server-image.sh --platform linux/amd64 --target runtime-ingestion` | Passed |
| `scripts/test-server-image.sh --platform linux/arm64 --target runtime-ingestion` | Passed under local emulation |
| `scripts/test-postgres.sh` | Passed |
| `scripts/test-korean-tokenization.sh` | Passed |
| `scripts/test-oxibelt.sh --profile fixture` | Passed |
| `scripts/test-oxibelt.sh --profile kubernetes` | Passed |
| `scripts/test-document-worker.sh` | Passed; 7 confined format/OCR fixtures and existing maintenance-notice exceptions |
| ShellCheck and Bash syntax for changed shell scripts; actionlint | Passed |
| Node syntax, changed Markdown relative links, `git diff --check` | Passed |

The packaged-client fixture checks actual quota retrieval, strict create and delete,
explicit context selection, missing files/context/token, untrusted CA, 401/403 and
token-file replacement between commands. Its measured cache was 2,678 bytes on
both amd64 and emulated ARM64 for the synthetic API; this is not a real-cluster sizing estimate. Synthetic
server validation does not establish Kubernetes admission or authorization. Docker
scratch is a bounded tmpfs; deployment scratch is disk-backed with kubelet eviction.

## Outstanding external gates

Real-cluster token projection/rotation, narrow RBAC enforcement, API Service routing,
provider IP maintenance, DNS/CNI enforcement, prepared-node gVisor/user namespaces,
seccomp/AppArmor/PID/resource controls and worker isolation require operator
acceptance. The existing configured sandbox gate needs the operator identity, not
an expanded controller Role. Live LAW OPEN DATA acceptance and production traffic
acceptance remain separately authorized and pending.
