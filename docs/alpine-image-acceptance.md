# Alpine 3.24 migration acceptance (2026-09-22)

This record covers the change from Debian to Alpine for the server, ingestion
server and amd64 document-worker images. It does not qualify production deployment
or live legal-provider ingestion. Existing captures and corpus formats are unchanged.
Moving Alpine packages can change native extraction output on later rebuilds;
retain accepted image digests and inventories rather than rebuilding for rollback.

## Inputs

The migration starts from `4ad6c8b89d6bcc247ad67c3c3c4daa8ebb89dc36`; the signed
implementation revision is `a227b802f4b9b8f3911e2abe6e42aad0e2e7dce5`.
Rust 1.98.1, Node 24.21.0, pnpm 12.3.4 and the application lockfiles are unchanged.
Every production Dockerfile stage now uses a pinned Alpine 3.24 image. The
production binaries dynamically link musl, without glibc compatibility packages;
GNU development remains supported. Server platforms remain amd64/x86-64-v3 and
ARM64/generic, and the worker remains amd64/x86-64-v3.

| Base | Multi-platform index digest |
| --- | --- |
| Alpine 3.24 | `sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6` |
| Rust 1.98.1 Alpine 3.24 | `sha256:7cc1c22d77d9432f7fe012a70e6d3e555af54c2a6832700ed7d553f1769ae89f` |
| Node 24.21.0 Alpine 3.24 | `sha256:ebfe2f90462722a7a4de65e91990e97fe0d401c70e0e762c5b53302f905ec1c1` |

The locally retained amd64 build digests are:

| Target | Local repository digest |
| --- | --- |
| Server | `openlegal-alpine-runtime@sha256:9e825c6c518c695b45472ea8ce7b6a3968596cd2d144fd880d0f84f5ddd75831` |
| Ingestion server | `openlegal-alpine-ingestion@sha256:1f33f146bdb58e44cf110e0fed67d1f1992fef0bad2561c867ad31cb6185d6a9` |
| Document worker | `openlegal-document-worker@sha256:efbeaee27ac0cfb312d85de1d649e016c0e4aeff0558a5fdcdba1f4a136f5284` |

These artifacts were built locally, not published to a registry. Both server
images carry the implementation revision in their OCI labels. The worker retains
its existing labeling contract; its accepted Dockerfile and application inputs
are those in the implementation commit.

## Validation

Run commands from the repository root. The amd64 Docker integration ran on the
remote disposable development VM. ARM64 image checks used emulation on the local
rootless Docker host while that VM was occupied; they are not native ARM64
evidence. Local Rust and widget checks used the GNU development environment.
No legal-provider traffic was generated.

| Check | Observed result |
| --- | --- |
| Workspace fmt, Clippy with warnings denied, locked tests | Passed: 217 tests; 47 integration tests separately gated |
| `cargo audit`, `cargo deny check` | Passed with existing admitted warnings/exceptions |
| APK helper/fixture unit tests | 16 passed |
| `scripts/test-alpine-packages.sh` scenarios | All 11 observed passing across helper revisions; ten non-stall scenarios rerun on the final helper, including real APK signature/hash/TLS rejection |
| Stalled response headers | Six package requests, rejected after 752 seconds; no installed marker |
| APK outer transaction deadline fixture | Rejected after 12 seconds (two-second test deadline plus ten-second grace) |
| Real v3.24 package download and offline installation | Passed, inventory generated |
| Deployment validation | 72 tests and strict validation of 47 resources for each Kubernetes 1.36/1.37 target passed |
| Acceptance fixture and memory preflight | 15 existing fixture tests and six memory tests passed |
| Native amd64 `runtime` image | Full image, retained corpus, administration/recovery harness passed |
| Native amd64 `runtime-ingestion` image | Same harness plus synthetic kubectl credentials/rotation/401/403 checks passed |
| Emulated ARM64 `runtime` and `runtime-ingestion` | Both full image/retained-data gates passed; ingestion credentials/rotation/401/403 checks passed |
| Worker build/native admission and confined fixtures | Passed: three unit tests, seven binary/native/OCR fixtures, production framing/PID/log assertions |
| PostgreSQL integration | 47 tests passed; zero failures |
| Complete Korean dictionary | Full-dictionary test passed |
| Serving-smoke example unit tests | Six passed |
| OxiBelt Docker profiles | Both fixture and kubernetes profiles passed |
| Widget typechecking/unit/build | Passed; 30 unit tests |
| Widget browser/dependency checks | 39 browser tests passed; no known advisory, 30 installed dependency licenses checked |
| ShellCheck, actionlint, `git diff --check` | Passed |

Image checks assert Alpine version, recorded APK inventory, dynamic musl linkage,
absence of glibc compatibility/build tools and existing runtime hardening. The
worker gate additionally tests the actual production target's framed XML/HTML
processing, inert scripts, PID enforcement and absence of document content in logs.
The initial BusyBox inspection shell failed at process creation under the
worker seccomp allowlist; the same shell passed under Docker's default profile.
Image-content inspection therefore uses Docker's default seccomp while keeping its other restrictions.
Actual parser fixtures and production-worker execution retain the unchanged
canonical profile. Docker confinement evidence does not establish Kubernetes
sandbox enforcement.

The final APK helper SHA-256 is
`50fb66d49f93367afb03720393c9713cdd251467502b48d8d4ca5932a4a8c762`.
Ten native scenarios passed against that final helper. The 752-second stalled-header
run passed against the preceding helper, before GNU timeout exit-code handling and
the unrelated-signal guard were added. It was not repeated on the final helper;
the final 16 unit tests cover those signal and watchdog branches. The two native
batches used these commands:

```sh
scripts/test-alpine-packages.sh stall
scripts/test-alpine-packages.sh baseline recovery persistent stale tls signature hash unknown mixed deadline
```

Reproduce the production image checks after provisioning the tools and complete
Korean dictionary described in the canonical deployment and contributor guides:

```sh
scripts/test-server-image.sh --platform linux/amd64 --target runtime
scripts/test-server-image.sh --platform linux/amd64 --target runtime-ingestion
scripts/test-server-image.sh --platform linux/arm64 --target runtime
scripts/test-server-image.sh --platform linux/arm64 --target runtime-ingestion
scripts/test-document-worker.sh
scripts/test-postgres.sh
scripts/test-korean-tokenization.sh
scripts/test-oxibelt.sh --profile fixture
scripts/test-oxibelt.sh --profile kubernetes
```

ARM64 validation used QEMU on x86_64. Its initial two-job build was deliberately
canceled, preserving its cache and exit 130 record. The successful run supplied
`--build-arg BUILD_JOBS=8` to the server Docker build through a private command
wrapper; production Dockerfile defaults and CPU/linker flags were unchanged.
Host-side Cargo used four jobs. Both completed gates recorded exit zero, and
their recorded input hashes match the implementation revision. Native ARM64 CI
was not observed; hosted Actions results are not claimed here.

The first native integration attempt was stopped before disk exhaustion and is
not counted as a passing run. Its logs were preserved. The rerun retained the
same Cargo profiles; only owned incremental/build artifacts were cleaned up,
with required executables copied and SHA-256 verified before cache removal.

The OxiBelt `kubernetes` profile above is a Docker handoff fixture, not a real
cluster. A current-source GNU server/client build and Alpine-built widget artifacts
are its inputs; its separately pinned edge binary verifies its own clean revision.

## Disposable Kubernetes serving observations

The real-cluster run used the amd64 server digest above and a clean checkout of
`a227b802f4b9b8f3911e2abe6e42aad0e2e7dce5`. It followed the
[serving fixture guide](../test-support/kubernetes-acceptance/README.md) and
[operations appendix](../test-support/kubernetes-acceptance/OPERATIONS.md), using
kind 0.33.0, Kubernetes 1.36.4, Calico 3.32.2, kubectl 1.37.0 and the pinned
OxiBelt revision `72564d165dfd05cb29a64aeebd19fccd7944ea6f`.
The enclosing guest exposed approximately 6.94 GiB RAM; the node cap was 6 GiB,
the external edge cap 256 MiB. Ingestion remained disabled. All supplied records
were fictional, PostgreSQL and storage were disposable, and this was not a
production sizing benchmark or target-production acceptance.

| Stage | Observed result |
| --- | --- |
| Cluster, artifact admission, bootstrap and external edge | Passed; exact source revision and immutable image admitted |
| Retained corpus and runtime DDL denial | Passed |
| Pod storage permissions and credential separation | Passed |
| Initial public HTTP/WebTransport and health checks | Passed |
| Network/CNI and NodePort isolation | Passed with permitted-client controls |
| Backend trust and wrong-hostname rejection | Passed with restoration controls |
| SIGTERM, Recreate and retained-data lifecycle | Passed |
| Competing runtime lease | Passed |
| Final public smoke before explicit edge recovery | **Failed**: legacy HTTP discovery returned `unexpected_status` |
| Public smoke and retained data after explicit edge restart | Passed; the earlier failure remains recorded |
| Owned node, edge, network and volume cleanup | Passed |

The failed final run reported `http.2025-11-25.discovery: failed
(unexpected_status)`; subsequent legacy HTTP checks were not run because their
prerequisite failed. Modern HTTP, both native WebTransport revisions, public
health exclusion and private `/live`/`/ready` passed in that same run. The overall
runner retained **exit 1** despite the separate successful recovery stage.
This resembles the [previously documented symptom](deployment-acceptance.md#failures-and-remaining-boundaries),
but current causation is unproven. Current edge logs were not retained, so the
historical `channel closed` observation must not be attributed to this run.
Full serving qualification and seamless backend-only upgrades remain unaccepted.

The node cgroup peak was 4,451,274,752 bytes (about 4.146 GiB), within its 6 GiB
cap. It excludes sibling containers and is not an aggregate peak measurement.
No host firewall policy or AppArmor profile was changed. Private credentials,
raw logs and workload dumps were retained outside Git; owned runtime resources
were deleted. The historical Phase 9 acceptance record remains unchanged.

## Independent review and remaining boundaries

For the implementation revision above, independent agent `worker_alpine` reviewed
the image/libc changes and production-worker smoke tests; independent agent
`server_alpine` reviewed package acquisition and retry failure handling,
CI/documentation integration and memory admission. No unresolved finding remains
in those reviewed scopes. These were agent reviews, not human GitHub approvals or
a repository-wide security scan.

See [cluster qualification](alpine-cluster-qualification.md) for the
immutable gVisor AppArmor incompatibility and actual guest-memory observations.
No runtime policy was weakened. Hardened worker and dependent real-cluster
controller integration remain pending a compatible runtime decision. Synthetic
controller Docker checks do not satisfy that separate gate.

Production deployment, live-provider acceptance and native ARM64 Kubernetes
execution remain separate unexecuted gates. No images or commits were published.
