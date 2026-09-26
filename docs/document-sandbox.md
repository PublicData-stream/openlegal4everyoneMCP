# Disposable document processing

The legal ingestion adapter sends provider XML/HTML and linked PDF, HWP5 and HWPX
bytes through `application::document::DocumentProcessor`. The implementation in
`adapters::document_jobs` creates one disposable Kubernetes Pod per document and
returns a bounded typed result. It does not fetch source URLs, publish records,
choose freshness, or retry upstream requests. Those responsibilities remain with
the ingestion application and its durable job store.

The separate `apps/document-worker` Cargo workspace prevents native document
dependencies from entering the serving binary. XML and HTML parsing also occur in
the Pod. Unit tests use explicitly fictional supplied fixtures. Provider identity
and legal-date mapping happen separately: parser success does not establish
identity, authoritative legal applicability, or complete text.

## Input, output and lifecycle

The trusted controller requires explicit absolute `kubectl` and kubeconfig paths,
an explicit context and namespace, and an image pinned by SHA-256 digest. There is
no ambient-context or mutable-image fallback. Configure one controller process;
the namespace quota additionally limits live Pods to two across controller crashes.

Each `kubectl` subprocess starts with a cleared environment and only fixed
`PATH=/usr/local/bin:/usr/bin:/bin`, `HOME=/tmp` and `TMPDIR=/tmp`. This includes
cleanup commands. Provider/database credentials, proxy settings, `KUBECONFIG`
and credential-plugin environment variables are not inherited. Explicit kubeconfig,
context and namespace arguments remain required; authentication configurations
that depended on inherited environment variables must be changed by the operator.
The discovery cache is explicitly `/tmp/openlegal-kubectl-cache`. Provide writable,
bounded temporary storage at `/tmp` when using a read-only controller root.

The opt-in [Kubernetes ingestion procedure](deployment-kubernetes.md#optional-ingestion-integration) preserves this
configuration contract using a dedicated read-only kubeconfig and projected
ServiceAccount identity. The kubeconfig references the rotating token through
`tokenFile` and the cluster CA through `certificate-authority`, both at explicit
absolute paths. Mount the projected identity directory without `subPath` so token
rotation remains visible. Parser Pods never receive these controller mounts.
The controller identity is bound only to the existing namespaced Role; the
acceptance harness uses a separately authorized operator identity because its
NetworkPolicy and log checks require permissions beyond that Role.
Follow that procedure for sandbox qualification and separately authorized live
acceptance before enabling ongoing ingestion; the serving acceptance record does
not qualify the document sandbox or provider integration.

The application supplies a typed format, at most 100 MiB of already fetched source
bytes, their SHA-256, and the OCR choice. A fixed `kubectl exec -i` invocation runs
`/usr/local/bin/openlegal-document-worker --process`. The input is a four-byte
big-endian JSON-header length, a header of at most 4096 bytes, and exactly the
declared source bytes. The worker verifies the digest and rejects trailing bytes.
Output is a four-byte big-endian length and one JSON success/error response,
bounded to 16 MiB. The controller rejects extra bytes, bad framing, digest/format
mismatches, invalid page sequences, unrequested OCR and malformed trees.

The XML output preserves ordered mixed content, repeated element names, attributes
and namespaces. It does not flatten repeated provisions into a map. XML DTDs and
external entities are rejected; UTF-8 is required. HTML parsing is inert, retains
source structure/attributes, and excludes active-element text from its display
projection. Neither parser follows links. Trees are limited to 100,000 nodes and
32 element/text levels, within the JSON decoder's independent nesting bound.

Binary formats return separate native and OCR page arrays with one-based physical
page locators. They are capped at 500 pages. Native PDF extraction disables OCR,
caching and quality rewriting. HWP native text is a **rendered text projection**;
diagnostics disclose possible font substitution. It must not be called a byte-
preserving serialization or a complete legal-text reconstruction.

OCR is an explicit additional representation. PDF pages are rendered individually;
HWP pages are rendered by rhwp to SVG, rasterized with resvg, and passed to Xberg's
Tesseract backend with Korean and English models. Raster dimensions are bounded
before allocation to 4000 by 4000 pixels. SVG filesystem/URL image resolution is
disabled, and every generic font family maps explicitly to the bundled Noto font.
Otherwise an unavailable platform-default serif can silently erase rasterized text.
Model files must already exist in the immutable image. No model choice,
path, URL, extraction configuration or executable is accepted from a public caller.
Missing models fail processing; no runtime download is authorized.
Tesseract's explicit `text` output and disabled table detection avoid its
Markdown/hOCR dictionary filter, which can remove unfamiliar Korean/legal terms.
The representation is derived plaintext OCR, including Xberg's common control-
character cleanup; it is not byte-exact raw Tesseract output.

The worker records processing versions and stable diagnostic codes. Warnings are
not a completeness measurement, and clean extraction is not proof of complete
legal evidence. Raw provider bytes, derived results and processing versions must
remain separately associated in corpus persistence. OCR is excluded from default
search/comparison; the application chooses it only through the explicit option.

The controller verifies the named namespace quota has a two-Pod limit before
creating work, and owns a process-wide two-job semaphore, 300-second deadline and
cleanup after success, cancellation, failure, timeout or uncertain Pod creation.
Caller-future disposal does not dispose the cleanup task. API-server failures
during deletion return `sandbox_unavailable` and keep that process admission slot
consumed until restart; the quota prevents unbounded Pod
creation. Administrators must reconcile terminated Pods after controller/cluster
failure. Durable queue coalescing and crash replay belong to the ingestion worker.
Neither source content nor extracted content is sent to container logs.

## Image and dependency admission

The release pipeline publishes the amd64-only worker at
`ghcr.io/publicdata-stream/openlegal-document-worker`. Stable `X.Y.Z` and beta
`X.Y.Z-beta.N` versions follow a published GitHub Release; build candidates use
`X.Y.Z-build.<8-hex-commit-prefix>` annotated tags. It publishes the image that
passed the native smoke gate and records its digest and provenance in the
`ghcr-release-digests` artifact. There is no `latest` or major-version alias.
Use the accepted `ghcr.io/publicdata-stream/openlegal-document-worker@sha256:<digest>`
reference for `DOCUMENT_IMAGE` and the ingestion overlay; the worker requires an
amd64 node with x86-64-v3 support. The first public release also requires package
visibility and anonymous-pull verification. Publication alone does not qualify
the worker's real-cluster sandbox or authorize provider traffic; follow the
[release image procedure](deployment-kubernetes.md#ghcr-release-images) and the
cluster gate below.

Build from the repository root:

```sh
docker build --platform linux/amd64 -f apps/document-worker/Dockerfile -t openlegal-document-worker:local .
```

Run the comprehensive standalone CI gate with:

```sh
scripts/test-document-worker.sh
```

It builds the separate `fixture-tests` image target, checks formatting, native
Clippy, pure parser tests, the standalone advisory/license policy, then runs
seven fictional PDF/HWP5/HWPX native and OCR fixtures with the worker's seccomp profile,
no network, a read-only root and its CPU/memory/PID/scratch limits. Tests require
positive English and Korean native extraction and OCR recognition, including a
PDF with only glyph outlines and no native text layer;
merely returning an empty success is insufficient. The production image excludes
the fixture executable. Rustfmt/Clippy follow the pinned toolchain; cargo-audit
0.22.2 and cargo-deny 0.20.2 are installed with their locked dependencies. Advisory
data is intentionally refreshed when this validation stage executes.

The Dockerfile pins Rust 1.98.1 and Alpine 3.24 base digests and builds a native,
dynamically linked musl executable. Signed moving Alpine v3.24 repositories supply
Tesseract/Leptonica/Fontconfig; `/opt/notices/alpine-packages.txt` records installed
runtime versions. The standalone lockfile fixes Rust dependencies. Both native
build and runtime package installation use the
shared [bounded Alpine download policy](dependencies.md#production-image-build-inputs),
including strict metadata failure and a 15-minute package transaction deadline.
Run `scripts/test-alpine-packages.sh` for its isolated failure-injection gate;
the full worker gate above remains required for native extraction and OCR.
Moving native package versions can change extraction results between rebuilds.
Record the accepted image digest and native inventory alongside qualification
evidence; previous image digests, not rebuilt source alone, identify rollback
artifacts. Existing retained captures are not rewritten by this image migration.

The following source/version selections were verified on 2026-09-16:

| Component | Selection and reason | License |
| --- | --- | --- |
| [roxmltree](https://github.com/RazrFalcon/roxmltree) | 0.21.1; bounded pure XML tree, DTD disabled | MIT/Apache-2.0 |
| [scraper](https://github.com/rust-scraper/scraper) | 0.27.0; inert HTML5 tree parsing | ISC |
| [rhwp](https://github.com/edwardkim/rhwp/tree/680111ec7bea2fe11110de18c3676ba5a1cf7847) | 0.8.6 at the linked commit; native HWP5/HWPX and page rendering, default features disabled | MIT |
| [Xberg](https://crates.io/crates/xberg/1.2.2) | 1.2.2, default features disabled; PDF, OCR, dynamic Tesseract and Tokio only | MIT |
| [resvg](https://github.com/linebender/resvg) | 0.47.0, text and raster images; no system font discovery | MIT/Apache-2.0 |
| [tessdata_fast](https://github.com/tesseract-ocr/tessdata_fast/tree/65727574dfcd264acbb0c3e07860e4e9e9b22185) | 4.1.0 English/Korean model assets, SHA-256 checked | Apache-2.0 |
| [Noto Sans CJK](https://github.com/notofonts/noto-cjk/tree/523d033d6cb47f4a80c58a35753646f5c3608a78) | 2.004 Korean regular OTF, SHA-256 checked | SIL OFL-1.1 |

rhwp is unavailable as a crates.io package and its source repository includes a
large fixture history; fresh builds need substantial temporary source-cache space.
Provide at least **20 GiB of free build disk**. The observed rhwp Cargo Git cache
alone occupied approximately 5.2 GiB, plus 0.9 GiB for registry sources before
compiler artifacts and image layers. These build requirements are separate from
the disposable worker's 2 GiB scratch and 4 GiB memory limits.
No upstream test documents are committed here. Xberg uses its published registry
package to avoid cloning its unrelated test-document submodule. Its optional
remote-service, server, Office, layout-model and VLM capabilities are not enabled.
Native build scripts and transitive unsafe code run only in the build/image
boundary; the first-party crate retains the workspace's unsafe-code deny policy.
The worker explicitly initializes only Tokio's timer driver. Its native dependency
graph enables signal features, but initializing the I/O driver would create an
unnecessary Unix socketpair. The worker's policy continues to deny socket creation.
The profile returns ENOSYS for clone3 so glibc can use the filtered clone fallback;
EPERM would prevent thread startup before the PID limit can be tested.

The image includes model/font, dependency and project notices. Dependency admission
also covers the standalone lockfile; a successful serving-workspace check does not
cover it:

```sh
cargo audit --file apps/document-worker/Cargo.lock
cargo deny --manifest-path apps/document-worker/Cargo.toml --config apps/document-worker/deny.toml check
```

The isolated policy retains the root policy defaults and adds exact package license
admission, the immutable rhwp source, and three time-limited maintenance-notice
exceptions. `paste` 1.0.15 ([RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436/))
is a build-time macro dependency. `rustybuzz` 0.20.1
([RUSTSEC-2026-0206](https://rustsec.org/advisories/RUSTSEC-2026-0206/)) and `ttf-parser`
0.25.1 ([RUSTSEC-2026-0192](https://rustsec.org/advisories/RUSTSEC-2026-0192/)) remain in
rhwp/resvg's runtime graph, including parsing untrusted embedded font data. Sandbox
isolation limits impact; it does not restore upstream maintenance. No safe upgrade
exists in the selected graph. Owner PiQuark6046 must review replacement/update
options by **2026-10-16** and whenever parser dependencies change. These exceptions
require independent parser/security review and do not suppress vulnerability or
unsoundness advisories. The root serving policy is unchanged.

`undoc` and `office_oxide` were considered for broader Office extraction, but the
implemented format set uses rhwp and Xberg instead of adding redundant parsers.

## Cluster preparation and acceptance

The [Alpine qualification record](alpine-cluster-qualification.md) identifies an
AppArmor incompatibility in the inspected gVisor release. The requirements below
remain mandatory; Alpine image acceptance does not qualify that runtime.

Provision a dedicated namespace with `deploy/document-sandbox/namespace.yaml` and
bind `controller-role.yaml` to a separately managed trusted controller identity.
The Pod never contains that identity or mounts any token, secret, host directory,
or host namespace. The controller needs Pod exec access (WebSocket GET and SPDY POST) and read access
to the single named quota; it does not need Pod-log
read access. The acceptance harness's operator needs log read access to verify
that source bytes do not reach logs.

The selected same-host K3s worker Pod uses the node's existing `runc` handler.
This shares the host kernel with serving and therefore requires independent
security review of the changed isolation boundary and the complete real-cluster
acceptance gate below before live documents are processed:

- Verify the generated containerd `runc` handler and require actual
  `hostUsers: false` user-namespace support. Do not label the node based on the
  RuntimeClass object alone.
- Merge `podPidsLimit: 128` into the actual kubelet configuration.
- Install the seccomp profile as `openlegal-document.json` under the kubelet's
  seccomp root and load the named AppArmor profile in enforce mode.
- Verify CNI deny-all NetworkPolicy enforcement, then qualify the runtime using
  the acceptance gate. If the selected runtime cannot enforce every required
  property together, leave the node unqualified; do not weaken the manifest.

Each Pod has a read-only root, non-root UID/GID 65532, no capabilities, no privilege
escalation, explicit seccomp/AppArmor, two CPUs, 4 GiB memory, 2 GiB ephemeral
storage and a 2 GiB `emptyDir`. PID limits come from kubelet configuration, never
an invented Pod resource field. Kubernetes ephemeral-storage enforcement is
eviction-based unless the underlying node filesystem supplies stronger quotas;
the configured `emptyDir` size is not a synchronous per-write disk quota.

Run the real-cluster gate only with explicit configuration:

```sh
RUN_DOCUMENT_SANDBOX_TESTS=1 \
DOCUMENT_KUBECONFIG=/absolute/path/to/test-kubeconfig \
DOCUMENT_CONTEXT=isolated-test \
DOCUMENT_IMAGE=registry.example/openlegal-document-worker@sha256:<64-hex-digest> \
scripts/test-document-sandbox.sh
```

The gate creates/deletes synthetic worker Pods and checks user mapping,
privilege/capability restrictions, AppArmor/seccomp, network EPERM/EACCES (not mere unreachability),
read-only root (EROFS at an image-owned writable probe path), absent tokens, actual CPU/memory/PID cgroups, XML exec framing,
absence of document logs, PID/memory exhaustion and deadline enforcement. On
the selected user-namespaced `runc` Pod, the container-local `pids.max` can
report a wider ancestor; the gate verifies effective PID denial with the
exhaustion probe, and the operator separately verifies kubelet `podPidsLimit=128`.
It
uses a shortened deadline probe for the same kubelet enforcement mechanism.
Manifest inspection and local Docker tests cannot establish those Kubernetes
properties. Full format/OCR fixture acceptance and actual cluster acceptance must
be reported separately from ordinary Rust checks; no deployment or live provider
acceptance is implied by these artifacts.

## Local validation evidence (2026-09-16)

The complete `scripts/test-document-worker.sh` gate passed from source: formatting,
native Clippy, advisory/license admission, three release XML/HTML unit tests,
production runtime image construction, and seven confined release binary-format
tests. The advisory check reported the three explicitly documented maintenance
notices and no vulnerability advisory failures.

The seven fictional binary-format tests passed under the supplied seccomp profile
with a read-only root, no network, dropped capabilities, no privilege escalation,
2 CPUs, 4 GiB memory, 128 PIDs and a 2 GiB scratch tmpfs. They verify native English
and Korean text for PDF/HWP5/HWPX, derived English and Korean OCR for each, and OCR
recognition from an outlined PDF with no native text layer. They do not establish
completeness on real legal documents or arbitrary fonts/layouts.

XML/HTML framed processing and the PID exhaustion probe also passed locally; the
probe created 127 threads before denial. Independent testing showed that omitting
the clone3 ENOSYS rule makes the first pthread fail with EPERM. The unconstrained
probe reported both network denial and root-readonly as false; the constrained
probe required actual EPERM/EACCES and EROFS, respectively. These negative controls
prevent unreachable networks or ordinary filesystem permissions from posing as
sandbox evidence. Controller tests verify quota scope rejection, worker identity
validation and Pod cleanup after cancellation.

## Same-host K3s qualification (2026-09-26)

The real-cluster synthetic gate passed on the selected single-node K3s 1.36.4
cluster with containerd 2.3.4, `runc` 1.4.2 and the immutable worker image
`sha256:e60c5f55445031acecf6a0602bbd78115b9a90f62c650b985b19649950a676d7`.
The node's effective kubelet configuration reported `podPidsLimit=128`; a worker
probe could create 125 threads before denial. The worker's local `pids.max`
reported a wider value, so the gate uses the actual denial result. AppArmor
enforce, seccomp, user namespaces, network denial, non-root credentials,
capability drop, read-only root, absent token, CPU and memory limits, XML
framing, empty logs, memory exhaustion, deadline and cleanup all passed.

The worker-ready node label was removed after the test; no worker Pod remains.
This establishes the listed controls for a disposable synthetic Pod, not
live-provider parsing. Independent security review found no confirmed Pod isolation
bypass and identified the shared-kernel escape impact and resource-pressure risks;
the selected same-host design accepts that kernel boundary, subject to the
resource and policy checks below.
The CNI deny-all egress check separately used identical short-lived BusyBox Pods:
TCP connection to the Kubernetes API Service succeeded from the default namespace
and failed from `openlegal-documents`. Both probe Pods were deleted afterward.
The installed seccomp file at `/var/lib/kubelet/seccomp/openlegal-document.json`
matched this repository's `seccomp.json` SHA-256
`f7e707dfd75b2a421f8ae18ff6e3b060e428fa74dff1a476b8003a074cbc1f9d`;
the installed `/etc/apparmor.d/openlegal-document` matched `apparmor.profile`
SHA-256 `8f6579dad3e99efdb1734c76af035dac7afbe1159aaa42d43fe5f0beff30dc3f`.
On this 40-core, 251 GiB node, Kubernetes reported no memory or disk pressure,
2,779 MiB node memory use and 1.7 TiB free on the kubelet filesystem. These
point-in-time measurements establish ample current headroom for the two 4 GiB
worker limits, but do not replace concurrent OCR load and eviction observation.
