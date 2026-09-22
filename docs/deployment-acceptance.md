# Phase 9 serving acceptance record

This record covers the Phase 9 patch built on
`885af347183a0e211ceb2689929f203a22aa3fd8`, exercised on 2026-09-22.
It is synthetic deployment evidence, not production or legal-provider acceptance.
The [acceptance checklist](deployment-kubernetes.md#phase-9-serving-acceptance)
owns the gate definitions; the [disposable fixture guide](../test-support/kubernetes-acceptance/README.md)
owns reproducible preparation and cleanup. Private credentials, host identifiers,
raw workload dumps and test logs are deliberately absent from this record.

## Environment and artifacts

- One native amd64 Kubernetes node, x86-64-v3 verified; enclosing dev VM has
  approximately 6.9 GiB physical RAM. Node cap is 6 GiB after bootstrap, edge
  256 MiB and each probe 192 MiB. This is not a measured production sizing model
  or an aggregate cgroup guarantee. No benchmark was run.
  Observed node cgroup peak was 3,960,508,416 bytes (about 3.69 GiB);
  this excludes sibling containers and is not an aggregate peak measurement.
- kind 0.33.0, Kubernetes 1.36.4, Calico 3.32.2 and kubectl 1.37.0. Exact
  dependency digests/checksums are in the fixture lock and deployment tool pins.
  Native-platform import preserved the admitted image indexes; CRI image
  resolution was checked before applying workloads.
- Production `runtime` image and fictional seed image built from the source
  baseline; full provisioned standard MeCab-Ko dictionary, PostgreSQL 18 with
  verified TLS, separate migration/runtime roles, and ingestion disabled.
  The admitted server image digest was
  `sha256:cfda5175c0621d3002250a4a64da149091947346ddc3a203ccd8fb078fdbfa63`.
- OxiBelt revision `72564d165dfd05cb29a64aeebd19fccd7944ea6f`, outside Kubernetes.
  Application NodePorts were not published on the VM host. A dedicated Docker
  bridge supplies node bootstrap routing and permits outbound connections;
  serving policy was separately tested through Calico and node firewall rules.
- Four distinct Local PV directories inside the disposable node. They are
  ext4-backed test paths, not ZFS datasets. PostgreSQL used disposable `emptyDir`;
  database Pod replacement durability was not tested.

## Local checks actually executed

| Command | Result and scope |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed |
| `cargo test --workspace --locked` | Passed; separately gated database tests remained explicitly ignored here |
| `cargo test --locked -p openlegal-server --example wt_client serving_smoke` | Six native smoke tests passed |
| `cargo audit` | Passed with the existing documented allowed advisory warning; cargo-audit 0.22.2 |
| `cargo deny check` | Advisories, bans, licenses and sources passed; cargo-deny 0.20.2 |
| `python3 -B -m unittest discover -s scripts/tests -p 'test_serving_smoke.py'` | 32 tests passed, including isolated loopback TLS |
| `target/deployment-tools/bin/python -B -m unittest discover -s test-support/kubernetes-acceptance -p 'test_*.py'` | 11 fixture preparation tests passed |
| `scripts/test-kubernetes-serving.sh` | 72 tests and 47 resources under each pinned offline schema version passed |
| `scripts/test-server-image.sh --platform linux/amd64` | Native Docker runtime/image gate passed |
| `scripts/test-server-image.sh --platform linux/arm64` | Emulated Docker runtime/image gate passed; not native ARM64 CI evidence |
| `scripts/test-postgres.sh` | PostgreSQL 18 integration gate passed |
| `scripts/test-korean-tokenization.sh` | Full dictionary and dual-engine gate passed |
| `scripts/test-oxibelt.sh --profile fixture` | Both revisions/transports and deployed-smoke profile passed in Docker |
| `scripts/test-oxibelt.sh --profile kubernetes` | Both revisions/transports and deployed-smoke profile passed in Docker; this profile itself is not Kubernetes routing evidence |
| ShellCheck on changed shell scripts; `actionlint`; `git diff --check` | Passed |

Image/database gates reused the existing explicitly provisioned dictionary through
`OPENLEGAL_TEST_MECAB_DICTIONARY`; OxiBelt gates reused the verified pinned binary
through `OXIBELT_BINARY`. Rust checks cleared CPU-flag overrides. Image builds
used existing local caches, then artifacts were transferred privately to dev;
cluster acceptance did not compete with Rust compilation in the small VM.
CI was updated to run the deterministic Python and native-example tests, but
hosted workflow results were not watched and are not claimed as passed here.

## Real disposable Kubernetes observations

Use the fixture guide's numbered `apply`, Job waits and explicit scale-to-one
sequence, then the smoke command in the deployment guide. The operational checks
below used explicit kubeconfig/context and only this disposable cluster.
The [operations appendix](../test-support/kubernetes-acceptance/OPERATIONS.md)
transcribes those checks into portable commands; its volume-based edge staging
adaptation was reviewed separately and was not rerun after cluster deletion.

| Check | Observed result |
| --- | --- |
| API admission, CNI/node preparation | Passed on Kubernetes 1.36.4; node and Calico DaemonSet became Ready |
| Stable Pod readiness | Passed using the new selected-field readiness checker |
| Private `/live` and `/ready` | Passed from the selected monitor namespace/Pod |
| HTTP and native WebTransport through OxiBelt | Both MCP revisions passed discovery, tools, source metadata, comparison/patch checks and owned-handle cleanup |
| HTTP SSE progress and final response | Passed with bounded, correlated monotonic progress |
| Invalid HTTP Host/Origin and public health paths | Expected 404/403/404 responses passed |
| Native invalid Origin | Explicit session rejection passed; the SDK does not expose an HTTP status |
| Direct NodePorts from actual edge container | TCP initialize and native UDP WebTransport passed with verified backend identity |
| Unintended client NodePort access | TCP and UDP denied; corresponding node raw-table drop counters increased and the permitted edge control remained successful |
| CNI health isolation | A client carrying the monitor label in the wrong namespace was denied while the real monitor succeeded; Calico workload drop counters increased |
| Migration Job and credential placement | Migration completed before serving; serving had only the runtime credential reference and no migration credential mount; migration had only its own credential |
| Runtime DDL denial | `SET ROLE runtime; CREATE TABLE public.openlegal_acceptance_ddl_probe (...)` failed with SQLSTATE 42501; the table remained absent |
| Effective storage permissions | UID/GID 10004; writable data paths were owner-only (2700, inherited setgid); distinct mounts; dictionary mount was `ro` and a write attempt failed |
| Ready-server SIGTERM | Scale-to-zero produced exit code 0 / Completed within the termination grace |
| Recreate ordering | Old container completion preceded replacement start; observed running-server count never exceeded one |
| Retained restart | Capture IDs, both retained versions, current body and zero-lag Korean query results matched before and after restart through the trusted NodePort |
| Competing runtime | A separate-index competing Job failed with the runtime conflict while the original server stayed healthy; retained assertions still passed |
| Backend trust and wrong-hostname negatives | Fresh native WT connections failed with an unrelated backend CA and a trusted wrong-SAN certificate while HTTP controls passed; restoring each valid input recovered WT |
| Final smoke | Initial final run failed legacy HTTP discovery with `unexpected_status`; modern HTTP, native WT and private health passed. A separate full run immediately after edge restart passed all selected checks; intermittent HTTP stability remains unresolved |
| Cleanup | Owned edge, kind node and Docker network deleted; no acceptance containers or networks remained. Private run artifacts were retained outside Git for deliberate operator disposal |

NodePort controls used a dedicated raw-table chain **inside the disposable node**,
restricted to the node address and TCP 30080 / UDP 30433: the intended edge source
returned to normal processing and other sources were dropped. CNI denial used
fresh connections and before/after workload drop counters. No VM-host firewall
rules were changed. These observations do not qualify any production interface,
external source translation, multi-node route or host firewall.

## Failures and remaining boundaries

Three fixture bootstrap attempts failed before application acceptance: an internal
Docker bridge had no default gateway for kind's DNS rewrite; kind's all-platform
image import requested absent architecture blobs; and a tagged digest alias did
not match CRI's canonical digest reference. The final fixture uses a dedicated
private bridge, explicit native-platform import and canonical digest aliases.
Failed owned clusters were deleted before fresh runs. The initial flat OxiBelt
certificate layout also failed closed; the guide now stages `config/` and sibling
`cert/` correctly. These were fixture preparation failures, not passed checks.

The pinned OxiBelt upstream fast path logged `channel closed` alongside failed
HTTP requests after backend replacement and again during a later smoke run with
no intervening backend replacement. Stale upstream connection reuse is a possible
cause, not an established diagnosis. Retained-data recovery passed independently
through the trusted NodePort. Restarting the edge restored the complete public
smoke run. This is observed recovery, not a lasting fix: intermittent public HTTP
stability and seamless backend-only upgrades are **not accepted** by this record.
No proxy dependency or serving behavior was changed to hide this result.

Production traffic, actual host firewall/ZFS qualification, cross-node routing,
native ARM64 Kubernetes execution, browser/ChatGPT, live LAW OPEN DATA and the
gVisor/AppArmor document sandbox remain pending. The serving fixture does not
enable ingestion or establish any of those separate gates.

## Independent review

Independent agents `cluster_finalize` and `smoke_finalize` reviewed the client
and fixture patch now recorded in commits `4dc40009131a4fc59fc650efc767abcc4bbbe7e9`
and `db185c58a729d962a68f78b950eb706a6c42415b`, respectively, with overlapping
Security and MCP/API Boundary coverage. They inspected the Python/native client boundaries,
fixture pins/bootstrap/cleanup, generated workload credentials/mounts/network
policies and CI wiring. Review findings about Origin alignment, source metadata,
stable Pod identity, subnet admission, temporary files, memory claims and native
example test execution were corrected and re-reviewed. The workload reviewer
also ran the real renderer/OpenSSL preparer and verified private output modes and
configuration references. `cluster_finalize` completed the final documentation,
evidence and operations-appendix consistency review with no outstanding blockers.
It checked the implementation against the executing agent's remote observations;
it did not independently replay the remote acceptance. Eleven Bash blocks, nine
embedded Python blocks and the generated trust-control code passed syntax checks;
relative documentation links and heading anchors resolved.
These reviews are technical evidence, not human GitHub approvals.
