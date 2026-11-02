# Storage stability preparation review, 2026-11-02

Recorded on **2026-11-02**, against signed source
`ab679f0f0a845dece1d03064ebda913bbd0af1e8`. The recording commit is the signed
commit introducing this file, discoverable with
`git log --diff-filter=A --format=%H -- docs/reviews-storage-stability-preparation-2026-11-02.md`.
Current behavior belongs to the [persistence contract](persistence.md),
[operator runbook](deployment-kubernetes.md) and
[operator implementation guide](../scripts/storage-stability/README.md).
Later lifecycle integration is outside this point-in-time record.

## Source and independent review

Signed changes separate the responsibilities:

- `d8a0f62797e2b2d9a2fa3eb15168ed8054481bff`: targeted standalone worker
  `pulldown-cmark` security update. No other locked package changed.
- `52ac5a9`: opt-in checkpoint, planned recovery, preservation, command ownership,
  canary and isolated HDD diagnostic actors, their tests and operator CI.
- `bd02e41`: separately pinned initial enrollment and shared epoch primitives.
- `ab679f0`: current-head and read-only primary fence ports, with lossless serial
  validation matching the existing PostgreSQL binding domain.

Separate Codex implementation and review agents inspected actual patches and
relevant callers under **Security Review** and **Upstream/Cache Correctness
Review**. The Codex primary agent independently reviewed delegate-authored
maintenance, pair corrections, certificate, current-head and fence changes.
A separate reviewer inspected primary-agent observation refactors and policy/CI
changes. These are agent source reviews, not human GitHub approvals or native
operating acceptance.

Review findings were corrected before the affected changes were committed:
uncertain helper/SQL ownership must retain ROOT; column, inherited and private
catalog privileges cannot bypass role checks; raw source graphs and approved
authority must agree; certificate reason and epoch lineage must agree; and
current-head selection cannot fall back to an older certificate.

The final read-port source identities are:

| File | SHA-256 |
| --- | --- |
| `epoch_current.py` | `d08426f653ab3910fab0bae889376db2e296ab5ea15d11d6f2725350ec756a6d` |
| `epoch_primary_fence.py` | `0eaeb4f2bd1ac8e9a4388504f59a314a03e31743ad023cbd1167b8f5aca474cc` |
| `linux_backend.py` | `6f638c42466b6ac6cd2e239aef8012db7edc2d280945cba1bbeb9b0795f4e91d` |

## Checks actually completed

The frozen initial preparation series passed **785 tests, zero skips** as ROOT,
using only disposable temporary storage and a reviewed native libpq client.
The runner selected every then-frozen operator test module and checked source
hashes before and after execution. The still-in-progress primary fence module
was explicitly excluded; the current-head module had not yet been added.
EOF-only cleanup afterward preserved the exact ASTs of the four affected files.

Portable reproductions of the subsequent focused checks are:

```sh
sudo -n env TMPDIR=/tmp python3 -I -B -m unittest discover \
  -s scripts/storage-stability/tests -p 'test_epoch_current.py'
sudo -n env TMPDIR=/tmp python3 -I -B -m unittest discover \
  -s scripts/storage-stability/tests -p 'test_epoch_primary_fence.py'
env TMPDIR=/tmp python3 -I -B -m unittest discover \
  -s scripts/storage-stability/tests -p 'test_linux_backend.py' \
  -k StaticReplicationSerialDomain
systemd-analyze verify deploy/systemd/openlegal-storage-checkpoint.service \
  deploy/systemd/openlegal-storage-checkpoint.timer
git diff --check
```

The focused suites passed **20**, **26** and **2** tests respectively, without
skips; the serial suite covers 22 accepted/rejected cases. Actual root invocations
also specified `-X pycache_prefix` with separate fresh temporary directories. The unit verification exited zero;
the host reported unrelated installed-unit CPUAccounting warnings.
The native libpq regression was executed, not skipped. These client-library,
file-descriptor and controlled-child fixtures do not run PostgreSQL or establish
two-device durability.

Worker `cargo audit --file apps/document-worker/Cargo.lock` and
`cargo deny --manifest-path apps/document-worker/Cargo.toml --config apps/document-worker/deny.toml check`
passed. Hosted [CI for the dependency fix](https://github.com/PublicData-stream/openlegal4everyoneMCP/actions/runs/41457478687)
completed successfully. That result does not validate later operator commits.

## Operational evidence and remaining gates

A private, isolated local Docker/containerd fixture completed an image phase and
a fixed tiny-container phase with actual external child exit zero and native
process closure. Its AppArmor exception was fixture-only. It establishes neither
hardened-container acceptance nor a production HDD diagnostic or deployment.

A separately approved operational calibration used 256 synthetic files totaling
4,849,664 bytes in one fresh 32MiB-quota dataset. The actual runner exited **1**
after **300.46 seconds**; its original deadline was detected after a copy-directory
barrier. Twelve recorded native commands had exited zero with owned processes
closed. Subsequent read-only audit confirmed unchanged protected pool properties
and original-seven dataset properties. The synthetic target and failure marker
were retained, with no child datasets or snapshots. No automatic retry or cleanup
was performed.

This failed window establishes no valid copy throughput, per-file quantiles,
single-fsync latency, or 40-minute full enrollment fit. Public server-info and
corpus-status calls still responded afterward. The existing SSD lookup deployment
was retained; no collection activation or unrelated database adjustment occurred.

Actual migration/role/pair acceptance, current completed epoch installation,
renewal before every controlled restart or Ready path, coordinated backup and
isolated restoration, manual recovery, and the six-hour bounded collection and
eligible-publication acceptance remain separate gates. These source tests and
read-port results authorize none of them and do not establish production readiness.
