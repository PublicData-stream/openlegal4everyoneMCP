# Initial epoch producer source review, 2026-11-02

Recorded against signed producer commit
`cd0334585b5c5eda264c07f9557e1af0ced5f30d` and signed serial correction
`a80f4abbf85e211a3901eca7b73857651d534f60`. The recording commit is discoverable
with `git log --diff-filter=A --format=%H -- docs/reviews-initial-epoch-producer-2026-11-02.md`.
The [operator guide](../scripts/storage-stability/README.md#initial-certificate-producer)
and [persistence contract](persistence.md#strict-two-copy-durability) own current
behavior. This record establishes source preparation, not operating acceptance.

## Reviewed behavior

The callable initial producer consumes the original live ROOT/backend/shared
lock, genuine first-pair bind and final acknowledgement, and the unchanged
initial deadline. It observes strict configuration and four Blob roots before
constructing an initial generation-1 certificate. Distinct source-fixed children
perform observation and dual installation; ROOT separately records their actual
waitpid status and descendant closure. Independent current-head reads and fresh
post-install observations precede detached completion with final both-database
SQL closure. The result explicitly denies Ready/public ACK and cannot observe
its own outer ROOT exit.

A separate Codex reviewer inspected the actual producer, tests, surrounding
contracts, manual serial patch and operator-guide changes under Security Review
and Upstream/Cache Correctness Review. The final reviewed identities were:

| File under `scripts/storage-stability/` | SHA-256 |
| --- | --- |
| `initial_epoch_certificate.py` | `402bd5c5b95e0c2e49ad8b9f7d760014bd6604554f0061fe053a14344402612d` |
| `tests/test_initial_epoch_certificate.py` | `2d49dbee91a915983367c0c59d86f60ac8e652f585a81fb6b7000344fe748132` |
| `manual_backend.py` | `9deb71680b3f67c86f5e0fe132c4ef6e9ca2d204b43e3dc63f0557aa510feca5` |
| `tests/test_manual_backend.py` | `ec4e702fc77d47d270ecb0adfa4581b3a7b59d49c8def08b7df9e8d7a993f827` |

The reviewer cleared this exact source-preparation scope with no open findings.
This is an agent source review, not a plugin scan, human GitHub approval or
permission for native enrollment, deployment, provider traffic or hold release.

Resolved findings include configuration/CA mount shadowing and post-fork socket
cleanup errors escaping actual child ownership. Wait ownership now exists before
fork, receives the raw PID before fallible cleanup or identity registration, and
uses uncapped descendant drainage on failure. Child-prologue errors cannot unwind
into ROOT caller control flow. The manual serial domain now matches the existing
canonical positive 1–60 decimal digit replication-binding domain without coercion.

## Checks actually run

The primary agent ran the frozen existing operator suite as ROOT: **875 tests,
zero skips, zero failures/errors**, in 126.951 seconds. The runner selected every
existing test module and compared source hashes before and after execution.
The new, separately in-progress completion reader and its tests were explicitly
excluded from that frozen selection; this result does not cover that reader.
A reviewed native libpq client was supplied to execute the environment regression
rather than silently skip it. All test storage and fresh Python bytecode-cache
prefixes were under `/tmp`.

Portable reproduction for that source revision:

```sh
sudo -n env TMPDIR=/tmp OPENLEGAL_TEST_LIBPQ=/path/to/reviewed/libpq.so \
  python3 -I -B -X pycache_prefix=/tmp/fresh-operator-cache \
  -m unittest discover -s scripts/storage-stability/tests -p 'test_*.py'
systemd-analyze verify deploy/systemd/openlegal-storage-checkpoint.service \
  deploy/systemd/openlegal-storage-checkpoint.timer
git diff --check
```

The independent reviewer separately reran **40 producer** and **46 manual** tests
without skips. A separate actual harmless setsid-descendant fixture, with TERM
ignored and persistent parent discovery errors, verified actual leader/descendant
drainage and reaping. These tests use synthetic fixed work and private files on
one temporary filesystem; they do not run PostgreSQL, Docker or Kubernetes or
establish independent-media durability. Syntax, whitespace and systemd unit
verification also passed. Unit verification emitted unrelated installed xfs
CPUAccounting warnings.

## Remaining gates

The earlier [failed copy calibration](reviews-storage-stability-preparation-2026-11-02.md#operational-evidence-and-remaining-gates)
remains failed and preserved. It was not retried or cleaned up, and establishes no
throughput, quantile or full-copy fit result. Existing SSD corpus lookup and
stopped collection were preserved; this work performed no operational DDL,
role provisioning, hold release, migration or workload activation.

The historical completion reader, orchestration CLI, separate closed controller
source registry, emergency schema-2 consumer, certificate renewal and pre-start/
pre-release admission remain separate integration work. Full native copy fit,
physical two-copy qualification, immutable images, coherent snapshot/restore,
manual recovery drill, actual outer exit and the six-hour bounded collection
acceptance remain unestablished. No new hosted CI, GHCR or production acceptance
is inferred from these local source tests.
