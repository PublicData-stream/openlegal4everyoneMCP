# Historical epoch completion reader source review, 2026-11-02

The recording commit is discoverable with
`git log --diff-filter=A --format=%H -- docs/reviews-epoch-completion-reader-2026-11-02.md`.
The reviewed dependencies include the signed initial producer
`cd0334585b5c5eda264c07f9557e1af0ced5f30d`. Current behavior belongs to the
[operator guide](../scripts/storage-stability/README.md#historical-completion-read-port)
and [persistence contract](persistence.md#strict-two-copy-durability).

## Scope and independent review

`epoch_completion.read_completed_install` is a limited historical read port for
initial-bind generation1. It receives held archive/source/control root FDs and
expected operation, producer and whole-source-graph hashes. It validates actual
FD descriptors/path associations, one filesystem/mount profile, relative
no-follow paths, retained file/directory identities, private metadata, raw hashes
and bounded unique record accounting. Supplied root descriptors and locks remain
caller-owned on success and failure.

Fixed branches connect the certificate and producer authority, first-pair bind,
seed references, actual observation/install child exit records, native SQL output
and command receipts, strict TOML/template mappings, final SQL closure and dual
installation/readback inode history. Actual source emitters generate the fixture
SQL wrappers. Ancillary predecessor authority, role, maintenance and inventory
records are authenticated data and cross-linked; the reader does not replay all
prior ACL, provider or snapshot workflows. A source-owned controller must still
qualify those dependencies for its actual operation.

A separate Codex reviewer inspected the module, tests, affected producer schemas
and README under Security Review and Upstream/Cache Correctness Review. The final
limited historical read-port source-preparation scope was cleared with no open
findings at these exact identities:

| File under `scripts/storage-stability/` | SHA-256 |
| --- | --- |
| `epoch_completion.py` | `6f6ab13b5b706a53c67ee18cce754bf1b1a6faa50004821b94982d9b259e4170` |
| `tests/test_epoch_completion.py` | `3723c27e8d848fd66cebcc1edc4bf1e82eaf75e60eb5ccdc1bee4da2ff8b38c3` |

Review corrections preserve host-only IP prefixes, storage metadata ranges,
64KiB native stream limits distinct from the16MiB general metadata limit,
original ROOT parent/deadline association, and exact historical control-root
metadata. The historical producer source profile has27 files; the legacy
checkpoint dispatch profile remains15. No archive/provenance absolute-path open,
live loader invocation, native command dispatch or mutation is provided.
This agent source review is not a plugin scan or human GitHub approval.

## Checks actually completed

The author ran17 focused tests as ROOT with zero skips in3.380 seconds. The
independent reviewer reran the frozen17 tests with zero skips in4.349 seconds
and checked source/test/README hashes before and after. Coverage includes a
complete source-generated synthetic success bundle, actual temporary-file/FD
reads and replacement, symlink/hardlink/ownership/mode rejection, byte/record
limits, command receipts, bool child exits, install/readback inodes, original
ROOT/deadline and unavailable-HDD descriptor negatives.

```sh
sudo -n env TMPDIR=/tmp python3 -I -B \
  -X pycache_prefix=/tmp/fresh-completion-cache \
  -m unittest discover -s scripts/storage-stability/tests -p 'test_epoch_completion.py'
```

All disposable data and fresh bytecode-cache prefixes were under `/tmp`.
Syntax and whitespace checks passed. These are source-generated synthetic SQL
receipts and filesystem fixtures; no actual PostgreSQL/Docker/Kubernetes,
independent physical media or production acceptance was exercised.

## Full integrated operator gate

The final frozen integrated run selected all32 operator test modules, including
this reader and the initial producer: **892 tests, zero skips, zero failures and
zero errors**, in114.992 seconds. All66 Python source/test file hashes matched
before and after execution. The root run supplied a freshly hash-verified native
libpq client and a new `/tmp` bytecode-cache prefix. Syntax, whitespace and
checkpoint systemd unit verification passed; unit verification reported unrelated
installed xfs CPUAccounting warnings.

The first integrated run is preserved as a failure:892 tests with one error in
the unchanged synthetic HDD short-actor fixture. A runner-level `umask077` made
its requested0755 gate directory0700, producing `start_gate_rebound`. The same
single fixture reproduced failure under077 and success under022. No production
or fixture source was changed. The successful full rerun used baseline022 and
pre-created its log as0600 separately. Neither run executed an operational HDD
measurement or retried the failed deployment-host calibration.

```sh
umask 022
sudo -n env TMPDIR=/tmp OPENLEGAL_TEST_LIBPQ=/path/to/reviewed/libpq.so \
  python3 -I -B -X pycache_prefix=/tmp/fresh-integrated-cache \
  -m unittest discover -s scripts/storage-stability/tests -p 'test_*.py'
systemd-analyze verify deploy/systemd/openlegal-storage-checkpoint.service \
  deploy/systemd/openlegal-storage-checkpoint.timer
git diff --check
```

## Remaining boundaries

The reader returns historical raw data. It cannot observe outer ROOT exit,
current HEAD, current health, Ready or public ACK. Different mounts, requalified
roots after reboot and replaced-source escrow require separate reviewed profiles.
The reader never opens an unavailable HDD, recreates a journal, rewrites a
reference, copies escrow or chooses an older certificate. Historical approval
is not current execution authority.

Actual initial orchestration, the closed controller graph/authority profile,
emergency schema-2 consumption, renewal and one-use admission before starts or
hold release remain separate integration work. The failed HDD calibration is
preserved and still establishes no full-copy fit. This work does not deploy,
create roles, apply DDL, release holds, activate collection, retry the operational
measurement or complete the six-hour collection acceptance. Hosted CI, GHCR and
operational evidence remain distinct gates.
