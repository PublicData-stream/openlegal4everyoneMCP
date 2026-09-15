# Rustls 0.23.45 maintenance review

Date: 2026-09-15. Base revision:
`313fc03a4052189b7f2a355d5adca3a08c4b7dfe`.

## Finding and patch

Current advisory checks identified
[RUSTSEC-2026-0285](https://rustsec.org/advisories/RUSTSEC-2026-0285) in the existing
Rustls 0.23.44 lockfile entry. The maintainer separately authorized the smallest
upgrade while the search-query processor was being validated. The patch changes
only Rustls's locked version/checksum to 0.23.45, adds a retained TLS regression,
and documents dependency admission. No manifest, feature, or other package changes
are included. No advisory exception or security-control relaxation is used.

Peer-supplied handshake bytes reach the shared Rustls package through
wtransport/Quinn on incoming QUIC, reqwest on outbound HTTPS, and SQLx on configured
PostgreSQL TLS. The native WebTransport client also uses that package. Private
HTTP serving remains plaintext behind the independently built OxiBelt edge; this
workspace upgrade does not change that edge project's dependencies.

The security invariant is that handshake messages cannot cross encryption-key
boundaries within a TLS record. The previous deframer treated pending complete
handshake messages as aligned; a complete plaintext message following ServerHello
could therefore be accepted. Rustls 0.23.45 updates alignment after extracting each
message and requires no pending complete or partial messages. The same processing
path is used by QUIC handshake input. Transcript authentication remains intact;
this finding does not establish handshake forgery or certificate-verification
bypass. See [dependency admission](dependencies.md#rustls-security-maintenance-2026-09-15).

## Reproduction and ordered verification

The retained `apps/server/tests/tls.rs` creates synthetic certificates and TLS1.3
connections entirely in memory. It obtains a real ServerHello, appends an empty
EncryptedExtensions handshake message in plaintext to that same record, and fixes
the record length. The security assertion expects
`PeerMisbehaved(KeyEpochWithPendingFragment)`.

1. Before the upgrade, `cargo test -p openlegal-server --test tls --locked` failed
   the complete-message test: the old library returned `Ok(())`. The partial-message
   rejection and valid-handshake controls passed (one failed, two passed).
2. After the upgrade, the same command passed all three tests. Both ring and
   AWS-LC are explicitly exercised. Complete and partial extra plaintext handshake
   data are rejected; a normally separated TLS1.3 handshake and application-data
   transfer succeed with certificate verification enabled.
3. Final workspace formatting, Clippy, tests, and advisory/license admission passed.
   The workspace run passed 171 tests including doctests, with 32 PostgreSQL tests
   explicitly ignored by the ordinary suite. These counts include the separately
   reviewed search-query processor patch present during validation.
4. `scripts/test-postgres.sh` passed all 32 database tests, including trusted-CA
   success, wrong-CA rejection, and hostname-mismatch rejection.
5. The OxiBelt HTTP/WebTransport gate passed both MCP revisions (`2026-07-28` and
   `2025-11-25`), including progress, resources, comparison lifecycle, and certificate
   rejection. Its intentional invalid-access/certificate probes returned errors as
   expected; the complete gate exited successfully and cleaned up its fixtures.

Final baseline commands:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo audit
cargo deny check
cargo build --workspace --bins --examples --locked
scripts/test-postgres.sh
scripts/test-oxibelt.sh
git diff --check
```

The edge-gate run set `OXIBELT_SOURCE` to an existing local reference clone; the
portable command above fetches and archives the same pinned revision when that
override is absent. The local run still archived immutable revision
`72564d165dfd05cb29a64aeebd19fccd7944ea6f`. The gate uses synthetic transport HTML
fixtures rather than built React assets. No live legal provider or deployed service
was contacted.

Both `cargo audit` and `cargo deny check` initially failed on the old Rustls
advisory. On the final graph, advisories, bans, licenses, and sources pass;
pre-existing duplicate-version diagnostics remain warnings. Cargo's upgrade
command also rewrote the tempfile-to-getrandom dependency edge without changing
package versions. Independent review caught that unrelated change; it was restored
and the final baseline was rerun successfully.

## Independent review

Codex agent `rustls_investigation` independently traced the affected boundary and
compatibility requirements before the patch. A fresh agent `rustls_patch_review`
then performed a read-only Security Review of the actual candidate, direct callers,
and cached upstream source changes. The corrected patch had no concrete surviving
bypass or introduced runtime regression identified. The reviewer did not implement
the patch or run tests; the parent owns the execution evidence above. This is not
a human GitHub approval or a full-repository security assessment.

Reviewed SHA-256 contents, excluding this evidence document:

```text
0b46042855aa0eb7f04944807c422ea54af0b889ed6e97131218748032ee0f00  Cargo.lock
d799b44f826de24a634f430e388107eb7363c6182fe6423c554510e2a78a1077  apps/server/tests/tls.rs
4d6bb98e8c2f847e957ab256679624ffd5989dbbaafc25599ef362365a259b57  docs/dependencies.md
```

## Limits

The malicious regression operates at the shared TLS library boundary; it does not
directly inject malicious QUIC traffic or cover every HelloRetryRequest change in
this upstream release. Normal native QUIC and database compatibility are tested
separately. Fixture results do not establish deployed exposure, hosted runner
topology, live ChatGPT compatibility, or security of the separately pinned edge.
