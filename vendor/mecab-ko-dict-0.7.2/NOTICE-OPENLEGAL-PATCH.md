# Local dependency patch

Package: `mecab-ko-dict 0.7.2` (MIT OR Apache-2.0).
Published source: https://static.crates.io/crates/mecab-ko-dict/mecab-ko-dict-0.7.2.crate
Archive SHA-256: `93fd443cec048a9ae1c05f9bc081b86e1af181b0d41bd589063ebcc15aa1b936`.
Upstream revision: `6e0d75beb3c1921e8b2cca456497b8e73423e515`.

The only edit to a published file is the normalized `Cargo.toml` zstd
requirement: `0.13` becomes `0.14.3`, admitting the RustSec-fixed compression
dependencies. Rust source, features, package version, upstream attribution,
`Cargo.toml.orig` and `.cargo_vcs_info.json` are preserved.
The published development `Cargo.lock` is omitted; this dependency is resolved
by the application workspace lockfile. This notice is added locally.

The local patch does not change the upstream license. Preserve `LICENSE-MIT`
and `LICENSE-APACHE` when redistributing.
