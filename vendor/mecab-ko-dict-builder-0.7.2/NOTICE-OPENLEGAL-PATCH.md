# Local dependency patch

Package: `mecab-ko-dict-builder 0.7.2` (MIT OR Apache-2.0).
Published source: https://static.crates.io/crates/mecab-ko-dict-builder/mecab-ko-dict-builder-0.7.2.crate
Archive SHA-256: `f4bd4c4f58bf8a5f8405f8f68b6a9d9ce969c267d757102efd92abd31e323e88`.
Upstream revision: `6e0d75beb3c1921e8b2cca456497b8e73423e515`.

The only edit to a published file is the normalized `Cargo.toml` zstd
requirement: `0.13` becomes `0.14.3`, admitting the RustSec-fixed compression
dependencies. Rust source, features, package version, upstream attribution,
`Cargo.toml.orig` and `.cargo_vcs_info.json` are preserved.
The published development `Cargo.lock` is omitted; this dependency is resolved
by the application workspace lockfile. This notice is added locally.

The local patch does not change the upstream license. Preserve `LICENSE-MIT`
and `LICENSE-APACHE` when redistributing.
