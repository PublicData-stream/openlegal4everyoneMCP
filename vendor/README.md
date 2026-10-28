# Local third-party dependency patches

These directories contain the published crates.io `mecab-ko-dict` and
`mecab-ko-dict-builder` 0.7.2 sources. The root Cargo workspace patches both
packages locally and excludes them from first-party workspace membership.

The only modification to each published file set is its normalized `Cargo.toml`
zstd requirement, from `0.13` to `0.14.3`. The published development lockfiles are
omitted because the root application lockfile owns dependency resolution.
Each package's `NOTICE-OPENLEGAL-PATCH.md` records the verified archive URL,
SHA-256, upstream revision and exact changes. Both upstream licenses are retained.

The patch selects `zstd 0.14.3`, `zstd-safe 8.2.0` and
`zstd-sys 2.1.2+zstd.1.5.7` through the root lockfile to address
RUSTSEC-2026-0391, RUSTSEC-2026-0398, RUSTSEC-2026-0403,
RUSTSEC-2026-0410 and the associated zstd-safe unsoundness advisories.
MeCab 0.7.2's `zstd ^0.13` requirement cannot select these fixed releases.

Keep runtime features, tokenizer source and dictionary format unchanged. Replace
these patches with an official crates.io release once it admits the fixed graph
and passes dictionary compatibility, dependency admission, notice collection and
image checks. Review any further local source modification independently.
