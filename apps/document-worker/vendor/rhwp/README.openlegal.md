# Vendored rhwp library

Source: [rhwp v0.8.7](https://github.com/edwardkim/rhwp/releases/tag/v0.8.7),
commit `1a76570e833917d15817415a53c09ad61ab3203f`.
The upstream MIT license and third-party notices are retained alongside the source.
This dependency keeps its upstream licenses; the surrounding project's AGPL license
is not a relicensing of these files.

The import contains the complete `src` tree, the three required internal crates,
`build.rs`, the library manifest and notices, and `saved/blank2010.hwp`. The latter is
an upstream embedded blank-document template required by
`DocumentCore::create_blank_document_native`; it is dependency source data, not a
legal-provider capture or a claim of independently established synthetic provenance.
Upstream bulk samples, regression documents, browser assets, fonts, unrelated
applications and tools are excluded. The worker retains its separately pinned
Noto font and Tesseract models. Upstream tests referring to excluded fixtures are
not run; this project's fictional format fixtures validate the worker integration.

`UPSTREAM.json` records every imported file's original Git blob identity and
SHA-256, plus the local modifications. `SHA256SUMS` records the effective imported
source and provenance files; the worker gate and image build verify it before
compilation. To verify the inventory from this directory, run:

```sh
sha256sum -c SHA256SUMS
```

Local changes are deliberately limited:

- The root manifest describes the library and three internal workspace crates;
  exact internal path-dependency versions are declared; unrelated binary, example
  and upstream test targets, development dependencies,
  release profiles and the ignored nested `svg2pdf` patch are removed. The nested
  patch was not active when rhwp was a Git dependency of this worker either.
- Root and password-crypto ZIP dependencies require `=9.0.5`, replacing the
  vulnerable 8.x line. ZIP 9 filenames decode fallibly: existing typed parser,
  password and diagnostic errors now propagate decoding failures; package checks
  report them, thumbnails return no result, and optional threat scans record their
  existing incomplete-scan diagnostic. No caller supplies a default or lossy name.
  The exact source compatibility edits are listed in `UPSTREAM.json`.
- The worker consumes this directory as a path dependency, with default features
  disabled. No runtime or build-time source download is added.

The upstream build script checks local render-backend file availability and emits
Rust cfg values; it does not download assets. Revisit this source selection and
patch on every rhwp or ZIP update, preserving the upstream notices and rerunning
standalone dependency admission and the confined document-worker image gate.
