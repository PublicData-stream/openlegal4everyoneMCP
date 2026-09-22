# Supplemental dependency notices

`manifest.json` binds exact package versions to notice files omitted from their
published crate archives. Each file retains its upstream bytes, immutable source
URL and SHA-256. Shared files have one stored copy per upstream revision and
explicit bindings for each package. These records supplement notices collected
from the resolved crate sources; they are not a claim of comprehensive legal
compliance or an alternative to dependency admission.

For 20 packages, the revision comes from the published crate's
`.cargo_vcs_info.json`. Review covered the root and package-subdirectory notice
paths at that revision. Apache DataSketches includes both `LICENSE` and `NOTICE`;
Tantivy includes the `AUTHORS` file referenced by its license. The historical
`httlib-huffman` metadata omits `path_in_vcs`; its `huffman` directory was confirmed
by matching the upstream Git blob against the packaged `Cargo.toml.orig`.

`htmlescape 0.3.1` has neither VCS metadata nor a separate upstream license or
notice file. Its 12 packaged files (excluding the Cargo cache marker) exactly
match the Git blobs at `1699b539179798e705ad8464128492a0a0092876`. Its unchanged
`Cargo.toml` supplies the published author attribution and declaration of
Apache-2.0 / MIT / MPL-2.0 alternatives. This image selects Apache-2.0 and supplies
the standard license text from the Apache Software Foundation's website repository
at the recorded immutable revision. That text is not represented as a missing
htmlescape-authored file, and no copyright statement has been invented.

`crc-catalog 2.5.0` needs no supplement: its archive already contains
`LICENSES/Apache-2.0.txt` and `LICENSES/MIT.txt`. Collectors must include notice
directories as well as conventional notice basenames.

When dependencies change, inspect the exact locked package archive and its source
revision. Preserve relevant root and package-specific licenses, notices and
referenced attribution files. Add or update explicit version bindings and source
digests; do not infer an upstream notice from an SPDX identifier. Check all stored
digests, rerun the image notice collector and image acceptance, and obtain the
required independent review. Keep unavailable provenance explicit and fail the
collection if an omitted notice has no reviewed supplement.
