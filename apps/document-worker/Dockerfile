# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
FROM rust:1.98.1-slim-trixie@sha256:a2de23e559fd8afd260d22beb00f3987073ea0dcc2ba2646cccdaeda6a62a095 AS toolchain
# A dated Debian snapshot fixes native Tesseract/Leptonica and compiler libraries.
RUN --mount=type=bind,source=scripts/install-snapshot-packages.sh,target=/tmp/install-snapshot-packages.sh \
    timeout --kill-after=10s 15m /bin/sh /tmp/install-snapshot-packages.sh \
        ca-certificates curl git clang libclang-dev pkg-config libtesseract-dev libleptonica-dev libfontconfig1-dev
FROM toolchain AS build
RUN rustup component add clippy rustfmt
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo install cargo-audit --version 0.22.2 --locked \
    && cargo install cargo-deny --version 0.20.2 --locked
WORKDIR /src
COPY Cargo.toml Cargo.lock LICENSE ./
COPY .cargo/config.toml ./.cargo/config.toml
COPY crates ./crates
COPY apps/server ./apps/server
COPY apps/document-worker ./apps/document-worker
ENV CARGO_TARGET_DIR=/build
ARG ADMISSION_REFRESH=manual
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/usr/local/cargo/git --mount=type=cache,target=/build,sharing=locked \
    test -n "$ADMISSION_REFRESH" \
    && cargo fmt --manifest-path apps/document-worker/Cargo.toml --check \
    && cargo audit --file apps/document-worker/Cargo.lock \
    && cargo deny --manifest-path apps/document-worker/Cargo.toml --config apps/document-worker/deny.toml check \
    && cargo clippy --manifest-path apps/document-worker/Cargo.toml --locked --all-targets -j 2 -- -D warnings \
    && cargo test --manifest-path apps/document-worker/Cargo.toml --locked --release --lib -j 2 \
    && find /build/release/deps -maxdepth 1 -type f -name 'binary_formats-*' -executable -delete \
    && cargo test --manifest-path apps/document-worker/Cargo.toml --locked --release --test binary_formats --no-run -j 2 \
    && cargo build --manifest-path apps/document-worker/Cargo.toml --locked --release -j 2 \
    && mkdir -p /assets/notices \
    && cp /build/release/openlegal-document-worker /assets/openlegal-document-worker \
    && test "$(find /build/release/deps -maxdepth 1 -type f -name 'binary_formats-*' -executable | wc -l)" -eq 1 \
    && find /build/release/deps -maxdepth 1 -type f -name 'binary_formats-*' -executable -exec cp {} /assets/document-format-tests \; \
    && test -x /assets/document-format-tests \
    && find /usr/local/cargo/registry/src /usr/local/cargo/git/checkouts -type f \( -iname "license*" -o -iname "copying*" -o -iname "notice*" \) -exec cp --parents {} /assets/notices \;
WORKDIR /assets
COPY apps/document-worker/assets.sha256 ./
RUN curl -fsSLO https://raw.githubusercontent.com/tesseract-ocr/tessdata_fast/65727574dfcd264acbb0c3e07860e4e9e9b22185/eng.traineddata \
    && curl -fsSLO https://raw.githubusercontent.com/tesseract-ocr/tessdata_fast/65727574dfcd264acbb0c3e07860e4e9e9b22185/kor.traineddata \
    && curl -fsSLO https://raw.githubusercontent.com/notofonts/noto-cjk/523d033d6cb47f4a80c58a35753646f5c3608a78/Sans/OTF/Korean/NotoSansCJKkr-Regular.otf \
    && sha256sum -c assets.sha256 \
    && curl -fsSL https://raw.githubusercontent.com/tesseract-ocr/tessdata_fast/65727574dfcd264acbb0c3e07860e4e9e9b22185/LICENSE -o TESSDATA-LICENSE \
    && curl -fsSL https://raw.githubusercontent.com/notofonts/noto-cjk/523d033d6cb47f4a80c58a35753646f5c3608a78/LICENSE -o NOTO-LICENSE

FROM debian:trixie-slim@sha256:abc9cb88a5587630d7f915f47b23b0668fe250fbfc6457aa4d52b534c1bbf73f AS runtime
COPY --from=toolchain /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN --mount=type=bind,source=scripts/install-snapshot-packages.sh,target=/tmp/install-snapshot-packages.sh \
    timeout --kill-after=10s 15m /bin/sh /tmp/install-snapshot-packages.sh \
        libtesseract5 libleptonica6 libfontconfig1 \
    && mkdir -p /scratch /opt/probe /opt/fonts /opt/tessdata /opt/notices /usr/share/fonts/truetype/nanum \
    && chown 65532:65532 /scratch /opt/probe
COPY --from=build /assets/openlegal-document-worker /usr/local/bin/openlegal-document-worker
COPY --from=build /assets/*.traineddata /opt/tessdata/
COPY --from=build /assets/NotoSansCJKkr-Regular.otf /opt/fonts/
COPY --from=build /assets/*LICENSE /opt/notices/
COPY --from=build /assets/notices /opt/notices/dependencies
COPY LICENSE /opt/notices/OPENLEGAL-LICENSE
# rhwp's fixed fallback pathname, explicitly backed by the pinned Noto font.
RUN ln -s /opt/fonts/NotoSansCJKkr-Regular.otf /usr/share/fonts/truetype/nanum/NanumGothic.ttf
ENV TMPDIR=/scratch TESSDATA_PREFIX=/opt/tessdata OMP_THREAD_LIMIT=2 RAYON_NUM_THREADS=2
USER 65532:65532
WORKDIR /scratch
ENTRYPOINT ["/usr/local/bin/openlegal-document-worker"]
CMD ["--idle"]

# Test executable is confined exactly like the worker, but never shipped in it.
FROM runtime AS fixture-tests
COPY --from=build /assets/document-format-tests /usr/local/bin/document-format-tests
ENTRYPOINT ["/usr/local/bin/document-format-tests"]
CMD ["--ignored", "--test-threads=1"]

FROM runtime AS worker
