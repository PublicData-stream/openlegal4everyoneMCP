# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
FROM rust:1.98.1-alpine3.24@sha256:7cc1c22d77d9432f7fe012a70e6d3e555af54c2a6832700ed7d553f1769ae89f AS toolchain
# Native musl build; APK signatures and branch admission are enforced by the helper.
RUN --mount=type=bind,source=scripts/install-alpine-packages.sh,target=/tmp/install-alpine-packages.sh \
    timeout -k 10 900 /bin/sh /tmp/install-alpine-packages.sh \
        ca-certificates curl git build-base binutils cmake clang-dev pkgconf openssl-dev \
        tesseract-ocr-dev leptonica-dev fontconfig-dev coreutils findutils tar
# The document worker remains amd64-only with the repository CPU baseline.
RUN test "$(apk --print-arch)" = x86_64
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=-crt-static" \
    RUSTDOCFLAGS="-C target-cpu=x86-64-v3 -C target-feature=-crt-static"
FROM toolchain AS build
RUN rustup component add clippy rustfmt
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    cargo install cargo-audit --version 0.22.2 --locked -j 2 \
    && cargo install cargo-deny --version 0.20.2 --locked -j 2
WORKDIR /src
COPY Cargo.toml Cargo.lock LICENSE ./
COPY .cargo/config.toml ./.cargo/config.toml
COPY crates ./crates
COPY apps/server ./apps/server
COPY apps/document-worker ./apps/document-worker
ENV CARGO_TARGET_DIR=/build
ARG ADMISSION_REFRESH=manual
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked --mount=type=cache,target=/usr/local/cargo/git,sharing=locked --mount=type=cache,id=openlegal-worker-musl-rust-1.98.1-amd64,target=/build,sharing=locked \
    export LIBCLANG_PATH="$(dirname "$(find /usr/lib -name libclang.so -print -quit)")" \
    && test -f "$LIBCLANG_PATH/libclang.so" \
    && test -n "$ADMISSION_REFRESH" \
    && cargo fmt --manifest-path apps/document-worker/Cargo.toml --check \
    && cargo audit --file apps/document-worker/Cargo.lock \
    && cargo deny --manifest-path apps/document-worker/Cargo.toml --config apps/document-worker/deny.toml check \
    && cargo clippy --manifest-path apps/document-worker/Cargo.toml --locked --all-targets -j 2 -- -D warnings \
    && cargo test --manifest-path apps/document-worker/Cargo.toml --locked --release --lib -j 2 \
    && find /build/release/deps -maxdepth 1 -type f -name 'binary_formats-*' -executable -delete \
    && cargo test --manifest-path apps/document-worker/Cargo.toml --locked --release --test binary_formats --no-run -j 2 \
    && cargo build --manifest-path apps/document-worker/Cargo.toml --locked --release -j 2 \
    && readelf -l /build/release/openlegal-document-worker | grep -F '[Requesting program interpreter: /lib/ld-musl-x86_64.so.1]' \
    && ! readelf -d /build/release/openlegal-document-worker | grep -F 'libc.so.6' \
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

FROM alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6 AS runtime
COPY --from=toolchain /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN --mount=type=bind,source=scripts/install-alpine-packages.sh,target=/tmp/install-alpine-packages.sh \
    timeout -k 10 900 /bin/sh /tmp/install-alpine-packages.sh \
        tesseract-ocr leptonica fontconfig libgcc libstdc++ \
    && mkdir -p /scratch /opt/probe /opt/fonts /opt/tessdata /opt/notices /usr/share/fonts/truetype/nanum \
    && chown 65532:65532 /scratch /opt/probe \
    && apk info -v | LC_ALL=C sort > /opt/notices/alpine-packages.txt
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
ARG REVISION=unknown
ARG VERSION=development
LABEL org.opencontainers.image.source="https://github.com/PublicData-stream/openlegal4everyoneMCP" \
    org.opencontainers.image.revision="$REVISION" \
    org.opencontainers.image.version="$VERSION" \
    org.opencontainers.image.licenses="AGPL-3.0-only" \
    org.openlegal.cpu-baseline="x86-64-v3"
