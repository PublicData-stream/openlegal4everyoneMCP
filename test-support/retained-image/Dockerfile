# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
# Test-only tooling; never copied into the production image. Build natively even
# when acceptance runs the production image under another architecture.
FROM rust:1.98.1-alpine3.24@sha256:7cc1c22d77d9432f7fe012a70e6d3e555af54c2a6832700ed7d553f1769ae89f AS build
# Share verified, bounded acquisition with the production image builds.
RUN --mount=type=bind,source=scripts/install-alpine-packages.sh,target=/tmp/install-alpine-packages.sh \
    timeout -k 10 900 /bin/sh /tmp/install-alpine-packages.sh build-base binutils cmake pkgconf
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo/config.toml ./.cargo/config.toml
COPY crates ./crates
COPY vendor ./vendor
COPY apps/server/Cargo.toml ./apps/server/
COPY apps/server/src ./apps/server/src
ENV CARGO_TARGET_DIR=/tmp/rust/target
ARG TARGETARCH
RUN --mount=type=cache,id=openlegal-server-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=openlegal-retained-musl-rust-1.98.1-${TARGETARCH},target=/tmp/rust/target,sharing=locked \
    case "$TARGETARCH" in \
        amd64) export RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=-crt-static"; interpreter=/lib/ld-musl-x86_64.so.1 ;; \
        arm64) export RUSTFLAGS="-C target-feature=-crt-static"; interpreter=/lib/ld-musl-aarch64.so.1 ;; \
        *) exit 1 ;; \
    esac \
    && cargo build --release --locked -j 2 -p openlegal-adapters --example seed_retained_fixture \
    && readelf -l /tmp/rust/target/release/examples/seed_retained_fixture | grep -F "[Requesting program interpreter: $interpreter]" \
    && ! readelf -d /tmp/rust/target/release/examples/seed_retained_fixture | grep -F 'libc.so.6' \
    && cp /tmp/rust/target/release/examples/seed_retained_fixture /seed-retained-fixture
FROM alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6
RUN --mount=type=bind,source=scripts/install-alpine-packages.sh,target=/tmp/install-alpine-packages.sh \
    timeout -k 10 900 /bin/sh /tmp/install-alpine-packages.sh ca-certificates libgcc
COPY --from=build /seed-retained-fixture /usr/local/bin/seed-retained-fixture
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN ldd /usr/local/bin/seed-retained-fixture > /tmp/ldd 2>&1 \
    && cat /tmp/ldd \
    && ! grep -Eq 'not found|Error loading|Error relocating|libc\.so\.6' /tmp/ldd \
    && rm /tmp/ldd
USER 10004:10004
ENTRYPOINT ["/usr/local/bin/seed-retained-fixture"]
