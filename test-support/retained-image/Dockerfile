# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
# Test-only tooling; never copied into the production image. Build natively even
# when acceptance runs the production image under another architecture.
FROM rust:1.98.1-slim-trixie@sha256:f47a8de237dcbb0b0ce1099901e60a89728e3d51f24e664b40e947171538ade7 AS build
RUN rm -f /etc/apt/sources.list.d/* \
    && printf 'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20260915T000000Z trixie main\n' > /etc/apt/sources.list \
    && apt-get -o APT::Update::Error-Mode=any update \
    && apt-get install -y --no-install-recommends cmake pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo/config.toml ./.cargo/config.toml
COPY crates ./crates
COPY apps/server/Cargo.toml ./apps/server/
COPY apps/server/src ./apps/server/src
ENV CARGO_TARGET_DIR=/build
ARG TARGETARCH
RUN --mount=type=cache,id=openlegal-server-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=openlegal-server-build-${TARGETARCH},target=/build,sharing=locked \
    cargo build --release --locked -j 2 -p openlegal-adapters --example seed_retained_fixture \
    && cp /build/release/examples/seed_retained_fixture /seed-retained-fixture
FROM debian:trixie-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a
COPY --from=build /seed-retained-fixture /usr/local/bin/seed-retained-fixture
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER 10004:10004
ENTRYPOINT ["/usr/local/bin/seed-retained-fixture"]
