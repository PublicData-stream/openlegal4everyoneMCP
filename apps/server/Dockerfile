# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
# These are multi-platform index digests, not single-architecture manifest digests.
FROM --platform=$BUILDPLATFORM node:24.21.0-trixie-slim@sha256:8ec5d7557396cfe32d21c3f9c13072355ceab22b584578ca4bb28af31120cffe AS widgets
RUN npm install --global --ignore-scripts pnpm@12.3.4
WORKDIR /src
COPY LICENSE ./
COPY apps/widget/package.json apps/widget/pnpm-lock.yaml apps/widget/pnpm-workspace.yaml ./apps/widget/
RUN pnpm --dir apps/widget install --frozen-lockfile
COPY apps/widget/src ./apps/widget/src
COPY apps/widget/tsconfig.json ./apps/widget/
COPY apps/widget/scripts/build.mjs ./apps/widget/scripts/build.mjs
COPY apps/widget/THIRD_PARTY_NOTICES.md ./apps/widget/
RUN pnpm --dir apps/widget build

FROM rust:1.98.1-slim-trixie@sha256:f47a8de237dcbb0b0ce1099901e60a89728e3d51f24e664b40e947171538ade7 AS build
# Native build helpers only; nothing from this toolchain is installed at runtime.
RUN rm -f /etc/apt/sources.list.d/* \
    && printf 'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20260915T000000Z trixie main\n' > /etc/apt/sources.list \
    && apt-get -o APT::Update::Error-Mode=any update \
    && apt-get install -y --no-install-recommends cmake pkg-config python3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock LICENSE ./
COPY .cargo/config.toml ./.cargo/config.toml
COPY crates ./crates
COPY apps/server/Cargo.toml ./apps/server/
COPY apps/server/src ./apps/server/src
COPY apps/server/collect-notices.py ./apps/server/
COPY apps/server/notices ./apps/server/notices
COPY apps/server/tests/test_collect_notices.py ./apps/server/tests/
RUN python3 apps/server/tests/test_collect_notices.py
ENV CARGO_TARGET_DIR=/build
ARG TARGETARCH
ARG BUILD_JOBS=2
RUN --mount=type=cache,id=openlegal-server-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=openlegal-server-build-${TARGETARCH},target=/build,sharing=locked \
    cargo build --locked --release -p openlegal-server -j "$BUILD_JOBS" \
    && mkdir -p /assets/notices \
    && cp /build/release/openlegal-server /assets/openlegal-server \
    && cargo metadata --locked --format-version 1 --filter-platform "$(rustc -vV | sed -n 's/^host: //p')" > /assets/metadata.json \
    && python3 apps/server/collect-notices.py /assets/metadata.json /assets/notices \
    && mkdir -p /assets/notices/rust-standard-library \
    && cp "$(rustc --print sysroot)/share/doc/rust/COPYRIGHT-library.html" /assets/notices/rust-standard-library/ \
    && cp -r "$(rustc --print sysroot)/share/doc/rust/licenses" /assets/notices/rust-standard-library/

FROM debian:trixie-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a AS runtime-base
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN rm -f /etc/apt/sources.list.d/* \
    && printf 'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20260915T000000Z trixie main\n' > /etc/apt/sources.list \
    && apt-get -o APT::Update::Error-Mode=any -o Acquire::https::CaInfo=/etc/ssl/certs/ca-certificates.crt update \
    && apt-get -o Acquire::https::CaInfo=/etc/ssl/certs/ca-certificates.crt install -y --no-install-recommends ca-certificates libgcc-s1 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /etc/openlegal /opt/openlegal/widgets /opt/openlegal/notices
COPY --from=build /assets/openlegal-server /usr/local/bin/openlegal-server
COPY --from=build /assets/notices/ /opt/openlegal/notices/dependencies/
COPY --from=widgets /src/apps/widget/dist/ /opt/openlegal/widgets/
COPY --chmod=0644 LICENSE /opt/openlegal/notices/OPENLEGAL-LICENSE
COPY --chmod=0644 apps/widget/THIRD_PARTY_NOTICES.md /opt/openlegal/notices/WIDGET-NOTICES.md
WORKDIR /opt/openlegal
USER 10004:10004
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/openlegal-server"]
CMD ["/etc/openlegal/server.toml"]

FROM runtime-base AS runtime-amd64
LABEL org.openlegal.cpu-baseline="x86-64-v3"
FROM runtime-base AS runtime-arm64
LABEL org.openlegal.cpu-baseline="generic-arm64"

# Only the two supported CPU architectures have a runtime stage.
FROM runtime-${TARGETARCH} AS runtime
ARG REVISION=unknown
ARG VERSION=development
LABEL org.opencontainers.image.source="https://github.com/PublicData-stream/openlegal4everyoneMCP" \
    org.opencontainers.image.revision="$REVISION" \
    org.opencontainers.image.version="$VERSION" \
    org.opencontainers.image.licenses="AGPL-3.0-only"
