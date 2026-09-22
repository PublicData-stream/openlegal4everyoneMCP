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
RUN --mount=type=bind,source=scripts/install-snapshot-packages.sh,target=/tmp/install-snapshot-packages.sh \
    timeout --kill-after=10s 15m /bin/sh /tmp/install-snapshot-packages.sh cmake pkg-config python3
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
RUN --mount=type=bind,source=scripts/install-snapshot-packages.sh,target=/tmp/install-snapshot-packages.sh \
    timeout --kill-after=10s 15m /bin/sh /tmp/install-snapshot-packages.sh ca-certificates libgcc-s1 \
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
FROM runtime-${TARGETARCH} AS runtime-serving
ARG REVISION=unknown
ARG VERSION=development
LABEL org.opencontainers.image.source="https://github.com/PublicData-stream/openlegal4everyoneMCP" \
    org.opencontainers.image.revision="$REVISION" \
    org.opencontainers.image.version="$VERSION" \
    org.opencontainers.image.licenses="AGPL-3.0-only"

# Download on the builder architecture; only verified artifacts reach runtime.
FROM --platform=$BUILDPLATFORM node:24.21.0-trixie-slim@sha256:8ec5d7557396cfe32d21c3f9c13072355ceab22b584578ca4bb28af31120cffe AS kubectl
ARG TARGETARCH
COPY scripts/deployment-kubectl.sha256 /pins/kubectl.sha256
RUN mkdir /artifacts && TARGETARCH="$TARGETARCH" node --input-type=module -e '\
    import {readFileSync, writeFileSync} from "node:fs"; \
    import {createHash} from "node:crypto"; \
    const arch = process.env.TARGETARCH; \
    if (!["amd64", "arm64"].includes(arch)) throw Error("Unsupported architecture"); \
    const pin = readFileSync("/pins/kubectl.sha256", "utf8").split("\n").find(line => line.endsWith("  " + arch))?.split(" ")[0]; \
    if (!/^[a-f0-9]{64}$/.test(pin ?? "")) throw Error("Missing checksum"); \
    const artifacts = [ \
      ["kubectl", `https://dl.k8s.io/release/v1.37.0/bin/linux/${arch}/kubectl`, pin], \
      ["kubernetes.tar.gz", "https://codeload.github.com/kubernetes/kubernetes/tar.gz/refs/tags/v1.37.0", "956ddae3b12acc08a715aea0411a168a36d5989b3e7185deeb6bf46b7dac19cf"], \
      ["GO-LICENSE", "https://raw.githubusercontent.com/golang/go/go1.26.6/LICENSE", "911f8f5782931320f5b8d1160a76365b83aea6447ee6c04fa6d5591467db9dad"] \
    ]; \
    for (const [name, url, checksum] of artifacts) { \
      const response = await fetch(url, {signal: AbortSignal.timeout(300000)}); \
      if (!response.ok) throw Error(`Download failed: ${response.status}`); \
      const bytes = Buffer.from(await response.arrayBuffer()); \
      if (createHash("sha256").update(bytes).digest("hex") !== checksum) throw Error(`${name} checksum mismatch`); \
      writeFileSync(`/artifacts/${name}`, bytes, {mode: name === "kubectl" ? 0o755 : 0o644}); \
    }' \
    && tar -xzf /artifacts/kubernetes.tar.gz -C /artifacts --wildcards '*/LICENSES/*' \
    && mv /artifacts/kubernetes-1.37.0/LICENSES /artifacts/notices \
    && mv /artifacts/GO-LICENSE /artifacts/notices/GO-LICENSE \
    && find /artifacts/notices -name OWNERS -delete \
    && chmod -R a+rX,go-w /artifacts/notices

FROM runtime-serving AS runtime-ingestion
COPY --from=kubectl --chmod=0755 /artifacts/kubectl /usr/local/bin/kubectl
COPY --from=kubectl /artifacts/notices/ /opt/openlegal/notices/kubectl/
COPY --chmod=0644 apps/server/kubectl-notices.md /opt/openlegal/notices/kubectl/provenance.md

# The final/default target must continue to exclude controller tooling.
FROM runtime-serving AS runtime
