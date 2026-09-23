#!/usr/bin/env bash
# Release-only GHCR publication. The platform image has already passed its smoke gate.
set -euo pipefail

registry=ghcr.io/publicdata-stream
version=${RELEASE_VERSION:?RELEASE_VERSION is required}
revision=${RELEASE_REVISION:?RELEASE_REVISION is required}
[[ $version =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-beta\.[1-9][0-9]*|-build\.[0-9a-f]{8})?$ ]] || {
    echo 'Invalid release version' >&2; exit 2;
}
[[ $revision =~ ^[0-9a-f]{40}$ ]] || { echo 'Invalid release revision' >&2; exit 2; }

login() {
    [[ -n ${GHCR_TOKEN:-} ]] || { echo 'GHCR token is missing' >&2; exit 1; }
    printf '%s' "$GHCR_TOKEN" | docker login ghcr.io --username "$GITHUB_ACTOR" --password-stdin >/dev/null
}

# Exit 1 means absent; all other errors fail closed.
registry_manifest() {
    local error_file=$1 ref=$2 response
    if response=$(docker buildx imagetools inspect "$ref" --format '{{json .Manifest}}' 2>"$error_file"); then
        printf '%s\n' "$response"
    elif grep -Eqi 'manifest unknown|404|not found' "$error_file"; then
        return 1
    else
        cat "$error_file" >&2
        return 2
    fi
}

manifest_digest() {
    jq -er '.digest | select(test("^sha256:[0-9a-f]{64}$"))'
}

check_remote_platform() {
    local ref=$1 expected_arch=$2 expected_config=${3:-} image_config manifest
    manifest=$(docker manifest inspect "$ref")
    [[ $(jq -er '.schemaVersion' <<<"$manifest") == 2 ]]
    image_config=$(jq -er '.config.digest | select(test("^sha256:[0-9a-f]{64}$"))' <<<"$manifest")
    if [[ -n $expected_config && $image_config != "$expected_config" ]]; then
        echo "$ref has config $image_config, expected $expected_config" >&2
        exit 1
    fi
    docker buildx imagetools inspect "$ref" --format '{{json .Image}}' |
        jq -e --arg arch "$expected_arch" --arg version "$version" --arg revision "$revision" '
            .os == "linux" and .architecture == $arch
            and .config.Labels["org.opencontainers.image.source"] == "https://github.com/PublicData-stream/openlegal4everyoneMCP"
            and .config.Labels["org.opencontainers.image.revision"] == $revision
            and .config.Labels["org.opencontainers.image.version"] == $version
            and .config.Labels["org.opencontainers.image.licenses"] == "AGPL-3.0-only"
        ' >/dev/null
}

push_platform() {
    local package=$1 arch=$2 image image_json media_type local_config error_file existing status remote digest
    case "$package:$arch" in
        openlegal-server:amd64|openlegal-server:arm64|openlegal-server-ingestion:amd64|openlegal-server-ingestion:arm64|openlegal-document-worker:amd64) ;;
        *) echo 'Unsupported image target or architecture' >&2; exit 2 ;;
    esac
    image="$registry/$package:$version-$arch"
    image_json=$(docker image inspect "$image")
    # Containerd exposes the manifest as .Id; classic Docker exposes the config as .Id.
    media_type=$(jq -r '.[0].Descriptor.mediaType // ""' <<<"$image_json")
    case "$media_type" in
        '') local_config=$(jq -er '.[0].Id' <<<"$image_json") ;;
        application/vnd.oci.image.manifest.v1+json|application/vnd.docker.distribution.manifest.v2+json)
            local_config=$(jq -er '.[0].Descriptor.annotations["config.digest"]' <<<"$image_json") ;;
        *) echo "Local release image is not a single manifest: $media_type" >&2; exit 1 ;;
    esac
    [[ $local_config =~ ^sha256:[0-9a-f]{64}$ ]] || { echo 'Local image ID is invalid' >&2; exit 1; }
    [[ $(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image") == "linux/$arch" ]]
    login
    error_file=$(mktemp)
    if existing=$(registry_manifest "$error_file" "$image"); then
        echo "Refusing to replace existing architecture tag $image" >&2
        exit 1
    else
        status=$?
        (( status == 1 )) || exit "$status"
        docker push "$image"
    fi
    rm -f -- "$error_file"
    remote=$(docker buildx imagetools inspect "$image" --format '{{json .Manifest}}')
    digest=$(manifest_digest <<<"$remote")
    check_remote_platform "$image" "$arch" "$local_config"
    printf 'Published tested %s at %s@%s\n' "$arch" "$registry/$package" "$digest"
    if [[ -n ${GITHUB_OUTPUT:-} ]]; then printf 'digest=%s\n' "$digest" >>"$GITHUB_OUTPUT"; fi
}

publish_index() {
    local package=$1 amd64=$2 arm64=$3
    local ref="$registry/$package:$version" error_file existing status expected actual digest
    expected=$(jq -cn --arg amd "$amd64" --arg arm "$arm64" \
        '[{os:"linux",arch:"amd64",digest:$amd},{os:"linux",arch:"arm64",digest:$arm}] | sort_by(.arch)')
    error_file=$(mktemp)
    if existing=$(registry_manifest "$error_file" "$ref"); then
        actual=$(jq -c '[.manifests[] | {os:.platform.os,arch:.platform.architecture,digest:.digest}] | sort_by(.arch)' <<<"$existing")
        [[ $actual == "$expected" ]] || { echo "Refusing to replace $ref" >&2; exit 1; }
    else
        status=$?
        (( status == 1 )) || exit "$status"
        docker buildx imagetools create --tag "$ref" \
            "$registry/$package:$version-amd64" "$registry/$package:$version-arm64" >/dev/null
    fi
    rm -f -- "$error_file"
    actual=$(docker buildx imagetools inspect "$ref" --format '{{json .Manifest}}')
    [[ $(jq -c '[.manifests[] | {os:.platform.os,arch:.platform.architecture,digest:.digest}] | sort_by(.arch)' <<<"$actual") == "$expected" ]] || {
        echo "Published index $ref does not match tested platforms" >&2; exit 1;
    }
    digest=$(manifest_digest <<<"$actual")
    printf '%s\n' "$digest"
}

publish_worker() {
    local child=$1 ref="$registry/openlegal-document-worker:$version" error_file existing status digest
    error_file=$(mktemp)
    if existing=$(registry_manifest "$error_file" "$ref"); then
        [[ $(manifest_digest <<<"$existing") == "$child" ]] || { echo "Refusing to replace $ref" >&2; exit 1; }
    else
        status=$?
        (( status == 1 )) || exit "$status"
        docker buildx imagetools create --prefer-index=false --tag "$ref" \
            "$registry/openlegal-document-worker:$version-amd64" >/dev/null
    fi
    rm -f -- "$error_file"
    digest=$(docker buildx imagetools inspect "$ref" --format '{{json .Manifest}}' | manifest_digest)
    [[ $digest == "$child" ]] || { echo 'Worker version tag changed manifest digest' >&2; exit 1; }
    printf '%s\n' "$digest"
}

assemble() {
    local server_amd server_arm ingestion_amd ingestion_arm worker_amd server_digest ingestion_digest worker_digest
    local output=${RELEASE_DIGESTS_FILE:?RELEASE_DIGESTS_FILE is required}
    local source_url=${RELEASE_SOURCE_URL:?RELEASE_SOURCE_URL is required}
    local kind=${RELEASE_KIND:?RELEASE_KIND is required}
    [[ $source_url == "https://github.com/PublicData-stream/openlegal4everyoneMCP/archive/$revision.tar.gz" ]]
    case "$kind" in stable|beta|build) ;; *) echo 'Invalid release kind' >&2; exit 2;; esac
    login
    for package in openlegal-server openlegal-server-ingestion; do
        for arch in amd64 arm64; do
            check_remote_platform "$registry/$package:$version-$arch" "$arch"
        done
    done
    check_remote_platform "$registry/openlegal-document-worker:$version-amd64" amd64
    server_amd=$(docker buildx imagetools inspect "$registry/openlegal-server:$version-amd64" --format '{{json .Manifest}}' | manifest_digest)
    server_arm=$(docker buildx imagetools inspect "$registry/openlegal-server:$version-arm64" --format '{{json .Manifest}}' | manifest_digest)
    ingestion_amd=$(docker buildx imagetools inspect "$registry/openlegal-server-ingestion:$version-amd64" --format '{{json .Manifest}}' | manifest_digest)
    ingestion_arm=$(docker buildx imagetools inspect "$registry/openlegal-server-ingestion:$version-arm64" --format '{{json .Manifest}}' | manifest_digest)
    worker_amd=$(docker buildx imagetools inspect "$registry/openlegal-document-worker:$version-amd64" --format '{{json .Manifest}}' | manifest_digest)
    server_digest=$(publish_index openlegal-server "$server_amd" "$server_arm")
    ingestion_digest=$(publish_index openlegal-server-ingestion "$ingestion_amd" "$ingestion_arm")
    worker_digest=$(publish_worker "$worker_amd")
    jq -n --arg version "$version" --arg revision "$revision" --arg kind "$kind" --arg source "$source_url" \
        --arg server "$server_digest" --arg ingestion "$ingestion_digest" --arg worker "$worker_digest" \
        --arg server_amd "$server_amd" --arg server_arm "$server_arm" \
        --arg ingestion_amd "$ingestion_amd" --arg ingestion_arm "$ingestion_arm" --arg worker_amd "$worker_amd" '
        {schemaVersion:1,version:$version,revision:$revision,kind:$kind,sourceUrl:$source,
         images:{server:{name:"ghcr.io/publicdata-stream/openlegal-server",digest:$server,
                         platforms:{amd64:$server_amd,arm64:$server_arm}},
                 ingestion:{name:"ghcr.io/publicdata-stream/openlegal-server-ingestion",digest:$ingestion,
                            platforms:{amd64:$ingestion_amd,arm64:$ingestion_arm}},
                 documentWorker:{name:"ghcr.io/publicdata-stream/openlegal-document-worker",digest:$worker,
                                 platforms:{amd64:$worker_amd}}}}
        ' >"$output"
    if [[ -n ${GITHUB_OUTPUT:-} ]]; then
        printf 'server_digest=%s\ningestion_digest=%s\nworker_digest=%s\n' \
            "$server_digest" "$ingestion_digest" "$worker_digest" >>"$GITHUB_OUTPUT"
    fi
}

case ${1:-} in
    push-platform) [[ $# == 3 ]] || exit 2; push_platform "$2" "$3" ;;
    assemble) [[ $# == 1 ]] || exit 2; assemble ;;
    *) echo 'Usage: publish_ghcr_images.sh push-platform PACKAGE ARCH | assemble' >&2; exit 2 ;;
esac
