#!/bin/sh
# Build-only Alpine packages. Callers enforce timeout -k 10s 15m.
set -eu
export LC_ALL=C

if [ "$#" -eq 0 ]; then
    echo 'usage: install-alpine-packages.sh PACKAGE...' >&2
    exit 2
fi
for package do
    case "$package" in
        ''|[!a-z0-9]*|*[!a-z0-9+._-]*)
            echo "Invalid package name: $package" >&2
            exit 2 ;;
    esac
done

# Explicit repositories-file excludes repositories.d as well. Each invocation
# starts with an empty private cache, so pre-existing metadata cannot be reused.
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir "$work/cache"
printf '%s\n' \
    'https://dl-cdn.alpinelinux.org/alpine/v3.24/main' \
    'https://dl-cdn.alpinelinux.org/alpine/v3.24/community' > "$work/repositories"
cp "$work/repositories" /etc/apk/repositories

retry_apk() {
    attempt=1
    delay=1
    while :; do
        status=0
        # Acquisition only: never interrupt/retry a package installation. APK's
        # native inactivity timeout does not bound every TLS-header stall.
        timeout -k 10s 120s apk --repositories-file "$work/repositories" --cache-dir "$work/cache" \
            --timeout 30 --no-progress "$@" > "$work/output" 2>&1 || status=$?
        cat "$work/output"
        if [ "$status" -eq 0 ]; then return 0; fi
        # Only recognized transport diagnostics are retryable. Every diagnostic
        # must match, so a simultaneous trust/integrity/unknown error is terminal.
        diagnostic_status=0
        awk '
            /^(WARNING|ERROR):/ {
                seen=1
                if ($0 !~ /: (temporary error \(try again later\)|remote server returned error \(try again later\)|HTTP 50[234]:.*|operation timed out|Operation timed out|Connection timed out|Connection reset by peer|DNS: transient error)$/) bad=1
            }
            END { exit (bad ? 1 : (!seen ? 2 : 0)) }
        ' "$work/output" || diagnostic_status=$?
        # Unrelated signal failures (including possible OOM SIGKILL) are terminal.
        if [ "$status" -ge 128 ] && [ "$status" -ne 143 ]; then return "$status"; fi
        if { [ "$status" -eq 143 ] || [ "$status" -eq 124 ]; } && [ "$diagnostic_status" -ne 1 ]; then
            echo 'APK acquisition watchdog timed out after 120s' >&2
            status=1
        elif [ "$diagnostic_status" -ne 0 ]; then
            return "$status"
        fi
        if [ "$attempt" -ge 6 ]; then return "$status"; fi
        echo "Retrying APK after transient transport failure ($attempt/6; ${delay}s)" >&2
        sleep "$delay"
        delay=$((delay * 2))
        attempt=$((attempt + 1))
    done
}

retry_apk --force-refresh update
# Metadata admitted above is fresh for this bounded transaction. Do not refresh
# during add: a failed metadata request must never turn into stale acceptance.
retry_apk --cache-max-age 60 cache --add-dependencies download "$@"
# Installation cannot issue network requests, and every installation error is
# terminal. A killed/failed acquisition never reaches this operation.
apk --no-network --repositories-file "$work/repositories" --cache-dir "$work/cache" \
    --no-progress add "$@"
mkdir -p /usr/share/openlegal
apk --no-network --repositories-file "$work/repositories" --cache-dir "$work/cache" info -v > /usr/share/openlegal/alpine-packages.txt
cat /usr/share/openlegal/alpine-packages.txt
