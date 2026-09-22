#!/bin/sh
# Run inside a fresh pinned image; all trust additions die with this container.
set -eu
scenario=$1
package=openlegal-alpine-fixture
if [ "$scenario" != tls ]; then
    cat /fixture/ca.crt >> /etc/ssl/certs/ca-certificates.crt
fi
cp /fixture/fixture.rsa.pub /etc/apk/keys/

if [ "$scenario" = stale ]; then
    # Seed valid native metadata in the normal APK cache; the helper must neither
    # reuse it nor install from it when both subsequent refreshes fail.
    apk --timeout 30 update > /tmp/seed.log 2>&1 || { cat /tmp/seed.log; exit 1; }
    found=false
    for file in /var/cache/apk/APKINDEX.*.tar.gz; do
        if [ -s "$file" ]; then found=true; fi
    done
    [ "$found" = true ] || { echo 'No stale metadata was seeded' >&2; exit 1; }
fi

duration=15m
if [ "$scenario" = deadline ]; then duration=2s; fi
start=$(date +%s)
status=0
timeout -k 10s "$duration" /bin/sh /fixture/install-alpine-packages.sh \
    "$package" > /tmp/install.log 2>&1 || status=$?
elapsed=$(($(date +%s) - start))
cat /tmp/install.log
printf 'scenario=%s status=%s elapsed=%ss\n' "$scenario" "$status" "$elapsed"

case "$scenario" in
    baseline|recovery)
        [ "$status" -eq 0 ]
        apk --no-network info -e "$package" >/dev/null
        [ "$(cat /usr/share/openlegal-alpine-fixture/installed)" = 'synthetic Alpine fixture installed' ]
        if [ "$scenario" = recovery ]; then
            [ "$(grep -c 'Retrying APK' /tmp/install.log)" -eq 4 ]
            [ "$elapsed" -ge 15 ]
        fi
        grep -qx 'openlegal-alpine-fixture-1.0-r0' /usr/share/openlegal/alpine-packages.txt
        ;;
    *)
        # BusyBox timeout exits with signal status (143), unlike GNU timeout.
        if [ "$scenario" != deadline ]; then
            [ "$status" -gt 0 ] && [ "$status" -lt 124 ]
        fi
        if apk --no-network info -e "$package" >/dev/null 2>&1; then
            echo 'Negative fixture unexpectedly installed the package' >&2
            exit 1
        fi
        [ ! -e /usr/share/openlegal-alpine-fixture/installed ]
        case "$scenario" in
            tls|signature|hash|unknown|mixed)
                if grep -q 'Retrying APK' /tmp/install.log; then
                    echo 'Terminal trust/integrity failure was retried' >&2; exit 1
                fi ;;
        esac
        case "$scenario" in
            persistent|stale)
                [ "$(grep -c 'Retrying APK' /tmp/install.log)" -eq 5 ]
                [ "$elapsed" -ge 31 ]
                grep -Eiq '503|remote server returned error|Service Unavailable' /tmp/install.log ;;
            stall)
                [ "$elapsed" -ge 720 ] && [ "$elapsed" -le 830 ]
                grep -Eiq 'timed out|timeout' /tmp/install.log ;;
            tls)
                grep -Eiq 'certificate|TLS' /tmp/install.log ;;
            signature|mixed)
                grep -Eiq 'signature|UNTRUSTED|BAD signature' /tmp/install.log ;;
            unknown)
                grep -q 'HTTP 404' /tmp/install.log ;;
            hash)
                grep -Eiq 'integrity|checksum|BAD archive|IO ERROR' /tmp/install.log ;;
            deadline)
                [ "$status" -eq 143 ] || [ "$status" -eq 137 ]
                [ "$elapsed" -le 15 ] ;;
            *) echo "Unknown scenario: $scenario" >&2; exit 1 ;;
        esac
        ;;
esac
