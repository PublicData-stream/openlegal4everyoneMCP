#!/bin/sh
# Run inside a fresh pinned image; all trust additions die with this container.
set -eu
scenario=$1
package=openlegal-snapshot-fixture
if [ "$scenario" != tls ]; then
    cat /fixture/ca.crt >> /etc/ssl/certs/ca-certificates.crt
fi
cp /fixture/fixture.gpg /etc/apt/trusted.gpg.d/openlegal-fixture.gpg
mkdir -p /tmp/fixture-bin

if [ "$scenario" = baseline ]; then
    # The helper remains unchanged. Reproduce the old APT retry allowance only.
    cat > /tmp/fixture-bin/apt-get <<'EOF'
#!/bin/sh
exec /usr/bin/apt-get "$@" -o Acquire::Retries=3
EOF
    chmod +x /tmp/fixture-bin/apt-get
    export PATH="/tmp/fixture-bin:$PATH"
fi

if [ "$scenario" = stale ]; then
    # Seed authentic metadata through the real helper, stopping before install
    # and its success-only cleanup. Subsequent metadata requests return 503.
    cat > /tmp/fixture-bin/apt-get <<'EOF'
#!/bin/sh
for argument do
    if [ "$argument" = install ]; then exit 23; fi
done
exec /usr/bin/apt-get "$@"
EOF
    chmod +x /tmp/fixture-bin/apt-get
    seed_status=0
    PATH="/tmp/fixture-bin:$PATH" timeout --kill-after=10s 2m \
        /bin/sh /fixture/install-snapshot-packages.sh "$package" > /tmp/seed.log 2>&1 || seed_status=$?
    if [ "$seed_status" -ne 23 ]; then cat /tmp/seed.log; exit 1; fi
    found=false
    for file in /var/lib/apt/lists/*InRelease; do
        if [ -s "$file" ]; then found=true; fi
    done
    if [ "$found" != true ]; then echo 'No stale metadata was seeded' >&2; exit 1; fi
    rm /tmp/fixture-bin/apt-get
fi

duration=6m
if [ "$scenario" = deadline ]; then duration=2s; fi
start=$(date +%s)
status=0
timeout --kill-after=10s "$duration" /bin/sh /fixture/install-snapshot-packages.sh \
    "$package" > /tmp/install.log 2>&1 || status=$?
elapsed=$(($(date +%s) - start))
cat /tmp/install.log
printf 'scenario=%s status=%s elapsed=%ss\n' "$scenario" "$status" "$elapsed"

if [ "$scenario" = recovery ]; then
    [ "$status" -eq 0 ]
    [ "$(dpkg-query -W -f='${Status}' "$package")" = 'install ok installed' ]
    [ "$(cat /usr/share/openlegal-snapshot-fixture/installed)" = 'synthetic snapshot fixture installed' ]
    for file in /var/lib/apt/lists/*; do
        [ ! -f "$file" ] || { echo 'Successful install retained metadata' >&2; exit 1; }
    done
else
    # Native APT errors must exhaust their own policy. Neither timeout's 124
    # nor its forced-kill 137 is evidence that APT's retry bounds worked.
    if [ "$scenario" != deadline ]; then [ "$status" -eq 100 ]; fi
    if dpkg-query -W -f='${Status}' "$package" 2>/dev/null | grep -q 'install ok installed'; then
        echo 'Negative fixture unexpectedly installed the package' >&2
        exit 1
    fi
    [ ! -e /usr/share/openlegal-snapshot-fixture/installed ]
    case "$scenario" in
        baseline|persistent|stale)
            grep -q '503' /tmp/install.log ;;
        stall)
            [ "$elapsed" -ge 150 ]
            grep -Eiq 'timed out|timeout' /tmp/install.log ;;
        tls)
            grep -Eiq 'certificate verif(y|ication) failed|certificate.*not trusted' /tmp/install.log ;;
        signature)
            grep -Eiq 'signature|not signed' /tmp/install.log ;;
        hash)
            grep -qi 'Hash Sum mismatch' /tmp/install.log ;;
        deadline)
            [ "$status" -eq 124 ]
            [ "$elapsed" -le 15 ] ;;
        *) echo "Unknown scenario: $scenario" >&2; exit 1 ;;
    esac
fi
