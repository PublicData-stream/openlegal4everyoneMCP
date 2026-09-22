#!/bin/sh
# Build-only Debian packages. Invoke under timeout; callers own the package list.
set -eu

if [ "$#" -eq 0 ]; then
    echo 'usage: install-snapshot-packages.sh PACKAGE...' >&2
    exit 2
fi
# No options, local archives or alternate repositories through this interface.
for package do
    case "$package" in
        ''|[!a-z0-9]*|*[!a-z0-9+.-]*)
            echo "Invalid package name: $package" >&2
            exit 2
            ;;
    esac
done

apt_snapshot() {
    apt-get \
        -o Acquire::Retries=5 \
        -o Acquire::http::Timeout=30 \
        -o Acquire::https::Timeout=30 \
        -o Acquire::http::Pipeline-Depth=0 \
        -o Acquire::https::Pipeline-Depth=0 \
        -o Acquire::https::CaInfo=/etc/ssl/certs/ca-certificates.crt \
        -o APT::Update::Error-Mode=any \
        "$@"
}

# Keep package selection reproducible; never fall back to a moving mirror.
rm -f /etc/apt/sources.list.d/*
printf '%s\n' 'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20260915T000000Z trixie main' > /etc/apt/sources.list
apt_snapshot update
apt_snapshot install -y --no-install-recommends "$@"
rm -rf /var/lib/apt/lists/*
