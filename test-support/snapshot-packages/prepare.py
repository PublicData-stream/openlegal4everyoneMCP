#!/usr/bin/env python3
"""Create disposable, synthetic APT evidence; never download Debian packages."""

import datetime
import gzip
import hashlib
import os
from pathlib import Path
import subprocess
import sys


def run(*args):
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL)


def main():
    os.umask(0o022)
    output = Path(sys.argv[1]).resolve()
    output.mkdir(parents=True, exist_ok=True)
    package = output / "package"
    (package / "DEBIAN").mkdir(parents=True)
    (package / "DEBIAN/control").write_text(
        "Package: openlegal-snapshot-fixture\nVersion: 1.0\nArchitecture: all\n"
        "Maintainer: Fixture <fixture@example.invalid>\n"
        "Description: Synthetic snapshot retry fixture\n"
    )
    marker = package / "usr/share/openlegal-snapshot-fixture/installed"
    marker.parent.mkdir(parents=True)
    marker.write_text("synthetic snapshot fixture installed\n")
    archive = output / "repository/archive/debian/20260915T000000Z"
    pool = archive / "pool/main/o/openlegal-snapshot-fixture"
    pool.mkdir(parents=True)
    deb = pool / "openlegal-snapshot-fixture_1.0_all.deb"
    run("dpkg-deb", "--root-owner-group", "--build", str(package), str(deb))
    package_index = (
        (package / "DEBIAN/control").read_text()
        + f"Filename: {deb.relative_to(archive)}\nSize: {deb.stat().st_size}\n"
        + f"SHA256: {hashlib.sha256(deb.read_bytes()).hexdigest()}\n\n"
    ).encode()
    distribution = archive / "dists/trixie"
    for architecture in ("amd64", "arm64"):
        directory = distribution / f"main/binary-{architecture}"
        directory.mkdir(parents=True)
        (directory / "Packages").write_bytes(package_index)
        (directory / "Packages.gz").write_bytes(gzip.compress(package_index, mtime=0))
    release = (
        "Origin: Openlegal snapshot fixture\nLabel: Openlegal snapshot fixture\n"
        "Suite: trixie\nCodename: trixie\nArchitectures: amd64 arm64\nComponents: main\n"
        f"Date: {datetime.datetime.now(datetime.timezone.utc):%a, %d %b %Y %H:%M:%S +0000}\n"
        "SHA256:\n"
    )
    for path in sorted(distribution.glob("main/binary-*/*")):
        release += (
            f" {hashlib.sha256(path.read_bytes()).hexdigest()} {path.stat().st_size} "
            f"{path.relative_to(distribution)}\n"
        )
    (distribution / "Release").write_text(release)
    key_home = output / "gnupg"
    key_home.mkdir(mode=0o700)
    gpg = ("gpg", "--homedir", str(key_home), "--batch", "--pinentry-mode", "loopback")
    try:
        run(*gpg, "--passphrase", "", "--quick-gen-key",
            "Snapshot fixture <fixture@example.invalid>", "rsa2048", "sign", "1d")
        run(*gpg, "--output", str(output / "fixture.gpg"), "--export")
        run(*gpg, "--output", str(distribution / "InRelease"), "--clearsign",
            str(distribution / "Release"))
        run(*gpg, "--output", str(distribution / "Release.gpg"), "--detach-sign",
            str(distribution / "Release"))
    finally:
        run("gpgconf", "--homedir", str(key_home), "--kill", "gpg-agent")
    run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-keyout", str(output / "server.key"), "-out", str(output / "ca.crt"),
        "-subj", "/CN=snapshot.debian.org",
        "-addext", "subjectAltName=DNS:snapshot.debian.org")


if __name__ == "__main__":
    main()
