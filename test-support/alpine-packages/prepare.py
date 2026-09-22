#!/usr/bin/env python3
"""Generate an isolated signed APK v2 repository with fictional package data."""
import base64
import gzip
import hashlib
import io
import os
from pathlib import Path
import subprocess
import sys
import tarfile


def run(*args):
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL)


def archive(files, terminate=True):
    # APK v2 signature/control gzip members form one tar stream. Only the final
    # member has tar end markers; signing covers the compressed control member.
    stream = io.BytesIO()
    directories = set()
    for name in files:
        for parent in reversed(Path(name).parents):
            if str(parent) != '.' and str(parent) not in directories:
                info = tarfile.TarInfo(str(parent) + '/')
                info.type = tarfile.DIRTYPE
                info.mode = 0o755
                stream.write(info.tobuf(format=tarfile.USTAR_FORMAT))
                directories.add(str(parent))
    for name, body in files.items():
        info = tarfile.TarInfo(name)
        info.size = len(body)
        info.pax_headers = {'APK-TOOLS.checksum.SHA1': hashlib.sha1(body).hexdigest()}
        info.mode = 0o644
        stream.write(info.tobuf(format=tarfile.PAX_FORMAT))
        stream.write(body)
        stream.write(b'\0' * (-len(body) % 512))
    if terminate:
        stream.write(b'\0' * 1024)
    return gzip.compress(stream.getvalue(), mtime=0)


def main():
    os.umask(0o022)
    output = Path(sys.argv[1]).resolve()
    output.mkdir(parents=True, exist_ok=True)
    key = output / 'fixture.rsa'
    run('openssl', 'genrsa', '-out', str(key), '2048')
    run('openssl', 'rsa', '-in', str(key), '-pubout', '-out', str(output / 'fixture.rsa.pub'))

    def signed(body, corrupt=False):
        signature = subprocess.run(
            ['openssl', 'dgst', '-sha1', '-sign', str(key)],
            input=body, capture_output=True, check=True,
        ).stdout
        if corrupt:
            signature = bytes([signature[0] ^ 1]) + signature[1:]
        return archive({'.SIGN.RSA.fixture.rsa.pub': signature}, False) + body

    data = archive({'usr/share/openlegal-alpine-fixture/installed': b'synthetic Alpine fixture installed\n'})
    control = archive({'.PKGINFO': (
        'pkgname = openlegal-alpine-fixture\npkgver = 1.0-r0\n'
        'pkgdesc = Synthetic Alpine retry fixture\nurl = https://example.invalid\n'
        'builddate = 0\npackager = Fixture\nsize = 4096\narch = noarch\nlicense = MIT\n'
        f'datahash = {hashlib.sha256(data).hexdigest()}\n'
    ).encode()}, False)
    package = signed(control) + data
    checksum = base64.b64encode(hashlib.sha1(control).digest()).decode()
    index = (
        f'C:Q1{checksum}\nP:openlegal-alpine-fixture\nV:1.0-r0\nA:noarch\n'
        f'S:{len(package)}\nI:4096\nT:Synthetic Alpine retry fixture\n'
        'U:https://example.invalid\nL:MIT\n\n'
    ).encode()
    for arch in ('x86_64', 'aarch64', 'noarch'):
        for component in ('main', 'community'):
            repo = output / f'repository/alpine/v3.24/{component}/{arch}'
            repo.mkdir(parents=True)
            index_archive = archive({
                'DESCRIPTION': b'Openlegal synthetic fixture\n',
                'APKINDEX': index if component == 'main' else b'',
            })
            (repo / 'APKINDEX.tar.gz').write_bytes(signed(index_archive))
            (repo / 'bad-index.tar.gz').write_bytes(signed(index_archive, corrupt=True))
            if component == 'main':
                (repo / 'openlegal-alpine-fixture-1.0-r0.apk').write_bytes(package)
                bad_data = archive({'usr/share/openlegal-alpine-fixture/installed': b'CORRUPTED Alpine fixture installed\n'})
                (repo / 'bad-package.apk').write_bytes(signed(control) + bad_data)
    run('openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
        '-keyout', str(output / 'server.key'), '-out', str(output / 'ca.crt'),
        '-subj', '/CN=dl-cdn.alpinelinux.org',
        '-addext', 'subjectAltName=DNS:dl-cdn.alpinelinux.org')


if __name__ == '__main__':
    main()
