#!/usr/bin/env bash
# Provision a fixed full dictionary outside the server and ordinary offline tests.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# != 1 || -e "$1" ]]; then
  echo 'usage: scripts/prepare-korean-dictionary.sh NEW_DESTINATION_DIRECTORY' >&2
  exit 2
fi
destination=$(realpath -m -- "$1")
temporary=$(mktemp -d)
trap 'rm -rf -- "$temporary"' EXIT
archive=${MECAB_SOURCE_ARCHIVE:-$temporary/source.tar.gz}
if [[ -z ${MECAB_SOURCE_ARCHIVE:-} ]]; then
  curl --fail --location --proto '=https' --tlsv1.2 --max-time 120 \
    --max-filesize 60000000 --output "$archive" \
    https://lindera.dev/mecab-ko-dic-2.1.1-20180720.tar.gz
fi
archive=$(realpath -- "$archive")
python3 - "$archive" "$temporary" <<'PY'
import hashlib, pathlib, sys, tarfile
archive, temporary = map(pathlib.Path, sys.argv[1:])
if archive.stat().st_size > 60_000_000:
    raise SystemExit('source archive exceeds size limit')
if hashlib.sha256(archive.read_bytes()).hexdigest() != '702ced21c6167e9d9aebc674ab5ee54af58d4443975f2940d37d0567c020591a':
    raise SystemExit('source archive checksum mismatch')
with tarfile.open(archive) as source:
    members = source.getmembers()
    if sum(m.size for m in members) > 512 * 1024 * 1024:
        raise SystemExit('expanded dictionary exceeds size limit')
    if any(not (m.isfile() or m.isdir()) for m in members):
        raise SystemExit('unexpected dictionary archive member')
    source.extractall(temporary, filter='data')
PY
cargo run --locked -p openlegal-adapters --example provision_korean_dictionary -- \
  "$archive" "$temporary/mecab-ko-dic-2.1.1-20180720" "$temporary/artifact"
mkdir -p -- "$(dirname "$destination")"
# Destination publication is explicit and never replaces an existing artifact.
mv -T --no-clobber -- "$temporary/artifact" "$destination"
test ! -e "$temporary/artifact"
