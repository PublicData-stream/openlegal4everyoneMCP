#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
temporary=$(mktemp -d)
trap 'rm -rf -- "$temporary"' EXIT
if [[ -z ${OPENLEGAL_TEST_MECAB_DICTIONARY:-} ]]; then
  scripts/prepare-korean-dictionary.sh "$temporary/dictionary"
  export OPENLEGAL_TEST_MECAB_DICTIONARY="$temporary/dictionary"
fi
cargo test -p openlegal-adapters --locked korean_analysis::tests::full_dictionary -- --ignored --nocapture
