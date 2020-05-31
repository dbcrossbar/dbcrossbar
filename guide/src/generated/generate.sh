#!/bin/bash
#
# Generate text files to include in the book.

set -euo pipefail

cargo build

function dbxb {
    ../../../target/debug/dbcrossbar --enable-unstable "$@" 2>&1
}

for c in cp count "schema conv"; do
    dbxb $c --help | tail -n +2 > "$(echo "$c" | sed 's/ /_/g')"_help.txt
done

dbxb features > features.txt

for d in bigml bigquery csv gs postgres redshift s3 shopify; do
    dbxb features $d > features_$d.txt
done