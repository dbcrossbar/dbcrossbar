#!/bin/bash
#
# Generate text files to include in the book.

set -euo pipefail

cargo build

function dbxb {
    ../../../target/debug/dbcrossbar "$@" 2>&1
}

for c in cp count conv; do
    dbxb $c --help > ${c}_help.txt
done

dbxb features > features.txt

for d in bigml bigquery csv gs postgres redshift s3; do
    dbxb features $d > features_$d.txt
done