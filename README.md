# `dbcrossbar`: Tools for converting between database table schemas (WIP)

[![Build Status](https://travis-ci.org/faradayio/dbcrossbar.svg)](https://travis-ci.org/faradayio/dbcrossbar)

This tool is intended to help convert between schema formats. It's still very incomplete. Right now, `dbcrossbar` is most useful for moving data from PostgreSQL to Google's BigQuery.

## What we currently support

We currently support many small pieces which are helpful when converting from PostgreSQL to BigQuery:

- Parsing (some) Postgre `CREATE TABLE` statements, or extracting schemas from `information_schema.columns` in a running database.
- Generating SQL to dump a Postgres table as CSV.
- Generating a "temporary" BigQuery JSON schema allowing the CSV to be loaded into BigQuery.
- Generating BigQuery SQL to tranform the temporary table into a final table, parsing any data types which can't be directly loaded from SQL.
- Generating a BigQuery JSON schema for the final table. This isn't normally necessary, but it's nice to have for the sake of completeness.
- Transforming the schema in several ways.

However, we do not yet have any higher-level interface that just transfers data directly.

## Installation

```sh
# Install Rust compiler.
curl https://sh.rustup.rs -sSf | sh

# Install dbcrossbar.
cargo install -f --git https://github.com/faradayio/dbcrossbar dbcrossbar
```

## Examples

Run `dbcrossbar --help` for more documentation.

```sh
# Given a `postgres:` URL, dump a table schema as JSON.
dbcrossbar "$DATABASE_URL#mytable" > schema.json

# Dump a table schema as BigQuery schema JSON.
dbcrossbar "$DATABASE_URL#mytable" -O bq:schema > bigquery-schema.json

# Ditto, but using PostgreSQL `CREATE TABLE` SQL as input.
dbcrossbar -I pg -O bq:schema < table.sql > bigquery-schema.json

# Dump a table schema as quoted PostgreSQL `SELECT ...` arguments.
dbcrossbar "$DATABASE_URL#mytable" -O pg:select > select-args.txt
```

You can also edit the default schema JSON (generated with no `-O` flag, or with `-O json`), and then run it back through to generate another format:

```sh
dbcrossbar "$DATABASE_URL#mytable" > schema.json
# (Edit schema.json.)

dbcrossbar -O bq < schema.json > bigquery-schema.json
```

## "Interchange" table schemas

In order to make `dbcrossbar` work, we define a "interchange" table schema format using JSON. This format uses a highly-simplied and carefully curated set of column data types that make sense when passing data between databases. This represents a compromise between the richness of PostgreSQL data types, and the relative poverty of BigQuery data types, while still preserving as much information as possible. It includes timestamps, geodata, etc.

Seee [`schema.rs`](./dbcrossbarlib/src/schema.rs) for the details of this "interchange" schema.

## Contributing

For more instructions about building `dbcrossbar`, running tests, and contributing code, see [CONTRIBUTING.md](./CONTRIBUTING.md).

We require nightly Rust. We lock a specific version of nightly Rust using the [`rust-toolchain`](./rust-toolchain) file. If you want to update this, take a look at [Rustup components history](https://mexus.github.io/rustup-components-history/) and choose the newest version with support for `rls`, `clippy` and `rustfmt`.

## Running integration tests

You can run the regular test suite with `cargo test`, but to run the full integration tests, you'll need to do the following:

```sh
# Run a local PostgreSQL on port 5432.
docker run --name postgres -e POSTGRES_PASSWORD= -p 5432:5432 -d postgres
createdb -h localhost -U postgres -w dbcrossbar_test
export POSTGRES_TEST_URL=postgres://postgres:@localhost:5432/dbcrossbar_test

# Point to a Goolge Cloud Storage bucket for which you have write permissions.
export GS_TEST_URL=gs://$MY_TEST_BUCKET/dbcrossbar/

# Run the integration tests.
env RUST_BACKTRACE=1 RUST_LOG=warn,dbcrossbarlib=trace,dbcrossbar=trace \
    cargo test --all -- --ignored
```
