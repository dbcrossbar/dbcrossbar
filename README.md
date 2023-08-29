# `dbcrossbar`: Copy tabular data between databases, CSV files and cloud storage

[![Build Status](https://travis-ci.org/dbcrossbar/dbcrossbar.svg)](https://travis-ci.org/dbcrossbar/dbcrossbar)

- [Documentation][docs]
- [Releases][releases]
- [Changelog](./CHANGELOG.md)

[docs]: https://www.dbcrossbar.org/
[releases]: https://github.com/dbcrossbar/dbcrossbar/releases

`dbcrossbar` moves large data sets between different databases and storage formats.

Some examples:

```sh
# Copy from a CSV file to a PostgreSQL table.
dbcrossbar cp \
    --if-exists=overwrite \
    --schema=postgres-sql:my_table.sql \
    csv:my_table.csv \
    'postgres://postgres@127.0.0.1:5432/postgres#my_table'

# Upsert from a PostgreSQL table to BigQuery.
dbcrossbar cp \
    --if-exists=upsert-on:id \
    --temporary=gs://$GS_TEMP_BUCKET \
    --temporary=bigquery:$GCLOUD_PROJECT:temp_dataset \
    'postgres://postgres@127.0.0.1:5432/postgres#my_table' \
    bigquery:$GCLOUD_PROJECT:my_dataset.my_table
```

It can also convert between table schema formats, including PostgreSQL `CREATE TABLE` statements and BigQuery JSON schemas:

```sh
# Convert a PostgreSQL `CREATE TABLE` statement to a BigQuery JSON schema.
dbcrossbar schema conv postgres-sql:my_table.sql bigquery-schema:my_table.json

# Extract a schema from a CSV file and convert to Postgres `CREATE TABLE`.
dbcrossbar schema conv csv:data.csv postgres-sql:schema.sql
```

For more information, see the [documentation][docs].

## Contributing

For more instructions about building `dbcrossbar`, running tests, and contributing code, see [CONTRIBUTING.md](./CONTRIBUTING.md).

We require nightly Rust. We lock a specific version of nightly Rust using the [`rust-toolchain`](./rust-toolchain) file. If you want to update this, take a look at [Rustup components history](https://mexus.github.io/rustup-components-history/) and choose the newest version with support for `rls`, `clippy` and `rustfmt`.

## Running integration tests

You can run the regular test suite with `cargo test`, but to run the full integration tests, you'll need to do the following:

```sh
# Run a local PostgreSQL on port 5432.
docker run --name postgres -e POSTGRES_PASSWORD= -p 5432:5432 -d mdillon/postgis
createdb -h localhost -U postgres -w dbcrossbar_test
export POSTGRES_TEST_URL=postgres://postgres:@localhost:5432/dbcrossbar_test
echo "create extension if not exists postgis;" | psql $POSTGRES_TEST_URL
echo "create extension if not exists citext;" | psql $POSTGRES_TEST_URL
echo "create schema if not exists testme1;" | psql $POSTGRES_TEST_URL

# Point to test databases and test buckets.
export GS_TEST_URL=gs://$MY_GS_TEST_BUCKET/dbcrossbar/
export BQ_TEST_DATASET=$MY_BQ_ROOT:test
export S3_TEST_URL=s3://$MT_S3_TEST_BUCKET/dbcrossbar/

# This helps to ensure that we're not depending on our users to have set
# a default gcloud project anywhere.
gcloud config unset project

# These can be omitted if you don't want to test Redshift.
export REDSHIFT_TEST_URL=redshift://user:pass@server:port/db
export REDSHIFT_TEST_IAM_ROLE=$MY_IAM_ROLE
export REDSHIFT_TEST_REGIION=$MY_AWS_REGION

# Needed for BigML. Does not work with AWS_SESSION_TOKEN.
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
export BIGML_USERNAME=... BIGML_API_KEY=...

# Run the integration tests.
env RUST_BACKTRACE=1 RUST_LOG=warn,dbcrossbar=debug \
    cargo test --all -- --ignored --nocapture
```

## License

Licensed under either of:

- Apache License, Version 2.0, ([LICENSE-APACHE.txt](./LICENSE-APACHE.txt) or [on the web](http://www.apache.org/licenses/LICENSE-2.0))
- MIT license ([LICENSE-MIT.txt](./LICENSE-MIT.txt) or [on the web](http://opensource.org/licenses/MIT))

...at your option.

### Contributions

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above without any additional terms or conditions.

### Third-party libraries

`dbcrossbar` depends on a variety of third-party libraries, [each with their own copyright and license](./dbcrossbar/ALL_LICENSES.html). We have configured a [`deny.toml` file](./deny.toml) that currently attempts to restrict our dependencies to MIT, Apache-2.0, BSD-3-Clause, BSD-2-Clause, CC0-1.0, ISC, OpenSSL and Zlib licenses, with a single MPL-2.0 dependency. But please verify the `deny.toml` file (and individual dependencies) to be certain, because details may change in the future. Each of these licenses imposes certain obligations on redistribution.
