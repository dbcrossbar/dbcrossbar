# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) for the `dbcrossbar` CLI tool. (The `dbcrossbarlib` crate is an internal-only dependency with no versioning policy at this time.)

## 0.4.2-beta.8 - 2020-10-16

### Fixed

- Linux: Fix Linux binary builds by updating to latest `rust-musl-builder` release, which has the new `cargo-deny`.

## 0.4.2-beta.7 - 2020-10-14

### Added

- shopify: Added a "partner" argument which can be used to include a "-- partner:" comment in all generated RedShift SQL for use by RedShift partners.

## 0.4.2-beta.6 - 2020-09-15

### Fixed

- shopify: Retry failed downloads a few times. We've been seeing some intermittent failures.

## 0.4.2-beta.5 - 2020-08-01

### Fixed

- gcloud: We now print more useful error messages when Google doesn't send JSON-formatted errors.
- gcloud: We now retry Google Cloud GET requests automatically a few times if it looks like it might help. We'd also love to retry POST requests, but that will require the ability to try to restart streams.

## 0.4.2-beta.4 - 2020-07-07

### Changed

- Update dependencies. The latest `bigml` release contains tweaks to error retry behavior.

## 0.4.2-beta.3 - 2020-07-07

### Changed

- postgres: Our last `diesel` code has been removed, and replaced with `tokio-postgres` (which we use elsewhere).

### Fixed

- postgres: Fixed [#148](https://github.com/dbcrossbar/dbcrossbar/issues/148) to improve support for PostGIS under PostgreSQL 12.

### Removed

- The experimental `citus`-related APIs have been removed from `dbcrossbarlib`, because they used `diesel`. This is technically a breaking change for `dbcrosslib`, but we don't claim to honor semver for `dbcrossbarlib` 0.x.y releases.

## 0.4.2-beta.2 - 2020-06-28

### Added

- redshift: Support `--if-exists=upsert-on:key1,key2`.
- redshift: Enable `--if-exists=error`.

### Changed

- postgres: Temporary tables now use the same schema (i.e. namespace) as the tables they're linked to. This shouldn't be a breaking change unless you've set up your database permissions to forbid it.

### Fixed

- postgres: Fixed likely bug upserting into tables with a non-"public" schema.
- postgres: Verify that upsert columns are NOT NULL to prevent possible incorrect upserts. This may be a breaking change, but it also prevents a possible bug.

## 0.4.2-beta.1 - 2020-06-23

### Changed

- Mac: Move configuration directory from `~/Library/Preferences/dbcrossbar` to `~/Library/Application Support/dbcrossbar`. If we detect a config directory in the old location, we should print a deprecation warning and use it.
- Many dependencies have been updated.

### Fixed

- We should now handle multiple sets of Google Cloud OAuth2 credentials correctly.

## 0.4.1 - 2020-06-16

A bug fix to `gs`, and other minor improvements.

### Changed

- Replace deprecated `tempdir` with `tempfile`.

### Fixed

- gs: Correctly pass `page_token` when listing. This prevents an infinite loop in large directories.
- Fix new Rust 0.44.0 warnings.

## 0.4.0 - 2020-06-02

This is a summary of all the major changes since the 0.3.3 release. For more details and minor changes, see the individual CHANGELOG entries for the 0.4.0 preleases.

### Added

- `dbcrossbar` now supports "struct" types, which have a fixed set of named fields. These will be automatically translated to BigQuery STRUCT types or to JSON columns, depending on the destination database.
- We now support a CLI-editable config file using commands like `dbcrossbar config add temporary s3://example/temp/`.
- Parsing-related error messages should now include context.
- bigquery: Users can now specify billing labels for jobs.
- `dbcrossbar license` will display the licences for all dependencies.
- Unstable features can now be hidden behind the `--enable-unstable` flag, including two new drivers:
  - UNSTABLE: We now support specifying schemas using a subset of TypeScript.
  - UNSTABLE: We now support reading data from Shopify's REST API. This is a testbed for new struct and JSON-related features.

### Changed

- `dbcrossbar conv` is now `dbcrossbar schema conv`.
- Because of the new STRUCT support, some corner cases involving struct types and JSON may have changed subtly.
- We replaced `gcloud auth`, `gsutil` and `bq` with native Rust. This simplifies installation and configuration substantially, and fixes a number of BigQuery-related issues.
- AWS credentials must now always be passed via `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (optional) and `AWS_DEFAULT_REGION` (required). This lays the groundwork for replacing the `aws` CLI tool with native Rust code, so that we will someday be able to remove our last CLI dependency.

### Fixed

- Lots of issues.

### Removed

- The data type `{ "other": string }` has been removed from the portable schema format. It was not actually generated by any of our drivers.
- bigquery: We now export `ARRAY<STRUCT<...>>` as `{ "array": { "struct": ... } }`, instead of exporting it as as single `"json"` value.

## 0.4.0-rc.2 - 2020-06-01

### Changed

- postgres: We now transform the portable types `{ "array": "json" }` and `{ "array": { "struct": fields } }` into `jsonb[]`, instead of automatically changing it to plain `jsonb` in an effort to help our users.

## 0.4.0-rc.1 - 2020-05-31

This is a release candidate for v0.4.0. If no issues are discovered, this will be published as 0.4.0.

This release contains a last few breaking changes that we want to include before we publicize `dbcrossbar` more widely. When migrating, particular attention to the `conv` subcommand and `AWS_DEFAULT_REGION` below, which have significant breaking changes.

### Changed

- Rename `dbcrossbar conv` to `dbcrossbar schema conv`.
- s3: Require `AWS_DEFAULT_REGION` instead of optionally using `AWS_REGION`. This is more compatiable with the `aws` CLI command, and it doesn't rely on undocumented region defaults or `aws` configuration files.

### Documented

- Document our portable schema format.
- Document schema-only drivers.
- Improve the documentation in other minor ways.

### Removed

- Remove `DataType::Other(String)`, which was not actually used by any of our drivers.

## 0.4.0-beta.1 - 2020-05-28

We're finally ready to start preparing for an 0.4.0 release! This beta will be deployed to several production systems to help verify that there are no surprising regressions.

### Changed

- gs: We now verify CRC32C checksums when uploading.
- gs: We specify `isGenerationMatch` on many operations to make sure that nothing has been created or overridden that we didn't expect.

## 0.4.0-alpha.7 - 2020-05-26

This release adds support for labeling BigQuery jobs.

### Added

- bigquery: Optionally specify billing labels for jobs. See the manual for details.
- Allow driver argument names to be specified as either `x.y` or `x[y]`, interchangeably. This makes `job_labels` look nicer.
- Hide URL passwords from (most) logs using a dedicated wrapper type.

### Changed

- We now have test cases that make sure we catch duplicate driver arguments and raise an error.
- redshift: Authentication argument names may no longer include `-` characters. I'm not even sure whether these are valid, but they won't work with the new scheme for parsing driver arguments.
- `DriverArguments::from_cli_args` now takes an iterator instead of a slice.

## 0.4.0-alpha.6 - 2020-05-22

This release improves the example `shopify.ts` schema, and adds new features to `dbcrossbar-ts` to parse it.

### Added

- dbcrossbar-ts:
  - Parse `/* */` comments.
  - Allow `Date` to be used as a type. This requires the date to be a string in ISO 8601 format, including a time zone.
  - Allow `decimal`, `int16`, `int32` and `int64` to be defined as any of `number`, `string`, `number | string` or `string | number`. This allows the schema to more accurately represent what appears on the wire. It allows `decimal` values to be represented as a mix of floats and strings, which is seen in Shopify.
- postgres-sql: Use new format for parse errors.

### Fixed

- shopify: The example `shopify.ts` schema has been updated to use `Date` and `int64` in many places. `Address` and `CustomerAddress` are now distinct types, and several other minor issues have been fixed.

## 0.4.0-alpha.5 - 2020-05-21

### Added

- BigQuery: Support `--if-exists=error`.

### Changed

- Require `--enable-unstable` to use `dbcrossbar-ts` or `shopify` locators, which are unstable.
- AWS credentials must now always be passed via `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (optional) and `AWS_REGION` (optional). This lays the groundwork for replacing the `aws` CLI tool with native Rust code, so that we will someday be able to remove our last CLI dependency.

## 0.4.0-alpha.4 - 2020-05-19

### Added

- BigQuery now imports and exports decimal (aka NUMERIC) values everywhere.
- The `dbcrossbar-ts` driver now supports magic type aliases that will convert to the corresponding `dbcrossbar` types:
  - `type decimal = string;`
  - `type int16 = number | string;`
  - `type int32 = number | string;`
  - `type int64 = number | string;`

### Changed

- The sample `shopify.ts` schema now uses `decimal` instead of `string` when appropriate. It does not attempt to use `int64` yet.

## 0.4.0-alpha.3 - 2020-05-19 0 YANKED

This release was yanked because it was missing several things it should have included.

## 0.4.0-alpha.2 - 2020-05-19

This is a significant release, with support for "struct" types.

### Added

- The portable schema now supports a `DataType::Struct(fields)` type that can be used to represent BigQuery STRUCT values (as long as they have unique, named fields) and JSON objects with known keys.
- The BigQuery driver now supports importing and exporting `STRUCT` fields using the new `DataType::Struct(fields)` type.
- EXPERIMENTAL: Schemas can now be specified using the `dbcrossbar-ts` driver, which supports subset of TypeScript type declarations. This is useful for specifying complex, nested structs. This can be used as `--schema="dbcrossbar-ts:shopify.ts#Order"`, where `Order` is the name of the type within the `*.ts` file to use as the table's type.
- EXPERIMENTAL: We now support a Shopify input driver that uses the Shopify REST API. See the manual for details.
- We now have support for fancy parser error messages, which we use with the `dbcrossbar-ts` parser.
- We now support a CLI-editable config file using commands like `dbcrossbar config add temporary s3://example/temp/`.

### Changed

- BREAKING: Some corner cases involving struct types and JSON may have changed subtly.
- We've upgraded to the latest `rust-peg` parser syntax everywhere.

### Fixed

- `--if-exists=overwrite` now overwrites when writing to local files (instead of appending).
- We automatically create `~/.local/share` if it does not exist.
- More `clippy` warnings have been fixed, and unsafe code has been forbidden.
- Various obsolete casting libraries have been removed.

## 0.4.0-alpha.1 - 2020-04-07

### Changed

- Replace `gcloud auth`, `gsutil` and `bq` with native Rust. This changes how we authenticate to Google Cloud. In particular, we now support `GCLOUD_CLIENT_SECRET`, `~/.config/dbcrossbar/gcloud_client_secret.json`, `GCLOUD_SERVICE_ACCOUNT_KEY` or `~/.config/dbcrossbar/gcloud_service_account_key.json`, as [explained in the manual](https://www.dbcrossbar.org/gs.html#configuration--authentication). We no longer use `gcloud auth`, and the Google Cloud SDK tools are no longer required. In the current alpha version, uploads and deletions are probably slower than before.

### Fixed

- gs: Avoid download stalls when backpressure is applied ([#103](https://github.com/dbcrossbar/dbcrossbar/issues/102)).
- bigquery: Display error messages more reliably ([#110](https://github.com/dbcrossbar/dbcrossbar/issues/110)).
- bigquery: Detect "\`" quotes in the CLI form of table names, and report an error.

## 0.3.3 - 2020-03-30

### Added

- BigML: Honor BIGML_DOMAIN, allowing the user to point the BigML driver to a custom VPC instance of BigML.

## 0.3.2 - 2020-03-30

### Fixed

- Correctly quote BigQuery column names again (which regressed in 0.3.0), and added test cases to prevent further regressions.
- Fix an error that caused `bigquery_upsert` test to fail.

## 0.3.1 - 2020-03-29

### Added

- Write a new [manual](https://www.dbcrossbar.org/)!

### Changed

- Encapsulate all calls to `bq` and `gsutil`
- Improve performance of `--stream-size`

### Fixed

- BigQuery: Honor NOT NULL on import (fixes #45)

## 0.3.0 - 2020-03-26

### Added

- Use `cargo deny` to enforce license and duplicate dependency policies
- Add notes about license and contribution policies

### Changed

- Update to tokio 0.2 and the latest stable Rust
- Replace `wkb` with `postgis` for licensing reasons
- BigML: Fail immediately if no S3 temporary bucket provided (fixes #101)

### Fixed

- BigQuery: Handle mixed-case column names using BigQuery semantics (fixes #84)
- PostgreSQL: Fix upserts with mixed-case column names
- BigQuery: Correctly output NULL values in Boolean columns (#104)

### Removed

- BREAKING: BigQuery: Remove code that tried to rename column names to make them valid (fixes #84)
