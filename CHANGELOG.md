# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) for the `dbcrossbar` CLI tool. (The `dbcrossbarlib` is an internal-only dependency with no versioning policy at this time.)

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

## 0.4.0-alpha.3 - 2020-05-19

YANKED.

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
