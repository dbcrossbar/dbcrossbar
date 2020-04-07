# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) for the `dbcrossbar` CLI tool. (The `dbcrossbarlib` is an internal-only dependency with no versioning policy at this time.)

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
