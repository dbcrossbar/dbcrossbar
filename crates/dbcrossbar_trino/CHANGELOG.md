# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.2] - 2024-12-04

### Added

- `Value::Null` is now available.

### Fixed

- We can deserialize `NULL` values returned by Trino.

## [0.2.1] - 2024-12-04

### Added

- `DataType` now supports `FromStr`.
- `ClientBuilder::for_tests` will create a `ClientBuilder` that points to Trino on `localhost`, similar to `Client::default`.
- `ClientBuilder::catalog_and_schema` can be used to specify the default catalog and schema for the client.
- `Client::get_table_column_info` can be used to get information about the columns in a table.
- `IsCloseEnoughTo` is now implemented for `&[T]`, `Vec<T>`, and `Option<T>`.

### Fixed

- Crate features listed in docs are now correct and complete.

## [0.2.0] - 2024-12-03

### Added

- A basic Trino client.
- Support for `proptest`.
- Support for using formerly test-only features as part of the regular library API.
- Pretty-printing and SQL AST support.

### Changed

- `store_expr` and `load_expr` now take AST expressions and support
  pretty-printing, instead of just using strings. This changes the API
  slightly.

## [0.1.0] - 2024-10-21

### Added

- Initial release.

