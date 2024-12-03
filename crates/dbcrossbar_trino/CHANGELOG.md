# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2024-10-21

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

