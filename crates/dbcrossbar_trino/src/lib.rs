//! This is an interface for working with the [Trino][] database, written for
//! use by [`dbcrossbar`][dbcrossbar] and related tools.
//!
//! ## Features
//!
//! These may be selected at compile-time, using `features = ["feature-name"]`
//! in your `Cargo.toml`. If you don't specify any features, this library is
//! extremely lightweight.
//!
//! - `values`: Provides a [`Value`] enum that can represent a subset of Trino's
//!   values. This pulls in dependencies for lots of things, including geodata,
//!   decimals, JSON and UUIDs.
//! - `proptest`: Support for testing using the [`proptest`][proptest] crate.
//!   This pulls in `proptest` and related libraries.
//! - `client`: A basic Trino REST client. This is mostly intended for testing,
//!   and does not currently attempt to be a production-quality client. It
//!   currently has no HTTPS or password support. This pulls in a full-fledged
//!   async HTTP stack.
//! - `rustls`: Enable Rust-native HTTPS support with WebPKI roots in the
//!   client.
//!
//! ## What this library provides
//!
//! This is a bit of a grab-bag of types and utilities, driven by the common
//! needs of several related tools.
//!
//! ### Storage transforms
//!
//! This is the heart of the library. This library exists because Trino doesn't
//! store any data itself. Instead, it _delegates_ storage to connectors. And
//! these connectors expose nearly all the limitations of the underlying storage
//! system. They're often missing key data types, or don't support `NOT NULL`,
//! or don't support transactions. The following types help generate code that
//! works around these limitations:
//!
//! - [`ConnectorType`] is the main entry point to this part of the library,
//!   providing an API to describe a connector's limitations. See this section
//!   for example code!
//! - [`StorageTransform`] describes how to transform data when storing it using
//!   a specific connector, and when reading it back.
//!
//! ### Basic utility types
//!
//! These are included mostly because they're needed by other parts of the
//! library.
//!
//! - [`DataType`] and [`Field`], which describe a subset of available data
//!   types in Trino.
//! - [`Ident`], which represents and prints a simple Trino identifier.
//! - [`QuotedString`], which formats a quoted and escaped string.
//! - [`TableOptions`], which represents the `WITH` clause of a `CREATE TABLE`
//!   statement.
//!
//! ### Values (requires the `values` feature)
//!
//! - [`Value`] represents a subset of Trino's values.
//! - [`IsCloseEnoughTo`] is a trait for comparing values that knows about the
//!   limitations of Trino's connectors.
//!
//! ### Other features
//!
//! - [`crate::proptest`] (requires the `proptest` feature) provides tools for
//!   generating random values for testing.
//! - [`crate::client`] (requires the `client` feature) provides a basic Trino
//!   client.
//!
//! [Trino]: https://trino.io/
//! [dbcrossbar]: https://www.dbcrossbar.org/
//! [proptest]: https://proptest-rs.github.io/proptest/intro.html

// Enable `doc_auto_cfg` on docs.rs. This will enable all features, and include
// information about which features are required for API.
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

#[cfg(feature = "values")]
pub use crate::values::Value;
pub use crate::{
    connectors::ConnectorType,
    errors::IdentifierError,
    ident::Ident,
    quoted_string::QuotedString,
    table_options::{TableOptionValue, TableOptions},
    transforms::{LoadExpr, StorageTransform, StoreExpr},
    types::{DataType, Field},
};
#[cfg(feature = "macros")]
pub use dbcrossbar_trino_macros::TrinoRow;

#[cfg(feature = "client")]
pub mod client;
mod connectors;
mod errors;
mod ident;
#[cfg(feature = "proptest")]
pub mod proptest;
mod quoted_string;
mod table_options;
mod transforms;
mod types;
#[cfg(feature = "values")]
pub mod values;
