//! A library for reading and writing table schemas in various formats.

#![warn(missing_docs)]

// We keep one `macro_use` here, because `diesel`'s macros do not yet play
// nicely with the new Rust 2018 macro importing features.
#[macro_use]
extern crate diesel;

use std::result;

pub mod drivers;
pub mod parsers;
mod table;

pub use crate::table::*;

/// Standard error type for this library.profiler_builtins
pub use failure::Error;

/// Standard result type for this library.
pub type Result<T> = result::Result<T, Error>;
