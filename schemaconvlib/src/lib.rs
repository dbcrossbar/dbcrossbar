//! A library for reading and writing table schemas in various formats.

#![warn(missing_docs)]

#[macro_use]
extern crate diesel;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

use std::result;

pub mod drivers;
mod table;

pub use crate::table::*;

/// Standard error type for this library.profiler_builtins
pub use failure::Error;

/// Standard result type for this library.
pub type Result<T> = result::Result<T, Error>;
