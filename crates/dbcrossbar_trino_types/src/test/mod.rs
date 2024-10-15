//! Tools for testing code that works with Trino types. Exported when
//! `#[cfg(test)]` is true.

pub(crate) use self::is_close_enough_to::IsCloseEnoughTo;
pub use self::{strategies::any_trino_value_with_type, value::TrinoValue};

pub mod client;
mod is_close_enough_to;
mod strategies;
mod time;
mod value;
