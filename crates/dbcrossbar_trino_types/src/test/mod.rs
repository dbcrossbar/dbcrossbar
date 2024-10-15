//! Tools for testing code that works with Trino types. Exported when
//! `#[cfg(test)]` is true.

pub(crate) use self::approx_eq_to_json::ApproxEqToJson;
pub use self::{
    client::Client, strategies::any_trino_value_with_type, value::TrinoValue,
};

mod approx_eq_to_json;
mod client;
mod strategies;
mod time;
mod value;
