//! Interfaces to AWS.

mod auth;
mod signing;

pub(crate) use auth::*;
pub(crate) use signing::*;
