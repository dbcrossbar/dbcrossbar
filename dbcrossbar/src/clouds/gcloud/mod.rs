//! Interfaces to Google Cloud.

pub(crate) mod auth;
pub(crate) mod bigquery;
mod client;
pub(crate) mod crc32c_stream;
pub(crate) mod storage;

pub(crate) use client::*;
