//! Common monitoring tools we'll break out into an open source crate.
//!
//! We focus on supporting the following Rust APIs:
//!
//! - `tracing` for tracing, with support for fowarding from `log`.
//! - `metrics` for monitoring.
//!
//! We specifically try to integrate with OpenTelemetry and to support standard
//! `"traceparent"` and `"tracestate"` headers.

// Re-export all the APIs we encourage people to use.
pub use ::metrics::{
    counter, decrement_gauge, gauge, histogram, increment_counter, increment_gauge,
    register_counter, register_gauge, register_histogram,
};
pub use ::tracing::{
    debug, debug_span, error, error_span, event, info, info_span, instrument, span,
    trace, trace_span, warn, warn_span, Instrument, Level,
};

mod debug_exporter;
mod env_extractor;
mod env_injector;
mod error;
mod glue;
mod metrics;
mod tracing;

pub use self::env_injector::EnvInjector;
pub use self::error::{Error, Result};
pub use self::tracing::{
    end_tracing, inject_current_context, start_tracing,
    trace_with_parent_span_from_env, WithExternalContext,
};
