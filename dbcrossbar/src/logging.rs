//! Support for structured logging.

use dbcrossbarlib::Error;
use failure::format_err;
use slog::{Drain, Never};
use slog_json;
use slog_term;
use std::{io::stderr, str::FromStr};

/// A polymorphic log drain (which means we need to use `Box<dyn ...>`,
/// because that's how Rust does runtime polymorphism).
type BoxDrain = Box<dyn Drain<Ok = (), Err = Never> + Send + 'static>;

/// What log format we should use.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum LogFormat {
    /// Pretty, indented logs.
    Indented,
    /// Single-line log entries with all keys on each line.
    Flat,
    /// JSON records.
    Json,
}

impl LogFormat {
    /// Create an appropriate `Drain` for this log format.
    pub(crate) fn create_drain(self) -> BoxDrain {
        match self {
            Self::Indented => {
                let decorator = slog_term::TermDecorator::new().stderr().build();
                Box::new(slog_term::CompactFormat::new(decorator).build().fuse())
                    as BoxDrain
            }
            Self::Flat => {
                let decorator = slog_term::PlainDecorator::new(stderr());
                Box::new(slog_term::FullFormat::new(decorator).build().fuse())
                    as BoxDrain
            }
            Self::Json => {
                Box::new(slog_json::Json::default(stderr()).fuse()) as BoxDrain
            }
        }
    }
}

impl FromStr for LogFormat {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "indented" => Ok(LogFormat::Indented),
            "flat" => Ok(LogFormat::Flat),
            "json" => Ok(LogFormat::Json),
            _ => Err(format_err!("unknown log format: {}", s)),
        }
    }
}
