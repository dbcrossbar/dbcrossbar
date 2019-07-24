//! Support for structured logging.

use dbcrossbarlib::{Error, Result};
use failure::format_err;
use slog::{error, slog_o as o, Drain, Logger, Never};
use slog_json;
use slog_term;
use std::{io::stderr, result, str::FromStr, sync::Arc};

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

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "indented" => Ok(LogFormat::Indented),
            "flat" => Ok(LogFormat::Flat),
            "json" => Ok(LogFormat::Json),
            _ => Err(format_err!("unknown log format: {}", s)),
        }
    }
}

/// Given a log `drain`, and a list of `extra` values in the format
/// `"key=value"`, create a global logger.
pub(crate) fn global_logger_with_extra_values(
    drain: slog::Fuse<slog_async::Async>,
    extra: &[String],
) -> Result<Logger> {
    // Construct our base logger.
    let values = o!(
        "app" => env!("CARGO_PKG_NAME"),
        "ver" => env!("CARGO_PKG_VERSION"),
    );
    let mut log = Logger::root(drain, values);

    // If we have `log_extra` values, add them to our logger. This is much
    // trickier than you think, because the `o!` macro doesn't return a normal
    // hash-map, but instead a highly-optimized type that can't be mutated, and
    // which has a different compile-time type for different numbers of keys. So
    // the only way to build up a variable-length set of key-value pairs is to
    // use `log.new` and construct a chain of loggers, _each_ with a single
    // key-value pair.
    for kv_str in extra {
        let kv = kv_str.splitn(2, '=').collect::<Vec<_>>();
        if kv.len() != 2 {
            let err =
                format_err!("expected {:?} to contain a \"=\" character", kv_str);
            error!(log, "{}", err);
            return Err(err);
        }

        // YUCK. The `o!` macro requires a `&'static str` as a key. We can only
        // create one of these by putting a `String` into a `Box` and using
        // `Box::leak` to permanently leak the `Box<String>` for the rest of the
        // program's lifetime. This is an ugly Rust trick that almost always
        // means we're doing something _horribly_ wrong and we should stop
        // immediately, but in this case we have no other choice, given the
        // `slog` API. Happily, this is a global logger, so this shouldn't
        // normally happen.
        let key = &Box::leak::<'static>(Box::new(kv[0].to_owned()))[..];
        let value = kv[1].to_owned();
        log = log.new(o!(key => value));
    }

    Ok(log)
}
