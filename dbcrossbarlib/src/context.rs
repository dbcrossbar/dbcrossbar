//! Logging and error-handling context.

use slog::{OwnedKV, SendSyncRefUnwindSafeKV};
use tokio_process::Child;

use crate::common::*;

/// Context shared by our various asynchronous operations.
#[derive(Debug, Clone)]
pub struct Context {
    /// The logger to use for code in this context.
    log: Logger,
    /// To report asynchronous errors anywhere in the application, send them to
    /// this channel.
    error_sender: mpsc::Sender<Error>,
}

impl Context {
    /// Create a new context, and a future represents our background workers,
    /// returning `()` if they all succeed, or an `Error` as soon as one of them
    /// fails.
    pub fn create(log: Logger) -> (Self, impl Future<Item = (), Error = Error>) {
        let (error_sender, receiver) = mpsc::channel(1);
        let context = Context { log, error_sender };
        let worker_future = async move {
            match await!(receiver.into_future()) {
                // An error occurred in the low-level mechanisms of our `mpsc`
                // channel.
                Err((_err, _rcvr)) => {
                    Err(format_err!("background task reporting failed"))
                }
                // All senders have shut down correctly.
                Ok((None, _rcvr)) => Ok(()),
                // We received an error from a background worker, so report that
                // as the result for all our background workers.
                Ok((Some(err), _rcvr)) => Err(err),
            }
        };
        (context, tokio_fut(worker_future))
    }

    /// Get the logger associated with this context.
    pub fn log(&self) -> &Logger {
        &self.log
    }

    /// Create a child context, adding extra `slog` logging context. You can
    /// create the `log_kv` value using `slog`'s `o!` macro.
    pub fn child<T>(&self, log_kv: OwnedKV<T>) -> Self
    where
        T: SendSyncRefUnwindSafeKV + 'static,
    {
        Context {
            log: self.log.new(log_kv),
            error_sender: self.error_sender.clone(),
        }
    }

    /// Spawn an async worker in this context, and report any errors to the
    /// future returned by `create`.
    pub fn spawn_worker<W>(&self, worker: W)
    where
        W: Future<Item = (), Error = Error> + Send + 'static,
    {
        let log = self.log.clone();
        let error_sender = self.error_sender.clone();
        tokio::spawn_async(async move {
            if let Err(err) = await!(worker) {
                debug!(log, "reporting background worker error: {}", err);
                if let Err(_err) = await!(error_sender.send(err)) {
                    debug!(log, "broken pipe reporting background worker error");
                }
            }
        });
    }

    /// Monitor an asynchrnous child process, and report any errors or non-zero
    /// exit codes that occur.
    pub fn spawn_process(&self, name: String, child: Child) {
        let worker = async move {
            match await!(child) {
                Ok(ref status) if status.success() => Ok(()),
                Ok(status) => Err(format_err!("{} failed with {}", name, status)),
                Err(err) => Err(format_err!("{} failed with error: {}", name, err)),
            }
        };
        self.spawn_worker(tokio_fut(worker));
    }
}
