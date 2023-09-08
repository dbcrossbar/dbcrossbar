//! Logging and error-handling context.

use tokio::process::Child;
use tokio_stream::wrappers::ReceiverStream;
use tracing::Span;

use crate::common::*;

/// Context shared by our various asynchronous operations.
#[derive(Debug, Clone)]
pub struct Context {
    /// To report asynchronous errors anywhere in the application, send them to
    /// this channel.
    error_sender: mpsc::Sender<Error>,
}

impl Context {
    /// Create a new context, and a future represents our background workers,
    /// returning `()` if they all succeed, or an `Error` as soon as one of them
    /// fails.
    pub fn create() -> (Self, BoxFuture<()>) {
        let (error_sender, receiver) = mpsc::channel(1);
        let mut receiver = ReceiverStream::new(receiver);
        let context = Context { error_sender };
        let worker_future = async move {
            match receiver.next().await {
                // All senders have shut down correctly.
                None => Ok(()),
                // We received an error from a background worker, so report that
                // as the result for all our background workers.
                Some(err) => Err(err),
            }
        };
        (context, worker_future.boxed())
    }

    /// Spawn an async worker in this context, and report any errors to the
    /// future returned by `create`.
    pub fn spawn_worker<W>(&self, span: Span, worker: W)
    where
        W: Future<Output = Result<()>> + Send + 'static,
    {
        let error_sender = self.error_sender.clone();
        tokio::spawn(
            async move {
                if let Err(err) = worker.await {
                    debug!("reporting background worker error: {}", err);
                    if let Err(_err) = error_sender.send(err).await {
                        debug!("broken pipe reporting background worker error");
                    }
                }
            }
            .instrument(span)
            .boxed(),
        );
    }

    /// Monitor an asynchrnous child process, and report any errors or non-zero
    /// exit codes that occur.
    pub fn spawn_process(&self, name: String, mut child: Child) {
        let name_copy = name.clone();
        let worker = async move {
            match child.wait().await {
                Ok(ref status) if status.success() => Ok(()),
                Ok(status) => Err(format_err!("{} failed with {}", name, status)),
                Err(err) => Err(format_err!("{} failed with error: {}", name, err)),
            }
        };
        self.spawn_worker(
            debug_span!("spawn_process", name = ?name_copy),
            worker.boxed(),
        );
    }
}
