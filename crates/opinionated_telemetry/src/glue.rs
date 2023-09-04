//! Glue routines for interfacing with various things.

use futures::{
    future::FutureObj,
    task::{Spawn, SpawnError},
};
use tracing::{instrument::WithSubscriber, subscriber::NoSubscriber};

pub(crate) struct TokioGlue;

impl Spawn for TokioGlue {
    fn spawn_obj(&self, future: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        // Turn off `tracing` so that `tracing` on `h2` while submitting traces
        // via `h2` does not cause an infinite loop.
        let future = future.with_subscriber(NoSubscriber::default());
        tokio::spawn(future);
        Ok(())
    }
}
