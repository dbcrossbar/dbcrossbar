//! Synchronous telemetry wrapper.
//!
//! Normally, this library is used with `async`, but we want to provide a
//! synchronous wrapper for use in tests and other synchronous contexts.

use tokio::runtime::Runtime;

use crate::{Error, Result, TelemetryConfig, TelemetryHandle};

/// Helper function which installs our telemetry.
pub(crate) fn install_sync_helper(
    config: TelemetryConfig,
) -> Result<TelemetrySyncHandle> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::could_not_install_telemetry)?;
    let inner = rt.block_on(config.install())?;
    Ok(TelemetrySyncHandle { inner, rt })
}

/// Like [`TelemetryHandle`], but for synchronous telemetry.
pub struct TelemetrySyncHandle {
    /// An async telemetry handle.
    inner: TelemetryHandle,
    /// The runtime we're using.
    rt: Runtime,
}

impl TelemetrySyncHandle {
    /// Halt all telemetry subsystems, flushing any remaining data.
    pub fn flush_and_shutdown(self) {
        self.rt.block_on(self.inner.flush_and_shutdown());
        self.rt.shutdown_background();
    }
}
