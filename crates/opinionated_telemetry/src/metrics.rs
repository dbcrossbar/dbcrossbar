//! Tools for setting up tracing.

use metrics_tracing_context::{LabelFilter, TracingContextLayer};
pub use metrics_util::debugging::Snapshotter;
use metrics_util::{debugging::DebuggingRecorder, layers::Layer};

use super::debug;

/// Filter the labels used in our metrics.
#[derive(Clone)]
struct CustomLabelFilter;

impl LabelFilter for CustomLabelFilter {
    fn should_include_label(
        &self,
        _key_name: &metrics::KeyName,
        label: &metrics::Label,
    ) -> bool {
        !label.key().starts_with("otel.")
    }
}

/// Install a debug reporting for metrics.
#[allow(dead_code)]
fn install_debug_recorder() -> Snapshotter {
    let recorder = DebuggingRecorder::default();
    let snapshoter = recorder.snapshotter();
    let recorder_with_tracing =
        TracingContextLayer::new(CustomLabelFilter).layer(recorder);
    metrics::set_boxed_recorder(Box::new(recorder_with_tracing))
        .expect("found already installed metric recorder");
    snapshoter
}

/// Log a snapshot of our metrics at `Level::DEBUG`.
#[allow(dead_code)]
fn log_metrics_snapshot(snapshotter: &Snapshotter) {
    let snapshot = snapshotter.snapshot();
    for (key, _unit, _desc, value) in snapshot.into_vec() {
        match value {
            metrics_util::debugging::DebugValue::Counter(value) => {
                debug!("{}: {}", key.key(), value);
            }
            value => debug!("{}: {:?}", key.key(), value),
        }
    }
}
