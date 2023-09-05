//! A simple interface for recording `metrics` data and converting it to
//! a Prometheus-compatible format.
//!
//! We may have some limitations:
//!
//! - We always report all metrics that we've ever seen. We could stop reporting
//!   stale metrics if we figured out [`metrics_util::Recency`].

use std::{
    collections::HashMap,
    io::Write,
    sync::{atomic::Ordering, Arc, RwLock},
};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Recorder, SharedString, Unit};
use metrics_util::registry::{AtomicStorage, Registry};

use crate::{Error, Result};

/// Builder for creating and installing a `NewRelicRecoder` and exporter.
pub(crate) struct PrometheusBuilder {}

impl PrometheusBuilder {
    /// Create a `NewRelicBuilder`, including our API key.
    pub(crate) fn new() -> Self {
        PrometheusBuilder {}
    }

    /// Construct a `NewRelicRecorder` with the specified parameters.
    pub(crate) fn build(self) -> Result<PrometheusRecorder> {
        Ok(PrometheusRecorder {
            inner: Arc::new(Inner {
                registry: Registry::new(AtomicStorage),
                descriptions: RwLock::new(HashMap::new()),
            }),
        })
    }
}

/// The actual implementation of `NewRelicRecorder`.
///
/// We keep this behind `Inner` so that we can access it even after it has been
/// installed.
struct Inner {
    /// The registry which manages the low-level details of our metrics.
    registry: Registry<Key, AtomicStorage>,

    /// Descriptions for our metrics, where available. Keys are actually
    /// `KeyName` values, but there's no way to get a `KeyName` from a `Key`, so
    /// we use `String` here.
    descriptions: RwLock<HashMap<String, SharedString>>,
}

impl Inner {
    /// Report metrics to NewRelic.
    fn render(&self) -> Result<String> {
        let mut rendered: Vec<u8> = vec![];

        for (key_name, counters) in
            group_by_key_name(self.registry.get_counter_handles())
        {
            self.print_metric_header(&mut rendered, &key_name, "counter");
            for (key, counter) in counters {
                let count = counter.load(Ordering::Acquire);
                self.print_key(&mut rendered, &key);
                writeln!(&mut rendered, " {}", count).unwrap();
            }
            writeln!(&mut rendered).unwrap();
        }
        for (key_name, gauges) in group_by_key_name(self.registry.get_gauge_handles())
        {
            self.print_metric_header(&mut rendered, &key_name, "gauge");
            for (key, gauge) in gauges {
                let value = gauge.load(Ordering::Acquire);
                self.print_key(&mut rendered, &key);
                writeln!(&mut rendered, " {}", value).unwrap();
            }
            writeln!(&mut rendered).unwrap();
        }
        for (key_name, _buckets) in
            group_by_key_name(self.registry.get_histogram_handles())
        {
            writeln!(&mut rendered, "# skipped histogram: {}", key_name).unwrap();
        }

        String::from_utf8(rendered).map_err(Error::could_not_report_metrics)
    }

    /// Print out the header lines above a Prometheus metric.
    fn print_metric_header(&self, rendered: &mut Vec<u8>, key_name: &str, typ: &str) {
        if let Some(description) = self
            .descriptions
            .read()
            .expect("lock poisoned")
            .get(key_name)
        {
            writeln!(rendered, "# HELP {} {}", key_name, description).unwrap();
        }
        writeln!(rendered, "# TYPE {} {}", key_name, typ).unwrap();
    }

    /// Print `Key` in Prometheus format.
    fn print_key(&self, rendered: &mut Vec<u8>, key: &Key) {
        write!(rendered, "{}", key.name()).unwrap();
        if key.labels().len() > 0 {
            write!(rendered, "{{").unwrap();
            for (i, label) in key.labels().enumerate() {
                if i > 0 {
                    write!(rendered, ",").unwrap();
                }
                // TODO: Escape label values better than using Rust escaping.
                write!(rendered, "{}={:?}", label.key(), label.value()).unwrap();
            }
            write!(rendered, "}}").unwrap();
        }
    }
}

/// Group keys by their `KeyName`.
fn group_by_key_name<T, I>(
    metrics: I,
) -> impl IntoIterator<Item = (String, impl IntoIterator<Item = (Key, Arc<T>)>)>
where
    I: IntoIterator<Item = (Key, Arc<T>)>,
{
    let mut grouped = HashMap::new();
    for (key, value) in metrics {
        grouped
            .entry(key.name().to_owned())
            .or_insert_with(Vec::new)
            .push((key, value));
    }
    grouped
}

/// A metrics recorder that reports to Prometheus.
pub struct PrometheusRecorder {
    /// Our actual data, in a shared structure.
    inner: Arc<Inner>,
}

impl PrometheusRecorder {
    /// Get a handle which can be used to access certain reporter features after
    /// it's installed.
    pub(crate) fn renderer(&self) -> PrometheusRenderer {
        PrometheusRenderer {
            inner: self.inner.clone(),
        }
    }

    /// Install this recorder as the global default recorder.
    pub(crate) fn install(self) -> Result<PrometheusRenderer> {
        let handle = self.renderer();
        metrics::set_boxed_recorder(Box::new(self))
            .map_err(Error::could_not_configure_metrics)?;
        Ok(handle)
    }

    /// Record description for a metric.
    fn record_description(&self, key_name: KeyName, description: SharedString) {
        self.inner
            .descriptions
            .write()
            .expect("lock poisoned")
            .insert(key_name.as_str().to_owned(), description);
    }
}

impl Recorder for PrometheusRecorder {
    fn describe_counter(
        &self,
        key: KeyName,
        _unit: Option<Unit>,
        description: SharedString,
    ) {
        self.record_description(key, description);
    }

    fn describe_gauge(
        &self,
        key: KeyName,
        _unit: Option<Unit>,
        description: SharedString,
    ) {
        self.record_description(key, description);
    }

    fn describe_histogram(
        &self,
        key: KeyName,
        _unit: Option<Unit>,
        description: SharedString,
    ) {
        self.record_description(key, description);
    }

    fn register_counter(&self, key: &Key) -> Counter {
        // TODO: Enforce valid Prometheus names.
        self.inner
            .registry
            .get_or_create_counter(key, |c| c.clone().into())
    }

    fn register_gauge(&self, key: &Key) -> Gauge {
        // TODO: Enforce valid Prometheus names.
        self.inner
            .registry
            .get_or_create_gauge(key, |c| c.clone().into())
    }

    fn register_histogram(&self, key: &Key) -> Histogram {
        // TODO: Enforce valid Prometheus names.
        self.inner
            .registry
            .get_or_create_histogram(key, |c| c.clone().into())
    }
}

/// A handle to a `PrometheusRecorder`. You can use this to access certain
/// features of the recorder after installing it.
#[derive(Clone)]
pub(crate) struct PrometheusRenderer {
    inner: Arc<Inner>,
}

impl PrometheusRenderer {
    /// Render the current metrics as a string.
    pub(crate) fn render(&self) -> Result<String> {
        self.inner.render()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_reporter() {
        PrometheusBuilder::new().build().unwrap();
    }
}
