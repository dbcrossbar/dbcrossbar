# `opinionated_telemetry`: Easy-to-use backend for `metrics` and `tracing`

This is a single crate that provides a simple, opinionated backend for `metrics` and `tracing`. The goal is to easily enable an existing Rust app to emit metrics and traces, without having to spend a lot of effort on configuration.

## This crate's opinions

All opionated software should try to list what opinions it holds, so users know
whether it's a good fit for them. Here are the opinions of this crate:

- Tracing and metrics are incredibly useful, and should be ubiquitous.
  - This includes CLI tools, not just servers!
- [Prometheus][] and [Grafana][] are a solid combination for metrics and dashboards.
  - CLI tools should normally use [`prom-aggregation-gateway`][agg] instead of
    [Prometheus `pushgateway`][push].
- [OpenTelemetry][] and [W3C Trace Context][] are a popular choice for tracing.
  - But tracing backends are less standardized than Prometheus.
- Rust's [`tracing`][tracing] and [`metrics`][metrics] façades are good enough
  to handle the basics, and reasonably standard.
- "Labels" on metrics should be carefully chosen and "low-arity" (having few
  possible values). Therefore, inheriting labels from parent scopes is almost
  always the wrong thing to do.

[Prometheus]: https://prometheus.io/
[Grafana]: https://grafana.com/
[agg]: https://github.com/zapier/prom-aggregation-gateway
[push]: <https://github.com/prometheus/pushgateway>
[OpenTelemetry]: <https://opentelemetry.io/>
[W3C Trace Context]: <https://www.w3.org/TR/trace-context/>
[tracing]: https://github.com/tokio-rs/tracing
[metrics]: https://github.com/metrics-rs/metrics

## Supported backends

Tracing:

- Vendors
  - [x] [Google Cloud Trace](https://cloud.google.com/trace)
- [ ] [Jaeger](https://www.jaegertracing.io/) (not yet supported, but we'd love a PR)
- [x] Debug (printed to stderr)

Metrics:

- [x] Prometheus (scraping)
- [x] Prometheus (push gateway)
- [x] Debug (logged via `tracing`)
