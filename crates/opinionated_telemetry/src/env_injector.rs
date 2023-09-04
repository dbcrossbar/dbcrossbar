use opentelemetry::propagation::Injector;

use crate::env_extractor::ENV_VAR_PREFIX;

/// Used to convert current OpenTelemetry context into a set of environment
/// variables.
#[derive(Debug, Default)]
pub struct EnvInjector {
    remove_from_env: Vec<String>,
    add_to_env: Vec<(String, String)>,
}

impl EnvInjector {
    /// Create a new `EnvInjector`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Environment variables that should be removed from the environment.
    pub fn remove_from_env(&self) -> impl Iterator<Item = &str> {
        self.remove_from_env.iter().map(|s| s.as_str())
    }

    /// Environment variables that should be added to the environment.
    pub fn add_to_env(&self) -> impl Iterator<Item = (&str, &str)> {
        self.add_to_env
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

impl Injector for EnvInjector {
    fn set(&mut self, key: &str, value: String) {
        let key = format!("{}{}", ENV_VAR_PREFIX, key.to_ascii_uppercase());
        if value.is_empty() {
            // TODO: For now, we remove empty values from the environment. This
            // will usually affect `W3C_TRACESTATE`. I'm not sure if this is
            // actually a reasonable thing to do, but:
            //
            // 1. Empty environment values are weird.
            // 2. We don't want to use an inherited value that came from who
            //    knows where.
            //
            // We'll probably need to revisit this at some point, because (for
            // example) we might have a `W3C_BAGGAGE` value from a distant
            // parent process, but our local context might not include an empty
            // `baggage` value. We might want to strip all unknown `W3C_`
            // environment variables? But since we made up the `W3C_` prefix, we
            // might clobber something we don't know about in the future.
            self.remove_from_env.push(key);
        } else {
            self.add_to_env.push((key, value));
        }
    }
}
