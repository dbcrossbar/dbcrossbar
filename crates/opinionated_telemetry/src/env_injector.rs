use std::vec;

use opentelemetry::propagation::Injector;

/// Used to convert current OpenTelemetry context into a set of environment
/// variables.
#[derive(Debug, Default)]
pub(crate) struct EnvInjector {
    add_to_env: Vec<(String, String)>,
}

impl EnvInjector {
    /// Create a new `EnvInjector`.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Injector for EnvInjector {
    fn set(&mut self, key: &str, value: String) {
        self.add_to_env.push((key.to_ascii_uppercase(), value));
    }
}

impl IntoIterator for EnvInjector {
    type Item = (String, String);
    type IntoIter = EnvInjectorIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        EnvInjectorIntoIter(self.add_to_env.into_iter())
    }
}

pub(crate) struct EnvInjectorIntoIter(vec::IntoIter<(String, String)>);

impl Iterator for EnvInjectorIntoIter {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}
