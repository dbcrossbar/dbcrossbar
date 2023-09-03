use std::{collections::HashMap, env, ffi::OsString, iter::IntoIterator};

use opentelemetry::propagation::Extractor;

use super::warn;

pub(crate) const ENV_VAR_PREFIX: &str = "W3C_";

/// Extract trace information from environment variables.
///
/// We expect environment variables to be of the form:
///
/// - `W3C_TRACEPARENT`
/// - `W3C_TRACESTATE`
/// - `W3C_BAGGAGE`
pub(crate) struct EnvExtractor {
    extracted: HashMap<String, String>,
}

impl EnvExtractor {
    /// Build an `EnvExtractor` from the current environment.
    ///
    /// This always succeeds, though it may ignore anything in the environment
    /// that it doesn't understand, including strings that can't be converted to
    /// UTF-8, or multiple environment variables that differ only in case.
    pub(crate) fn from_env() -> Self {
        Self::from_iter(env::vars_os())
    }

    /// Build an extractor from an iterator over `OsString` pairs. Used
    /// internally for testing.
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (OsString, OsString)>,
    {
        // Iterate over environment variables using `OsString` to represent
        // names and values. We do this just in case someone has a weird
        // environment variable that can't be safely represented as UTF-8.
        let mut extracted = HashMap::new();
        for (var, value) in iter.into_iter() {
            // Ignore anything we can't convert to UTF-8;
            if let (Some(var), Some(value)) = (var.to_str(), value.to_str()) {
                if let Some(stripped) = var.strip_prefix(ENV_VAR_PREFIX) {
                    let key = stripped.to_ascii_lowercase();
                    if extracted.insert(key, value.to_owned()).is_some() {
                        warn!(
                            "multiple environment variable names similar to {:?}",
                            var
                        );
                    }
                }
            }
        }
        Self { extracted }
    }
}

impl Extractor for EnvExtractor {
    fn get(&self, key: &str) -> Option<&str> {
        self.extracted
            .get(&key.to_ascii_lowercase())
            .map(|v| &v[..])
    }

    fn keys(&self) -> Vec<&str> {
        self.extracted.keys().map(|v| &v[..]).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_standard_headers() {
        let traceparent_var = OsString::from("W3C_TRACEPARENT");
        let traceparent_val =
            OsString::from("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        let other_var = OsString::from("OTHER");
        let other_val = OsString::from("VALUE");

        let fake_env =
            vec![(traceparent_var, traceparent_val), (other_var, other_val)];
        let extractor = EnvExtractor::from_iter(fake_env);

        assert_eq!(
            extractor.get("traceparent"),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
        assert_eq!(
            extractor.get("TRACEPARENT"),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
        assert_eq!(extractor.get("other"), None);
        assert_eq!(extractor.keys(), vec!["traceparent"]);
    }
}
