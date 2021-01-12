//! Temporary storage management.

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::iter;

use crate::common::*;
use crate::config::Configuration;

/// Provides different types of temporary storage.
#[derive(Clone, Debug)]
pub struct TemporaryStorage {
    /// Various places we can store things temporarily.
    locations: Vec<String>,
}

impl TemporaryStorage {
    /// Create a new `TemporaryStorage` object. The `locations` should be a list
    /// of locator-like strings, such as `gs://bucket/tempdir` or
    /// `bigquery:project:dataset`.
    pub fn new(locations: Vec<String>) -> Self {
        TemporaryStorage { locations }
    }

    /// Like `new`, but also use temporaries from `config`.
    pub fn with_config(
        mut locations: Vec<String>,
        config: &Configuration,
    ) -> Result<Self> {
        // These go _after_, so that they can be overridden by values in `locations`.
        locations.extend(config.temporaries()?);
        Ok(TemporaryStorage { locations })
    }

    /// Find a location with the specified scheme.
    pub fn find_scheme<'a, 'b>(&'a self, scheme: &'b str) -> Option<&'a str> {
        assert!(scheme.ends_with(':'));
        self.locations
            .iter()
            .find(|l| l.starts_with(scheme))
            .map(|l| l.as_str())
    }

    /// Generate a random alphanumeric tag for use in temporary directory names.
    pub fn random_tag() -> String {
        let mut rng = thread_rng();
        let bytes = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(10)
            .collect::<Vec<u8>>();
        String::from_utf8(bytes)
            .expect("random alphanumeric value should always be valid UTF-8")
    }
}

#[test]
fn find_schema() {
    let storage = TemporaryStorage::new(vec![
        "s3://example/".to_string(),
        "gs://example/1/".to_string(),
        "gs://example/2/".to_string(),
    ]);
    assert_eq!(storage.find_scheme("s3:"), Some("s3://example/"));
    assert_eq!(storage.find_scheme("gs:"), Some("gs://example/1/"));
}

#[test]
fn random_tag() {
    assert_eq!(TemporaryStorage::random_tag().len(), 10);
}
