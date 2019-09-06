use serde::de::DeserializeOwned;
use serde_json::{map::Entry, Map, Value};
use std::iter::FromIterator;

use crate::common::*;

/// Driver-specific arguments.
#[derive(Clone, Debug, Default)]
pub struct DriverArguments {
    /// A list of key-value pairs, in order.
    args: Vec<(String, String)>,
}

impl DriverArguments {
    /// Parse a list of command-line arguments of the form `key=value` into a
    /// `DriverArgs` structure.
    pub fn from_cli_args(args: &[String]) -> Result<Self> {
        let args = args
            .iter()
            .map(|arg| -> Result<(String, String)> {
                let split = arg.splitn(2, '=').collect::<Vec<_>>();
                if split.len() != 2 {
                    return Err(format_err!(
                        "cannot parse driver argument: {:?}",
                        arg
                    ));
                }
                Ok((split[0].to_owned(), split[1].to_owned()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { args })
    }

    /// Is this collection of driver arguments empty?
    pub(crate) fn is_empty(&self) -> bool {
        self.args.is_empty()
    }

    /// Return an iterator over the key-value pairs of this `DriverArgs`.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.args.iter().map(|(k, v)| (&k[..], &v[..]))
    }

    /// Convert these arguments to a JSON object. We treat keys of the form
    /// "parent.nested" as `{ "parent": { "nested": ... } }`.
    fn to_json(&self) -> Result<Value> {
        let mut map = Map::new();
        for (k, v) in &self.args {
            let path = k.split('.').collect::<Vec<_>>();
            let mut m = &mut map;
            for &ancestor in &path[..path.len() - 1] {
                m = match m.entry(ancestor) {
                    Entry::Vacant(vacant) => vacant
                        .insert(Value::Object(Map::new()))
                        .as_object_mut()
                        .expect("inserted Value::Object but didn't get it back"),
                    Entry::Occupied(occupied) => match occupied.into_mut() {
                        Value::Object(new_m) => new_m,
                        value => {
                            return Err(format_err!(
                                "argument {:?} conflicts with existing {:?} value {}",
                                k,
                                ancestor,
                                value,
                            ));
                        }
                    },
                };
            }
            m.insert(path[path.len() - 1].to_owned(), Value::String(v.to_owned()));
        }
        Ok(Value::Object(map))
    }

    /// Deserialize our driver arguments into a struct of type `T` using
    /// `serde`. This obeys the same nesting rules as `[DriverArguments::to_json]`.
    pub(crate) fn deserialize<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_value(self.to_json()?)?)
    }
}

#[test]
fn to_json_handles_nested_keys() {
    use serde_json::json;
    let raw_args = &[("a", "b"), ("c.d", "x"), ("c.e", "y")];
    let args = DriverArguments::from_iter(raw_args.iter().cloned());
    assert_eq!(
        args.to_json().unwrap(),
        json!({"a": "b", "c": { "d": "x", "e": "y" } }),
    );

    let raw_conflicting_args = &[("a", "x"), ("a.b", "y")];
    let conflicting_args =
        DriverArguments::from_iter(raw_conflicting_args.iter().cloned());
    assert!(conflicting_args.to_json().is_err())
}

impl<K, V> FromIterator<(K, V)> for DriverArguments
where
    K: Into<String>,
    V: Into<String>,
{
    /// Construct a `DriverArgs` from key/value pairs.
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (K, V)>,
    {
        let args = iter
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        Self { args }
    }
}
