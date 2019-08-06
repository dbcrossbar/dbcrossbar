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
