use std::fmt;

use crate::errors::IdentifierError;

/// A Trino identifier, which [may need to be quoted][idents], depending on
/// contents.
///
/// > Identifiers must start with a letter, and subsequently include
/// > alphanumeric characters and underscores. Identifiers with other characters
/// > must be delimited with double quotes ("). When delimited with double
/// > quotes, identifiers can use any character. Escape a " with another
/// > preceding double quote in a delimited identifier.
/// >
/// > Identifiers are not treated as case sensitive.
///
/// We store identifiers as ASCII lowercase. It's unclear how we should handle
/// Unicode identifiers, so we leave them unchanged for now.
///
/// [idents]: https://trino.io/docs/current/language/reserved.html#language-identifiers
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TrinoIdent(String);

impl TrinoIdent {
    /// Create a new `TrinoIdent`.
    pub fn new(ident: &str) -> Result<Self, IdentifierError> {
        if ident.is_empty() {
            Err(IdentifierError::EmptyIdentifier)
        } else {
            Ok(Self(ident.to_ascii_lowercase()))
        }
    }

    /// Create a "placeholder" identifier for when we need to name an anonymous
    /// `ROW` field.
    pub fn placeholder(idx: usize) -> Self {
        Self(format!("f__{}", idx))
    }

    /// Get the underlying string.
    pub fn as_unquoted_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TrinoIdent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // We always quote identifiers, because that way we don't need a list of
        // reserved words.
        if self.0.contains('"') {
            // Double any double quotes in the identifier.
            let escaped = self.0.replace('"', r#""""#);
            write!(f, r#""{}""#, escaped)
        } else {
            write!(f, r#""{}""#, self.0)
        }
    }
}

// Deserialize a string as an identifier.
impl<'de> serde::Deserialize<'de> for TrinoIdent {
    fn deserialize<D>(deserializer: D) -> Result<TrinoIdent, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        TrinoIdent::new(&s).map_err(serde::de::Error::custom)
    }
}

// Serialize an identifier as a string.
impl serde::Serialize for TrinoIdent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

#[cfg(all(test, feature = "proptest"))]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn test_ident_round_trip(ident: TrinoIdent) {
            let s = ident.as_unquoted_str();
            let ident2 = TrinoIdent::new(s).unwrap();
            prop_assert_eq!(ident, ident2);
        }
    }
}
