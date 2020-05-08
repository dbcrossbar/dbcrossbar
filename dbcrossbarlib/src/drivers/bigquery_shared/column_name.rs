//! BigQuery column names.

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::{
    cmp::Ordering,
    convert::TryFrom,
    fmt,
    hash::{Hash, Hasher},
    str::FromStr,
};

use crate::common::*;

/// A BigQuery column name.
///
/// This behaves like a string that preserves case, but which ignores it for
/// comparisons. It may only contain valid BigQuery column names.
///
/// According to the official docs:
///
/// > A column name must contain only letters (a-z, A-Z), numbers (0-9), or
/// > underscores (_), and it must start with a letter or underscore. The
/// > maximum column name length is 128 characters. A column name cannot use any
/// > of the following prefixes:
/// >
/// > - _TABLE_
/// > - _FILE_
/// > - _PARTITION
/// >
/// > Duplicate column names are not allowed even if the case differs. For
/// > example, a column named Column1 is considered identical to a column named
/// > column1.
///
/// [docs]: https://cloud.google.com/bigquery/docs/schemas#column_names
#[derive(Clone)]
pub(crate) struct ColumnName {
    /// The original, mixed-case string, followed by an all-lowercase copy.
    ///
    /// Since we know that ASCII strings always have one character per byte, and
    /// that lowercasing a string doesn't change its length, we can assume that
    /// the dividing point is always exactly in the middle.
    data: String,
}

impl ColumnName {
    /// The original string, including case information.
    pub(crate) fn as_str(&self) -> &str {
        // We store the original string in the first half.
        &self.data[..self.data.len() / 2]
    }

    /// Am all-lowecase version. Used for comparison.
    fn as_lowercase(&self) -> &str {
        // We store the lowercase string in the second half.
        &self.data[self.data.len() / 2..]
    }

    /// Convert this to a portable name.
    pub(crate) fn to_portable_name(&self) -> String {
        self.as_str().to_owned()
    }

    /// Quote this for use in SQL.
    pub(crate) fn quoted(&self) -> ColumnNameQuoted<'_> {
        ColumnNameQuoted(self)
    }

    /// Quote this for use in JavaScript.
    pub(crate) fn javascript_quoted(&self) -> ColumnNameJavaScriptQuoted<'_> {
        ColumnNameJavaScriptQuoted(self)
    }
}

impl PartialEq for ColumnName {
    fn eq(&self, other: &Self) -> bool {
        // Compare only the lowercase versions.
        self.as_lowercase() == other.as_lowercase()
    }
}

impl Eq for ColumnName {}

impl PartialOrd for ColumnName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ColumnName {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_lowercase().cmp(other.as_lowercase())
    }
}

impl Hash for ColumnName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_lowercase().hash(state);
    }
}

impl fmt::Debug for ColumnName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl TryFrom<&str> for ColumnName {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        // Check for validity.
        let mut chars = s.chars();
        match chars.next() {
            Some(c) if c == '_' || c.is_ascii_alphabetic() => {}
            _ => {
                return Err(format_err!(
                        "BigQuery column name {:?} must start with an underscore or an ASCII letter",
                        s,
                    ));
            }
        }
        if !chars.all(|c| c == '_' || c.is_ascii_alphanumeric()) {
            return Err(format_err!("BigQuery column name {:?} must contain only underscores, ASCII letters, or ASCII digits", s,));
        }

        // Build data.
        let mut data = String::with_capacity(s.len() * 2);
        data.push_str(s);
        data.extend(s.chars().map(|c| c.to_ascii_lowercase()));
        assert!(data.len() == 2 * s.len());
        Ok(ColumnName { data })
    }
}

impl TryFrom<&String> for ColumnName {
    type Error = Error;

    fn try_from(s: &String) -> Result<Self, Self::Error> {
        Self::try_from(&s[..])
    }
}

impl FromStr for ColumnName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl Serialize for ColumnName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ColumnName {
    fn deserialize<D>(deserializer: D) -> Result<ColumnName, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        Ok(ColumnName::try_from(s).map_err(de::Error::custom)?)
    }
}

/// A wrapper type used to display column names in a quoted format.
///
/// We avoid defining `Display` directly on `ColumnName`, so that there's no way
/// to display it without making a decision.
pub(crate) struct ColumnNameQuoted<'a>(&'a ColumnName);

impl<'a> fmt::Display for ColumnNameQuoted<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Always quote, just in case the column name is a keyword.
        write!(f, "`{}`", self.0.as_str())
    }
}

/// A wrapper type used to display column names as quoted JavaScript
/// identifiers.
///
/// TODO: Do we need to anything special with case-handling here? BigQuery
/// ignores case, but JavaScript treats it as significant.
pub(crate) struct ColumnNameJavaScriptQuoted<'a>(&'a ColumnName);

impl<'a> fmt::Display for ColumnNameJavaScriptQuoted<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{}\"", self.0.as_str())
    }
}

#[test]
fn preserves_valid() {
    let valid_names = &["a", "A", "_", "a2", "AA", "A_", "abc"];
    for &n in valid_names {
        assert_eq!(ColumnName::try_from(n).unwrap().as_str(), n);
    }
}

#[test]
fn rejects_invalid() {
    // The Turkish dotted İ character would break our underlying `data` layout.
    let invalid_names = &["", "2", "a,", "é", "İ"];
    for &n in invalid_names {
        assert!(ColumnName::try_from(n).is_err());
    }
}

#[test]
fn ignores_case_for_comparison() {
    assert_eq!(
        ColumnName::try_from("a").unwrap(),
        ColumnName::try_from("A").unwrap(),
    );
    assert!(ColumnName::try_from("a").unwrap() < ColumnName::try_from("B").unwrap());
    assert!(ColumnName::try_from("A").unwrap() < ColumnName::try_from("b").unwrap());
}

#[test]
fn ignores_case_for_hash() {
    use std::collections::hash_map::DefaultHasher;

    let mut hasher_1 = DefaultHasher::new();
    ColumnName::try_from("a").unwrap().hash(&mut hasher_1);

    let mut hasher_2 = DefaultHasher::new();
    ColumnName::try_from("A").unwrap().hash(&mut hasher_2);

    assert_eq!(hasher_1.finish(), hasher_2.finish());
}

#[test]
fn format_preserves_case() {
    let s = "Aa";
    let name = ColumnName::from_str(s).unwrap();
    assert_eq!(format!("{}", name.quoted()), format!("`{}`", s));
    assert_eq!(format!("{:?}", name), format!("{:?}", s));
}
