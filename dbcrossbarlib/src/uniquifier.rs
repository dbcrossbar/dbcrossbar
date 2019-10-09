//! Make unique identifiers from messy strings.

use std::collections::HashSet;

use crate::common::*;

/// Turns arbitrary Unicode names into unique, lowercase ASCII identifiers. All
/// identifiers start with an underscore or a lowercase ASCII letter, followed
/// by zero or more underscores, lowercase ASCII letters and digits.
#[derive(Debug, Default)]
pub(crate) struct Uniquifier {
    /// Identifiers that we have already generated.
    used: HashSet<String>,
}

impl Uniquifier {
    /// Given a `name`, return an idenfitier
    pub(crate) fn unique_id_for<'a>(&mut self, name: &'a str) -> Result<&str> {
        let id = name_to_lowercase_id(name);
        if self.used.insert(id.to_owned()) {
            Ok(&self.used.get(&id).expect("just verified id was present")[..])
        } else {
            let mut offset = 1;
            while offset < 50 {
                offset += 1;
                let alt_id = format!("{}_{}", id, offset);
                if self.used.insert(alt_id.to_owned()) {
                    return Ok(&self
                        .used
                        .get(&alt_id)
                        .expect("just verified alt_id was present")[..]);
                }
            }
            Err(format_err!("too many name collisions"))
        }
    }
}

#[test]
fn uniquifier_generates_unique_ids() {
    let examples = &[
        ("a", "a"),
        ("A", "a_2"),
        ("a_2", "a_2_2"), // Sneaky.
        ("B", "b"),
    ];
    let mut uniqifier = Uniquifier::default();
    for &(input, expected) in examples {
        assert_eq!(uniqifier.unique_id_for(input).unwrap(), expected);
    }
}

/// Given a unique string, turn it into a lower-case identifier.
fn name_to_lowercase_id(name: &str) -> String {
    if name.is_empty() {
        "_".to_owned()
    } else {
        name.char_indices()
            .map(|(idx, c)| {
                if c == '_' || c.is_ascii_lowercase() {
                    c
                } else if c.is_ascii_uppercase() {
                    c.to_ascii_lowercase()
                } else if idx != 0 && c.is_ascii_digit() {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>()
    }
}

#[test]
fn name_to_lowercase_id_cleans_non_id_characters() {
    let examples = &[("", "_"), ("_aA1?", "_aa1_"), ("1", "_")];
    for &(input, expected) in examples {
        assert_eq!(name_to_lowercase_id(input), expected);
    }
}
