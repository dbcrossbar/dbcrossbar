use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use std::{ops::Range, str::FromStr, sync::Arc};

use crate::common::*;
use crate::parse_error::{Annotation, FileInfo, ParseError};

/// Driver-specific arguments.
#[derive(Clone, Debug, Default)]
pub struct DriverArguments {
    /// A list of arguments pairs, in order.
    args: Vec<Arg>,
}

impl DriverArguments {
    /// Parse a list of command-line arguments of the form `key=value` into a
    /// `DriverArgs` structure.
    pub fn from_cli_args<I>(args: I) -> Result<Self>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let args = args
            .into_iter()
            .map(|s| Arg::from_str(s.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { args })
    }

    /// Is this collection of driver arguments empty?
    pub(crate) fn is_empty(&self) -> bool {
        self.args.is_empty()
    }

    /// Convert these arguments to a JSON object. We treat keys of the form
    /// "parent.nested" as `{ "parent": { "nested": ... } }`.
    fn to_json(&self) -> Result<Value> {
        let mut json = Value::Object(Map::new());
        for arg in &self.args {
            insert_into_json(
                &mut json,
                &arg.file_info,
                &arg.name.0,
                arg.value.to_owned(),
            )?;
        }
        Ok(json)
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
    let raw_args = &["a=b", "c.d=x", "c.e[]=y", "c[e][]=z"];
    let args = DriverArguments::from_cli_args(raw_args).unwrap();
    assert_eq!(
        args.to_json().unwrap(),
        json!({"a": "b", "c": { "d": "x", "e": ["y", "z"] } }),
    );
}

#[test]
fn to_json_detects_conflicts() {
    let conflicts = &[&["a=x", "a=y"], &["a=x", "a.b=y"], &["a=x", "a[]=y"]];
    for &conflict in conflicts {
        let conflicting_args = DriverArguments::from_cli_args(conflict).unwrap();
        assert!(conflicting_args.to_json().is_err());
    }
}

/// The name of a driver argument.
#[derive(Clone, Debug)]
struct Arg {
    file_info: Arc<FileInfo>,
    name: ArgName,
    value: Value,
}

impl FromStr for Arg {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let name = "driver argument";
        let file_info = Arc::new(FileInfo::new(name.to_owned(), s.to_owned()));
        grammar::arg(s, &file_info).map_err(|err| {
            ParseError::new(
                file_info.clone(),
                vec![Annotation::primary(
                    err.location.offset,
                    format!("expected {}", err.expected),
                )],
                format!("error parsing {}", file_info.name),
            )
        })
    }
}

/// An `ArgName` must start with a `Component::Member`, followed any number of
/// non-`Component::FinalArray` value, and finish with a optional
/// `Component::FinalArray`. These rules are enforced by the parser's grammar.
#[derive(Clone, Debug)]
struct ArgName(Vec<Component>);

/// A component of the name of a driver argument.
#[derive(Clone, Debug)]
enum Component {
    /// A ".name" or `[name]` expression.
    Member(Range<usize>, String),
    /// A "[]" expression, which can only appear at the end.
    FinalArray(Range<usize>),
}

peg::parser! {
    /// A grammar for parsing driver argument names and values.
    grammar grammar() for str {
        /// A driver argument.
        pub(super) rule arg(file_info: &Arc<FileInfo>) -> Arg
            = name:arg_name() "=" value:value() { Arg { file_info: file_info.to_owned(), name, value }}

        /// A driver argument name.
        rule arg_name() -> ArgName
            = initial:initial() rest:(component()*) final_array:final_array() {
                let mut path = Vec::with_capacity(2 + rest.len());
                path.push(initial);
                path.extend(rest);
                if let Some(final_array) = final_array {
                    path.push(final_array);
                }
                ArgName(path)
            }

        /// The first component of a driver argument.
        rule initial() -> Component
            = s:position!() id:id() e:position!() { Component::Member(s..e, id) }

        /// A single component in the name of a driver argument.
        rule component() -> Component
            = s:position!() "." id:id() e:position!() { Component::Member(s..e, id) }
            / s:position!() "[" id:id() "]" e:position!() { Component::Member(s..e, id) }

        /// An optional final `[]` component.
        rule final_array() -> Option<Component>
           = s:position!() "[]" e:position!() { Some(Component::FinalArray(s..e)) }
           / { None }

        /// An identifier.
        rule id() -> String
            = quiet! {
                id:$(
                    ['A'..='Z' | 'a'..='z' | '_']
                    ['A'..='Z' | 'a'..='z' | '_' | '0'..='9']*
                )
                { id.to_owned() }
            }
            / expected!("identifier")

        rule value() -> Value
            = s:$([_]*) { Value::String(s.to_owned()) }
    }
}

/// Insert `value` into `json` at `path`.
///
/// `path` must never be empty. If `json` is `Value::Null`, it will be replaced
/// with either a JSON object or a JSON array, depending on the type of the next
/// component in `path`.
fn insert_into_json(
    json: &mut Value,
    file_info: &Arc<FileInfo>,
    path: &[Component],
    value: Value,
) -> Result<(), ParseError> {
    // We should never be called with an empty path.
    assert!(!path.is_empty());

    // Helper function that builds a useful error.
    let conflict_err =
        |pos: &Range<usize>, message: &'static str, existing: &Value| {
            let existing = serde_json::to_string(existing).unwrap();
            ParseError::new(
                file_info.to_owned(),
                vec![Annotation::primary(pos.to_owned(), "conflict here")],
                format!("{}, but earlier arguments specified {}", message, existing),
            )
        };

    // Create a location where our next value will be stored, and temporarily
    // fill it with `Value::None`.
    let (pos, place) = match &path[0] {
        Component::Member(pos, key) => {
            if let Value::Null = json {
                *json = Value::Object(Map::new());
            }
            if let Value::Object(obj) = json {
                Ok((pos, obj.entry(key).or_insert(Value::Null)))
            } else {
                Err(conflict_err(pos, "tried to insert into a hash", json))
            }
        }
        Component::FinalArray(pos) => {
            if let Value::Null = json {
                *json = Value::Array(vec![]);
            }
            if let Value::Array(arr) = json {
                arr.push(Value::Null);
                Ok((pos, arr.last_mut().expect("array should have item")))
            } else {
                Err(conflict_err(pos, "tried to append to an array", json))
            }
        }
    }?;

    // Decide what to store in `place`.
    if path.len() == 1 {
        // This is our last path component, so store our value.
        if let Value::Null = place {
            *place = value;
            Ok(())
        } else {
            Err(conflict_err(pos, "tried to set a value", json))
        }
    } else {
        // We have more path components, so recurse.
        insert_into_json(place, file_info, &path[1..], value)
    }
}
