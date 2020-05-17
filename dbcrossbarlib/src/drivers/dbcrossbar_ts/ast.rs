//! Support for schemas specif{ ty: (), location: (), message: ()}using a subset of TypeScript.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    hash::Hash,
    ops::Range,
    sync::Arc,
};

use crate::common::*;
use crate::parse_error::{Annotation, AnnotationType, Location, ParseError};
use crate::schema::{Column, DataType, StructField, Table};

/// We represent a span in our source code using a Rust range.
type Span = Range<usize>;

/// A node in our abstract syntax tree.
pub(crate) trait Node: fmt::Debug {
    /// The soure span corresponding to this node.
    fn span(&self) -> Span;
}

/// Interface for types which can be converted to `DataType`.
pub(crate) trait ToDataType {
    /// Convert this type to a `DataType`, given
    fn to_data_type(&self, source_file: &SourceFile) -> Result<DataType, ParseError>;
}

/// A TypeScript source file containing a limited set of type definitions.
pub(crate) struct SourceFile {
    /// The name of our input file.
    file_name: String,
    /// Our original input data. We manage this using an atomic reference count,
    /// so that we can share ownership with `ParseError`.
    file_string: Arc<String>,
    /// The type definitions found in this file.
    definitions: HashMap<String, Definition>,
}

impl SourceFile {
    /// Parse a source file from a `&str`.
    pub(crate) fn parse(
        file_name: String,
        file_string: String,
    ) -> Result<Self, ParseError> {
        let file_string = Arc::new(file_string);
        let definition_vec = typescript_grammar::definitions(file_string.as_ref())
            .map_err(|err| {
                ParseError::from_file_string(
                    file_name.clone(),
                    file_string.clone(),
                    vec![Annotation {
                        ty: AnnotationType::Primary,
                        location: Location::Position(err.location.offset),
                        message: format!("expected {}", err.expected),
                    }],
                    format!("error parsing {}", file_name),
                )
            })?;
        let mut definitions = HashMap::with_capacity(definition_vec.len());
        for d in definition_vec {
            let name = d.name().to_owned();
            if let Some(existing) = definitions.insert(name.as_str().to_owned(), d) {
                return Err(ParseError::from_file_string(
                    file_name,
                    file_string,
                    vec![
                        Annotation {
                            ty: AnnotationType::Secondary,
                            location: Location::Range(existing.name().span()),
                            message: "existing definition here".to_owned(),
                        },
                        Annotation {
                            ty: AnnotationType::Primary,
                            location: Location::Range(name.span()),
                            message: "duplicate definition here".to_owned(),
                        },
                    ],
                    format!("duplicate definition of {}", name),
                ));
            }
        }
        Ok(SourceFile {
            file_name,
            file_string,
            definitions,
        })
    }

    /// Look up a definition in this source file.
    pub(crate) fn definition<'a>(&'a self, name: &str) -> Result<&'a Definition> {
        self.definitions.get(name).ok_or_else(|| {
            format_err!("type `{}` is not defined in {}", name, self.file_name)
        })
    }

    /// Look up a definition in the source file using an identifier.
    fn definition_for_identifier<'a>(
        &'a self,
        id: &Identifier,
    ) -> Result<&'a Definition, ParseError> {
        self.definitions.get(id.as_str()).ok_or_else(|| {
            ParseError::from_file_string(
                self.file_name.clone(),
                self.file_string.clone(),
                vec![Annotation {
                    ty: AnnotationType::Primary,
                    location: Location::Range(id.span()),
                    message: "cannot find the definition of this type".to_string(),
                }],
                format!("cannot find definition of {}", id),
            )
        })
    }

    /// Look up a definition and recursively convert it to a portable [`Table`].
    pub(crate) fn definition_to_table(&self, name: &str) -> Result<Table> {
        let def = self.definition(name)?;
        match def.to_data_type(self)? {
            DataType::Struct(fields) => {
                Ok(Table {
                    name: name.to_owned(),
                    columns: fields.into_iter().map(|f| {
                        Column {
                            name: f.name,
                            is_nullable: f.is_nullable,
                            data_type: f.data_type,
                            comment: None,
                        }
                    }).collect(),
                })
            }
            _ => Err(ParseError::from_file_string(
                self.file_name.clone(),
                self.file_string.clone(),
                vec![Annotation {
                    ty: AnnotationType::Primary,
                    location: Location::Range(def.name().span()),
                    message: "expected an interface type".to_string(),
                }],
                format!("cannot convert {} to a table schema because it is not an interface", name),
            ).into()),
        }
    }

    /// Look up an identifier and recursively convert it to a [`DataType`].
    pub(crate) fn identifier_to_data_type(
        &self,
        id: &Identifier,
    ) -> Result<DataType, ParseError> {
        let def = self.definition_for_identifier(id)?;
        def.to_data_type(self)
    }
}

/// A type definition.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Definition {
    /// An interface definition.
    Interface(Interface),
    /// A type alias.
    TypeAlias(Identifier, Type),
}

impl Definition {
    pub(crate) fn name(&self) -> &Identifier {
        match self {
            Definition::Interface(iface) => &iface.name,
            Definition::TypeAlias(name, _) => name,
        }
    }
}

impl ToDataType for Definition {
    fn to_data_type(&self, source_file: &SourceFile) -> Result<DataType, ParseError> {
        match self {
            Definition::Interface(iface) => iface.to_data_type(source_file),
            Definition::TypeAlias(_, ty) => ty.to_data_type(source_file),
        }
    }
}

/// An `interface` type.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Interface {
    name: Identifier,
    fields: Vec<Field>,
}

impl ToDataType for Interface {
    fn to_data_type(&self, source_file: &SourceFile) -> Result<DataType, ParseError> {
        // Check for duplicate fields.
        //
        // TODO: Ideally, we would do this is a syntax-checking pass.
        let mut seen = HashSet::new();
        for f in &self.fields {
            if !seen.insert(f.name.clone()) {
                let existing = seen.get(&f.name).expect("item should be in set");
                return Err(ParseError::from_file_string(
                    source_file.file_name.clone(),
                    source_file.file_string.clone(),
                    vec![
                        Annotation {
                            ty: AnnotationType::Secondary,
                            location: Location::Range(existing.span()),
                            message: "original definition here".to_owned(),
                        },
                        Annotation {
                            ty: AnnotationType::Primary,
                            location: Location::Range(f.name.span()),
                            message: "defined again here".to_owned(),
                        },
                    ],
                    format!("duplicate definition of {} field", f.name),
                ));
            }
        }

        // Convert our struct.
        let fields = self
            .fields
            .iter()
            .map(|f| {
                let (optional, ty) = f.ty.to_possibly_optional_type();
                Ok(StructField {
                    name: f.name.as_str().to_owned(),
                    is_nullable: f.optional | optional,
                    data_type: ty.to_data_type(source_file)?,
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(DataType::Struct(fields))
    }
}

/// A field in an interface (or other struct-like type).
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Field {
    name: Identifier,
    optional: bool,
    ty: Type,
}

/// A TypeScript type, without any span information.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum TypeDetails {
    /// Any value.
    Any,
    //// An array type.
    Array(Box<Type>),
    /// A true or false value.
    Boolean,
    /// The null value.
    Null,
    /// A 64-bit floating point number.
    Number,
    /// A reference to another type by name.
    Ref(Identifier),
    /// A string.
    String,
    /// A type union (made with `|`).
    Union(Box<Type>, Box<Type>),
}

/// A TypeScript type.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Type {
    span: Span,
    details: TypeDetails,
}

impl Type {
    /// Is this type equivalent to an SQL NULL?
    fn is_sql_null(&self) -> bool {
        match &self.details {
            TypeDetails::Null => true,
            _ => false,
        }
    }

    /// If this type is `x | null` or `null | x`, return `(true, x)`. Otherwise,
    /// return `(false, self)`.
    fn to_possibly_optional_type(&self) -> (bool, &Type) {
        match &self.details {
            TypeDetails::Union(t1, t2) if t2.is_sql_null() => (true, t1),
            TypeDetails::Union(t1, t2) if t1.is_sql_null() => (true, t2),
            _ => (false, self),
        }
    }
}

impl ToDataType for Type {
    fn to_data_type(&self, source_file: &SourceFile) -> Result<DataType, ParseError> {
        match &self.details {
            TypeDetails::Any => Ok(DataType::Json),

            TypeDetails::Array(elem) => elem
                .to_data_type(source_file)
                .map(|ty| DataType::Array(Box::new(ty))),

            TypeDetails::Boolean => Ok(DataType::Bool),

            TypeDetails::Null => Err(ParseError::from_file_string(
                source_file.file_name.clone(),
                source_file.file_string.clone(),
                vec![Annotation {
                    ty: AnnotationType::Primary,
                    location: Location::Range(self.span.clone()),
                    message: "null type found here".to_string(),
                }],
                "cannot convert `null` type to dbcrossbar type".to_owned(),
            )),

            TypeDetails::Number => Ok(DataType::Float64),

            TypeDetails::Ref(id) => source_file.identifier_to_data_type(&id),

            TypeDetails::String => Ok(DataType::Text),

            TypeDetails::Union(_, _) => Err(ParseError::from_file_string(
                source_file.file_name.clone(),
                source_file.file_string.clone(),
                vec![Annotation {
                    ty: AnnotationType::Primary,
                    location: Location::Range(self.span.clone()),
                    message: "union type found here".to_string(),
                }],
                "cannot convert union type to dbcrossbar type".to_owned(),
            )),
        }
    }
}

/// A TypeScript identifier.
#[derive(Clone, Debug)]
pub(crate) struct Identifier(Span, String);

impl Identifier {
    /// The underlying string for this identifier.
    fn as_str(&self) -> &str {
        &self.1
    }
}

/// Two identifiers are equal if their name is equal, ignoring the span information.
impl PartialEq for Identifier {
    fn eq(&self, other: &Self) -> bool {
        self.1.eq(&other.1)
    }
}

/// Two identifiers are equal if their name is equal, ignoring the span information.
impl Eq for Identifier {}

/// Two identifiers hash the same if their names hash the same, ignoring the
/// span information.
impl Hash for Identifier {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.1.hash(state)
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}`", self.1)
    }
}

impl Node for Identifier {
    fn span(&self) -> Span {
        self.0.clone()
    }
}

peg::parser! {
    grammar typescript_grammar() for str {
        pub(crate) rule definitions() -> Vec<Definition>
            = ws()? definitions:(definition() ** (ws()?)) ws()? {
                definitions
            }

        rule definition() -> Definition
            = iface:interface() { Definition::Interface(iface) }
            / "type" ws() name:identifier() ws()? "=" ws()? ty:ty() ws()? ";" {
                Definition::TypeAlias(name, ty)
            }

        rule interface() -> Interface
            = "interface" ws() name:identifier() ws()? "{" fields:fields() "}" ws()? ";" {
                Interface { name, fields }
            }

        rule fields() -> Vec<Field>
            = ws()? fields:(field() ** (ws()? "," ws()?)) (ws()? ",")? ws()? { fields }

        rule field() -> Field
            = name:identifier() optional:optional_mark() ":" ws()? ty:ty() {
                Field { name, optional, ty }
            }

        // For optional fields.
        rule optional_mark() -> bool
            = ws()? "?" { true }
            / { false }

        // Type expressions. We parse this using `precedence!`, which parses
        // prefix, postfix, left-associative and right associative operators
        // using the "packrat" algorithm, so we don't need to write this rule
        // out the hard way.
        //
        // The lowest-pecedence operators are on top, and the highest on bottom.
        rule ty() -> Type = precedence! {
            // This rule is special: it exists to wrap all the other rules in a
            // `Type` with span information. The `position!()` macro only works
            // in this rule.
            s:position!() details:@ e:position!() {
                Type { span: s..e, details }
            }
            --
            left:(@) ws()? "|" ws()? right:@ {
                TypeDetails::Union(Box::new(left), Box::new(right))
            }
            --
            elem:@ ws()? "[" ws()? "]" e:position!() {
                TypeDetails::Array(Box::new(elem))
            }
            --
            "any" { TypeDetails::Any }
            "string" { TypeDetails::String }
            "null" { TypeDetails::Null }
            "number" { TypeDetails::Number }
            "boolean" { TypeDetails::Boolean }
            id:identifier() { TypeDetails::Ref(id) }
        }

        rule identifier() -> Identifier
            = quiet! {
                s:position!()
                id:$(
                    ['A'..='Z' | 'a'..='z' | '_']
                    ['A'..='Z' | 'a'..='z' | '_' | '0'..='9']*
                )
                e:position!()
                { Identifier(s..e, id.to_owned()) }

            }
            / expected!("identifier")

        rule ws() = quiet! { ([' ' | '\t' | '\r' | '\n'] / line_comment())+ }

        rule line_comment() = "//" (!['\n'][_])* ( "\n" / ![_] )
    }
}

// Use `main_error` for pretty test output.
#[test]
fn parses_typescript_and_converts_to_data_type() -> Result<(), main_error::MainError> {
    let input = r#"
interface PriceSet {
    shop_money: Money,
    presentement_money: Money,
};
    
interface Money {
    amount: string, // Currency decimal encoded as string.
    currency_code: string,
};
"#;
    let source_file = SourceFile::parse("test.ts".to_owned(), input.to_owned())?;
    assert_eq!(
        source_file.definition_to_table("PriceSet")?,
        Table {
            name: "PriceSet".to_owned(),
            columns: vec![
                Column {
                    name: "shop_money".to_owned(),
                    is_nullable: false,
                    data_type: DataType::Struct(vec![
                        StructField {
                            name: "amount".to_owned(),
                            is_nullable: false,
                            data_type: DataType::Text,
                        },
                        StructField {
                            name: "currency_code".to_owned(),
                            is_nullable: false,
                            data_type: DataType::Text,
                        },
                    ]),
                    comment: None,
                },
                Column {
                    name: "presentement_money".to_owned(),
                    is_nullable: false,
                    data_type: DataType::Struct(vec![
                        StructField {
                            name: "amount".to_owned(),
                            is_nullable: false,
                            data_type: DataType::Text,
                        },
                        StructField {
                            name: "currency_code".to_owned(),
                            is_nullable: false,
                            data_type: DataType::Text,
                        },
                    ]),
                    comment: None,
                },
            ]
        },
    );

    Ok(())
}

#[test]
fn detects_duplicate_field_names() {
    let input = r#"
interface Point {
    x: number,
    x: number,
};
"#;
    // At some point is the future, we might fail on `parse` instead.
    let source_file =
        SourceFile::parse("test.ts".to_owned(), input.to_owned()).unwrap();
    assert!(source_file.definition_to_table("Point").is_err());
}

// Use `main_error` for pretty test output.
#[test]
fn parses_shopify_schema() -> Result<(), main_error::MainError> {
    let file_string = include_str!("shopify.ts");
    let source_file =
        SourceFile::parse("shopify.ts".to_owned(), file_string.to_owned())?;
    for def in &["Order"] {
        source_file.definition_to_table(def)?;
    }
    Ok(())
}
