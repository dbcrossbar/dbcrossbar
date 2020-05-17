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
    /// The source span corresponding to this node.
    fn span(&self) -> Span;

    /// Recursively chec this node and all its child nodes for correctness.
    fn check(&self, source_file: &SourceFile) -> Result<(), ParseError>;
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
        // Parse our input file into statements.
        let file_string = Arc::new(file_string);
        let statements = typescript_grammar::statements(file_string.as_ref())
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

        // Keep just the definitions.
        let definitions_vec = statements
            .into_iter()
            .filter_map(Statement::definition)
            .collect::<Vec<_>>();

        // Build a `HashMap` of our definitions, returning an error if we find
        // duplicate names.
        let mut definitions = HashMap::with_capacity(definitions_vec.len());
        for d in definitions_vec {
            let name = d.name().to_owned();
            if let Some(existing) = definitions.insert(name.as_str().to_owned(), d) {
                return Err(ParseError::from_file_string(
                    file_name,
                    file_string,
                    vec![
                        Annotation {
                            ty: AnnotationType::Primary,
                            location: Location::Range(name.span()),
                            message: "duplicate definition here".to_owned(),
                        },
                        Annotation {
                            ty: AnnotationType::Secondary,
                            location: Location::Range(existing.name().span()),
                            message: "existing definition here".to_owned(),
                        },
                    ],
                    format!("duplicate definition of {}", name),
                ));
            }
        }

        // Build our `source_file` and check it for correctness.
        let source_file = SourceFile {
            file_name,
            file_string,
            definitions,
        };
        for d in source_file.definitions.values() {
            d.check(&source_file)?;
        }
        Ok(source_file)
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

#[derive(Debug)]
pub(crate) enum Statement {
    Definition(Definition),
    Empty,
}

impl Statement {
    fn definition(self) -> Option<Definition> {
        match self {
            Statement::Definition(d) => Some(d),
            Statement::Empty => None,
        }
    }
}

/// A type definition.
#[derive(Debug)]
pub(crate) enum Definition {
    /// An interface definition.
    Interface(Interface),
    /// A type alias.
    TypeAlias(Span, Identifier, Type),
}

impl Definition {
    pub(crate) fn name(&self) -> &Identifier {
        match self {
            Definition::Interface(iface) => &iface.name,
            Definition::TypeAlias(_, name, _) => name,
        }
    }
}

impl Node for Definition {
    fn span(&self) -> Span {
        match self {
            Definition::Interface(iface) => iface.span(),
            Definition::TypeAlias(span, _, _) => span.to_owned(),
        }
    }

    fn check(&self, source_file: &SourceFile) -> Result<(), ParseError> {
        match self {
            Definition::Interface(iface) => iface.check(source_file),
            Definition::TypeAlias(_, name, ty) => {
                name.check(source_file)?;
                ty.check(source_file)
            }
        }
    }
}

impl ToDataType for Definition {
    fn to_data_type(&self, source_file: &SourceFile) -> Result<DataType, ParseError> {
        match self {
            Definition::Interface(iface) => iface.to_data_type(source_file),
            Definition::TypeAlias(_, _, ty) => ty.to_data_type(source_file),
        }
    }
}

/// An `interface` type.
#[derive(Debug)]
pub(crate) struct Interface {
    span: Span,
    name: Identifier,
    fields: Vec<Field>,
}

impl Node for Interface {
    fn span(&self) -> Span {
        self.span.clone()
    }

    fn check(&self, source_file: &SourceFile) -> Result<(), ParseError> {
        self.name.check(source_file)?;

        // Check our field.
        let mut seen = HashSet::new();
        for f in &self.fields {
            // Check for duplicate field names.
            if !seen.insert(f.name.clone()) {
                let existing = seen.get(&f.name).expect("item should be in set");
                return Err(ParseError::from_file_string(
                    source_file.file_name.clone(),
                    source_file.file_string.clone(),
                    vec![
                        Annotation {
                            ty: AnnotationType::Primary,
                            location: Location::Range(f.name.span()),
                            message: "defined again here".to_owned(),
                        },
                        Annotation {
                            ty: AnnotationType::Secondary,
                            location: Location::Range(existing.span()),
                            message: "original definition here".to_owned(),
                        },
                    ],
                    format!("duplicate definition of {} field", f.name),
                ));
            }

            // Check our field itself.
            f.check(source_file)?;
        }
        Ok(())
    }
}

impl ToDataType for Interface {
    fn to_data_type(&self, source_file: &SourceFile) -> Result<DataType, ParseError> {
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
#[derive(Debug)]
pub(crate) struct Field {
    span: Span,
    name: Identifier,
    optional: bool,
    ty: Type,
}

impl Node for Field {
    fn span(&self) -> Span {
        self.span.clone()
    }

    fn check(&self, source_file: &SourceFile) -> Result<(), ParseError> {
        self.name.check(source_file)?;
        self.ty.check(source_file)
    }
}

/// A TypeScript type, without any span information.
#[derive(Debug)]
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
#[derive(Debug)]
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

impl Node for Type {
    fn span(&self) -> Span {
        self.span.clone()
    }

    fn check(&self, source_file: &SourceFile) -> Result<(), ParseError> {
        match &self.details {
            TypeDetails::Any
            | TypeDetails::Boolean
            | TypeDetails::Null
            | TypeDetails::Number
            | TypeDetails::String => Ok(()),

            TypeDetails::Array(elem_ty) => elem_ty.check(source_file),

            // Make sure names refer to something.
            TypeDetails::Ref(id) => {
                source_file.definition_for_identifier(id).map(|_| ())
            }

            TypeDetails::Union(t1, t2) => {
                t1.check(source_file)?;
                t2.check(source_file)
            }
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

    fn check(&self, _source_file: &SourceFile) -> Result<(), ParseError> {
        // There's nothing to check here, not really.
        Ok(())
    }
}

peg::parser! {
    grammar typescript_grammar() for str {
        pub(crate) rule statements() -> Vec<Statement>
            = ws()? statements:(statement() ** (ws()?)) ws()? {
                statements
            }

        rule statement() -> Statement
            = d:definition() { Statement::Definition(d) }
            / ";" { Statement::Empty }

        rule definition() -> Definition
            = iface:interface() { Definition::Interface(iface) }
            / s:position!() "type" ws() name:identifier() ws()? "=" ws()?
                ty:ty() ws()? e:position!() ";"
            {
                Definition::TypeAlias(s..e, name, ty)
            }

        rule interface() -> Interface
            = s:position!() "interface" ws() name:identifier() ws()? "{"
                ws()? fields:fields() ws()? "}" e:position!()
            {
                Interface { span: s..e, name, fields }
            }

        rule fields() -> Vec<Field>
            = fields:(field() ** (ws()? "," ws()?)) (ws()? ",")? { fields }

        rule field() -> Field
            = s:position!() name:identifier() optional:optional_mark() ":" ws()?
                ty:ty() e:position!()
            {
                Field { span: s..e, name, optional, ty }
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
            elem:@ ws()? "[" ws()? "]" {
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
    // This error should be detected at `parse` time.
    assert!(SourceFile::parse("test.ts".to_owned(), input.to_owned()).is_err());
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
