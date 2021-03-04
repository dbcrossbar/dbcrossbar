//! This file contains a [`rust-peg`][peg] grammar. A "PEG" is a "parser
//! expression grammar". It's basically similar to a regular expression,
//! except it can contain recursive rules. See the site for an overview
//! of the basic syntax.
//!
//! `#quiet` and `#expected` are used in a few places to give better
//! error messages. `#quiet` suppresses certain possible tokens (such as
//! whitespace) from "expected ___" errors, and `#expected` allows us to
//! insert descriptive names into those messages.
//!
//! [peg]: https://github.com/kevinmehall/rust-peg

use super::super::{
    PgColumn, PgCreateTable, PgCreateType, PgCreateTypeDefinition, PgDataType, PgName,
    PgScalarDataType, PgSchema,
};
use crate::schema::Srid;

pub(crate) use schema_grammar::schema as parse;

/// A top-level definition in the SQL. We'll separate these into multiple lists
/// before returning.
pub(self) enum Definition {
    /// `CREATE TYPE`.
    Type(PgCreateType),
    /// `CREATE TABLE`.
    Table(PgCreateTable),
}

/// Group `CREATE` definitions by type.
pub(self) fn group_definitions(
    defs: Vec<Definition>,
) -> (Vec<PgCreateType>, Vec<PgCreateTable>) {
    let mut types = vec![];
    let mut tables = vec![];
    for d in defs {
        match d {
            Definition::Type(ty) => types.push(ty),
            Definition::Table(table) => tables.push(table),
        }
    }
    (types, tables)
}

peg::parser! {
    grammar schema_grammar() for str {
        /// A mix of tables and data types.
        pub(crate) rule schema() -> PgSchema
            = ws()? defs:definition() ** (ws()? ";" ws()?) (";" ws()?)?
            {
                let (types, tables) = group_definitions(defs);
                PgSchema { types, tables }
            }

        /// Either a `CREATE TYPE` definition or a `CREATE TABLE` definition.
        rule definition() -> Definition
            = def:create_type() { Definition::Type(def) }
            / def:create_table() { Definition::Table(def) }

        /// A `CREATE TYPE` definition.
        rule create_type() -> PgCreateType
            = i("CREATE") ws() i("TYPE") ws() name:name() ws() i("AS")
              ws() definition:create_type_definition()
            {
                PgCreateType { name, definition }
            }

        /// The body of a `CREATE TYPE` definition.
        rule create_type_definition() -> PgCreateTypeDefinition
            = create_type_enum_definition()

        /// An `ENUM` in the body of a `CREATE TYPE` definition.
        rule create_type_enum_definition() -> PgCreateTypeDefinition
            = i("ENUM") ws() "(" ws()? values:string_constant() ** (ws()? "," ws()?) ws()? ")"
            {
                PgCreateTypeDefinition::Enum(values)
            }

        /// A `CREATE TABLE` expression.
        rule create_table() -> PgCreateTable
            = i("CREATE") ws() (i("UNLOGGED") ws())? i("TABLE") ws() name:name() ws()? "("
                ws()? columns:(column() ** (ws()? "," ws()?)) ws()?
            ")" ws()? (";" ws()?)?
            {
                PgCreateTable {
                    name,
                    columns,
                    if_not_exists: false,
                    // We don't worry about trying to parse this, which we only use
                    // internally at the moment.
                    temporary: false,
                }
            }

        /// A column expression of the form "name type".
        rule column() -> PgColumn
            = name:identifier() ws() data_type:data_type() is_nullable:is_nullable() primary_key()? {
                PgColumn {
                    name,
                    is_nullable,
                    data_type,
                }
            }

        /// An optional `NOT NULL` expression.
        rule is_nullable() -> bool
            = ws() i("NOT") ws() i("NULL") { false }
            / { true }

        /// A `PRIMARY KEY` specifier. We can ignore this.
        rule primary_key()
            = ws() i("PRIMARY") ws() i("KEY")

        /// A Postgres data type.
        rule data_type() -> PgDataType
            = quiet! {
                // Array type.
                data_type:scalar_data_type() ws()? "[" ws()? "]" {
                    PgDataType::Array { dimension_count: 1, ty: data_type }
                }

                // All other types.
                / data_type:scalar_data_type() { PgDataType::Scalar(data_type) }
            }
            / expected!("data type")

        /// A scalar data type, never an array.
        rule scalar_data_type() -> PgScalarDataType
            = i("bigint") { PgScalarDataType::Bigint }
            / i("boolean") { PgScalarDataType::Boolean }
            / i("character") ( ws()? "(" ws()? ['0'..='9']+ ws()? ")" )? { PgScalarDataType::Text }
            / i("citext") { PgScalarDataType::Text }
            / i("date") { PgScalarDataType::Date }
            / i("double") ws() i("precision") { PgScalarDataType::DoublePrecision }
            / i("float") { PgScalarDataType::DoublePrecision }
            / i("public.")? i("geometry") ws()? "(" ws()? identifier() ws()? "," ws()? srid:srid() ws()? ")" {
                PgScalarDataType::Geometry(Srid::new(srid))
            }
            / i("integer") { PgScalarDataType::Int } // Longer keyword first!
            / i("int") { PgScalarDataType::Int }
            / i("jsonb") { PgScalarDataType::Jsonb }
            / i("json") { PgScalarDataType::Json }
            / i("numeric") { PgScalarDataType::Numeric }
            / i("real") { PgScalarDataType::Real }
            / i("smallint") { PgScalarDataType::Smallint }
            / i("text") { PgScalarDataType::Text }
            / i("timestamp") ws() i("with") ws() i("time") ws() i("zone") {
                PgScalarDataType::TimestampWithTimeZone
            }
            / i("timestamp") ws() i("without") ws() i("time") ws() i("zone") {
                PgScalarDataType::TimestampWithoutTimeZone
            }
            / i("timestamp") {
                PgScalarDataType::TimestampWithoutTimeZone
            }
            / i("uuid") { PgScalarDataType::Uuid }
            / name:name() { PgScalarDataType::Named(name) }

        /// A GeoJSON SRID number, used to identify a coordinate system.
        rule srid() -> u32
            = srid:$(['0'..='9']+) { srid.parse().expect("should always parse") }

        /// The name of a table.
        rule name() -> PgName
            = table:identifier() {
                PgName::new(None, table)
            }
            / schema:identifier() "." table:identifier() {
                PgName::new(schema, table)
            }

        /// An SQL identifier.
        rule identifier() -> String
            = quiet! {
                // Unquoted identifier.
                id:$(
                    ['A'..='Z' | 'a'..='z' | '_']
                    ['A'..='Z' | 'a'..='z' | '_' | '0'..='9' |'$']*
                ) { id.to_string() }

                // Double-quoted identifier.
                / "\"" quoted:$((  !['"'][_] / "\"\"")*) "\"" {
                    quoted.replace("\"\"", "\"")
                }
            }
            / expected!("identifier")

        /// An ordinary PostgreSQL string literal.
        rule string_constant() -> String
            = "'" text:$(( !"'" [_] / "''" )*) "'"
            {
                text.replace("''", "'")
            }

        /// One or more characters of whitespace, including comments.
        rule ws() = quiet! {
            ([' ' | '\t' | '\r' | '\n'] / ("--" (!['\n'][_])* "\n"))+
        }

        /// Match a string literal, ignoring case.
        rule i(literal: &'static str)
            // From https://github.com/kevinmehall/rust-peg/issues/216.
            = input:$([_]*<{literal.len()}>) {?
                if input.eq_ignore_ascii_case(literal) {
                    Ok(())
                } else {
                    Err(literal)
                }
            }
    }
}
