//! A Trino-compatible `CREATE TABLE` statement.

use std::{fmt, sync::Arc};

use crate::{
    common::*,
    parse_error::{Annotation, FileInfo, ParseError},
    schema::Column,
};

use super::{TrinoDataType, TrinoField, TrinoIdent, TrinoTableName};

/// A Trino-compatible `CREATE TABLE` statement.
#[derive(Clone, Debug)]
pub struct TrinoCreateTable {
    name: TrinoTableName,
    columns: Vec<TrinoColumn>,
}

impl TrinoCreateTable {
    /// Parse from an SQL string. `path` is used for error messages.
    pub(crate) fn parse(
        path: &str,
        sql: &str,
    ) -> Result<TrinoCreateTable, ParseError> {
        let file_info = Arc::new(FileInfo::new(path.to_owned(), sql.to_owned()));
        trino_parser::create_table(&file_info.contents).map_err(|err| {
            ParseError::new(
                file_info,
                vec![Annotation::primary(
                    err.location.offset,
                    format!("expected {}", err.expected),
                )],
                "error parsing Postgres CREATE TABLE",
            )
        })
    }

    /// Create from a table name and a portable schema.
    pub fn from_schema_and_name(
        schema: &Schema,
        name: &TrinoTableName,
    ) -> Result<Self> {
        let columns = schema
            .table
            .columns
            .iter()
            .map(|column| TrinoColumn::from_column(schema, column))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            name: name.clone(),
            columns,
        })
    }

    /// Convert to a portable schema.
    pub fn to_schema(&self) -> Result<Schema> {
        let columns = self
            .columns
            .iter()
            .map(|column| column.to_column())
            .collect::<Result<Vec<_>>>()?;
        Schema::from_table(Table {
            // Leave out any schema or catalog.
            name: self.name.table().as_unquoted_str().to_owned(),
            columns,
        })
    }
}

impl fmt::Display for TrinoCreateTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE TABLE {} (\n    ", self.name)?;
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ",\n    ")?;
            }
            write!(f, "{}", column)?;
        }
        write!(f, "\n);\n")
    }
}

/// A Trino column.
#[derive(Clone, Debug)]
pub struct TrinoColumn {
    name: TrinoIdent,
    data_type: TrinoDataType,
    is_nullable: bool,
}

impl TrinoColumn {
    /// Construct from a portable column.
    pub fn from_column(schema: &Schema, column: &Column) -> Result<Self> {
        Ok(Self {
            name: TrinoIdent::new(&column.name)?,
            data_type: TrinoDataType::from_data_type(schema, &column.data_type)?,
            is_nullable: column.is_nullable,
        })
    }

    /// Convert to a portable column.
    pub fn to_column(&self) -> Result<Column> {
        Ok(Column {
            name: self.name.as_unquoted_str().to_owned(),
            is_nullable: self.is_nullable,
            data_type: self.data_type.to_data_type()?,
            comment: None,
        })
    }
}

impl fmt::Display for TrinoColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)?;
        if !self.is_nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

// `rustpeg` grammar for parsing Trino data types.
peg::parser! {
    grammar trino_parser() for str {
        rule _ = quiet! { (
            [' ' | '\t' | '\r' | '\n']
            / "--" ([^'\n']* "\n")
            / "/*" (!"*/" [_])* "*/"
        )* }

        // Case-insensitive keywords.
        rule k(kw: &'static str) -> &'static str
            = quiet! { s:$(['a'..='z' | 'A'..='Z' | '_'] ['a'..='z' | 'A'..='Z' | '_' | '0'..='9']*) {?
                if s.eq_ignore_ascii_case(kw) {
                    Ok(kw)
                } else {
                    Err(kw)
                }
            } }
            / expected!(kw)

        rule ident() -> TrinoIdent
            // Note: No leading underscores allowed.
            = quiet! {
                s:$(['a'..='z' | 'A'..='Z'] ['a'..='z' | 'A'..='Z' | '_' | '0'..='9']*) {
                    TrinoIdent::new(s).unwrap()
                }
                / "\"" s:$(([^ '"'] / "\"\"")*) "\"" {
                    TrinoIdent::new(&s.replace("\"\"", "\"")).unwrap()
                }
            } / expected!("identifier")

        rule table_name() -> TrinoTableName
            // Match these as a list of identifiers using `**<1,3>`, because
            // otherwise the backtracking and error messages can be slightly
            // fiddly.
            = quiet! { idents:(ident() **<1,3> (_? "." _?)) {
                match idents.len() {
                    1 => TrinoTableName::Table(idents[0].clone()),
                    2 => TrinoTableName::Schema(idents[0].clone(), idents[1].clone()),
                    3 => TrinoTableName::Catalog(idents[0].clone(), idents[1].clone(), idents[2].clone()),
                    _ => unreachable!(),
                }
            } }
            / expected!("table name")

        // An integer literal.
        rule uint() -> u32
            // `unwrap` is safe because the parser controls our input.
            = quiet! { n:$(['0'..='9']+) { n.parse().unwrap() } }
            / expected!("integer")

        rule size_opt() -> Option<u32>
            = _? "(" _? size:uint() _? ")" { Some(size) }
            / { None }

        rule size_default(default: u32) -> u32
            = _? "(" _? size:uint() _? ")" { size }
            / { default }

        rule boolean_ty() -> TrinoDataType
            = k("boolean") { TrinoDataType::Boolean }

        rule tinyint_ty() -> TrinoDataType
            = k("tinyint") { TrinoDataType::TinyInt }

        rule smallint_ty() -> TrinoDataType
            = k("smallint") { TrinoDataType::SmallInt }

        rule int_ty() -> TrinoDataType
            = (k("integer") / k("int")) { TrinoDataType::Int }

        rule bigint_ty() -> TrinoDataType
            = k("bigint") { TrinoDataType::BigInt }

        rule real_ty() -> TrinoDataType
            = k("real") { TrinoDataType::Real }

        rule double_ty() -> TrinoDataType
            = k("double") { TrinoDataType::Double }

        rule decimal_ty() -> TrinoDataType
            = k("decimal") _? "(" _? precision:uint() _? "," _? scale:uint() _? ")" {
                TrinoDataType::Decimal { precision, scale }
            }

        rule varchar_ty() -> TrinoDataType
            = k("varchar") length:size_opt() {
                TrinoDataType::Varchar { length }
            }

        rule char_ty() -> TrinoDataType
            = k("char") length:size_default(1) {
                TrinoDataType::Char { length }
            }

        rule varbinary_ty() -> TrinoDataType
            = k("varbinary") { TrinoDataType::Varbinary }

        rule json_ty() -> TrinoDataType
            = k("json") { TrinoDataType::Json }

        rule date_ty() -> TrinoDataType
            = k("date") { TrinoDataType::Date }

        rule time_ty() -> TrinoDataType
            = k("time") precision:size_default(3) {
                TrinoDataType::Time { precision }
            }

        rule time_with_time_zone_ty() -> TrinoDataType
            = k("time") precision:size_default(3) _ k("with") _ k("time") _ k("zone") {
                TrinoDataType::TimeWithTimeZone { precision }
            }

        rule timestamp_ty() -> TrinoDataType
            = k("timestamp") precision:size_default(3) {
                TrinoDataType::Timestamp { precision }
            }

        rule timestamp_with_time_zone_ty() -> TrinoDataType
            = k("timestamp") precision:size_default(3) _ k("with") _ k("time") _ k("zone") {
                TrinoDataType::TimestampWithTimeZone { precision }
            }

        rule interval_day_to_second_ty() -> TrinoDataType
            = k("interval") _ "day" _ "to" _ "second" { TrinoDataType::IntervalDayToSecond }

        rule interval_year_to_month_ty() -> TrinoDataType
            = k("interval") _ "year" _ "to" _ "month" { TrinoDataType::IntervalYearToMonth }

        rule array_ty() -> TrinoDataType
            = k("array") _? "(" _? elem_ty:ty() _? ")" {
                TrinoDataType::Array(Box::new(elem_ty))
            }

        rule map_ty() -> TrinoDataType
            = k("map") _? "(" _? key_ty:ty() _? "," _? value_ty:ty() _? ")" {
                TrinoDataType::Map {
                    key_type: Box::new(key_ty),
                    value_type: Box::new(value_ty),
                }
            }

        rule row_ty() -> TrinoDataType
            = k("row") _? "(" _? fields:(field() ++ (_? "," _?)) _? ")" {
                TrinoDataType::Row(fields)
            }

        rule field() -> TrinoField
            = ty:ty() { TrinoField::anonymous(ty) }
            / name:ident() _ ty:ty() { TrinoField::named(name, ty) }

        rule uuid_ty() -> TrinoDataType
            = k("uuid") { TrinoDataType::Uuid }

        rule spherical_geography_ty() -> TrinoDataType
            = k("sphericalgeography") { TrinoDataType::SphericalGeography }

        rule ty() -> TrinoDataType
            = boolean_ty()
            / tinyint_ty()
            / smallint_ty()
            / int_ty()
            / bigint_ty()
            / real_ty()
            / double_ty()
            / decimal_ty()
            / varchar_ty()
            / char_ty()
            / varbinary_ty()
            / json_ty()
            / date_ty()
            // The `with_time_zone` versions must come first.
            / time_with_time_zone_ty()
            / time_ty()
            / timestamp_with_time_zone_ty()
            / timestamp_ty()
            / interval_day_to_second_ty()
            / interval_year_to_month_ty()
            / array_ty()
            / map_ty()
            / row_ty()
            / uuid_ty()
            / spherical_geography_ty()

        pub rule create_table() -> TrinoCreateTable
            = _? "CREATE" _ "TABLE" _ name:table_name() _?
              "(" _? columns:(column() ++ (_? "," _?)) _? ")" _? ";"
              _?
            {
                TrinoCreateTable { name, columns }
            }

        rule column() -> TrinoColumn
            = name:ident() _ ty:ty() is_nullable:is_nullable() {
                TrinoColumn { name, data_type: ty, is_nullable }
            }

        rule is_nullable() -> bool
            = _ "NOT" _ "NULL" { false }
            / { true }
    }
}
