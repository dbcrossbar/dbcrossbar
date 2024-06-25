//! A Trino-compatible `CREATE TABLE` statement.

use std::{collections::HashMap, fmt, sync::Arc};

use pretty::RcDoc;
#[cfg(test)]
use proptest_derive::Arbitrary;

use crate::{
    common::*,
    drivers::trino_shared::ast::BinOp,
    parse_error::{Annotation, FileInfo, ParseError},
    schema::Column,
};

use super::{
    ast::{Expr, Literal},
    pretty::{comma_sep_list, indent, parens, select_from, sql_clause, WIDTH},
    TrinoConnectorType, TrinoDataType, TrinoField, TrinoIdent, TrinoTableName,
};

/// A Trino-compatible `CREATE TABLE` statement.
#[derive(Clone, Debug, PartialEq)]
pub struct TrinoCreateTable {
    separate_drop_if_exists: bool,
    or_replace: bool,
    if_not_exists: bool,
    pub(crate) name: TrinoTableName,
    columns: Vec<TrinoColumn>,
    with: HashMap<TrinoIdent, Literal>,
}

impl TrinoCreateTable {
    /// Parse from an SQL string. `path` is used for error messages.
    pub(crate) fn parse(
        path: &str,
        sql: &str,
    ) -> Result<TrinoCreateTable, ParseError> {
        parse_rule(
            trino_parser::create_table,
            path,
            sql,
            "error parsing Postgres CREATE TABLE",
        )
    }

    /// Create from [`TrinoColumn`] values.
    pub fn from_trino_columns_and_name(
        columns: Vec<TrinoColumn>,
        name: TrinoTableName,
    ) -> Result<Self> {
        if columns.is_empty() {
            Err(format_err!(
                "Trino table {} must have at least one column",
                name
            ))
        } else {
            Ok(Self {
                separate_drop_if_exists: false,
                or_replace: false,
                if_not_exists: false,
                name,
                columns,
                with: HashMap::new(),
            })
        }
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
        Self::from_trino_columns_and_name(columns, name.clone())
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

    /// Set our table creation options. You probably want to follow this with a
    /// call to [`downgrade_for_connector_type`] to ensure that the options
    /// selected will actually work with a given Trino connector.
    pub fn set_if_exists_options(&mut self, if_exists: IfExists) {
        self.separate_drop_if_exists = false;
        match if_exists {
            IfExists::Error => {
                self.or_replace = false;
                self.if_not_exists = false;
            }
            IfExists::Append | IfExists::Upsert(_) => {
                self.or_replace = false;
                self.if_not_exists = true;
            }
            IfExists::Overwrite => {
                self.or_replace = true;
                self.if_not_exists = false;
            }
        }
    }

    /// Downgrade this for a specific connector type, as needed. This is
    /// necessary because not all of Trino's connectors support the same table
    /// declaration features and they tend to error out if we use an unsupported
    /// feature. But `dbcrossbar`'s job is to create a table as close as
    /// possible to the one requested, even if this means occasionally
    /// "downgrading" something like `NOT NULL`.
    pub fn downgrade_for_connector_type(
        &mut self,
        connector_type: &TrinoConnectorType,
    ) {
        // Erase `NOT NULL` constraints if the connector doesn't support them.
        if !connector_type.supports_not_null_constraint() {
            for column in &mut self.columns {
                column.is_nullable = true;
            }
        }

        // Erase `OR REPLACE` if the connector doesn't support it.
        if !connector_type.supports_replace_table() && self.or_replace {
            self.or_replace = false;
            self.separate_drop_if_exists = true;
        }
    }

    /// A separate `DROP TABLE IF EXISTS` statement.
    pub fn separate_drop_if_exists(&self) -> Option<String> {
        if self.separate_drop_if_exists {
            Some(format!("DROP TABLE IF EXISTS {}", self.name))
        } else {
            None
        }
    }

    /// Add `WITH` clauses for a CSV files stored in the specified external
    /// location, and accessed via a Hive connector. We assume that these CSV
    /// files have a single-line header, and are otherwise in `dbcrossbar`'s CSV
    /// interchange format.
    pub fn add_csv_external_location(&mut self, location: &Url) -> Result<()> {
        self.with.insert(
            TrinoIdent::new("format")?,
            Literal::String("csv".to_owned()),
        );
        self.with.insert(
            TrinoIdent::new("external_location")?,
            Literal::String(location.as_str().to_owned()),
        );
        self.with
            .insert(TrinoIdent::new("skip_header_line_count")?, Literal::Int(1));
        Ok(())
    }

    /// Generate a version of this table that can be used with a Hive S3
    /// backend. Among other things, all our columns will be `VARCHAR`, and we
    /// need to set up `WITH` options.
    pub fn hive_csv_wrapper_table(&self, external_csv_url: &Url) -> Result<Self> {
        let mut table = self.clone();
        // TODO: Allow the user to specify which Hive catalog and schema to use
        // for temp tables?
        table.name = TrinoTableName::with_catalog(
            "dbcrossbar",
            "default",
            &format!("dbcrossbar_temp_{}", TemporaryStorage::random_tag()),
        )?;
        table.set_if_exists_options(IfExists::Error);
        for column in &mut table.columns {
            column.data_type = TrinoDataType::Varchar { length: None };
            column.is_nullable = true;
        }
        table.add_csv_external_location(external_csv_url)?;
        Ok(table)
    }

    /// Our column names, as an iterator.
    pub fn column_names(&self) -> impl Iterator<Item = &TrinoIdent> {
        self.columns.iter().map(|column| &column.name)
    }

    /// Generate a `SELECT` expression that will fetch data for this table from
    /// a wrapper table, and convert it the appropriate data types.
    fn select_from_wrapper_table_doc(
        &self,
        wrapper_table: &TrinoTableName,
    ) -> Result<RcDoc<'static, ()>> {
        Ok(select_from(
            self.columns
                .iter()
                .map(|column| Ok(column.import_expr()?.to_doc()))
                .collect::<Result<Vec<_>>>()?,
            wrapper_table,
        ))
    }

    // Generate an `INSERT INTO ... SELECT ...` statement that will copy data
    // from a wrapper table to this table.
    pub(crate) fn insert_from_wrapper_table_doc(
        &self,
        create_s3_wrapper_table: &TrinoCreateTable,
    ) -> Result<RcDoc<'static, ()>> {
        Ok(RcDoc::concat(vec![
            sql_clause(RcDoc::concat(vec![
                RcDoc::text("INSERT INTO "),
                RcDoc::as_string(&self.name),
                RcDoc::space(),
                parens(comma_sep_list(self.column_names().map(RcDoc::as_string))),
            ])),
            self.select_from_wrapper_table_doc(&create_s3_wrapper_table.name)?,
        ]))
    }

    /// Print the `CREATE [OR REPLACE] TABLE [IF NOT EXISTS] name` portion of
    /// the `CREATE TABLE` statement.
    ///
    /// Does not end with a space.
    fn create_table_and_name_doc(&self) -> RcDoc<'static, ()> {
        RcDoc::concat(vec![
            RcDoc::text("CREATE"),
            if self.or_replace {
                RcDoc::text(" OR REPLACE")
            } else {
                RcDoc::nil()
            },
            RcDoc::text(" TABLE"),
            if self.if_not_exists {
                RcDoc::text(" IF NOT EXISTS")
            } else {
                RcDoc::nil()
            },
            RcDoc::space(),
            RcDoc::as_string(&self.name),
        ])
    }

    /// Write the `WITH` block if it's not empty.
    fn with_doc(&self) -> RcDoc<'static, ()> {
        if self.with.is_empty() {
            RcDoc::nil()
        } else {
            sql_clause(RcDoc::concat(vec![
                RcDoc::text("WITH "),
                parens(comma_sep_list(self.with.iter().map(|(key, value)| {
                    RcDoc::concat(vec![
                        RcDoc::as_string(key),
                        RcDoc::text(" ="),
                        indent(value.to_doc()),
                    ])
                    .group()
                }))),
            ]))
        }
    }

    /// Write SQL including `CREATE TABLE ... [WITH ...] AS`. This is normally
    /// used together with [`Self::select_as_named_varchar_values_to_doc`]
    /// below, _except_ that:
    ///
    /// - `create_as_prologue_to_doc` is called on a temporary table configured
    ///   to store data as CSV, and
    /// - `select_as_named_varchar_values_to_doc` is called on the source table
    ///   from which we're copying data.
    fn create_as_prologue_doc(&self) -> RcDoc<'static, ()> {
        RcDoc::concat(vec![
            self.create_table_and_name_doc(),
            RcDoc::line(),
            self.with_doc(),
            RcDoc::text("AS "),
        ])
    }

    /// Write a `SELECT` statement that converts all columns to `VARCHAR` in
    /// `dbcrossbar` CSV interchange format, but preserves column names. This is
    /// normally used together with [`Self::create_as_prologue_to_doc`] above.
    fn select_as_named_varchar_values_doc(&self) -> Result<RcDoc<'static, ()>> {
        Ok(select_from(
            self.columns
                .iter()
                .map(|column| {
                    Ok(RcDoc::concat(vec![
                        column.export_expr()?.to_doc(),
                        RcDoc::text(" AS "),
                        RcDoc::as_string(&column.name),
                    ]))
                })
                .collect::<Result<Vec<_>>>()?,
            &self.name,
        ))
    }

    /// Create a wrapper table by selecting from an existing table and exporting
    /// as VARCHAR in `dbcrossbar` CSV interchange format.
    pub(crate) fn create_wrapper_table_doc(
        &self,
        source_table: &TrinoCreateTable,
    ) -> Result<RcDoc<'static, ()>> {
        let create_as_prologue_sql = self.create_as_prologue_doc();
        let select_as_varchar_sql =
            source_table.select_as_named_varchar_values_doc()?;
        Ok(RcDoc::concat(vec![
            create_as_prologue_sql,
            select_as_varchar_sql,
        ]))
    }
}

impl fmt::Display for TrinoCreateTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // NOTE: We don't include `self.separate_drop_if_exists` because it's
        // no longer part of the create table. This is somewhat of a kludge,
        // because Trino doesn't allow semi-colons between statements. But we
        // can at least include a commented version.
        if self.separate_drop_if_exists {
            writeln!(f, "-- DROP TABLE IF EXISTS {};", self.name)?;
        }
        let doc = RcDoc::concat(vec![
            self.create_table_and_name_doc(),
            RcDoc::space(),
            parens(comma_sep_list(
                self.columns.iter().map(|column| column.to_doc()),
            )),
            RcDoc::line(),
            self.with_doc(),
        ]);
        write!(f, "{}", doc.pretty(WIDTH))
    }
}

/// A Trino column.
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(test, derive(Arbitrary))]
pub struct TrinoColumn {
    /// The name of this column.
    pub name: TrinoIdent,
    /// The data type of this column.
    pub data_type: TrinoDataType,
    /// Can we store NULL values in this column?
    pub is_nullable: bool,
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

    /// Write the SQL for importing this column from a wrapper table.
    fn import_expr(&self) -> Result<Expr> {
        let var = Expr::Var(self.name.clone());
        let expr = self.data_type.string_import_expr(&var)?;
        if self.is_nullable {
            Ok(Expr::r#if(
                Expr::binop(Expr::func("LENGTH", vec![var]), BinOp::Eq, Expr::int(0)),
                Expr::null(),
                expr,
            ))
        } else {
            Ok(expr)
        }
    }

    /// Write the SQL for exporting this column to a wrapper table.
    pub(super) fn export_expr(&self) -> Result<Expr> {
        let var = Expr::Var(self.name.clone());
        // This always needs to be a VARCHAR with no length, or else the Hive
        // connector will refuse to store it in a table represented as CSV.
        Ok(Expr::cast(
            self.data_type.string_export_expr(&var)?,
            TrinoDataType::varchar(),
        ))
    }

    /// Create an [`RcDoc`] for this column.
    pub(super) fn to_doc(&self) -> RcDoc<'static, ()> {
        RcDoc::concat(vec![
            RcDoc::as_string(&self.name),
            RcDoc::space(),
            self.data_type.to_doc(),
            if self.is_nullable {
                RcDoc::nil()
            } else {
                RcDoc::space().append(RcDoc::as_string("NOT NULL"))
            },
        ])
        .group()
    }
}

/// Parse a data type (without any surrounding whitespace, because this is used
/// to parse `information_schema.columns`).
pub fn parse_data_type(data_type: &str) -> Result<TrinoDataType, ParseError> {
    parse_rule(
        trino_parser::ty,
        "data_type",
        data_type,
        "error parsing Trino data type",
    )
}

/// Wrap a [`peg`] parser function and convert the error to a pretty
/// [`ParseError`].
fn parse_rule<Output, ParseFn>(
    parse_fn: ParseFn,
    path: &str,
    s: &str,
    err_msg: &str,
) -> Result<Output, ParseError>
where
    ParseFn: Fn(&str) -> Result<Output, peg::error::ParseError<peg::str::LineCol>>,
{
    let file_info = Arc::new(FileInfo::new(path.to_owned(), s.to_owned()));
    parse_fn(&file_info.contents).map_err(|err| {
        ParseError::new(
            file_info,
            vec![Annotation::primary(
                err.location.offset,
                format!("expected {}", err.expected),
            )],
            err_msg,
        )
    })
}

// `rustpeg` grammar for parsing Trino data types.
peg::parser! {
    grammar trino_parser() for str {
        rule _ = quiet! { (
            [' ' | '\t' | '\r' | '\n']
            / "--" [^'\n']* "\n"
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
                    // `unwrap` is safe because the parser controls our input.
                    TrinoIdent::new(s).unwrap()
                }
                / "\"" s:$(([^ '"'] / "\"\"")+) "\"" {
                    // `unwrap` is safe because the parser controls our input.
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

        // A signed integer literal.
        rule i64() -> i64
            = quiet! { n:$("-"? ['0'..='9']+) {?
                n.parse().map_err(|_| "64-bit signed integer")
            } }
            / expected!("64-bit signed integer")

        // An unsigned integer literal.
        rule u32() -> u32
            = quiet! { n:$(['0'..='9']+) {?
                n.parse().map_err(|_| "32-bit unsigned integer")
            } }
            / expected!("32-bit unsigned integer")

        // A string literal.
        rule string() -> String
            = quiet! { "\'" s:$(([^ '\''] / "''")*) "\'" {
                s.replace("''", "'")
            } }
            / expected!("string literal")

        rule size_opt() -> Option<u32>
            = _? "(" _? size:u32() _? ")" { Some(size) }
            / { None }

        rule size_default(default: u32) -> u32
            = _? "(" _? size:u32() _? ")" { size }
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
            = k("decimal") _? "(" _? precision:u32() _? "," _? scale:u32() _? ")" {
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
            = k("interval") _ k("day") _ k("to") _ k("second") { TrinoDataType::IntervalDayToSecond }

        rule interval_year_to_month_ty() -> TrinoDataType
            = k("interval") _ k("year") _ k("to") _ k("month") { TrinoDataType::IntervalYearToMonth }

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

        pub rule ty() -> TrinoDataType
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
            = _?
              "CREATE" or_replace:or_replace() _ "TABLE"
                if_not_exists:if_not_exists() _
                name:table_name() _?
              "(" _? columns:(column() ++ (_? "," _?)) _? ")"
              with:with()
              (_? ";")?
              _?
            {
                TrinoCreateTable {
                    separate_drop_if_exists: false,
                    or_replace,
                    if_not_exists,
                    name,
                    columns,
                    with,
                }
            }

        rule or_replace() -> bool
            = _ "OR" _ "REPLACE" { true }
            / { false }

        rule if_not_exists() -> bool
            = _ "IF" _ "NOT" _ "EXISTS" { true }
            / { false }

        rule with() -> HashMap<TrinoIdent, Literal>
            = _? "WITH" _? "(" _? properties:(property() ** (_? "," _?)) _? ")" {
                properties.into_iter().collect()
            }
            / { HashMap::new() }

        rule property() -> (TrinoIdent, Literal)
            = key:ident() _? "=" _? value:literal() {
                (key, value)
            }

        rule literal() -> Literal
            = s:string() { Literal::String(s) }
            / i:i64() { Literal::Int(i) }
            / k("true") { Literal::Bool(true) }
            / k("false") { Literal::Bool(false) }
            / k("null") { Literal::Null }

        rule column() -> TrinoColumn
            = name:ident() _ ty:ty() is_nullable:is_nullable() {
                TrinoColumn { name, data_type: ty, is_nullable }
            }

        rule is_nullable() -> bool
            = _ "NOT" _ "NULL" { false }
            / { true }
    }
}

#[cfg(test)]
mod tests {
    use prop::collection;
    use proptest::prelude::*;

    use super::*;

    impl Arbitrary for TrinoCreateTable {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;

        fn arbitrary_with(_args: ()) -> Self::Strategy {
            (
                any::<bool>(),
                any::<bool>(),
                any::<bool>(),
                any::<TrinoTableName>(),
                // Make sure we have at least one column.
                collection::vec(any::<TrinoColumn>(), 1..3),
                collection::hash_map(any::<TrinoIdent>(), any::<Literal>(), 0..3),
            )
                .prop_map(
                    |(
                        separate_drop_if_exists,
                        or_replace,
                        if_not_exists,
                        name,
                        columns,
                        with,
                    )| {
                        TrinoCreateTable {
                            separate_drop_if_exists,
                            or_replace,
                            if_not_exists,
                            name,
                            columns,
                            with,
                        }
                    },
                )
                .boxed()
        }
    }

    /// Normalize whitespace for tests. This is mostly so that we're testing
    /// table names and column names, not the pretty-printer.
    fn normalize_whitespace(s: &str) -> String {
        lazy_static::lazy_static! {
            static ref WHITESPACE: regex::Regex = regex::Regex::new(r"\s+").unwrap();
            static ref OPEN_DELIM: regex::Regex = regex::Regex::new(r"([(\[])\s*").unwrap();
            static ref CLOSE_DELIM: regex::Regex = regex::Regex::new(r"\s*([)\]])").unwrap();
        }
        let s = WHITESPACE.replace_all(s, " ");
        let s = s.trim();
        let s = OPEN_DELIM.replace_all(s, "$1");
        let s = CLOSE_DELIM.replace_all(&s, "$1");
        s.to_string()
    }

    #[test]
    fn test_trino_create_table() {
        let create_table = TrinoCreateTable::parse(
            "test_trino_create_table",
            "CREATE TABLE foo.bar (id INT NOT NULL, name VARCHAR(255));",
        )
        .unwrap();
        assert_eq!(
            normalize_whitespace(&create_table.to_string()),
            normalize_whitespace("CREATE TABLE \"foo\".\"bar\" (\"id\" INT NOT NULL, \"name\" VARCHAR(255))"),
        );
    }

    // A few odd tables, mostly found by proptest, that should parse. We put
    // these into a separate test to prevent future regressions, and to
    // pretty-print any parse errors they produce.
    #[test]
    fn odd_tables_parse() {
        let odd_tables = &[r#"CREATE TABLE "¡" (id INT)"#];
        for odd_table in odd_tables {
            let result = TrinoCreateTable::parse("test", odd_table);
            if let Err(err) = result {
                // Pretty-print our error.
                panic!("A parsing error occurred:\n{}", err);
            }
        }
    }

    proptest! {
        #[test]
        fn serialize_and_parse_is_identity(mut create_table in any::<TrinoCreateTable>()) {
            // We don't recover this when parsing, so don't generate it.
            create_table.separate_drop_if_exists = false;

            let s = create_table.to_string();
            match TrinoCreateTable::parse("test", &s) {
                Err(err) => {
                    // Pretty-print our error.
                    panic!("A parsing error occurred:\n{}", err);
                }
                Ok(parsed) => {
                    prop_assert_eq!(parsed, create_table);
                }
            }
        }
    }
}
