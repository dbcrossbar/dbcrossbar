//! Code shared between various PostgreSQL-related drivers.

use failure::Fail;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use std::{fmt, str::FromStr};
pub use tokio_postgres::Client;
use tokio_postgres::Config;

use crate::common::*;

mod catalog;
mod column;
mod data_type;
mod table;

pub(crate) use self::column::PgColumn;
pub(crate) use self::data_type::{PgDataType, PgScalarDataType};
pub(crate) use self::table::{CheckCatalog, PgCreateTable};

/// Connect to the database, using SSL if possible.
pub(crate) async fn connect(
    ctx: &Context,
    url: &UrlWithHiddenPassword,
) -> Result<Client> {
    let mut base_url = url.clone();
    base_url.as_url_mut().set_fragment(None);

    // Build a basic config from our URL args.
    let config = Config::from_str(base_url.with_password().as_str())
        .context("could not configure PostgreSQL connection")?;
    let tls_connector = TlsConnector::builder()
        .build()
        .context("could not build PostgreSQL TLS connector")?;
    let (client, connection) = config
        .connect(MakeTlsConnector::new(tls_connector))
        .await
        .context("could not connect to PostgreSQL")?;

    // The docs say we need to run this connection object in the background.
    ctx.spawn_worker(
        connection.map_err(|e| -> Error {
            e.context("error on PostgreSQL connection").into()
        }),
    );

    Ok(client)
}

/// Escape and quote a PostgreSQL string literal. See the [docs][]. We need this
/// because PostgreSQL doesn't accept `$1`-style escapes in certain places in
/// its SQL grammar.
///
/// [docs]: https://www.postgresql.org/docs/9.2/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-ESCAPE
pub(crate) fn pg_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[test]
fn pg_quote_doubles_single_quotes() {
    let examples = &[
        ("", "''"),
        ("a", "'a'"),
        ("'", "''''"),
        ("'hello'", "'''hello'''"),
    ];
    for &(input, expected) in examples {
        assert_eq!(pg_quote(input), expected);
    }
}

/// A PostgreSQL identifier. This will be printed with quotes as necessary to
/// prevent clashes with keywords.
pub(crate) struct Ident<'a>(pub(crate) &'a str);

impl<'a> fmt::Display for Ident<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "\"")?;
        write!(f, "{}", self.0.replace('"', "\"\""))?;
        write!(f, "\"")?;
        Ok(())
    }
}

/// A PostgreSQL table name, including a possible scheme (i.e., a namespace).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableName {
    schema: Option<String>,
    table: String,
}

impl TableName {
    /// Create a new `TableName`.
    pub(crate) fn new<S, T>(schema: S, table: T) -> Self
    where
        S: Into<Option<String>>,
        T: Into<String>,
    {
        Self {
            schema: schema.into(),
            table: table.into(),
        }
    }

    /// The schema (namespace) portion of the table name, or `None` if none was provided.
    pub(crate) fn schema(&self) -> Option<&str> {
        self.schema.as_ref().map(|s| &s[..])
    }

    /// The table portion of the table name, not including the schema.
    pub(crate) fn table(&self) -> &str {
        &self.table
    }

    /// Format this table name as an unquoted string.
    pub(crate) fn unquoted(&self) -> String {
        if let Some(schema) = &self.schema {
            format!("{}.{}", schema, self.table)
        } else {
            self.table.clone()
        }
    }

    /// Properly quote a table name for use in SQL. Returns a value that
    /// implements `Display`.
    pub(crate) fn quoted(&self) -> TableNameQuoted<'_> {
        TableNameQuoted(self)
    }

    /// Create a temporary table name based on this table name.
    pub(crate) fn temporary_table_name(&self) -> Result<TableName> {
        Ok(Self {
            // We leave this as `None` because that's what we used to do for
            // PostgreSQL. It would probably be fine to use `self.namespace`
            // here.
            schema: None,
            table: format!("{}_temp_{}", self.table, TemporaryStorage::random_tag()),
        })
    }
}

impl FromStr for TableName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let components = s.splitn(2, '.').collect::<Vec<_>>();
        match components.len() {
            1 => Ok(Self {
                schema: None,
                table: components[0].to_owned(),
            }),
            2 => Ok(Self {
                schema: Some(components[0].to_owned()),
                table: components[1].to_owned(),
            }),
            _ => Err(format_err!("cannot parse table name {:?}", s)),
        }
    }
}

/// A wrapper for `TableName` that implemented `Display`.
pub(crate) struct TableNameQuoted<'a>(&'a TableName);

impl fmt::Display for TableNameQuoted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(schema) = self.0.schema() {
            write!(f, "{}.{}", Ident(schema), Ident(&self.0.table))?
        } else {
            write!(f, "{}", Ident(&self.0.table))?
        }
        Ok(())
    }
}

#[test]
fn table_name_is_quoted_correctly() {
    assert_eq!(
        format!("{}", TableName::from_str("example").unwrap().quoted()),
        "\"example\""
    );
    assert_eq!(
        format!(
            "{}",
            TableName::from_str("schema.example").unwrap().quoted()
        ),
        "\"schema\".\"example\""
    );

    // Don't parse this one, because we haven't decided how to parse weird names
    // like this yet.
    let with_quote = TableName {
        schema: Some("testme1".to_owned()),
        table: "lat-\"lon".to_owned(),
    };
    assert_eq!(
        format!("{}", with_quote.quoted()),
        "\"testme1\".\"lat-\"\"lon\""
    );
}
