//! Code shared between various PostgreSQL-related drivers.

use std::{fmt, str::FromStr};
pub use tokio_postgres::Client;
use tokio_postgres::Config;

use crate::common::*;
use crate::tls::rustls_client_config;

mod catalog;
mod column;
mod create_type;
mod data_type;
mod schema;
mod table;

pub(crate) use self::column::PgColumn;
pub(crate) use self::create_type::{PgCreateType, PgCreateTypeDefinition};
pub(crate) use self::data_type::{PgDataType, PgScalarDataType};
pub(crate) use self::schema::PgSchema;
pub(crate) use self::table::{CheckCatalog, PgCreateTable};

/// Connect to the database, using SSL if possible.
#[instrument(level = "trace", skip(ctx))]
pub(crate) async fn connect(
    ctx: &Context,
    url: &UrlWithHiddenPassword,
) -> Result<Client> {
    let mut base_url = url.clone();
    base_url.as_url_mut().set_fragment(None);

    // Build a basic config from our URL args.
    let config = Config::from_str(base_url.with_password().as_str())
        .context("could not configure PostgreSQL connection")?;

    // Set up RusTLS.
    let tls_config = rustls_client_config()?;
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);

    // Actually create our PostgreSQL client and connect.
    let (client, connection) = config
        .connect(tls)
        .await
        .context("could not connect to PostgreSQL")?;

    // The docs say we need to run this connection object in the background.
    ctx.spawn_worker(
        debug_span!("postgres_shared::connect worker"),
        connection.map_err(|e| -> Error {
            Error::new(e).context("error on PostgreSQL connection")
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

/// A PostgreSQL table or type name, including a possible PostgreSQL schema (in
/// the PostgreSQL sense of a namespace, not what `dbcrossbar` calls a
/// "schema").
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PgName {
    /// A PostgreSQL namespace (not what `dbcrossbar` normally means by
    /// "schema")!
    schema: Option<String>,
    /// Our underlying name.
    name: String,
}

impl PgName {
    /// Create a new `TableName`.
    pub(crate) fn new<S, T>(schema: S, name: T) -> Self
    where
        S: Into<Option<String>>,
        T: Into<String>,
    {
        Self {
            schema: schema.into(),
            name: name.into(),
        }
    }

    /// Given the name of `NamedDataType`, construct a PostgreSQL `TableName`.
    pub(crate) fn from_portable_type_name<T>(type_name: T) -> Result<Self>
    where
        T: Into<String>,
    {
        let type_name = type_name.into();
        if type_name.contains('.') {
            // We don't yet have a design for mapping portable enums to enums in
            // PostgreSQL schemas other than `"public"`. Getting this right will
            // require some thought, so just error out for now.
            Err(format_err!(
                "portable type names containing \".\" are not yet supported: {:?}",
                type_name
            ))
        } else {
            Ok(Self::new(None, type_name))
        }
    }

    /// The schema (namespace) portion of the table name, or `None` if none was provided.
    pub(crate) fn schema(&self) -> Option<&str> {
        self.schema.as_ref().map(|s| &s[..])
    }

    /// The schema (namespace) portion of the table name, or `"public""` if none was provided.
    pub(crate) fn schema_or_public(&self) -> &str {
        self.schema.as_ref().map_or_else(|| "public", |s| &s[..])
    }

    /// The base portion of the name, not including the schema.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Format this name as an unquoted string.
    pub(crate) fn unquoted(&self) -> String {
        if let Some(schema) = &self.schema {
            format!("{}.{}", schema, self.name)
        } else {
            self.name.clone()
        }
    }

    /// Properly quote a name for use in SQL. Returns a value that implements
    /// `Display`.
    pub(crate) fn quoted(&self) -> TableNameQuoted<'_> {
        TableNameQuoted(self)
    }

    /// Convert this name to a portable name, if we know how. For now, we err on
    /// the side of refusing.
    pub(crate) fn to_portable_name(&self) -> Result<String> {
        match &self.schema {
            None => Ok(self.name.clone()),
            // If we're in the "public" PostgreSQL schema (which is a namespace,
            // not what dbcrossbar calls a "schema"), we can just drop it to
            // produce a cleaner portable name.
            Some(schema) if schema == "public" => Ok(self.name.clone()),
            Some(_) => Err(format_err!(
                "don't know how to convert {} to portable name yet, because it has a schema",
                self.quoted()
            ))
        }
    }

    /// Create a temporary table name based on this name.
    pub(crate) fn temporary_table_name(&self) -> Result<PgName> {
        Ok(Self {
            // We leave this as `None` because that's what we used to do for
            // PostgreSQL. It would probably be fine to use `self.namespace`
            // here.
            schema: None,
            name: format!("{}_temp_{}", self.name, TemporaryStorage::random_tag()),
        })
    }
}

impl FromStr for PgName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let components = s.splitn(2, '.').collect::<Vec<_>>();
        match components.len() {
            1 => Ok(Self {
                schema: None,
                name: components[0].to_owned(),
            }),
            2 => Ok(Self {
                schema: Some(components[0].to_owned()),
                name: components[1].to_owned(),
            }),
            _ => Err(format_err!("cannot parse PostgreSQL name {:?}", s)),
        }
    }
}

/// A wrapper for `TableName` that implemented `Display`.
pub(crate) struct TableNameQuoted<'a>(&'a PgName);

impl fmt::Display for TableNameQuoted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(schema) = self.0.schema() {
            write!(f, "{}.{}", Ident(schema), Ident(&self.0.name))?
        } else {
            write!(f, "{}", Ident(&self.0.name))?
        }
        Ok(())
    }
}

#[test]
fn postgres_name_is_quoted_correctly() {
    assert_eq!(
        format!("{}", PgName::from_str("example").unwrap().quoted()),
        "\"example\""
    );
    assert_eq!(
        format!("{}", PgName::from_str("schema.example").unwrap().quoted()),
        "\"schema\".\"example\""
    );

    // Don't parse this one, because we haven't decided how to parse weird names
    // like this yet.
    let with_quote = PgName {
        schema: Some("testme1".to_owned()),
        name: "lat-\"lon".to_owned(),
    };
    assert_eq!(
        format!("{}", with_quote.quoted()),
        "\"testme1\".\"lat-\"\"lon\""
    );
}
