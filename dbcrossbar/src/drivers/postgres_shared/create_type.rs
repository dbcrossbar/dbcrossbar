//! `CREATE TYPE` declarations.

use std::fmt;

use tokio_postgres::Client;

use crate::schema::NamedDataType;
use crate::{common::*, schema::DataType};

use super::{catalog, PgName};
/// A PostgreSQL `CREATE TYPE` declaration.
#[derive(Clone, Debug)]
pub(crate) struct PgCreateType {
    /// The name of the custom type.
    pub(crate) name: PgName,
    /// The definition of the custom type.
    pub(crate) definition: PgCreateTypeDefinition,
}

impl PgCreateType {
    /// Convert a Postgres `PgCreateType` to a portable `NamedDataType`.
    pub(crate) fn to_named_data_type(&self) -> Result<NamedDataType> {
        Ok(NamedDataType {
            name: self.name.to_portable_name()?,
            data_type: self.definition.to_data_type()?,
        })
    }

    /// Convert a portable `NamedDataType` to a Postgres `PgCreateType`.
    pub(crate) fn from_named_data_type(ty: &NamedDataType) -> Result<Self> {
        Ok(Self {
            name: PgName::from_portable_type_name(&ty.name)?,
            definition: PgCreateTypeDefinition::from_data_type(&ty.data_type)?,
        })
    }

    /// Look up a `PgCreateType` by name using the specified database
    /// connection.
    #[instrument(level = "trace", skip(client))]
    pub(crate) async fn from_database(
        client: &Client,
        type_name: &PgName,
    ) -> Result<Option<Self>> {
        catalog::fetch_create_type(client, type_name).await
    }
}

impl fmt::Display for PgCreateType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CREATE TYPE {} AS {}",
            self.name.quoted(),
            self.definition
        )?;
        writeln!(f)?;
        Ok(())
    }
}

/// Definition of a PostgreSQL custom type.
#[derive(Clone, Debug)]
pub(crate) enum PgCreateTypeDefinition {
    /// The body of a `CREATE TYPE name AS ENUM(...)` definition.
    Enum(Vec<String>),
}

impl PgCreateTypeDefinition {
    /// Convert a PostgreSQL `CREATE TYPE` definition into a portable data type.
    pub(crate) fn to_data_type(&self) -> Result<DataType> {
        match self {
            PgCreateTypeDefinition::Enum(values) => {
                Ok(DataType::OneOf(values.to_owned()))
            }
        }
    }

    /// Construct a PostgreSQL `CREATE TYPE` definition from a portable data
    /// type.
    pub(crate) fn from_data_type(ty: &DataType) -> Result<Self> {
        match ty {
            DataType::OneOf(values) => Ok(Self::Enum(values.to_owned())),
            _ => Err(format_err!(
                "cannot convert {:?} to PostgreSQL CREATE TYPE",
                ty
            )),
        }
    }
}

impl fmt::Display for PgCreateTypeDefinition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PgCreateTypeDefinition::Enum(values) => {
                write!(f, "ENUM (")?;
                for (idx, value) in values.iter().enumerate() {
                    if idx != 0 {
                        write!(f, ", ")?;
                    }
                    // Escape PostgreSQL strings properly.
                    write!(f, "'{}'", value.replace('\'', "''"))?;
                }
                write!(f, ");")?;
            }
        }
        Ok(())
    }
}
