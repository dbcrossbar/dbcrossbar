//! PostgreSQL columns.

use std::fmt;

use super::PgDataType;
use crate::common::*;
use crate::schema::Column;

/// A PostgreSQL identifier. This will be printed with quotes as necessary to
/// prevent clashes with keywords.
pub(crate) struct Ident<'a>(pub(crate) &'a str);

impl<'a> fmt::Display for Ident<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // TODO: For now, we just quote everything, and not even necessarily
        // correctly.
        write!(f, "{:?}", self.0)
    }
}

/// A column in a PostgreSQL table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PgColumn {
    /// The name of this column.
    pub(crate) name: String,
    /// The type of data stored in this column.
    pub(crate) data_type: PgDataType,
    /// Can this column be `NULL`?
    pub(crate) is_nullable: bool,
}

impl PgColumn {
    /// Given a portable `Column`, construct a `PgColumn`.
    pub(crate) fn from_column(col: &Column) -> Result<PgColumn> {
        let data_type = PgDataType::from_data_type(&col.data_type)?;
        Ok(PgColumn {
            name: col.name.clone(),
            data_type,
            is_nullable: col.is_nullable,
        })
    }

    /// Given a `PgColumn`, construct a portable `Column`.
    pub(crate) fn to_column(&self) -> Result<Column> {
        Ok(Column {
            name: self.name.clone(),
            data_type: self.data_type.to_data_type()?,
            is_nullable: self.is_nullable,
            comment: None,
        })
    }
}

impl fmt::Display for PgColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {}", Ident(&self.name), self.data_type)?;
        if !self.is_nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}
