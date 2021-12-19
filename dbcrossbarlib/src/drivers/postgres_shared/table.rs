//! A PostgreSQL `CREATE TABLE` declaration.

use itertools::Itertools;
use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use super::{PgColumn, PgDataType, PgName, PgScalarDataType};
use crate::common::*;
use crate::schema::Column;
use crate::separator::Separator;

/// Should we check the PostgreSQL catalog for a schema, or just use the one we
/// were given?
///
/// This is basically a fancy boolean that exists in order to make the related
/// logic clear at a glance, and easy to verify.
#[derive(Debug)]
pub(crate) enum CheckCatalog {
    /// Check the PostgreSQL catalog for an existing schema.
    Yes,
    /// Always use the schema given by the user.
    No,
}

impl From<&IfExists> for CheckCatalog {
    fn from(if_exists: &IfExists) -> CheckCatalog {
        match if_exists {
            IfExists::Error | IfExists::Overwrite => CheckCatalog::No,
            IfExists::Append | IfExists::Upsert(_) => CheckCatalog::Yes,
        }
    }
}

/// A PostgreSQL table declaration.
///
/// This is marked as `pub` and not `pub(crate)` because of a limitation of the
/// `peg` crate, which can only declare regular `pub` functions, which aren't
/// allowed to expose `pub(crate)` types. But we don't actually want to export
/// this outside of our crate, so we mark it `pub` here but take care to not
/// export it from a `pub` module anywhere.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgCreateTable {
    /// The name of the table.
    pub(crate) name: PgName,
    /// The columns in the table.
    pub(crate) columns: Vec<PgColumn>,
    /// Only create the table if it doesn't already exist.
    pub(crate) if_not_exists: bool,
    /// Create a temporary table local to a specific client session.
    pub(crate) temporary: bool,
}

impl PgCreateTable {
    /// Given a table name and a list of portable columns, construct a
    /// corresponding `PgCreateTable`.
    ///
    /// We don't take a portable `Table` as an argument, because the `name`
    /// contained in the `Table` might be an input table name, something from a
    /// schema, etc., and it's usually a mistake to use it directly without
    /// thinking things through first.
    ///
    /// We set `if_not_exists` to false, but the caller can change this directly
    /// once once the `PgCreateTable` has been created.
    pub(crate) fn from_name_and_columns(
        schema: &Schema,
        table_name: PgName,
        columns: &[Column],
    ) -> Result<PgCreateTable> {
        let pg_columns = columns
            .iter()
            .map(|c| PgColumn::from_column(schema, c))
            .collect::<Result<Vec<PgColumn>>>()?;
        Ok(PgCreateTable {
            name: table_name,
            columns: pg_columns,
            if_not_exists: false,
            temporary: false,
        })
    }

    /// Given a `PgCreateTable`, convert it to a portable `Table`.
    pub(crate) fn to_table(&self) -> Result<Table> {
        let columns = self
            .columns
            .iter()
            .map(|c| c.to_column())
            .collect::<Result<Vec<Column>>>()?;
        Ok(Table {
            name: self.name.unquoted(),
            columns,
        })
    }

    /// Create a new table based on this table, but with columns matching the
    /// the names and order of the columns in `other_table`. This is useful if
    /// we want to insert from `other_table` into `self`.
    ///
    /// Hypothetically, we could also check for compatibility between column
    /// types in the two tables, but for now, we're happy to let the database
    /// verify all that for us.
    pub(crate) fn aligned_with(
        &self,
        other_table: &PgCreateTable,
    ) -> Result<PgCreateTable> {
        let column_map = self
            .columns
            .iter()
            .map(|c| (&c.name[..], c))
            .collect::<HashMap<_, _>>();
        Ok(PgCreateTable {
            name: self.name.clone(),
            columns: other_table
                .columns
                .iter()
                .map(|c| {
                    if let Some(&col) = column_map.get(&c.name[..]) {
                        Ok(col.to_owned())
                    } else {
                        Err(format_err!(
                            "could not find column {} in destination table: {}",
                            c.name,
                            column_map.keys().join(", "),
                        ))
                    }
                })
                .collect::<Result<Vec<_>>>()?,
            if_not_exists: self.if_not_exists,
            temporary: self.temporary,
        })
    }

    /// Return all the unique named types in this `PgTable`.
    pub(crate) fn named_type_names(&self) -> HashSet<&PgName> {
        let mut names = HashSet::new();
        for col in &self.columns {
            let scalar_ty = match &col.data_type {
                PgDataType::Array { ty, .. } => ty,
                PgDataType::Scalar(ty) => ty,
            };
            if let PgScalarDataType::Named(name) = scalar_ty {
                names.insert(name);
            }
        }
        names
    }

    /// Write a `COPY (SELECT ...) TO STDOUT ...` statement for this table.
    pub(crate) fn write_export_sql(
        &self,
        f: &mut dyn Write,
        source_args: &SourceArguments<Verified>,
    ) -> Result<()> {
        write!(f, "COPY (")?;
        self.write_export_select_sql(f, source_args)?;
        write!(f, ") TO STDOUT WITH CSV HEADER")?;
        Ok(())
    }

    /// Write a `SELECT ...` statement for this table.
    pub(crate) fn write_export_select_sql(
        &self,
        f: &mut dyn Write,
        source_args: &SourceArguments<Verified>,
    ) -> Result<()> {
        write!(f, "SELECT ")?;
        if self.columns.is_empty() {
            return Err(format_err!("cannot export 0 columns"));
        }
        let mut sep = Separator::new(",");
        for col in &self.columns {
            write!(f, "{}", sep.display())?;
            col.write_export_select_expr(f)?;
        }
        write!(f, " FROM {}", &self.name.quoted())?;
        if let Some(where_clause) = source_args.where_clause() {
            write!(f, " WHERE ({})", where_clause)?;
        }
        Ok(())
    }

    /// Write a `SELECT COUNT(*) ...` statement for this table.
    pub(crate) fn write_count_sql(
        &self,
        f: &mut dyn Write,
        source_args: &SourceArguments<Verified>,
    ) -> Result<()> {
        writeln!(f, "SELECT COUNT(*)")?;
        writeln!(f, " FROM {}", &self.name.quoted())?;
        if let Some(where_clause) = source_args.where_clause() {
            writeln!(f, " WHERE ({})", where_clause)?;
        }
        Ok(())
    }
}

impl fmt::Display for PgCreateTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE")?;
        if self.temporary {
            write!(f, " TEMPORARY")?;
        }
        write!(f, " TABLE")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        writeln!(f, " {} (", &self.name.quoted())?;
        for (idx, col) in self.columns.iter().enumerate() {
            write!(f, "    {}", col)?;
            if idx + 1 == self.columns.len() {
                writeln!(f)?;
            } else {
                writeln!(f, ",")?;
            }
        }
        writeln!(f, ");")?;
        Ok(())
    }
}
