use crate::{common::*, drivers::trino::types::TrinoTable};

use super::TrinoLocator;

#[instrument(level = "trace", name = "trino::schema", skip(source_args))]
pub(crate) async fn schema_helper(
    source: TrinoLocator,
    source_args: SourceArguments<Unverified>,
) -> Result<Option<Schema>> {
    // TODO: Don't forget to look at these.
    let source_args = source_args.verify(TrinoLocator::features())?;

    let client = source.client()?;
    let table_name = source.table_name()?;

    let table = TrinoTable::from_database(&client, table_name).await?;
    Ok(Some(table.to_schema()?))
}
