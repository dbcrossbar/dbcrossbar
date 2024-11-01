//! Implementation of `count`, but as a real `async` function.

use super::TrinoLocator;
use crate::common::*;

/// Implementation of `count`, but as a real `async` function.
#[instrument(level = "trace", name = "trino::count", skip(shared_args, source_args))]
pub(crate) async fn count_helper(
    locator: TrinoLocator,
    shared_args: SharedArguments<Unverified>,
    source_args: SourceArguments<Unverified>,
) -> Result<usize> {
    let _shared_args = shared_args.verify(TrinoLocator::features())?;
    let source_args = source_args.verify(TrinoLocator::features())?;

    let client = locator.client()?;
    let sql = format!(
        "SELECT COUNT(*) AS \"count\"\nFROM {}{}",
        locator.table_name()?,
        if let Some(where_clause) = source_args.where_clause() {
            format!("\nWHERE ({})", where_clause)
        } else {
            "".to_string()
        }
    );
    let count = client.get_one_value::<i64>(&sql).await?;
    usize::try_from(count).context("could not convert count to usize")
}
