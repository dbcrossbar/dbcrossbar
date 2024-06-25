//! Implementation of `count`, but as a real `async` function.

use prusto::Presto;

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
    let _source_args = source_args.verify(TrinoLocator::features())?;

    #[derive(Debug, Presto)]
    struct Row {
        cnt: u64,
    }

    let client = locator.client()?;
    let sql = format!("SELECT COUNT(*) AS cnt FROM {}", locator.table_name()?);
    let rows = client.get_all::<Row>(sql).await?;
    let row = rows
        .as_slice()
        .first()
        .ok_or_else(|| format_err!("no count returned for {}", locator))?;

    usize::try_from(row.cnt).context("could not convert count to usize")
}
