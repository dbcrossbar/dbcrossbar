//! Support for Amazon's S3.

use std::{fmt, str::FromStr};

use crate::common::*;

mod local_data;
mod prepare_as_destination;
mod write_local_data;

use local_data::local_data_helper;
pub(crate) use prepare_as_destination::prepare_as_destination_helper;
use write_local_data::write_local_data_helper;

/// Locator scheme for S3.
pub(crate) const S3_SCHEME: &str = "s3:";

#[derive(Clone, Debug)]
pub(crate) struct S3Locator {
    url: Url,
}

impl fmt::Display for S3Locator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.url.fmt(f)
    }
}

impl FromStr for S3Locator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s.starts_with(S3_SCHEME) {
            let url = s
                .parse::<Url>()
                .with_context(|_| format!("cannot parse {}", s))?;
            if !url.path().starts_with('/') {
                Err(format_err!("{} must start with s3://", url))
            } else if !url.path().ends_with('/') {
                Err(format_err!("{} must end with a '/'", url))
            } else {
                Ok(S3Locator { url })
            }
        } else {
            Err(format_err!("expected {} to begin with s3://", s))
        }
    }
}

impl Locator for S3Locator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn local_data(
        &self,
        ctx: Context,
        _schema: Table,
        query: Query,
        _temporary_storage: TemporaryStorage,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        local_data_helper(ctx, self.url.clone(), query).boxed()
    }

    fn write_local_data(
        &self,
        ctx: Context,
        schema: Table,
        data: BoxStream<CsvStream>,
        _temporary_storage: TemporaryStorage,
        if_exists: IfExists,
    ) -> BoxFuture<BoxStream<BoxFuture<()>>> {
        write_local_data_helper(ctx, self.url.clone(), schema, data, if_exists).boxed()
    }
}
