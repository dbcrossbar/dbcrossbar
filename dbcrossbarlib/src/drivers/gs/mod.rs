//! Support for Google Cloud Storage.

use std::{fmt, str::FromStr};

use crate::common::*;

mod local_data;
mod write_local_data;

use local_data::local_data_helper;
use write_local_data::write_local_data_helper;

/// Locator scheme for Google Cloud Storage.
pub(crate) const GS_SCHEME: &str = "gs:";

#[derive(Debug)]
pub(crate) struct GsLocator {
    url: Url,
}

impl GsLocator {
    /// Access the `gs://` URL in this locator.
    pub(crate) fn as_url(&self) -> &Url {
        &self.url
    }
}

impl fmt::Display for GsLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.url.fmt(f)
    }
}

impl FromStr for GsLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if s.starts_with(GS_SCHEME) {
            let url = s
                .parse::<Url>()
                .with_context(|_| format!("cannot parse {}", s))?;
            if !url.path().starts_with('/') {
                Err(format_err!("{} must start with gs://", url))
            } else if !url.path().ends_with('/') {
                Err(format_err!("{} must end with a '/'", url))
            } else {
                Ok(GsLocator { url })
            }
        } else {
            Err(format_err!("expected {} to begin with gs://", s))
        }
    }
}

impl Locator for GsLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn local_data(
        &self,
        ctx: Context,
        _schema: Table,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        local_data_helper(ctx, self.url.clone()).into_boxed()
    }

    fn write_local_data(
        &self,
        ctx: Context,
        schema: Table,
        data: BoxStream<CsvStream>,
        if_exists: IfExists,
    ) -> BoxFuture<BoxStream<BoxFuture<()>>> {
        write_local_data_helper(ctx, self.url.clone(), schema, data, if_exists)
            .into_boxed()
    }
}
