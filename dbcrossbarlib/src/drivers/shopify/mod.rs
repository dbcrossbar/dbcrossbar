//! Driver for working with Shopify REST endpoints.
//!
//! We include this because it forced us to face a number of interesting issues, including struct types,

use std::{fmt, str::FromStr};

use crate::common::*;

mod json_to_csv;
mod local_data;

use local_data::local_data_helper;

/// A Shopify REST endpoint.
#[derive(Clone, Debug)]
pub(crate) struct ShopifyLocator {
    url: Url,
}

impl ShopifyLocator {
    /// Convert this locator to a `https` URL.
    fn to_https_url(&self) -> Result<Url> {
        assert!(self.url.as_str().starts_with(Self::scheme()));
        let https_str =
            format!("https:{}", &self.url.as_str()[Self::scheme().len()..]);
        let https_url = https_str
            .parse::<Url>()
            .with_context(|_| format_err!("could not set URL scheme for {}", self))?;
        Ok(https_url)
    }
}

#[test]
fn to_https_url() {
    let loc = ShopifyLocator::from_str(
        "shopify://example.myshopify.com/admin/api/2020-04/orders.json",
    )
    .unwrap();
    assert_eq!(
        loc.to_https_url().unwrap().as_str(),
        "https://example.myshopify.com/admin/api/2020-04/orders.json",
    );
}

impl fmt::Display for ShopifyLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.url.fmt(f)
    }
}

impl FromStr for ShopifyLocator {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let url = s
            .parse::<Url>()
            .with_context(|_| format!("could not parse Shopify locator {:?}", s))?;
        if url.scheme() == "shopify" {
            Ok(ShopifyLocator { url })
        } else {
            Err(format_err!("expected {:?} to start with \"shopify:\"", s))
        }
    }
}

impl Locator for ShopifyLocator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn local_data(
        &self,
        ctx: Context,
        shared_args: SharedArguments<Unverified>,
        source_args: SourceArguments<Unverified>,
    ) -> BoxFuture<Option<BoxStream<CsvStream>>> {
        local_data_helper(ctx, self.clone(), shared_args, source_args).boxed()
    }
}

impl LocatorStatic for ShopifyLocator {
    fn scheme() -> &'static str {
        "shopify:"
    }

    fn features() -> Features {
        Features {
            locator: LocatorFeatures::LocalData.into(),
            write_schema_if_exists: EnumSet::empty(),
            source_args: EnumSet::empty(),
            dest_args: EnumSet::empty(),
            dest_if_exists: EnumSet::empty(),
            _placeholder: (),
        }
    }

    /// This locator type is currently unstable.
    fn is_unstable() -> bool {
        true
    }
}
