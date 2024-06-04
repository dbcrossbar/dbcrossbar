//! Support for URLs with passwords that can be printed and debugged safely.

use std::{borrow::Cow, fmt, str::FromStr};

use crate::common::*;

/// A URL which can be safely printed or logged using `Display` or `Debug`
/// without revealing the password.
#[derive(Clone)]
pub(crate) struct UrlWithHiddenPassword(Url);

impl UrlWithHiddenPassword {
    /// Create a new URL with a hidden password.
    pub(crate) fn new(url: Url) -> Self {
        UrlWithHiddenPassword(url)
    }

    // Get our actual URL, including the password.
    pub(crate) fn with_password(&self) -> &Url {
        &self.0
    }

    // Get a copy of our underlying URL.
    pub(crate) fn as_url(&self) -> &Url {
        &self.0
    }

    // Get a mutable copy of our underlying URL.
    pub(crate) fn as_url_mut(&mut self) -> &mut Url {
        &mut self.0
    }

    /// Get our underlying URL with any password removed.
    fn without_password(&self) -> Cow<'_, Url> {
        if self.0.password().is_some() {
            let mut url = self.0.clone();
            url.set_password(Some("XXXXXX")).expect(
                "should always be able to set password for `UrlWithHiddenPassword`",
            );
            Cow::Owned(url)
        } else {
            Cow::Borrowed(&self.0)
        }
    }
}

impl fmt::Debug for UrlWithHiddenPassword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.without_password(), f)
    }
}

impl fmt::Display for UrlWithHiddenPassword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.without_password(), f)
    }
}

impl FromStr for UrlWithHiddenPassword {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Ok(UrlWithHiddenPassword(
            // Be careful not to print the password if you change this error!
            Url::parse(s).context("cannot parse URL")?,
        ))
    }
}
