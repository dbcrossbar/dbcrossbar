//! Support for setting up RusTLS in a consistent fashion.

use rustls::{Certificate, ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;

use crate::common::*;

/// Standard RusTLS `ClientConfig` setup.
///
/// We hope to be able to reuse this for multiple different types of TLS
/// connections.
pub(crate) fn rustls_client_config(ctx: &Context) -> Result<ClientConfig> {
    // Set up RusTLS.
    let mut root_store = RootCertStore::empty();
    for cert in load_native_certs().context("could not find system cert store")? {
        root_store
            .add(&Certificate(cert.0))
            .context("could not add certificate to cert store")?;
    }

    // Because we'll surely need to debug cert stores again someday, here's an ugly
    // printing routine.
    //
    // for subject in root_store.subjects() {
    //     let bytes = subject.0.iter().map(|n| *n as u8).collect::<Vec<_>>();
    //     let s = String::from_utf8_lossy(&bytes);
    //     trace!(ctx.log(), "Subject: {:?}", s.as_ref());
    // }

    Ok(ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth())
}
