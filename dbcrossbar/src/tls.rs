//! Support for setting up RusTLS in a consistent fashion.

use rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;

use crate::common::*;

/// Standard RusTLS `ClientConfig` setup.
///
/// We hope to be able to reuse this for multiple different types of TLS
/// connections.
pub(crate) fn rustls_client_config() -> Result<ClientConfig> {
    // Set up RusTLS.
    let mut root_store = RootCertStore::empty();
    let cert_result = load_native_certs();
    for cert in cert_result.certs {
        root_store
            .add(cert)
            .context("could not add certificate to cert store")?;
    }
    if let Some(err) = cert_result.errors.into_iter().next() {
        return Err(err).context("error loading native certs");
    }

    // Because we'll surely need to debug cert stores again someday, here's an ugly
    // printing routine.
    //
    // for subject in root_store.subjects() {
    //     let bytes = subject.0.iter().map(|n| *n as u8).collect::<Vec<_>>();
    //     let s = String::from_utf8_lossy(&bytes);
    //     trace!("Subject: {:?}", s.as_ref());
    // }

    Ok(ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth())
}
