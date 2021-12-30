//! Support for setting up RusTLS in a consistent fashion.

use std::{
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    sync::RwLock,
};

use lazy_static::lazy_static;
use rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore};
use rustls_native_certs::load_native_certs;

use crate::common::*;

/// Where to find our TLS client certificates.
pub struct ClientCertInfo {
    /// Path to our public key.
    pub cert_path: PathBuf,

    /// Path to our client key.
    pub key_path: PathBuf,
}

lazy_static! {
    /// Our client cert, if we have one.
    static ref CLIENT_CERT: RwLock<Option<ClientCertInfo>> = RwLock::new(None);

    /// Extra trusted CAs.
    static ref EXTRA_TRUSTED_CAS: RwLock<Vec<PathBuf>> = RwLock::new(vec![]);
}

/// Specify the client cert to use when configuring TLS.
///
/// This must be called before `rustls_client_config` if you want to use a
/// client cert. This is a global setting, so that we don't need to thread it
/// through every TLS caller and test case.
pub fn register_client_cert(client_cert: ClientCertInfo) -> Result<()> {
    let mut cert = CLIENT_CERT.write().expect("lock poisoned");
    if cert.is_some() {
        Err(format_err!("tried to call `register_client_cert` twice"))
    } else {
        *cert = Some(client_cert);
        Ok(())
    }
}

/// Register a trusted certificate authority.
pub fn register_trusted_ca(trusted_ca: &Path) -> Result<()> {
    let mut cas = EXTRA_TRUSTED_CAS.write().expect("lock poisoned");
    cas.push(trusted_ca.to_owned());
    Ok(())
}

/// Standard RusTLS `ClientConfig` setup.
///
/// We hope to be able to reuse this for multiple different types of TLS
/// connections.
///
/// We load our server certificates out of the operating system's certificate
/// store, because this is reasonably standardized. We only support a single
/// client certificate, and that must be passed manually for now.
#[instrument(level = "trace")]
pub(crate) fn rustls_client_config() -> Result<ClientConfig> {
    // Set up RusTLS.
    let mut root_store = RootCertStore::empty();
    for cert in load_native_certs().context("could not find system cert store")? {
        root_store
            .add(&Certificate(cert.0))
            .context("could not add certificate to cert store")?;
    }

    // Install any extra trusted CAs manually.
    let trusted_cas = EXTRA_TRUSTED_CAS.read().expect("lock poisoned");
    for trusted_ca in trusted_cas.iter() {
        trace!("trusting CA {}", trusted_ca.display());
        let chain = read_cert_chain(trusted_ca)?;
        for cert in chain {
            root_store
                .add(&cert)
                .context("could not add certificate to cert store")?;
        }
    }

    // Because we'll surely need to debug cert stores again someday, here's an ugly
    // printing routine.
    //
    // for subject in root_store.subjects() {
    //     let bytes = subject.0.iter().map(|n| *n as u8).collect::<Vec<_>>();
    //     let s = String::from_utf8_lossy(&bytes);
    //     trace!("Subject: {:?}", s.as_ref());
    // }

    // Configure our TLS client.
    let base_config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store);

    // Set up client certificates, if any.
    let client_cert = CLIENT_CERT.read().expect("lock poisoned");
    if let Some(client_cert) = client_cert.as_ref() {
        let cert_chain = read_cert_chain(&client_cert.cert_path)?;
        let key_der = read_private_key(&client_cert.key_path)?;
        base_config
            .with_single_cert(cert_chain, key_der)
            .context("could not configure client TLS cert")
    } else {
        Ok(base_config.with_no_client_auth())
    }
}

/// Parse a `.pem` or `.key` file into a list of items.
fn read_pemfile(path: &Path) -> Result<Vec<rustls_pemfile::Item>> {
    let f = File::open(path)
        .with_context(|| format!("could not open {}", path.display()))?;
    let mut rdr = BufReader::new(f);
    rustls_pemfile::read_all(&mut rdr).with_context(|| {
        format!("could not load TLS certificates from {}", path.display())
    })
}

/// Read a `.pem` file containing certs.
fn read_cert_chain(path: &Path) -> Result<Vec<Certificate>> {
    let cert_chain = read_pemfile(path)?
        .into_iter()
        .filter_map(|item| match item {
            rustls_pemfile::Item::X509Certificate(bytes) => Some(Certificate(bytes)),
            _ => None,
        })
        .collect::<Vec<_>>();
    if cert_chain.is_empty() {
        Err(format_err!("no certs found in {}", path.display()))
    } else {
        Ok(cert_chain)
    }
}

/// Read a `.key` file containing a private key.
fn read_private_key(path: &Path) -> Result<PrivateKey> {
    let keys = read_pemfile(path)?
        .into_iter()
        .filter_map(|item| match item {
            rustls_pemfile::Item::RSAKey(key)
            | rustls_pemfile::Item::PKCS8Key(key) => Some(PrivateKey(key)),
            _ => None,
        })
        .collect::<Vec<_>>();
    if keys.len() != 1 {
        Err(format_err!(
            "expected to find a single private key in {}",
            path.display()
        ))
    } else {
        Ok(keys[0].clone())
    }
}
