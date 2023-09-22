//! Support for looking up credentials.

use async_trait::async_trait;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    env, fmt,
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::{fs, sync::Mutex};

use crate::config::config_dir;
use crate::{common::*, config::system_config_dir};

/// A set of credentials that we can use to access a service.
///
/// This might be, for example, an account ID and an access key. A credential is
/// a set of key/value pairs. In general, the keys will be named using the same
/// naming conventions as the keys inside a vault secret.
#[derive(Clone, Debug)]
pub(crate) struct Credentials {
    data: HashMap<String, String>,
    expires: Option<Instant>,
}

impl Credentials {
    /// Get the specied value for this credential.
    pub(crate) fn get_required<'a>(&'a self, key: &str) -> Result<&'a str> {
        self.get_optional(key)
            .ok_or_else(|| format_err!("no key {:?} in credential", key))
    }

    /// Get the specified value for this credential, or `None` if it isn't
    /// available.
    pub(crate) fn get_optional<'a>(&'a self, key: &str) -> Option<&'a str> {
        self.data.get(key).map(|v| &v[..])
    }

    /// Does this secret need to be refreshed?
    fn needs_refresh(&self) -> bool {
        if let Some(expires) = self.expires {
            // Refresh any secret which will expire in the next 30 minutes.
            let minimum_useful_expiration =
                Instant::now() + Duration::from_secs(30 * 60);
            expires < minimum_useful_expiration
        } else {
            false
        }
    }
}

lazy_static! {
    /// Our `CredentialsManager` singleton.
    ///
    /// TODO: Figure out how to handle initialization errors without panicking.
    static ref MANAGER: CredentialsManager = CredentialsManager::new().unwrap();
}

/// An interface which can be used to look up credentials.
pub(crate) struct CredentialsManager {
    /// Places to look up credentials.
    ///
    /// You must _always_ lock the appropriate key here _before_ locking `cache`
    /// below.
    sources: HashMap<String, Mutex<Box<dyn CredentialsSource>>>,

    /// Credentials that we've already looked up. These may have expired.
    ///
    /// Before locking this, you must have locked exactly one key in `sources`.
    cache: Mutex<HashMap<String, Credentials>>,
}

impl CredentialsManager {
    /// Get the global singleton instance of `CredentialsManager`.
    pub(crate) fn singleton() -> &'static CredentialsManager {
        &MANAGER
    }

    /// Create a new credentials manager and install the default handlers.
    fn new() -> Result<CredentialsManager> {
        let mut sources = HashMap::new();
        let config_dir = config_dir()?;
        let system_config_dir =
            system_config_dir().expect("Failed to resolve system config path");

        // Specify how to connect to AWS.
        let aws = EnvCredentialsSource::new(vec![
            EnvMapping::required("access_key_id", "AWS_ACCESS_KEY_ID"),
            EnvMapping::required("secret_access_key", "AWS_SECRET_ACCESS_KEY"),
            EnvMapping::optional("session_token", "AWS_SESSION_TOKEN"),
            EnvMapping::required("default_region", "AWS_DEFAULT_REGION"),
        ]);
        sources.insert("aws".to_owned(), Mutex::new(aws.boxed()));

        // Specify how to find Google Cloud service account keys.
        let gcloud_service_account_key = CredentialsSources::new(vec![
            EnvCredentialsSource::new(vec![EnvMapping::required(
                "value",
                "GCLOUD_SERVICE_ACCOUNT_KEY",
            )])
            .boxed(),
            FileCredentialsSource::new(
                "value",
                config_dir.join("gcloud_service_account_key.json"),
            )
            .boxed(),
        ]);
        sources.insert(
            "gcloud_service_account_key".to_owned(),
            Mutex::new(gcloud_service_account_key.boxed()),
        );

        // Specify how to find Google Cloud client secret.
        let gcloud_client_secret = CredentialsSources::new(vec![
            EnvCredentialsSource::new(vec![EnvMapping::required(
                "value",
                "GCLOUD_CLIENT_SECRET",
            )])
            .boxed(),
            FileCredentialsSource::new(
                "value",
                config_dir.join("gcloud_client_secret.json"),
            )
            .boxed(),
        ]);
        sources.insert(
            "gcloud_client_secret".to_owned(),
            Mutex::new(gcloud_client_secret.boxed()),
        );

        // Specify how to find Google autorized user secret.
        let gcloud_authorized_user_secret =
            CredentialsSources::new(vec![FileCredentialsSource::new(
                "value",
                system_config_dir
                    .join("gcloud")
                    .join("application_default_credentials.json"),
            )
            .boxed()]);
        sources.insert(
            "gcloud_authorized_user_secret".to_owned(),
            Mutex::new(gcloud_authorized_user_secret.boxed()),
        );

        // Specify how to find a Shopify secret.
        let shopify_secret = EnvCredentialsSource::new(vec![EnvMapping::required(
            "auth_token",
            "SHOPIFY_AUTH_TOKEN",
        )]);
        sources.insert("shopify".to_owned(), Mutex::new(shopify_secret.boxed()));

        let cache = Mutex::new(HashMap::new());
        Ok(CredentialsManager { sources, cache })
    }

    /// Look up the credential `name` and return it.
    pub(crate) async fn get(&self, name: &str) -> Result<Credentials> {
        // Look up our source and lock it. We _must_ do this before locking
        // `cache`.
        let source = self
            .sources
            .get(name)
            .ok_or_else(|| format_err!("unknown credential {:?}", name))?;
        let source = source.lock().await;

        // See if we already have this secret in our cache. We need to be careful to
        // not hold `self.cache.lock()` beyond the end of the line.
        let credentials: Option<Credentials> =
            self.cache.lock().await.get(name).map(|c| c.to_owned());
        match credentials {
            Some(c) if !c.needs_refresh() => return Ok(c),
            _ => {}
        }

        // Try to look up these credentials.
        if let Some(c) = source.get_credentials().await? {
            // Cache it and return it.
            self.cache.lock().await.insert(name.to_owned(), c.clone());
            Ok(c)
        } else {
            // Explain to the user how they could have specified this
            // credential. The `Display` method on `CredentialsSource` is
            // responsible for explaining how to set credentials.
            Err(format_err!(
                "could not find credentials for {} in any of:\n{}",
                name,
                source,
            ))
        }
    }
}

/// An interface for looking up credentials.
///
/// We use the `async_trait` macro, which allows async functions to be declared
/// inside a trait. This isn't yet supported by standard Rust because there are
/// some tricky design issues that still need to be settled.
#[async_trait]
trait CredentialsSource: fmt::Debug + fmt::Display + Send + Sync + 'static {
    /// Look up an appropriate set of credentials.
    async fn get_credentials(&self) -> Result<Option<Credentials>>;
}

/// Extra methods for `CredentialsSource` which aren't "object safe" and which
/// would prevent us from using `dyn CredentialsSource` if we put them in
/// `CredentialsSource`.
trait CredentialsSourceExt: CredentialsSource + Sized + 'static {
    /// Convert to a `Box<dyn CredentialsSource>`.
    fn boxed(self) -> Box<dyn CredentialsSource> {
        Box::new(self)
    }
}

impl<CS: CredentialsSource> CredentialsSourceExt for CS {}

/// A mapping from a `Credentials` key name to an environment variable.
#[derive(Debug)]
struct EnvMapping {
    key: &'static str,
    var: &'static str,
    optional: bool,
}

impl EnvMapping {
    /// Fetch the value of `key` from `var`.
    fn required(key: &'static str, var: &'static str) -> Self {
        Self {
            key,
            var,
            optional: false,
        }
    }

    /// Fetch the value of `key` from `var`, if present.
    fn optional(key: &'static str, var: &'static str) -> Self {
        Self {
            key,
            var,
            optional: true,
        }
    }
}

impl fmt::Display for EnvMapping {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.optional {
            write!(f, "(optional) {}", self.var)
        } else {
            write!(f, "{}", self.var)
        }
    }
}

/// Look up credentials stored in environment variables.
#[derive(Debug)]
struct EnvCredentialsSource {
    mapping: Vec<EnvMapping>,
}

impl EnvCredentialsSource {
    /// Create a new `EnvCredentialsSource`.
    ///
    /// `mapping` should contain at least one element. The first element must
    /// not be `optional`.
    fn new(mapping: Vec<EnvMapping>) -> Self {
        // Check our preconditions with assertions, since all callers will be
        // hard-coded in the source.
        assert!(!mapping.is_empty());
        assert!(!mapping[0].optional);
        Self { mapping }
    }
}

impl fmt::Display for EnvCredentialsSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.mapping.len() == 1 {
            writeln!(f, "- The environment variable {}", &self.mapping[0])
        } else {
            writeln!(
                f,
                "- The environment variables {}",
                self.mapping.iter().join(", "),
            )
        }
    }
}

#[async_trait]
impl CredentialsSource for EnvCredentialsSource {
    async fn get_credentials(&self) -> Result<Option<Credentials>> {
        if let Some(value) = try_var(self.mapping[0].var)? {
            // Our first environment variable is present
            let mut data = HashMap::new();
            data.insert(self.mapping[0].key.to_owned(), value);
            for m in &self.mapping[1..] {
                if m.optional {
                    if let Some(value) = try_var(m.var)? {
                        data.insert(m.key.to_owned(), value);
                    }
                } else {
                    data.insert(m.key.to_owned(), var(m.var)?);
                }
            }
            Ok(Some(Credentials {
                data,
                expires: None,
            }))
        } else {
            // The necessary environment varaibles are not set.
            Ok(None)
        }
    }
}

/// Look up an environment variable by name, returning `Ok(None)` if it does not
/// exist.
fn try_var(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(..)) => Err(format_err!(
            "environment variable {} cannot be converted to UTF-8",
            name,
        )),
    }
}

/// Look up an environment variable by name, returning an error if it does not exist.
fn var(name: &str) -> Result<String> {
    match try_var(name)? {
        Some(value) => Ok(value),
        None => Err(format_err!(
            "expected environment variable {} to be set",
            name,
        )),
    }
}

/// Load credentials stored in a file.
#[derive(Debug)]
struct FileCredentialsSource {
    key: &'static str,
    path: PathBuf,
}

impl FileCredentialsSource {
    /// Specify how to find credentials in a file. The contents of the file will
    /// be mapped to `key` in the credential.
    fn new(key: &'static str, path: PathBuf) -> Self {
        Self { key, path }
    }
}

impl fmt::Display for FileCredentialsSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "- The file {}", self.path.display())
    }
}

#[async_trait]
impl CredentialsSource for FileCredentialsSource {
    async fn get_credentials(&self) -> Result<Option<Credentials>> {
        match fs::read_to_string(&self.path).await {
            Ok(value) => {
                let mut data = HashMap::new();
                data.insert(self.key.to_owned(), value);
                Ok(Some(Credentials {
                    data,
                    expires: None,
                }))
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(format_err!(
                "error reading {}: {}",
                self.path.display(),
                err
            )),
        }
    }
}

/// Look in multiple places for credentials.
#[derive(Debug)]
struct CredentialsSources {
    sources: Vec<Box<dyn CredentialsSource>>,
}

impl CredentialsSources {
    /// Create a new list of credentials sources that will be searched in order.
    fn new(sources: Vec<Box<dyn CredentialsSource>>) -> Self {
        Self { sources }
    }
}

impl fmt::Display for CredentialsSources {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for s in &self.sources {
            write!(f, "{}", s)?;
        }
        Ok(())
    }
}

#[async_trait]
impl CredentialsSource for CredentialsSources {
    async fn get_credentials(&self) -> Result<Option<Credentials>> {
        for source in &self.sources {
            if let Some(credentials) = source.get_credentials().await? {
                return Ok(Some(credentials));
            }
        }
        Ok(None)
    }
}
