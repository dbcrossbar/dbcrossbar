//! Command parsing.

use std::path::PathBuf;

use dbcrossbarlib::{
    config::Configuration,
    tls::{register_client_cert, ClientCertInfo},
    Context, Result,
};
//use structopt::StructOpt;
use structopt_derive::StructOpt;

pub(crate) mod config;
pub(crate) mod count;
pub(crate) mod cp;
pub(crate) mod features;
pub(crate) mod license;
pub(crate) mod schema;

/// Command-line options, parsed using `structopt`.
#[derive(Debug, StructOpt)]
#[structopt(
    name = "dbcrossbar",
    about = "Convert schemas and data between databases."
)]
pub(crate) struct Opt {
    /// Enable unstable, experimental features.
    #[structopt(long = "enable-unstable")]
    pub(crate) enable_unstable: bool,

    /// Path to TLS client certificate file (*.pem).
    #[structopt(long = "tls-client-cert", requires("tls_client_key"))]
    pub(crate) tls_client_cert: Option<PathBuf>,

    /// Path to TLS client private key file (*.key).
    #[structopt(long = "tls-client-key", requires("tls_client_cert"))]
    pub(crate) tls_client_key: Option<PathBuf>,

    /// The command to run.
    #[structopt(subcommand)]
    pub(crate) cmd: Command,
}

impl Opt {
    /// Get our client cert, if any.
    fn client_cert(&self) -> Option<ClientCertInfo> {
        match (&self.tls_client_cert, &self.tls_client_key) {
            (None, None) => None,
            (Some(cert_path), Some(key_path)) => Some(ClientCertInfo {
                cert_path: cert_path.to_owned(),
                key_path: key_path.to_owned(),
            }),
            _ => panic!("arg parser is not enforcing `requires`"),
        }
    }
}

/// The command to run.
#[derive(Debug, StructOpt)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Command {
    /// Update configuration.
    #[structopt(name = "config")]
    Config {
        #[structopt(flatten)]
        command: config::Opt,
    },

    /// Count records.
    #[structopt(name = "count")]
    #[structopt(after_help = r#"EXAMPLE LOCATORS:
    postgres://localhost:5432/db#table
    bigquery:project:dataset.table
"#)]
    Count {
        #[structopt(flatten)]
        command: count::Opt,
    },

    /// Copy tables from one location to another.
    #[structopt(name = "cp")]
    #[structopt(after_help = r#"EXAMPLE LOCATORS:
    postgres://localhost:5432/db#table
    bigquery:project:dataset.table
"#)]
    Cp {
        #[structopt(flatten)]
        command: cp::Opt,
    },

    /// List available drivers and supported features.
    #[structopt(name = "features")]
    Features {
        #[structopt(flatten)]
        command: features::Opt,
    },

    /// Display license information.
    #[structopt(name = "license")]
    License {
        #[structopt(flatten)]
        command: license::Opt,
    },

    /// Schema-related commands.
    Schema {
        #[structopt(flatten)]
        command: schema::Opt,
    },
}

pub(crate) async fn run(ctx: Context, config: Configuration, opt: Opt) -> Result<()> {
    if let Some(client_cert) = opt.client_cert() {
        register_client_cert(client_cert)?;
    }

    match opt.cmd {
        Command::Config { command } => config::run(config, command).await,

        Command::Count { command } => {
            count::run(ctx, config, opt.enable_unstable, command).await
        }
        Command::Cp { command } => {
            cp::run(ctx, config, opt.enable_unstable, command).await
        }
        Command::Features { command } => {
            features::run(config, opt.enable_unstable, command).await
        }
        Command::License { command } => {
            license::run(config, opt.enable_unstable, command).await
        }
        Command::Schema { command } => {
            schema::run(ctx, config, opt.enable_unstable, command).await
        }
    }
}
