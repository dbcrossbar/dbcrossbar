//! The `config` subcommand.

use anyhow::{format_err, Result};
use clap::Parser;
use dbcrossbarlib::{
    config::{Configuration, Key},
    tokio_glue::spawn_blocking,
};

/// Configutation-editing arguments.
#[derive(Debug, Parser)]
pub(crate) struct Opt {
    /// The command to perform on the configuration key.
    #[clap(subcommand)]
    command: Command,
}

/// Shared options that specify a key.
#[derive(Debug, Parser)]
pub(crate) struct KeyOpt {
    /// The configuration key to operate on [values: temporary].
    key: String,
    // We'll probably extend this with options for driver-specific and
    // host-specific keys at some point.
}

impl KeyOpt {
    /// Get our configuration key.
    fn to_key(&self) -> Result<Key<'static>> {
        match &self.key[..] {
            "temporary" => Ok(Key::temporary()),
            other => Err(format_err!("unknown configuration key {:?}", other)),
        }
    }
}

/// A command that we can perform on a config key.
#[derive(Debug, Parser)]
pub(crate) enum Command {
    /// Add the specified value to the configuration key if it isn't already there.
    #[clap(name = "add")]
    Add {
        #[clap(flatten)]
        key: KeyOpt,

        /// The value to add.
        value: String,
    },

    /// Remove the specified value from the configuration key if it isn't
    /// already there.
    #[clap(name = "rm")]
    Remove {
        #[clap(flatten)]
        key: KeyOpt,

        /// The value to remove.
        value: String,
    },
}

/// Edit our config file.
pub(crate) async fn run(mut config: Configuration, opt: Opt) -> Result<()> {
    match &opt.command {
        Command::Add { key, value } => {
            config.add_to_string_array(&key.to_key()?, value)?;
        }
        Command::Remove { key, value } => {
            config.remove_from_string_array(&key.to_key()?, value)?;
        }
    }
    spawn_blocking(move || config.write()).await
}
