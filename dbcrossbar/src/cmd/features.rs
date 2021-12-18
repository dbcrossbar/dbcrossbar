//! The `features` subcommand.

use anyhow::Result;
use dbcrossbarlib::{
    config::Configuration,
    drivers::{all_drivers, find_driver},
    Context,
};
use structopt::{self, StructOpt};

/// Schema conversion arguments.
#[derive(Debug, StructOpt)]
pub(crate) struct Opt {
    /// Print help about a specific driver name.
    driver: Option<String>,
}

/// Perform our schema conversion.
pub(crate) async fn run(
    _ctx: Context,
    _config: Configuration,
    enable_unstable: bool,
    opt: Opt,
) -> Result<()> {
    if let Some(name) = &opt.driver {
        let scheme = format!("{}:", name);
        let driver = find_driver(&scheme, enable_unstable)?;
        println!("{} features:", name);
        print!("{}", driver.features());
        if driver.is_unstable() {
            println!("\nThis driver is UNSTABLE and may change without warning.");
        }
    } else {
        println!("Supported drivers:");
        for driver in all_drivers() {
            if !driver.is_unstable() || enable_unstable {
                if driver.is_unstable() {
                    println!("- {} (UNSTABLE)", driver.name());
                } else {
                    println!("- {}", driver.name());
                }
            }
        }
        println!(
            "\nUse `dbcrossbar features $DRIVER` to list the features supported by a driver."
        );
    }
    Ok(())
}
