//! The `license` subcommand.

use anyhow::{Context as _, Result};
use dbcrossbarlib::{config::Configuration, tokio_glue::spawn_blocking};
use std::path::{Path, PathBuf};
use structopt::StructOpt;
use tokio::{fs, io::AsyncWriteExt};

/// License output arguments.
#[derive(Debug, StructOpt)]
pub(crate) struct Opt {
    /// File in which to save the licenses [if missing, open in browser]
    out_html: Option<PathBuf>,
}

// The license text is huge, so compress it using the `flate!` macro. This adds
// a number of dependencies, but we'll evenutally want `libflate` for other
// things.
include_flate::flate!(
    /// Compile our license text into the binary.
    static LICENSE: [u8] from "ALL_LICENSES.html"
);

/// Perform our schema conversion.
pub(crate) async fn run(
    _config: Configuration,
    _enable_unstable: bool,
    opt: Opt,
) -> Result<()> {
    if let Some(out_html) = opt.out_html {
        write_licenses_html(&out_html).await
    } else {
        // Create a temporary file (that we don't clean up).
        let out_html: PathBuf = spawn_blocking(|| {
            tempfile::TempDir::new().context("could not create temporary directory")
        })
        .await?
        // `into_path` makes the directory stay around after we exit.
        .into_path()
        .join("ALL_LICENSES.html");
        write_licenses_html(&out_html).await?;

        // Open it in the browser.
        spawn_blocking(move || {
            opener::open(out_html).context("could not open temporary file in browser")
        })
        .await?;

        Ok(())
    }
}

/// Decompress our license text and write it to a file.
async fn write_licenses_html(path: &Path) -> Result<()> {
    let mut out = fs::File::create(path)
        .await
        .with_context(|| format!("could not create {}", path.display()))?;
    out.write_all(&LICENSE)
        .await
        .with_context(|| format!("could not write to {}", path.display()))?;
    Ok(())
}
