//! Configuration file support.
use std::{
    env, fmt,
    fs::{create_dir_all, File},
    io::{self, Read},
    path::{Path, PathBuf},
};
use toml_edit::{Array, Document, Item, Value};

use crate::common::*;

/// Return `dirs::config_dir()`.
#[cfg(not(target_os = "macos"))]
pub(crate) fn system_config_dir() -> Option<PathBuf> {
    dirs::config_dir()
}

/// (Mac only.) Return `dirs::preference_dir()` if contains a `dbcrossbar`
/// directory, or `dirs::config_dir()` otherwise.
///
/// See https://github.com/dirs-dev/directories-rs/issues/62 for an explanation
/// of why this changed.
///
/// TODO: We should really use xdg directories on all platforms.
#[cfg(target_os = "macos")]
fn system_config_dir() -> Option<PathBuf> {
    if let Some(preference_dir) = dirs::preference_dir() {
        let old_config_dir = preference_dir.join("dbcrossbar");
        if old_config_dir.is_dir() {
            // Warn the user only once per run.
            use std::sync::Once;
            static ONCE: Once = Once::new();
            ONCE.call_once(|| {
                // Deprecate the old location.
                if let Some(system_config_dir) = dirs::config_dir() {
                    let new_config_dir = system_config_dir.join("dbcrossbar");
                    eprintln!(
                        "DEPRECATION WARNING: Please move `{}` to `{}`",
                        old_config_dir.display(),
                        new_config_dir.display(),
                    );
                }
            });

            return Some(preference_dir);
        }
    }
    dirs::config_dir()
}

/// Find the path to our configuration directory.
pub(crate) fn config_dir() -> Result<PathBuf> {
    // Use `var_os` instead of `var`, because if it returns a non-Unicode path,
    // we can hand it off directly to `PathBuf`.
    match env::var_os("DBCROSSBAR_CONFIG_DIR") {
        // The user specified a config directory, so use that.
        Some(dir) => Ok(PathBuf::from(dir)),
        // Use `dbcrossbar/` in the system configuration directory.
        None => Ok(system_config_dir()
            // AFAIK, this only fails under weird conditions, such as no home
            // directory.
            .ok_or_else(|| format_err!("could not find user config dir"))?
            .join("dbcrossbar")),
    }
}

/// Find the path to our configuration file.
pub(crate) fn config_file() -> Result<PathBuf> {
    Ok(config_dir()?.join("dbcrossbar.toml"))
}

/// A configuration file key.
///
/// This is opaque to allow for driver-specific and host-specific keys in the
/// future.
#[derive(Debug)]
pub struct Key<'a> {
    /// The key in the TOML file.
    key: &'a str,
}

impl Key<'_> {
    /// A key for accessing `temporary`
    pub fn temporary() -> Key<'static> {
        Self::global("temporary")
    }

    /// A top-level configuration key.
    pub(crate) fn global(key: &str) -> Key<'_> {
        Key { key }
    }
}

impl<'a> fmt::Display for Key<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.key.fmt(f)
    }
}

/// Our `dbcrossbar.toml` configuration file.
#[derive(Debug)]
pub struct Configuration {
    /// The path from which we read this file.
    path: PathBuf,
    /// Our raw configuration data.
    doc: Document,
}

// A lot of the implementation of `Configuration` is fairly verbose, because
// we're using `toml_edit`. The upside of `toml_edit` is that it allows us to
// load, edit and configuration file without losing whitespace or comments that
// the user had in the file. The downside is that it's not nearly as nice as
// `serde`, and we have to manipulate a lot of dynamic, untyped data in Rust.
// This tends to be more verbose than either using `serde` or writing similar
// code in Ruby. But on the other hand: We can edit files with comments in them!
impl Configuration {
    /// Load our default configuration.
    pub fn try_default() -> Result<Self> {
        Self::from_path(&config_file()?)
    }

    /// Load the configuration file at `path`.
    pub(crate) fn from_path(path: &Path) -> Result<Self> {
        match File::open(path) {
            Ok(rdr) => Ok(Self::from_reader(path.to_owned(), rdr)
                .with_context(|| format!("could not read file {}", path.display()))?),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Self {
                path: path.to_owned(),
                doc: Document::default(),
            }),
            Err(err) => {
                Err(err).context(format!("could not open file {}", path.display()))
            }
        }
    }

    /// Load a configuration file from the specified reader.
    fn from_reader<R>(path: PathBuf, mut rdr: R) -> Result<Self>
    where
        R: Read + 'static,
    {
        let mut buf = String::new();
        rdr.read_to_string(&mut buf)?;
        let doc = buf.parse::<Document>()?;
        Ok(Self { path, doc })
    }

    /// Write the configuration file to disk.
    pub fn write(&self) -> Result<()> {
        let parent = self.path.parent().ok_or_else(|| {
            format_err!("cannot find parent directory of {}", self.path.display())
        })?;
        create_dir_all(parent)
            .with_context(|| format!("cannot create {}", parent.display()))?;
        let data = self.doc.to_string();
        let mut f = File::create(&self.path)
            .with_context(|| format!("cannot create {}", self.path.display()))?;
        f.write_all(data.as_bytes())
            .with_context(|| format!("error writing to {}", self.path.display()))?;
        f.flush()
            .with_context(|| format!("error writing to {}", self.path.display()))?;
        Ok(())
    }

    /// Return a list of places to store temporary data.
    pub fn temporaries(&self) -> Result<Vec<String>> {
        self.string_array(&Key::global("temporary"))
    }

    /// Get an array of strings from our config file.
    fn string_array(&self, key: &Key<'_>) -> Result<Vec<String>> {
        let mut temps = vec![];
        if let Some(raw_value) = self.doc.as_table().get(key.key) {
            if let Some(raw_array) = raw_value.as_array() {
                for raw_item in raw_array.iter() {
                    if let Some(temp) = raw_item.as_str() {
                        temps.push(temp.to_owned());
                    } else {
                        return Err(format_err!(
                            "expected string, found {:?} in {}",
                            raw_item,
                            self.path.display(),
                        ));
                    }
                }
            } else {
                return Err(format_err!(
                    "expected array, found {:?} in {}",
                    raw_value,
                    self.path.display(),
                ));
            }
        }
        Ok(temps)
    }

    /// Get our an array of strings in mutable form.
    fn raw_string_array_mut<'a>(&'a mut self, key: &Key<'_>) -> Result<&'a mut Array> {
        let array_value = self
            .doc
            .as_table_mut()
            .entry(key.key)
            .or_insert(Item::Value(Value::Array(Array::default())));
        match array_value.as_array_mut() {
            Some(array) => Ok(array),
            None => Err(format_err!(
                "expected array for {} in {}",
                key,
                self.path.display(),
            )),
        }
    }

    /// Add a new value to an array of strings, if it's not already there.
    pub fn add_to_string_array(&mut self, key: &Key<'_>, value: &str) -> Result<()> {
        let raw_array = self.raw_string_array_mut(key)?;
        for raw_item in raw_array.iter() {
            if raw_item.as_str() == Some(value) {
                // Already present, so don't add it again.
                return Ok(());
            }
        }
        raw_array.push(value);
        raw_array.fmt();
        Ok(())
    }

    /// Remove a value from an array of strings, if present. If more
    /// than one copy is present it will remove all.
    pub fn remove_from_string_array(
        &mut self,
        key: &Key<'_>,
        value: &str,
    ) -> Result<()> {
        let raw_array = self.raw_string_array_mut(key)?;

        // Find matches.
        let mut indices = vec![];
        for (idx, raw_item) in raw_array.iter().enumerate() {
            if raw_item.as_str() == Some(value) {
                indices.push(idx);
            }
        }

        // Remove in reverse order to avoid invalidating indices.
        for idx in indices.iter().rev().cloned() {
            raw_array.remove(idx);
        }
        raw_array.fmt();
        Ok(())
    }
}

#[test]
fn temporaries_can_be_added_and_removed() {
    let temp = tempfile::Builder::new()
        .prefix("dbcrossbar")
        .suffix(".toml")
        .tempfile()
        .unwrap();
    let path = temp.path();
    let mut config = Configuration::from_path(path).unwrap();
    let key = Key::global("temporary");
    assert_eq!(config.temporaries().unwrap(), Vec::<String>::new());
    config.add_to_string_array(&key, "s3://example/").unwrap();
    assert_eq!(config.temporaries().unwrap(), &["s3://example/".to_owned()]);
    config.write().unwrap();
    config = Configuration::from_path(path).unwrap();
    assert_eq!(config.temporaries().unwrap(), &["s3://example/".to_owned()]);
    config
        .remove_from_string_array(&key, "s3://example/")
        .unwrap();
    assert_eq!(config.temporaries().unwrap(), Vec::<String>::new());
}
