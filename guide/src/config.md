# Configuring `dbcrossbar`

`dbcrossbar` can read information from a configuration directory. By default, this can be found at:

- Linux: `~/.config`
- MacOS: `~/Library/Preferences`

To override this default location, you can set `DBCROSSBAR_CONFIG_DIR` to point to an alternate configuration directory.

If a file `dbcrossbar.toml` appears in this directory, `dbcrossbar` will read its configuration from that file. Other files may be placed in this directory, including certain local credential files.

## Modifying the configuration file

You can modify the `dbcrossbar.toml` file from the command line using the `config` subcommand. For example:

```sh
dbcrossbar config add temporary s3://example/temp/
dbcrossbar config rm temporary s3://example/temp/
```

Using `config add temporary` allows you to specify default values for `--temporary` flags. You can still override specific defaults by passing `--temporary` to commands that use it.
