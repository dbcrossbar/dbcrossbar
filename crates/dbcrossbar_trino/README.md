# `dbcrossbar_trino`: A lightweight Trino interface for `dbcrossbar` and related tools

[Documentation][docs]

This crate is a support crate shared by `dbcrossbar` and related tools. It provides:

- Tools for working around limitations of various Trino connectors
    - Tools for listing which features are supported by a given Trino connector.
    - Tools for translating common Trino data types to "storage" types that are supported by a given connector.
- A simple Trino client, intended for testing.
- A dynamic representation of Trino values, and the ability to format them as SQL.
- Other miscellaneous utilities that are useful for working with Trino.

You are welcome to use this crate outside of `dbcrossbar` if you find it useful. We plan to obey semver. But the features supported by this crate will be driven largely by `dbcrossbar` and related tools.

For more information, see the [documentation][docs].

[docs]: https://docs.rs/dbcrossbar_trino/
