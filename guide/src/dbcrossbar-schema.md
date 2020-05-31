# Native `dbcrossbar` schemas

`dbcrossbar` supports a [native schema format][schema] that exactly represents all types supported by `dbcrossbar`. It can be used as follows:

```txt
--schema dbcrossbar-schema:my_table.json
```

For more details and example, see the chaper on [portable table schemas][schema].

## Typical uses

This format is cumbersome to edit by hand, but it is fairly useful in a number of circumstances:

- Specifying column types that can't be exactly represented by other schema formats.
- Reading or editing schemas using scripts.

[schema]: ./schema.html
