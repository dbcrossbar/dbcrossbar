# TypeScript schemas (UNSTABLE)

**WARNING:** This is highly experimental and subject to change. To use it, you must enable it using the `--enable-unstable` flag.

To specify the column names and types for table using a subset of TypeScript, use:

```txt
--schema "dbcrossbar-ts:my_table.ts#MyTable"
```

The file `my_table.ts` can contain one or more `interface` definitions:

```ts
interface MyTable {
    id: string,
    name: string,
    quantity: number,
}
```

## "Magic" types

Certain `dbcrossbar` types can be specified by adding the following declarations to a TypeScript file:

```ts
// Decimal numbers which can exactly represent
// currency values with no rounding.
type decimal = number | string;

// Integers of various sizes.
type int16 = number | string;
type int32 = number | string;
type int64 = number | string;
```

These may then be used as follows:

```ts
interface OrderItem {
    id: int64,
    sku: string,
    unit_price: decimal,
    quantity: int16,
}
```

When the TypeScript schema is converted to a portable `dbcrossbar` schema, the "magic" types will be replaced with the corresponding portable type.

### Advanced features

We also support nullable values, arrays and nested structures:

```ts
type decimal = number | string;
type int16 = number | string;
type int32 = number | string;
type int64 = number | string;

interface Order {
    id: int64,
    line_items: OrderItem[],
    note: string | null,
}

interface OrderItem {
    id: int64,
    sku: string,
    unit_price: decimal,
    quantity: int16,
}
```

Nested arrays and structs will translate to appropriate database-specific types, such as BigQuery `ARRAY` and `STRUCT` types.

## Limitations

This schema format has a number of limitations:

- There's no way to convert other schema formats into this one (yet).
- Some portable `dbcrossbar` types can't be represented in this format.
- Only a small subset of TypeScript is supported (but we try to give good error messages).
