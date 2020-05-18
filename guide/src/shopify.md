# Experimental: Shopify

**WARNING:** This is highly experimental and subject to change.

Shopify is an online e-commerce platform with a REST API for fetching data.

## Example locators

Locators look just like Shopify REST API URLs, but with `https:` replaced with `shopify`:

- `shopify://$SHOP/admin/api/2020-04/orders.json?status=any`

For a schema, download [shopify.ts][], and refer to it as follows:

- `--schema="dbcrossbar-ts:shopify.ts#Order"`

We do not currently include a default Shopify schema in `dbcrossbar` itself, because it's still undergoing significant changes.

[shopify.ts]: https://github.com/dbcrossbar/dbcrossbar/blob/master/dbcrossbar/fixtures/shopify.ts

## Configuration & authentication

The following environment variables are required:

- `SHOPIFY_AUTH_TOKEN`: The Shopify authorization token to use. (We don't yet support password authentication, but it would be easy enough to add.)

## Supported features

```txt
{{#include generated/features_shopify.txt}}
```
