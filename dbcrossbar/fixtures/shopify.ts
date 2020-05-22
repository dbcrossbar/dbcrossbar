// This is a Shopify REST schema that we built by reading the docs.

/** A decimal value, usually represented as a string for accuracy. */
type decimal = number | string;

/**
 * An integer which can be serialized as either a floating point value, or a
 * string value.
 */
type int64 = number | string;

/**
 * A Shopify [Order][].
 * 
 * [Order]: https://shopify.dev/docs/admin-api/rest/reference/orders/order?api[version]=2020-04
 */
interface Order {
    app_id: int64,
    billing_address: Address | null,
    browser_ip: string | null,
    buyer_accepts_marketing: string | null,
    cancel_reason: string | null,
    cancelled_at: Date | null, // ISO 8601
    cart_token: string | null,
    client_details: ClientDetails | null,
    closed_at: Date | null,
    created_at: Date | null,
    currency: string | null,
    current_total_duties_set: string | null,
    customer: Customer | null,
    customer_local: string | null,
    discount_applications: DiscountApplication[] | null,
    discount_codes: DiscountCode[] | null,
    email: string | null,
    financial_status: string | null,
    fulfillments: Fulfillment[] | null,
    fulfillment_status: string | null,
    gateway: string | null, // Deprecated.
    id: int64,
    landing_site: string | null,
    line_items: LineItem[] | null,
    location_id: int64 | null,
    name: string | null,
    note: string | null,
    note_attributes: Property[] | null,
    number: int64 | null,
    order_number: int64 | null,
    original_total_duties_set: PriceSet | null,
    payment_details: PaymentDetails | null, // Deprecated.
    payment_gateway_names: string[] | null,
    phone: string | null,
    presentment_currency: string | null,
    processed_at: Date | null,
    processing_method: string | null,
    referring_site: string | null,
    refunds: Refund[] | null,
    shipping_address: Address | null, // Optional.
    shipping_lines: ShippingLine[] | null,
    source_name: string | null,
    subtotal_price: number | null,
    subtotal_price_set: PriceSet | null,
    tags: string | null, // Comma-separated.
    tax_lines: TaxLine[] | null, // May not have `price_set` here?
    taxes_included: boolean | null,
    test: boolean | null,
    token: string | null,
    total_discounts: decimal | null,
    total_discounts_set: PriceSet | null,
    total_line_items_price: decimal | null,
    total_line_items_price_set: PriceSet | null,
    total_price_set: PriceSet | null,
    total_tax: decimal | null,
    total_tax_set: PriceSet | null,
    total_tip_received: decimal | null,
    total_weight: number | null,
    updated_at: Date | null,
    user_id: int64 | null,
    order_status_url: string | null,
}

/**
 * An address in an [Order][].
 * 
 * [Order]: https://shopify.dev/docs/admin-api/rest/reference/orders/order?api[version]=2020-04
 */
interface Address {
    address1: string | null,
    address2: string | null,
    city: string | null,
    company: string | null,
    country: string | null,
    first_name: string | null,
    last_name: string | null,
    phone: string | null,
    province: string | null,
    zip: string | null,
    name: string | null,
    province_code: string | null,
    country_code: string | null,
    // We could treat these as decimal values instead.
    latitude: string | null,
    longitude: string | null,
}

/**
 * Information about the browser used to place an [Order][].
 * 
 * [Order]: https://shopify.dev/docs/admin-api/rest/reference/orders/order?api[version]=2020-04
 */
interface ClientDetails {
    accepts_language: string | null,
    browser_height: number | null,
    browser_ip: string | null,
    browser_width: number | null,
    session_hash: string | null,
    user_agent: string | null,
}

/**
 * A Shopify [Customer][].
 * 
 * [Customer]: https://shopify.dev/docs/admin-api/rest/reference/customers/customer?api[version]=2020-04
 */
interface Customer {
    accepts_marketing: boolean | null,
    accepts_marketing_updated_at: Date | null,
    addresses: CustomerAddress[] | null,
    admin_graphql_api_id: string | null,
    created_at: string | null,
    currency: string | null,
    default_address: CustomerAddress | null,
    email: string | null,
    first_name: string | null,
    id: int64,
    last_name: string | null,
    last_order_id: int64 | null,
    last_order_name: string | null,
    //metafield: Metafield | null,
    multipass_identifier: string | null,
    note: string | null,
    orders_count: int64 | null, // String as integer.
    phone: string | null,
    state: string | null, // "disabled" is a valid value.
    tags: string | null,
    tax_exempt: boolean | null,
    tax_exemptions: string[] | null,
    total_spent: decimal | null,
    updated_at: Date | null,
    verified_email: boolean | null,
}

/**
 * An address associated with a Shopify [Customer][].
 * 
 * This is not actually the same as the `Address` type on `Order`. 
 * 
 * [Customer]: https://shopify.dev/docs/admin-api/rest/reference/customers/customer?api[version]=2020-04
 */
interface CustomerAddress {
    address1: string | null,
    address2: string | null,
    city: string | null,
    company: string | null,
    country_code: string | null,
    country: string | null,
    country_name: string | null,
    customer_id: int64 | null,
    default: boolean | null,
    first_name: string | null,
    id: int64,
    last_name: string | null,
    name: string | null,
    phone: string | null,
    province_code: string | null,
    province: string | null,
    zip: string | null,
}

/**
 * A discount application. Part of an [Order][].
 * 
 * [Order]: https://shopify.dev/docs/admin-api/rest/reference/orders/order?api[version]=2020-04
 */
interface DiscountApplication {
    type: string | null,
    description: string | null,
    value: decimal | null,
    value_type: string | null,
    allocation_method: string | null,
    target_selection: string | null,
    target_type: string | null,
}

/**
 * A discount applied to an [Order][].
 * 
 * [Order]: https://shopify.dev/docs/admin-api/rest/reference/orders/order?api[version]=2020-04
 */
interface DiscountCode {
    code: string | null,
    amount: decimal | null,
    type: string | null,
}

/**
 * A Shopify [Fulfillment][].
 * 
 * [Fulfillment]: https://shopify.dev/docs/admin-api/rest/reference/shipping-and-fulfillment/fulfillment?api[version]=2020-04
 */
interface Fulfillment {
    created_at: Date | null,
    id: int64,
    line_items: LineItem[] | null,
    location_id: number | null,
    name: string | null,
    notify_customer: boolean | null,
    order_id: string | null,
    receipt: Receipt | null,
    service: string | null,
    shipment_status: string | null,
    status: string | null,
    tracking_company: string | null,
    tracking_numbers: string[] | null,
    tracking_urls: string[] | null,
    updated_at: string | null,
    variant_inventory_management: string | null,
}

/**
 * A line item in an [Order][] or a [Fulfillment][].
 * 
 * [Order]: https://shopify.dev/docs/admin-api/rest/reference/orders/order?api[version]=2020-04
 * [Fulfillment]: https://shopify.dev/docs/admin-api/rest/reference/shipping-and-fulfillment/fulfillment?api[version]=2020-04
 */
interface LineItem {
    fulfillable_quantity: number | null,
    fulfillment_service: string | null,
    fulfillment_status: string | null,
    grams: number | null,
    id: int64,
    price: decimal | null,
    product_id: int64 | null,
    quantity: int64 | null, // Let's hope this is an integer.
    requires_shipping: boolean | null,
    sku: string | null,
    title: string | null,
    variant_id: int64 | null,
    variant_title: string | null,
    vendor: string | null,
    name: string | null,
    gift_card: boolean | null,
    price_set: PriceSet | null,
    properties: Property[] | null,
    taxable: boolean | null,
    tax_lines: TaxLine[] | null,
    total_discount: decimal | null,
    total_discount_set: PriceSet | null,
    discount_allocations: DiscountAllocation[] | null,
    duties: Duty[] | null,
    tip_payment_gateway?: string | null,
    tip_payment_method?: string | null,

    // These are seen on the fulfillment page, but not the order page.
    variant_inventory_management?: string | null,
    product_exists?: boolean | null,
}

/**
 * A receipt for a Shopify [Fulfillment][].
 * 
 * [Fulfillment]: https://shopify.dev/docs/admin-api/rest/reference/shipping-and-fulfillment/fulfillment?api[version]=2020-04
 */
interface Receipt {
    testcase: boolean | null,
    // In the example, this is a string containing an integer value, but I'm not sure that's guaranteed.
    authorization: string | null,
}

/**
 * Prices in the shop's internal currency and the customer-facing currency.
 * 
 * Used widely throughout the API.
 */
interface PriceSet {
    shop_money: Money | null,
    presentement_money: Money | null,
}

/**
 * A sum of money and a currency. Used in `PriceSet`.
 */
interface Money {
    // This appears as decimal strings or floating point numbers, depending on
    // the example.
    amount: decimal | null,
    currency_code: string | null,
}

/**
 * A key/value property attached to another object. Appears in several places.
 */
interface Property {
    name: string,
    value: string, // Well, we hope that's the only possibility.
}


/**
 * Tax information. Appears in many places.
 */
interface TaxLine {
    title: string | null,
    price: decimal | null,
    price_set?: PriceSet | null, // Present in some examples, not others.
    rate: number | null,
}

/**
 * A discount allocation to a line item in an [Order][].
 * 
 * [Order]: https://shopify.dev/docs/admin-api/rest/reference/orders/order?api[version]=2020-04
 */
interface DiscountAllocation {
    amount: decimal | null,
    discount_application_index: int64 | null,
    amount_set: PriceSet | null,
}

/**
 * Duty information. Appears in several places.
 */
interface Duty {
    id: string, // Shown as string in example, but we could treat it as int64.
    harmonized_system_code: string | null,
    country_code_of_origin: string | null,
    shop_money: Money | null,
    presentment_money: Money | null,
    tax_lines: TaxLine[] | null,
    admin_graphql_api_id: string | null,
}

/**
 * How an [Order][] was paid for.
 * 
 * [Order]: https://shopify.dev/docs/admin-api/rest/reference/orders/order?api[version]=2020-04
 */
interface PaymentDetails {
    avs_result_code: string | null,
    credit_card_bin: string | null,
    cvv_result_code: string | null,
    credit_card_number: string | null,
    credit_card_company: string | null,
}

/**
 * A Shopify [Refund][].
 * 
 * [Refund]: https://shopify.dev/docs/admin-api/rest/reference/orders/refund?api[version]=2020-04
 */
interface Refund {
    created_at: Date | null,
    duties: Duty[] | null,
    id: int64,
    note: string | null,
    order_adjustments: OrderAdjustment[] | null,
    processed_at: Date | null,
    refund_line_items: RefundLineItem[] | null,
    restock: boolean | null,
    transactions: Transaction[] | null,
    user_id: number | null,
}

/**
 * An order adjustment in a Shopify [Refund][].
 * 
 * [Refund]: https://shopify.dev/docs/admin-api/rest/reference/orders/refund?api[version]=2020-04
 */
interface OrderAdjustment {
    id: int64,
    order_id: int64,
    refund_id: int64 | null,
    amount: decimal | null,
    tax_amount: decimal | null,
    kind: string | null,
    reason: string | null,
    amount_set: PriceSet | null,
    tax_amount_set: PriceSet | null,
}

/**
 * A line-item in a Shopify [Refund][].
 * 
 * [Refund]: https://shopify.dev/docs/admin-api/rest/reference/orders/refund?api[version]=2020-04
 */
interface RefundLineItem {
    id: int64,
    line_item: LineItem | null,
    line_item_id: int64 | null,
    quantity: int64 | null,
    location_id: int64 | null,
    restock_type: string | null,
    subtotal: decimal | null,
    total_tax: decimal | null,
    subtotal_set: PriceSet | null,
    total_tax_set: PriceSet | null,
}

/**
 * A Shopify [Transaction][].
 * 
 * [Transaction]: https://shopify.dev/docs/admin-api/rest/reference/orders/transaction?api[version]=2020-04
 */
interface Transaction {
    amount: decimal | null,
    authorization: string | null,
    created_at: Date | null,
    currency: string | null,
    device_id: int64 | null,
    error_code: string | null,
    gateway: string | null,
    id: int64,
    kind: string | null,
    location_id: int64 | null,
    message: string | null,
    order_id: int64 | null,
    payment_details: string | null,
    parent_id: int64 | null,
    processed_at: Date | null,
    receipt: any | null, // "The value of this field depends on which gateway the shop is using."
    source_name: string | null,
    status: string | null,
    test: boolean | null,
    user_id: int64 | null,
    currency_exchange_adjustment: CurrencyExchangeAdjustment | null,
}

/**
 * An adjustment to a Shopify [Transaction][] due to currency exhange rates.
 * 
 * [Transaction]: https://shopify.dev/docs/admin-api/rest/reference/orders/transaction?api[version]=2020-04
 */
interface CurrencyExchangeAdjustment {
    id: int64,
    adjustment: decimal | null,
    original_amount: decimal | null,
    final_amount: decimal | null,
    currency: string | null,
}

/**
 * Shipping fees for an [Order][].
 * 
 * [Order]: https://shopify.dev/docs/admin-api/rest/reference/orders/order?api[version]=2020-04
 */
interface ShippingLine {
    code: string | null,
    price: decimal | null,
    price_set: PriceSet | null,
    discounted_price: decimal | null,
    discounted_price_set: PriceSet | null,
    source: string | null,
    title: string | null,
    tax_lines: TaxLine[] | null,
    carrier_identifier: string | null,
    requested_fulfillment_service_id: string | null,
}
