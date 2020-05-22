// This is a Shopify REST schema that we built by reading the docs.

// A decimal value represented as a string for accuracy.
type decimal = string;

// https://shopify.dev/docs/admin-api/rest/reference/orders/order?api[version]=2020-04
interface Order {
    app_id: number,
    billing_address: Address | null,
    browser_ip: string | null,
    buyer_accepts_marketing: string | null,
    cancel_reason: string | null,
    cancelled_at: string | null, // ISO 8601
    cart_token: string | null,
    client_details: ClientDetails | null,
    closed_at: string | null,
    created_at: string | null,
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
    id: string,
    landing_site: string | null,
    line_items: LineItem[] | null,
    location_id: number | null,
    name: string | null,
    note: string | null,
    note_attributes: Property[] | null,
    number: number | null,
    order_number: number | null,
    original_total_duties_set: PriceSet | null,
    payment_details: PaymentDetails | null, // Deprecated.
    payment_gateway_names: string[] | null,
    phone: string | null,
    presentment_currency: string | null,
    processed_at: string | null,
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
    updated_at: string | null,
    user_id: number | null,
    order_status_url: string | null,
}

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
    latitude: string | null,
    longitude: string | null,
}

interface ClientDetails {
    accepts_language: string | null,
    browser_height: number | null,
    browser_ip: string | null,
    browser_width: number | null,
    session_hash: string | null,
    user_agent: string | null,
}

// https://shopify.dev/docs/admin-api/rest/reference/customers/customer?api[version]=2020-04
interface Customer {
    accepts_marketing: boolean | null,
    accepts_marketing_updated_at: string | null,
    addresses: Address[] | null,
    admin_graphql_api_id: string | null,
    created_at: string | null,
    currency: string | null,
    default_address: Address | null,
    email: string | null,
    first_name: string | null,
    id: number | null,
    last_name: string | null,
    last_order_id: number | null,
    last_order_name: string | null,
    // metafield,
    multipass_identifier: string | null,
    note: string | null,
    orders_count: string | null, // String as integer.
    phone: string | null,
    state: string | null, // "disabled" is a valid value.
    tags: string | null,
    tax_exempt: boolean | null,
    tax_exemptions: string[] | null,
    total_spent: decimal | null,
    updated_at: string | null,
    verified_email: boolean | null,
}

interface DiscountApplication {
    type: string | null,
    description: string | null,
    value: decimal | null,
    value_type: string | null,
    allocation_method: string | null,
    target_selection: string | null,
    target_type: string | null,
}

interface DiscountCode {
    code: string | null,
    amount: decimal | null,
    type: string | null,
}

// https://shopify.dev/docs/admin-api/rest/reference/shipping-and-fulfillment/fulfillment?api[version]=2020-04
interface Fulfillment {
    created_at: string | null,
    id: number,
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

interface LineItem {
    fulfillable_quantity: number | null,
    fulfillment_service: string | null,
    fulfillment_status: string | null,
    grams: number | null,
    id: number,
    price: decimal | null,
    product_id: number | null,
    quantity: number | null,
    requires_shipping: boolean | null,
    sku: string | null,
    title: string | null,
    variant_id: number | null,
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
}

interface Receipt {
    testcase: boolean | null,
    authorization: string | null,
}

interface PriceSet {
    shop_money: Money | null,
    presentement_money: Money | null,
}

interface Money {
    amount: decimal | null,
    currency_code: string | null,
}

interface Property {
    name: string,
    value: string, // Well, we hope that's the only possibility.
}

interface TaxLine {
    title: string | null,
    price: decimal | null,
    price_set: PriceSet | null,
    rate: number | null,
}

interface DiscountAllocation {
    amount: decimal | null,
    discount_application_index: number | null,
    amount_set: PriceSet | null,
}

interface Duty {
    id: string,
    harmonized_system_code: string | null,
    country_code_of_origin: string | null,
    shop_money: Money | null,
    presentment_money: Money | null,
    tax_lines: TaxLine[] | null,
    admin_graphql_api_id: string | null,
}

interface PaymentDetails {
    avs_result_code: string | null,
    credit_card_bin: string | null,
    cvv_result_code: string | null,
    credit_card_number: string | null,
    credit_card_company: string | null,
}

// https://shopify.dev/docs/admin-api/rest/reference/orders/refund?api[version]=2020-04
interface Refund {
    created_at: string | null,
    duties: Duty[] | null,
    id: number,
    note: string | null,
    order_adjustments: OrderAdjustment[] | null,
    processed_at: string | null,
    refund_line_items: RefundLineItem[] | null,
    restock: boolean | null,
    transactions: Transaction[] | null,
    user_id: number | null,
}

interface OrderAdjustment {
    id: number,
    order_id: number,
    refund_id: number | null,
    amount: decimal | null,
    tax_amount: decimal | null,
    kind: string | null,
    reason: string | null,
    amount_set: PriceSet | null,
    tax_amount_set: PriceSet | null,
}

interface RefundLineItem {
    id: number,
    line_item: LineItem | null,
    line_item_id: number | null,
    quantity: number | null,
    location_id: number | null,
    restock_type: string | null,
    subtotal: number | null,
    total_tax: number | null,
    subtotal_set: PriceSet | null,
    total_tax_set: PriceSet | null,
}

// https://shopify.dev/donullcs/admin-api/rest/reference/orders/transaction?api[version]=2020-04
interface Transaction {
    amount: decimal | null,
    authorization: string | null,
    created_at: string | null,
    currency: string | null,
    device_id: number | null,
    error_code: string | null,
    gateway: string | null,
    id: number,
    kind: string | null,
    location_id: number | null,
    message: string | null,
    order_id: number | null,
    payment_details: string | null,
    parent_id: number | null,
    processed_at: string | null,
    receipt: any | null,
    source_name: string | null,
    status: string | null,
    test: boolean | null,
    user_id: number | null,
    currency_exchange_adjustment: CurrencyExchangeAdjustment | null,
}

interface CurrencyExchangeAdjustment {
    id: number,
    adjustment: decimal | null,
    original_amount: decimal | null,
    final_amount: decimal | null,
    currency: string | null,
}

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
