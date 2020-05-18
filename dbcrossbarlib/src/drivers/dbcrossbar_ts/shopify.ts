// This is a Shopify REST schema that we built by reading the docs.

// A decimal value represented as a string for accuracy.
type Decimal = string;

// A type that we still need to look up.
//type TODO = string;

// https://shopify.dev/docs/admin-api/rest/reference/orders/order?api[version]=2020-04
interface Order {
    app_id: number,
    billing_address: Address,
    browser_ip: string,
    buyer_accepts_marketing: string,
    cancel_reason: string,
    cancelled_at: string, // ISO 8601
    cart_token: string,
    client_details: ClientDetails,
    closed_at: string,
    created_at: string,
    currency: string,
    current_total_duties_set: string,
    customer: Customer,
    customer_local: string,
    discount_applications: DiscountApplication[],
    discount_codes: DiscountCode[],
    email: string,
    financial_status: string,
    fulfillments: Fulfillment[],
    fulfillment_status: string,
    gateway: string, // Deprecated.
    id: string,
    landing_site: string,
    line_items: LineItem[],
    location_id: number,
    name: string,
    note: string,
    note_attributes: Property[],
    number: number,
    order_number: number,
    original_total_duties_set: PriceSet,
    payment_details: PaymentDetails, // Deprecated.
    payment_gateway_names: string[],
    phone: string,
    presentment_currency: string,
    processed_at: string,
    processing_method: string,
    referring_site: string,
    refunds: Refund[],
    shipping_address: Address, // Optional.
    shipping_lines: ShippingLine[],
    source_name: string,
    subtotal_price: number,
    subtotal_price_set: PriceSet,
    tags: string, // Comma-separated.
    tax_lines: TaxLine[], // May not have `price_set` here?
    taxes_included: boolean,
    test: boolean,
    token: string,
    total_discounts: Decimal,
    total_discounts_set: PriceSet,
    total_line_items_price: Decimal,
    total_line_items_price_set: PriceSet,
    total_price_set: PriceSet,
    total_tax: Decimal,
    total_tax_set: PriceSet,
    total_tip_received: Decimal,
    total_weight: number,
    updated_at: string,
    user_id: number,
    order_status_url: string,
}

interface Address {
    address1: string,
    address2: string,
    city: string,
    company: string,
    country: string,
    first_name: string,
    last_name: string,
    phone: string,
    province: string,
    zip: string,
    name: string,
    province_code: string,
    country_code: string,
    latitude: string,
    longitude: string,
}

interface ClientDetails {
    accepts_language: string,
    browser_height: number,
    browser_ip: string,
    browser_width: number,
    session_hash: string,
    user_agent: string,
}

// https://shopify.dev/docs/admin-api/rest/reference/customers/customer?api[version]=2020-04
interface Customer {
    accepts_marketing: boolean,
    accepts_marketing_updated_at: string,
    addresses: Address[],
    admin_graphql_api_id: string,
    created_at: string,
    currency: string,
    default_address: Address,
    email: string,
    first_name: string,
    id: number,
    last_name: string,
    last_order_id: number,
    last_order_name: string,
    // metafield,
    multipass_identifier: string | null,
    note: string,
    orders_count: string, // String as integer.
    phone: string,
    state: string, // "disabled" is a valid value.
    tags: string,
    tax_exempt: boolean,
    tax_exemptions: string[],
    total_spent: Decimal,
    updated_at: string,
    verified_email: boolean,
}

interface DiscountApplication {
    type: string,
    description: string,
    value: Decimal,
    value_type: string,
    allocation_method: string,
    target_selection: string,
    target_type: string,
}

interface DiscountCode {
    code: string,
    amount: Decimal,
    type: string,
}

// https://shopify.dev/docs/admin-api/rest/reference/shipping-and-fulfillment/fulfillment?api[version]=2020-04
interface Fulfillment {
    created_at: string,
    id: number,
    line_items: LineItem[],
    location_id: number,
    name: string,
    notify_customer: boolean,
    order_id: string,
    receipt: Receipt,
    service: string,
    shipment_status: string,
    status: string,
    tracking_company: string,
    tracking_numbers: string[],
    tracking_urls: string[],
    updated_at: string,
    variant_inventory_management: string,
}

interface LineItem {
    fulfillable_quantity: number,
    fulfillment_service: string,
    fulfillment_status: string,
    grams: number,
    id: number,
    price: Decimal,
    product_id: number,
    quantity: number,
    requires_shipping: boolean,
    sku: string,
    title: string,
    variant_id: number,
    variant_title: string,
    vendor: string,
    name: string,
    gift_card: boolean,
    price_set: PriceSet,
    properties: Property[],
    taxable: boolean,
    tax_lines: TaxLine[],
    total_discount: Decimal,
    total_discount_set: PriceSet,
    discount_allocations: DiscountAllocation[],
    duties: Duty[],
    tip_payment_gateway?: string,
    tip_payment_method?: string,
}

interface Receipt {
    testcase: boolean,
    authorization: string,
}

interface PriceSet {
    shop_money: Money,
    presentement_money: Money,
}

interface Money {
    amount: Decimal,
    currency_code: string,
}

interface Property {
    name: string,
    value: string, // Well, we hope that's the only possibility.
}

interface TaxLine {
    title: string,
    price: string,
    price_set: PriceSet,
    rate: number,
}

interface DiscountAllocation {
    amount: Decimal,
    discount_application_index: number,
    amount_set: PriceSet,
}

interface Duty {
    id: string,
    harmonized_system_code: string,
    country_code_of_origin: string,
    shop_money: Money,
    presentment_money: Money,
    tax_lines: TaxLine[],
    admin_graphql_api_id: string,
}

interface PaymentDetails {
    avs_result_code: string,
    credit_card_bin: string,
    cvv_result_code: string,
    credit_card_number: string,
    credit_card_company: string,
}

// https://shopify.dev/docs/admin-api/rest/reference/orders/refund?api[version]=2020-04
interface Refund {
    created_at: string,
    duties: Duty[],
    id: number,
    note: string,
    order_adjustments: OrderAdjustment[],
    processed_at: string,
    refund_line_items: RefundLineItem[],
    restock: boolean,
    transactions: Transaction[],
    user_id: number,
}

interface OrderAdjustment {
    id: number,
    order_id: number,
    refund_id: number,
    amount: Decimal,
    tax_amount: Decimal,
    kind: string,
    reason: string,
    amount_set: PriceSet,
    tax_amount_set: PriceSet,
}

interface RefundLineItem {
    id: number,
    line_item: LineItem,
    line_item_id: number,
    quantity: number,
    location_id: number,
    restock_type: string,
    subtotal: number,
    total_tax: number,
    subtotal_set: PriceSet,
    total_tax_set: PriceSet,
}

// https://shopify.dev/donullcs/admin-api/rest/reference/orders/transaction?api[version]=2020-04
interface Transaction {
    amount: Decimal,
    authorization: string,
    created_at: string,
    currency: string,
    device_id: number,
    error_code: string,
    gateway: string,
    id: number,
    kind: string,
    location_id: number,
    message: string,
    order_id: number,
    payment_details: string,
    parent_id: number,
    processed_at: string,
    receipt: any,
    source_name: string,
    status: string,
    test: boolean,
    user_id: number,
    currency_exchange_adjustment: CurrencyExchangeAdjustment,
}

interface CurrencyExchangeAdjustment {
    id: number,
    adjustment: Decimal,
    original_amount: Decimal,
    final_amount: Decimal,
    currency: string,
}

interface ShippingLine {
    code: string,
    price: Decimal,
    price_set: PriceSet,
    discounted_price: Decimal,
    discounted_price_set: PriceSet,
    source: string,
    title: string,
    tax_lines: TaxLine[],
    carrier_identifier: string,
    requested_fulfillment_service_id: string,
}
