{{ config(materialized='view') }}

with raw_orders as (
    select * from {{ source('raw', 'orders_raw') }}
)

select
    -- Identifiers
    cast("Order Item Id" as int) as order_item_id,
    cast("Order Id" as int) as order_id,
    cast("Customer Id" as int) as customer_id,
    cast("Product Card Id" as int) as product_card_id,
    cast(to_char(cast("order date (DateOrders)" as timestamp), 'YYYYMMDD') as int) as date_key,

    -- Date conversions
    cast("order date (DateOrders)" as timestamp) as order_date,
    cast("order date (DateOrders)" as date) as full_date,
    cast("shipping date (DateOrders)" as timestamp) as shipping_date,

    -- Customer attributes (cleaned)
    trim(cast("Customer Fname" as varchar)) as customer_first_name,
    trim(coalesce(cast("Customer Lname" as varchar), 'Unknown')) as customer_last_name,
    trim(cast("Customer Segment" as varchar)) as customer_segment,
    trim(cast("Customer City" as varchar)) as customer_city,
    trim(cast("Customer State" as varchar)) as customer_state,
    trim(cast("Customer Country" as varchar)) as customer_country,
    trim(cast("Customer Street" as varchar)) as customer_street,
    trim(coalesce("Customer Zipcode"::text, 'Unknown')) as customer_zipcode,

    -- Product attributes (cleaned)
    trim(cast("Product Name" as varchar)) as product_name,
    cast("Product Price" as float) as product_price,
    cast("Product Status" as int) as product_status,
    cast("Category Id" as int) as category_id,
    trim(cast("Category Name" as varchar)) as category_name,
    cast("Department Id" as int) as department_id,
    trim(cast("Department Name" as varchar)) as department_name,

    -- Shipping / Order attributes (cleaned)
    trim(cast("Type" as varchar)) as payment_type,
    trim(cast("Delivery Status" as varchar)) as delivery_status,
    trim(cast("Order Status" as varchar)) as order_status,
    cast("Late_delivery_risk" as int) as late_delivery_risk,
    cast("Days for shipping (real)" as float) as days_for_shipping_real,
    cast("Days for shipment (scheduled)" as float) as days_for_shipment_scheduled,
    trim(cast("Shipping Mode" as varchar)) as shipping_mode,

    -- Financial attributes
    cast("Sales" as float) as sales,
    cast("Sales per customer" as float) as sales_per_customer,
    cast("Benefit per order" as float) as benefit_per_order,
    cast("Order Profit Per Order" as float) as order_profit_per_order,
    cast("Order Item Total" as float) as order_item_total,
    cast("Order Item Discount" as float) as order_item_discount,
    cast("Order Item Discount Rate" as float) as order_item_discount_rate,
    cast("Order Item Profit Ratio" as float) as order_item_profit_ratio,
    cast("Order Item Quantity" as int) as order_item_quantity,

    -- Shipping location attributes (cleaned)
    trim(cast("Order City" as varchar)) as order_city,
    trim(cast("Order State" as varchar)) as order_state,
    trim(cast("Order Country" as varchar)) as order_country,
    trim(coalesce("Order Zipcode"::text, 'Unknown')) as order_zipcode,
    cast("Latitude" as float) as latitude,
    cast("Longitude" as float) as longitude,
    trim(cast("Order Region" as varchar)) as order_region

from raw_orders
