{{ config(materialized='table') }}

with stg_orders as (
    select * from {{ ref('stg_orders') }}
),

dim_shipping_location as (
    select * from {{ ref('dim_shipping_location') }}
),

stg_with_location as (
    select
        s.*,
        d.shipping_location_id
    from stg_orders s
    left join dim_shipping_location d
        on  s.order_city     = d.order_city
        and s.order_state    = d.order_state
        and s.order_country  = d.order_country
        and s.order_zipcode  = d.order_zipcode
        and s.latitude       = d.latitude
        and s.longitude      = d.longitude
        and s.order_region   = d.order_region
)

select
    order_item_id,
    order_id,
    customer_id,
    product_card_id,
    date_key,
    shipping_location_id,
    payment_type,
    delivery_status,
    order_status,
    late_delivery_risk,
    shipping_mode,
    order_date,
    shipping_date,
    days_for_shipping_real,
    days_for_shipment_scheduled,
    sales,
    sales_per_customer,
    benefit_per_order,
    order_profit_per_order,
    order_item_total,
    order_item_discount,
    order_item_discount_rate,
    order_item_profit_ratio,
    order_item_quantity
from stg_with_location
