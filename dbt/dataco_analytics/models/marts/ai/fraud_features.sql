{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_orders') }}
),
dim_customer as (
    select * from {{ ref('dim_customers') }}
),
dim_product as (
    select * from {{ ref('dim_products') }}
),
dim_date as (
    select * from {{ ref('dim_date') }}
),
dim_location as (
    select * from {{ ref('dim_shipping_location') }}
),
fact as (
    select * from {{ ref('fact_order_items') }}
)

select
    -- Identifiers (kept for predict.py, dropped in train.py before X)
    f.order_item_id,
    f.order_id,
    f.customer_id,

    -- Financial / operational
    f.payment_type,
    f.order_item_quantity,
    f.sales,
    f.sales_per_customer,
    f.benefit_per_order,
    f.order_profit_per_order,
    f.order_item_total,
    f.order_item_discount,
    f.order_item_discount_rate,
    f.order_item_profit_ratio,
    f.shipping_mode,

    -- Derived temporal (replaces Python-side derivation)
    extract(month from f.order_date)::int  as order_month,
    extract(day from f.order_date)::int    as order_day,
    extract(hour from f.order_date)::int   as order_hour,
    extract(dow from f.order_date)::int    as order_day_of_week,

    -- Customer
    c.customer_segment,

    -- Product
    p.product_price,
    p.category_name,
    p.department_name,

    -- Date
    d.is_weekend,

    -- Shipping location
    s.latitude,
    s.longitude,
    s.order_region,
    s.order_country,

    -- Target (for training; predict.py ignores this)
    f.order_status,
    case when f.order_status = 'SUSPECTED_FRAUD' then 1 else 0 end as target,

    -- Audit
    now() as created_at

from fact f
left join dim_customer  c on f.customer_id          = c.customer_id
left join dim_product   p on f.product_card_id      = p.product_card_id
left join dim_date      d on f.date_key             = d.date_key
left join dim_location  s on f.shipping_location_id  = s.shipping_location_id
