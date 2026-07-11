{{ config(materialized='table') }}

with stg_orders as (
    select * from {{ ref('stg_orders') }}
)

select distinct
    product_card_id,
    product_name,
    product_price,
    product_status,
    category_id,
    category_name,
    department_id,
    department_name
from stg_orders
