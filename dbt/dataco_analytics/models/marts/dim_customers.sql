{{ config(materialized='table') }}

with stg_orders as (
    select * from {{ ref('stg_orders') }}
)

select distinct
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_segment,
    customer_city,
    customer_state,
    customer_country,
    customer_street,
    customer_zipcode
from stg_orders
