{{ config(materialized='table') }}

with stg_orders as (
    select * from {{ ref('stg_orders') }}
),

unique_locations as (
    select distinct
        order_city,
        order_state,
        order_country,
        order_zipcode,
        latitude,
        longitude,
        order_region
    from stg_orders
)

select
    {{ dbt_utils.generate_surrogate_key([
        'order_city',
        'order_state',
        'order_country',
        'order_zipcode',
        'latitude',
        'longitude',
        'order_region'
    ]) }} as shipping_location_id,
    order_city,
    order_state,
    order_country,
    order_zipcode,
    latitude,
    longitude,
    order_region
from unique_locations
