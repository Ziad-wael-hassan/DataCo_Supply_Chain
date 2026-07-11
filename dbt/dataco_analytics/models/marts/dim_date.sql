{{ config(materialized='table') }}

with stg_orders as (
    select * from {{ ref('stg_orders') }}
),

unique_dates as (
    select distinct date_key, full_date
    from stg_orders
)

select
    date_key,
    full_date,
    extract(year from full_date) as year,
    extract(quarter from full_date) as quarter,
    extract(month from full_date) as month,
    to_char(full_date, 'Month') as month_name,
    extract(week from full_date) as week,
    extract(day from full_date) as day,
    case when extract(isodow from full_date) in (6, 7) then true else false end as is_weekend
from unique_dates
