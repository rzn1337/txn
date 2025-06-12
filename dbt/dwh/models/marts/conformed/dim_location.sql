{{ config(materialized="table") }}

with distinct_loc as (
    select 
        customer_location_country as country_code,
        customer_location_state as state,
        customer_location_city as city,
    from {{ ref("stg_transactions") }}
    group by 1, 2, 3
)

select 
    dense_rank() over (order by country_code, state, city) as location_key,
    *
from 
    distinct_loc
where 
    country_code is not null