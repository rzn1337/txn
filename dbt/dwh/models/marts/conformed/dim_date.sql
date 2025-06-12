{{ config(materialized="table") }}

with calendar as (
    select date_key 
    from unnest(generate_date_array('2000-01-01', '2030-12-31')) as date_key
)

select 
    date_key,
    extract(day from date_key) as day,
    extract(week  from date_key) as week,
    extract(month from date_key) as month,
    extract(quarter from date_key) as quarter,
    extract(year from date_key) as year,
    extract(dayofweek from date_key) as day_of_week,
    case when extract(dayofweek from date_key) in (1, 7) then true else false end as is_weekend
from calendar

