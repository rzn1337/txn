{{ config(materialized="table") }}

with t as (
  select
    time_add(time '00:00:00', interval seq minute) as time_key
  from
    unnest(generate_array(0, 24 * 60 - 1)) as seq
)

select 
    time_key,
    extract(hour from time_key) as hour,
    extract(minute from time_key) as minute,
    cast(floor(extract(minute from time_key) / 15) as int) as bucket_15min
from t