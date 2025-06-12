{{ config(materialized='table') }}

select
  row_number() over (order by device_id) as device_key,
  device_id,
  user_agent
from (
  select
    device_id,
    user_agent,
  from {{ ref('stg_transactions') }}
  where device_id is not null
)
