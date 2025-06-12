{{ config(materialized="table") }}

select 
    row_number() over (order by channel) as channel_key,
    channel as channel_name
from (
    select distinct(channel) 
    from {{ ref("stg_transactions") }}
)