{{ config(materialized="view", tags=['staging']) }}

select *
from {{ source("stg", "ip_intelligence") }}
