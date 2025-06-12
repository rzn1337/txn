{{ config(materialized='view', tags=['staging']) }}

select *
from {{ source("stg", "customer_accounts") }}
