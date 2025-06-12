{{ config(materialized="view", tags=["staging"]) }}

select *
from {{ source("stg", "merchant_profiles") }}