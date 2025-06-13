{{ config(materialized="incremental", unique_key="customer_id") }}

with source as (
    select 
        customer_id,
        customer_since,
        kyc_status,
        customer_segment as segment,
        income_bracket,
        employment_status,
        gender,
        extract(year from date_of_birth) as birth_year,
        DATE '2024-01-01' as effective_from,
        DATE '9999-12-31' as effective_to
    from {{ ref("stg_customers") }}
)

select 
    customer_id,
    customer_since,
    kyc_status,
    segment,
    income_bracket,
    employment_status,
    gender,
    birth_year,
    effective_from,
    effective_to,
    true as current_flag
from source

{% if is_incremental() %}
    where effective_from > (
        select max(effective_from) from {{ this }}
    )
{% endif %}