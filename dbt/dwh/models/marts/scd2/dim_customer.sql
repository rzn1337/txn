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
        last_profile_update as effective_from
    from {{ ref("stg_customers") }}
),

history as (
    select 
        *,
        lead(effective_from) over (partition by customer_id order by effective_from) as next_effective_from
    from source 
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
    coalesce(next_effective_from, timestamp('9999-12-31')) as effective_to,
    case when next_effective_from is null then true else false end as current_flag
from history

{% if is_incremental() %}
    where effective_from > (
        select max(effective_from) from {{ this }}
    )
{% endif %}