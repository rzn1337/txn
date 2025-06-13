{{ config(materialized="incremental", unique_key="account_id") }}

with source as (
    select 
        account_id,
        customer_id,
        account_status,
        credit_limit,
        opening_date,
        DATE '2024-01-01' as effective_from,
        DATE '9999-12-31' as effective_to
    from {{ ref("stg_accounts")}}
)

select
    account_id,
    customer_id,
    account_status,
    credit_limit,
    opening_date,
    effective_from,
    effective_to,
    true as current_flag
from source

{% if is_incremental() %}
    where effective_from > (
        select max(effective_from) from {{ this }}
    )
{% endif %}