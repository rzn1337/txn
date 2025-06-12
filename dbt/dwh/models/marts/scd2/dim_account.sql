{{ config(materialized="incremental", unique_key="account_id") }}

with source as (
    select 
        account_id,
        customer_id,
        account_status,
        credit_limit,
        opening_date,
        last_account_update as effective_from
    from {{ ref("stg_accounts")}}
),

history as (
  select
    *,
    lead(effective_from) over (
      partition by account_id order by effective_from
    ) as next_effective_from
  from source
)

select
    account_id,
    customer_id,
    account_status,
    credit_limit,
    opening_date,
    effective_from,
    coalesce(next_effective_from, timestamp('9999-12-31')) as effective_to,
    case when next_effective_from is null then true else false end as current_flag
from history

{% if is_incremental() %}
    where effective_from > (
        select max(effective_from) from {{ this }}
    )
{% endif %}