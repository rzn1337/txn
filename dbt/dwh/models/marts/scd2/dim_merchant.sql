{{ config(materialized='incremental', unique_key='merchant_id') }}

with source as (
  select
    merchant_id,
    merchant_category_code   as category_code,
    merchant_registration_country as registration_country,
    is_high_risk_merchant    as high_risk_flag,
    merchant_reputation_score as reputation_score,
    last_merchant_update     as effective_from
  from {{ ref('stg_merchants') }}
),

history as (
  select
    *,
    lead(effective_from) over (
      partition by merchant_id order by effective_from
    ) as next_effective_from
  from source
)

select
  merchant_id,
  category_code,
  registration_country,
  high_risk_flag,
  reputation_score,
  effective_from,
  coalesce(next_effective_from, timestamp('9999-12-31')) as effective_to,
  case when next_effective_from is null then true else false end as current_flag
from history

{% if is_incremental() %}
  where effective_from > (
    select max(effective_from) from {{ this }}
  )
{% endif %}
