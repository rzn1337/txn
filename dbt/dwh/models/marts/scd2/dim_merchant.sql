{{ config(materialized='incremental', unique_key='merchant_id') }}

with source as (
  select
    merchant_id,
    merchant_category_code   as category_code,
    merchant_registration_country as registration_country,
    is_high_risk_merchant    as high_risk_flag,
    merchant_reputation_score as reputation_score,
    DATE '2024-01-01' as effective_from,
    DATE '9999-12-31' as effective_to
  from {{ ref('stg_merchants') }}
)

select
  merchant_id,
  category_code,
  registration_country,
  high_risk_flag,
  reputation_score,
  effective_from,
  effective_to,
  true as current_flag
from source

{% if is_incremental() %}
  where effective_from > (
    select max(effective_from) from {{ this }}
  )
{% endif %}
