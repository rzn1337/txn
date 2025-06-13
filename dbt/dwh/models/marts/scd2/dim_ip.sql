{{ config(materialized='incremental', unique_key='ip_address') }}

with source as (
  select
    ip_address,
    is_proxy,
    is_tor,
    ip_risk_score      as risk_score,
    DATE '2024-01-01' as effective_from,
    DATE '9999-12-31' as effective_to
  from {{ ref('stg_ip_intel') }}
)

select
  ip_address,
  is_proxy,
  is_tor,
  risk_score,
  effective_from,
  effective_to,
  true as current_flag
from source

{% if is_incremental() %}
  where effective_from > (
    select max(effective_from) from {{ this }}
  )
{% endif %}
