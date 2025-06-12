{{ config(materialized='incremental', unique_key='ip_address') }}

with source as (
  select
    ip_address,
    is_proxy,
    is_tor,
    ip_risk_score      as risk_score,
    last_updated       as effective_from
  from {{ ref('stg_ip_intel') }}
),

history as (
  select
    *,
    lead(effective_from) over (
      partition by ip_address order by effective_from
    ) as next_effective_from
  from source
)

select
  ip_address,
  is_proxy,
  is_tor,
  risk_score,
  effective_from,
  coalesce(next_effective_from, timestamp('9999-12-31')) as effective_to,
  case when next_effective_from is null then true else false end as current_flag
from history

{% if is_incremental() %}
  where effective_from > (
    select max(effective_from) from {{ this }}
  )
{% endif %}
