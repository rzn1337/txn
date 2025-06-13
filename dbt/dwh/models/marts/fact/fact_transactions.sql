{{ config(materialized='incremental', unique_key='transaction_id') }}

with tx as (
    select
        transaction_id,
        transaction_date,
        time_trunc(cast(transaction_timestamp as time), minute) as t_key,
        customer_id as cid,
        account_id as aid,
        merchant_id as mid,
        channel as channel_name,
        struct(
            customer_location_country,
            customer_location_state,
            customer_location_city) as loc,
        device_id,
        customer_ip_address,
        payment_method,
        amount,
        is_recurring
    from {{ ref("stg_transactions") }}
),

enrich as (
    select
        t.*,
        d_date.date_key,
        d_time.time_key,
        dc.customer_id,
        da.account_id,
        dm.merchant_id,
        dch.channel_key,
        dl.location_key,
        dd.device_key,
        -- dip.ip_address,
        dpm.payment_method_key,
    from tx t
        left join {{ ref('dim_date') }}           d_date on t.transaction_date = d_date.date_key
        left join {{ ref('dim_time') }}           d_time on d_time.time_key = t.t_key
        left join {{ ref('dim_customer') }}       dc on dc.customer_id = t.cid
        left join {{ ref('dim_account') }}        da on da.account_id = t.aid
        left join {{ ref('dim_merchant') }}       dm on dm.merchant_id = t.mid
        left join {{ ref('dim_channel') }}        dch on dch.channel_name = t.channel_name
        left join {{ ref('dim_location') }}       dl on dl.country_code = t.loc.customer_location_country and dl.state = t.loc.customer_location_state and dl.city  = t.loc.customer_location_city
        left join {{ ref('dim_device') }}         dd on dd.device_id = t.device_id
        -- left join {{ ref('dim_ip') }}             dip on dip.ip_address = t.customer_ip_address
        left join {{ ref('dim_payment_method') }} dpm on dpm.payment_method = t.payment_method
)

select
    transaction_id,
    date_key,
    time_key,
    customer_id,
    account_id,
    merchant_id,
    channel_key,
    location_key,
    device_key,
    -- ip_address,
    payment_method_key,
    amount,
    is_recurring,
    -- fraud_flag,
    -- fraud_type_key,
    null           as fraud_score,
    null           as anomaly_score,
    null           as z_score_amount,
    null           as rolling_30d_spend,
    null           as velocity_1h,
    null           as device_usage_count_30d,
    null           as distinct_merchants_30d,
    coalesce(null, current_timestamp()) as ingestion_ts,
from enrich

{% if is_incremental() %}
    where ingestion_ts > (
        select max(ingestion_ts) from {{ this }}
    )
{% endif %}