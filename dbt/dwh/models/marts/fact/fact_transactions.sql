{{ config(materialized='incremental', unique_key='transaction_id') }}

with tx as (
    select
        transaction_id,
        transaction_date as date_key,
        cast(transaction_timestamp as time) as time_key,
        customer_id,
        account_id,
        merchant_id,
        channel as channel_name,
        struct(
            customer_location_country,
            customer_location_state,
            customer_location_city,
            transaction_location_lat,
            transaction_location_lon) as loc,
        device_id,
        customer_ip_address  as ip_address,
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
        dc.customer_key,
        da.account_key,
        dm.merchant_key,
        dch.channel_key,
        dl.location_key,
        dd.device_key,
        dip.ip_key,
        dpm.payment_method_key,
    from tx t
        left join {{ ref('dim_date') }}           d_date using(date_key)
        left join {{ ref('dim_time') }}           d_time using(time_key)
        left join {{ ref('dim_customer') }}       dc using(customer_id)
        left join {{ ref('dim_account') }}        da using(account_id)
        left join {{ ref('dim_merchant') }}       dm using(merchant_id)
        left join {{ ref('dim_channel') }}        dch on dch.channel_name = t.channel_name
        left join {{ ref('dim_location') }}       dl on dl.country_code = t.loc.customer_location_country and dl.state = t.loc.customer_location_state and dl.city  = t.loc.customer_location_city
        left join {{ ref('dim_device') }}         dd on dd.device_id = t.device_id
        left join {{ ref('dim_ip') }}             dip on dip.ip_address = t.ip_address
        left join {{ ref('dim_payment_method') }} dpm on dpm.payment_method = t.payment_method
)

select
    transaction_id,
    date_key,
    time_key,
    customer_key,
    account_key,
    merchant_key,
    channel_key,
    location_key,
    device_key,
    ip_key,
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
    coalesce(f.flagged_timestamp, current_timestamp()) as ingestion_ts,
    current_timestamp()          as last_updated_ts
from enrich

{% if is_incremental() %}
    where ingestion_ts > (
        select max(ingestion_ts) from {{ this }}
    )
{% endif %}