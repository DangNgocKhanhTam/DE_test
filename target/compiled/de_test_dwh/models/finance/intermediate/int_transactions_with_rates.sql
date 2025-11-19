

with transactions as (
    select * from `cusma-383203`.`dwh_dev_staging`.`stg_transactions`
),

rates as (
    select * from `cusma-383203`.`dwh_dev_intermediate`.`int_rates_usdt`
),

latest_source_rate as (
    select
        t.txn_id,
        r.usdt_price as source_usdt_price,
        r.price_timestamp as source_rate_timestamp,
        row_number() over (
            partition by t.txn_id
            order by r.price_timestamp desc
        ) as rn
    from transactions t
    left join rates r
        on r.currency = t.source_currency
        and r.price_timestamp <= t.created_at 
),

latest_dest_rate as (
    select
        t.txn_id,
        r.usdt_price as destination_usdt_price,
        r.price_timestamp as destination_rate_timestamp,
        row_number() over (
            partition by t.txn_id
            order by r.price_timestamp desc
        ) as rn
    from transactions t
    left join rates r
        on r.currency = t.destination_currency
        and r.price_timestamp <= t.created_at 
),

transactions_with_rates as (
    select
        t.*,
        lsr.source_usdt_price,
        lsr.source_rate_timestamp,
        ldr.destination_usdt_price,
        ldr.destination_rate_timestamp
    from transactions t
    left join latest_source_rate lsr
        on lsr.txn_id = t.txn_id
        and lsr.rn = 1
    left join latest_dest_rate ldr
        on ldr.txn_id = t.txn_id
        and ldr.rn = 1
)

select
    *,
    date(created_at) as date_created,
    source_amount * coalesce(source_usdt_price, 0) as source_amount_usd,
    destination_amount * coalesce(destination_usdt_price, 0) as destination_amount_usd
from transactions_with_rates