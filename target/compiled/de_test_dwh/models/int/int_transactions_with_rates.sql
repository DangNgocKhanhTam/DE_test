

with transactions as (
    select * from `cusma-383203`.`dwh_dev_staging`.`stg_transactions`
),

rates as (
    select * from `cusma-383203`.`dwh_dev_intermediate`.`int_rates_usdt`
),

latest_rates as (
    select
        currency,
        usdt_price,
        price_timestamp,
        row_number() over (
            partition by currency, timestamp_trunc(price_timestamp, hour)
            order by price_timestamp desc
        ) as rn
    from rates
),

transactions_with_rates as (
    select
        t.*,
        lr_source.usdt_price as source_usdt_price,
        lr_source.price_timestamp as source_rate_timestamp,
        lr_dest.usdt_price as destination_usdt_price,
        lr_dest.price_timestamp as destination_rate_timestamp
    from transactions t
    left join latest_rates lr_source
        on lr_source.currency = t.source_currency
       and lr_source.price_timestamp <= t.created_at
       and lr_source.rn = 1
    left join latest_rates lr_dest
        on lr_dest.currency = t.destination_currency
       and lr_dest.price_timestamp <= t.created_at
       and lr_dest.rn = 1
)

select
    *,
    date(created_at) as date_created,
    source_amount * coalesce(source_usdt_price, 0) as source_amount_usd,
    destination_amount * coalesce(destination_usdt_price, 0) as destination_amount_usd
from transactions_with_rates