


with rates as (
    select * from `cusma-383203`.`dwh_dev_staging`.`stg_rates`
),

parsed_rates as (
    select
        symbol,
        regexp_extract(symbol, r'^(.+)USDT$') as currency,
        close_price as usdt_price,
        price_timestamp,
        close_timestamp, 
        DATE(price_timestamp) as price_date
    from rates
    where symbol like '%USDT'
)
    select
        currency,
        usdt_price,
        price_timestamp,
        close_timestamp,
        symbol,
        price_date

    from parsed_rates
    where currency is not null