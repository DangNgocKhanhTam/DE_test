{{
    config(
        materialized='table',
        partition_by={"field": "price_date"},
        cluster_by=["symbol"]
    )
}}


with rates as (
    select * from {{ ref('stg_rates') }}
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


