


with transactions as (
    select distinct
        source_currency as currency
    from `cusma-383203`.`dwh_dev_staging`.`stg_transactions`
    
    union distinct
    
    select distinct
        destination_currency as currency
    from `cusma-383203`.`dwh_dev_staging`.`stg_transactions`
)

select
    currency,
    currency as currency_code,
    case
        when currency = 'USDT' then 'Tether'
        when currency = 'BTC' then 'Bitcoin'
        when currency = 'ETH' then 'Ethereum'
        when currency = 'BNB' then 'Binance Coin'
        when currency = 'ADA' then 'Cardano'
        when currency = 'SOL' then 'Solana'
        when currency = 'DOGE' then 'Dogecoin'
        when currency = 'SHIB' then 'Shiba Inu'
        when currency = 'UNI' then 'Uniswap'
        when currency = 'NGN' then 'Nigerian Naira'
        else 'Unknown'
    end as currency_name
from transactions
where currency is not null
order by currency