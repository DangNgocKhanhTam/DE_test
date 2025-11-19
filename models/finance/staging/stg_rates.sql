{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw_data', 'rates') }}
),

renamed as (
    select
        cast(symbol as string) as symbol,
        cast(open_time as int64) as open_time,
        cast(close as float64) as close_price,
        cast(open as float64) as open_price,
        cast(high as float64) as high_price,
        cast(low as float64) as low_price,
        cast(volume as float64) as volume,
        cast(close_time as int64) as close_time,
        cast(quote_volume as float64) as quote_volume,
        cast(trades as int64) as trades,
        timestamp_millis(cast(open_time as int64)) as price_timestamp,
        timestamp_millis(cast(close_time as int64)) as close_timestamp
    from source
)

select * from renamed

