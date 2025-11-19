

  create or replace view `cusma-383203`.`dwh_dev_staging`.`stg_transactions`
  OPTIONS()
  as 

with source as (
    select * from `cusma-383203`.`raw_data`.`transaction`
),

renamed as (
    select
        cast(txn_id as int64) as txn_id,
        cast(user_id as int64) as user_id,
        cast(status as string) as status,
        cast(source_currency as string) as source_currency,
        cast(destination_currency as string) as destination_currency,
        cast(created_at as timestamp) as created_at,
        cast(source_amount as float64) as source_amount,
        cast(destination_amount as float64) as destination_amount
    from source
)

select * from renamed;

