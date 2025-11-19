

with source as (
    select * from `cusma-383203`.`raw_data`.`users`
),

renamed as (
    select
        cast(user_id as int64) as user_id,
        cast(kyc_level as int64) as kyc_level,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
)

select * from renamed