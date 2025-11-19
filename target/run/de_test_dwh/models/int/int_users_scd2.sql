
  
    

    create or replace table `cusma-383203`.`dwh_dev_intermediate`.`int_users_scd2`
      
    
    

    
    OPTIONS()
    as (
      

with users as (
    select * from `cusma-383203`.`dwh_dev_staging`.`stg_users`
),

user_periods as (
    select
        user_id,
        kyc_level,
        created_at as period_start,
        coalesce(updated_at, timestamp('9999-12-31 23:59:59')) as period_end,
        updated_at as last_updated,
        case
            when updated_at is null or updated_at = created_at then true
            else false
        end as is_current_period
    from users
),

final as (
    select
        to_hex(md5(cast(coalesce(cast(user_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(period_start as string), '_dbt_utils_surrogate_key_null_') as string))) as user_kyc_key,
        user_id,
        kyc_level,
        period_start as effective_from,
        period_end as effective_to,
        last_updated,
        is_current_period as is_current,
        timestamp_diff(period_end, period_start, second) as duration_seconds
    from user_periods
)

select * 
from final
order by user_id, effective_from
    );
  