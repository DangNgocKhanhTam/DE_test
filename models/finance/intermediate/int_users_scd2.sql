{{
    config(
        materialized='table',
        unique_key='user_kyc_key'  
    )
}}

with users as (
    select * from {{ ref('stg_users') }}
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
        {{ dbt_utils.generate_surrogate_key(['user_id', 'period_start']) }} as user_kyc_key,
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
