
    
    

with child as (
    select user_id as from_field
    from `cusma-383203`.`dwh_dev_staging`.`stg_transactions`
    where user_id is not null
),

parent as (
    select user_id as to_field
    from `cusma-383203`.`dwh_dev_staging`.`stg_users`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


