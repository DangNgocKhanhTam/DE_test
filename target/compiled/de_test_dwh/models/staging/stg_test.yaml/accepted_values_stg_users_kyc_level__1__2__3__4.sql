
    
    

with all_values as (

    select
        kyc_level as value_field,
        count(*) as n_records

    from `cusma-383203`.`dwh_dev_staging`.`stg_users`
    group by kyc_level

)

select *
from all_values
where value_field not in (
    '1','2','3','4'
)


