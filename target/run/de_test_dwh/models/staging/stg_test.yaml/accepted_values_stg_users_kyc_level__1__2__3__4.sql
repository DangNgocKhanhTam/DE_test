
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test