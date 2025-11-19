
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select updated_at
from `cusma-383203`.`dwh_dev_staging`.`stg_users`
where updated_at is null



  
  
      
    ) dbt_internal_test