
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select created_at
from `cusma-383203`.`dwh_dev_staging`.`stg_transactions`
where created_at is null



  
  
      
    ) dbt_internal_test