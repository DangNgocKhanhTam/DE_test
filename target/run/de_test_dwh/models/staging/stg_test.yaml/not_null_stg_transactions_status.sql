
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select status
from `cusma-383203`.`dwh_dev_staging`.`stg_transactions`
where status is null



  
  
      
    ) dbt_internal_test