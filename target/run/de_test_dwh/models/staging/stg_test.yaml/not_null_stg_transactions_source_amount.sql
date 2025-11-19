
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_amount
from `cusma-383203`.`dwh_dev_staging`.`stg_transactions`
where source_amount is null



  
  
      
    ) dbt_internal_test