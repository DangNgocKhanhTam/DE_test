
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select txn_id
from `cusma-383203`.`dwh_dev_staging`.`stg_transactions`
where txn_id is null



  
  
      
    ) dbt_internal_test