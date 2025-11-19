
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select kyc_level
from `cusma-383203`.`dwh_dev_mart`.`dim_users`
where kyc_level is null



  
  
      
    ) dbt_internal_test