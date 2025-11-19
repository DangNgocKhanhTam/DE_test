
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select user_kyc_key
from `cusma-383203`.`dwh_dev_mart`.`dim_users`
where user_kyc_key is null



  
  
      
    ) dbt_internal_test