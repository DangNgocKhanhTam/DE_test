
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select effective_to
from `cusma-383203`.`dwh_dev_mart`.`dim_users`
where effective_to is null



  
  
      
    ) dbt_internal_test