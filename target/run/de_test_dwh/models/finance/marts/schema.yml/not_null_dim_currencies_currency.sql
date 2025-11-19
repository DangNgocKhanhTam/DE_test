
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select currency
from `cusma-383203`.`dwh_dev_mart`.`dim_currencies`
where currency is null



  
  
      
    ) dbt_internal_test