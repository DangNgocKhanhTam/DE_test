
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select destination_amount_usd
from `cusma-383203`.`dwh_dev_mart`.`fct_transactions`
where destination_amount_usd is null



  
  
      
    ) dbt_internal_test