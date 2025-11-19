
    
    

with dbt_test__target as (

  select txn_id as unique_field
  from `cusma-383203`.`dwh_dev_mart`.`fct_transactions`
  where txn_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


