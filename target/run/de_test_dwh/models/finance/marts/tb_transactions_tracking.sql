
  
    

    create or replace table `cusma-383203`.`dwh_dev_mart`.`tb_transactions_tracking`
      
    
    

    
    OPTIONS()
    as (
      

select
    transaction_date timeframe,
    "daily" timeframe_type,
    count(distinct txn_id) as nb_transaction,
    count(distinct case when status = 'completed' THEN txn_id END ) as nb_completed_transaction,
    count(distinct user_id) as nb_users,
    sum(destination_amount_usd) as total_volume_usd,
    sum(case when status = 'completed' then destination_amount_usd else 0 end) as completed_volume_usd,
from `cusma-383203`.`dwh_dev_mart`.`fct_transactions`
group by ALL 

UNION ALL 

select
    DATE_TRUNC(transaction_date, WEEK(MONDAY)) timeframe,
    "weekly" timeframe_type,
    count(distinct txn_id) as nb_transaction,
    count(distinct case when status = 'completed' THEN txn_id END ) as nb_completed_transaction,
    count(distinct user_id) as nb_users,
    sum(destination_amount_usd) as total_volume_usd,
    sum(case when status = 'completed' then destination_amount_usd else 0 end) as completed_volume_usd,
from `cusma-383203`.`dwh_dev_mart`.`fct_transactions`
group by ALL 

UNION ALL 

select
    DATE_TRUNC(transaction_date, month) timeframe,
    "monthly" timeframe_type,
    count(distinct txn_id) as nb_transaction,
    count(distinct case when status = 'completed' THEN txn_id END ) as nb_completed_transaction,
    count(distinct user_id) as nb_users,
    sum(destination_amount_usd) as total_volume_usd,
    sum(case when status = 'completed' then destination_amount_usd else 0 end) as completed_volume_usd,
from `cusma-383203`.`dwh_dev_mart`.`fct_transactions`
group by ALL 

UNION ALL 

select
    DATE_TRUNC(transaction_date, QUARTER) timeframe,
    "quarterly" timeframe_type,
    count(distinct txn_id) as nb_transaction,
    count(distinct case when status = 'completed' THEN txn_id END ) as nb_completed_transaction,
    count(distinct user_id) as nb_users,
    sum(destination_amount_usd) as total_volume_usd,
    sum(case when status = 'completed' then destination_amount_usd else 0 end) as completed_volume_usd,
from `cusma-383203`.`dwh_dev_mart`.`fct_transactions`
group by ALL
    );
  