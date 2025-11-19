

-- Aggregated fact: Daily transaction volume in USD
-- Answers business question: "Total transaction volume (in USD) by day"

select
    transaction_date,
    transaction_year,
    transaction_quarter,
    transaction_month,
    count(distinct txn_id) as transaction_count,
    count(distinct user_id) as unique_users,
    sum(transaction_amount_usd) as total_volume_usd,
    avg(transaction_amount_usd) as avg_transaction_amount_usd,
    min(transaction_amount_usd) as min_transaction_amount_usd,
    max(transaction_amount_usd) as max_transaction_amount_usd,
    sum(case when status = 'completed' then transaction_amount_usd else 0 end) as completed_volume_usd,
    count(case when status = 'completed' then 1 end) as completed_transaction_count
from `cusma-383203`.`dwh_dev_mart`.`fct_transactions`
group by
    transaction_date,
    transaction_year,
    transaction_quarter,
    transaction_month