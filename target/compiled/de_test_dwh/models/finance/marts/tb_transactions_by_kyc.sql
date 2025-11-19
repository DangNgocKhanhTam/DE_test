
      

select
    kyc_level_at_transaction,
    count(distinct txn_id) as nb_transaction,
    count(distinct case when status = 'completed' THEN txn_id END ) as nb_completed_transaction,
    count(distinct user_id) as nb_users,
    sum(destination_amount_usd) as total_volume_usd,
    sum(case when status = 'completed' then destination_amount_usd END) as completed_volume_usd
from `cusma-383203`.`dwh_dev_mart`.`fct_transactions`
where kyc_level_at_transaction is not null
group by all