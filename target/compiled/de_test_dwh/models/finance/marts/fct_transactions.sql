


with transactions_with_rates as (
    select * from `cusma-383203`.`dwh_dev_intermediate`.`int_transactions_with_rates`
),

users_scd2 as (
    select * from `cusma-383203`.`dwh_dev_intermediate`.`int_users_scd2`
),

transactions_with_kyc as (
    select
        t.txn_id,
        t.user_id,
        u.user_kyc_key,
        t.status,
        t.source_currency,
        t.destination_currency,
        t.created_at as transaction_timestamp,
        date(t.created_at) as transaction_date,
        t.source_amount,
        t.destination_amount,
        t.source_amount_usd,
        t.destination_amount_usd,
        u.kyc_level as kyc_level_at_transaction
    from transactions_with_rates t
    left join users_scd2 u
        on t.user_id = u.user_id
        and t.created_at >= u.effective_from
        and t.created_at < u.effective_to
)

select * from transactions_with_kyc