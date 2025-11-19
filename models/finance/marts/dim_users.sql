{{
    config(
        materialized='table'
    )
}}


select * from {{ ref('int_users_scd2') }}
order by user_id, effective_from

