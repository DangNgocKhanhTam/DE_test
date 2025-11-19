
  
    

    create or replace table `cusma-383203`.`dwh_dev_mart`.`dim_date`
      
    
    

    
    OPTIONS()
    as (
      


with date_range as (
    select
        min(date(created_at)) as start_date,
        max(date(created_at)) as end_date
    from `cusma-383203`.`dwh_dev_staging`.`stg_transactions`
),

date_series as (
    select
        date_add(start_date, interval day_offset day) as date
    from date_range,
    unnest(generate_array(0, date_diff(end_date, start_date, day))) as day_offset
)

select
    date,
    extract(year from date) as year,
    extract(quarter from date) as quarter,
    extract(month from date) as month,
    extract(day from date) as day,
    extract(dayofweek from date) as day_of_week,
    format_date('%A', date) as day_name,
    format_date('%B', date) as month_name,
    extract(week from date) as week_of_year,
    extract(dayofyear from date) as day_of_year,
    case
        when extract(dayofweek from date) in (1, 7) then true
        else false
    end as is_weekend
from date_series
    );
  