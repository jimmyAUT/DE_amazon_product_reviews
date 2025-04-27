{{ config(
    materialized='table' 
) }}

with dates as (
  select
    date as date_day
  from
    UNNEST(GENERATE_DATE_ARRAY(DATE('1995-01-01'), DATE('2026-12-31'), INTERVAL 1 DAY)) as date
)

select
  date_day,
  extract(year from date_day) as year,
  extract(month from date_day) as month,
  extract(day from date_day) as day,
  extract(quarter from date_day) as quarter,
  format_date('%A', date_day) as day_name,
  format_date('%B', date_day) as month_name,
  extract(week from date_day) as week_of_year,
  extract(dayofweek from date_day) as day_of_week,
  case when extract(dayofweek from date_day) in (1,7) then true else false end as is_weekend
from dates
