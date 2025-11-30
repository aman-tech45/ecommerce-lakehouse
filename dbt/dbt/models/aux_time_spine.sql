{{ config(materialized='table') }}

-- aux_time_spine: continuous day-level spine between min(order_ts) and max(order_ts)
with bounds as (
  select
    min(date_trunc('day', order_ts))::date as min_day,
    max(date_trunc('day', order_ts))::date as max_day
  from {{ ref('fact_orders') }}
),

seq as (
  select row_number() over () - 1 as value
  from (select 1 from (select 1) t cross join (select 1) u limit 10000) x
),

days as (
  select
    date_add('day', seq.value, bounds.min_day) as date_day
  from bounds
  join seq on date_add('day', seq.value, bounds.min_day) <= bounds.max_day
)

select
  date_day::date as date_day,
  extract(year from date_day) as year,
  extract(month from date_day) as month,
  extract(day from date_day) as day_of_month
from days
order by date_day
