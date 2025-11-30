

-- time spine at DAY granularity built from existing orders dates
with dates as (
  select
    date_trunc('day', order_ts)::date as date_day
  from "ecommerce"."main"."fact_orders"
)

select
  date_day,
  extract(year from date_day) as year,
  extract(month from date_day) as month,
  extract(day from date_day) as day_of_month
from dates
group by 1,2,3,4
order by 1