
  
  create view "ecommerce"."main"."fact_orders__dbt_tmp" as (
    with ord as (
  select * from "ecommerce"."main"."stg_orders"
),
prod as (
  select * from "ecommerce"."main"."dim_products"
)

select
  o.order_id,
  o.order_ts,
  extract(year from o.order_ts) as order_year,
  extract(month from o.order_ts) as order_month,
  o.customer_id,
  o.product_id,
  p.category,
  o.qty,
  p.price,
  (o.qty * p.price) as total_price
from ord o
left join prod p using (product_id)
  );
