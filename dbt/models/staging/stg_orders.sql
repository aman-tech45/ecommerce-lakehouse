with raw as (
  select * from read_parquet('../data/silver/orders.parquet')
)

select
  order_id,
  cast(order_ts as timestamp) as order_ts,
  customer_id,
  product_id,
  cast(qty as integer) as qty
from raw
