with raw as (
  select * from read_parquet('../data/silver/products.parquet')
)

select
  product_id,
  category,
  cast(price as double) as price
from raw
