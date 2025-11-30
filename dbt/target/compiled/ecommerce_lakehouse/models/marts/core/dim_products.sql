with src as (
  select * from "ecommerce"."main"."stg_products"
)

select
  product_id,
  category,
  price
from src
group by product_id, category, price