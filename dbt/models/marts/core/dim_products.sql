with src as (
  select * from {{ ref('stg_products') }}
)

select
  product_id,
  category,
  price
from src
group by product_id, category, price
