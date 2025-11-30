with src as (
  select * from {{ ref('stg_customers') }}
)

select
  customer_id,
  name,
  signup_date,
  country
from src
group by customer_id, name, signup_date, country
