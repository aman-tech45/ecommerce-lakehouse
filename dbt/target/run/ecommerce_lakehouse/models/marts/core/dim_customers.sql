
  
  create view "ecommerce"."main"."dim_customers__dbt_tmp" as (
    with src as (
  select * from "ecommerce"."main"."stg_customers"
)

select
  customer_id,
  name,
  signup_date,
  country
from src
group by customer_id, name, signup_date, country
  );
