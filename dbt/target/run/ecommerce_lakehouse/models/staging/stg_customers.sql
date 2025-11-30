
  
  create view "ecommerce"."main"."stg_customers__dbt_tmp" as (
    with raw as (
  select * from read_parquet('../data/silver/customers.parquet')
)

select
  customer_id,
  name,
  cast(signup_date as date) as signup_date,
  country
from raw
  );
