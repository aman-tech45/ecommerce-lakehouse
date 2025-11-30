# src/transform_silver.py

import polars as pl
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE = PROJECT_ROOT / "data" / "bronze"
SILVER = PROJECT_ROOT / "data" / "silver"
SILVER.mkdir(parents=True, exist_ok=True)

def transform_customers():
    df = pl.read_parquet(BRONZE / "customers.parquet")
    df = df.unique(subset=["customer_id"])
    df.write_parquet(SILVER / "customers.parquet")
    print("silver/customers.parquet written")

def transform_products():
    df = pl.read_parquet(BRONZE / "products.parquet")
    df = df.with_columns([
        pl.col("price").cast(pl.Float64)
    ]).unique(subset=["product_id"])
    df.write_parquet(SILVER / "products.parquet")
    print("silver/products.parquet written")

def transform_orders():
    df = pl.read_parquet(BRONZE / "orders.parquet")

    df = df.with_columns([
        pl.col("order_ts").str.strptime(pl.Datetime, strict=False),
        pl.col("qty").cast(pl.Int64)
    ]).unique(subset=["order_id"])

    df.write_parquet(SILVER / "orders.parquet")
    print("silver/orders.parquet written")

def main():
    transform_customers()
    transform_products()
    transform_orders()

if __name__ == "__main__":
    main()
