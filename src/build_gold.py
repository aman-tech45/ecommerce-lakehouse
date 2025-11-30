# src/build_gold.py
"""
Build GOLD layer:
 - read silver parquet files
 - produce dim_customers, dim_products, fact_orders
 - write parquet to data/gold/
 - register tables into a DuckDB file for easy querying

Run:
    /Users/amanjha/miniconda3/envs/dsenv/bin/python src/build_gold.py
"""

from pathlib import Path
import polars as pl
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER = PROJECT_ROOT / "data" / "silver"
GOLD = PROJECT_ROOT / "data" / "gold"
GOLD.mkdir(parents=True, exist_ok=True)

DUCKDB_PATH = PROJECT_ROOT / "data" / "ecommerce.duckdb"

def build_dim_customers():
    df = pl.read_parquet(SILVER / "customers.parquet")
    # pick only necessary columns and sort
    dim = df.select([
        pl.col("customer_id"),
        pl.col("name"),
        pl.col("signup_date").alias("signup_date"),
        pl.col("country")
    ]).unique(subset=["customer_id"]).sort("customer_id")
    # write parquet
    out = GOLD / "dim_customers.parquet"
    dim.write_parquet(out, compression="snappy")
    print(f"Wrote {out} (rows={dim.height})")
    return dim

def build_dim_products():
    df = pl.read_parquet(SILVER / "products.parquet")
    dim = df.select([
        pl.col("product_id"),
        pl.col("category"),
        pl.col("price").cast(pl.Float64)
    ]).unique(subset=["product_id"]).sort("product_id")
    out = GOLD / "dim_products.parquet"
    dim.write_parquet(out, compression="snappy")
    print(f"Wrote {out} (rows={dim.height})")
    return dim

def build_fact_orders(dim_products: pl.DataFrame):
    orders = pl.read_parquet(SILVER / "orders.parquet")

    # join product price
    # polars join by product_id
    joined = orders.join(dim_products, on="product_id", how="left")

    # ensure types
    joined = joined.with_columns([
        pl.col("qty").cast(pl.Int64),
        pl.col("price").cast(pl.Float64),
        pl.col("order_ts").cast(pl.Datetime)
    ])

    # compute total price and add partition columns
    joined = joined.with_columns([
        (pl.col("qty") * pl.col("price")).alias("total_price"),
        pl.col("order_ts").dt.year().alias("order_year"),
        pl.col("order_ts").dt.month().alias("order_month")
    ])

    # reorder columns in an analytics-friendly order
    fact = joined.select([
        "order_id",
        "order_ts",
        "order_year",
        "order_month",
        "customer_id",
        "product_id",
        "category",
        "qty",
        "price",
        "total_price"
    ])

    out = GOLD / "fact_orders.parquet"
    fact.write_parquet(out, compression="snappy")
    print(f"Wrote {out} (rows={fact.height})")
    return fact

def write_to_duckdb(dim_customers: pl.DataFrame, dim_products: pl.DataFrame, fact_orders: pl.DataFrame):
    """
    Safely write Parquet-backed tables into DuckDB.
    This version drops a VIEW first (if exists) then drops TABLE (if exists),
    registers pandas DataFrames temporarily, and creates persistent tables.
    """
    conn = duckdb.connect(database=str(DUCKDB_PATH), read_only=False)

    # convert to pandas
    pc = dim_customers.to_pandas()
    pp = dim_products.to_pandas()
    pf = fact_orders.to_pandas()

    # Register temporary names (these are in-memory)
    conn.register("tmp_dim_customers", pc)
    conn.register("tmp_dim_products", pp)
    conn.register("tmp_fact_orders", pf)

    # Safely drop any existing object (view or table) before creating table
    for name in ("dim_customers", "dim_products", "fact_orders"):
        try:
            conn.execute(f"DROP VIEW IF EXISTS {name};")
        except Exception:
            pass
        try:
            conn.execute(f"DROP TABLE IF EXISTS {name};")
        except Exception:
            pass

    # Create persistent tables from the registered temp objects
    conn.execute("CREATE TABLE dim_customers AS SELECT * FROM tmp_dim_customers;")
    conn.execute("CREATE TABLE dim_products AS SELECT * FROM tmp_dim_products;")
    conn.execute("CREATE TABLE fact_orders AS SELECT * FROM tmp_fact_orders;")

    # sanity check
    total = conn.execute("SELECT COUNT(*) AS cnt FROM fact_orders;").fetchone()
    print(f"Wrote tables to DuckDB: {DUCKDB_PATH} (fact_orders_count={total[0]})")

    # unregister temp names (clean up)
    try:
        conn.unregister("tmp_dim_customers")
        conn.unregister("tmp_dim_products")
        conn.unregister("tmp_fact_orders")
    except Exception:
        pass

    conn.close()

def main():
    dim_c = build_dim_customers()
    dim_p = build_dim_products()
    fact = build_fact_orders(dim_p)
    write_to_duckdb(dim_c, dim_p, fact)
    print("GOLD build complete.")

if __name__ == "__main__":
    main()
