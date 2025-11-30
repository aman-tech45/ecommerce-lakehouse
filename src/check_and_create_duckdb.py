# src/check_and_create_duckdb.py
"""
Checks duckdb for gold tables. If missing, loads them from data/gold/*.parquet.
Run with:
  /Users/amanjha/miniconda3/envs/dsenv/bin/python src/check_and_create_duckdb.py
"""

from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD = PROJECT_ROOT / "data" / "gold"
DB_PATH = PROJECT_ROOT / "data" / "ecommerce.duckdb"

def ensure_table(conn, table_name, parquet_path):
    tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
    if table_name in tables:
        print(f"OK: table '{table_name}' already exists in DuckDB.")
        return
    if not parquet_path.exists():
        print(f"ERROR: parquet for {table_name} missing at {parquet_path}")
        return
    print(f"Creating table '{table_name}' from {parquet_path} ...")
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")
    print(f"Created table {table_name}")

def main():
    conn = duckdb.connect(database=str(DB_PATH), read_only=False)
    print("Connected to DuckDB:", DB_PATH)
    try:
        # list current tables
        print("Existing tables:", conn.execute("SHOW TABLES").fetchall())

        # ensure each gold table exists
        ensure_table(conn, "dim_customers", GOLD / "dim_customers.parquet")
        ensure_table(conn, "dim_products", GOLD / "dim_products.parquet")
        ensure_table(conn, "fact_orders", GOLD / "fact_orders.parquet")

        # final sanity counts
        if "fact_orders" in [r[0] for r in conn.execute("SHOW TABLES").fetchall()]:
            cnt = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
            print("fact_orders count ->", cnt)
            rev = conn.execute("SELECT order_year, SUM(total_price) FROM fact_orders GROUP BY order_year ORDER BY order_year").fetchall()
            print("revenue by year ->", rev)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
