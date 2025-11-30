# src/ingest_bronze.py
from pathlib import Path
import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW = PROJECT_ROOT / "data" / "raw"
BRONZE = PROJECT_ROOT / "data" / "bronze"
BRONZE.mkdir(parents=True, exist_ok=True)

TABLES = ["customers", "products", "orders"]

def ingest_table(table_name: str):
    src = RAW / f"{table_name}.csv"
    dst = BRONZE / f"{table_name}.parquet"
    if not src.exists():
        print(f"Skipping {table_name}: source not found at {src}")
        return
    # read with polars (fast), then write parquet (snappy)
    df = pl.read_csv(src)
    df.write_parquet(dst, compression="snappy")
    print(f"Wrote bronze: {dst}  (rows={df.height})")

def main():
    for t in TABLES:
        ingest_table(t)

if __name__ == "__main__":
    main()
