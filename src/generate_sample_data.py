import polars as pl
from datetime import datetime, timedelta
import random
from pathlib import Path

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)


def gen_customers(n=500):
    df = pl.DataFrame({
        "customer_id": [f"C{1000 + i}" for i in range(n)],
        "name": [f"user_{i}" for i in range(n)],
        "signup_date": [
            (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1000))).date()
            for _ in range(n)
        ],
        "country": [random.choice(["IN", "US", "UK", "AU", "CA"]) for _ in range(n)]
    })
    df.write_csv(RAW / "customers.csv")


def gen_products(n=200):
    df = pl.DataFrame({
        "product_id": [f"P{2000 + i}" for i in range(n)],
        "category": [random.choice(["electronics", "apparel", "home", "beauty"]) for _ in range(n)],
        "price": [round(random.uniform(100, 5000), 2) for _ in range(n)]
    })
    df.write_csv(RAW / "products.csv")


def gen_orders(n=2000):
    cust_ids = [f"C{1000 + i}" for i in range(500)]
    prod_ids = [f"P{2000 + i}" for i in range(200)]

    rows = []
    for i in range(n):
        rows.append({
            "order_id": f"O{300000 + i}",
            "customer_id": random.choice(cust_ids),
            "product_id": random.choice(prod_ids),
            "qty": random.randint(1, 5),
            "order_ts": (
                    datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))
            ).isoformat()
        })
    pl.DataFrame(rows).write_csv(RAW / "orders.csv")


if __name__ == "__main__":
    gen_customers()
    gen_products()
    gen_orders()
    print(f"Sample data generated at: {RAW}")
