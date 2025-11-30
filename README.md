# 🛒 Ecommerce Lakehouse (DuckDB + dbt + Python ETL)

A complete end-to-end **modern data engineering project** built using:

- **Python** (data generation, ingestion, transformations)
- **DuckDB** (OLAP engine + analytical storage)
- **dbt Core** (staging, marts, metrics, documentation)
- **Multi-layer Lakehouse Architecture** (Raw → Bronze → Silver → Gold)

This project simulates a real-world ecommerce analytics pipeline and is designed to showcase:
**data modeling, ETL orchestration, dbt transformations, testing, semantic modeling, and documentation.**

---

## 📂 Project Architecture

## 🧱 Lakehouse Architecture Diagram

```mermaid
flowchart TD

A[Raw Layer - CSV] --> B[Bronze Layer - Parquet]
B --> C[Silver Layer - Cleaned Parquet]
C --> D[Gold Layer - Dim/Fact Tables]
D --> E[dbt Models - STG/Marts]
E --> F[Metrics + Semantic Layer]
F --> G[Docs / Lineage Graph]

subgraph Python ETL
A
B
C
D
end

subgraph dbt Pipeline
E
F
G
end

It will render perfectly on GitHub.

---

# 🔥 PNG Image Version (Optional)

If you want, I’ll generate a **high-quality PNG architecture image**  
that you can upload to your repo.

Just say:

👉 **“Generate PNG”**

---

# 📌 Before moving ahead — CONFIRM  
Should I:

✅ Insert the Mermaid diagram **automatically into your README**,  
**OR**  
📌 Just give you instructions where to place it?

Reply with:

- **“Auto-insert”**  
or  
- **“I’ll paste manually”**

---

When you're ready → say **“next”** to continue to Step 2.
