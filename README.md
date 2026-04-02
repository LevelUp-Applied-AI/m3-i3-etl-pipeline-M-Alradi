[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

This ETL pipeline extracts order, customer, product, and order item data from a PostgreSQL database, transforms it into a customer-level summary, performs data quality checks, and loads the results back into the database and as a CSV file.  
It provides analytics such as total revenue, average order value, and top product category per customer.

## Setup

1. Start PostgreSQL container:
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine
   ```
2. Load schema and data:
   ```bash
   psql -h localhost -U postgres -d amman_market -f schema.sql
   psql -h localhost -U postgres -d amman_market -f seed_data.sql
   ```
   or use 

   ```bash
   docker exec -i postgres-m3-int psql -U postgres -d amman_market < schema.sql
   docker exec -i postgres-m3-int psql -U postgres -d amman_market < seed_data.sql  
   ```
3. Create a virtual environment: `python -m venv .venv`
4. Install dependencies: `pip install -r requirements.txt`

## How to Run

```bash
python etl_pipeline.py
```

## Output

1. The pipeline produces:
- customer_analytics table in PostgreSQL with columns:
   - customer_id
   - customer_name
   - total_orders — count of distinct orders
   - total_revenue — sum of all order line totals
   - avg_order_value — average revenue per order
   - top_category — most purchased product category

2. output/customer_analytics.csv — CSV export of the same customer summary.

## Quality Checks

- The following validations are performed on the transformed data:

   - No nulls in customer_id or customer_name — ensures each record is identifiable.
   - total_orders > 0 — confirms that only customers with actual orders are included.
   - total_revenue > 0 — ensures meaningful revenue values.
   - No duplicate customer_id values — maintains one row per customer.

If any critical check fails, the pipeline raises an error to prevent loading invalid data.
---

## Tier 2 — Incremental ETL with Change Detection

**Full vs Incremental ETL Comparison**

**Full Load:**

- Rows processed: 100
- Execution time: 0.38 seconds
- Description: Processes all historical orders. Useful for the first run or when a complete refresh is needed. Takes longer as data grows.

**Incremental Load:**

- Rows processed: 0 (no new orders)
- Execution time: ~0.01 seconds
- Description: Only processes orders added since the last successful ETL run. Very fast, reduces database load and memory usage. Risk: only new data is processed; historical updates are ignored.

**Observations / Tradeoffs:**

- Full runs are reliable for complete historical aggregation but slower.
- Incremental runs are efficient for ongoing ETL but require accurate metadata tracking (`etl_metadata`) to avoid missing new orders.
- Incremental loading significantly reduces execution time when there are few or no new records.


## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.
