import pandas as pd
from sqlalchemy import create_engine
import json
from datetime import datetime

# --- 1. Connect to the database ---
engine = create_engine("postgresql+psycopg://postgres:postgres@localhost:5432/amman_market")

# --- 2. Extract data ---
customers = pd.read_sql("SELECT * FROM customers", engine)
orders = pd.read_sql("SELECT * FROM orders", engine)
order_items = pd.read_sql("SELECT * FROM order_items", engine)
products = pd.read_sql("SELECT * FROM products", engine)

# --- 3. Transform: Calculate total revenue per customer ---
# Merge orders -> order_items -> products
order_items_products = order_items.merge(products, on="product_id", how="left")
orders_with_revenue = orders.merge(order_items_products, on="order_id", how="left")

# Only consider completed orders for revenue
orders_with_revenue = orders_with_revenue[orders_with_revenue["status"] == "completed"]
orders_with_revenue["revenue"] = orders_with_revenue["quantity"] * orders_with_revenue["unit_price"]

customer_revenue = orders_with_revenue.groupby("customer_id")["revenue"].sum().reset_index()
customer_revenue.rename(columns={"revenue": "total_revenue"}, inplace=True)

# Merge back with customer info
customer_summary = customers.merge(customer_revenue, on="customer_id", how="left").fillna(0)

# --- 4. Detect outliers ---
mean_revenue = customer_summary["total_revenue"].mean()
std_revenue = customer_summary["total_revenue"].std()
threshold = mean_revenue + 3 * std_revenue

customer_summary["is_outlier"] = customer_summary["total_revenue"] > threshold

# --- 5. Generate quality report ---
quality_report = {
    "timestamp": datetime.now().isoformat(),
    "total_records_checked": len(customer_summary),
    "checks_passed": int((~customer_summary["is_outlier"]).sum()),
    "checks_failed": int(customer_summary["is_outlier"].sum()),
    "flagged_outliers": customer_summary[customer_summary["is_outlier"]][["customer_id", "total_revenue"]]
        .to_dict(orient="records")
}

# --- 6. Save report ---
with open("output/quality_report.json", "w") as f:
    json.dump(quality_report, f, indent=4)

print("Quality report generated at output/quality_report.json")