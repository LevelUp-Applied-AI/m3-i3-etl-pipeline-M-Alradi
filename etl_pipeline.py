"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    tables = ["customers", "products", "orders", "order_items"]
    with engine.connect() as conn:
        return {
            table: pd.read_sql(f"SELECT * FROM {table}", conn)
            for table in tables
    } 

def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        DataFrame: customer-level summary with columns:
            customer_id, customer_name, city, total_orders,
            total_revenue, avg_order_value, top_category
    """
    data_dict = data_dict.copy()
    
    customers = data_dict["customers"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]
    products = data_dict["products"]

    # Filter cancelled orders
    orders_filtered = orders[orders["status"] != "cancelled"]

    # Filter suspicious quantities (quantity <= 100)
    order_items_filtered = order_items[order_items["quantity"] <= 100]

    # Join tables
    df = (
        orders_filtered
        .merge(customers, on="customer_id", how="inner")
        .merge(order_items_filtered, on="order_id", how="inner")
        .merge(products, on="product_id", how="inner")
    )

    # Compute line_total
    df["line_total"] = df["quantity"] * df["unit_price"]

    # Customer-level aggregation
    # total_orders (distinct orders)
    orders_per_customer = (
        df.groupby("customer_id")["order_id"]
        .nunique()
        .reset_index(name="total_orders")
    )

    # total_revenue
    revenue_per_customer = (
        df.groupby("customer_id")["line_total"]
        .sum()
        .reset_index(name="total_revenue")
    )

    # merge order count + revenue
    customer_summary = (
        orders_per_customer
        .merge(revenue_per_customer, on="customer_id")
    )

    # avg_order_value
    customer_summary["avg_order_value"] = (
        customer_summary["total_revenue"] /
        customer_summary["total_orders"]
    )

    # Top category per customer

    category_revenue = (
        df.groupby(["customer_id", "category"])["line_total"]
        .sum()
        .reset_index()
    )

    # get category with max revenue per customer
    idx = category_revenue.groupby("customer_id")["line_total"].idxmax()

    top_category = (
        category_revenue.loc[idx]
        .rename(columns={"category": "top_category"})
        [["customer_id", "top_category"]]
    )

    #  Final merge
    customer_summary = (
        customer_summary
        .merge(customers[["customer_id", "customer_name"]],
               on="customer_id",
               how="left")
        .merge(top_category, on="customer_id", how="left")
    )

    # reorder columns
    customer_summary = customer_summary[
        [
            "customer_id",
            "customer_name",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "top_category"
        ]
    ]

    return customer_summary


def validate(df):
    """Run data quality checks on the transformed DataFrame.

    Checks:
    - No nulls in customer_id or customer_name
    - total_revenue > 0 for all customers
    - No duplicate customer_ids
    - total_orders > 0 for all customers

    Args:
        df: transformed customer summary DataFrame

    Returns:
        dict: {check_name: bool} for each check

    Raises:
        ValueError: if any critical check fails
    """
    results = {}

    # No nulls in customer_id or customer_name
    null_check = (
        df["customer_id"].isnull().any() or
        df["customer_name"].isnull().any()
    )
    results["no_null_customer_fields"] = not null_check
    print(f"No nulls in customer_id/customer_name: {'PASS' if not null_check else 'FAIL'}")

    # total_revenue > 0
    revenue_check = (df["total_revenue"] > 0).all()
    results["positive_total_revenue"] = revenue_check
    print(f"All customers have total_revenue > 0: {'PASS' if revenue_check else 'FAIL'}")

    # No duplicate customer_id
    duplicate_check = not df["customer_id"].duplicated().any()
    results["no_duplicate_customer_id"] = duplicate_check
    print(f"No duplicate customer_id values: {'PASS' if duplicate_check else 'FAIL'}")

    # total_orders > 0
    orders_check = (df["total_orders"] > 0).all()
    results["positive_total_orders"] = orders_check
    print(f"All customers have total_orders > 0: {'PASS' if orders_check else 'FAIL'}")

    # Critical checks
    
    critical_checks = [
        results["no_null_customer_fields"],
        results["no_duplicate_customer_id"]
    ]

    if not all(critical_checks):
        raise ValueError("Critical data validation failed.")

    return results


def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: path for CSV output
    """
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # Write to PostgreSQL
    df.to_sql(
        "customer_analytics",
        con=engine,
        if_exists="replace",
        index=False
    )

    # Write to CSV
    df.to_csv(csv_path, index=False)

    # Print row count
    row_count = len(df)
    print(f"Loaded {row_count} rows into customer_analytics table and saved CSV to '{csv_path}'.")

    return row_count


def main():
    """Orchestrate the ETL pipeline: extract -> transform -> validate -> load."""

    engine = create_engine("postgresql+psycopg://postgres:postgres@localhost:5432/amman_market")
    print("Connected to PostgreSQL database.")
    data_dict = extract(engine) 
    data_dict = transform(data_dict)
    validate(data_dict)
    load(data_dict, engine, "output/customer_analytics.csv")


if __name__ == "__main__":
    main()
