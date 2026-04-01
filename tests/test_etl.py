"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pandas as pd
import pytest
from etl_pipeline import transform, validate


def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""
    orders = pd.DataFrame({
        'order_id': [1, 2],
        'customer_id': [1, 2],
        'order_date': ['2024-01-01', '2024-01-02'],
        'status': ['completed', 'cancelled']
    })
    order_items = pd.DataFrame({
        'item_id': [1, 2],
        'order_id': [1, 2],
        'product_id': [101, 102],
        'quantity': [2, 3]
    })
    products = pd.DataFrame({
        'product_id': [101, 102],
        'product_name': ['ProdA', 'ProdB'],
        'category': ['Cat1', 'Cat2'],
        'unit_price': [10, 20]
    })
    customers = pd.DataFrame({
        'customer_id': [1, 2],
        'customer_name': ['Alice', 'Bob']
    })

    data_dict = {
        'orders': orders,
        'order_items': order_items,
        'products': products,
        'customers': customers
    }

    df_transformed = transform(data_dict)
    # Confirm cancelled order is removed
    assert 2 not in df_transformed['customer_id'].values


def test_transform_filters_suspicious_quantity():
    """Create test DataFrames with quantity > 100. Confirm it's excluded."""
    orders = pd.DataFrame({
        'order_id': [1],
        'customer_id': [1],
        'order_date': ['2024-01-01'],
        'status': ['completed']
    })
    order_items = pd.DataFrame({
        'item_id': [1, 2],
        'order_id': [1, 1],
        'product_id': [101, 102],
        'quantity': [2, 150]  # 150 is suspicious
    })
    products = pd.DataFrame({
        'product_id': [101, 102],
        'product_name': ['ProdA', 'ProdB'],
        'category': ['Cat1', 'Cat2'],
        'unit_price': [10, 20]
    })
    customers = pd.DataFrame({
        'customer_id': [1],
        'customer_name': ['Alice']
    })

    data_dict = {
        'orders': orders,
        'order_items': order_items,
        'products': products,
        'customers': customers
    }

    df_transformed = transform(data_dict)
    # Confirm suspicious item is excluded from revenue calculation
    assert df_transformed['total_revenue'].iloc[0] == 2 * 10


def test_validate_catches_nulls():
    """Create a DataFrame with null customer_id. Confirm validate() raises ValueError."""
    df = pd.DataFrame({
        'customer_id': [1, None],
        'customer_name': ['Alice', 'Bob'],
        'total_orders': [1, 1],
        'total_revenue': [100, 50],
        'avg_order_value': [100, 50],
        'top_category': ['Cat1', 'Cat2']
    })

    with pytest.raises(ValueError):
        validate(df)
