import pytest
from pyspark.sql.types import *
from src.processing import get_customer_metrics

# Test data
EXPECTED_METRICS = {
    "John Doe": {
        "orders": 2,
        "sales": 2499.98,
        "segment": "Medium Value"
    },
    "Jane Smith": {
        "orders": 3,
        "sales": 12000.00,
        "segment": "High Value"
    }
}

def setup_test_data(spark):
    # Create test data with known values
    orders_data = [
        ("O1", "C1", "2023-01-01", 1499.99),
        ("O2", "C1", "2023-02-01", 999.99),
        ("O3", "C2", "2023-01-15", 4000.00),
        ("O4", "C2", "2023-02-15", 5000.00),
        ("O5", "C2", "2023-03-15", 3000.00)
    ]
    
    customers_data = [
        ("C1", "John Doe", "US"),
        ("C2", "Jane Smith", "UK")
    ]
    
    # Create DataFrames
    orders_df = spark.createDataFrame(
        orders_data, 
        ["Order ID", "Customer ID", "Order Date", "Sales"]
    )
    
    customers_df = spark.createDataFrame(
        customers_data,
        ["Customer ID", "Customer Name", "Country"]
    )
    
    return orders_df, customers_df

def test_customer_value_segments(spark):
    orders_df, customers_df = setup_test_data(spark)
    result_df = get_customer_metrics(orders_df, customers_df)
    metrics = {row["Customer Name"]: row for row in result_df.collect()}
    
    for name, expected in EXPECTED_METRICS.items():
        customer = metrics[name]
        assert customer["Total Orders"] == expected["orders"], \
            f"Wrong order count for {name}"
        assert abs(customer["Total Sales"] - expected["sales"]) < 0.01, \
            f"Sales mismatch for {name}"
        assert customer["Customer Segment"] == expected["segment"], \
            f"Wrong segment for {name}"

def test_inactive_customers(spark):
    orders_df, customers_df = setup_test_data(spark)
    result_df = get_customer_metrics(orders_df, customers_df)
    
    # All test customers should be inactive due to old dates
    actives = result_df.filter("Activity Status != 'Inactive'").count()
    assert actives == 0, "Found active customers in old test data"