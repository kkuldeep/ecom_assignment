import pytest
from datetime import datetime
from pyspark.sql.types import *
from src.processing import enrich_orders

# Test data constants
ORDER_ID = "O1"
CUSTOMER_ID = "C1"
PRODUCT_ID = "P1"
ORDER_DATE = "2023-01-15"

def create_test_dataframes(spark):
    orders = [(
        ORDER_ID,
        CUSTOMER_ID,
        PRODUCT_ID,
        datetime.strptime(ORDER_DATE, "%Y-%m-%d"),
        1,
        999.99,
        199.99
    )]
    
    customers = [(CUSTOMER_ID, "John Doe", "USA")]
    products = [(PRODUCT_ID, "Laptop", "Electronics")]
    
    # Create DataFrames
    orders_df = spark.createDataFrame(
        orders,
        ["Order ID", "Customer ID", "Product ID", "Order Date", 
         "Quantity", "Sales", "Profit"]
    )
    
    customers_df = spark.createDataFrame(
        customers,
        ["Customer ID", "Customer Name", "Country"]
    )
    
    products_df = spark.createDataFrame(
        products,
        ["Product ID", "Product Name", "Category"]
    )
    
    return orders_df, customers_df, products_df

def test_order_enrichment(spark):
    # Setup
    orders_df, customers_df, products_df = create_test_dataframes(spark)
    
    # Execute
    result = enrich_orders(orders_df, customers_df, products_df)
    enriched = result.filter(f"Order ID == '{ORDER_ID}'").first()
    
    # Verify
    assert enriched is not None, f"Order {ORDER_ID} not found"
    assert enriched["Customer Name"] == "John Doe"
    assert enriched["Category"] == "Electronics"
    assert enriched["Order Month"] == 1  # January
    assert enriched["Order Year"] == 2023

def test_missing_customer(spark):
    orders_df, customers_df, products_df = create_test_dataframes(spark)
    
    # Remove customer to test join behavior
    bad_customers_df = customers_df.filter("Customer ID != 'C1'")
    
    result = enrich_orders(orders_df, bad_customers_df, products_df)
    assert result.count() == 0, "Should have no results with missing customer"