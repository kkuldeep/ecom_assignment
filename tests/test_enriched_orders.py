import pytest
from datetime import datetime
from decimal import Decimal
from pyspark.sql.types import *
from pyspark.sql.functions import col
from src.processing import enrich_orders

# Test data constants
ORDER_ID = "O1"
CUSTOMER_ID = "C1"
PRODUCT_ID = "P1"
ORDER_DATE = "2023-01-15"

def create_test_dataframes(spark):
    # Create orders data with multiple records
    orders = [
        (ORDER_ID, CUSTOMER_ID, PRODUCT_ID, 
         datetime.strptime(ORDER_DATE, "%Y-%m-%d"), 1, 999.99, 199.99),
        ("O2", CUSTOMER_ID, "P2",
         datetime.strptime("2023-02-15", "%Y-%m-%d"), 2, 1499.99, 299.99)
    ]
    
    # Create customer data
    customers = [
        (CUSTOMER_ID, "John Doe", "USA"),
        ("C2", "Jane Smith", "UK")  # Extra customer for join testing
    ]
    
    # Create product data with sub-categories
    products = [
        (PRODUCT_ID, "Laptop", "Electronics", "Computers"),
        ("P2", "Desk", "Furniture", "Office")  # Additional product
    ]
    
    # Create DataFrames with schema
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
        ["Product ID", "Product Name", "Category", "Sub-Category"]
    )
    
    return orders_df, customers_df, products_df

def test_enriched_order_columns(spark, sample_data):
    """Test that all required columns are present in enriched orders with correct values"""
    # Execute
    result = enrich_orders(sample_data["orders"], sample_data["customers"], sample_data["products"])
    
    # Verify required columns exist
    required_columns = [
        "Order ID", "Customer ID", "Customer Name", "Country",
        "Product ID", "Product Name", "Category", "Sub-Category",
        "Order Date", "Order Year", "Order Month",
        "Quantity", "Sales", "Profit"
    ]
    
    for col_name in required_columns:
        assert col_name in result.columns, f"Missing required column: {col_name}"
    
    # Verify key columns have correct values
    test_row = result.filter(result["Product ID"] == 101).first()
    assert test_row is not None, "Test product not found"
    
    # Verify customer data
    assert test_row["Customer Name"] == "John Doe", "Incorrect customer name"
    assert test_row["Country"] == "USA", "Incorrect country"
    
    # Verify product data
    assert test_row["Category"] == "Electronics", "Incorrect category"
    assert test_row["Sub-Category"] == "Computers", "Incorrect sub-category"

def test_enriched_order_data_types(spark):
    """Test data types of enriched order columns"""
    # Setup and execute
    orders_df, customers_df, products_df = create_test_dataframes(spark)
    result = enrich_orders(orders_df, customers_df, products_df)
    
    # Verify data types
    schema = result.schema
    assert isinstance(schema["Order Year"].dataType, IntegerType), "Order Year should be integer"
    assert isinstance(schema["Order Month"].dataType, IntegerType), "Order Month should be integer"
    assert isinstance(schema["Sales"].dataType, DecimalType), "Sales should be decimal"
    assert isinstance(schema["Profit"].dataType, DecimalType), "Profit should be decimal"

def test_enriched_order_joins(spark):
    """Test that joins preserve correct relationships and data"""
    # Setup
    orders_df, customers_df, products_df = create_test_dataframes(spark)
    result = enrich_orders(orders_df, customers_df, products_df)
    
    # Test join integrity
    test_order = result.filter(col("Order ID") == ORDER_ID).collect()[0]
    assert test_order["Customer Name"] == "John Doe", "Customer join failed"
    assert test_order["Category"] == "Electronics", "Product join failed"
    assert test_order["Sub-Category"] == "Computers", "Product sub-category join failed"

def test_enriched_order_date_components(spark):
    """Test date component calculations"""
    # Setup
    orders_df, customers_df, products_df = create_test_dataframes(spark)
    result = enrich_orders(orders_df, customers_df, products_df)
    
    # Test date components for specific order
    test_order = result.filter(col("Order ID") == ORDER_ID).collect()[0]
    assert test_order["Order Year"] == 2023, "Incorrect year extraction"
    assert test_order["Order Month"] == 1, "Incorrect month extraction"

def test_enriched_order_edge_cases(spark, sample_data):
    """Test edge cases in order enrichment"""
    # Test with empty orders
    empty_orders = spark.createDataFrame([], sample_data["orders"].schema)
    result = enrich_orders(empty_orders, sample_data["customers"], sample_data["products"])
    assert result.count() == 0, "Empty orders should result in empty enriched orders"
    
    # Test profit rounding
    result = enrich_orders(sample_data["orders"], sample_data["customers"], sample_data["products"])
    rows = result.collect()
    
    for row in rows:
        # Verify exact 2 decimal places for Profit
        profit_str = str(row["Profit"])
        decimal_places = profit_str.split('.')[-1]
        assert len(decimal_places) == 2, f"Profit {profit_str} not rounded to exactly 2 decimal places"
        
        # Verify Sales has 2 decimal places
        sales_str = str(row["Sales"])
        sales_decimal = sales_str.split('.')[-1]
        assert len(sales_decimal) == 2, f"Sales {sales_str} not rounded to exactly 2 decimal places"
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