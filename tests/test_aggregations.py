import pytest
from pyspark.sql import SparkSession
from src.processing import get_sales_aggregations, enrich_orders
from decimal import Decimal

def test_monthly_sales_aggregations(spark, sample_data):
    """Test monthly sales aggregation calculations"""
    # Get sample data
    orders_df = sample_data["orders"]
    customers_df = sample_data["customers"]
    products_df = sample_data["products"]
    
    # Create enriched orders first
    enriched_orders = enrich_orders(orders_df, customers_df, products_df)
    
    # Get aggregations
    monthly_sales, customer_patterns = get_sales_aggregations(enriched_orders)
    
    # Test monthly sales aggregations
    monthly_results = monthly_sales.collect()
    assert len(monthly_results) > 0, "Should have monthly sales data"
    
    # Get a specific month's data
    test_month = monthly_sales.filter(
        (monthly_sales["Order Year"] == 2023) & 
        (monthly_sales["Order Month"] == 1) &
        (monthly_sales["Category"] == "Electronics")
    ).collect()
    
    if test_month:
        row = test_month[0]
        assert isinstance(row["Monthly Sales"], Decimal), "Sales should be Decimal type"
        assert isinstance(row["Monthly Profit"], Decimal), "Profit should be Decimal type"
        assert row["Monthly Sales"] >= row["Monthly Profit"], "Sales should be >= Profit"

def test_customer_pattern_aggregations(spark, sample_data):
    """Test customer purchase pattern aggregations"""
    # Get sample data
    orders_df = sample_data["orders"]
    customers_df = sample_data["customers"]
    products_df = sample_data["products"]
    
    # Create enriched orders first
    enriched_orders = enrich_orders(orders_df, customers_df, products_df)
    
    # Get aggregations
    monthly_sales, customer_patterns = get_sales_aggregations(enriched_orders)
    
    # Test customer patterns
    customer_results = customer_patterns.collect()
    assert len(customer_results) > 0, "Should have customer pattern data"
    
    # Verify calculations for a specific customer
    test_customer = customer_patterns.filter(
        customer_patterns["Customer Name"] == "John Doe"
    ).collect()
    
    if test_customer:
        row = test_customer[0]
        assert row["Number of Orders"] > 0, "Should have at least one order"
        assert row["Average Order Value"] > 0, "Average order value should be positive"
        assert row["Total Spend"] >= row["Average Order Value"], "Total spend should be >= Average order"

def test_aggregation_datatypes(spark, sample_data):
    """Test that aggregated values have correct data types and precision"""
    # Get sample data and create enriched orders
    orders_df = sample_data["orders"]
    customers_df = sample_data["customers"]
    products_df = sample_data["products"]
    enriched_orders = enrich_orders(orders_df, customers_df, products_df)
    
    # Get aggregations
    monthly_sales, customer_patterns = get_sales_aggregations(enriched_orders)
    
    # Check schema types
    monthly_schema = monthly_sales.schema
    customer_schema = customer_patterns.schema
    
    # Verify monthly sales schema
    assert str(monthly_schema["Monthly Sales"].dataType) == "DecimalType(10,2)", \
        "Monthly Sales should be Decimal(10,2)"
    assert str(monthly_schema["Monthly Profit"].dataType) == "DecimalType(10,2)", \
        "Monthly Profit should be Decimal(10,2)"
    
    # Verify customer patterns schema
    assert str(customer_schema["Average Order Value"].dataType) == "DecimalType(10,2)", \
        "Average Order Value should be Decimal(10,2)"
    assert str(customer_schema["Total Spend"].dataType) == "DecimalType(10,2)", \
        "Total Spend should be Decimal(10,2)"

def test_aggregation_edge_cases(spark, sample_data):
    """Test aggregation behavior with edge cases"""
    # Get sample data
    orders_df = sample_data["orders"]
    customers_df = sample_data["customers"]
    products_df = sample_data["products"]
    
    # Create enriched orders first
    enriched_orders = enrich_orders(orders_df, customers_df, products_df)
    
    # Test with empty dataframe
    empty_orders = spark.createDataFrame([], enriched_orders.schema)
    monthly_sales, customer_patterns = get_sales_aggregations(empty_orders)
    
    assert monthly_sales.count() == 0, "Should handle empty input gracefully"
    assert customer_patterns.count() == 0, "Should handle empty input gracefully"
    
    # Test with single record
    single_record = enriched_orders.limit(1)
    monthly_sales, customer_patterns = get_sales_aggregations(single_record)
    
    assert monthly_sales.count() > 0, "Should process single record"
    assert customer_patterns.count() > 0, "Should process single record"
