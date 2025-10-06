"""
Task 1: Create Raw Tables Test Cases
Tests for raw data ingestion and table creation
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
import os


class TestTask1RawTables:
    """Test cases for Task 1: Raw table creation"""
    
    def test_customers_raw_table_creation(self, spark, sample_data):
        """Test that customers raw table is created with correct schema"""
        customers_df = sample_data["customers"]
        
        # Verify DataFrame is not empty
        assert customers_df.count() > 0, "Customers raw table should not be empty"
        
        # Verify required columns exist
        expected_columns = ["Customer ID", "Customer Name", "Country"]
        for col_name in expected_columns:
            assert col_name in customers_df.columns, f"Column '{col_name}' missing in customers raw table"
        
        # Verify data types
        schema = customers_df.schema
        customer_id_field = next(field for field in schema.fields if field.name == "Customer ID")
        assert customer_id_field.dataType == IntegerType(), "Customer ID should be IntegerType"
        
        customer_name_field = next(field for field in schema.fields if field.name == "Customer Name")
        assert customer_name_field.dataType == StringType(), "Customer Name should be StringType"
    
    def test_products_raw_table_creation(self, spark, sample_data):
        """Test that products raw table is created with correct schema"""
        products_df = sample_data["products"]
        
        # Verify DataFrame is not empty
        assert products_df.count() > 0, "Products raw table should not be empty"
        
        # Verify required columns exist
        expected_columns = ["Product ID", "Product Name", "Category", "Sub-Category"]
        for col_name in expected_columns:
            assert col_name in products_df.columns, f"Column '{col_name}' missing in products raw table"
        
        # Verify no null values in critical columns
        null_product_ids = products_df.filter(col("Product ID").isNull()).count()
        assert null_product_ids == 0, "Product ID should not have null values"
        
        null_categories = products_df.filter(col("Category").isNull()).count()
        assert null_categories == 0, "Category should not have null values"
    
    def test_orders_raw_table_creation(self, spark, sample_data):
        """Test that orders raw table is created with correct schema"""
        orders_df = sample_data["orders"]
        
        # Verify DataFrame is not empty
        assert orders_df.count() > 0, "Orders raw table should not be empty"
        
        # Verify required columns exist
        expected_columns = ["Order ID", "Customer ID", "Product ID", "Order Date", "Quantity", "Sales", "Profit"]
        for col_name in expected_columns:
            assert col_name in orders_df.columns, f"Column '{col_name}' missing in orders raw table"
        
        # Verify numeric columns have correct data types
        schema = orders_df.schema
        sales_field = next(field for field in schema.fields if field.name == "Sales")
        assert sales_field.dataType in [DoubleType(), DecimalType(10,2)], "Sales should be numeric type"
        
        profit_field = next(field for field in schema.fields if field.name == "Profit")
        assert profit_field.dataType in [DoubleType(), DecimalType(10,2)], "Profit should be numeric type"
    
    def test_raw_tables_data_integrity(self, spark, sample_data):
        """Test data integrity across raw tables"""
        customers_df = sample_data["customers"]
        products_df = sample_data["products"]
        orders_df = sample_data["orders"]
        
        # Test referential integrity
        customer_ids_in_orders = orders_df.select("Customer ID").distinct().rdd.flatMap(lambda x: x).collect()
        customer_ids_in_customers = customers_df.select("Customer ID").rdd.flatMap(lambda x: x).collect()
        
        for customer_id in customer_ids_in_orders:
            assert customer_id in customer_ids_in_customers, f"Customer ID {customer_id} in orders but not in customers table"
        
        # Test product referential integrity
        product_ids_in_orders = orders_df.select("Product ID").distinct().rdd.flatMap(lambda x: x).collect()
        product_ids_in_products = products_df.select("Product ID").rdd.flatMap(lambda x: x).collect()
        
        for product_id in product_ids_in_orders:
            assert product_id in product_ids_in_products, f"Product ID {product_id} in orders but not in products table"
    
    def test_raw_tables_business_rules(self, spark, sample_data):
        """Test business rules for raw tables"""
        orders_df = sample_data["orders"]
        
        # Test that sales values are positive or zero
        negative_sales = orders_df.filter(col("Sales") < 0).count()
        assert negative_sales == 0, "Sales values should not be negative"
        
        # Test that quantities are positive
        non_positive_quantities = orders_df.filter(col("Quantity") <= 0).count()
        assert non_positive_quantities == 0, "Quantities should be positive"
        
        # Test order ID uniqueness
        total_orders = orders_df.count()
        unique_order_ids = orders_df.select("Order ID").distinct().count()
        assert total_orders == unique_order_ids, "Order IDs should be unique"