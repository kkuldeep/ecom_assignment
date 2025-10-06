"""
Task 2: Enriched Customer and Product Tables Test Cases
Tests for customer and product enrichment logic
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, sum as spark_sum, count, avg, round as spark_round


class TestTask2EnrichedTables:
    """Test cases for Task 2: Enriched customer and product tables"""
    
    def test_enriched_customers_table_creation(self, spark, sample_data):
        """Test enriched customers table with calculated metrics"""
        from src.processing import get_customer_metrics
        
        # Create enriched customers table
        enriched_customers = get_customer_metrics(sample_data["orders"], sample_data["customers"])
        
        # Verify table is created and not empty
        assert enriched_customers.count() > 0, "Enriched customers table should not be empty"
        
        # Verify required columns exist
        expected_columns = ["Customer ID", "Customer Name", "Country", "Total Orders", "Total Sales", "Total Profit"]
        for col_name in expected_columns:
            assert col_name in enriched_customers.columns, f"Column '{col_name}' missing in enriched customers table"
        
        # Verify calculated metrics are correct
        enriched_data = enriched_customers.collect()
        for row in enriched_data:
            assert row["Total Orders"] >= 1, "Total Orders should be at least 1"
            assert row["Total Sales"] > 0, "Total Sales should be positive"
            assert row["Average Order Value"] > 0, "Average Order Value should be positive"
    
    def test_enriched_products_table_creation(self, spark, sample_data):
        """Test enriched products table with performance metrics"""
        from src.processing import analyze_product_performance
        
        # Create enriched products table
        enriched_products = analyze_product_performance(sample_data["orders"], sample_data["products"])
        
        # Verify table is created and not empty
        assert enriched_products.count() > 0, "Enriched products table should not be empty"
        
        # Verify required columns exist
        expected_columns = ["Product ID", "Product Name", "Category", "Sub-Category", "Total Sales", "Total Profit", "Profit Margin"]
        for col_name in expected_columns:
            assert col_name in enriched_products.columns, f"Column '{col_name}' missing in enriched products table"
        
        # Verify profit margin calculation
        enriched_data = enriched_products.collect()
        for row in enriched_data:
            if row["Total Sales"] > 0:
                expected_margin = (row["Total Profit"] / row["Total Sales"]) * 100
                actual_margin = row["Profit Margin"]
                assert abs(actual_margin - expected_margin) < 0.01, f"Profit margin calculation incorrect for product {row['Product ID']}"
    
    def test_customer_segmentation_logic(self, spark, sample_data):
        """Test customer segmentation based on value"""
        from src.processing import get_customer_metrics
        from src.config import BusinessConfig
        
        enriched_customers = get_customer_metrics(sample_data["orders"], sample_data["customers"])
        
        # Test high-value customer identification
        high_value_customers = enriched_customers.filter(col("Total Sales") >= BusinessConfig.HIGH_VALUE_THRESHOLD)
        
        for row in high_value_customers.collect():
            assert row["Customer Segment"] == "High Value", f"Customer {row['Customer Name']} should be High Value"
        
        # Test medium-value customer identification
        medium_value_customers = enriched_customers.filter(
            (col("Total Sales") >= BusinessConfig.MEDIUM_VALUE_THRESHOLD) & 
            (col("Total Sales") < BusinessConfig.HIGH_VALUE_THRESHOLD)
        )
        
        for row in medium_value_customers.collect():
            assert row["Customer Segment"] == "Medium Value", f"Customer {row['Customer Name']} should be Medium Value"
    
    def test_product_performance_classification(self, spark, sample_data):
        """Test product performance classification logic"""
        from src.processing import analyze_product_performance
        
        enriched_products = analyze_product_performance(sample_data["orders"], sample_data["products"])
        
        # Verify performance flags are assigned correctly
        performance_data = enriched_products.collect()
        
        for row in performance_data:
            profit_margin = row["Profit Margin"]
            performance_flag = row["Performance Flag"]
            
            if profit_margin >= 20:
                assert performance_flag == "High Performing", f"Product {row['Product Name']} should be High Performing"
            elif profit_margin >= 10:
                assert performance_flag == "Good Performing", f"Product {row['Product Name']} should be Good Performing"
            else:
                assert performance_flag == "Needs Improvement", f"Product {row['Product Name']} should be Needs Improvement"
    
    def test_enriched_tables_data_consistency(self, spark, sample_data):
        """Test data consistency between enriched and raw tables"""
        from src.processing import get_customer_metrics, analyze_product_performance
        
        enriched_customers = get_customer_metrics(sample_data["orders"], sample_data["customers"])
        enriched_products = analyze_product_performance(sample_data["orders"], sample_data["products"])
        
        # Verify customer count consistency
        raw_customer_count = sample_data["customers"].count()
        enriched_customer_count = enriched_customers.count()
        assert enriched_customer_count <= raw_customer_count, "Enriched customers should not exceed raw customers"
        
        # Verify product count consistency
        raw_product_count = sample_data["products"].count()
        enriched_product_count = enriched_products.count()
        assert enriched_product_count <= raw_product_count, "Enriched products should not exceed raw products"
        
        # Verify total sales consistency
        total_sales_from_orders = sample_data["orders"].agg(spark_sum("Sales").alias("total")).collect()[0]["total"]
        total_sales_from_customers = enriched_customers.agg(spark_sum("Total Sales").alias("total")).collect()[0]["total"]
        
        assert abs(total_sales_from_orders - total_sales_from_customers) < 0.01, "Total sales should match between orders and enriched customers"
    
    def test_enriched_tables_null_handling(self, spark, sample_data):
        """Test null value handling in enriched tables"""
        from src.processing import get_customer_metrics, analyze_product_performance
        
        enriched_customers = get_customer_metrics(sample_data["orders"], sample_data["customers"])
        enriched_products = analyze_product_performance(sample_data["orders"], sample_data["products"])
        
        # Check for null values in critical calculated fields
        null_total_sales_customers = enriched_customers.filter(col("Total Sales").isNull()).count()
        assert null_total_sales_customers == 0, "Total Sales should not be null for any customer"
        
        null_profit_margin_products = enriched_products.filter(col("Profit Margin").isNull()).count()
        assert null_profit_margin_products == 0, "Profit Margin should not be null for any product"