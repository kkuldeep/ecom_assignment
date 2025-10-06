"""
Task 3: Enriched Orders Table Test Cases
Tests for order enrichment with customer and product information
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, year, month, round as spark_round


class TestTask3EnrichedOrders:
    """Test cases for Task 3: Enriched orders table"""
    
    def test_enriched_orders_table_creation(self, spark, sample_data):
        """Test enriched orders table creation with all required information"""
        from src.processing import enrich_orders
        
        # Create enriched orders table
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        # Verify table is created and not empty
        assert enriched_orders.count() > 0, "Enriched orders table should not be empty"
        
        # Verify all required columns exist
        expected_columns = [
            "Order ID", "Customer ID", "Product ID", "Order Date", "Quantity", "Sales", "Profit",
            "Customer Name", "Country", "Product Name", "Category", "Sub-Category"
        ]
        
        for col_name in expected_columns:
            assert col_name in enriched_orders.columns, f"Column '{col_name}' missing in enriched orders table"
    
    def test_profit_rounding_to_2_decimal_places(self, spark, sample_data):
        """Test that profit is rounded to exactly 2 decimal places"""
        from src.processing import enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        # Check profit rounding
        profit_data = enriched_orders.select("Profit").collect()
        
        for row in profit_data:
            profit_value = row["Profit"]
            profit_str = str(float(profit_value))
            
            # Check decimal places
            if '.' in profit_str:
                decimal_part = profit_str.split('.')[1]
                assert len(decimal_part) <= 2, f"Profit {profit_value} should be rounded to 2 decimal places"
    
    def test_customer_information_enrichment(self, spark, sample_data):
        """Test that customer information is correctly joined"""
        from src.processing import enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        # Verify customer information is populated
        enriched_data = enriched_orders.collect()
        
        for row in enriched_data:
            assert row["Customer Name"] is not None, "Customer Name should not be null"
            assert row["Country"] is not None, "Country should not be null"
            
            # Verify customer information consistency
            customer_id = row["Customer ID"]
            customer_name = row["Customer Name"]
            
            # Get original customer info
            original_customer = sample_data["customers"].filter(col("Customer ID") == customer_id).collect()[0]
            assert original_customer["Customer Name"] == customer_name, f"Customer name mismatch for Customer ID {customer_id}"
    
    def test_product_information_enrichment(self, spark, sample_data):
        """Test that product information is correctly joined"""
        from src.processing import enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        # Verify product information is populated
        enriched_data = enriched_orders.collect()
        
        for row in enriched_data:
            assert row["Product Name"] is not None, "Product Name should not be null"
            assert row["Category"] is not None, "Category should not be null"
            assert row["Sub-Category"] is not None, "Sub-Category should not be null"
            
            # Verify product information consistency
            product_id = row["Product ID"]
            product_name = row["Product Name"]
            category = row["Category"]
            sub_category = row["Sub-Category"]
            
            # Get original product info
            original_product = sample_data["products"].filter(col("Product ID") == product_id).collect()[0]
            assert original_product["Product Name"] == product_name, f"Product name mismatch for Product ID {product_id}"
            assert original_product["Category"] == category, f"Category mismatch for Product ID {product_id}"
            assert original_product["Sub-Category"] == sub_category, f"Sub-Category mismatch for Product ID {product_id}"
    
    def test_order_date_handling(self, spark, sample_data):
        """Test proper order date handling and date-based calculations"""
        from src.processing import enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        # Check if date-based columns are added (if implemented)
        if "Order Year" in enriched_orders.columns:
            enriched_data = enriched_orders.collect()
            
            for row in enriched_data:
                order_year = row["Order Year"]
                assert order_year is not None, "Order Year should not be null"
                assert isinstance(order_year, int), "Order Year should be integer"
                assert 2020 <= order_year <= 2025, f"Order Year {order_year} should be in reasonable range"
        
        if "Order Month" in enriched_orders.columns:
            enriched_data = enriched_orders.collect()
            
            for row in enriched_data:
                order_month = row["Order Month"]
                assert order_month is not None, "Order Month should not be null"
                assert isinstance(order_month, int), "Order Month should be integer"
                assert 1 <= order_month <= 12, f"Order Month {order_month} should be between 1-12"
    
    def test_enriched_orders_data_integrity(self, spark, sample_data):
        """Test data integrity in enriched orders table"""
        from src.processing import enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        # Verify row count matches original orders
        original_order_count = sample_data["orders"].count()
        enriched_order_count = enriched_orders.count()
        assert enriched_order_count == original_order_count, "Enriched orders count should match original orders count"
        
        # Verify no data loss in key fields
        enriched_data = enriched_orders.collect()
        
        for row in enriched_data:
            assert row["Order ID"] is not None, "Order ID should not be null"
            assert row["Sales"] is not None, "Sales should not be null"
            assert row["Profit"] is not None, "Profit should not be null"
            assert row["Quantity"] is not None, "Quantity should not be null"
    
    def test_enriched_orders_join_accuracy(self, spark, sample_data):
        """Test accuracy of joins between orders, customers, and products"""
        from src.processing import enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        # Sample a few records and verify join accuracy
        sample_enriched = enriched_orders.limit(5).collect()
        
        for row in sample_enriched:
            customer_id = row["Customer ID"]
            product_id = row["Product ID"]
            
            # Verify customer join accuracy
            customer_info = sample_data["customers"].filter(col("Customer ID") == customer_id).collect()[0]
            assert customer_info["Customer Name"] == row["Customer Name"]
            assert customer_info["Country"] == row["Country"]
            
            # Verify product join accuracy
            product_info = sample_data["products"].filter(col("Product ID") == product_id).collect()[0]
            assert product_info["Product Name"] == row["Product Name"]
            assert product_info["Category"] == row["Category"]
            assert product_info["Sub-Category"] == row["Sub-Category"]