"""
Task 4: Profit Aggregation Table Test Cases
Tests for profit aggregation by Year, Category, Sub-Category, and Customer
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, sum as spark_sum, round as spark_round


class TestTask4ProfitAggregations:
    """Test cases for Task 4: Profit aggregation tables"""
    
    def test_profit_aggregation_table_creation(self, spark, sample_data):
        """Test creation of profit aggregation table with all required dimensions"""
        from src.processing import get_profit_aggregations, enrich_orders
        
        # First create enriched orders
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        # Create profit aggregation table
        profit_aggregations = get_profit_aggregations(enriched_orders)
        
        # Verify table is created and not empty
        assert profit_aggregations.count() > 0, "Profit aggregations table should not be empty"
        
        # Verify all required grouping columns exist
        expected_columns = ["Order Year", "Category", "Sub-Category", "Customer Name", "Total Profit"]
        for col_name in expected_columns:
            assert col_name in profit_aggregations.columns, f"Column '{col_name}' missing in profit aggregations table"
    
    def test_profit_aggregation_by_year(self, spark, sample_data):
        """Test profit aggregation by year dimension"""
        from src.processing import get_profit_aggregations, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        profit_aggregations = get_profit_aggregations(enriched_orders)
        
        # Test year-level aggregation accuracy
        aggregated_data = profit_aggregations.collect()
        
        # Group by year and verify totals
        year_totals = {}
        for row in aggregated_data:
            year = row["Order Year"]
            profit = row["Total Profit"]
            
            if year not in year_totals:
                year_totals[year] = 0
            year_totals[year] += profit
        
        # Verify each year has reasonable profit totals
        for year, total_profit in year_totals.items():
            assert total_profit > 0, f"Year {year} should have positive total profit"
            assert isinstance(year, int), f"Year should be integer, got {type(year)}"
    
    def test_profit_aggregation_by_category(self, spark, sample_data):
        """Test profit aggregation by product category dimension"""
        from src.processing import get_profit_aggregations, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        profit_aggregations = get_profit_aggregations(enriched_orders)
        
        # Test category-level aggregation
        aggregated_data = profit_aggregations.collect()
        
        categories_found = set()
        for row in aggregated_data:
            category = row["Category"]
            sub_category = row["Sub-Category"]
            profit = row["Total Profit"]
            
            categories_found.add(category)
            
            assert category is not None, "Category should not be null"
            assert sub_category is not None, "Sub-Category should not be null"
            assert profit is not None, "Total Profit should not be null"
        
        # Verify we have multiple categories
        assert len(categories_found) > 0, "Should have at least one category in aggregations"
    
    def test_profit_aggregation_by_customer(self, spark, sample_data):
        """Test profit aggregation by customer dimension"""
        from src.processing import get_profit_aggregations, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        profit_aggregations = get_profit_aggregations(enriched_orders)
        
        # Test customer-level aggregation
        aggregated_data = profit_aggregations.collect()
        
        customers_found = set()
        for row in aggregated_data:
            customer_name = row["Customer Name"]
            profit = row["Total Profit"]
            
            customers_found.add(customer_name)
            
            assert customer_name is not None, "Customer Name should not be null"
            assert profit >= 0, f"Total Profit should be non-negative for customer {customer_name}"
        
        # Verify we have multiple customers
        assert len(customers_found) > 0, "Should have at least one customer in aggregations"
    
    def test_aggregation_completeness(self, spark, sample_data):
        """Test that aggregations include all combinations and are complete"""
        from src.processing import get_profit_aggregations, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        profit_aggregations = get_profit_aggregations(enriched_orders)
        
        # Get unique combinations from enriched orders
        enriched_combinations = enriched_orders.select(
            "Order Year", "Category", "Sub-Category", "Customer Name"
        ).distinct().collect()
        
        # Get combinations from aggregations
        agg_combinations = profit_aggregations.select(
            "Order Year", "Category", "Sub-Category", "Customer Name"
        ).distinct().collect()
        
        # Convert to sets for comparison
        enriched_set = set()
        for row in enriched_combinations:
            enriched_set.add((row["Order Year"], row["Category"], row["Sub-Category"], row["Customer Name"]))
        
        agg_set = set()
        for row in agg_combinations:
            agg_set.add((row["Order Year"], row["Category"], row["Sub-Category"], row["Customer Name"]))
        
        # Verify all combinations are included
        assert agg_set == enriched_set, "Aggregation should include all unique combinations from enriched orders"
    
    def test_profit_calculation_accuracy(self, spark, sample_data):
        """Test accuracy of profit calculations in aggregations"""
        from src.processing import get_profit_aggregations, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        profit_aggregations = get_profit_aggregations(enriched_orders)
        
        # Sample verification: pick one combination and verify manually
        sample_agg = profit_aggregations.limit(1).collect()[0]
        
        year = sample_agg["Order Year"]
        category = sample_agg["Category"]
        sub_category = sample_agg["Sub-Category"]
        customer = sample_agg["Customer Name"]
        agg_profit = sample_agg["Total Profit"]
        
        # Calculate manually from enriched orders
        manual_calculation = enriched_orders.filter(
            (col("Order Year") == year) &
            (col("Category") == category) &
            (col("Sub-Category") == sub_category) &
            (col("Customer Name") == customer)
        ).agg(spark_sum("Profit").alias("manual_total")).collect()[0]["manual_total"]
        
        # Compare results (allowing for small rounding differences)
        assert abs(float(agg_profit) - float(manual_calculation)) < 0.01, \
            f"Aggregated profit {agg_profit} doesn't match manual calculation {manual_calculation}"
    
    def test_profit_rounding_in_aggregations(self, spark, sample_data):
        """Test that profit values are properly rounded in aggregations"""
        from src.processing import get_profit_aggregations, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        profit_aggregations = get_profit_aggregations(enriched_orders)
        
        # Check profit rounding in aggregations
        agg_data = profit_aggregations.collect()
        
        for row in agg_data:
            total_profit = row["Total Profit"]
            profit_str = str(float(total_profit))
            
            # Check decimal places
            if '.' in profit_str:
                decimal_part = profit_str.split('.')[1]
                assert len(decimal_part) <= 2, f"Total Profit {total_profit} should be rounded to 2 decimal places"