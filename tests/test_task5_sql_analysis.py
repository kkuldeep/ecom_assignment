"""
Task 5: SQL Profit Analysis Test Cases
Tests for SQL-based profit analysis queries
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col


class TestTask5SQLProfitAnalysis:
    """Test cases for Task 5: SQL profit analysis queries"""
    
    def test_sql_profit_analysis_setup(self, spark, sample_data):
        """Test SQL analysis setup and table registration"""
        from src.processing import get_sql_profit_analysis, enrich_orders
        
        # Create enriched orders for SQL analysis
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        # Run SQL profit analysis
        sql_results = get_sql_profit_analysis(spark, enriched_orders)
        
        # Verify all required queries are executed
        expected_queries = ["yearly_profit", "category_profit", "customer_profit", "customer_yearly_profit"]
        for query_name in expected_queries:
            assert query_name in sql_results, f"Missing SQL query result: {query_name}"
            assert sql_results[query_name] is not None, f"SQL query result is None: {query_name}"
    
    def test_profit_by_year_sql_query(self, spark, sample_data):
        """Test SQL query for profit by year"""
        from src.processing import get_sql_profit_analysis, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        sql_results = get_sql_profit_analysis(spark, enriched_orders)
        yearly_profit = sql_results["yearly_profit"]
        
        # Verify result structure
        assert yearly_profit.count() > 0, "Yearly profit query should return results"
        
        expected_columns = ["Order_Year", "Total_Profit", "Total_Sales", "Order_Count", "Profit_Margin"]
        for col_name in expected_columns:
            assert col_name in yearly_profit.columns, f"Column '{col_name}' missing in yearly profit query"
        
        # Verify data quality
        yearly_data = yearly_profit.collect()
        for row in yearly_data:
            assert row["Order_Year"] is not None, "Order_Year should not be null"
            assert row["Total_Profit"] is not None, "Total_Profit should not be null"
            assert row["Total_Sales"] > 0, "Total_Sales should be positive"
            assert row["Order_Count"] > 0, "Order_Count should be positive"
            
            # Verify profit margin calculation
            if row["Total_Sales"] > 0:
                expected_margin = (row["Total_Profit"] / row["Total_Sales"]) * 100
                actual_margin = row["Profit_Margin"]
                assert abs(actual_margin - expected_margin) < 0.1, "Profit margin calculation should be accurate"
    
    def test_profit_by_year_category_sql_query(self, spark, sample_data):
        """Test SQL query for profit by year and product category"""
        from src.processing import get_sql_profit_analysis, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        sql_results = get_sql_profit_analysis(spark, enriched_orders)
        category_profit = sql_results["category_profit"]
        
        # Verify result structure
        assert category_profit.count() > 0, "Category profit query should return results"
        
        expected_columns = ["Order_Year", "Category", "Total_Profit", "Total_Sales", "Order_Count", "Profit_Margin"]
        for col_name in expected_columns:
            assert col_name in category_profit.columns, f"Column '{col_name}' missing in category profit query"
        
        # Verify data quality
        category_data = category_profit.collect()
        for row in category_data:
            assert row["Order_Year"] is not None, "Order_Year should not be null"
            assert row["Category"] is not None, "Category should not be null"
            assert row["Total_Profit"] is not None, "Total_Profit should not be null"
            assert row["Total_Sales"] > 0, "Total_Sales should be positive"
            assert row["Order_Count"] > 0, "Order_Count should be positive"
    
    def test_profit_by_customer_sql_query(self, spark, sample_data):
        """Test SQL query for profit by customer"""
        from src.processing import get_sql_profit_analysis, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        sql_results = get_sql_profit_analysis(spark, enriched_orders)
        customer_profit = sql_results["customer_profit"]
        
        # Verify result structure
        assert customer_profit.count() > 0, "Customer profit query should return results"
        
        expected_columns = ["Customer_Name", "Total_Profit", "Total_Sales", "Order_Count", "Avg_Order_Value", "Profit_Margin"]
        for col_name in expected_columns:
            assert col_name in customer_profit.columns, f"Column '{col_name}' missing in customer profit query"
        
        # Verify data quality
        customer_data = customer_profit.collect()
        for row in customer_data:
            assert row["Customer_Name"] is not None, "Customer_Name should not be null"
            assert row["Total_Profit"] is not None, "Total_Profit should not be null"
            assert row["Total_Sales"] > 0, "Total_Sales should be positive"
            assert row["Order_Count"] > 0, "Order_Count should be positive"
            
            # Verify average order value calculation
            expected_avg = row["Total_Sales"] / row["Order_Count"]
            actual_avg = row["Avg_Order_Value"]
            assert abs(actual_avg - expected_avg) < 0.01, "Average order value calculation should be accurate"
    
    def test_profit_by_customer_year_sql_query(self, spark, sample_data):
        """Test SQL query for profit by customer and year"""
        from src.processing import get_sql_profit_analysis, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        sql_results = get_sql_profit_analysis(spark, enriched_orders)
        customer_yearly_profit = sql_results["customer_yearly_profit"]
        
        # Verify result structure
        assert customer_yearly_profit.count() > 0, "Customer yearly profit query should return results"
        
        expected_columns = ["Customer_Name", "Order_Year", "Total_Profit", "Total_Sales", "Order_Count", "Profit_Margin"]
        for col_name in expected_columns:
            assert col_name in customer_yearly_profit.columns, f"Column '{col_name}' missing in customer yearly profit query"
        
        # Verify data quality
        customer_yearly_data = customer_yearly_profit.collect()
        for row in customer_yearly_data:
            assert row["Customer_Name"] is not None, "Customer_Name should not be null"
            assert row["Order_Year"] is not None, "Order_Year should not be null"
            assert row["Total_Profit"] is not None, "Total_Profit should not be null"
            assert row["Total_Sales"] > 0, "Total_Sales should be positive"
            assert row["Order_Count"] > 0, "Order_Count should be positive"
    
    def test_sql_query_result_consistency(self, spark, sample_data):
        """Test consistency between different SQL query results"""
        from src.processing import get_sql_profit_analysis, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        sql_results = get_sql_profit_analysis(spark, enriched_orders)
        
        # Get total profits from different queries
        yearly_total = sql_results["yearly_profit"].agg({"Total_Profit": "sum"}).collect()[0][0]
        customer_total = sql_results["customer_profit"].agg({"Total_Profit": "sum"}).collect()[0][0]
        
        # Verify totals are consistent (allowing for small rounding differences)
        assert abs(float(yearly_total) - float(customer_total)) < 1.0, \
            "Total profit should be consistent across different SQL queries"
    
    def test_sql_query_performance_metrics(self, spark, sample_data):
        """Test that SQL queries include proper performance metrics"""
        from src.processing import get_sql_profit_analysis, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        sql_results = get_sql_profit_analysis(spark, enriched_orders)
        
        # Test yearly profit metrics
        yearly_profit = sql_results["yearly_profit"]
        yearly_sample = yearly_profit.limit(1).collect()[0]
        
        # Verify calculated metrics are reasonable
        assert yearly_sample["Profit_Margin"] >= -100, "Profit margin should be reasonable (>= -100%)"
        assert yearly_sample["Profit_Margin"] <= 1000, "Profit margin should be reasonable (<= 1000%)"
        
        # Test customer profit metrics
        customer_profit = sql_results["customer_profit"]
        customer_sample = customer_profit.limit(1).collect()[0]
        
        assert customer_sample["Avg_Order_Value"] > 0, "Average order value should be positive"
        
    def test_sql_queries_handle_edge_cases(self, spark, sample_data):
        """Test that SQL queries handle edge cases properly"""
        from src.processing import get_sql_profit_analysis, enrich_orders
        
        enriched_orders = enrich_orders(
            sample_data["orders"], 
            sample_data["customers"], 
            sample_data["products"]
        )
        
        sql_results = get_sql_profit_analysis(spark, enriched_orders)
        
        # Test for zero division handling in profit margin calculations
        for query_name, result_df in sql_results.items():
            if "Profit_Margin" in result_df.columns:
                data = result_df.collect()
                for row in data:
                    profit_margin = row["Profit_Margin"]
                    # Should not be None or infinite
                    assert profit_margin is not None, f"Profit margin should not be None in {query_name}"
                    assert profit_margin != float('inf'), f"Profit margin should not be infinite in {query_name}"
                    assert profit_margin != float('-inf'), f"Profit margin should not be negative infinite in {query_name}"