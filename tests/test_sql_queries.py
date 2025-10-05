import pytest
from pyspark.sql import SparkSession
from src.processing import enrich_orders, get_sql_profit_analysis
from decimal import Decimal
from pyspark.sql.types import DecimalType, IntegerType

def test_sql_profit_queries(spark, sample_data):
    """Test all profit analysis SQL queries"""
    # Setup enriched orders
    enriched_orders = enrich_orders(
        sample_data["orders"], 
        sample_data["customers"], 
        sample_data["products"]
    )
    
    # Execute all SQL queries
    results = get_sql_profit_analysis(spark, enriched_orders)
    
    # Test yearly profit query
    yearly_profit = results["yearly_profit"]
    assert "Order_Year" in yearly_profit.columns
    assert "Total_Profit" in yearly_profit.columns
    assert isinstance(yearly_profit.schema["Total_Profit"].dataType, DecimalType)
    assert yearly_profit.count() > 0
    
    # Test category profit query
    category_profit = results["category_profit"]
    assert all(col in category_profit.columns for col in ["Order_Year", "Category"])
    assert isinstance(category_profit.schema["Total_Sales"].dataType, DecimalType)
    assert category_profit.count() > 0
    
    # Test customer profit query
    customer_profit = results["customer_profit"]
    assert "Customer_Name" in customer_profit.columns
    assert isinstance(customer_profit.schema["Avg_Order_Value"].dataType, DecimalType)
    assert customer_profit.count() > 0
    
    # Test customer yearly profit query
    customer_yearly = results["customer_yearly_profit"]
    assert all(col in customer_yearly.columns for col in ["Customer_Name", "Order_Year"])
    assert isinstance(customer_yearly.schema["Profit_Margin"].dataType, DecimalType)
    assert customer_yearly.count() > 0

def test_category_sales_query(spark, sample_data):
    """Test category sales and customer count query"""
    # Get sample data
    orders_df = sample_data["orders"]
    customers_df = sample_data["customers"]
    products_df = sample_data["products"]
    
    # Create enriched orders
    enriched_orders = enrich_orders(orders_df, customers_df, products_df)
    enriched_orders.createOrReplaceTempView("enriched_orders")
    
    # Test category sales query
    category_sales = spark.sql("""
        SELECT 
            Category,
            ROUND(SUM(Sales), 2) as Total_Sales,
            COUNT(DISTINCT `Customer ID`) as Unique_Customers,
            ROUND(AVG(Sales), 2) as Avg_Sale_Value
        FROM enriched_orders
        GROUP BY Category
        ORDER BY Total_Sales DESC
    """)
    
    results = category_sales.collect()
    assert len(results) > 0, "Should have category sales data"
    
    # Verify first category has highest sales
    top_category = results[0]
    assert all(top_category["Total_Sales"] >= row["Total_Sales"] for row in results[1:]), \
        "Categories should be ordered by sales"
    assert top_category["Unique_Customers"] > 0, "Should have customers"
    assert top_category["Avg_Sale_Value"] > 0, "Should have valid average"

def test_customer_rankings_query(spark, sample_data):
    """Test customer rankings and purchase patterns query"""
    # Get sample data and create view
    orders_df = sample_data["orders"]
    customers_df = sample_data["customers"]
    products_df = sample_data["products"]
    enriched_orders = enrich_orders(orders_df, customers_df, products_df)
    enriched_orders.createOrReplaceTempView("enriched_orders")
    
    # Test customer rankings query
    customer_rankings = spark.sql("""
        SELECT 
            Customer_Name,
            ROUND(SUM(Sales), 2) as Total_Spend,
            COUNT(*) as Number_of_Orders,
            ROUND(AVG(Sales), 2) as Avg_Order_Value,
            MAX(Order_Date) as Last_Order_Date
        FROM enriched_orders
        GROUP BY Customer_Name
        ORDER BY Total_Spend DESC
    """)
    
    results = customer_rankings.collect()
    assert len(results) > 0, "Should have customer rankings"
    
    # Verify ordering and calculations
    top_customer = results[0]
    assert all(top_customer["Total_Spend"] >= row["Total_Spend"] for row in results[1:]), \
        "Customers should be ordered by total spend"
    assert top_customer["Number_of_Orders"] > 0, "Should have orders"
    assert isinstance(top_customer["Total_Spend"], Decimal), "Should use Decimal for currency"

def test_time_based_queries(spark, sample_data):
    """Test time-based sales analysis queries"""
    # Setup
    orders_df = sample_data["orders"]
    customers_df = sample_data["customers"]
    products_df = sample_data["products"]
    enriched_orders = enrich_orders(orders_df, customers_df, products_df)
    enriched_orders.createOrReplaceTempView("enriched_orders")
    
    # Test monthly trends query
    monthly_trends = spark.sql("""
        SELECT 
            Order_Year,
            Order_Month,
            ROUND(SUM(Sales), 2) as Monthly_Sales,
            COUNT(DISTINCT `Customer ID`) as Active_Customers,
            ROUND(AVG(Sales), 2) as Avg_Transaction_Value
        FROM enriched_orders
        GROUP BY Order_Year, Order_Month
        ORDER BY Order_Year, Order_Month
    """)
    
    results = monthly_trends.collect()
    assert len(results) > 0, "Should have monthly trends data"
    
    # Verify temporal ordering
    for i in range(1, len(results)):
        curr, prev = results[i], results[i-1]
        assert (curr["Order_Year"] > prev["Order_Year"]) or \
               (curr["Order_Year"] == prev["Order_Year"] and 
                curr["Order_Month"] > prev["Order_Month"]), \
               "Results should be ordered chronologically"

def test_complex_metrics_query(spark, sample_data):
    """Test complex business metrics queries"""
    # Setup
    orders_df = sample_data["orders"]
    customers_df = sample_data["customers"]
    products_df = sample_data["products"]
    enriched_orders = enrich_orders(orders_df, customers_df, products_df)
    enriched_orders.createOrReplaceTempView("enriched_orders")
    
    # Test category performance metrics
    performance_metrics = spark.sql("""
        SELECT 
            Category,
            ROUND(SUM(Sales), 2) as Total_Sales,
            ROUND(SUM(Profit), 2) as Total_Profit,
            ROUND((SUM(Profit) / SUM(Sales)) * 100, 2) as Profit_Margin,
            COUNT(DISTINCT `Product ID`) as Product_Count,
            COUNT(DISTINCT `Customer ID`) as Customer_Count
        FROM enriched_orders
        GROUP BY Category
        HAVING SUM(Sales) > 0
        ORDER BY Profit_Margin DESC
    """)
    
    results = performance_metrics.collect()
    assert len(results) > 0, "Should have performance metrics"
    
    # Verify profit margin calculation
    for row in results:
        assert row["Profit_Margin"] <= 100, "Profit margin should be a valid percentage"
        assert isinstance(row["Total_Sales"], Decimal), "Should use Decimal for currency"
        assert row["Product_Count"] > 0, "Should have products"
        assert row["Customer_Count"] > 0, "Should have customers"