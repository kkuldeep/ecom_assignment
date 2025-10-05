import pytest
from src.processing import create_enriched_orders

def test_sql_queries(spark, sample_data):
    """Test SQL query functionality"""
    # Get sample data
    orders_df = sample_data["orders"]
    customers_df = sample_data["customers"]
    products_df = sample_data["products"]
    
    # Create enriched orders
    enriched_orders = create_enriched_orders(orders_df, customers_df, products_df)
    enriched_orders.createOrReplaceTempView("enriched_orders")
    
    # Test category sales query
    category_sales = spark.sql("""
        SELECT Category,
               round(sum(Sales), 2) as Total_Sales,
               count(distinct "Customer ID") as Unique_Customers
        FROM enriched_orders
        GROUP BY Category
        ORDER BY Total_Sales DESC
    """)
    
    result = category_sales.collect()
    electronics = next(x for x in result if x["Category"] == "Electronics")
    
    assert electronics["Total_Sales"] == 2499.98
    assert electronics["Unique_Customers"] == 1
    
    # Test customer rankings query
    customer_rankings = spark.sql("""
        SELECT Customer_Name,
               round(sum(Sales), 2) as Total_Spend,
               count(*) as Number_of_Orders,
               round(avg(Sales), 2) as Avg_Order_Value
        FROM enriched_orders
        GROUP BY Customer_Name
        ORDER BY Total_Spend DESC
    """)
    
    rankings = customer_rankings.collect()
    top_customer = rankings[0]
    
    assert top_customer["Customer_Name"] == "John Doe"
    assert top_customer["Total_Spend"] == 2499.98