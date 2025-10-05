import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, year, month
from pyspark.sql.functions import round as spark_round, when, current_date, datediff
from pyspark.sql.window import Window
from pyspark.sql.types import *
from .config import SparkConfig, BusinessConfig, LoggingConfig, get_spark_configs

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=LoggingConfig.LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def init_spark(name=SparkConfig.APP_NAME):
    spark = SparkSession.builder.appName(name).master(SparkConfig.MASTER)
    configs = get_spark_configs()
    for k, v in configs.items():
        spark = spark.config(k, v)
    return spark.getOrCreate()

def get_customer_metrics(orders_df, customers_df):
    # Get last order dates
    last_order_dates = orders_df.groupBy("Customer ID").agg(
        {"Order Date": "max"}
    ).withColumnRenamed("max(Order Date)", "Last Order Date")
    
    # Calculate days since last order
    last_order_dates = last_order_dates.withColumn(
        "Days Since Order",
        datediff(current_date(), col("Last Order Date"))
    )
    
    # Calculate base metrics
    base = orders_df.join(customers_df, "Customer ID")
    stats = base.groupBy("Customer ID", "Customer Name", "Country").agg(
        count("Order ID").alias("Total Orders"),
        spark_round(sum("Sales"), 2).alias("Total Sales"),
        spark_round(avg("Sales"), 2).alias("Average Order Value"),
        spark_round(sum("Profit"), 2).alias("Total Profit")
    )
    
    # Join metrics with last order dates
    results = stats.join(last_order_dates, "Customer ID")
    
    # Add customer value segment
    results = results.withColumn(
        "Customer Segment",
        when(col("Total Sales") > BusinessConfig.HIGH_VALUE_THRESHOLD, "High Value")
        .when(col("Total Sales") > BusinessConfig.MEDIUM_VALUE_THRESHOLD, "Medium Value")
        .otherwise("Regular")
    )
    
    # Add activity status
    return results.withColumn(
        "Activity Status",
        when(col("Days Since Order") <= BusinessConfig.RECENT_ORDERS_DAYS, "Active")
        .when(col("Days Since Order") > BusinessConfig.INACTIVE_CUSTOMER_DAYS, "Inactive")
        .otherwise("At Risk")
    )

def analyze_product_performance(orders_df, products_df):
    # Join orders with products
    df = orders_df.join(products_df, "Product ID")
    group_cols = ["Product ID", "Product Name", "Category", "Sub-Category"]
    
    # Get base metrics
    metrics = df.groupBy(group_cols).agg(
        count("Order ID").alias("Total Orders"),
        sum("Quantity").alias("Units Sold"),
        spark_round(sum("Sales"), 2).alias("Total Sales"),
        spark_round(sum("Profit"), 2).alias("Total Profit")
    )
    
    # Add margin calculation
    with_margin = metrics.withColumn(
        "Profit Margin",
        spark_round(col("Total Profit") / col("Total Sales") * 100, 2)
    )
    
    # Add performance flags
    return with_margin.withColumn(
        "Performance Flag",
        when(col("Profit Margin") < BusinessConfig.MIN_PROFIT_MARGIN, "Low Margin")
        .when(col("Units Sold") < BusinessConfig.LOW_STOCK_THRESHOLD, "Low Stock")
        .otherwise("Normal")
    )

def enrich_orders(orders, customers, products):
    # Select required fields
    cust_cols = ["Customer ID", "Customer Name", "Country"]
    prod_cols = ["Product ID", "Product Name", "Category", "Sub-Category"]  # Added Sub-Category
    
    # Join with customer data
    df = orders.join(
        customers.select(cust_cols), 
        "Customer ID"
    )
    
    # Join with product data
    df = df.join(
        products.select(prod_cols),
        "Product ID"
    )
    
    # Add date components
    df = df.withColumn("Order Month", month("Order Date"))
    df = df.withColumn("Order Year", year("Order Date"))
    
    # Round Profit to 2 decimal places
    df = df.withColumn("Profit", spark_round(col("Profit"), 2))
    
    # Ensure all required columns are present
    required_columns = [
        "Order ID", "Customer ID", "Customer Name", "Country",
        "Product ID", "Product Name", "Category", "Sub-Category",
        "Order Date", "Order Year", "Order Month",
        "Quantity", "Sales", "Profit"
    ]
    
    # Verify all required columns exist
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    return df

def get_sales_aggregations(orders):
    logger.info("Starting sales aggregations calculation")
    
    try:
        # Calculate monthly sales
        monthly = orders.groupBy(
            "Order Year", 
            "Order Month", 
            "Category"
        ).agg(
            spark_round(sum("Sales"), 2).alias("Monthly Sales"),
            spark_round(sum("Profit"), 2).alias("Monthly Profit")
        )
        
        # Calculate customer patterns
        customer = orders.groupBy(
            "Customer ID",
            "Customer Name" 
        ).agg(
            count("Order ID").alias("Number of Orders"),
            spark_round(avg("Sales"), 2).alias("Average Order Value"), 
            spark_round(sum("Sales"), 2).alias("Total Spend")
        )
        
        logger.info("Aggregations completed successfully")
        return monthly, customer
        
    except Exception as e:
        logger.error(f"Error in sales aggregations: {str(e)}")
        raise

def get_profit_aggregations(orders):
    """
    Creates the required profit aggregation table for Task 4.
    Aggregates profit by Year, Product Category, Product Sub-Category, and Customer.
    
    Args:
        orders: Enriched orders DataFrame containing all required columns
        
    Returns:
        DataFrame with profit aggregations containing columns:
        - Order Year
        - Category
        - Sub-Category
        - Customer Name
        - Total Profit (rounded to 2 decimals)
        - Total Sales (rounded to 2 decimals)
        - Order Count
        - Profit Margin % (rounded to 2 decimals)
    """
    logger.info("Starting profit aggregations calculation")
    
    try:
        # Verify required columns exist
        required_cols = [
            "Order Year", "Category", "Sub-Category", "Customer Name",
            "Order ID", "Sales", "Profit"
        ]
        missing_cols = [col for col in required_cols if col not in orders.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns for profit aggregation: {missing_cols}")
        
        # Group by required dimensions
        profit_aggs = orders.groupBy(
            "Order Year",
            "Category",
            "Sub-Category",
            "Customer Name"
        ).agg(
            # Profit metrics
            spark_round(sum("Profit"), 2).alias("Total Profit"),
            spark_round(sum("Sales"), 2).alias("Total Sales"),
            count("Order ID").alias("Order Count")
        )
        
        # Calculate profit margin percentage
        profit_aggs = profit_aggs.withColumn(
            "Profit Margin",
            spark_round((col("Total Profit") / col("Total Sales")) * 100, 2)
        )
        
        # Sort by year and profit
        profit_aggs = profit_aggs.orderBy(
            "Order Year",
            "Category",
            "Sub-Category",
            col("Total Profit").desc()
        )
        
        logger.info("Profit aggregations completed successfully")
        return profit_aggs
        
    except Exception as e:
        logger.error(f"Error in profit aggregations: {str(e)}")
        raise

def get_sql_profit_analysis(spark, enriched_orders):
    """
    Executes the required SQL queries for Task 5 profit analysis.
    
    Args:
        spark: SparkSession object
        enriched_orders: Enriched orders DataFrame containing required columns
        
    Returns:
        Dictionary containing results of all four required profit analysis queries:
        - yearly_profit: Profit by Year
        - category_profit: Profit by Year and Product Category
        - customer_profit: Profit by Customer
        - customer_yearly_profit: Profit by Customer and Year
    """
    logger.info("Starting SQL profit analysis")
    
    try:
        # Register the DataFrame as a temp view
        enriched_orders.createOrReplaceTempView("enriched_orders")
        
        # 1. Profit by Year
        yearly_profit = spark.sql("""
            SELECT 
                Order_Year,
                ROUND(SUM(Profit), 2) as Total_Profit,
                ROUND(SUM(Sales), 2) as Total_Sales,
                COUNT(DISTINCT `Order ID`) as Order_Count,
                ROUND((SUM(Profit) / SUM(Sales)) * 100, 2) as Profit_Margin
            FROM enriched_orders
            GROUP BY Order_Year
            ORDER BY Order_Year
        """)
        
        # 2. Profit by Year and Product Category
        category_profit = spark.sql("""
            SELECT 
                Order_Year,
                Category,
                ROUND(SUM(Profit), 2) as Total_Profit,
                ROUND(SUM(Sales), 2) as Total_Sales,
                COUNT(DISTINCT `Order ID`) as Order_Count,
                ROUND((SUM(Profit) / SUM(Sales)) * 100, 2) as Profit_Margin
            FROM enriched_orders
            GROUP BY Order_Year, Category
            ORDER BY Order_Year, Total_Profit DESC
        """)
        
        # 3. Profit by Customer
        customer_profit = spark.sql("""
            SELECT 
                Customer_Name,
                ROUND(SUM(Profit), 2) as Total_Profit,
                ROUND(SUM(Sales), 2) as Total_Sales,
                COUNT(DISTINCT `Order ID`) as Order_Count,
                ROUND(AVG(Sales), 2) as Avg_Order_Value,
                ROUND((SUM(Profit) / SUM(Sales)) * 100, 2) as Profit_Margin
            FROM enriched_orders
            GROUP BY Customer_Name
            ORDER BY Total_Profit DESC
        """)
        
        # 4. Profit by Customer and Year
        customer_yearly_profit = spark.sql("""
            SELECT 
                Customer_Name,
                Order_Year,
                ROUND(SUM(Profit), 2) as Total_Profit,
                ROUND(SUM(Sales), 2) as Total_Sales,
                COUNT(DISTINCT `Order ID`) as Order_Count,
                ROUND((SUM(Profit) / SUM(Sales)) * 100, 2) as Profit_Margin
            FROM enriched_orders
            GROUP BY Customer_Name, Order_Year
            ORDER BY Customer_Name, Order_Year
        """)
        
        logger.info("SQL profit analysis completed successfully")
        
        return {
            "yearly_profit": yearly_profit,
            "category_profit": category_profit,
            "customer_profit": customer_profit,
            "customer_yearly_profit": customer_yearly_profit
        }
        
    except Exception as e:
        logger.error(f"Error in SQL profit analysis: {str(e)}")
        raise