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
    prod_cols = ["Product ID", "Product Name", "Category"]
    
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