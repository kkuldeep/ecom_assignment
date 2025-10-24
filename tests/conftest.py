import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timedelta

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing - Python 3.13 compatible"""
    from src.config import get_spark_configs
    
    builder = SparkSession.builder \
        .appName("EcommerceTest") \
        .master("local[1]")
    
    # Add Windows + Python 3.13 compatible configs
    try:
        configs = get_spark_configs()
        for key, value in configs.items():
            builder = builder.config(key, value)
    except:
        # Fallback configs if import fails
        builder = builder \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.python.worker.reuse", "false")
    
    return builder.getOrCreate()

@pytest.fixture(scope="session")
def sample_data(spark):
    """Create sample data for testing"""
    # Customer data
    customer_data = [
        (1, "John Doe", "USA"),
        (2, "Jane Smith", "UK"),
        (3, "Bob Wilson", "Canada")
    ]
    customer_schema = StructType([
        StructField("Customer ID", IntegerType(), False),
        StructField("Customer Name", StringType(), False),
        StructField("Country", StringType(), False)
    ])
    customers_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Product data
    product_data = [
        (101, "Laptop", "Electronics", "Computers"),
        (102, "Desk Chair", "Furniture", "Office"),
        (103, "Printer", "Electronics", "Peripherals")
    ]
    product_schema = StructType([
        StructField("Product ID", IntegerType(), False),
        StructField("Product Name", StringType(), False),
        StructField("Category", StringType(), False),
        StructField("Sub-Category", StringType(), False)
    ])
    products_df = spark.createDataFrame(product_data, product_schema)
    
    # Order data
    current_date = datetime.now()
    order_data = [
        (1001, 1, 101, current_date, 2, 1200.00, 240.00),
        (1002, 2, 102, current_date - timedelta(days=45), 1, 300.00, 75.00),
        (1003, 3, 103, current_date - timedelta(days=15), 1, 450.00, 90.00),
        (1004, 1, 103, current_date - timedelta(days=60), 3, 1350.00, 270.00)
    ]
    order_schema = StructType([
        StructField("Order ID", IntegerType(), False),
        StructField("Customer ID", IntegerType(), False),
        StructField("Product ID", IntegerType(), False),
        StructField("Order Date", TimestampType(), False),
        StructField("Quantity", IntegerType(), False),
        StructField("Sales", DoubleType(), False),
        StructField("Profit", DoubleType(), False)
    ])
    orders_df = spark.createDataFrame(order_data, order_schema)
    
    return {
        "customers": customers_df,
        "products": products_df,
        "orders": orders_df
    }

@pytest.fixture(scope="session")
def actual_data(spark):
    """Load actual data files for integration testing"""
    try:
        # Try to load actual files
        customers_df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("data/Customer.xlsx")
        
        orders_df = spark.read \
            .option("multiline", "true") \
            .json("data/Orders.json")
        
        products_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("data/Products.csv")
        
        return {
            "customers": customers_df,
            "products": products_df,
            "orders": orders_df
        }
    except Exception as e:
        pytest.skip(f"Actual data files not available: {e}")
