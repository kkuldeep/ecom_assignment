from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("TestEcommerce") \
    .master("local[1]") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Electronics", 100), ("Furniture", 200)]
df = spark.createDataFrame(data, ["Category", "Sales"])

# Try to collect the results
result = df.collect()
print("Test successful!")
print("Results:", result)

# Clean up
spark.stop()