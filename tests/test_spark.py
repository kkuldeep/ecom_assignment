import pytest
from pyspark.sql import SparkSession
from src.processing import init_spark
from src.config import SparkConfig

@pytest.fixture(scope="session")
def spark():
    """Create a test Spark session"""
    session = init_spark("TestEcommerce")
    yield session
    session.stop()

def test_spark_session_creation():
    # Test basic session creation
    spark = init_spark("TestApp")
    try:
        assert spark.conf.get("spark.app.name") == "TestApp"
        df = spark.createDataFrame([(1,)], ["id"])
        assert df.count() == 1
    finally:
        spark.stop()

def test_spark_memory_settings():
    spark = init_spark()
    try:
        driver_mem = spark.conf.get("spark.driver.memory")
        exec_mem = spark.conf.get("spark.executor.memory")
        
        assert any(x in driver_mem for x in ['g', 'm']), \
            "Invalid driver memory format"
        assert any(x in exec_mem for x in ['g', 'm']), \
            "Invalid executor memory format"
    finally:
        spark.stop()

def test_spark_sql_config():
    spark = init_spark()
    try:
        # Check key SQL configs
        partitions = spark.conf.get("spark.sql.shuffle.partitions")
        assert partitions.isdigit(), "Invalid partition setting"
        assert int(partitions) == SparkConfig.SHUFFLE_PARTITIONS
        
        # Verify adaptive query execution
        assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
    finally:
        spark.stop()