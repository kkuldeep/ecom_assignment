"""
Configuration settings for the e-commerce data processing application
"""

class SparkConfig:
    """Spark configuration settings"""
    APP_NAME = "EcommerceDataProcessing"
    MASTER = "local[*]"
    DRIVER_MEMORY = "2g"
    EXECUTOR_MEMORY = "2g"
    
    @staticmethod
    def get_spark_configs():
        """Get all Spark configuration settings"""
        return {
            "spark.sql.legacy.timeParserPolicy": "LEGACY",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.sql.adaptive.enabled": "true",
            # Windows compatibility fixes
            "spark.sql.execution.arrow.pyspark.enabled": "false",
            "spark.python.worker.reuse": "false",
            "spark.executor.instances": "1",
            "spark.default.parallelism": "1",
            "spark.sql.shuffle.partitions": "1"
        }

class BusinessConfig:
    """Business logic configuration"""
    HIGH_VALUE_THRESHOLD = 10000
    MEDIUM_VALUE_THRESHOLD = 5000
    
    RECENT_ORDERS_DAYS = 30
    INACTIVE_CUSTOMER_DAYS = 90
    
    MIN_PROFIT_MARGIN = 15.0
    LOW_STOCK_THRESHOLD = 5

class LoggingConfig:
    """Logging configuration"""
    LOG_FILE = "logs/ecommerce_etl.log"
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    LOG_LEVEL = "INFO"
    MAX_MEMORY_USAGE = 0.8
    SLOW_QUERY_THRESHOLD = 10.0

def get_spark_configs():
    """Helper function to get Spark configurations"""
    return SparkConfig.get_spark_configs()

def validate_config():
    """Validate configuration settings"""
    _validate_memory_settings()
    _validate_business_thresholds()
    _validate_logging_settings()
    return True

def _validate_memory_settings():
    """Validate Spark memory configuration"""
    valid_suffixes = ('g', 'm')
    
    if not SparkConfig.DRIVER_MEMORY.endswith(valid_suffixes):
        raise ValueError("Invalid DRIVER_MEMORY format")
    
    if not SparkConfig.EXECUTOR_MEMORY.endswith(valid_suffixes):
        raise ValueError("Invalid EXECUTOR_MEMORY format")

def _validate_business_thresholds():
    """Validate business logic thresholds make sense"""
    if BusinessConfig.HIGH_VALUE_THRESHOLD <= BusinessConfig.MEDIUM_VALUE_THRESHOLD:
        raise ValueError("HIGH_VALUE_THRESHOLD must be greater than MEDIUM_VALUE_THRESHOLD")

def _validate_logging_settings():
    """Validate logging configuration values"""
    if not (0 < LoggingConfig.MAX_MEMORY_USAGE <= 1):
        raise ValueError("MAX_MEMORY_USAGE must be between 0 and 1")
    
    if LoggingConfig.SLOW_QUERY_THRESHOLD <= 0:
        raise ValueError("SLOW_QUERY_THRESHOLD must be positive")
