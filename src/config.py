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
            "spark.sql.adaptive.enabled": "true"
        }

class BusinessConfig:
    """Business logic configuration"""
    # Customer segmentation thresholds
    HIGH_VALUE_THRESHOLD = 10000  # High value customer threshold
    MEDIUM_VALUE_THRESHOLD = 5000  # Medium value customer threshold
    
    # Activity status thresholds (in days)
    RECENT_ORDERS_DAYS = 30       # Active customer threshold
    INACTIVE_CUSTOMER_DAYS = 90   # Inactive customer threshold
    
    # Product performance thresholds
    MIN_PROFIT_MARGIN = 15.0      # Minimum acceptable profit margin
    LOW_STOCK_THRESHOLD = 5       # Low stock warning threshold

class LoggingConfig:
    """Logging configuration"""
    LOG_FILE = "../logs/ecommerce_etl.log"
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    LOG_LEVEL = "INFO"
    MAX_MEMORY_USAGE = 0.8      # 80% memory usage threshold
    SLOW_QUERY_THRESHOLD = 10.0 # Slow query threshold in seconds

def get_spark_configs():
    """Helper function to get Spark configurations"""
    return SparkConfig.get_spark_configs()

def validate_config():
    """Validate configuration settings"""
    # Validate memory settings
    if not SparkConfig.DRIVER_MEMORY.endswith('g') and not SparkConfig.DRIVER_MEMORY.endswith('m'):
        raise ValueError("Invalid DRIVER_MEMORY format")
    
    if not SparkConfig.EXECUTOR_MEMORY.endswith('g') and not SparkConfig.EXECUTOR_MEMORY.endswith('m'):
        raise ValueError("Invalid EXECUTOR_MEMORY format")
    
    # Validate thresholds
    if BusinessConfig.HIGH_VALUE_THRESHOLD <= BusinessConfig.MEDIUM_VALUE_THRESHOLD:
        raise ValueError("HIGH_VALUE_THRESHOLD must be greater than MEDIUM_VALUE_THRESHOLD")
    
    # Validate logging
    if LoggingConfig.MAX_MEMORY_USAGE <= 0 or LoggingConfig.MAX_MEMORY_USAGE > 1:
        raise ValueError("MAX_MEMORY_USAGE must be between 0 and 1")
    
    if LoggingConfig.SLOW_QUERY_THRESHOLD <= 0:
        raise ValueError("SLOW_QUERY_THRESHOLD must be positive")
    
    return True
