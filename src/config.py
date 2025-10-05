"""
Configuration settings for the e-commerce data processing application
"""

class SparkConfig:
    """Spark configuration settings"""
    APP_NAME = "EcommerceDataProcessing"
    MASTER = "local[*]"
    
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

def get_spark_configs():
    """Helper function to get Spark configurations"""
    return SparkConfig.get_spark_configs()
