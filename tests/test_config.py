import pytest
from src.config import *

# Test data
MEMORY_SETTINGS = ['spark.driver.memory', 'spark.executor.memory']
DATA_TYPES = ['products', 'customers', 'orders']
FILE_EXTENSIONS = {
    'products': '.csv',
    'customers': '.xlsx',
    'orders': '.json'
}

def test_spark_memory_config():
    cfg = get_spark_configs()
    for setting in MEMORY_SETTINGS:
        assert setting in cfg, f"Missing {setting} in Spark config"
        assert cfg[setting][-1] in ['g', 'm'], f"Invalid memory format for {setting}"

def test_spark_performance_settings():
    cfg = get_spark_configs()
    assert 'spark.sql.shuffle.partitions' in cfg
    
    partitions = SparkConfig.SHUFFLE_PARTITIONS
    assert isinstance(partitions, int)
    assert partitions > 0, "Shuffle partitions must be positive"

def test_data_file_paths():
    for dtype in DATA_TYPES:
        path = get_data_path(dtype)
        assert path.endswith(FILE_EXTENSIONS[dtype])

def test_data_path_invalid_type():
    with pytest.raises(KeyError):
        get_data_path('nonexistent_type')

def test_business_value_thresholds():
    assert BusinessConfig.HIGH_VALUE_THRESHOLD > BusinessConfig.MEDIUM_VALUE_THRESHOLD, \
        "High value threshold must be greater than medium value threshold"
    assert BusinessConfig.ROUND_DECIMALS in [2, 3, 4], \
        "Decimal places should be between 2-4"

def test_business_timing_rules():
    recent = BusinessConfig.RECENT_ORDERS_DAYS
    inactive = BusinessConfig.INACTIVE_CUSTOMER_DAYS
    
    assert recent > 0, "Recent orders days must be positive"
    assert inactive > recent, "Inactive threshold must be greater than recent orders"

def test_data_quality_settings():
    # Check thresholds
    assert DataConfig.MAX_NULL_PERCENTAGE <= 0.1, \
        "Null threshold too high"
    assert DataConfig.MAX_DUPLICATE_PERCENTAGE <= 0.05, \
        "Duplicate threshold too high"

def test_required_columns_exist():
    cols = DataConfig.REQUIRED_COLUMNS
    for dtype in DATA_TYPES:
        assert dtype in cols, f"Missing required columns for {dtype}"
        assert len(cols[dtype]) > 0, f"No columns defined for {dtype}"

def test_logging_setup():
    fmt = LoggingConfig.LOG_FORMAT
    assert all(x in fmt for x in ['%(asctime)s', '%(levelname)s'])
    assert LoggingConfig.LOG_LEVEL in ['DEBUG', 'INFO', 'WARNING', 'ERROR']

@pytest.mark.parametrize("threshold,name", [
    (LoggingConfig.MAX_MEMORY_USAGE, "memory usage"),
    (LoggingConfig.SLOW_QUERY_THRESHOLD, "query threshold")
])
def test_performance_thresholds(threshold, name):
    assert threshold > 0, f"Invalid {name}"
    if name == "memory usage":
        assert threshold <= 1, "Memory usage must be <= 100%"

def test_validate_good_config():
    try:
        validate_config()
    except Exception as e:
        pytest.fail(f"Valid config failed: {str(e)}")

def test_validate_bad_config():
    old_mem = SparkConfig.DRIVER_MEMORY
    SparkConfig.DRIVER_MEMORY = "bad_value"
    
    try:
        with pytest.raises(ValueError):
            validate_config()
    finally:
        SparkConfig.DRIVER_MEMORY = old_mem