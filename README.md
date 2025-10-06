# E-commerce Data Analytics Pipeline

> A comprehensive PySpark-based data processing pipeline for e-commerce analytics, built with Test-Driven Development principles and production-ready architecture.

---

## Project Overview

This project demonstrates a complete e-commerce data analytics solution that transforms raw business data into actionable insights. Using PySpark's distributed computing capabilities, we process customer orders, product information, and sales data to create a robust analytics platform suitable for executive reporting and business intelligence.

### What This Project Delivers

- **Multi-format Data Ingestion**: Seamlessly processes CSV, JSON, and Excel files
- **Customer Analytics**: Advanced segmentation and behavioral analysis
- **Product Performance**: Comprehensive product and category analytics  
- **Financial Intelligence**: Profit analysis and multi-dimensional aggregations
- **SQL-Ready Analytics**: Production-ready queries for business intelligence
- **Quality Assurance**: 33 automated tests ensuring data accuracy and reliability

---

## Project Architecture

```
ecom_assignment/
├── data/                    # Sample business data
│   ├── Customer.xlsx           # Customer information and demographics
│   ├── Orders.json            # Transaction records and order details
│   └── Products.csv           # Product catalog and categories
├── src/                    # Core business logic
│   ├── config.py              # Configuration and Spark settings
│   ├── processing.py          # Main data processing functions
│   └── __init__.py            # Package initialization
├── tests/                  # Comprehensive test suite (33 test methods)
│   ├── conftest.py            # Shared test configuration
│   ├── test_task1_raw_tables.py      # Raw data ingestion tests
│   ├── test_task2_enriched_tables.py # Customer & product analytics tests
│   ├── test_task3_enriched_orders.py # Order enrichment tests
│   ├── test_task4_profit_aggregations.py # Multi-dimensional aggregation tests
│   └── test_task5_sql_analysis.py    # SQL analytics validation tests
├── notebooks/              # Interactive demonstrations
│   ├── task1_raw_tables.ipynb        # Data ingestion walkthrough
│   ├── task2_enriched_tables.ipynb   # Customer & product analytics
│   ├── task3_enriched_orders.ipynb   # Order enrichment process
│   ├── task4_profit_aggregations.ipynb # Executive reporting
│   └── task5_sql_analysis.ipynb      # Advanced SQL analytics
├── logs/                   # Processing logs and monitoring
└── docs/                   # Project documentation
    ├── EXECUTION_SUMMARY.md   # Technical implementation summary
    └── TEST_COVERAGE_SUMMARY.md # Comprehensive testing documentation
```

---

## Quick Start Guide

### Prerequisites
- Python 3.8+ (Developed and tested with Python 3.13.7)
- Java 8 or 11 (required for PySpark)
- Minimum 4GB RAM (8GB recommended for larger datasets)

### 1. Environment Setup
```bash
# Clone the repository
git clone <repository-url>
cd ecom_assignment

# Create and activate virtual environment
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux  
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Verify Installation
```bash
# Run the complete test suite
python -m pytest tests/ -v

# Expected output: 33 tests should pass
# test_task1_raw_tables.py::5 methods PASSED
# test_task2_enriched_tables.py::6 methods PASSED  
# test_task3_enriched_orders.py::7 methods PASSED
# test_task4_profit_aggregations.py::7 methods PASSED
# test_task5_sql_analysis.py::8 methods PASSED
```

### 3. Explore the Pipeline
```bash
# Launch Jupyter to explore interactive notebooks
jupyter lab

# Navigate to notebooks/ directory and start with:
# 1. task1_raw_tables.ipynb - Data ingestion basics
# 2. task2_enriched_tables.ipynb - Customer analytics
# 3. task3_enriched_orders.ipynb - Order processing
# 4. task4_profit_aggregations.ipynb - Executive reporting  
```

---

## Business Use Cases & Analytics Pipeline

### Task 1: Multi-Format Data Ingestion
**Business Value**: Consolidate data from different business systems
- **CSV Processing**: Product catalogs and inventory data
- **JSON Processing**: Real-time order and transaction data
- **Excel Processing**: Customer demographics and contact information
- **Quality Assurance**: Automated schema validation and data quality checks

### Task 2: Customer & Product Intelligence  
**Business Value**: Advanced analytics for marketing and merchandising teams
- **Customer Segmentation**: High-value customer identification and behavioral analysis
- **Product Performance**: Best-seller identification and category analysis
- **Purchase Patterns**: Repeat customer detection and loyalty analysis
- **Business Insights**: Data-driven recommendations for customer retention

### Task 3: Order Enrichment & Processing
**Business Value**: Complete 360-degree view of business transactions
- **Data Integration**: Seamless joining of customer, product, and order data
- **Financial Accuracy**: Precise profit calculations with 2-decimal precision
- **Temporal Analysis**: Order date processing for trend analysis
- **Data Integrity**: Comprehensive validation ensuring no data loss during processing

### Task 4: Executive Reporting & Aggregations
**Business Value**: Strategic insights for executive decision-making
- **Multi-Dimensional Analysis**: Profit by Year, Category, Sub-Category, and Customer
- **Performance Trends**: Year-over-year growth and seasonal patterns
- **Market Intelligence**: Geographic performance and market penetration analysis
- **Executive Dashboards**: Ready-to-use metrics for board presentations

### Task 5: Advanced SQL Analytics
**Business Value**: Self-service analytics for business users
- **Complex Queries**: Window functions, cohort analysis, and statistical calculations
- **Business Intelligence**: Customer lifetime value and retention analysis
- **Cross-Selling Analysis**: Product affinity and market basket analysis  
- **Performance Monitoring**: Automated reporting and alerting capabilities

---

## Testing Philosophy & Quality Assurance

### Test-Driven Development Approach
We've implemented comprehensive Test-Driven Development (TDD) to ensure reliability and maintainability:

#### **Why TDD for Data Pipelines?**
- **Early Bug Detection**: Issues caught before reaching production
- **Refactoring Confidence**: Safe code improvements with test validation
- **Documentation**: Tests serve as living documentation of business requirements
- **Regression Prevention**: Automated validation prevents breaking changes

#### **Our Testing Strategy**
```
33 Total Test Methods across 5 Test Suites:
├── Data Quality Tests (8 methods) - Schema validation, null checks, data types
├── Business Logic Tests (12 methods) - Calculations, segmentation, classifications  
├── Integration Tests (7 methods) - End-to-end pipeline validation
├── Performance Tests (3 methods) - Memory usage, processing time validation
└── SQL Analytics Tests (3 methods) - Query accuracy and result validation
```

#### **Quality Metrics We Track**
-  **Data Accuracy**: Every calculation verified to 2+ decimal places
-  **Business Rule Compliance**: Real-world business constraints enforced
-  **Performance Standards**: Processing time and memory usage benchmarks
-  **Integration Integrity**: End-to-end pipeline validation

---

## 🔧 Technical Implementation Details

### PySpark Configuration
```python
# Optimized for both development and production environments
SPARK_CONFIG = {
    "spark.app.name": "EcommerceAnalytics",
    "spark.master": "local[*]",  # Uses all available cores
    "spark.sql.adaptive.enabled": "true",  # Adaptive query execution
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.execution.arrow.pyspark.enabled": "true"  # Faster pandas integration
}
```

### Key Design Decisions & Assumptions

#### **Data Processing Assumptions**
1. **Profit Calculations**: All profit values rounded to 2 decimal places for financial accuracy
2. **Customer Segmentation**: High-value customers defined as >$1000 total profit
3. **Product Classification**: Best-sellers defined as products ordered >2 times
4. **Date Handling**: Order dates processed as strings, converted to proper date types for analysis
5. **Data Quality**: Null values in critical fields (Customer ID, Product ID) treated as data quality issues

#### **Performance Optimizations**
1. **Lazy Evaluation**: Leverages PySpark's lazy evaluation for efficient processing
2. **Partition Strategy**: Data partitioned by year for optimal query performance
3. **Caching Strategy**: Frequently accessed DataFrames cached in memory
4. **Join Optimization**: Broadcast joins used for smaller dimension tables

#### **Business Logic Assumptions**
1. **Currency**: All monetary values assumed to be in USD
2. **Geographic Scope**: International customer base with country-level analysis
3. **Product Hierarchy**: Two-level category structure (Category > Sub-Category)
4. **Temporal Scope**: Analysis covers 2021-2023 with support for future years

---

##  Sample Data & Business Scenarios

### Customer Demographics
- **Geographic Diversity**: 10+ countries represented
- **Purchase Behavior**: Mix of one-time and repeat customers
- **Value Distribution**: Range from low-value to high-value customer segments

### Product Catalog  
- **Categories**: Technology and Furniture (expandable architecture)
- **Sub-Categories**: Computers, Mobile Devices, Office Furniture, etc.
- **Price Points**: Range from $100 to $5000+ for diverse market analysis

### Order Patterns
- **Seasonality**: Orders distributed across months for trend analysis
- **Volume**: Multi-year data for growth pattern analysis
- **Complexity**: Various quantity and profit scenarios for comprehensive testing

---

##  Production Deployment Considerations

### Scalability Features
- **Distributed Processing**: Built on PySpark for horizontal scaling
- **Memory Management**: Efficient DataFrame operations with garbage collection
- **Partition Strategy**: Year-based partitioning for optimal query performance
- **Resource Optimization**: Configurable Spark settings for different cluster sizes

### Monitoring & Observability
- **Logging Framework**: Comprehensive logging for debugging and monitoring
- **Data Quality Metrics**: Built-in validation with quality score tracking
- **Performance Metrics**: Processing time and resource utilization tracking
- **Error Handling**: Graceful error handling with detailed error reporting

### Security & Compliance
- **Data Privacy**: PII handling considerations in customer data processing
- **Access Control**: Role-based access patterns for different user types
- **Audit Trail**: Complete lineage tracking for compliance requirements
- **Data Retention**: Configurable retention policies for historical data

---

##  Additional Resources

### Documentation
- **[EXECUTION_SUMMARY.md](EXECUTION_SUMMARY.md)**: Detailed technical implementation summary
- **[TEST_COVERAGE_SUMMARY.md](TEST_COVERAGE_SUMMARY.md)**: Comprehensive testing documentation with business context

### Learning Path
1. Start with `notebooks/task1_raw_tables.ipynb` for data ingestion basics
2. Progress through tasks 2-5 for increasing complexity
3. Review test files to understand validation strategies
4. Explore source code in `src/` for implementation details

---

*Built with  using PySpark, pytest, and Test-Driven Development principles*

**Version**: 1.0.0 | **Last Updated**: October 2025 | **Python**: 3.13.7 | **PySpark**: 3.5.0
```bash
pytest -v
```

## Notebooks

1. `01_ingest_and_raw_tables.ipynb`: Data ingestion and initial processing
   - Load data from CSV, JSON, and Excel files
   - Apply schema validation
   - Create raw tables

2. `02_transform_and_enrich.ipynb`: Data transformation and analysis
   - Customer segmentation
   - Product performance analysis
   - Order enrichment
   - Aggregation and metrics

## Data Files

The project uses three main data files:

1. `Customer.xlsx`: Customer information
   - Demographics and customer details
   - Customer segmentation attributes
   - Contact information

2. `Orders.json`: Order transactions
   - Order details and timestamps
   - Customer purchase history
   - Product quantities and amounts

3. `Products.csv`: Product catalog
   - Product details and categories
   - Pricing information
   - Inventory data

## Data Processing

The main processing logic is distributed across the following files:

1. `src/config.py`:
   - Configuration settings
   - Data schema definitions
   - Environment variables

2. `src/processing.py`:
   - Customer metrics and segmentation
   - Product performance analysis
   - Order enrichment
   - Various aggregations

## Tests

The test suite covers various aspects of the data processing pipeline:

1. Core Functionality Tests:
   - `test_customers_products.py`: Customer segmentation and product metrics
   - `test_enriched_orders.py`: Order enrichment process
   - `test_aggregations.py`: Aggregation calculations
   - `test_sql_queries.py`: SQL query functionality

2. Infrastructure Tests:
   - `test_config.py`: Configuration settings validation
   - `test_spark.py`: Spark session and context management

3. Test Configuration:
   - `conftest.py`: Pytest fixtures and shared resources
   - Sample data generation
   - Spark session management