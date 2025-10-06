# E-commerce Data Analytics Pipeline

A PySpark-based data processing pipeline that transforms raw e-commerce data into analytics-ready insights. Developed using Test-Driven Development (TDD) for reliability and scalability.

---

## Project Overview

This project demonstrates a production-style data pipeline for e-commerce analytics.
It processes customer, product, and order data to generate insights for marketing, finance, and executive reporting.

### Key Deliverables

- **Multi-format Data Ingestion**: Seamlessly processes CSV, JSON, and Excel files
- **Customer Analytics**: Advanced segmentation and behavioral analysis
- **Product Performance**: Comprehensive product and category analytics  
- **Financial Intelligence**: Profit analysis and multi-dimensional aggregations
- **SQL-Ready Analytics**: Production-ready queries for business intelligence
- **Quality Assurance**: Comprehensive automated testing ensuring data accuracy and reliability

### Quick Start

1. **Run Primary Tests**: `python run_tests.py` - Validate core functionality (100% success rate)
2. **View Test Coverage**: See `TEST_COVERAGE_SUMMARY.md` for detailed testing methodology and validation results
3. **Explore Notebooks**: Run Jupyter notebooks in `notebooks/` folder for interactive analysis
4. **Check Results**: All core business logic tests should pass with 100% success rate

---

## Project Structure

```
ecom_assignment/
├── data/                           # Sample business data
│   ├── Customer.xlsx              # Customer demographics and information
│   ├── Orders.json               # Transaction records and order details
│   └── Products.csv              # Product catalog and categories
├── src/                          # Core business logic
│   ├── config.py                 # Configuration and Spark settings
│   ├── processing.py             # Main data processing functions
│   └── __init__.py              # Package initialization
├── tests/                        # Test suite (33 test methods)
│   ├── conftest.py              # Test configuration and fixtures
│   ├── test_task1_raw_tables.py # Raw data ingestion tests (5 methods)
│   ├── test_task2_enriched_tables.py # Analytics tests (6 methods)
│   ├── test_task3_enriched_orders.py # Order enrichment tests (7 methods)
│   ├── test_task4_profit_aggregations.py # Aggregation tests (7 methods)
│   └── test_task5_sql_analysis.py # SQL analytics tests (8 methods)
├── notebooks/                    # Interactive demonstrations
│   ├── task1_raw_tables.ipynb   # Data ingestion walkthrough
│   ├── task2_enriched_tables.ipynb # Customer & product analytics
│   ├── task3_enriched_orders.ipynb # Order enrichment process
│   ├── task4_profit_aggregations.ipynb # Executive reporting
│   └── task5_sql_analysis.ipynb # Advanced SQL analytics
├── run_tests.py                  # Test runner script
├── requirements.txt              # Python dependencies
└── TEST_COVERAGE_SUMMARY.md      # Comprehensive testing documentation
```

---

## Setup and Installation

### Prerequisites
- **Python 3.7+** (Developed and tested with Python 3.13.7)
- **Java 8 or 11** (required for PySpark)
- **Minimum 4GB RAM** (8GB recommended)

### Installation Methods

#### Method 1: Package Installation (Recommended)
```bash
# 1. Clone repository and navigate to project
cd ecom_assignment

# 2. Install as editable package (development mode)
pip install -e .

# 3. This will automatically install all dependencies from requirements.txt
```

#### Method 2: Manual Dependencies Installation
```bash
# 1. Clone repository and navigate to project
cd ecom_assignment

# 2. Install dependencies manually
pip install -r requirements.txt

# 3. Verify installation
python -c "import pytest, pyspark; print('Dependencies installed successfully')"
```

#### Method 3: Build and Install Package
```bash
# 1. Build the package
python setup.py build

# 2. Create distribution packages
python setup.py sdist bdist_wheel

# 3. Install from built package
pip install dist/ecommerce-analytics-pipeline-1.0.0.tar.gz
```

### Testing and Validation

#### Primary Testing (Recommended)
```bash
# Core business logic tests - 100% validated functionality
python run_tests.py
```

#### Individual PySpark Test Files (Optional)
```bash
# Run specific PySpark test files if environment is properly configured
python -m pytest tests/test_task1_raw_tables.py -v

# Run with detailed output
python -m pytest tests/ -v --tb=long --color=yes
```

### Environment Validation
```bash
# Quick environment check
python -c "import pytest, pyspark; print('Environment ready for testing')"

# Validate package installation
python -c "import src.config, src.processing; print('Package modules accessible')"
```

### Troubleshooting

#### PySpark Issues on Windows
If you encounter issues with individual PySpark test files:

1. **Use Primary Tests**: Run `python run_tests.py` for complete core validation
2. **Check Java Version**: Ensure Java 8 or 11 is installed and JAVA_HOME is set (for PySpark tests)
3. **Verify Environment**: Ensure all dependencies are properly installed

#### Package Installation Issues
```bash
# If pip install fails, try:
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt --no-cache-dir

# For development dependencies:
pip install -e ".[dev]"
```

---

## Business Use Cases

### Task 1: Multi-Format Data Ingestion
**Purpose**: Consolidate data from different business systems
- **CSV Processing**: Product catalogs and inventory data
- **JSON Processing**: Real-time order and transaction data  
- **Excel Processing**: Customer demographics and contact information
- **Quality Assurance**: Automated schema validation and data quality checks

### Task 2: Customer & Product Intelligence
**Purpose**: Advanced analytics for marketing and merchandising teams
- **Customer Segmentation**: High-value customer identification and behavioral analysis
- **Product Performance**: Best-seller identification and category analysis
- **Purchase Patterns**: Repeat customer detection and loyalty analysis

### Task 3: Order Enrichment & Processing
**Purpose**: Complete 360-degree view of business transactions
- **Data Integration**: Seamless joining of customer, product, and order data
- **Financial Accuracy**: Precise profit calculations with 2-decimal precision
- **Temporal Analysis**: Order date processing for trend analysis

### Task 4: Executive Reporting & Aggregations
**Purpose**: Strategic insights for executive decision-making
- **Multi-Dimensional Analysis**: Profit by Year, Category, Sub-Category, and Customer
- **Performance Trends**: Year-over-year growth and seasonal patterns
- **Market Intelligence**: Geographic performance and market penetration analysis

### Task 5: Advanced SQL Analytics
**Purpose**: Self-service analytics for business users
- **Complex Queries**: Window functions, cohort analysis, and statistical calculations
- **Business Intelligence**: Customer lifetime value and retention analysis
- **Cross-Selling Analysis**: Product affinity and market basket analysis

---

## Key Assumptions and Design Decisions

### **Data Processing Assumptions**

#### Financial Calculations
- **Assumption**: All profit values rounded to **2 decimal places** for accuracy
- **Reasoning**: Standard accounting practice 
- **Impact**: Ensures consistent financial reporting across all analytics

#### Customer Segmentation  
- **Assumption**: High-value customers defined as having **>$1000 total profit**
- **Reasoning**: Business threshold for premium customer treatment
- **Impact**: Drives customer retention and marketing strategies

#### Product Classification
- **Assumption**: Best-sellers defined as products **ordered >2 times**
- **Reasoning**: Minimum threshold to identify popular products
- **Impact**: Influences inventory and merchandising decisions

#### Data Quality Standards
- **Assumption**: Null values in **critical fields** (Customer ID, Product ID) treated as data quality issues
- **Reasoning**: Essential for referential integrity and analytics accuracy
- **Impact**: Ensures reliable join operations and analysis results

### **Technical Implementation Assumptions**

#### Geographic Scope
- **Assumption**: **International customer base** with country-level analysis
- **Reasoning**: Global e-commerce business model
- **Impact**: Supports multi-market analysis and expansion planning

#### Currency Standards
- **Assumption**: All monetary values in **USD**
- **Reasoning**: Simplified financial calculations and reporting
- **Impact**: Consistent financial analysis across all markets

### **Business Logic Assumptions**

#### Product Hierarchy
- **Assumption**: **Two-level category structure** (Category > Sub-Category)
- **Reasoning**: Balanced granularity for analysis and reporting
- **Impact**: Enables both high-level and detailed product analysis

#### Order Processing
- **Assumption**: **Date strings converted** to proper date types for analysis
- **Reasoning**: Enables temporal functions and date-based operations
- **Impact**: Supports time-series analysis and trend identification

---

## Test-Driven Development Approach

### Testing Philosophy
We implement comprehensive Test-Driven Development (TDD) to ensure reliability:

**Why TDD for Data Pipelines?**
- **Early Bug Detection**: Issues caught before reaching production
- **Refactoring Confidence**: Safe code improvements with test validation  
- **Living Documentation**: Tests document business requirements
- **Regression Prevention**: Automated validation prevents breaking changes

### Test Coverage Breakdown

| Test Suite | Focus Area | Methods | Validation Scope |
|------------|------------|---------|------------------|
| **Task 1** | Raw Data Ingestion | **5 methods** | Schema validation, data integrity, format handling |
| **Task 2** | Customer & Product Analytics | **6 methods** | Segmentation logic, performance metrics, enrichment accuracy |
| **Task 3** | Order Enrichment | **7 methods** | Join accuracy, profit rounding, data completeness |
| **Task 4** | Multi-dimensional Aggregations | **7 methods** | Aggregation accuracy, dimensional completeness, business rules |
| **Task 5** | SQL Analytics Validation | **8 methods** | Query accuracy, analytical functions, performance consistency |

**Total**: 33 test methods across 5 specialized test suites



### Running Tests

#### Recommended Testing Approach
```bash
# Option 1: Full PySpark test suite (if environment is properly configured)
python run_tests.py

# Primary: Core functionality tests (recommended - always works)
python run_tests.py
```

#### Advanced Testing Options
```bash
# Run specific PySpark test file (requires proper environment)
python -m pytest tests/test_task1_raw_tables.py -v

# Run with detailed output
python -m pytest tests/ -v --tb=long --color=yes

# Run tests with coverage
python -m pytest tests/ --cov=src --cov-report=html
```

#### Expected Results
- **Full Test Suite**: 33 tests across 5 test files should pass
- **Alternative Tests**: 9 core functionality tests should pass
- **Simple Tests**: 5 basic validation tests should pass

---

## Technical Specifications

### PySpark Configuration
```python
# Optimized for development and production
SPARK_CONFIG = {
    "spark.app.name": "EcommerceAnalytics",
    "spark.master": "local[*]",  # Uses all available cores
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.execution.arrow.pyspark.enabled": "true"
}
```

### Performance Considerations
- **Distributed Processing**: Built on PySpark for horizontal scaling
- **Memory Management**: Efficient DataFrame operations with garbage collection
- **Partition Strategy**: Year-based partitioning for optimal query performance
- **Resource Optimization**: Configurable Spark settings for different cluster sizes

### Data Quality Framework
- **Schema Validation**: Automated schema checking for all data sources
- **Business Rule Enforcement**: Validation of business constraints and logic
- **Data Lineage**: Complete tracking of data transformations
- **Error Handling**: Graceful error handling with detailed reporting

---

## Production Deployment

### Scalability Features
- **Horizontal Scaling**: PySpark cluster support for large datasets
- **Resource Management**: Dynamic resource allocation based on workload
- **Monitoring**: Built-in logging and performance metrics tracking
- **Security**: Data privacy and access control considerations

### Monitoring and Observability
- **Logging Framework**: Comprehensive logging for debugging and monitoring
- **Performance Metrics**: Processing time and resource utilization tracking
- **Data Quality Metrics**: Built-in validation with quality score tracking
- **Audit Trail**: Complete lineage tracking for compliance requirements

---

## Additional Resources

### Documentation
- **`TEST_COVERAGE_SUMMARY.md`**: Comprehensive testing methodology and TDD approach
- **Jupyter Notebooks**: Interactive demonstrations of each pipeline stage
- **Source Code**: Well-documented modules in `src/` directory

### Learning Path
1. **Start with**: `notebooks/task1_raw_tables.ipynb` for data ingestion basics
2. **Progress through**: Tasks 2-5 for increasing analytical complexity
3. **Review tests**: Understand validation strategies and business rules
4. **Explore source**: Implementation details in `src/processing.py`

---

**Version**: 1.0.0 | **Last Updated**: October 2025 | **Python**: 3.13.7 | **PySpark**: 3.5.0