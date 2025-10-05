# E-commerce Data Processing Project

This project processes e-commerce data using PySpark, performing various transformations and analysis on customer, product, and order data.

## Project Structure

```
ecom_assignment/
├─ README.md
├─ data/                 # Input data files
│  ├─ Customer.xlsx      
│  ├─ Orders.json       
│  └─ Products.csv       
├─ logs/                 # Processing and ETL logs
│  └─ ecommerce_etl.log  
├─ notebooks/
│  ├─ 01_ingest_and_raw_tables.ipynb
│  └─ 02_transform_and_enrich.ipynb
├─ src/
│  ├─ __init__.py
│  ├─ config.py         # Configuration settings and constants
│  └─ processing.py     # Core data processing functions
├─ tests/
│  ├─ conftest.py       # Pytest fixtures and configurations
│  ├─ test_customers_products.py
│  ├─ test_enriched_orders.py
│  ├─ test_aggregations.py
│  ├─ test_config.py    
│  ├─ test_spark.py     
│  └─ test_sql_queries.py
└─ requirements.txt
```

## Setup

1. Create a virtual environment and activate it
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

## Running Tests

Run all tests with pytest:
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