# E-commerce Data Processing Project

This project processes e-commerce data using PySpark, performing various transformations and analysis on customer, product, and order data.

## Project Structure

```
ecom_assignment/
├─ README.md
├─ notebooks/
│  ├─ 01_ingest_and_raw_tables.ipynb
│  └─ 02_transform_and_enrich.ipynb
├─ src/
│  └─ processing.py
├─ tests/
│  ├─ conftest.py
│  ├─ test_customers_products.py
│  ├─ test_enriched_orders.py
│  ├─ test_aggregations.py
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

## Data Processing

The main processing logic is in `src/processing.py` and includes:
- Customer metrics and segmentation
- Product performance analysis
- Order enrichment
- Various aggregations

## Tests

- `test_customers_products.py`: Test customer segmentation and product metrics
- `test_enriched_orders.py`: Test order enrichment process
- `test_aggregations.py`: Test aggregation calculations
- `test_sql_queries.py`: Test SQL query functionality