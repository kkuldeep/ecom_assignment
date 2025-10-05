import pytest
from pyspark.sql.functions import col
from src.processing import enrich_orders

# Test data
SAMPLE_ORDER = {
    'id': 'O1',
    'customer': 'John Doe',
    'category': 'Electronics',
    'month': 1,
    'year': 2023
}

@pytest.fixture
def enriched_df(sample_data):
    return enrich_orders(
        sample_data["orders"],
        sample_data["customers"],
        sample_data["products"]
    )

def test_order_enrichment(enriched_df):
    order = enriched_df.filter(
        col("Order ID") == SAMPLE_ORDER['id']
    ).collect()[0]
    
    assert order["Customer Name"] == SAMPLE_ORDER['customer']
    assert order["Category"] == SAMPLE_ORDER['category']

def test_date_components(enriched_df):
    order = enriched_df.filter(
        col("Order ID") == SAMPLE_ORDER['id']
    ).collect()[0]
    
    assert order["Order Month"] == SAMPLE_ORDER['month']
    assert order["Order Year"] == SAMPLE_ORDER['year']

def test_join_completeness(enriched_df, sample_data):
    # Check if we didn't lose any orders
    orig_count = sample_data["orders"].count()
    enriched_count = enriched_df.count()
    
    assert enriched_count == orig_count, "Order count mismatch after enrichment"