"""Pytest configuration for E-Commerce Data Pipelines tests."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

@pytest.fixture
def sample_product_csv():
    """Sample CSV content for testing."""
    return """sku,name,price,regular_price,sale_price,stock_quantity,manage_stock,stock_status,categories,tags
SKU-001,Widget A,29.99,39.99,29.99,100,true,instock,Electronics|Gadgets,new|featured
SKU-002,Widget B,19.99,19.99,,50,true,instock,Electronics,,
SKU-003,Widget C,9.99,14.99,9.99,0,false,outofstock,Gadgets,sale
"""


@pytest.fixture
def sample_product_json():
    """Sample JSON content for testing."""
    return """[
    {
        "sku": "SKU-001",
        "name": "Widget A",
        "price": "29.99",
        "regular_price": "39.99",
        "sale_price": "29.99",
        "stock_quantity": 100,
        "manage_stock": true,
        "stock_status": "instock",
        "categories": ["Electronics", "Gadgets"],
        "tags": ["new", "featured"]
    },
    {
        "sku": "SKU-002",
        "name": "Widget B",
        "price": "19.99",
        "regular_price": "19.99",
        "stock_quantity": 50
    }
]
"""


@pytest.fixture
def sample_product_dict():
    """Sample product dictionary for testing."""
    return {
        "sku": "TEST-001",
        "name": "Test Product",
        "price": "29.99",
        "regular_price": "39.99",
        "sale_price": "29.99",
        "stock_quantity": 100,
        "manage_stock": True,
        "stock_status": "instock",
        "categories": "Electronics, Gadgets",
        "tags": "new, featured",
    }


@pytest.fixture
def mock_s3_resource():
    """Mock S3 resource for testing."""
    from unittest.mock import MagicMock

    from dagster_ecommerce.resources.s3_resource import S3Resource

    mock = MagicMock(spec=S3Resource)
    mock.bucket = "test-bucket"
    mock.prefix = "products/incoming/"
    mock.dead_letter_prefix = "products/dead_letter/"
    mock.list_new_objects.return_value = []
    mock.download_file.return_value = b"test content"
    mock.parse_file.return_value = [{"sku": "TEST-001", "name": "Test Product"}]
    mock.move_to_processed.return_value = "products/processed/test.csv"

    return mock


@pytest.fixture
def mock_woo_resource():
    """Mock WooCommerce resource for testing."""
    from unittest.mock import MagicMock

    from dagster_ecommerce.resources.woo_resource import WooCommerceResource

    mock = MagicMock(spec=WooCommerceResource)
    mock.get_products.return_value = []
    mock.get_products_by_sku.return_value = {}
    mock.create_product.return_value = {"id": 1, "sku": "TEST-001"}
    mock.update_product.return_value = {"id": 1, "sku": "TEST-001"}
    mock.batch_update_products.return_value = {"create": [], "update": [], "delete": []}

    return mock
