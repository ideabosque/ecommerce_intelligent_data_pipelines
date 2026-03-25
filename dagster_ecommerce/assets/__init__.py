"""Dagster Assets Package."""

from dagster_ecommerce.assets.product_processing import (
    validated_products,
)
from dagster_ecommerce.assets.s3_ingestion import (
    processed_files,
    raw_product_data,
    s3_product_files,
)
from dagster_ecommerce.assets.woocommerce_sync import (
    dead_letter_queue,
    woocommerce_products,
)

__all__ = [
    "s3_product_files",
    "raw_product_data",
    "processed_files",
    "validated_products",
    "woocommerce_products",
    "dead_letter_queue",
]
