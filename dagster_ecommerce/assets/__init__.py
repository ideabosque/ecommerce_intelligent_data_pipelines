"""Dagster Assets Package."""

from dagster_ecommerce.assets.product_processing import (
    validated_products,
)
from dagster_ecommerce.assets.s3_ingestion import (
    processed_files,
    raw_product_data,
    s3_product_files,
)
from dagster_ecommerce.assets.knowledge_graph_sync import (
    knowledge_graph_products,
    woo_products_for_kg,
)
from dagster_ecommerce.assets.woocommerce_sync import (
    dead_letter_queue,
    woocommerce_products,
)
from dagster_ecommerce.assets.rfq_sync import (
    rfq_items_source,
    rfq_items_synced,
    woo_products_for_rfq,
    rfq_dead_letter_queue,
)

__all__ = [
    "s3_product_files",
    "raw_product_data",
    "processed_files",
    "validated_products",
    "woocommerce_products",
    "dead_letter_queue",
    "woo_products_for_kg",
    "knowledge_graph_products",
    # RFQ sync assets
    "woo_products_for_rfq",
    "rfq_items_source",
    "rfq_items_synced",
    "rfq_dead_letter_queue",
]
