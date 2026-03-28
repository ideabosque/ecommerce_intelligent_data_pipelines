"""Dagster Definitions for E-Commerce Product Sync Pipeline.

This module defines the complete Dagster application including:
- Assets (S3 ingestion, processing, WooCommerce sync)
- Resources (S3, WooCommerce API)
- Sensors (S3 file detection)
- Jobs (product sync pipeline)
"""

from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
)

from dagster_ecommerce import assets
from dagster_ecommerce.config.settings import settings
from dagster_ecommerce.resources.knowledge_graph_resource import knowledge_graph_resource
from dagster_ecommerce.resources.s3_resource import s3_resource
from dagster_ecommerce.resources.woo_resource import woo_resource
from dagster_ecommerce.sensors.s3_sensor import s3_product_file_sensor

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# Define the job that the sensor triggers (S3 → WooCommerce pipeline)
product_sync_job = define_asset_job(
    name="product_sync_job",
    selection=AssetSelection.groups("ingestion", "processing", "sync"),
    description="Sync product data from S3 to WooCommerce",
)

# Separate job: WooCommerce → Knowledge Graph pipeline
knowledge_graph_sync_job = define_asset_job(
    name="knowledge_graph_sync_job",
    selection=AssetSelection.groups("knowledge_graph"),
    description="Fetch products from WooCommerce and extract into knowledge graph",
)

# Load definitions with configured resources
defs = Definitions(
    assets=all_assets,
    jobs=[product_sync_job, knowledge_graph_sync_job],
    resources={
        "s3": s3_resource.configured(
            {
                "bucket": settings.s3_bucket_name,
                "prefix": settings.s3_prefix,
                "region": settings.aws_region,
                "processed_prefix": settings.s3_processed_prefix,
                "dead_letter_prefix": settings.s3_dead_letter_prefix,
                **(
                    {"aws_access_key_id": settings.aws_access_key_id}
                    if settings.aws_access_key_id
                    else {}
                ),
                **(
                    {"aws_secret_access_key": settings.aws_secret_access_key}
                    if settings.aws_secret_access_key
                    else {}
                ),
            }
        ),
        "woo": woo_resource.configured(
            {
                "url": settings.woocommerce_url,
                "consumer_key": settings.woocommerce_consumer_key,
                "consumer_secret": settings.woocommerce_consumer_secret,
                "timeout": settings.woocommerce_timeout,
                "requests_per_second": settings.woocommerce_requests_per_second,
                "max_retries": settings.max_retries,
            }
        ),
        "knowledge_graph": knowledge_graph_resource.configured(
            {
                "url": settings.knowledge_graph_url,
                "endpoint_id": settings.knowledge_graph_endpoint_id,
                "part_id": settings.knowledge_graph_part_id,
                "bearer_token": settings.knowledge_graph_bearer_token,
            }
        ),
    },
    sensors=[s3_product_file_sensor],
)

__all__ = ["defs"]
