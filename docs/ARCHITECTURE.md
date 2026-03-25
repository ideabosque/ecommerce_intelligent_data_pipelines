# E-Commerce Intelligent Data Pipelines Architecture

**Project:** `ecommerce_intelligent_data_pipelines`  
**Last Updated:** 2026-03-22

## Current Architecture

This repository implements a Dagster-based pipeline that:

1. Detects incoming product files in Amazon S3
2. Parses CSV, JSON, and NDJSON files into raw records
3. Validates and transforms records into WooCommerce-compatible payloads
4. Syncs valid products to WooCommerce using the REST API
5. Writes invalid records to S3 dead-letter storage
6. Emits metadata for monitoring in Dagster

## System Flow

`S3 incoming files -> Dagster ingestion assets -> validation -> WooCommerce sync + S3 dead-letter output`

## Main Components

### Dagster Assets

- `s3_product_files`: lists incoming S3 objects
- `raw_product_data`: downloads and parses supported file formats
- `validated_products`: validates records and separates valid from invalid payloads
- `woocommerce_products`: creates or updates products in WooCommerce
- `dead_letter_queue`: persists invalid records to the dead-letter S3 prefix
- `processed_files`: moves successfully parsed files to the processed prefix

### Resources

- `S3Resource`: listing, downloading, parsing, moving, and writing files in S3
- `WooCommerceResource`: WooCommerce API access with retry and rate-limiting behavior

### Sensor

- `s3_product_file_sensor`: checks the configured S3 prefix and triggers runs when new files are detected

## Storage Model

The implementation uses S3-backed file movement and dead-letter storage.

### S3 Prefixes

- Incoming: `products/incoming/`
- Processed: `products/processed/`
- Dead letter: `products/dead_letter/`

### Dead-Letter Format

Invalid records are written as JSON documents containing:

- UTC timestamp
- invalid record count
- category counts
- the invalid records themselves

## Validation Model

Validation currently enforces:

- required product name
- required price or regular price
- non-negative numeric pricing
- SKU uniqueness within a batch
- supported WooCommerce product type
- supported WooCommerce product status
- supported WooCommerce stock status

## Sync Model

WooCommerce reconciliation uses:

1. SKU lookup for existing products
2. update when a SKU match exists
3. create when no SKU match exists

The sync layer tracks created, updated, skipped, and failed records and attaches these as Dagster metadata.

## Operational Notes

- File-level parse failures do not stop the entire batch
- Invalid records do not block valid records from syncing
- Only successfully parsed files are moved to the processed prefix
- Dead-letter output is stored in S3 for later review and reprocessing

## Out of Scope

The current implementation does not use database-backed staging or fallback local database storage.
