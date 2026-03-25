# E-Commerce Intelligent Data Pipelines

A Dagster project for ingesting product data from AWS S3, validating it, and syncing valid records to WooCommerce.

The pipeline accepts CSV, JSON, and NDJSON inputs, isolates invalid records in dead-letter storage, and emits run metadata that is useful for monitoring and troubleshooting.

## Features

- Multi-format ingestion for CSV, JSON, and NDJSON files.
- Product validation before any WooCommerce write occurs.
- SKU-based reconciliation for create versus update behavior.
- Dead-letter storage for invalid records and failed processing paths.
- Structured logging and Dagster metadata for operational visibility.
- Retry and rate-limiting support around WooCommerce API calls.

## Pipeline Flow

`S3 source files -> ingestion -> validation -> WooCommerce sync + dead-letter storage`

## Components

### Ingestion

- Scans the configured S3 prefix for product files.
- Parses supported file formats into raw records.
- Moves successfully parsed files to the processed prefix.

### Processing

- Validates required fields and business rules.
- Converts raw records into WooCommerce-compatible payloads.
- Separates valid and invalid records for downstream handling.

### Sync

- Looks up existing WooCommerce products by SKU.
- Creates new products when no match is found.
- Updates existing products when a SKU match exists.

### Dead Letter Queue

- Stores invalid records as JSON in S3.
- Captures error categories and sample payloads for review.

## Prerequisites

- Python 3.11+
- AWS credentials with access to the target S3 bucket
- A WooCommerce store with REST API credentials

## Installation

```bash
git clone <repository-url>
cd ecommerce_intelligent_data_pipelines
pip install -e .
pip install -e ".[dev]"
```

## Configuration

Create a `.env` file in the project root:

```env
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name
S3_PREFIX=products/incoming/
WOOCOMMERCE_URL=https://yourstore.com/wp-json/wc/v3
WOOCOMMERCE_CONSUMER_KEY=your-consumer-key
WOOCOMMERCE_CONSUMER_SECRET=your-consumer-secret
```

## Running Locally

```bash
dagster dev
```

To materialize assets from the command line:

```bash
dagster asset materialize -m dagster_ecommerce.definitions --select "*"
```

## Development

Run tests:

```bash
pytest
```

Run code quality checks:

```bash
ruff check .
black .
mypy .
```

## Monitoring

Use the Dagster UI to inspect:

- Asset materializations
- Validation counts
- Sync success and failure metrics
- Dead-letter outputs and logs
