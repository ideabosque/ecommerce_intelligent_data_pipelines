# Data Pipeline: AWS S3 (DDS) to WooCommerce

**Project:** `ecommerce_intelligent_data_pipelines`  
**Document Version:** 1.0  
**Last Updated:** 2026-03-29

---

## Executive Summary

This document describes the complete data pipeline that synchronizes product data from AWS S3 (DDS) to WooCommerce. The pipeline automates the ingestion, validation, transformation, and synchronization of product data using Dagster as the orchestration framework.

### Key Capabilities

- **Automated Ingestion**: Detects and processes incoming files from S3
- **Multi-Format Support**: Handles CSV, JSON, and NDJSON formats
- **Shopify Compatibility**: Auto-detects and transforms Shopify CSV exports
- **Data Validation**: Ensures data quality before synchronization
- **Dead Letter Queue**: Captures invalid records for review and reprocessing
- **Idempotent Sync**: Prevents duplicate product creation via SKU-based matching

---

## Architecture Overview

### High-Level System Flow

```
S3 Incoming Files → Dagster Ingestion → Validation → WooCommerce Sync + S3 Dead-Letter Output
```

### S3 Storage Model

The pipeline uses a structured S3 prefix hierarchy for file lifecycle management:

| Prefix | Purpose |
|--------|---------|
| `products/incoming/` | New files awaiting processing |
| `products/processed/` | Successfully processed files |
| `products/dead_letter/` | Invalid records for review |

### Pipeline Components

#### Dagster Assets

| Asset | Purpose |
|-------|---------|
| `s3_product_files` | Lists incoming S3 objects |
| `raw_product_data` | Downloads and parses supported file formats |
| `validated_products` | Validates records and separates valid/invalid payloads |
| `woocommerce_products` | Creates or updates products in WooCommerce |
| `dead_letter_queue` | Persists invalid records to dead-letter S3 prefix |
| `processed_files` | Moves successfully parsed files to processed prefix |

#### Resources

- **S3Resource**: Handles listing, downloading, parsing, moving, and writing files in S3
- **WooCommerceResource**: Manages WooCommerce API access with retry and rate-limiting behavior

#### Sensor

- **s3_product_file_sensor**: Monitors the configured S3 prefix and triggers runs when new files are detected

---

## API Mapping

### Supported Input Formats

- CSV
- JSON
- NDJSON

### Shopify CSV Transformation

The pipeline auto-detects Shopify CSV exports and applies specialized transformation rules:

#### Multi-Row Consolidation Rules

1. **Grouping**: Rows are grouped by `Handle`
2. **Primary Row**: The first row with a non-empty `Title` carries product-level fields
3. **Images**: Collected from all rows, deduplicated by `Image Src`, ordered by `Image Position`
4. **Variants**: Options (`Option1/2/3 Name` + `Value`) collected across all rows per option name
5. **Product Type**: Set to `variable` if any option has multiple distinct values; otherwise `simple`

#### Shopify to WooCommerce Field Mapping

| Shopify Field | WooCommerce Field | Transformation |
|--------------|-------------------|----------------|
| `Title` | `name` | Direct copy |
| `Variant SKU` | `sku` | From primary row |
| `Variant Price` | `regular_price` | Direct copy |
| `Variant Compare At Price` | `regular_price` / `sale_price` | Compare At = `regular_price`, Variant Price = `sale_price` |
| `Body (HTML)` | `description` | HTML passed through |
| `SEO Description` | `short_description` | Direct copy |
| `Status` + `Published` | `status` | See status mapping below |
| `Tags` | `tags` | Comma-separated, vendor appended |
| `Vendor` | `tags` | Appended as "Vendor: {value}" |
| `Product Category` | `categories` | Hierarchy flattened to leaf category |
| `Type` | `categories` | Fallback when Product Category is empty |
| Collected `Image Src` / `Image Alt Text` | `images` | List of dicts with `src`, `alt`, `position` |
| Collected `Option Name` / `Value` | `attributes` | List with `name`, `options`, `variation: true` |
| `Variant Grams` | `weight` | Divided by 1000 (grams to kg) |
| `Gift Card` = true | `virtual` | Set to `true` |
| `Variant Requires Shipping` = false | `virtual` | Set to `true` |
| `Variant Taxable` | `tax_status` | `"taxable"` if true, `"none"` if false |
| `Handle` | `meta_data` | Preserved as `shopify_handle` |

#### Status Mapping

| Shopify Status | Published | WooCommerce Status |
|----------------|-------------|-------------------|
| `active` | `true` | `publish` |
| `active` | `false` | `draft` |
| `draft` | any | `draft` |
| `archived` | any | `private` |

#### Backward Compatibility

If incoming data lacks `Handle` and `Title` columns, transformation is skipped and records pass through unchanged. This enables WooCommerce-native CSVs and JSON files to work without modification.

### Core WooCommerce Native Mapping

| Source Field | Target Field | Rule |
|--------------|--------------|------|
| `name` | `name` | Required |
| `sku` | `sku` | Primary reconciliation key |
| `price` / `regular_price` | `regular_price` | At least one must be present |
| `sale_price` | `sale_price` | Used when present |
| `stock_quantity` | `stock_quantity` | Converted to integer |
| `stock_status` | `stock_status` | Must be `instock`, `outofstock`, or `onbackorder` |
| `type` | `type` | Defaults to `simple`; must be WooCommerce-supported |
| `status` | `status` | Defaults to `publish`; must be WooCommerce-supported |
| `featured` | `featured` | Parsed from bool-like values |
| `manage_stock` | `manage_stock` | Parsed from bool-like values |
| `description` | `description` | Passed through when provided |
| `short_description` | `short_description` | Passed through when provided |
| `categories` | `categories` | Parsed from comma-separated strings or lists |
| `tags` | `tags` | Parsed from comma-separated strings or lists |
| `images` | `images` | Parsed from comma-separated strings or lists |
| `attributes` | `attributes` | Parsed from list input |

### Pricing Logic

- When both active price and sale price are present, the higher value becomes `regular_price`
- The lower value becomes `sale_price`
- Otherwise `regular_price` is set from the available main price field

### Boolean Parsing

Values treated as `True`:
- `true`
- `1`
- `yes`
- `on`

All other values follow normal Python truthiness or resolve to `False` for empty values.

### Matching Strategy

1. Match by SKU when provided
2. Create new product when no SKU match exists

No fallback currently exists for slug matching, title matching, or source-handle matching.

---

## Data Flow and Processing Model

### Validation Model

The pipeline enforces the following validations:

- Required product name
- Required price or regular price
- Non-negative numeric pricing
- SKU uniqueness within a batch
- Supported WooCommerce product type
- Supported WooCommerce product status
- Supported WooCommerce stock status

### Sync Model

WooCommerce reconciliation process:

1. **SKU Lookup**: Search for existing products by SKU
2. **Update**: Modify existing product when SKU match exists
3. **Create**: Insert new product when no SKU match exists

The sync layer tracks created, updated, skipped, and failed records as Dagster metadata.

### Dead-Letter Format

Invalid records are written as JSON documents containing:

- UTC timestamp
- Invalid record count
- Category counts
- The invalid records themselves

Invalid records are not sent to WooCommerce; they are written to the dead-letter queue with structured error messages.

### Operational Behavior

- File-level parse failures do not stop the entire batch
- Invalid records do not block valid records from syncing
- Only successfully parsed files are moved to the processed prefix
- Dead-letter output is stored in S3 for later review and reprocessing

---

## Development Plan

### Current Status

The core Dagster pipeline exists and includes:

- S3 ingestion assets
- Validation and transformation logic
- WooCommerce sync logic
- S3 dead-letter handling
- Automated tests

### Near-Term Priorities

#### 1. Tighten Parsing Contracts

- Normalize source field naming consistently across CSV and JSON inputs
- Document expected input schemas per source format
- Add tests for malformed and partially valid files

#### 2. Improve Validation Coverage

- Add validation for tags, image URLs, and attribute shape
- Add clearer error categories for parse versus validation failures
- Add tests for mixed valid and invalid batches

#### 3. Strengthen WooCommerce Sync

- Implement safer behavior for products without SKU
- Add more explicit handling for partial update failures
- Expand tests around retry, API error, and rate-limit scenarios

#### 4. Operational Hardening

- Improve sensor run configuration and resource wiring
- Add better run metadata for processed files and failure summaries
- Document recovery and reprocessing procedures for dead-letter outputs

#### 5. Documentation Cleanup

- Keep docs aligned with the implemented S3-based architecture
- Avoid references to architecture components that are not actually implemented

### Done Criteria For Next Iteration

- Docs match the real implementation
- Tests cover the main failure paths
- Dependency files stay aligned with imports and packaging metadata
- The pipeline behavior is clear for operators and future contributors

---

## Implementation Checklist

### Phase 1: Foundation

- [x] S3 ingestion asset implementation
- [x] File format parsing (CSV, JSON, NDJSON)
- [x] Basic validation layer
- [x] WooCommerce resource with retry logic
- [x] Dead-letter queue implementation

### Phase 2: Data Transformation

- [x] Shopify CSV auto-detection and transformation
- [x] Multi-row consolidation for Shopify format
- [x] Field mapping normalization
- [x] Boolean parsing standardization
- [x] Pricing logic implementation

### Phase 3: Validation & Sync

- [x] SKU-based matching strategy
- [x] Product creation and update logic
- [x] Validation rules enforcement
- [x] Metadata emission for monitoring
- [x] Batch processing support

### Phase 4: Operational Readiness

- [ ] Enhanced error categorization
- [ ] Recovery and reprocessing documentation
- [ ] Performance optimization
- [ ] Extended test coverage
- [ ] Production deployment procedures

---

## Configuration

### Environment Variables

```bash
# S3 Configuration
S3_BUCKET=your-bucket-name
S3_PREFIX=products/incoming/
S3_PROCESSED_PREFIX=products/processed/
S3_DEAD_LETTER_PREFIX=products/dead_letter/

# WooCommerce Configuration
WC_API_URL=https://your-store.com/wp-json/wc/v3/
WC_CONSUMER_KEY=your-consumer-key
WC_CONSUMER_SECRET=your-consumer-secret
WC_TIMEOUT=60

# Dagster Configuration
DAGSTER_HOME=/path/to/dagster/home
```

### Dagster Resource Configuration

```python
s3_resource = S3Resource(
    bucket=EnvVar("S3_BUCKET"),
    incoming_prefix="products/incoming/",
    processed_prefix="products/processed/",
    dead_letter_prefix="products/dead_letter/"
)

woo_resource = WooCommerceResource(
    api_url=EnvVar("WC_API_URL"),
    consumer_key=EnvVar("WC_CONSUMER_KEY"),
    consumer_secret=EnvVar("WC_CONSUMER_SECRET"),
    timeout=60
)
```

---

## Monitoring and Observability

### Dagster Metadata

Each run emits structured metadata:

| Metric | Type | Purpose |
|--------|------|---------|
| `total_files` | int | Total files processed |
| `successful_files` | int | Files parsed successfully |
| `failed_files` | int | Files with parse errors |
| `total_records` | int | Total product records |
| `valid_records` | int | Records passing validation |
| `invalid_records` | int | Records failing validation |
| `created_count` | int | New products created |
| `updated_count` | int | Existing products updated |
| `failed_sync_count` | int | Sync failures |

### Alert Thresholds

- Success rate below 95%
- Parse failure rate above 5%
- Dead-letter backlog above agreed limit
- Run duration materially above baseline

---

## Security Considerations

- Store API credentials in environment variables or secrets manager
- Use least-privilege AWS IAM policies for S3 access
- Enable TLS for all WooCommerce API communication
- Avoid logging sensitive data (credentials, PII)
- Implement audit trails for sync operations

---

## Future Enhancements

### Event-Driven Sync

Use S3 event notifications or WooCommerce webhooks to trigger syncs immediately upon file arrival or product changes.

### Delta Sync

Track file modification dates and only process changed files to reduce processing overhead.

### Multi-Source Support

Generalize the pipeline to support additional e-commerce platforms beyond Shopify and native WooCommerce formats.

### Advanced Validation

Add schema validation, image URL verification, and category existence checks.

---

## Version History

| Version | Date | Summary |
|---------|------|---------|
| 1.0 | 2026-03-29 | Initial merged document combining API mapping, architecture, and development plan |

---

**Document Version:** 1.0  
**Last Updated:** 2026-03-29
