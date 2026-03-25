# Development Plan

**Project:** `ecommerce_intelligent_data_pipelines`  
**Last Updated:** 2026-03-22

## Current Status

The core Dagster pipeline exists and includes:

- S3 ingestion assets
- validation and transformation logic
- WooCommerce sync logic
- S3 dead-letter handling
- automated tests

## Near-Term Priorities

### 1. Tighten Parsing Contracts

- normalize source field naming more consistently across CSV and JSON inputs
- document expected input schemas per source format
- add tests for malformed and partially valid files

### 2. Improve Validation Coverage

- add validation for tags, image URLs, and attribute shape
- add clearer error categories for parse versus validation failures
- add tests for mixed valid and invalid batches

### 3. Strengthen WooCommerce Sync

- implement safer behavior for products without SKU
- add more explicit handling for partial update failures
- expand tests around retry, API error, and rate-limit scenarios

### 4. Operational Hardening

- improve sensor run configuration and resource wiring
- add better run metadata for processed files and failure summaries
- document recovery and reprocessing procedures for dead-letter outputs

### 5. Documentation Cleanup

- keep docs aligned with the implemented S3-based architecture
- avoid references to architecture components that are not actually implemented

## Done Criteria For Next Iteration

- docs match the real implementation
- tests cover the main failure paths
- dependency files stay aligned with imports and packaging metadata
- the pipeline behavior is clear for operators and future contributors
