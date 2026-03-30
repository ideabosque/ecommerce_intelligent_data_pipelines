# WooCommerce to RFQ Engine Pipeline

## Executive Summary

This document defines the design for synchronizing WooCommerce product data into the AI RFQ Engine. The pipeline converts WooCommerce products into RFQ Engine entities—primarily `Item` and `ProviderItem`—with optional support for `ItemPriceTier` when tiered pricing is available.

This pipeline is separate from the existing WooCommerce-to-Knowledge-Graph extraction pipeline (`knowledge_graph_sync_job`). Both pipelines fetch product data from WooCommerce but serve different purposes:

- **Knowledge Graph Pipeline**: Extracts product text into entities and relationships for semantic search and AI reasoning
- **RFQ Pipeline** (this document): Transforms product data into structured RFQ Engine records for quoting and procurement workflows

### Design Priorities

- Idempotent synchronization via lookup-before-write
- Clear data ownership and traceability through stable external identifiers
- Batch execution with configurable retry and rate limiting
- Recoverable failure handling with dead letter persistence
- Operational visibility through Dagster metadata and structured logging

The initial scope is one-way synchronization from WooCommerce to the RFQ Engine. Bidirectional sync, event-driven updates, and advanced pricing logic are intentionally deferred to future releases.

---

## Scope

### In Scope

- Fetch WooCommerce product data via the REST API
- Transform products into RFQ Engine payloads
- Upsert `Item` records via GraphQL
- Upsert `ProviderItem` records via GraphQL
- Optionally upsert `ItemPriceTier` records when tier data is present
- Capture failures for replay or manual review
- Expose operational metrics and run metadata in Dagster

### Out of Scope

- Inventory synchronization
- Bidirectional price updates back to WooCommerce
- Customer-specific quoting logic
- Real-time webhook ingestion in Scope v1
- Direct writes to RFQ Engine storage outside the GraphQL API
- Deactivation or deletion of RFQ Engine records when products are removed or unpublished in WooCommerce (deferred to the Reconciliation future enhancement)

---

## High-Level Architecture

### Data Flow

```text
WooCommerce REST API
  -> woo_products_for_rfq  (fetch all published products)
  -> rfq_items_source       (transform, validate, deduplicate)
  -> rfq_items_synced       (upsert Item + ProviderItem via GraphQL)
  -> RFQ Engine: Item / ProviderItem / ItemPriceTier

rfq_items_synced
  -> rfq_dead_letter_queue  (persist failures to S3 dead letter prefix)
```

### Asset Dependency Model

```text
woo_products_for_rfq -> rfq_items_source -> rfq_items_synced -> rfq_dead_letter_queue
                                         \
                                          -> rfq_price_tiers_synced  (optional, Scope v2)
```

### Pipeline Relationship

The project defines three independent Dagster jobs:

| Job | Flow | Trigger |
|-----|------|---------|
| `product_sync_job` | S3 -> WooCommerce | S3 sensor or manual |
| `knowledge_graph_sync_job` | WooCommerce -> Knowledge Graph | Manual |
| `rfq_sync_job` (this pipeline) | WooCommerce -> RFQ Engine | Manual or scheduled |

Each job has its own asset group and can be triggered independently from the Dagster UI. `rfq_sync_job` must be registered in `definitions.py` alongside existing jobs, with the `rfq` resource wired in.

### Processing Model

- WooCommerce is the source of truth for product master data
- RFQ Engine writes are performed through idempotent GraphQL upserts
- Products are processed in configurable batches to control throughput and isolate failures
- The synchronization layer records per-item outcomes so partial success does not lose visibility

---

## Assumptions

- Each product has a stable external identifier, ideally SKU
- RFQ Engine mutations support create-or-update semantics keyed by engine UUIDs. External IDs are queryable for lookup but are not the mutation key
- Authentication to the RFQ Engine uses an API key header (`x-api-key`) and a partition header (`Part-Id`). This differs from the Knowledge Graph resource, which uses Bearer token authentication
- The `endpoint_id` is a URL path segment (e.g., `gpt`) that routes to the correct engine instance
- The RFQ Engine remains the system of record for RFQ-facing item entities after sync

Products with missing or duplicated SKUs are treated as invalid in Scope v1 and routed to failure handling rather than inferring identity.

---

## Open Decisions

Confirm these items before implementation:

- Should promotional sale pricing sync, or only base pricing?
- Should products without numeric prices be excluded entirely from `ProviderItem` sync?
- Which WooCommerce metadata keys should be preserved in `item_spec`?
- Is `ItemPriceTier` required for Scope v1, or deferred until segment mapping exists?
- What is the agreed replay mechanism for DLQ records?
- Should this pipeline share `woo_products_for_kg` or have its own dedicated WooCommerce fetch asset?
- Do the RFQ Engine field names `quantity_greater_than` and `quantity_less_than` use standard spelling? Verify against the live schema before implementing `ItemPriceTier`.

---

## RFQ Engine Data Model

### Item

Represents the canonical RFQ-facing product definition. Created and updated via the `insertUpdateItem` mutation.

| RFQ Field | Type | Source | Notes |
|-----------|------|--------|-------|
| `item_uuid` | string | RFQ Engine | Generated on create; pass existing UUID on update |
| `item_type` | string | WooCommerce category | First category name, fallback to `"uncategorized"` |
| `item_name` | string | `name` | Required |
| `item_description` | string | `description` | Strip HTML before sending |
| `uom` | string | config or derived | Default to `"EA"` |
| `item_external_id` | string | SKU-derived | Format: `woo-{sku}` |
| `updated_by` | string | config | Required. Identifies the actor performing the upsert (e.g., `"dagster-pipeline"`) |

### ProviderItem

Represents WooCommerce as a provider-specific commercial view of an item.

| RFQ Field | Type | Source | Notes |
|-----------|------|--------|-------|
| `provider_item_uuid` | string | RFQ Engine | Generated on create; required on update |
| `item_uuid` | string | Item upsert result | Required for linkage |
| `provider_corp_external_id` | string | config | Default: `"woocommerce"` |
| `provider_item_external_id` | string | WooCommerce SKU | Required |
| `base_price_per_uom` | number | `price` or `regular_price` | Decimal normalization required (see Canonical Mapping Rules) |
| `item_spec` | object | attributes and selected metadata | Keep compact and structured (see Canonical Mapping Rules) |

### ItemPriceTier (Scope v2 — Deferred)

Optional entity for quantity-based pricing. Deferred until segment mapping is standardized.

**Note:** Verify field names `quantity_greater_than` and `quantity_less_than` against the live RFQ Engine GraphQL schema before implementing. Earlier drafts used `quantity_greater_then` / `quantity_less_then`.

| RFQ Field | Type | Source | Notes |
|-----------|------|--------|-------|
| `item_uuid` | string | Item upsert result | Required |
| `provider_item_uuid` | string | ProviderItem upsert result | Required for creation |
| `quantity_greater_than` | number | tier rule | Lower bound |
| `quantity_less_than` | number | tier rule | Upper bound or null |
| `price_per_uom` | number | tier rule | Preferred over margin if price exists |
| `margin_per_uom` | number | derived or configured | Optional |
| `status` | string | config | Engine default: `in_review` |

---

## GraphQL API Reference

### Required Request Headers

All requests to the RFQ Engine GraphQL API require:

| Header | Value / Description |
|--------|---------------------|
| `x-api-key` | API key for authentication |
| `Part-Id` | Partition identifier for data isolation |
| `Content-Type` | `application/json` |

### Endpoint

The GraphQL endpoint URL includes the `endpoint_id` as a path segment:

```
POST {RFQ_ENGINE_BASE_URL}/{endpoint_id}/ai_rfq_graphql
```

Example: `http://localhost:8000/gpt/ai_rfq_graphql`

### InsertUpdateItem Mutation

```graphql
mutation InsertUpdateItem(
    $itemUuid: String,
    $itemType: String,
    $itemName: String,
    $itemDescription: String,
    $uom: String,
    $itemExternalId: String,
    $updatedBy: String!
) {
    insertUpdateItem(
        itemUuid: $itemUuid,
        itemType: $itemType,
        itemName: $itemName,
        itemDescription: $itemDescription,
        uom: $uom,
        itemExternalId: $itemExternalId,
        updatedBy: $updatedBy
    ) {
        item {
            itemUuid
            itemType
            itemName
            itemDescription
            uom
            itemExternalId
            createdAt
            updatedAt
            updatedBy
        }
    }
}
```

**Variables:**

| Variable | Type | Required | Notes |
|----------|------|----------|-------|
| `itemUuid` | String | No | Pass existing UUID to update; omit to create |
| `itemType` | String | No | Product category (e.g., `"raw_material"`) |
| `itemName` | String | No | Product name |
| `itemDescription` | String | No | Plain text description (strip HTML) |
| `uom` | String | No | Unit of measure (e.g., `"EA"`, `"kg"`) |
| `itemExternalId` | String | No | Stable external ID (e.g., `"woo-{sku}"`) |
| `updatedBy` | String | **Yes** | Actor performing the upsert (e.g., `"dagster-pipeline"`) |

**Example request:**

```python
import requests
import json

url = "{base_url}/{endpoint_id}/ai_rfq_graphql"

payload = json.dumps({
    "query": INSERT_UPDATE_ITEM_MUTATION,
    "variables": {
        "itemType": "raw_material",
        "itemName": "Steel Plate",
        "itemDescription": "High grade steel plate",
        "uom": "kg",
        "itemExternalId": "woo-STEEL-001",
        "updatedBy": "dagster-pipeline"
    }
})

headers = {
    "x-api-key": "{x-api-key}",
    "Content-Type": "application/json",
    "Part-Id": "{part_id}"
}

response = requests.post(url, headers=headers, data=payload)
```

**Response:** Returns the upserted `item` object including the engine-generated `itemUuid`, `createdAt`, and `updatedAt` timestamps.

---

### InsertUpdateProviderItem Mutation

```graphql
mutation InsertUpdateProviderItem(
    $providerItemUuid: String,
    $itemUuid: String!,
    $providerCorpExternalId: String,
    $providerItemExternalId: String,
    $basePricePerUom: Float,
    $itemSpec: JSONCamelCase,
    $updatedBy: String!
) {
    insertUpdateProviderItem(
        providerItemUuid: $providerItemUuid,
        itemUuid: $itemUuid,
        providerCorpExternalId: $providerCorpExternalId,
        providerItemExternalId: $providerItemExternalId,
        basePricePerUom: $basePricePerUom,
        itemSpec: $itemSpec,
        updatedBy: $updatedBy
    ) {
        providerItem {
            providerItemUuid
            itemUuid
            providerCorpExternalId
            providerItemExternalId
            basePricePerUom
            itemSpec
            createdAt
            updatedAt
            updatedBy
        }
    }
}
```

**Variables:**

| Variable | Type | Required | Notes |
|----------|------|----------|-------|
| `providerItemUuid` | String | No | Pass existing UUID to update; omit to create |
| `itemUuid` | String | **Yes** | UUID of the linked `Item` record |
| `providerCorpExternalId` | String | No | Provider identifier (e.g., `"woocommerce"`) |
| `providerItemExternalId` | String | No | WooCommerce SKU |
| `basePricePerUom` | Float | No | Normalized decimal price |
| `itemSpec` | JSONCamelCase | No | Structured product metadata |
| `updatedBy` | String | **Yes** | Actor performing the upsert |

**Response:** Returns the upserted `providerItem` object including the engine-generated `providerItemUuid`.

> **Note:** Confirm exact argument types (especially `itemSpec` scalar type) against the live schema before implementation.

---

### Lookup Queries (for Idempotency)

These queries retrieve existing UUIDs by external ID for lookup-before-write flows.

#### Query Item by External ID

```graphql
query GetItemByExternalId($itemExternalId: String!) {
    item(itemExternalId: $itemExternalId) {
        itemUuid
        itemExternalId
    }
}
```

Returns `null` if no matching record exists. Use the returned `itemUuid` as the key in subsequent `insertUpdateItem` calls to perform updates rather than creates.

#### Query ProviderItem by External ID

```graphql
query GetProviderItemByExternalId($providerItemExternalId: String!) {
    providerItem(providerItemExternalId: $providerItemExternalId) {
        providerItemUuid
        providerItemExternalId
    }
}
```

Returns `null` if no matching record exists. Use the returned `providerItemUuid` in subsequent `insertUpdateProviderItem` calls.

> **Note:** Confirm exact query field names and argument signatures against the live RFQ Engine schema before implementing the lookup layer.

---

## Canonical Mapping Rules

### Identity

- `item_external_id = "woo-{sku}"`
- `provider_item_external_id = sku`
- Reject products with missing SKU unless the business explicitly approves an alternate identity rule
- External IDs alone do not make writes idempotent. The required pattern is: lookup by external ID first, then update by UUID if a record already exists

### Product Type

- Use the first WooCommerce category name as `item_type`
- If categories are empty, use `"uncategorized"`
- Keep the value stable; avoid category concatenation in Scope v1

### Description

- Prefer WooCommerce `description`
- Strip HTML tags before sending
- Trim whitespace
- Fall back to `short_description` if the main description is empty

### Pricing

- Prefer `price` when present and parseable as a positive number
- Fall back to `regular_price`
- Ignore sale windows in Scope v1 unless the business specifically wants promotional prices reflected in the RFQ Engine
- If no valid numeric price exists, sync `Item` but treat `ProviderItem` creation as invalid. The engine requires `base_price_per_uom` for persistence.

**Decimal normalization:** Parse `price` and `regular_price` from WooCommerce as strings (returned as `"10.00"` or `""`). Convert to `float` and round to 4 decimal places before sending. Reject any value that is not a finite positive number (e.g., `0`, negative, `NaN`, empty string) as invalid for `ProviderItem` creation.

```python
def normalize_price(raw: str | None) -> float | None:
    """Return a normalized positive float, or None if invalid."""
    try:
        value = float(raw or "")
        return round(value, 4) if value > 0 else None
    except (ValueError, TypeError):
        return None
```

### Specifications

Recommended `item_spec` content:

- WooCommerce attributes as key-value pairs
- Brand or manufacturer metadata if available
- Weight and dimensions when present
- Stock status only if useful to the RFQ workflow
- Source identifiers such as WooCommerce product ID

Avoid copying the entire WooCommerce payload into `item_spec`. Keep it intentional and queryable.

**Use snake_case keys** to match the RFQ Engine's field naming convention:

```json
{
    "woo_product_id": 1042,
    "brand": "Acme Steel",
    "material": "carbon steel",
    "weight_kg": 12.5,
    "dimensions_cm": "30x20x5",
    "stock_status": "instock"
}
```

---

## Pipeline Components

### 1. RFQ Engine Resource

**File:** `dagster_ecommerce/resources/rfq_resource.py`

Follows the same resource pattern used by `WooCommerceResource` and `KnowledgeGraphResource`.

**Responsibilities:**

- Build authenticated GraphQL requests with `x-api-key` and `Part-Id` headers
- Expose upsert methods for `Item`, `ProviderItem`, and `ItemPriceTier`
- Enforce client-side rate limiting
- Apply retry with exponential backoff
- Return structured results for logging and replay

**Implementation Notes:**

- The GraphQL endpoint URL is constructed as `{base_url}/{endpoint_id}/ai_rfq_graphql`
- Authentication uses the `x-api-key` header (not Bearer token—this differs from `KnowledgeGraphResource`)
- The `Part-Id` header controls data partitioning
- GraphQL mutations accept flat arguments, not nested `input` objects
- All mutations require `updatedBy` (String!) to identify the actor
- Register as a Dagster `@resource` with `config_schema` for consistency with existing resources
- Use `requests.Session` with pre-configured headers

**Recommended Methods:**

- `upsert_item(payload) -> dict`
- `upsert_provider_item(payload) -> dict`
- `upsert_item_price_tier(payload) -> dict`
- `query_item_by_external_id(external_id) -> dict | None`
- `query_provider_item_by_external_id(external_id) -> dict | None`
- `execute(query, variables) -> dict`

### 2. Transformation Layer

**File:** `dagster_ecommerce/transformations/woo_to_rfq.py`

Follows the same pattern as `shopify_to_woo.py`.

**Responsibilities:**

- Validate required source fields (SKU, name, price)
- Normalize strings, decimals, and HTML content
- Generate RFQ payloads from WooCommerce products
- Emit structured validation errors for invalid products

**Recommended Functions:**

- `transform_to_item(product, settings) -> dict`
- `transform_to_provider_item(product, item_uuid, settings) -> dict`
- `transform_to_price_tiers(product, item_uuid, provider_item_uuid, settings) -> list[dict]`
- `strip_html(text) -> str`
- `normalize_price(raw) -> float | None`
- `validate_rfq_product(product) -> tuple[bool, list[str]]`

### 3. Dagster Assets

**File:** `dagster_ecommerce/assets/rfq_sync.py`

| Asset | Group | Dependencies | Resource Keys | Purpose |
|-------|-------|--------------|---------------|---------|
| `woo_products_for_rfq` | `rfq` | none (fetches from WooCommerce API) | `woo` | Fetch all published products |
| `rfq_items_source` | `rfq` | `woo_products_for_rfq` | none | Transform, validate, and deduplicate into RFQ payloads |
| `rfq_items_synced` | `rfq` | `rfq_items_source` | `rfq` | Upsert Item + ProviderItem via GraphQL with lookup-before-write |
| `rfq_dead_letter_queue` | `rfq` | `rfq_items_synced` | `s3` | Persist failed sync records to S3 for review and replay |
| `rfq_price_tiers_synced` | `rfq` | `rfq_items_synced` | `rfq` | Optional: sync tiered pricing (Scope v2, behind feature flag) |

All assets use `required_resource_keys` for resource access, consistent with existing asset patterns.

`woo_products_for_rfq` calls `woo.get_products(params={"per_page": 100, "status": "publish"})`. The `WooCommerceResource` handles pagination internally—no additional pagination logic required in the asset. This mirrors `woo_products_for_kg`.

---

## Synchronization Strategy

### Order of Operations

For each valid product:

1. Transform WooCommerce product into `Item` payload
2. Query by `item_external_id` to check for existing records
3. Create or update `Item` (pass `item_uuid` on update)
4. Transform WooCommerce product into `ProviderItem` payload using the resolved `item_uuid`
5. Query by `provider_item_external_id` to check for existing records
6. Create or update `ProviderItem`
7. If tier pricing is enabled and required dependencies exist, transform and upsert `ItemPriceTier` records

This order keeps entity relationships explicit and avoids creating orphan provider records.

### Idempotency

Idempotency is mandatory.

Use stable external identifiers plus lookup-before-write so repeated runs:

- Do not create duplicates
- Can safely retry after transient failures
- Can support full re-syncs without cleanup steps

For the current engine:

- Query `item(itemExternalId: ...)` before deciding whether to pass `item_uuid`
- Query `providerItem(providerItemExternalId: ...)` before deciding whether to pass `provider_item_uuid`

If throughput becomes a concern, add a local cache or persisted mapping for `external_id -> uuid`.

### Batching

Recommended starting configuration:

| Parameter | Value |
|-----------|-------|
| Batch size | `25` |
| Requests per second | `5` |
| Request timeout | `60s` |
| Max retries | `3` |

All values are configurable via environment variables and should be tuned based on production metrics.

---

## Configuration

### Required Environment Variables

```bash
RFQ_ENGINE_BASE_URL=http://localhost:8000
RFQ_ENGINE_ENDPOINT_ID=gpt
RFQ_ENGINE_API_KEY=<x-api-key>
RFQ_ENGINE_PART_ID=<part-id>
RFQ_ENGINE_UPDATED_BY=dagster-pipeline
```

- `RFQ_ENGINE_BASE_URL`: Base URL of the RFQ Engine service
- `RFQ_ENGINE_ENDPOINT_ID`: Path segment for the endpoint (e.g., `gpt`). The full GraphQL URL is `{base_url}/{endpoint_id}/ai_rfq_graphql`
- `RFQ_ENGINE_API_KEY`: The `x-api-key` header value for authentication (required)
- `RFQ_ENGINE_PART_ID`: The `Part-Id` header value for data partitioning
- `RFQ_ENGINE_UPDATED_BY`: The actor name passed as `updatedBy` in all mutations

### Optional Authentication

```bash
RFQ_ENGINE_BEARER_TOKEN=<bearer-token>
```

- `RFQ_ENGINE_BEARER_TOKEN`: Optional `Authorization: Bearer` header for additional authentication. If provided, both `x-api-key` and `Authorization: Bearer` headers will be sent.

### Optional Environment Variables

```bash
RFQ_ENGINE_TIMEOUT=60
RFQ_ENGINE_REQUESTS_PER_SECOND=5
RFQ_ENGINE_PROVIDER_EXTERNAL_ID=woocommerce
RFQ_ENGINE_DEFAULT_UOM=EA
RFQ_ENGINE_ENABLE_PRICE_TIERS=false
```

- `RFQ_ENGINE_TIMEOUT`: Request timeout in seconds (default: 60)
- `RFQ_ENGINE_REQUESTS_PER_SECOND`: Rate limit for API calls (default: 5)
- `RFQ_ENGINE_PROVIDER_EXTERNAL_ID`: Provider corp identifier (default: "woocommerce")
- `RFQ_ENGINE_DEFAULT_UOM`: Default unit of measure (default: "EA")
- `RFQ_ENGINE_ENABLE_PRICE_TIERS`: Enable tier pricing sync (Scope v2, default: false)

> **Note on `max_retries`:** The existing `Settings` class in `dagster_ecommerce/config/settings.py` already defines a shared `max_retries` field (default `3`). This is reused rather than introducing `RFQ_ENGINE_MAX_RETRIES`.

**Settings field mapping:** Environment variables are mapped to Pydantic fields in `Settings` class as follows:
- `RFQ_ENGINE_BASE_URL` → `rfq_engine_base_url`
- `RFQ_ENGINE_ENDPOINT_ID` → `rfq_engine_endpoint_id`
- `RFQ_ENGINE_PART_ID` → `rfq_engine_part_id`
- `RFQ_ENGINE_API_KEY` → `rfq_engine_api_key`
- `RFQ_ENGINE_BEARER_TOKEN` → `rfq_engine_bearer_token`
- `RFQ_ENGINE_TIMEOUT` → `rfq_engine_timeout`
- `RFQ_ENGINE_REQUESTS_PER_SECOND` → `rfq_engine_requests_per_second`
- `RFQ_ENGINE_UPDATED_BY` → `rfq_engine_updated_by`
- `RFQ_ENGINE_PROVIDER_EXTERNAL_ID` → `rfq_engine_provider_external_id`
- `RFQ_ENGINE_DEFAULT_UOM` → `rfq_engine_default_uom`
- `RFQ_ENGINE_ENABLE_PRICE_TIERS` → `rfq_engine_enable_price_tiers`

These settings should be added to `.env.example` for documentation purposes.

### Dagster Resource Config Schema

```python
config_schema = {
    "base_url": Field(String, description="RFQ Engine base URL"),
    "endpoint_id": Field(String, description="Endpoint ID (URL path segment)"),
    "api_key": Field(String, description="x-api-key header value (required)"),
    "bearer_token": Field(String, default_value="", is_required=False, description="Optional Authorization: Bearer token"),
    "part_id": Field(String, description="Part-Id header value"),
    "updated_by": Field(String, default_value="dagster-pipeline", description="Actor for updatedBy"),
    "timeout": Field(Int, default_value=60, description="Request timeout in seconds"),
    "requests_per_second": Field(Int, default_value=5, description="Rate limit"),
    "max_retries": Field(Int, default_value=3, description="Max retry attempts"),
}
```

The resource constructs the full endpoint URL internally as `{base_url}/{endpoint_id}/ai_rfq_graphql`.

---

## Validation Rules

### Required for Item

- SKU present and non-empty
- Product name present
- External ID derivable (`woo-{sku}`)

### Required for ProviderItem

- Successful `Item` creation or update (resolved `item_uuid`)
- SKU present
- Provider external ID configured
- Valid positive numeric price (see decimal normalization rules)

### Invalid Data Handling

A product is marked as failed with a recorded reason when:

- SKU is missing
- SKU is duplicated within the same batch
- Name is missing
- Price field is missing or non-numeric for `ProviderItem` creation
- The GraphQL API returns a validation error

Failed products are routed to the dead letter queue for review and replay.

---

## Error Handling and Recovery

### Failure Categories

| Category | Example | Retryable |
|----------|---------|-----------|
| Transient infrastructure | Network timeout, DNS failure | Yes |
| API throttling | 429 Too Many Requests | Yes (with backoff) |
| API validation error | Missing required field | No |
| Source data quality | Missing SKU, invalid price | No |
| Partial batch failure | Some items succeed, others fail | Per-item |

### Retry Policy

- Retry only transient failures and throttling responses
- Use exponential backoff with `max_retries = 3` (3 retries, 4 total attempts): 1s → 2s → 4s delays between attempts
- Do not retry permanent validation errors

### Dead Letter Queue

Persist failed records with:

- `run_id`: Dagster run identifier for traceability
- `timestamp`: ISO 8601 timestamp of failure
- `sku`: WooCommerce product SKU
- `woo_product_id`: WooCommerce product ID
- `item_external_id`: Derived external ID (`woo-{sku}`)
- `product_name`: Product name for identification
- `failure_category`: `validation_error` or `api_error`
- `error_message`: Full error message
- `serialized_payload`: Complete payload attempted for replay

**DLQ File Structure:**
```json
{
  "timestamp": "2026-03-29T12:34:56.789Z",
  "count": 5,
  "records": [
    {
      "run_id": "abc123",
      "timestamp": "2026-03-29T12:34:56.789Z",
      "sku": "STEEL-001",
      "woo_product_id": 1042,
      "item_external_id": "woo-STEEL-001",
      "product_name": "Steel Plate",
      "failure_category": "validation_error",
      "error_message": "Validation error: Invalid price value",
      "serialized_payload": {...}
    }
  ]
}
```

The DLQ uses the existing S3 dead letter infrastructure (`products/dead_letter/rfq_sync/` prefix), which maps to the `s3_dead_letter_prefix` setting in `Settings`.

### Replay Strategy

Replay should support:

- Single-record retry after source data correction
- Batch retry for transient incidents
- Auditability of the original error and replay outcome

**Current Status:** Basic DLQ persistence implemented. Operational tooling for replay requires additional development (see Phase 4 checklist).

---

## Monitoring and Observability

### Dagster Metadata

Each asset should emit structured metadata visible in the Dagster UI:

| Metric | Type | Purpose |
|--------|------|---------|
| `rfq_sync_total` | int | Total products processed |
| `rfq_sync_succeeded` | int | Successful upserts |
| `rfq_sync_failed` | int | Failed upserts |
| `rfq_sync_created` | int | New records created |
| `rfq_sync_updated` | int | Existing records updated |
| `rfq_sync_skipped` | int | Products skipped (invalid) |

### Structured Log Fields

- Run ID and asset name
- WooCommerce product ID and SKU
- Item UUID and provider item UUID
- Outcome (created / updated / failed)
- Error code and message
- Retry count and duration

### Recommended Alert Thresholds

- Success rate below 95%
- API p95 latency above 5s
- DLQ backlog above agreed operational limit
- Run duration materially above baseline

---

## Testing Strategy

### Unit Tests

- Field mapping correctness (WooCommerce -> RFQ payloads)
- HTML stripping and description sanitization
- Price parsing and decimal normalization (including edge cases: empty string, `"0"`, negative, non-numeric)
- Fallback behavior for missing fields
- Invalid product rejection with proper error messages
- Tier extraction when enabled

### Integration Tests

- GraphQL request formatting and variable structure
- Authentication headers (`x-api-key`, `Part-Id`)
- Retry behavior on transient errors
- Rate limiting compliance
- Error parsing from GraphQL responses

### End-to-End Tests

- Full asset dependency execution through Dagster
- Successful sync of representative products
- Handling of partial failures within a batch
- DLQ persistence and structure
- Replay flow for failed records

Use mocked RFQ API responses by default. Run against a staging endpoint before production cutover.

---

## Deployment Guidance

### Rollout Plan

1. Implement resource, transformation, and asset code
2. Register `rfq_sync_job`, `RfqResource`, and all `rfq` group assets in `definitions.py`
3. Validate against mocked RFQ API responses in unit/integration tests
4. Run a limited sync subset against a staging RFQ endpoint
5. Verify entity creation, update, and idempotent re-run behavior
6. Enable production with a controlled catalog subset
7. Monitor success rate, latency, and duplicate creation risk
8. Expand to full scheduled sync

### Scheduling

**Scope v1:** Daily or hourly scheduled sync, depending on catalog change frequency.

Prefer scheduled execution first. Add event-driven sensors only after the batch pipeline is stable and operationally understood.

### Partitioning

For large catalogs, consider partitioning by:

- Category or brand
- Product update date
- Alphabetical SKU ranges

Partitioning is useful when throughput or run duration becomes a bottleneck but should only be introduced after the base sync path is proven.

---

## Security Considerations

- Store API keys in a secrets manager, not in plaintext environment files committed to source control
- Use least-privilege API keys for the RFQ Engine
- Enforce TLS for all API communication
- Avoid logging API keys, `x-api-key` header values, or sensitive payload fragments
- Mask customer-related metadata if such data is ever introduced into the flow
- Maintain an audit trail of sync runs and replay actions

Token rotation and secret refresh should be part of standard operational runbooks.

---

## Future Enhancements

### Event-Driven Sync

Use WooCommerce webhooks or change polling to reduce full-catalog scans and improve data freshness.

### Delta Sync

Track `date_modified` and only process products changed since the last successful run. This reduces API load and run duration for large catalogs.

### Advanced Pricing

Map plugin-specific tier or customer-segment pricing into `ItemPriceTier` once source formats are standardized.

Current engine constraints that defer this to a later phase:

- New `ItemPriceTier` creation requires both `provider_item_uuid` and `segment_uuid`
- The engine manages `quantity_less_than` internally when appending a new tier
- WooCommerce product data typically does not include RFQ segment identity

### Reconciliation

Introduce periodic full reconciliation to detect:

- Products present in WooCommerce but missing in RFQ Engine
- Orphaned RFQ records with no matching WooCommerce product (including products deleted or unpublished since last sync)
- Drift in price or category mappings

### Multi-Tenant Support

Generalize the resource and schedule configuration so multiple WooCommerce stores can sync to separate RFQ partitions.

---

## Implementation Checklist

### Phase 1: Foundation ✅

- [x] Add RFQ Engine settings to `dagster_ecommerce/config/settings.py` (as Pydantic fields) and `.env.example`
- [x] Implement `RfqResource` in `dagster_ecommerce/resources/rfq_resource.py`
- [x] Implement transformation helpers in `dagster_ecommerce/transformations/woo_to_rfq.py`
- [x] Add HTML sanitization, price normalization, and `item_spec` builder utilities

### Phase 2: Pipeline Integration ✅

- [x] Create `woo_products_for_rfq` asset (fetch from WooCommerce using `woo.get_products`)
- [x] Create `rfq_items_source` asset (transform and validate)
- [x] Create `rfq_items_synced` asset (upsert Item + ProviderItem via GraphQL)
- [x] Add lookup-before-write logic for idempotent `Item` and `ProviderItem` upserts
- [x] Register assets, resources, and `rfq_sync_job` in `definitions.py`
- [ ] Add optional `rfq_price_tiers_synced` asset behind the `RFQ_ENGINE_ENABLE_PRICE_TIERS` feature flag (Scope v2—wire the asset but keep it disabled by default)

### Phase 3: Reliability ✅

- [x] Add retry with exponential backoff (1s, 2s, 4s) for transient failures
- [x] Add client-side rate limiting
- [x] Persist structured failure records to the S3 dead letter queue
- [ ] Add replay path for failed records (requires operational tooling)
- [x] Emit Dagster metadata for all sync outcomes

### Phase 4: Testing and Cutover

- [ ] Add unit tests for mapping, validation, price normalization, and HTML stripping
- [ ] Add integration tests for RFQ API behavior
- [ ] Run staging verification with sample products
- [ ] Execute controlled production rollout
- [ ] Update `ARCHITECTURE.md` and `DEVELOPMENT_PLAN.md` to reflect the RFQ pipeline

---

## Summary

The recommended Scope v1 design is a scheduled, idempotent Dagster pipeline that fetches WooCommerce product data via the REST API, transforms it into compact RFQ payloads, and upserts `Item` and `ProviderItem` entities through the RFQ Engine GraphQL API.

The implementation should stay narrow at first: strong identity rules, predictable batching, durable failure handling, and clear observability. This narrower scope provides a stable operational baseline before adding delta sync, tiered pricing, reconciliation, or real-time event processing.

---

## Version History

| Version | Date | Summary |
|---------|------|---------|
| 6.2 | 2026-03-29 | Added `rfq_dead_letter_queue` to asset table and dependency diagram; added resource key column to asset table; clarified data flow diagram with explicit DLQ asset linkage |
| 6.1 | 2026-03-29 | Updated documentation to reflect actual implementation: Phase 1-3 checklist items marked complete, added settings field mapping details, updated configuration documentation |
| 6.0 | 2026-03-29 | Enhanced clarity, fixed grammar, removed redundant verification notes, improved flow and consistency throughout document |
| 5.1 | 2026-03-29 | Added `InsertUpdateProviderItem` mutation and lookup query specs; fixed field names with verification note; renamed scope phases to v1/v2; expanded Required Request Headers table; added decimal normalization rules and `item_spec` example; added WooCommerce pagination note; clarified retry backoff sequence; noted `max_retries` shared setting; added `definitions.py` registration note; moved Open Decisions before Data Model; added deleted-product out-of-scope note; added version history |
| 5.0 | 2026-03-28 | Previous release |

---

**Document Version:** 6.2
**Last Updated:** 2026-03-29
