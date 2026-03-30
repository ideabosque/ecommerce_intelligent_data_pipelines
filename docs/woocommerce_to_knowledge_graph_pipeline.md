# Data Pipeline: WooCommerce to Knowledge Graph

**Project:** `ecommerce_intelligent_data_pipelines`  
**Document Version:** 1.0  
**Last Updated:** 2026-03-29

---

## Executive Summary

This document describes the pipeline that extracts product data from WooCommerce and transforms it into structured entities and relationships within a Knowledge Graph. The pipeline enables semantic search, AI reasoning, and knowledge discovery by parsing product text into graph-based representations.

### Key Capabilities

- **Automated Extraction**: Fetches all published products from WooCommerce
- **Text Transformation**: Converts product data into structured text for NLP processing
- **Entity Extraction**: Identifies and extracts entities from product descriptions
- **Relationship Mapping**: Discovers relationships between entities
- **GraphQL Integration**: Uses GraphQL API for knowledge graph mutations
- **Batch Processing**: Handles large catalogs efficiently

### Pipeline Relationship

This pipeline operates alongside other sync pipelines:

| Pipeline | Flow | Purpose |
|----------|------|---------|
| **S3 → WooCommerce** | `product_sync_job` | Ingest product files into WooCommerce |
| **WooCommerce → Knowledge Graph** | `knowledge_graph_sync_job` | Extract product semantics into knowledge graph |
| **WooCommerce → RFQ Engine** | `rfq_sync_job` | Sync products for quoting workflows |

---

## Architecture Overview

### High-Level Data Flow

```
WooCommerce REST API
    ↓
woo_products_for_kg (Asset)
    ↓
Product Text Transformation
    ↓
knowledge_graph_products (Asset)
    ↓
Knowledge Graph GraphQL API
    ↓
Entities + Relationships Stored
```

### System Components

#### Dagster Assets

| Asset | Purpose |
|-------|---------|
| `woo_products_for_kg` | Fetches all published products from WooCommerce via REST API |
| `knowledge_graph_products` | Transforms products into text and extracts entities/relationships |

#### Resources

- **WooCommerceResource**: Fetches products via REST API with pagination
- **KnowledgeGraphResource**: Manages GraphQL API access for entity extraction

#### Job

- **knowledge_graph_sync_job**: Orchestrates the complete extraction workflow

---

## Data Transformation and Mapping

### Product to Text Conversion

Each WooCommerce product is converted into a structured text representation suitable for NLP extraction:

| WooCommerce Field | Text Format |
|-------------------|-------------|
| `name` | `Product: {name}` |
| `sku` | `SKU: {sku}` |
| `type` | `Type: {type}` |
| `status` | `Status: {status}` |
| `regular_price` | `Price: {regular_price}` |
| `sale_price` | `Sale Price: {sale_price}` |
| `description` | `Description: {description}` |
| `short_description` | `Short Description: {short_description}` |
| `categories` | `Categories: {comma_separated_names}` |
| `tags` | `Tags: {comma_separated_names}` |
| `attributes` | `{name}: {comma_separated_options}` |
| `stock_status` | `In Stock: {true/false}` |
| `weight` | `Weight: {weight}` |

#### Example Transformation

**WooCommerce Product:**
```json
{
  "name": "Premium Steel Plate",
  "sku": "STEEL-001",
  "type": "simple",
  "regular_price": "49.99",
  "description": "High-grade carbon steel plate for industrial use.",
  "categories": [{"name": "Raw Materials"}, {"name": "Steel"}],
  "attributes": [
    {"name": "Material", "options": ["Carbon Steel"]},
    {"name": "Thickness", "options": ["5mm", "10mm"]}
  ]
}
```

**Transformed Text:**
```
Product: Premium Steel Plate
SKU: STEEL-001
Type: simple
Price: 49.99
Description: High-grade carbon steel plate for industrial use.
Categories: Raw Materials, Steel
Material: Carbon Steel
Thickness: 5mm, 10mm
```

### External ID Generation

Products are tracked in the knowledge graph using stable external identifiers:

| Pattern | Example |
|---------|---------|
| SKU-based | `product-STEEL-001` |
| Name-based (fallback) | `product-premium-steel-plate` |

**Priority:**
1. Use SKU if available: `product-{sku}`
2. Fallback to slugified name: `product-{name[:50]}`

---

## Knowledge Graph API Integration

### GraphQL Endpoint

```
POST {KNOWLEDGE_GRAPH_URL}/{ENDPOINT_ID}/knowledge_graph_engine_graphql
```

**Example:**
```
POST https://api.example.com/gpt/knowledge_graph_engine_graphql
```

### Authentication

| Header | Value |
|--------|-------|
| `Content-Type` | `application/json` |
| `Part-Id` | Partition identifier for data isolation |
| `Authorization` | `Bearer {bearer_token}` |

### Extraction Mutation

```graphql
mutation ExecuteExtract(
    $text: String!,
    $graphSchema: JSONCamelCase,
    $documentSource: String,
    $documentExternalId: String
) {
    executeExtract(
        text: $text,
        graphSchema: $graphSchema,
        documentSource: $documentSource,
        documentExternalId: $documentExternalId
    ) {
        status
        partitionKey
        documentUuid
        schemaName
        entitiesExtracted
        relationshipsExtracted
        result
    }
}
```

**Variables:**

| Variable | Type | Required | Description |
|----------|------|----------|-------------|
| `text` | String | Yes | Product text for extraction |
| `documentSource` | String | No | Source identifier (e.g., "woocommerce") |
| `documentExternalId` | String | No | Stable external ID for the document |
| `graphSchema` | JSONCamelCase | No | Optional schema to guide extraction |

**Response Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `status` | String | Extraction status |
| `partitionKey` | String | Data partition identifier |
| `documentUuid` | String | Generated document UUID |
| `schemaName` | String | Schema used for extraction |
| `entitiesExtracted` | Int | Number of entities extracted |
| `relationshipsExtracted` | Int | Number of relationships discovered |
| `result` | JSON | Additional extraction results |

---

## Processing Model

### Extraction Workflow

1. **Fetch Products**
   - Query WooCommerce REST API for all published products
   - Handle pagination automatically
   - Default: 100 products per page

2. **Transform to Text**
   - Convert each product dict to structured text
   - Include all relevant product fields
   - Preserve hierarchical relationships (categories, attributes)

3. **Extract Entities**
   - Send text to Knowledge Graph API
   - Track document source and external ID
   - Capture extraction metrics

4. **Track Results**
   - Count successful/failed extractions
   - Aggregate total entities and relationships
   - Log errors for troubleshooting

### Batch Processing

- Products are processed sequentially
- Each product extraction is independent
- Failures do not block subsequent products
- Results aggregated in final output metadata

---

## Configuration

### Environment Variables

```bash
# Knowledge Graph Configuration
KNOWLEDGE_GRAPH_URL=https://api.example.com
KNOWLEDGE_GRAPH_ENDPOINT_ID=gpt
KNOWLEDGE_GRAPH_PART_ID=partition-1
KNOWLEDGE_GRAPH_BEARER_TOKEN=your-bearer-token

# WooCommerce Configuration
WOOCOMMERCE_URL=https://your-store.com
WOOCOMMERCE_CONSUMER_KEY=your-consumer-key
WOOCOMMERCE_CONSUMER_SECRET=your-consumer-secret
WOOCOMMERCE_TIMEOUT=30
WOOCOMMERCE_REQUESTS_PER_SECOND=10
```

### Dagster Resource Configuration

```python
# Knowledge Graph Resource
knowledge_graph_resource.configured({
    "url": settings.knowledge_graph_url,
    "endpoint_id": settings.knowledge_graph_endpoint_id,
    "part_id": settings.knowledge_graph_part_id,
    "bearer_token": settings.knowledge_graph_bearer_token,
    "timeout": 120,
})

# WooCommerce Resource
woo_resource.configured({
    "url": settings.woocommerce_url,
    "consumer_key": settings.woocommerce_consumer_key,
    "consumer_secret": settings.woocommerce_consumer_secret,
    "timeout": settings.woocommerce_timeout,
    "requests_per_second": settings.woocommerce_requests_per_second,
})
```

### Job Definition

```python
knowledge_graph_sync_job = define_asset_job(
    name="knowledge_graph_sync_job",
    selection=AssetSelection.groups("knowledge_graph"),
    description="Fetch products from WooCommerce and extract into knowledge graph",
)
```

---

## Monitoring and Observability

### Dagster Metadata

Each run emits structured metadata:

| Metric | Type | Purpose |
|--------|------|---------|
| `product_count` | int | Total products fetched from WooCommerce |
| `total` | int | Total products processed |
| `succeeded` | int | Successful extractions |
| `failed` | int | Failed extractions |
| `total_entities` | int | Total entities extracted across all products |
| `total_relationships` | int | Total relationships discovered |
| `sample_skus` | list | Sample SKUs for verification |
| `errors` | list | Error details for failed products |

### Logging

- Progress logging with `[current/total]` counters
- Per-product success/failure status
- Debug logs for entity and relationship counts
- Warning logs for extraction failures

### Error Handling

- **Success**: Product extracted with entity/relationship counts
- **Failure**: Product name, SKU, and error message captured
- **Continue**: Processing continues despite individual failures

---

## Implementation Details

### Asset Code Structure

```python
# Asset 1: Fetch from WooCommerce
@asset(
    group_name="knowledge_graph",
    required_resource_keys={"woo"},
)
def woo_products_for_kg(context) -> list[dict]:
    """Fetch all published products."""
    woo = context.resources.woo
    products = woo.get_products(params={"per_page": 100, "status": "publish"})
    return products

# Asset 2: Extract to Knowledge Graph
@asset(
    group_name="knowledge_graph",
    required_resource_keys={"knowledge_graph"},
)
def knowledge_graph_products(
    context,
    woo_products_for_kg: list[dict],
) -> dict:
    """Extract products into knowledge graph."""
    kg = context.resources.knowledge_graph
    
    for product in woo_products_for_kg:
        text = kg.format_product_text(product)
        external_id = kg.build_external_id(product)
        
        result = kg.extract_product(
            text=text,
            document_source="woocommerce",
            document_external_id=external_id,
        )
        # Track results...
```

### Resource Implementation

```python
class KnowledgeGraphResource:
    """Knowledge Graph Engine API client."""
    
    def __init__(self, url, endpoint_id, part_id, bearer_token, timeout=120):
        self.url = url.rstrip("/")
        self.endpoint_id = endpoint_id
        self.part_id = part_id
        self.bearer_token = bearer_token
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Part-Id": self.part_id,
            "Authorization": f"Bearer {self.bearer_token}",
        })
    
    @property
    def graphql_endpoint(self) -> str:
        return f"{self.url}/{self.endpoint_id}/knowledge_graph_engine_graphql"
    
    def extract_product(self, text, document_source, document_external_id):
        """Send product text for extraction."""
        variables = {
            "text": text,
            "documentSource": document_source,
            "documentExternalId": document_external_id,
        }
        payload = {"query": EXTRACT_MUTATION, "variables": variables}
        response = self._session.post(
            self.graphql_endpoint,
            data=json.dumps(payload),
            timeout=self.timeout,
        )
        return response.json()["data"]["executeExtract"]
```

---

## Security Considerations

- Store bearer tokens in environment variables or secrets manager
- Use HTTPS for all API communication
- Implement least-privilege access for Knowledge Graph API
- Avoid logging sensitive product data
- Mask customer information if present in descriptions

---

## Future Enhancements

### Delta Sync

Track `date_modified` and only process products changed since last sync to reduce API load.

### Incremental Extraction

Use WooCommerce webhooks to trigger extractions immediately upon product changes.

### Schema Customization

Support custom graph schemas for domain-specific entity extraction.

### Multi-Store Support

Generalize to support multiple WooCommerce stores with separate knowledge graph partitions.

### Batch Optimization

Implement parallel processing for large catalogs while respecting API rate limits.

---

## Implementation Checklist

### Phase 1: Foundation

- [x] Knowledge Graph resource implementation
- [x] Text formatting utilities
- [x] External ID generation logic
- [x] GraphQL mutation integration

### Phase 2: Pipeline Integration

- [x] `woo_products_for_kg` asset (fetch products)
- [x] `knowledge_graph_products` asset (extract entities)
- [x] Asset dependency wiring
- [x] Job registration in definitions

### Phase 3: Reliability

- [x] Error handling per product
- [x] Extraction result tracking
- [x] Metadata emission for observability
- [x] Logging and progress indicators

### Phase 4: Testing and Optimization

- [ ] Unit tests for text formatting
- [ ] Integration tests with mocked KG API
- [ ] Performance benchmarking
- [ ] Documentation updates

---

## Version History

| Version | Date | Summary |
|---------|------|---------|
| 1.0 | 2026-03-29 | Initial documentation for WooCommerce to Knowledge Graph pipeline |

---

**Document Version:** 1.0  
**Last Updated:** 2026-03-29
