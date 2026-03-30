"""Knowledge Graph API resource for extracting product data into a knowledge graph."""

from __future__ import annotations

import json

import requests
from dagster import Field, String, get_dagster_logger, resource

logger = get_dagster_logger()

EXTRACT_MUTATION = """
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
"""


class KnowledgeGraphResource:
    """Knowledge Graph Engine API client.

    Sends product data to a GraphQL-based knowledge graph extraction endpoint
    that parses product text into entities and relationships.

    Supports two authentication methods:
    - x-api-key header (primary, always used)
    - Authorization: Bearer header (optional, for additional auth)
    """

    def __init__(
        self,
        url: str,
        endpoint_id: str,
        part_id: str,
        api_key: str | None = None,
        bearer_token: str | None = None,
        timeout: int = 120,
    ):
        self.url = url.rstrip("/")
        self.endpoint_id = endpoint_id
        self.part_id = part_id
        self.api_key = api_key
        self.bearer_token = bearer_token
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Content-Type": "application/json",
                "Part-Id": self.part_id,
            }
        )
        if self.api_key:
            self._session.headers.update({"x-api-key": self.api_key})
        if self.bearer_token:
            self._session.headers.update(
                {"Authorization": f"Bearer {self.bearer_token}"}
            )

    @property
    def graphql_endpoint(self) -> str:
        return f"{self.url}/{self.endpoint_id}/knowledge_graph_engine_graphql"

    def extract_product(
        self,
        text: str,
        document_source: str = "woocommerce",
        document_external_id: str | None = None,
        graph_schema: dict | None = None,
    ) -> dict:
        """Send a product text to the knowledge graph for entity/relationship extraction.

        Args:
            text: Formatted product text for extraction.
            document_source: Source identifier (default: "woocommerce").
            document_external_id: Unique external ID for the document.
            graph_schema: Optional graph schema to guide extraction.

        Returns:
            Dict with extraction results (status, entities, relationships, etc.).
        """
        variables = {
            "text": text,
            "documentSource": document_source,
            "documentExternalId": document_external_id,
        }
        if graph_schema:
            variables["graphSchema"] = graph_schema

        payload = {
            "query": EXTRACT_MUTATION,
            "variables": variables,
        }

        response = self._session.post(
            self.graphql_endpoint,
            data=json.dumps(payload),
            timeout=self.timeout,
        )
        response.raise_for_status()

        result = response.json()

        if "errors" in result:
            raise RuntimeError(
                f"Knowledge Graph extraction failed: {result['errors']}"
            )

        return result.get("data", {}).get("executeExtract", {})

    def format_product_text(self, product: dict) -> str:
        """Convert a WooCommerce product dict into a text representation for extraction.

        Args:
            product: WooCommerce product data dict.

        Returns:
            Formatted text string describing the product.
        """
        lines = []

        if product.get("name"):
            lines.append(f"Product: {product['name']}")
        if product.get("sku"):
            lines.append(f"SKU: {product['sku']}")
        if product.get("type"):
            lines.append(f"Type: {product['type']}")
        if product.get("status"):
            lines.append(f"Status: {product['status']}")
        if product.get("regular_price"):
            lines.append(f"Price: {product['regular_price']}")
        if product.get("sale_price"):
            lines.append(f"Sale Price: {product['sale_price']}")
        if product.get("description"):
            lines.append(f"Description: {product['description']}")
        if product.get("short_description"):
            lines.append(f"Short Description: {product['short_description']}")

        # Categories
        categories = product.get("categories")
        if categories:
            cat_names = [
                c["name"] if isinstance(c, dict) else str(c) for c in categories
            ]
            lines.append(f"Categories: {', '.join(cat_names)}")

        # Tags
        tags = product.get("tags")
        if tags:
            tag_names = [
                t["name"] if isinstance(t, dict) else str(t) for t in tags
            ]
            lines.append(f"Tags: {', '.join(tag_names)}")

        # Attributes
        attributes = product.get("attributes")
        if attributes:
            for attr in attributes:
                if isinstance(attr, dict):
                    name = attr.get("name", "")
                    options = attr.get("options", [])
                    if name and options:
                        lines.append(f"{name}: {', '.join(options)}")

        # Inventory
        stock_status = product.get("stock_status")
        if stock_status:
            in_stock = stock_status == "instock"
            lines.append(f"In Stock: {in_stock}")

        if product.get("weight"):
            lines.append(f"Weight: {product['weight']}")

        return "\n".join(lines)

    def build_external_id(self, product: dict) -> str:
        """Build a document external ID from product data.

        Args:
            product: WooCommerce product data dict.

        Returns:
            External ID string like "product-SKU" or "product-NAME-slug".
        """
        sku = product.get("sku")
        if sku:
            return f"product-{sku}"

        name = product.get("name", "unknown")
        slug = name.lower().replace(" ", "-")[:50]
        return f"product-{slug}"


@resource(
    config_schema={
        "url": Field(String, description="Knowledge Graph Engine base URL"),
        "endpoint_id": Field(String, description="Endpoint ID"),
        "part_id": Field(String, description="Partition ID"),
        "api_key": Field(
            String,
            default_value="",
            is_required=False,
            description="Optional x-api-key header value",
        ),
        "bearer_token": Field(
            String,
            default_value="",
            is_required=False,
            description="Optional Authorization: Bearer token",
        ),
        "timeout": Field(
            int,
            default_value=120,
            description="Request timeout in seconds",
        ),
    },
    description="Knowledge Graph Engine API client",
)
def knowledge_graph_resource(context):
    config = context.resource_config
    return KnowledgeGraphResource(
        url=config["url"],
        endpoint_id=config["endpoint_id"],
        part_id=config["part_id"],
        api_key=config.get("api_key") or None,
        bearer_token=config.get("bearer_token") or None,
        timeout=config["timeout"],
    )
