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

GRAPH_SCHEMA = {
    "node_types": [
        {
            "label": "Item",
            "description": "A item with detailed information",
            "properties": [
                {"name": "partition_key", "type": "STRING", "required": True},
                {"name": "name", "type": "STRING", "required": True},
                {"name": "item_name", "type": "STRING", "required": True},
                {"name": "uom", "type": "STRING"},
                {"name": "item_type", "type": "STRING"},
                {"name": "item_description", "type": "STRING"},
                {"name": "item_uuid", "type": "STRING", "required": True},
                {"name": "item_external_id", "type": "STRING"},
                {"name": "created_at", "type": "ZONED_DATETIME"},
                {"name": "updated_at", "type": "ZONED_DATETIME"}
            ],
            # "constraints": [
            #     {"property_name": "item_external_id", "type": "UNIQUENESS", "node_type": "Item"}
            # ]
        }
    ],
    "relationship_types": [],
    "patterns": []
}


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
        return f"{self.url}/{self.endpoint_id}/knowledge_graph_graphql"

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
            "graphSchema": graph_schema if graph_schema else GRAPH_SCHEMA.get("item")
        }

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
