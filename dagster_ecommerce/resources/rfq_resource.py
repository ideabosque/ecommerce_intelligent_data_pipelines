"""RFQ Engine GraphQL API resource with retry and rate-limit handling."""

from __future__ import annotations

import json
import time

import requests
from dagster import Field, Int, String, get_dagster_logger, resource
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = get_dagster_logger()

INSERT_UPDATE_ITEM_MUTATION = """
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
"""

INSERT_UPDATE_PROVIDER_ITEM_MUTATION = """
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
"""

GET_ITEM_BY_EXTERNAL_ID_QUERY = """
query GetItemByExternalId($itemExternalId: String!) {
    item(itemExternalId: $itemExternalId) {
        itemUuid
        itemExternalId
    }
}
"""

GET_PROVIDER_ITEM_BY_EXTERNAL_ID_QUERY = """
query GetProviderItemByExternalId($providerItemExternalId: String!) {
    providerItem(providerItemExternalId: $providerItemExternalId) {
        providerItemUuid
        providerItemExternalId
    }
}
"""


class RfqEngineError(Exception):
    """Base RFQ Engine API error."""


class RfqEngineRateLimitError(RfqEngineError):
    """Raised when rate limit is exceeded."""


class RfqEngineValidationError(RfqEngineError):
    """Raised when API returns validation error."""


class RfqResource:
    """RFQ Engine GraphQL API client.

    This resource provides methods for:
    - Upserting Item records
    - Upserting ProviderItem records
    - Querying existing records by external ID for idempotent updates

    Supports two authentication methods:
    - x-api-key header (primary, always used)
    - Authorization: Bearer header (optional, for additional auth)

    Includes built-in rate limiting and retry logic.
    """

    def __init__(
        self,
        base_url: str,
        endpoint_id: str,
        part_id: str,
        api_key: str,
        bearer_token: str | None = None,
        updated_by: str = "dagster-pipeline",
        timeout: int = 60,
        requests_per_second: int = 5,
        max_retries: int = 3,
    ):
        """Initialize RFQ Engine resource.

        Args:
            base_url: RFQ Engine base URL (e.g., http://localhost:8000)
            endpoint_id: Endpoint ID (URL path segment, e.g., gpt)
            part_id: Partition ID for data isolation (Part-Id header)
            api_key: API key for authentication (x-api-key header, required)
            bearer_token: Optional bearer token for additional auth (Authorization header)
            updated_by: Actor name for updatedBy field
            timeout: Request timeout in seconds
            requests_per_second: Rate limit for API calls
            max_retries: Maximum retry attempts
        """
        self.base_url = base_url.rstrip("/")
        self.endpoint_id = endpoint_id
        self.part_id = part_id
        self.api_key = api_key
        self.bearer_token = bearer_token
        self.updated_by = updated_by
        self.timeout = timeout
        self.requests_per_second = requests_per_second
        self.max_retries = max_retries

        self._last_request_time = 0.0
        self._min_interval = 1.0 / requests_per_second

        self.session = requests.Session()

        # Always set x-api-key header (primary auth)
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "x-api-key": self.api_key,
                "Part-Id": self.part_id,
            }
        )

        # Optionally set Authorization header (secondary auth)
        if self.bearer_token:
            self.session.headers.update(
                {"Authorization": f"Bearer {self.bearer_token}"}
            )

        retry_adapter = HTTPAdapter(
            max_retries=Retry(
                total=max_retries,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
        )
        self.session.mount("https://", retry_adapter)
        self.session.mount("http://", retry_adapter)

    @property
    def graphql_endpoint(self) -> str:
        """Return the GraphQL endpoint URL."""
        return f"{self.base_url}/{self.endpoint_id}/ai_rfq_graphql"

    def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def _execute(
        self,
        query: str,
        variables: dict | None = None,
    ) -> dict:
        """Execute a GraphQL query/mutation.

        Args:
            query: GraphQL query or mutation string
            variables: Query variables

        Returns:
            GraphQL response data

        Raises:
            RfqEngineRateLimitError: Rate limit exceeded
            RfqEngineValidationError: Validation error from API
            RfqEngineError: Other API errors
        """
        self._rate_limit()

        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        response = self.session.post(
            self.graphql_endpoint,
            data=json.dumps(payload),
            timeout=self.timeout,
        )

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
            raise RfqEngineRateLimitError(
                f"Rate limit exceeded. Retry after {retry_after}s"
            )

        if response.status_code >= 400:
            raise RfqEngineError(f"HTTP error {response.status_code}: {response.text}")

        result = response.json()

        if "errors" in result:
            error_messages = [e.get("message", str(e)) for e in result["errors"]]
            error_msg = "; ".join(error_messages)
            if any(
                "validation" in e.get("message", "").lower() for e in result["errors"]
            ):
                raise RfqEngineValidationError(f"Validation error: {error_msg}")
            raise RfqEngineError(f"GraphQL error: {error_msg}")

        return result.get("data", {})

    def query_item_by_external_id(self, item_external_id: str) -> dict | None:
        """Query an Item by its external ID.

        Args:
            item_external_id: The external ID to search for

        Returns:
            Dict with itemUuid and itemExternalId, or None if not found
        """
        data = self._execute(
            GET_ITEM_BY_EXTERNAL_ID_QUERY,
            {"itemExternalId": item_external_id},
        )
        return data.get("item")

    def query_provider_item_by_external_id(
        self, provider_item_external_id: str
    ) -> dict | None:
        """Query a ProviderItem by its external ID.

        Args:
            provider_item_external_id: The external ID to search for

        Returns:
            Dict with providerItemUuid and providerItemExternalId, or None if not found
        """
        data = self._execute(
            GET_PROVIDER_ITEM_BY_EXTERNAL_ID_QUERY,
            {"providerItemExternalId": provider_item_external_id},
        )
        return data.get("providerItem")

    def upsert_item(
        self,
        item_type: str | None = None,
        item_name: str | None = None,
        item_description: str | None = None,
        uom: str | None = None,
        item_external_id: str | None = None,
        item_uuid: str | None = None,
    ) -> dict:
        """Upsert an Item record.

        Args:
            item_type: Product category/type
            item_name: Product name
            item_description: Product description (plain text)
            uom: Unit of measure
            item_external_id: External identifier (e.g., "woo-{sku}")
            item_uuid: Existing UUID for updates (omit for create)

        Returns:
            Upserted item data including itemUuid
        """
        variables = {
            "itemType": item_type,
            "itemName": item_name,
            "itemDescription": item_description,
            "uom": uom,
            "itemExternalId": item_external_id,
            "updatedBy": self.updated_by,
        }
        if item_uuid:
            variables["itemUuid"] = item_uuid

        data = self._execute(INSERT_UPDATE_ITEM_MUTATION, variables)

        if not data.get("insertUpdateItem", {}).get("item"):
            raise RfqEngineError(f"Failed to upsert item: {data}")

        return data["insertUpdateItem"]["item"]

    def upsert_provider_item(
        self,
        item_uuid: str,
        provider_item_uuid: str | None = None,
        provider_corp_external_id: str | None = None,
        provider_item_external_id: str | None = None,
        base_price_per_uom: float | None = None,
        item_spec: dict | None = None,
    ) -> dict:
        """Upsert a ProviderItem record.

        Args:
            item_uuid: UUID of the linked Item record
            provider_item_uuid: Existing UUID for updates (omit for create)
            provider_corp_external_id: Provider identifier (e.g., "woocommerce")
            provider_item_external_id: External identifier (usually SKU)
            base_price_per_uom: Price per unit of measure
            item_spec: Structured product metadata

        Returns:
            Upserted providerItem data including providerItemUuid
        """
        variables = {
            "itemUuid": item_uuid,
            "providerCorpExternalId": provider_corp_external_id,
            "providerItemExternalId": provider_item_external_id,
            "basePricePerUom": base_price_per_uom,
            "itemSpec": item_spec,
            "updatedBy": self.updated_by,
        }
        if provider_item_uuid:
            variables["providerItemUuid"] = provider_item_uuid

        data = self._execute(INSERT_UPDATE_PROVIDER_ITEM_MUTATION, variables)

        if not data.get("insertUpdateProviderItem", {}).get("providerItem"):
            raise RfqEngineError(f"Failed to upsert provider item: {data}")

        return data["insertUpdateProviderItem"]["providerItem"]


@resource(
    config_schema={
        "base_url": Field(String, description="RFQ Engine base URL"),
        "endpoint_id": Field(String, description="Endpoint ID (URL path segment)"),
        "part_id": Field(String, description="Part-Id header value"),
        "api_key": Field(String, description="x-api-key header value (required)"),
        "bearer_token": Field(
            String,
            default_value="",
            is_required=False,
            description="Optional Authorization: Bearer token",
        ),
        "updated_by": Field(
            String,
            default_value="dagster-pipeline",
            description="Actor for updatedBy field",
        ),
        "timeout": Field(
            Int,
            default_value=60,
            description="Request timeout in seconds",
        ),
        "requests_per_second": Field(
            Int,
            default_value=5,
            description="Rate limit for API calls",
        ),
        "max_retries": Field(
            Int,
            default_value=3,
            description="Maximum retry attempts",
        ),
    },
    description="RFQ Engine GraphQL API client",
)
def rfq_resource(context):
    """Create RFQ Engine resource from Dagster context."""
    config = context.resource_config
    return RfqResource(
        base_url=config["base_url"],
        endpoint_id=config["endpoint_id"],
        part_id=config["part_id"],
        api_key=config["api_key"],
        bearer_token=config.get("bearer_token") or None,
        updated_by=config["updated_by"],
        timeout=config["timeout"],
        requests_per_second=config["requests_per_second"],
        max_retries=config["max_retries"],
    )
