"""WooCommerce REST API resource with retry and rate-limit handling."""

from __future__ import annotations

import time

import requests
from dagster import Field, Int, String, get_dagster_logger, resource
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dagster_ecommerce.models.product import WooProduct

logger = get_dagster_logger()


class RateLimitError(Exception):
    """Raised when rate limit is exceeded."""


class WooCommerceError(Exception):
    """Base WooCommerce API error."""


class WooCommerceAuthError(WooCommerceError):
    """Authentication error."""


class WooCommerceNotFoundError(WooCommerceError):
    """Resource not found."""


class WooCommerceResource:
    """WooCommerce REST API resource.

    This resource provides methods for:
    - Listing products
    - Creating/updating products
    - Batch operations
    - SKU-based product matching

    Includes built-in rate limiting and retry logic.
    """

    def __init__(
        self,
        url: str,
        consumer_key: str,
        consumer_secret: str,
        timeout: int = 30,
        requests_per_second: int = 10,
        max_retries: int = 3,
    ):
        """Initialize WooCommerce resource.

        Args:
            url: WooCommerce API URL (e.g., https://store.com/wp-json/wc/v3)
            consumer_key: WooCommerce consumer key
            consumer_secret: WooCommerce consumer secret
            timeout: Request timeout in seconds
            requests_per_second: Rate limit for API calls
            max_retries: Maximum retry attempts
        """
        self.url = url.rstrip("/")
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.timeout = timeout
        self.requests_per_second = requests_per_second
        self.max_retries = max_retries

        # Rate limiting
        self._last_request_time = 0.0
        self._min_interval = 1.0 / requests_per_second

        # Create session with retry adapter
        self.session = requests.Session()
        retry_adapter = HTTPAdapter(
            max_retries=Retry(
                total=max_retries,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
        )
        self.session.mount("https://", retry_adapter)
        self.session.mount("http://", retry_adapter)

    def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict | None = None,
        json: dict | None = None,
    ) -> dict | list:
        """Make an authenticated request to WooCommerce API.

        Args:
            method: HTTP method
            endpoint: API endpoint (without base URL)
            params: Query parameters
            json: JSON body

        Returns:
            Response data

        Raises:
            WooCommerceAuthError: Authentication failed
            WooCommerceNotFoundError: Resource not found
            RateLimitError: Rate limit exceeded
            WooCommerceError: Other API errors
        """
        self._rate_limit()

        url = f"{self.url}{endpoint}"

        # WooCommerce requires OAuth for HTTP; basic auth only works over HTTPS
        is_https = self.url.startswith("https")
        auth = (self.consumer_key, self.consumer_secret) if is_https else None

        if not is_https:
            params = params or {}
            params["consumer_key"] = self.consumer_key
            params["consumer_secret"] = self.consumer_secret

        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json,
            auth=auth,
            timeout=self.timeout,
        )

        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
            time.sleep(retry_after)
            raise RateLimitError(f"Rate limit exceeded. Retry after {retry_after}s")

        # Handle errors
        if response.status_code == 401:
            raise WooCommerceAuthError("Authentication failed. Check API credentials.")

        if response.status_code == 404:
            raise WooCommerceNotFoundError(f"Resource not found: {endpoint}")

        if response.status_code >= 400:
            error_data = response.json() if response.content else {}
            error_message = error_data.get("message", response.text)
            raise WooCommerceError(f"API error {response.status_code}: {error_message}")

        return response.json() if response.content else {}

    # Product operations

    def get_product(self, product_id: int) -> dict:
        """Get a single product by ID.

        Args:
            product_id: Product ID

        Returns:
            Product data
        """
        return self._request("GET", f"/products/{product_id}")

    def get_products_by_sku(self, skus: list[str]) -> dict[str, dict]:
        """Get products by SKU.

        WooCommerce REST API only supports single-SKU lookup per request.
        This method queries each SKU individually and indexes the results.

        Args:
            skus: List of SKUs to look up

        Returns:
            Dictionary mapping SKU to product data
        """
        products_by_sku = {}

        for i, sku in enumerate(skus):
            if (i + 1) % 50 == 0:
                logger.debug(f"SKU lookup progress: {i + 1}/{len(skus)}")

            try:
                results = self._request("GET", "/products", params={"sku": sku, "per_page": 1})
                if isinstance(results, list) and results:
                    product = results[0]
                    product_sku = product.get("sku")
                    if product_sku:
                        products_by_sku[product_sku] = product
            except Exception as e:
                logger.warning(f"Failed to look up SKU {sku}: {e}")
                continue

        logger.info(f"Found {len(products_by_sku)} products across {len(skus)} requested SKUs")
        return products_by_sku

    def get_products(
        self,
        params: dict | None = None,
        max_pages: int = 100,
    ) -> list[dict]:
        """List products with pagination and safety guard.

        Args:
            params: Query parameters (e.g., per_page, page, sku, search)
            max_pages: Maximum pages to fetch (prevents infinite loops)

        Returns:
            List of products
        """
        params = params or {}
        params.setdefault("per_page", 100)

        products = []
        page = 1

        while page <= max_pages:
            params["page"] = page
            response = self._request("GET", "/products", params=params)

            if isinstance(response, list):
                products.extend(response)
                if len(response) < params["per_page"]:
                    break
            elif isinstance(response, dict):
                products.append(response)
                break
            else:
                logger.warning(f"Unexpected response type from /products: {type(response)}")
                break

            page += 1

        if page > max_pages:
            logger.warning(f"Reached max_pages limit ({max_pages}), results may be incomplete")

        logger.info(f"Retrieved {len(products)} products")
        return products

    def create_product(self, product: WooProduct | dict) -> dict:
        """Create a new product.

        Args:
            product: Product data (WooProduct model or dict)

        Returns:
            Created product data
        """
        if isinstance(product, WooProduct):
            data = product.model_dump(exclude_none=True, exclude={"id"})
        else:
            data = product

        result = self._request("POST", "/products", json=data)
        logger.info(f"Created product SKU={data.get('sku')}")
        return result

    def update_product(self, product_id: int, product: WooProduct | dict) -> dict:
        """Update an existing product.

        Args:
            product_id: Product ID
            product: Product data (WooProduct model or dict)

        Returns:
            Updated product data
        """
        if isinstance(product, WooProduct):
            data = product.model_dump(exclude_none=True, exclude={"id"})
        else:
            data = product

        result = self._request("PUT", f"/products/{product_id}", json=data)
        logger.debug(f"Updated product ID={product_id}")
        return result

    def delete_product(self, product_id: int, force: bool = False) -> dict:
        """Delete a product.

        Args:
            product_id: Product ID
            force: Force delete (skip trash)

        Returns:
            Deleted product data
        """
        params = {"force": "true"} if force else {}
        result = self._request("DELETE", f"/products/{product_id}", params=params)
        logger.info(f"Deleted product ID={product_id}")
        return result

    def batch_update_products(
        self,
        create: list[dict] | None = None,
        update: list[dict] | None = None,
        delete: list[int] | None = None,
    ) -> dict:
        """Batch create, update, and delete products.

        Args:
            create: Products to create
            update: Products to update (must include 'id')
            delete: Product IDs to delete

        Returns:
            Batch operation result
        """
        data = {}
        if create:
            data["create"] = create
        if update:
            data["update"] = update
        if delete:
            data["delete"] = delete

        result = self._request("POST", "/products/batch", json=data)
        logger.info(f"Batch operation: create={len(create or [])}, update={len(update or [])}, delete={len(delete or [])}")
        return result

    # Category operations

    def get_categories(self, params: dict | None = None) -> list[dict]:
        """List product categories.

        Args:
            params: Query parameters

        Returns:
            List of categories
        """
        return self._request("GET", "/products/categories", params=params)

    def get_category_by_name(self, name: str) -> dict | None:
        """Get category by name.

        Args:
            name: Category name

        Returns:
            Category data or None
        """
        results = self.get_categories(params={"search": name, "per_page": 1})
        for cat in results:
            if cat.get("name", "").lower() == name.lower():
                return cat
        return None

    # Tag operations

    def get_tags(self, params: dict | None = None) -> list[dict]:
        """List product tags.

        Args:
            params: Query parameters

        Returns:
            List of tags
        """
        return self._request("GET", "/products/tags", params=params)


@resource(
    config_schema={
        "url": Field(String, description="WooCommerce API URL"),
        "consumer_key": Field(String, description="WooCommerce consumer key"),
        "consumer_secret": Field(String, description="WooCommerce consumer secret"),
        "timeout": Field(Int, default_value=30, description="Request timeout in seconds"),
        "requests_per_second": Field(Int, default_value=10, description="Rate limit for API calls"),
        "max_retries": Field(Int, default_value=3, description="Maximum retry attempts"),
    },
    description="WooCommerce REST API resource.",
)
def woo_resource(context) -> WooCommerceResource:
    """Create WooCommerce resource from Dagster context."""
    config = context.resource_config
    return WooCommerceResource(
        url=config["url"],
        consumer_key=config["consumer_key"],
        consumer_secret=config["consumer_secret"],
        timeout=config["timeout"],
        requests_per_second=config["requests_per_second"],
        max_retries=config["max_retries"],
    )
