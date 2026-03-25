"""Product Processing Assets.

This module provides Dagster assets for validating and transforming
raw product data into WooCommerce-compatible format.
"""

from __future__ import annotations

from typing import Any

from dagster import MetadataValue, asset

from dagster_ecommerce.models.product import (
    WooAttribute,
    WooCategory,
    WooDimensions,
    WooImage,
    WooProduct,
)
from dagster_ecommerce.transformations.shopify_to_woo import (
    consolidate_shopify_rows,
    is_shopify_format,
    map_shopify_to_woo,
)


def validate_product(
    record: dict[str, Any], seen_skus: set[str]
) -> tuple[WooProduct | None, list[str]]:
    """Validate a single product record according to WooCommerce requirements.

    Performs comprehensive validation of product data including:
    - Required field checks (name, price)
    - SKU uniqueness within the current batch
    - Data type validation for prices, quantities, and statuses
    - WooCommerce API compatibility validation

    Args:
        record: Raw product record from source data
        seen_skus: Set of SKUs already encountered (for batch uniqueness validation)

    Returns:
        Tuple of (validated WooProduct or None, list of validation errors)

    Note:
        A product with validation errors will not be synced to WooCommerce.
        Errors are collected in the dead letter queue for manual review.
    """
    errors = []

    # Validate required fields
    if not record.get("name"):
        errors.append("Missing required field: name")

    # A product must have either a price or regular_price to be valid
    price_field = record.get("price") or record.get("regular_price")
    if not price_field:
        errors.append("Missing required field: price or regular_price")

    # Validate SKU uniqueness within current batch
    sku = record.get("sku")
    if sku:
        if sku in seen_skus:
            errors.append(f"Duplicate SKU in batch: {sku}")
        seen_skus.add(sku)

    # Validate price format (must be numeric and non-negative)
    if price_field:
        try:
            price_float = float(price_field)
            if price_float < 0:
                errors.append(f"Invalid price format: negative value {price_field}")
        except (ValueError, TypeError):
            errors.append(f"Invalid price format: {price_field}")

    # Validate stock status against WooCommerce accepted values
    stock_status = record.get("stock_status")
    if stock_status and stock_status not in ("instock", "outofstock", "onbackorder"):
        errors.append(
            f"Invalid stock_status: {stock_status} (must be instock, outofstock, or onbackorder)"
        )

    # Validate product type against WooCommerce accepted values
    product_type = record.get("type", "simple")
    valid_types = ("simple", "variable", "grouped", "external")
    if product_type not in valid_types:
        errors.append(
            f"Invalid product type: {product_type} (must be one of {', '.join(valid_types)})"
        )

    # Validate status against WooCommerce accepted values
    status = record.get("status", "publish")
    valid_statuses = ("publish", "draft", "pending", "private")
    if status not in valid_statuses:
        errors.append(
            f"Invalid status: {status} (must be one of {', '.join(valid_statuses)})"
        )

    # Return early if we have errors to prevent unnecessary processing
    if errors:
        return (None, errors)

    # Build validated WooCommerce product model
    try:
        # Handle optional fields with sensible defaults
        stock_qty = record.get("stock_quantity")
        stock_quantity = int(stock_qty) if stock_qty is not None else None

        # Apply pricing logic according to API_MAPPING.md
        # Regular price is the higher of price or regular_price
        # Sale price is the lower price when both are present
        sale_price = record.get("sale_price")
        if price_field and sale_price:
            regular_price = str(max(float(price_field), float(sale_price)))
            sale_price = str(min(float(price_field), float(sale_price)))
        else:
            regular_price = str(price_field)
            sale_price = str(sale_price) if sale_price else None

        product = WooProduct(
            # Required fields
            name=record.get("name", ""),
            regular_price=regular_price,
            # Identifiers
            sku=sku,
            id=None,  # Will be assigned by WooCommerce during sync
            # Type and status
            type=product_type,
            status=status,
            featured=parse_bool(record.get("featured", False)),
            # Pricing
            sale_price=sale_price,
            tax_class=record.get("tax_class", "") or "",
            tax_status=record.get("tax_status", "taxable") or "taxable",
            # Inventory
            manage_stock=parse_bool(record.get("manage_stock", False)),
            stock_quantity=stock_quantity,
            stock_status=stock_status or "instock",
            backorders=record.get("backorders", "no") or "no",
            low_stock_amount=None,  # Not typically in source data
            sold_individually=parse_bool(record.get("sold_individually", False)),
            # Shipping
            weight=str(record.get("weight")) if record.get("weight") else None,
            dimensions=parse_dimensions(record),
            shipping_class=record.get("shipping_class"),
            virtual=parse_bool(record.get("virtual", False)),
            downloadable=parse_bool(record.get("downloadable", False)),
            # Descriptions
            description=record.get("description"),
            short_description=record.get("short_description"),
            # Categories and tags
            categories=parse_categories(record.get("categories")),
            tags=parse_tags(record.get("tags")),
            # Images
            images=parse_images(record.get("images")),
            # Attributes
            attributes=parse_attributes(record.get("attributes")),
            variations=None,  # Will be populated by WooCommerce during sync
            parent_id=None,  # Will be populated by WooCommerce during sync
            # Metadata
            meta_data=None,  # Could add source metadata here if needed
        )
        return (product, [])
    except Exception as e:
        errors.append(f"Validation error during model construction: {str(e)}")
        return (None, errors)


def parse_bool(value: Any) -> bool:
    """Parse boolean from various formats."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)


def parse_dimensions(record: dict[str, Any]) -> WooDimensions | None:
    """Parse dimensions from record."""
    length = record.get("length") or record.get("dimensions", {}).get("length")
    width = record.get("width") or record.get("dimensions", {}).get("width")
    height = record.get("height") or record.get("dimensions", {}).get("height")

    if any([length, width, height]):
        return WooDimensions(
            length=str(length) if length else None,
            width=str(width) if width else None,
            height=str(height) if height else None,
        )
    return None


def parse_categories(value: Any) -> list[WooCategory] | None:
    """Parse categories from various formats."""
    if not value:
        return None

    if isinstance(value, str):
        # Comma-separated names
        names = [n.strip() for n in value.split(",") if n.strip()]
        return [WooCategory(name=name) for name in names]

    if isinstance(value, list):
        categories = []
        for item in value:
            if isinstance(item, str):
                categories.append(WooCategory(name=item))
            elif isinstance(item, dict):
                categories.append(
                    WooCategory(
                        id=item.get("id"),
                        name=item.get("name"),
                        slug=item.get("slug"),
                    )
                )
        return categories if categories else None

    return None


def parse_tags(value: Any) -> list[dict] | None:
    """Parse tags from various formats."""
    if not value:
        return None

    if isinstance(value, str):
        # Comma-separated names
        names = [n.strip() for n in value.split(",") if n.strip()]
        return [{"name": name} for name in names]

    if isinstance(value, list):
        tags = []
        for item in value:
            if isinstance(item, str):
                tags.append({"name": item})
            elif isinstance(item, dict):
                tags.append(
                    {
                        "id": item.get("id"),
                        "name": item.get("name"),
                    }
                )
        return tags if tags else None

    return None


def parse_images(value: Any) -> list[WooImage] | None:
    """Parse images from various formats."""
    if not value:
        return None

    if isinstance(value, str):
        # Comma-separated URLs
        urls = [u.strip() for u in value.split(",") if u.strip()]
        return [WooImage(src=url, position=i) for i, url in enumerate(urls)]

    if isinstance(value, list):
        images = []
        for i, item in enumerate(value):
            if isinstance(item, str):
                images.append(WooImage(src=item, position=i))
            elif isinstance(item, dict):
                images.append(
                    WooImage(
                        id=item.get("id"),
                        src=item.get("src"),
                        name=item.get("name"),
                        alt=item.get("alt"),
                        position=item.get("position", i),
                    )
                )
        return images if images else None

    return None


def parse_attributes(value: Any) -> list[WooAttribute] | None:
    """Parse product attributes."""
    if not value or not isinstance(value, list):
        return None

    attributes = []
    for item in value:
        if isinstance(item, dict):
            options = item.get("options", [])
            if isinstance(options, str):
                options = [o.strip() for o in options.split(",")]

            attributes.append(
                WooAttribute(
                    id=item.get("id"),
                    name=item.get("name", ""),
                    position=item.get("position", 0),
                    visible=item.get("visible", True),
                    variation=item.get("variation", False),
                    options=options,
                )
            )

    return attributes if attributes else None


def transform_records(
    records: list[dict[str, Any]],
    source_format: str | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """Apply format-specific transformation to produce WooCommerce-compatible records.

    Auto-detects the source format when not specified.

    Supported formats:
        - "shopify": Shopify CSV multi-row export
        - "woocommerce": Already WooCommerce-compatible (passthrough)

    Args:
        records: Raw product records
        source_format: Force a specific format instead of auto-detecting

    Returns:
        Tuple of (transformed records, detected format name)
    """
    if not records:
        return [], "empty"

    detected = source_format or (
        "shopify" if is_shopify_format(records) else "woocommerce"
    )

    if detected == "shopify":
        consolidated = consolidate_shopify_rows(records)
        transformed = [map_shopify_to_woo(p) for p in consolidated]
        return transformed, "shopify"

    return records, "woocommerce"


@asset(
    description="Validated product records split into valid/invalid batches",
    compute_kind="python",
    group_name="processing",
)
def validated_products(
    context,
    raw_product_data: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Transform and validate raw product records.

    Auto-detects the source format (Shopify CSV, WooCommerce-native, etc.)
    and applies the appropriate transformation before validation.

    Args:
        context: Dagster asset execution context
        raw_product_data: Raw product records from raw_product_data asset

    Returns:
        Tuple of (valid_records, invalid_records)
        Each invalid record has 'record' and 'errors' keys.
    """
    # Transform to WooCommerce field names based on detected format
    products, detected_format = transform_records(raw_product_data)
    context.log.info(
        f"Format detected: {detected_format} — "
        f"{len(raw_product_data)} raw rows → {len(products)} products"
    )

    valid_products: list[dict[str, Any]] = []
    invalid_products: list[dict[str, Any]] = []
    seen_skus: set[str] = set()

    for i, record in enumerate(products):
        product, errors = validate_product(record, seen_skus)

        if product:
            valid_products.append(product.model_dump(exclude_none=True))
        else:
            invalid_products.append(
                {
                    "record": record,
                    "errors": errors,
                    "index": i,
                }
            )

    context.log.info(
        f"Validated: {len(valid_products)} valid, {len(invalid_products)} invalid"
    )

    context.add_output_metadata(
        {
            "source_format": MetadataValue.text(detected_format),
            "raw_rows": MetadataValue.int(len(raw_product_data)),
            "products_after_transform": MetadataValue.int(len(products)),
            "valid_count": MetadataValue.int(len(valid_products)),
            "invalid_count": MetadataValue.int(len(invalid_products)),
            "validation_rate": MetadataValue.float(
                len(valid_products) / len(products) if products else 0.0
            ),
            "sample_errors": MetadataValue.json(invalid_products[:5]),
        }
    )

    return valid_products, invalid_products
