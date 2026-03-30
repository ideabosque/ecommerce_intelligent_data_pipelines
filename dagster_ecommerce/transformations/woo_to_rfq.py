"""WooCommerce to RFQ Engine field transformation.

This module transforms WooCommerce product data into RFQ Engine payloads
for Item and ProviderItem entities.
"""

from __future__ import annotations

import re
from html import unescape
from typing import Any


def strip_html(text: str | None) -> str:
    """Strip HTML tags from text and normalize whitespace.

    Args:
        text: HTML text to clean

    Returns:
        Plain text with HTML tags removed
    """
    if not text:
        return ""

    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_price(raw: str | None) -> float | None:
    """Normalize a price string to a positive float with 4 decimal places.

    Args:
        raw: Price string from WooCommerce (e.g., "10.00" or "")

    Returns:
        Normalized positive float, or None if invalid
    """
    if not raw:
        return None
    try:
        value = float(raw)
        if value > 0:
            return round(value, 4)
        return None
    except (ValueError, TypeError):
        return None


def extract_category(woo_product: dict) -> str | None:
    """Extract the first category name from a WooCommerce product.

    Args:
        woo_product: WooCommerce product dict

    Returns:
        Category name or "uncategorized" as fallback
    """
    categories = woo_product.get("categories", [])
    if categories:
        first_cat = categories[0]
        if isinstance(first_cat, dict):
            cat_name = first_cat.get("name", "")
        else:
            cat_name = str(first_cat)
        if cat_name:
            return cat_name
    return "uncategorized"


def build_item_spec(woo_product: dict) -> dict:
    """Build the item_spec dict from WooCommerce product attributes.

    Args:
        woo_product: WooCommerce product dict

    Returns:
        Dict with structured product metadata
    """
    spec: dict[str, Any] = {}

    woo_id = woo_product.get("id")
    if woo_id:
        spec["woo_product_id"] = woo_id

    attributes = woo_product.get("attributes", [])
    if attributes:
        for attr in attributes:
            if isinstance(attr, dict):
                name = attr.get("name", "")
                options = attr.get("options", [])
                if name and options:
                    key = name.lower().replace(" ", "_")
                    if len(options) == 1:
                        spec[key] = options[0]
                    else:
                        spec[key] = options

    weight = woo_product.get("weight")
    if weight:
        try:
            weight_val = float(weight)
            if weight_val > 0:
                spec["weight_kg"] = round(weight_val, 3)
        except (ValueError, TypeError):
            pass

    dimensions = woo_product.get("dimensions")
    if dimensions and isinstance(dimensions, dict):
        dims = []
        for dim in ("length", "width", "height"):
            val = dimensions.get(dim)
            if val:
                dims.append(val)
        if dims:
            spec["dimensions_cm"] = "x".join(dims)

    stock_status = woo_product.get("stock_status")
    if stock_status:
        spec["stock_status"] = stock_status

    return spec


def validate_rfq_product(woo_product: dict) -> tuple[bool, list[str]]:
    """Validate a WooCommerce product for RFQ sync.

    Args:
        woo_product: WooCommerce product dict

    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []

    sku = woo_product.get("sku")
    if not sku or not sku.strip():
        errors.append("Missing SKU")

    name = woo_product.get("name")
    if not name or not name.strip():
        errors.append("Missing product name")

    return (len(errors) == 0, errors)


def transform_woo_to_rfq(woo_product: dict) -> dict | None:
    """Transform a WooCommerce product into RFQ Item and ProviderItem payloads.

    Args:
        woo_product: WooCommerce product dict

    Returns:
        Dict with item_payload and provider_item_payload, or None if invalid
    """
    is_valid, errors = validate_rfq_product(woo_product)
    if not is_valid:
        return None

    sku = woo_product.get("sku", "").strip()

    item_external_id = f"woo-{sku}"

    item_name = woo_product.get("name", "").strip()

    description = woo_product.get("description")
    if not description:
        description = woo_product.get("short_description", "")
    item_description = strip_html(description)

    item_type = extract_category(woo_product)

    uom = "EA"

    item_payload = {
        "item_type": item_type,
        "item_name": item_name,
        "item_description": item_description,
        "uom": uom,
        "item_external_id": item_external_id,
    }

    price_raw = woo_product.get("price")
    if not price_raw:
        price_raw = woo_product.get("regular_price")

    normalized_price = normalize_price(price_raw)

    provider_corp_external_id = "woocommerce"
    provider_item_external_id = sku

    item_spec = build_item_spec(woo_product)

    provider_item_payload = {
        "provider_corp_external_id": provider_corp_external_id,
        "provider_item_external_id": provider_item_external_id,
        "base_price_per_uom": normalized_price,
        "item_spec": item_spec if item_spec else None,
    }

    return {
        "item_payload": item_payload,
        "provider_item_payload": provider_item_payload,
        "errors": errors,
    }


def transform_to_item(woo_product: dict, default_uom: str = "EA") -> dict:
    """Transform WooCommerce product to RFQ Item payload.

    Args:
        woo_product: WooCommerce product dict
        default_uom: Default unit of measure (e.g., "EA", "kg")

    Returns:
        Item payload dict for RFQ Engine
    """
    sku = woo_product.get("sku", "").strip() or ""
    item_external_id = f"woo-{sku}"

    item_name = woo_product.get("name", "").strip()

    description = woo_product.get("description")
    if not description:
        description = woo_product.get("short_description", "")
    item_description = strip_html(description)

    item_type = extract_category(woo_product)

    return {
        "item_type": item_type,
        "item_name": item_name,
        "item_description": item_description,
        "uom": default_uom,
        "item_external_id": item_external_id,
    }


def transform_to_provider_item(
    woo_product: dict,
    item_uuid: str,
    provider_corp_external_id: str = "woocommerce",
) -> dict | None:
    """Transform WooCommerce product to RFQ ProviderItem payload.

    Args:
        woo_product: WooCommerce product dict
        item_uuid: UUID of the linked Item record
        provider_corp_external_id: Provider corp external ID (e.g., "woocommerce")

    Returns:
        ProviderItem payload dict, or None if no valid price
    """
    sku = woo_product.get("sku", "").strip()
    if not sku:
        return None

    price_raw = woo_product.get("price")
    if not price_raw:
        price_raw = woo_product.get("regular_price")

    normalized_price = normalize_price(price_raw)
    if normalized_price is None:
        return None

    item_spec = build_item_spec(woo_product)

    return {
        "item_uuid": item_uuid,
        "provider_corp_external_id": provider_corp_external_id,
        "provider_item_external_id": sku,
        "base_price_per_uom": normalized_price,
        "item_spec": item_spec if item_spec else None,
    }
