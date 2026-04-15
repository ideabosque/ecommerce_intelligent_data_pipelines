"""WooCommerce to Knowledge Graph field transformation.

This module transforms WooCommerce product data into Knowledge Graph payloads
for Item and ProviderItem entities.
"""

from __future__ import annotations

import re
from html import unescape
from typing import Any, Dict, List, Tuple, Optional


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


def extract_category(woo_product: dict) -> str:
    """Extract the first category name from a WooCommerce product.

    Args:
        woo_product: WooCommerce product dict

    Returns:
        Category name or "product" as fallback
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
    return "product"


def build_external_id(woo_product: dict) -> str:
    sku = woo_product.get("sku", "").strip() or ""
    item_external_id = f"woo-{sku}" if sku else f"woo-{woo_product.get('id', 'unknown')}"
    return item_external_id


def build_item_spec(woo_product: dict) -> dict:
    """Build the item_spec dict from WooCommerce product attributes.

    Args:
        woo_product: WooCommerce product dict

    Returns:
        Dict with structured product metadata
    """
    spec: Dict[str, Any] = {}

    # Basic product info
    woo_id = woo_product.get("id")
    if woo_id:
        spec["woo_product_id"] = woo_id

    sku = woo_product.get("sku")
    if sku:
        spec["sku"] = sku

    # Categories
    categories = woo_product.get("categories", [])
    if categories:
        spec["categories"] = [
            c["name"] if isinstance(c, dict) else str(c) for c in categories
        ]

    # Tags
    tags = woo_product.get("tags", [])
    if tags:
        spec["tags"] = [
            t["name"] if isinstance(t, dict) else str(t) for t in tags
        ]

    # Attributes
    attributes = woo_product.get("attributes", [])
    if attributes:
        spec["attributes"] = {}
        for attr in attributes:
            if isinstance(attr, dict):
                name = attr.get("name", "")
                options = attr.get("options", [])
                if name and options:
                    spec["attributes"][name] = options

    # Inventory
    stock_status = woo_product.get("stock_status")
    if stock_status:
        spec["stock_status"] = stock_status
        spec["in_stock"] = stock_status == "instock"

    # Weight and dimensions
    weight = woo_product.get("weight")
    if weight:
        try:
            weight_val = float(weight)
            if weight_val > 0:
                spec["weight"] = weight_val
        except (ValueError, TypeError):
            pass

    dimensions = woo_product.get("dimensions")
    if dimensions and isinstance(dimensions, dict):
        spec["dimensions"] = dimensions

    # Price information
    regular_price = woo_product.get("regular_price")
    if regular_price:
        spec["regular_price"] = regular_price

    sale_price = woo_product.get("sale_price")
    if sale_price:
        spec["sale_price"] = sale_price

    return spec


def validate_kg_product(woo_product: dict) -> Tuple[bool, List[str]]:
    """Validate a WooCommerce product for Knowledge Graph sync.

    Args:
        woo_product: WooCommerce product dict

    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors: List[str] = []

    # Check required fields
    sku = woo_product.get("sku")
    if not sku or not sku.strip():
        errors.append("Missing SKU")

    name = woo_product.get("name")
    if not name or not name.strip():
        errors.append("Missing product name")

    # Check price (optional but recommended)
    price = woo_product.get("price") or woo_product.get("regular_price")
    if price:
        normalized_price = normalize_price(price)
        if normalized_price is None:
            errors.append("Invalid price format")

    return (len(errors) == 0, errors)


def transform_to_item_text(
    woo_product: dict,
    item_uuid: Optional[str] = None,
    partition_key: Optional[str] = None,
) -> str:
    """Transform WooCommerce product to Knowledge Graph Item payload.

    Args:
        woo_product: WooCommerce product dict
        item_uuid: Item UUID from RFQ system

    Returns:
        Item payload text for Knowledge Graph
    """
    item_external_id = build_external_id(woo_product)

    item_name = woo_product.get("name", "").strip()

    # Get description
    description = woo_product.get("description")
    if not description:
        description = woo_product.get("short_description", "")
    item_description = strip_html(description)

    # Get item type from category
    item_type = extract_category(woo_product)

    lines = []
    lines.append(f"Partition Key: {partition_key}")
    lines.append(f"Name: {item_external_id}-{item_name}")
    lines.append(f"Item Name: {item_name}")
    lines.append(f"Item Type: {item_type}")
    lines.append(f"Item Description: {item_description}")
    lines.append(f"Item External ID: {item_external_id}")
    lines.append(f"Item UUID: {item_uuid}")
    lines.append(f"UOM: EA")

    return "\n".join(lines)
