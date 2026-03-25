"""Shopify CSV to WooCommerce field transformation.

This module consolidates multi-row Shopify CSV exports into single
product records and maps Shopify field names to WooCommerce API fields.

Shopify CSV format:
- Multiple rows per product, grouped by Handle
- First row carries product-level fields (Title, Body, Vendor, etc.)
- Subsequent rows add images or variant option values
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def is_shopify_format(records: list[dict[str, Any]]) -> bool:
    """Detect whether records use Shopify CSV column names."""
    if not records:
        return False
    first = records[0]
    return "Handle" in first and "Title" in first


def consolidate_shopify_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group multi-row Shopify CSV data into one dict per product.

    Shopify exports use multiple rows per product:
    - Row with Title populated = primary product row
    - Rows without Title = continuation rows (extra images, variants)

    Args:
        rows: Flat list of CSV row dicts

    Returns:
        List of consolidated product dicts, one per Handle
    """
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        handle = (row.get("Handle") or "").strip()
        if not handle:
            continue
        groups[handle].append(row)

    products = []
    for handle, group_rows in groups.items():
        product = _consolidate_group(handle, group_rows)
        if product:
            products.append(product)

    return products


def _consolidate_group(
    handle: str, rows: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """Consolidate a group of rows sharing the same Handle."""
    # Find the primary row (first row with a Title)
    primary = None
    for row in rows:
        title = (row.get("Title") or "").strip()
        if title:
            primary = row
            break

    if primary is None:
        # All rows lack a Title — use the first row as fallback
        primary = rows[0]

    # Collect images from all rows
    images = []
    seen_srcs: set[str] = set()
    for row in rows:
        src = (row.get("Image Src") or "").strip()
        if src and src not in seen_srcs:
            seen_srcs.add(src)
            images.append(
                {
                    "src": src,
                    "alt": (row.get("Image Alt Text") or "").strip() or None,
                    "position": _parse_int(
                        row.get("Image Position"), fallback=len(images)
                    ),
                }
            )

    # Collect variant options to build attributes and detect product type
    option_values: dict[str, list[str]] = defaultdict(list)
    variant_skus: list[str] = []

    for row in rows:
        sku = (row.get("Variant SKU") or "").strip()
        if sku and sku not in variant_skus:
            variant_skus.append(sku)

        for n in (1, 2, 3):
            opt_name = (row.get(f"Option{n} Name") or "").strip()
            opt_value = (row.get(f"Option{n} Value") or "").strip()
            if opt_name and opt_value and opt_name != "Title":
                if opt_value not in option_values[opt_name]:
                    option_values[opt_name].append(opt_value)

    # Build consolidated dict
    consolidated = dict(primary)
    consolidated["_handle"] = handle
    consolidated["_images"] = sorted(images, key=lambda x: x.get("position", 0))
    consolidated["_option_values"] = dict(option_values)
    consolidated["_variant_skus"] = variant_skus
    consolidated["_is_variable"] = len(option_values) > 0 and any(
        len(vals) > 1 for vals in option_values.values()
    )

    return consolidated


def map_shopify_to_woo(consolidated: dict[str, Any]) -> dict[str, Any]:
    """Map a consolidated Shopify product dict to WooCommerce fields.

    Args:
        consolidated: Output from consolidate_shopify_rows

    Returns:
        Dict with WooCommerce-compatible field names
    """
    woo: dict[str, Any] = {}

    # Required fields
    woo["name"] = (consolidated.get("Title") or "").strip()
    woo["sku"] = (consolidated.get("Variant SKU") or "").strip() or None

    # Pricing
    variant_price = (consolidated.get("Variant Price") or "").strip()
    compare_price = (consolidated.get("Variant Compare At Price") or "").strip()

    if variant_price:
        woo["regular_price"] = variant_price
    if compare_price:
        # Compare At Price is typically the higher "was" price
        woo["sale_price"] = variant_price
        woo["regular_price"] = compare_price

    # Descriptions
    body_html = (consolidated.get("Body (HTML)") or "").strip()
    if body_html:
        woo["description"] = body_html

    seo_desc = (consolidated.get("SEO Description") or "").strip()
    if seo_desc:
        woo["short_description"] = seo_desc

    # Status mapping
    woo["status"] = _map_status(
        consolidated.get("Status"),
        consolidated.get("Published"),
    )

    # Product type
    if consolidated.get("_is_variable"):
        woo["type"] = "variable"
    else:
        woo["type"] = "simple"

    # Tags
    tags_str = (consolidated.get("Tags") or "").strip()
    vendor = (consolidated.get("Vendor") or "").strip()
    if vendor:
        vendor_tag = f"Vendor: {vendor}"
        tags_str = f"{tags_str}, {vendor_tag}" if tags_str else vendor_tag
    if tags_str:
        woo["tags"] = tags_str

    # Categories
    category_str = (consolidated.get("Product Category") or "").strip()
    shopify_type = (consolidated.get("Type") or "").strip()
    if category_str:
        # Shopify uses ">" for hierarchy — extract leaf categories
        categories = []
        for cat_path in category_str.split(","):
            parts = [p.strip() for p in cat_path.split(">")]
            leaf = parts[-1] if parts else ""
            if leaf:
                categories.append(leaf)
        woo["categories"] = ", ".join(categories)
    elif shopify_type:
        woo["categories"] = shopify_type

    # Images
    images = consolidated.get("_images", [])
    if images:
        woo["images"] = images

    # Attributes (from variant options)
    option_values = consolidated.get("_option_values", {})
    if option_values:
        attributes = []
        for i, (name, options) in enumerate(option_values.items()):
            attributes.append(
                {
                    "name": name,
                    "options": options,
                    "position": i,
                    "visible": True,
                    "variation": True,
                }
            )
        woo["attributes"] = attributes

    # Weight — Shopify Variant Grams is always in grams
    grams = consolidated.get("Variant Grams")
    if grams:
        try:
            weight_kg = float(grams) / 1000.0
            if weight_kg > 0:
                woo["weight"] = str(round(weight_kg, 3))
        except (ValueError, TypeError):
            pass

    # Virtual / downloadable
    gift_card = _parse_bool(consolidated.get("Gift Card"))
    requires_shipping = _parse_bool(consolidated.get("Variant Requires Shipping"))
    if gift_card or not requires_shipping:
        woo["virtual"] = True

    # Tax status
    taxable = consolidated.get("Variant Taxable")
    if taxable is not None:
        woo["tax_status"] = "taxable" if _parse_bool(taxable) else "none"

    # Featured — Shopify doesn't have this, default to False
    woo["featured"] = False

    # Preserve source handle for traceability
    handle = consolidated.get("_handle")
    if handle:
        woo["meta_data"] = [{"key": "shopify_handle", "value": handle}]

    # Preserve __source_key if present
    source_key = consolidated.get("__source_key")
    if source_key:
        woo["__source_key"] = source_key

    return woo


def _map_status(status: str | None, published: Any) -> str:
    """Map Shopify Status + Published fields to WooCommerce status.

    Shopify statuses: active, draft, archived
    WooCommerce statuses: publish, draft, pending, private
    """
    status_lower = (status or "").strip().lower()
    is_published = _parse_bool(published)

    if status_lower == "draft":
        return "draft"
    if status_lower == "archived":
        return "private"
    if status_lower == "active" and is_published:
        return "publish"
    if status_lower == "active" and not is_published:
        return "draft"
    return "draft"


def _parse_bool(value: Any) -> bool:
    """Parse boolean from various string representations."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    return bool(value) if value is not None else False


def _parse_int(value: Any, fallback: int = 0) -> int:
    """Parse integer with fallback."""
    if value is None:
        return fallback
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return fallback
