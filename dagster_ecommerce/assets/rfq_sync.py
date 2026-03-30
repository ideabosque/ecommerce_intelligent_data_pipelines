"""RFQ Sync Assets.

This module provides Dagster assets for syncing WooCommerce products
to the RFQ Engine via GraphQL API.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dagster import MetadataValue, asset

from dagster_ecommerce.config.settings import settings
from dagster_ecommerce.transformations.woo_to_rfq import (
    transform_to_item,
    transform_to_provider_item,
    validate_rfq_product,
)


@asset(
    description="Products fetched from WooCommerce for RFQ sync",
    compute_kind="api",
    group_name="rfq",
    required_resource_keys={"woo"},
)
def woo_products_for_rfq(context) -> list[dict]:
    """Fetch all published products from WooCommerce for RFQ sync.

    Returns:
        List of product dicts from WooCommerce.
    """
    woo = context.resources.woo

    context.log.info("Fetching all products from WooCommerce for RFQ sync...")
    products = woo.get_products(params={"per_page": 100, "status": "publish"})

    context.log.info(f"Fetched {len(products)} products from WooCommerce")

    context.add_output_metadata(
        {
            "product_count": MetadataValue.int(len(products)),
            "sample_skus": MetadataValue.json(
                [p.get("sku") for p in products[:10] if p.get("sku")]
            ),
        }
    )

    return products


@asset(
    description="WooCommerce products transformed to RFQ payloads",
    compute_kind="transform",
    group_name="rfq",
)
def rfq_items_source(
    context,
    woo_products_for_rfq: list[dict],
) -> list[dict]:
    """Transform WooCommerce products into RFQ Item and ProviderItem payloads.

    Validates each product and transforms valid ones into RFQ payloads.
    Products without SKU or with invalid data are skipped.

    Args:
        context: Dagster asset execution context
        woo_products_for_rfq: Products fetched from WooCommerce

    Returns:
        List of RFQ payload dicts ready for sync
    """
    if not woo_products_for_rfq:
        context.log.info("No products to transform for RFQ")
        context.add_output_metadata(
            {
                "total": MetadataValue.int(0),
                "valid": MetadataValue.int(0),
                "invalid": MetadataValue.int(0),
            }
        )
        return []

    context.log.info(
        f"Transforming {len(woo_products_for_rfq)} WooCommerce products into RFQ payloads..."
    )

    rfq_payloads = []
    invalid_products = []
    seen_skus: set[str] = set()

    for product in woo_products_for_rfq:
        sku = product.get("sku", "").strip()

        if not sku:
            invalid_products.append(
                {
                    "product": product,
                    "errors": ["Missing SKU"],
                }
            )
            continue

        if sku in seen_skus:
            invalid_products.append(
                {
                    "product": product,
                    "errors": [f"Duplicate SKU: {sku}"],
                }
            )
            continue

        seen_skus.add(sku)

        is_valid, errors = validate_rfq_product(product)
        if not is_valid:
            invalid_products.append(
                {
                    "product": product,
                    "errors": errors,
                }
            )
            continue

        item_payload = transform_to_item(product, default_uom=settings.rfq_engine_default_uom)

        rfq_payloads.append(
            {
                "woo_product": product,
                "item_payload": item_payload,
            }
        )

    context.log.info(
        f"Transformation complete: {len(rfq_payloads)} valid, {len(invalid_products)} invalid"
    )

    if invalid_products:
        context.log.warning(f"Skipped {len(invalid_products)} invalid products")

    context.add_output_metadata(
        {
            "total": MetadataValue.int(len(woo_products_for_rfq)),
            "valid": MetadataValue.int(len(rfq_payloads)),
            "invalid": MetadataValue.int(len(invalid_products)),
            "sample_skus": MetadataValue.json(
                [p["item_payload"]["item_external_id"] for p in rfq_payloads[:5]]
            ),
        }
    )

    return rfq_payloads


@asset(
    description="RFQ Items synced via GraphQL API",
    compute_kind="api",
    group_name="rfq",
    required_resource_keys={"rfq"},
)
def rfq_items_synced(
    context,
    rfq_items_source: list[dict],
) -> dict:
    """Sync RFQ Items and ProviderItems to the RFQ Engine.

    For each transformed product, performs lookup-before-write to ensure
    idempotent upserts. Creates or updates both Item and ProviderItem records.

    Args:
        context: Dagster asset execution context
        rfq_items_source: Transformed RFQ payloads

    Returns:
        Dict with sync results and metadata
    """
    rfq = context.resources.rfq

    if not rfq_items_source:
        context.log.info("No RFQ items to sync")
        context.add_output_metadata(
            {
                "total": MetadataValue.int(0),
                "items_created": MetadataValue.int(0),
                "items_updated": MetadataValue.int(0),
                "provider_items_created": MetadataValue.int(0),
                "provider_items_updated": MetadataValue.int(0),
                "failed": MetadataValue.int(0),
            }
        )
        return {
            "total": 0,
            "items_created": 0,
            "items_updated": 0,
            "provider_items_created": 0,
            "provider_items_updated": 0,
            "failed": 0,
            "errors": [],
        }

    context.log.info(f"Syncing {len(rfq_items_source)} RFQ items...")

    items_created = 0
    items_updated = 0
    provider_items_created = 0
    provider_items_updated = 0
    failed = 0
    errors = []

    for i, payload in enumerate(rfq_items_source):
        woo_product = payload["woo_product"]
        item_payload = payload["item_payload"]

        sku = woo_product.get("sku", "")
        item_external_id = item_payload["item_external_id"]
        product_name = woo_product.get("name", sku)

        try:
            # Check if item already exists (for idempotent upsert)
            existing_item = rfq.query_item_by_external_id(item_external_id)
            item_uuid = existing_item["itemUuid"] if existing_item else None

            if existing_item:
                items_updated += 1
                context.log.debug(
                    f"[{i + 1}/{len(rfq_items_source)}] Updating Item: {item_external_id}"
                )
            else:
                items_created += 1
                context.log.debug(
                    f"[{i + 1}/{len(rfq_items_source)}] Creating Item: {item_external_id}"
                )

            # Upsert the item
            item_result = rfq.upsert_item(
                item_uuid=item_uuid,
                item_type=item_payload.get("item_type"),
                item_name=item_payload.get("item_name"),
                item_description=item_payload.get("item_description"),
                uom=item_payload.get("uom"),
                item_external_id=item_payload.get("item_external_id"),
            )

            # Get the item UUID for linking ProviderItem
            item_uuid = item_result["itemUuid"]

            # Transform and upsert ProviderItem
            provider_payload = transform_to_provider_item(
                woo_product,
                item_uuid,
                provider_corp_external_id=settings.rfq_engine_provider_external_id,
            )

            if provider_payload is None:
                context.log.debug(
                    f"[{i + 1}/{len(rfq_items_source)}] Skipping ProviderItem "
                    f"(no valid price): {item_external_id}"
                )
                continue

            # Check if provider item already exists
            existing_provider = rfq.query_provider_item_by_external_id(
                provider_payload["provider_item_external_id"]
            )
            provider_item_uuid = (
                existing_provider["providerItemUuid"] if existing_provider else None
            )

            if existing_provider:
                provider_items_updated += 1
                context.log.debug(
                    f"[{i + 1}/{len(rfq_items_source)}] "
                    f"Updating ProviderItem: {provider_payload['provider_item_external_id']}"
                )
            else:
                provider_items_created += 1
                context.log.debug(
                    f"[{i + 1}/{len(rfq_items_source)}] "
                    f"Creating ProviderItem: {provider_payload['provider_item_external_id']}"
                )

            rfq.upsert_provider_item(
                provider_item_uuid=provider_item_uuid,
                item_uuid=item_uuid,
                provider_corp_external_id=provider_payload.get("provider_corp_external_id"),
                provider_item_external_id=provider_payload.get("provider_item_external_id"),
                base_price_per_uom=provider_payload.get("base_price_per_uom"),
                item_spec=provider_payload.get("item_spec"),
            )

        except Exception as e:
            failed += 1
            error_category = "validation_error" if "Validation" in str(e) else "api_error"
            errors.append(
                {
                    "run_id": context.run_id,
                    "timestamp": datetime.now(UTC).isoformat(),
                    "sku": sku,
                    "woo_product_id": woo_product.get("id"),
                    "item_external_id": item_external_id,
                    "product_name": product_name,
                    "failure_category": error_category,
                    "error_message": str(e),
                    "serialized_payload": {
                        "item_payload": item_payload,
                        "woo_product_id": woo_product.get("id"),
                    },
                }
            )
            context.log.warning(
                f"[{i + 1}/{len(rfq_items_source)}] Failed to sync '{product_name}': {e}"
            )

    context.log.info(
        f"RFQ sync complete: {items_created} items created, {items_updated} updated, "
        f"{provider_items_created} provider items created, {provider_items_updated} updated, "
        f"{failed} failed"
    )

    context.add_output_metadata(
        {
            "total": MetadataValue.int(len(rfq_items_source)),
            "items_created": MetadataValue.int(items_created),
            "items_updated": MetadataValue.int(items_updated),
            "provider_items_created": MetadataValue.int(provider_items_created),
            "provider_items_updated": MetadataValue.int(provider_items_updated),
            "failed": MetadataValue.int(failed),
            "errors": MetadataValue.json(errors[:10]),
        }
    )

    return {
        "total": len(rfq_items_source),
        "items_created": items_created,
        "items_updated": items_updated,
        "provider_items_created": provider_items_created,
        "provider_items_updated": provider_items_updated,
        "failed": failed,
        "errors": errors,
    }


@asset(
    description="Dead letter queue for failed RFQ syncs",
    compute_kind="s3",
    group_name="rfq",
    required_resource_keys={"s3"},
)
def rfq_dead_letter_queue(
    context,
    rfq_items_synced: dict,
) -> dict:
    """Persist failed RFQ sync records to S3 for review and reprocessing.

    Args:
        context: Dagster asset execution context
        rfq_items_synced: Sync results containing errors

    Returns:
        Dict with count and S3 location of dead letter file
    """
    s3 = context.resources.s3
    errors = rfq_items_synced.get("errors", [])

    if not errors:
        context.log.info("No failed RFQ records to store in dead letter queue")
        context.add_output_metadata(
            {
                "count": MetadataValue.int(0),
                "location": MetadataValue.text("N/A"),
            }
        )
        return {"count": 0, "location": None}

    context.log.info(f"Writing {len(errors)} failed records to dead letter queue")

    # Write to S3 dead letter prefix
    now = datetime.now(UTC)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    key = f"{s3.dead_letter_prefix}rfq_sync/{timestamp}.json"

    data = {
        "timestamp": now.isoformat(),
        "count": len(errors),
        "records": errors,
    }

    uri = s3.write_json(key, data)

    context.log.info(f"Wrote {len(errors)} failed records to {uri}")

    context.add_output_metadata(
        {
            "count": MetadataValue.int(len(errors)),
            "location": MetadataValue.text(uri),
            "sample_errors": MetadataValue.json(errors[:5]),
        }
    )

    return {
        "count": len(errors),
        "location": uri,
    }
