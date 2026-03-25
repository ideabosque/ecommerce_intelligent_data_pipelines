"""WooCommerce Sync Assets.

This module provides Dagster assets for syncing validated products
to WooCommerce via REST API.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dagster import MetadataValue, asset

from dagster_ecommerce.config.settings import settings
from dagster_ecommerce.models.product import SyncResult


@asset(
    description="Synced products in WooCommerce store",
    compute_kind="api",
    group_name="sync",
    required_resource_keys={"woo"},
)
def woocommerce_products(
    context,
    validated_products: tuple[list[dict], list[dict]],
) -> dict:
    """Sync validated products to WooCommerce using batch API.

    Fetches all existing products in bulk via pagination, then uses
    the /products/batch endpoint to create/update up to 100 products
    per request for dramatically better performance.

    Args:
        context: Dagster asset execution context
        validated_products: Tuple of (valid_records, invalid_records)

    Returns:
        Sync result dict with counts and errors
    """
    woo = context.resources.woo
    valid_products, _ = validated_products

    results = SyncResult()

    # Bulk-fetch all existing products by paginating (much faster than per-SKU lookup)
    context.log.info("Fetching all existing products from WooCommerce...")
    existing_skus = {}
    try:
        all_existing = woo.get_products(params={"per_page": 100})
        for p in all_existing:
            sku = p.get("sku")
            if sku:
                existing_skus[sku] = p
        context.log.info(f"Found {len(existing_skus)} existing products in store")
    except Exception as e:
        context.log.warning(f"Failed to fetch existing products: {e}")

    # Split into create vs update batches
    to_create = []
    to_update = []

    for product_data in valid_products:
        sku = product_data.get("sku")
        if sku and sku in existing_skus:
            product_data["id"] = existing_skus[sku]["id"]
            to_update.append(product_data)
        else:
            to_create.append(product_data)

    context.log.info(
        f"Batch plan: {len(to_create)} to create, {len(to_update)} to update"
    )

    # Process creates in batches
    for batch_idx in range(0, len(to_create), settings.batch_size):
        batch = to_create[batch_idx : batch_idx + settings.batch_size]
        batch_num = batch_idx // settings.batch_size + 1
        total_batches = (
            len(to_create) + settings.batch_size - 1
        ) // settings.batch_size
        context.log.info(
            f"Creating batch {batch_num}/{total_batches} ({len(batch)} products)..."
        )

        try:
            resp = woo.batch_update_products(create=batch)
            created_items = resp.get("create", [])
            for item in created_items:
                if item.get("error"):
                    results.errors.append(
                        {
                            "sku": item.get("sku"),
                            "error": str(item.get("error")),
                            "error_type": "batch_create",
                        }
                    )
                    results.skipped += 1
                else:
                    results.created += 1
        except Exception as e:
            context.log.error(f"Batch create failed: {e}")
            results.errors.append({"error": str(e), "error_type": "batch_create"})
            results.skipped += len(batch)

    # Process updates in batches
    for batch_idx in range(0, len(to_update), settings.batch_size):
        batch = to_update[batch_idx : batch_idx + settings.batch_size]
        batch_num = batch_idx // settings.batch_size + 1
        total_batches = (
            len(to_update) + settings.batch_size - 1
        ) // settings.batch_size
        context.log.info(
            f"Updating batch {batch_num}/{total_batches} ({len(batch)} products)..."
        )

        try:
            resp = woo.batch_update_products(update=batch)
            updated_items = resp.get("update", [])
            for item in updated_items:
                if item.get("error"):
                    results.errors.append(
                        {
                            "sku": item.get("sku"),
                            "error": str(item.get("error")),
                            "error_type": "batch_update",
                        }
                    )
                    results.skipped += 1
                else:
                    results.updated += 1
        except Exception as e:
            context.log.error(f"Batch update failed: {e}")
            results.errors.append({"error": str(e), "error_type": "batch_update"})
            results.skipped += len(batch)

    context.log.info(
        f"Sync complete: created={results.created}, "
        f"updated={results.updated}, skipped={results.skipped}"
    )

    if results.errors:
        context.log.warning(f"{len(results.errors)} products failed to sync")

    context.add_output_metadata(
        {
            "created": MetadataValue.int(results.created),
            "updated": MetadataValue.int(results.updated),
            "skipped": MetadataValue.int(results.skipped),
            "error_count": MetadataValue.int(len(results.errors)),
            "success_rate": MetadataValue.float(results.success_rate),
            "total_processed": MetadataValue.int(results.total_processed),
            "errors": MetadataValue.json(results.errors[:10]),
        }
    )

    return results.model_dump()


@asset(
    description="Dead letter queue for invalid products",
    compute_kind="s3",
    group_name="processing",
    required_resource_keys={"s3"},
    deps=["processed_files"],
)
def dead_letter_queue(
    context,
    validated_products: tuple[list[dict], list[dict]],
) -> dict:
    """Persist invalid product records to S3 for review and reprocessing.

    Categorizes errors for targeted remediation.

    Args:
        context: Dagster asset execution context
        validated_products: Tuple of (valid_records, invalid_records)

    Returns:
        Dict with count and S3 location of dead letter file
    """
    s3 = context.resources.s3
    _, invalid_products = validated_products

    if not invalid_products:
        context.log.info("No invalid records to store in dead letter queue")
        context.add_output_metadata(
            {
                "count": MetadataValue.int(0),
                "location": MetadataValue.text("N/A"),
            }
        )
        return {"count": 0, "location": None}

    # Categorize errors
    categorized_errors = {
        "validation": [],
        "parsing": [],
        "sync": [],
        "unknown": [],
    }

    for record in invalid_products:
        errors = record.get("errors", [])
        assigned_category = "unknown"
        for error in errors:
            error_lower = error.lower()
            if any(
                kw in error_lower
                for kw in ["missing", "required", "invalid", "duplicate"]
            ):
                assigned_category = "validation"
                break
            elif any(kw in error_lower for kw in ["parse", "format", "json", "csv"]):
                assigned_category = "parsing"
                break
            elif any(
                kw in error_lower for kw in ["api", "http", "sync", "woocommerce"]
            ):
                assigned_category = "sync"
                break
        categorized_errors[assigned_category].append(record)

    context.log.info(
        f"Categorized {len(invalid_products)} invalid records: "
        f"{len(categorized_errors['validation'])} validation, "
        f"{len(categorized_errors['parsing'])} parsing, "
        f"{len(categorized_errors['sync'])} sync, "
        f"{len(categorized_errors['unknown'])} unknown"
    )

    # Write to S3 dead letter prefix
    now = datetime.now(UTC)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    key = f"{s3.dead_letter_prefix}{timestamp}.json"

    data = {
        "timestamp": now.isoformat(),
        "count": len(invalid_products),
        "categorized_errors": {
            category: len(records) for category, records in categorized_errors.items()
        },
        "records": invalid_products,
    }

    uri = s3.write_json(key, data)

    context.log.info(f"Wrote {len(invalid_products)} invalid records to {uri}")

    context.add_output_metadata(
        {
            "count": MetadataValue.int(len(invalid_products)),
            "location": MetadataValue.text(uri),
            "validation_errors": MetadataValue.int(
                len(categorized_errors["validation"])
            ),
            "parsing_errors": MetadataValue.int(len(categorized_errors["parsing"])),
            "sync_errors": MetadataValue.int(len(categorized_errors["sync"])),
            "unknown_errors": MetadataValue.int(len(categorized_errors["unknown"])),
            "sample_errors": MetadataValue.json(invalid_products[:5]),
        }
    )

    return {
        "count": len(invalid_products),
        "location": uri,
    }
