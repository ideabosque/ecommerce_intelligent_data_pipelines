"""Knowledge Graph Sync Assets.

Standalone pipeline that fetches products from WooCommerce
and extracts them into the knowledge graph engine.
"""

from __future__ import annotations

from dagster import MetadataValue, asset

from dagster_ecommerce.transformations.woo_to_kg import (
    transform_to_item_text,
    build_external_id
)


@asset(
    description="Products fetched from WooCommerce for knowledge graph extraction",
    compute_kind="api",
    group_name="knowledge_graph",
    required_resource_keys={"woo"},
)
def woo_products_for_kg(context) -> list[dict]:
    """Fetch all products from WooCommerce.

    Paginates through the WooCommerce REST API to retrieve
    all published products for knowledge graph ingestion.

    Returns:
        List of product dicts from WooCommerce.
    """
    woo = context.resources.woo

    context.log.info("Fetching all products from WooCommerce...")
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
    description="Products extracted into knowledge graph entities and relationships",
    compute_kind="api",
    group_name="knowledge_graph",
    required_resource_keys={"knowledge_graph", "rfq"},
)
def knowledge_graph_products(
    context,
    woo_products_for_kg: list[dict],
) -> dict:
    """Extract WooCommerce products into the knowledge graph.

    For each product fetched from WooCommerce, formats it as text and
    sends it to the knowledge graph engine's GraphQL extraction endpoint
    for entity/relationship extraction.

    Args:
        context: Dagster asset execution context.
        woo_products_for_kg: Products fetched from WooCommerce.

    Returns:
        Dict with extraction summary.
    """
    kg = context.resources.knowledge_graph
    rfq = context.resources.rfq

    if not woo_products_for_kg:
        context.log.info("No products to extract into knowledge graph")
        context.add_output_metadata(
            {
                "total": MetadataValue.int(0),
                "succeeded": MetadataValue.int(0),
                "failed": MetadataValue.int(0),
            }
        )
        return {"total": 0, "succeeded": 0, "failed": 0, "errors": []}

    context.log.info(
        f"Extracting {len(woo_products_for_kg)} WooCommerce products "
        f"into knowledge graph..."
    )

    succeeded = 0
    failed = 0
    total_entities = 0
    total_relationships = 0
    errors = []
    partition_key = f"{kg.endpoint_id}#{kg.part_id}"

    for i, product in enumerate(woo_products_for_kg):
        item_external_id = build_external_id(product)
        product_name = product.get("name", product.get("sku", f"product-{i}"))
        # Check if item already exists (for idempotent upsert)
        existing_item = rfq.query_item_by_external_id(item_external_id)
        item_uuid = existing_item["itemUuid"] if existing_item else None
        try:
            text = transform_to_item_text(product, item_uuid, partition_key)

            result = kg.extract_product(
                text=text,
                document_source="woocommerce",
                document_external_id=item_external_id,
            )

            entities = result.get("entitiesExtracted", 0) or 0
            relationships = result.get("relationshipsExtracted", 0) or 0
            total_entities += entities
            total_relationships += relationships
            succeeded += 1

            context.log.debug(
                f"[{i + 1}/{len(woo_products_for_kg)}] Extracted '{product_name}': "
                f"{entities} entities, {relationships} relationships"
            )

        except Exception as e:
            failed += 1
            errors.append(
                {
                    "product": product_name,
                    "sku": product.get("sku"),
                    "error": str(e),
                }
            )
            context.log.warning(
                f"[{i + 1}/{len(woo_products_for_kg)}] "
                f"Failed to extract '{product_name}': {e}"
            )

    context.log.info(
        f"Knowledge graph extraction complete: {succeeded} succeeded, "
        f"{failed} failed, {total_entities} entities, "
        f"{total_relationships} relationships"
    )

    context.add_output_metadata(
        {
            "total": MetadataValue.int(len(woo_products_for_kg)),
            "succeeded": MetadataValue.int(succeeded),
            "failed": MetadataValue.int(failed),
            "total_entities": MetadataValue.int(total_entities),
            "total_relationships": MetadataValue.int(total_relationships),
            "errors": MetadataValue.json(errors[:10]),
        }
    )

    return {
        "total": len(woo_products_for_kg),
        "succeeded": succeeded,
        "failed": failed,
        "total_entities": total_entities,
        "total_relationships": total_relationships,
        "errors": errors,
    }
