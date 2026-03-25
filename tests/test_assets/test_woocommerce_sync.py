"""Tests for WooCommerce sync assets."""

from unittest.mock import MagicMock

from dagster_ecommerce.assets.woocommerce_sync import (
    dead_letter_queue,
    woocommerce_products,
)


class TestWooCommerceSync:
    """Tests for WooCommerce sync assets."""

    def test_sync_creates_new_products(self, mock_woo_resource):
        """Test syncing creates new products."""
        mock_woo_resource.get_products_by_sku.return_value = {}
        mock_woo_resource.create_product.return_value = {"id": 1, "sku": "NEW-001"}

        valid_products = [
            {"name": "New Product", "sku": "NEW-001", "regular_price": "29.99"}
        ]
        invalid_products = []

        context = MagicMock(log=MagicMock(), add_output_metadata=MagicMock())
        result = woocommerce_products.op.compute_fn.decorated_fn(
            context=context,
            woo=mock_woo_resource,
            validated_products=(valid_products, invalid_products),
        )

        assert result["created"] == 1
        assert result["updated"] == 0
        assert result["skipped"] == 0
        mock_woo_resource.create_product.assert_called_once()

    def test_sync_updates_existing_products(self, mock_woo_resource):
        """Test syncing updates existing products."""
        mock_woo_resource.get_products_by_sku.return_value = {
            "EXISTING-001": {"id": 123, "sku": "EXISTING-001"}
        }
        mock_woo_resource.update_product.return_value = {"id": 123, "sku": "EXISTING-001"}

        valid_products = [
            {"name": "Updated Product", "sku": "EXISTING-001", "regular_price": "19.99"}
        ]
        invalid_products = []

        context = MagicMock(log=MagicMock(), add_output_metadata=MagicMock())
        result = woocommerce_products.op.compute_fn.decorated_fn(
            context=context,
            woo=mock_woo_resource,
            validated_products=(valid_products, invalid_products),
        )

        assert result["created"] == 0
        assert result["updated"] == 1
        mock_woo_resource.update_product.assert_called_once_with(123, valid_products[0])

    def test_sync_handles_errors(self, mock_woo_resource):
        """Test syncing handles errors gracefully."""
        mock_woo_resource.get_products_by_sku.return_value = {}
        mock_woo_resource.create_product.side_effect = Exception("API Error")

        valid_products = [
            {"name": "Failed Product", "sku": "FAIL-001", "regular_price": "9.99"}
        ]
        invalid_products = []

        context = MagicMock(log=MagicMock(), add_output_metadata=MagicMock())
        result = woocommerce_products.op.compute_fn.decorated_fn(
            context=context,
            woo=mock_woo_resource,
            validated_products=(valid_products, invalid_products),
        )

        assert result["skipped"] == 1
        assert len(result["errors"]) == 1
        assert "FAIL-001" in result["errors"][0]["sku"]

    def test_sync_mixed_results(self, mock_woo_resource):
        """Test syncing with mixed create/update/skip results."""
        mock_woo_resource.get_products_by_sku.return_value = {
            "EXISTING-001": {"id": 100, "sku": "EXISTING-001"}
        }
        mock_woo_resource.create_product.return_value = {"id": 1}
        mock_woo_resource.update_product.return_value = {"id": 100}

        valid_products = [
            {"name": "New 1", "sku": "NEW-001", "regular_price": "10.00"},
            {"name": "New 2", "sku": "NEW-002", "regular_price": "20.00"},
            {"name": "Existing 1", "sku": "EXISTING-001", "regular_price": "15.00"},
        ]
        invalid_products = []

        context = MagicMock(log=MagicMock(), add_output_metadata=MagicMock())
        result = woocommerce_products.op.compute_fn.decorated_fn(
            context=context,
            woo=mock_woo_resource,
            validated_products=(valid_products, invalid_products),
        )

        assert result["created"] == 2
        assert result["updated"] == 1
        assert result["skipped"] == 0


class TestDeadLetterQueue:
    """Tests for dead letter queue asset."""

    def test_dead_letter_empty(self, mock_s3_resource):
        """Test dead letter with no invalid records."""
        valid_products = [{"name": "Valid", "sku": "OK-001"}]
        invalid_products = []

        context = MagicMock(log=MagicMock(), add_output_metadata=MagicMock())
        result = dead_letter_queue.op.compute_fn.decorated_fn(
            context=context,
            s3=mock_s3_resource,
            validated_products=(valid_products, invalid_products),
            processed_files=["test.csv"],
        )

        assert result["count"] == 0
        assert result["location"] is None

    def test_dead_letter_with_invalid_records(self, mock_s3_resource):
        """Test dead letter with invalid records."""
        mock_s3_resource.write_json.return_value = "s3://bucket/dead_letter/test.json"

        valid_products = []
        invalid_products = [
            {"record": {"name": "Bad"}, "errors": ["Missing price"]}
        ]

        context = MagicMock(log=MagicMock(), add_output_metadata=MagicMock())
        result = dead_letter_queue.op.compute_fn.decorated_fn(
            context=context,
            s3=mock_s3_resource,
            validated_products=(valid_products, invalid_products),
            processed_files=["test.csv"],
        )

        assert result["count"] == 1
        assert result["location"] == "s3://bucket/dead_letter/test.json"
        mock_s3_resource.write_json.assert_called_once()
