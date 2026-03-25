"""Tests for S3 ingestion assets."""

from datetime import datetime
from unittest.mock import MagicMock

from dagster_ecommerce.assets.s3_ingestion import (
    processed_files,
    raw_product_data,
    s3_product_files,
)
from dagster_ecommerce.resources.s3_resource import S3FileMetadata


class TestS3Resource:
    """Tests for S3Resource."""

    def test_list_objects(self, mock_s3_resource):
        """Test listing objects in S3."""
        # Setup mock response
        mock_s3_resource.list_objects.return_value = [
            S3FileMetadata(
                key="products/incoming/test.csv",
                last_modified=datetime(2026, 3, 22, 12, 0, 0),
                size=1024,
                etag="abc123",
            ),
        ]

        result = mock_s3_resource.list_objects()

        assert len(result) == 1
        assert result[0].key == "products/incoming/test.csv"

    def test_parse_csv(self, mock_s3_resource, sample_product_csv):
        """Test parsing CSV content."""
        mock_s3_resource.parse_file.return_value = [
            {"sku": "SKU-001", "name": "Widget A", "price": "29.99"},
            {"sku": "SKU-002", "name": "Widget B", "price": "19.99"},
        ]

        result = mock_s3_resource.parse_file("products/incoming/test.csv")

        assert len(result) == 2
        assert result[0]["sku"] == "SKU-001"
        assert result[1]["sku"] == "SKU-002"

    def test_move_to_processed(self, mock_s3_resource):
        """Test moving file to processed prefix."""
        mock_s3_resource.move_to_processed.return_value = (
            "products/processed/2026/03/22/test.csv"
        )

        result = mock_s3_resource.move_to_processed("products/incoming/test.csv")

        assert "processed" in result


class TestS3IngestionAssets:
    """Tests for S3 ingestion assets."""

    def test_s3_product_files_empty(self, mock_s3_resource):
        """Test s3_product_files with no files."""
        mock_s3_resource.list_new_objects.return_value = []

        context = MagicMock(log=MagicMock(), add_output_metadata=MagicMock())
        result = s3_product_files.op.compute_fn.decorated_fn(context, mock_s3_resource)

        assert result == []

    def test_s3_product_files_with_files(self, mock_s3_resource):
        """Test s3_product_files with files."""
        mock_s3_resource.list_new_objects.return_value = [
            S3FileMetadata(
                key="products/incoming/test.csv",
                last_modified=datetime(2026, 3, 22, 12, 0, 0),
                size=1024,
                etag="abc123",
            ),
        ]

        context = MagicMock(log=MagicMock(), add_output_metadata=MagicMock())
        result = s3_product_files.op.compute_fn.decorated_fn(context, mock_s3_resource)

        assert len(result) == 1
        assert result[0]["key"] == "products/incoming/test.csv"

    def test_raw_product_data_parsing(self, mock_s3_resource):
        """Test raw_product_data parsing."""
        mock_s3_resource.parse_file.return_value = [
            {"sku": "SKU-001", "name": "Widget A"},
            {"sku": "SKU-002", "name": "Widget B"},
        ]

        file_metadata = [
            {"key": "test.csv", "last_modified": "2026-03-22T12:00:00", "size": 1024, "etag": "abc"}
        ]

        context = MagicMock(log=MagicMock(), add_output_metadata=MagicMock())
        result = raw_product_data.op.compute_fn.decorated_fn(
            context, mock_s3_resource, file_metadata
        )

        assert len(result) == 2
        assert result[0]["sku"] == "SKU-001"
        assert result[0]["__source_key"] == "test.csv"

    def test_processed_files(self, mock_s3_resource):
        """Test processed_files asset."""
        mock_s3_resource.move_to_processed.return_value = "products/processed/test.csv"
        raw_records = [
            {"sku": "SKU-001", "name": "Widget A", "__source_key": "test.csv"}
        ]
        file_metadata = [
            {"key": "test.csv", "last_modified": "2026-03-22T12:00:00", "size": 1024, "etag": "abc"}
        ]

        context = MagicMock(log=MagicMock(), add_output_metadata=MagicMock())
        result = processed_files.op.compute_fn.decorated_fn(
            context, mock_s3_resource, raw_records, file_metadata
        )

        assert len(result) == 1
        assert "processed" in result[0]
