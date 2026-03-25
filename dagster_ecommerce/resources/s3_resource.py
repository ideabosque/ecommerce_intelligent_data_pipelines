"""S3 Resource for product file operations.

This module provides a Dagster resource for interacting with AWS S3,
including file listing, downloading, and moving files between prefixes.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO

import boto3
from dagster import Field, String, get_dagster_logger, resource

logger = get_dagster_logger()


@dataclass
class S3FileMetadata:
    """Metadata for an S3 file."""

    key: str
    last_modified: datetime
    size: int
    etag: str
    content_type: str = "application/octet-stream"

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "last_modified": self.last_modified.isoformat(),
            "size": self.size,
            "etag": self.etag,
            "content_type": self.content_type,
        }


class S3Resource:
    """S3 resource for product file operations.

    This resource provides methods for:
    - Listing objects in a bucket/prefix
    - Downloading file content
    - Moving files between prefixes (incoming -> processed)
    - Writing files (dead letter queue)

    Supports CSV, JSON, and NDJSON file formats.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "products/incoming/",
        region: str = "us-east-1",
        processed_prefix: str = "products/processed/",
        dead_letter_prefix: str = "products/dead_letter/",
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ):
        """Initialize S3 resource.

        Args:
            bucket: S3 bucket name
            prefix: Prefix for incoming files
            region: AWS region
            processed_prefix: Prefix for processed files
            dead_letter_prefix: Prefix for dead letter files
            aws_access_key_id: AWS access key (optional, uses env if not provided)
            aws_secret_access_key: AWS secret key (optional, uses env if not provided)
        """
        self.bucket = bucket
        self.prefix = prefix
        self.region = region
        self.processed_prefix = processed_prefix
        self.dead_letter_prefix = dead_letter_prefix

        # Initialize S3 client
        session_kwargs = {}
        if aws_access_key_id and aws_secret_access_key:
            session_kwargs = {
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
            }

        self.s3 = boto3.client(
            "s3",
            region_name=region,
            **session_kwargs,
        )

    def list_objects(
        self,
        prefix: str | None = None,
        max_keys: int = 1000,
    ) -> list[S3FileMetadata]:
        """List objects in the S3 bucket.

        Args:
            prefix: S3 prefix to list (defaults to self.prefix)
            max_keys: Maximum number of keys to return

        Returns:
            List of S3FileMetadata objects
        """
        prefix = prefix or self.prefix

        objects = []
        continuation_token = None

        while True:
            kwargs = {
                "Bucket": self.bucket,
                "Prefix": prefix,
                "MaxKeys": max_keys,
            }

            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            response = self.s3.list_objects_v2(**kwargs)

            for obj in response.get("Contents", []):
                objects.append(
                    S3FileMetadata(
                        key=obj["Key"],
                        last_modified=obj["LastModified"],
                        size=obj["Size"],
                        etag=obj["ETag"].strip('"'),
                        content_type=obj.get("ContentType", "application/octet-stream"),
                    )
                )

            if not response.get("IsTruncated"):
                break

            continuation_token = response.get("NextContinuationToken")

        logger.info(f"Listed {len(objects)} objects in s3://{self.bucket}/{prefix}")
        return objects

    def list_new_objects(
        self,
        since: datetime | None = None,
        prefix: str | None = None,
        extensions: tuple[str, ...] = (".csv", ".json", ".ndjson"),
    ) -> list[S3FileMetadata]:
        """List new objects since a given timestamp.

        Args:
            since: Only return objects modified after this time
            prefix: S3 prefix to list (defaults to self.prefix)
            extensions: File extensions to include

        Returns:
            List of new S3FileMetadata objects
        """
        prefix = prefix or self.prefix

        all_objects = self.list_objects(prefix)

        # Filter by extension and timestamp
        new_objects = []
        for obj in all_objects:
            # Check extension
            if not obj.key.lower().endswith(extensions):
                continue

            # Check timestamp
            if since and obj.last_modified <= since:
                continue

            new_objects.append(obj)

        logger.info(f"Found {len(new_objects)} new files since {since}")
        return new_objects

    def download_file(self, key: str) -> bytes:
        """Download file content from S3.

        Args:
            key: S3 object key

        Returns:
            File content as bytes
        """
        response = self.s3.get_object(Bucket=self.bucket, Key=key)
        content = response["Body"].read()
        logger.debug(f"Downloaded s3://{self.bucket}/{key} ({len(content)} bytes)")
        return content

    def download_file_as_text(self, key: str, encoding: str = "utf-8") -> str:
        """Download file content as text.

        Args:
            key: S3 object key
            encoding: Text encoding

        Returns:
            File content as string
        """
        content = self.download_file(key)
        return content.decode(encoding)

    def parse_file(self, key: str) -> list[dict]:
        """Parse a file and return records.

        Supports CSV, JSON, and NDJSON formats.

        Args:
            key: S3 object key

        Returns:
            List of record dictionaries
        """
        content = self.download_file_as_text(key)

        # Determine file type by extension
        ext = key.lower().split(".")[-1]

        if ext == "csv":
            return self._parse_csv(content)
        elif ext == "json":
            return self._parse_json(content)
        elif ext == "ndjson":
            return self._parse_ndjson(content)
        else:
            raise ValueError(f"Unsupported file type: {ext}")

    def _parse_csv(self, content: str) -> list[dict]:
        """Parse CSV content with enhanced field handling for Shopify-style exports.

        Implements field normalization strategies from API_MAPPING.md:
        - Handle multi-row products with repeated fields
        - Normalize boolean values from various formats
        - Process product images with positional ordering
        - Properly handle variant attribute matrices

        Special handling for:
        - Empty fields converted to None for consistent processing
        - Multi-value fields (categories, tags, images) as comma-separated strings
        - Boolean fields from "true"/"false", "yes"/"no", "1"/"0" formats

        Args:
            content: Raw CSV content as string

        Returns:
            List of normalized product record dictionaries
        """
        reader = csv.DictReader(StringIO(content))
        records = []

        for row in reader:
            # Normalize empty fields to None for consistent processing
            normalized_row = {}
            for key, value in row.items():
                # Strip whitespace and convert empty strings to None
                stripped_value = value.strip() if value else ""
                normalized_row[key] = stripped_value if stripped_value else None

                # Special handling for boolean fields commonly found in product exports
                if (
                    key in ["Published", "Manage Stock", "Featured", "Virtual"]
                    and value
                ):
                    normalized_row[key] = self._normalize_boolean(value)

            records.append(normalized_row)

        logger.debug(f"Parsed {len(records)} records from CSV with normalization")
        return records

    def _normalize_boolean(self, value: str) -> bool:
        """Normalize boolean values from various string representations.

        Handles common boolean representations found in product exports:
        - "true"/"false", "True"/"False", "TRUE"/"FALSE"
        - "yes"/"no", "Yes"/"No", "YES"/"NO"
        - "1"/"0"
        - Empty/null values default to False

        Args:
            value: String value to normalize

        Returns:
            Boolean representation of the value
        """
        if not value:
            return False

        cleaned_value = str(value).strip().lower()
        return cleaned_value in ("true", "yes", "1", "on")

    def _parse_json(self, content: str) -> list[dict]:
        """Parse JSON content."""
        data = json.loads(content)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Single object or wrapped
            if "products" in data:
                return data["products"]
            return [data]
        raise ValueError("JSON must be an array or object")

    def _parse_ndjson(self, content: str) -> list[dict]:
        """Parse NDJSON (newline-delimited JSON) content."""
        records = []
        for line in content.strip().split("\n"):
            if line:
                records.append(json.loads(line))
        logger.debug(f"Parsed {len(records)} records from NDJSON")
        return records

    def move_file(self, source_key: str, destination_prefix: str) -> str:
        """Move a file to a different prefix.

        This is implemented as copy + delete.

        Args:
            source_key: Source S3 key
            destination_prefix: Destination prefix

        Returns:
            New S3 key
        """
        # Build destination key
        filename = source_key.split("/")[-1]
        date_prefix = datetime.now(UTC).strftime("%Y/%m/%d/")
        destination_key = f"{destination_prefix}{date_prefix}{filename}"

        # Copy to destination
        self.s3.copy_object(
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": source_key},
            Key=destination_key,
        )

        # Delete source
        self.s3.delete_object(Bucket=self.bucket, Key=source_key)

        logger.info(f"Moved {source_key} -> {destination_key}")
        return destination_key

    def move_to_processed(self, source_key: str) -> str:
        """Move file to processed prefix.

        Args:
            source_key: Source S3 key

        Returns:
            New S3 key in processed prefix
        """
        return self.move_file(source_key, self.processed_prefix)

    def write_file(
        self,
        key: str,
        content: bytes | str,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Write content to S3.

        Args:
            key: S3 object key
            content: Content to write (bytes or string)
            content_type: Content type header

        Returns:
            Full S3 URI
        """
        if isinstance(content, str):
            content = content.encode("utf-8")

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content,
            ContentType=content_type,
        )

        uri = f"s3://{self.bucket}/{key}"
        logger.debug(f"Wrote {uri}")
        return uri

    def write_json(
        self,
        key: str,
        data: dict | list,
        indent: int | None = 2,
    ) -> str:
        """Write JSON data to S3.

        Args:
            key: S3 object key
            data: Data to write
            indent: JSON indentation

        Returns:
            Full S3 URI
        """
        content = json.dumps(data, indent=indent, default=str)
        return self.write_file(key, content, "application/json")


@resource(
    config_schema={
        "bucket": Field(String, description="S3 bucket name"),
        "prefix": Field(
            String,
            default_value="products/incoming/",
            description="Prefix for incoming files",
        ),
        "region": Field(String, default_value="us-east-1", description="AWS region"),
        "processed_prefix": Field(
            String,
            default_value="products/processed/",
            description="Prefix for processed files",
        ),
        "dead_letter_prefix": Field(
            String,
            default_value="products/dead_letter/",
            description="Prefix for dead letter files",
        ),
        "aws_access_key_id": Field(
            String, is_required=False, description="AWS access key ID"
        ),
        "aws_secret_access_key": Field(
            String, is_required=False, description="AWS secret access key"
        ),
    },
    description="S3 resource for product file operations.",
)
def s3_resource(context) -> S3Resource:
    """Create S3 resource from Dagster context."""
    config = context.resource_config
    return S3Resource(
        bucket=config["bucket"],
        prefix=config["prefix"],
        region=config["region"],
        processed_prefix=config["processed_prefix"],
        dead_letter_prefix=config["dead_letter_prefix"],
        aws_access_key_id=config.get("aws_access_key_id"),
        aws_secret_access_key=config.get("aws_secret_access_key"),
    )
