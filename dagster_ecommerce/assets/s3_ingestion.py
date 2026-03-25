"""S3 Ingestion Assets.

This module provides Dagster assets for detecting and downloading
product data files from AWS S3.
"""

from __future__ import annotations

from typing import Any

from dagster import MetadataValue, asset


@asset(
    description="List of new/updated product files detected in S3 bucket",
    compute_kind="s3",
    group_name="ingestion",
    required_resource_keys={"s3"},
)
def s3_product_files(
    context,
) -> list[dict[str, Any]]:
    """Scan S3 bucket for new product files since last run.

    This asset monitors the S3 incoming prefix for new or updated
    product data files (CSV, JSON, NDJSON).

    Args:
        context: Dagster asset execution context

    Returns:
        List of file metadata dicts with keys:
        - key: S3 object key
        - last_modified: datetime
        - size: bytes
        - etag: S3 ETag for deduplication
    """
    s3 = context.resources.s3

    # Get the last run time for incremental processing
    # For the first run, this will be None and all files will be processed
    context.log.info(f"Scanning for new files in s3://{s3.bucket}/{s3.prefix}")

    # List all files with supported extensions
    files = s3.list_new_objects()

    # Convert to serializable format
    file_metadata = [f.to_dict() for f in files]

    context.log.info(f"Detected {len(file_metadata)} new product files")

    # Add metadata for Dagster UI
    context.add_output_metadata(
        {
            "num_files": MetadataValue.int(len(file_metadata)),
            "files": MetadataValue.json([f["key"] for f in file_metadata[:10]]),
        }
    )

    return file_metadata


@asset(
    description="Downloaded and parsed raw product data from S3 with enhanced error handling",
    compute_kind="python",
    group_name="ingestion",
    required_resource_keys={"s3"},
)
def raw_product_data(
    context,
    s3_product_files: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Download S3 files and parse CSV/JSON into raw records with comprehensive error handling.

    This asset downloads each file from S3 and parses the content
    into a list of dictionaries. Supports CSV, JSON, and NDJSON formats.

    Implements fault tolerance strategies:
    - Continues processing despite individual file errors
    - Captures detailed error information for troubleshooting
    - Maintains partial success data for downstream processing
    - Logs structured metrics for operational monitoring

    Args:
        context: Dagster asset execution context
        s3_product_files: Output from s3_product_files asset

    Returns:
        List of raw product records (dictionaries)

    Error Handling:
        Individual file parsing failures do not halt the entire process.
        Failed files are logged and skipped, allowing valid data to proceed.
    """
    s3 = context.resources.s3
    all_records = []
    files_processed = 0
    total_bytes = 0
    failed_files = []

    context.log.info(f"Starting to process {len(s3_product_files)} files from S3")

    for i, file_meta in enumerate(s3_product_files):
        key = file_meta["key"]
        context.log.info(f"Processing file {i + 1}/{len(s3_product_files)}: {key}")

        # Progress indicator for large batches
        if (i + 1) % 10 == 0:
            context.log.info(
                f"Progress: {i + 1}/{len(s3_product_files)} files processed"
            )

        try:
            # Parse file content
            records = s3.parse_file(key)
            all_records.extend([{**record, "__source_key": key} for record in records])
            files_processed += 1
            total_bytes += file_meta.get("size", 0)

            context.log.info(f"Parsed {len(records)} records from {key}")

        except Exception as e:
            error_msg = str(e)
            context.log.error(f"Failed to parse {key}: {error_msg}")
            failed_files.append({"key": key, "error": error_msg, "index": i})
            # Continue processing other files instead of failing completely
            continue

    context.log.info(
        f"Completed processing: {len(all_records)} records from {files_processed} files"
    )

    if failed_files:
        context.log.warning(
            f"{len(failed_files)} files failed to parse. Details in dead letter queue."
        )

    # Add comprehensive metadata for Dagster UI
    context.add_output_metadata(
        {
            "total_records": MetadataValue.int(len(all_records)),
            "files_processed": MetadataValue.int(files_processed),
            "failed_files": MetadataValue.int(len(failed_files)),
            "total_bytes": MetadataValue.int(total_bytes),
            "first_record": MetadataValue.json(all_records[0] if all_records else {}),
            "failed_files_details": MetadataValue.json(
                failed_files[:5]
            ),  # Show first 5 failures
        }
    )

    return all_records


@asset(
    description="List of file keys that were successfully processed",
    compute_kind="s3",
    group_name="ingestion",
    required_resource_keys={"s3"},
)
def processed_files(
    context,
    raw_product_data: list[dict[str, Any]],
    s3_product_files: list[dict[str, Any]],
) -> list[str]:
    """Move processed files to the processed prefix in S3.

    This asset moves successfully processed files from the incoming
    prefix to the processed prefix with date-based organization.

    Args:
        context: Dagster asset execution context
        raw_product_data: Output from raw_product_data asset (dependency)
        s3_product_files: Output from s3_product_files asset

    Returns:
        List of new S3 keys in the processed prefix
    """
    s3 = context.resources.s3
    processed_keys = []
    successful_source_keys = {
        record["__source_key"]
        for record in raw_product_data
        if isinstance(record, dict) and "__source_key" in record
    }

    for file_meta in s3_product_files:
        key = file_meta["key"]

        if key not in successful_source_keys:
            context.log.warning(
                f"Skipping move for {key} because parsing did not succeed"
            )
            continue

        try:
            new_key = s3.move_to_processed(key)
            processed_keys.append(new_key)
            context.log.info(f"Moved {key} -> {new_key}")

        except Exception as e:
            context.log.error(f"Failed to move {key}: {e}")
            continue

    context.add_output_metadata(
        {
            "processed_count": MetadataValue.int(len(processed_keys)),
            "processed_keys": MetadataValue.json(processed_keys),
        }
    )

    return processed_keys
