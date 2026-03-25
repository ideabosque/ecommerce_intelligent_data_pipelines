"""S3 File Sensor for detecting new product files.

This module provides a Dagster sensor that detects new files
in the S3 incoming prefix and triggers the product sync job.
"""

from __future__ import annotations

from datetime import datetime

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

from dagster_ecommerce.config.settings import settings
from dagster_ecommerce.resources.s3_resource import S3Resource


@sensor(
    job_name="product_sync_job",
    minimum_interval_seconds=300,  # Check every 5 minutes
    description="Sensor that triggers product sync when new files appear in S3.",
)
def s3_product_file_sensor(context: SensorEvaluationContext):
    """Sensor for new S3 product files.

    This sensor monitors the S3 incoming prefix and triggers
    the product sync job when new files are detected.

    The sensor uses a cursor to track the last processed file
    timestamp, enabling incremental processing.

    Args:
        context: Sensor evaluation context

    Yields:
        RunRequest for each new file batch, or SkipReason if no new files
    """
    # Get cursor (last check time)
    cursor = context.cursor
    last_check_time = None

    if cursor:
        try:
            last_check_time = datetime.fromisoformat(cursor)
        except (ValueError, TypeError):
            context.log.warning(f"Invalid cursor: {cursor}")
            last_check_time = None

    # Create S3 resource for sensor-only file listing
    s3 = S3Resource(
        bucket=settings.s3_bucket_name,
        prefix=settings.s3_prefix,
        region=settings.aws_region,
        processed_prefix=settings.s3_processed_prefix,
        dead_letter_prefix=settings.s3_dead_letter_prefix,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )

    # Check for new files
    new_files = s3.list_new_objects(since=last_check_time)

    if not new_files:
        yield SkipReason("No new files detected")
        return

    # Create run key from file keys
    file_keys = [f.key for f in new_files[:10]]  # Limit to 10 files per run
    run_key = f"s3_sync_{'_'.join([f.split('/')[-1] for f in file_keys])}"

    # Tags for filtering in Dagster UI
    tags = {
        "source": "s3_sensor",
        "file_count": str(len(new_files)),
        "files": ",".join(file_keys[:5]),
    }

    yield RunRequest(
        run_key=run_key,
        tags=tags,
    )

    # Update cursor with latest timestamp
    if new_files:
        latest_time = max(f.last_modified for f in new_files)
        context.update_cursor(latest_time.isoformat())
