"""Service for repartitioning data from one partition structure to another.

This service provides a safe and verifiable way to migrate data between different
partition structures. It handles data integrity through backup copies, verification
steps, and atomic operations to ensure no data loss during migration.

Key features:
- Safe data migration with backup verification
- Support for custom partition keys
- Batch processing by partition date
- Error handling with detailed logging
- Atomic operations to prevent data corruption
- Optional parallel processing for improved performance
"""

from lib.log.logger import get_logger
from services.repartition_service.helper import (
    repartition_data_for_partition_dates,
)
from services.repartition_service.parallel_processing import (
    repartition_data_for_partition_dates_parallel,
    ParallelConfig,
)

logger = get_logger(__file__)

# Global configuration
PROCESS_IN_PARALLEL = False
DEFAULT_PARALLEL_CONFIG = ParallelConfig(
    max_workers=4,
    chunk_size=10,
    timeout=3600,
    progress_interval=5,
)


def repartition_service(payload: dict) -> None:
    """Repartition service data based on the provided configuration payload.

    Args:
        payload (dict): Configuration dictionary controlling the repartitioning process.
            Required fields:
                service (str): Name of the service to repartition. Must exist in
                    MAP_SERVICE_TO_METADATA.
            Optional fields:
                start_date (str): Start date in YYYY-MM-DD format (inclusive).
                    Defaults to "2024-09-28".
                end_date (str): End date in YYYY-MM-DD format (inclusive).
                    Defaults to "2025-12-01".
                new_service_partition_key (str): Field name to use as the new partition key.
                    Defaults to "created_at".
                exclude_partition_dates (list[str]): List of dates to exclude in YYYY-MM-DD format.
                    Defaults to ["2024-10-08"].
                parallel_config (Optional[ParallelConfig]): Configuration for parallel processing.
                    Only used if PROCESS_IN_PARALLEL is True.

    Raises:
        KeyError: If required 'service' field is missing from payload.
        ValueError: If service name is not found in MAP_SERVICE_TO_METADATA.

    Behavior:
        1. Extracts and validates configuration from payload
        2. Logs the repartitioning operation details
        3. If PROCESS_IN_PARALLEL is True:
            a. Uses parallel processing with multiple workers
            b. Processes date chunks concurrently
            c. Monitors progress and handles failures
        4. If PROCESS_IN_PARALLEL is False:
            a. Processes dates sequentially
            b. Uses single-threaded processing
        5. Logs completion status
    """
    start_date = payload.get("start_date", "2024-09-28")
    end_date = payload.get("end_date", "2025-12-01")
    service = payload["service"]  # Required parameter
    new_service_partition_key = payload.get("new_service_partition_key", "created_at")
    exclude_partition_dates = payload.get("exclude_partition_dates", ["2024-10-08"])
    parallel_config = payload.get("parallel_config", DEFAULT_PARALLEL_CONFIG)

    logger.info(
        f"Repartitioning {service} data from {start_date} to {end_date} "
        f"using partition key '{new_service_partition_key}', "
        f"excluding dates: {exclude_partition_dates}"
    )

    if PROCESS_IN_PARALLEL:
        logger.info("Using parallel processing")
        results = repartition_data_for_partition_dates_parallel(
            start_date=start_date,
            end_date=end_date,
            service=service,
            new_service_partition_key=new_service_partition_key,
            exclude_partition_dates=exclude_partition_dates,
            parallel_config=parallel_config,
        )
        # Log summary of results
        success_count = sum(1 for r in results.values() if r.status == "SUCCESS")
        failed_count = sum(1 for r in results.values() if r.status == "FAILED")
        skipped_count = sum(1 for r in results.values() if r.status == "SKIPPED")
        logger.info(
            f"Parallel processing complete: {success_count} succeeded, "
            f"{failed_count} failed, {skipped_count} skipped"
        )
    else:
        # might be good to process sequentially to make sure that all the data
        # up to a certain date is correctly processed.
        logger.info("Using sequential processing")
        repartition_data_for_partition_dates(
            start_date=start_date,
            end_date=end_date,
            service=service,
            new_service_partition_key=new_service_partition_key,
            exclude_partition_dates=exclude_partition_dates,
        )

    logger.info(f"Finished repartitioning {service} data.")


if __name__ == "__main__":
    # Example payload for testing
    payload = {
        "service": "uris_to_created_at",  # Required
        "parallel_config": ParallelConfig(max_workers=2, chunk_size=5),  # Optional
    }
    repartition_service(payload)
