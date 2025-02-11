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
"""

from lib.log.logger import get_logger
from services.repartition_service.helper import (
    repartition_data_for_partition_dates,
)

logger = get_logger(__file__)


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
                    Defaults to "preprocessing_timestam".
                exclude_partition_dates (list[str]): List of dates to exclude in YYYY-MM-DD format.
                    Defaults to ["2024-10-08"].
                use_parallel (bool): Whether to use parallel processing. Defaults to False.

    Raises:
        KeyError: If required 'service' field is missing from payload.
        ValueError: If service name is not found in MAP_SERVICE_TO_METADATA.

    Behavior:
        1. Extracts and validates configuration from payload
        2. Logs the repartitioning operation details
        3. Processes dates sequentially
        4. Logs completion status
    """
    start_date = payload.get("start_date", "2024-09-28")
    end_date = payload.get("end_date", "2025-12-01")
    service = payload["service"]  # Required parameter
    new_service_partition_key = payload.get(
        "new_service_partition_key", "preprocessing_timestam"
    )
    exclude_partition_dates = payload.get("exclude_partition_dates", ["2024-10-08"])
    use_parallel = payload.get("use_parallel", False)

    logger.info(
        f"Repartitioning {service} data from {start_date} to {end_date} "
        f"using partition key '{new_service_partition_key}', "
        f"excluding dates: {exclude_partition_dates}"
    )

    repartition_data_for_partition_dates(
        start_date=start_date,
        end_date=end_date,
        service=service,
        new_service_partition_key=new_service_partition_key,
        exclude_partition_dates=exclude_partition_dates,
        use_parallel=use_parallel,
    )

    logger.info(f"Finished repartitioning {service} data.")


if __name__ == "__main__":
    # Example payload for testing
    payload = {
        "service": "uris_to_preprocessing_timestam",  # Required
    }
    repartition_service(payload)
