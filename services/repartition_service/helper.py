"""Helper functions for repartitioning service data.

This module provides functions to safely migrate data from one partition structure
to another while maintaining data integrity. It implements a robust process that includes:
- Backup creation and verification
- Temporary staging area for atomic operations
- Data integrity checks at each step
- Detailed logging of operations and record counts
- Error handling with graceful degradation
"""

import os
import shutil
from dataclasses import dataclass
from typing import Dict, List, Optional
from enum import Enum, auto

import pandas as pd

from lib.constants import root_local_data_directory
from lib.db.manage_local_data import (
    export_data_to_local_storage,
    load_data_from_local_storage,
)
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.datetime_utils import get_partition_dates
from lib.log.logger import get_logger

logger = get_logger(__file__)


class RepartitionError(Exception):
    """Base exception for repartition service errors."""

    pass


class BackupError(RepartitionError):
    """Error during backup operations."""

    pass


class VerificationError(RepartitionError):
    """Error during data verification."""

    pass


class OperationStatus(Enum):
    """Status of repartition operations."""

    SUCCESS = auto()
    FAILED = auto()
    SKIPPED = auto()


@dataclass
class OperationResult:
    """Result of a repartition operation."""

    status: OperationStatus
    error: Optional[Exception] = None
    message: str = ""


def verify_dataframes_equal(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    """Verify that two DataFrames contain exactly the same data.

    Args:
        df1 (pd.DataFrame): First DataFrame to compare
        df2 (pd.DataFrame): Second DataFrame to compare

    Returns:
        bool: True if DataFrames have identical content (same length and values),
            False otherwise

    Behavior:
        1. Compares DataFrame lengths
        2. If lengths match, compares content using pandas equals()
        3. Logs warning messages if differences are found
    """
    if len(df1) != len(df2):
        logger.warning(f"DataFrame lengths differ: {len(df1)} vs {len(df2)}")
        return False

    if not df1.equals(df2):
        logger.warning("DataFrame contents differ")
        return False

    return True


def get_service_paths(service: str, partition_date: str) -> Dict[str, str]:
    """Generate paths for original, backup, and temporary data locations.

    Args:
        service (str): Name of the service being repartitioned. Must exist in
            MAP_SERVICE_TO_METADATA.
        partition_date (str): The partition date in YYYY-MM-DD format.

    Returns:
        Dict[str, str]: Dictionary containing paths for:
            - original: Current data location
            - backup: Backup data location (old_{service})
            - temp: Temporary staging location (tmp_{service})

    Raises:
        KeyError: If service is not found in MAP_SERVICE_TO_METADATA

    Behavior:
        1. Retrieves service metadata to get local_prefix
        2. Constructs paths for original, backup, and temp locations
        3. Creates parent directories if they don't exist
        4. Returns dictionary of paths
    """
    service_metadata = MAP_SERVICE_TO_METADATA[service]
    local_prefix = service_metadata["local_prefix"]
    service_name = os.path.basename(local_prefix)

    paths = {
        "original": os.path.join(
            local_prefix, "cache", f"partition_date={partition_date}"
        ),
        "backup": os.path.join(
            root_local_data_directory,
            f"old_{service_name}",
            "cache",
            f"partition_date={partition_date}",
        ),
        "temp": os.path.join(
            root_local_data_directory,
            f"tmp_{service_name}",
            "cache",
            f"partition_date={partition_date}",
        ),
    }

    # Create directories if they don't exist
    for path in paths.values():
        os.makedirs(os.path.dirname(path), exist_ok=True)

    return paths


def cleanup_temp_files(paths: Dict[str, str]) -> None:
    """Clean up temporary files after processing.

    Args:
        paths (Dict[str, str]): Dictionary of paths to clean up

    Behavior:
        1. Attempts to remove temporary directory if it exists
        2. Logs any errors but doesn't raise them
    """
    try:
        if os.path.exists(paths["temp"]):
            shutil.rmtree(paths["temp"])
            logger.info(f"Cleaned up temporary files at {paths['temp']}")
    except Exception as e:
        logger.warning(f"Failed to clean up temporary files: {e}")


def create_backup(
    df: pd.DataFrame, service: str, paths: Dict[str, str], partition_date: str
) -> OperationResult:
    """Create and verify backup of original data.

    Args:
        df (pd.DataFrame): Original data to backup
        service (str): Service name
        paths (Dict[str, str]): Service paths
        partition_date (str): Partition date being processed

    Returns:
        OperationResult: Result of the backup operation

    Behavior:
        1. Exports data to backup location
        2. Verifies backup integrity
        3. Returns operation status
    """
    try:
        # Create backup
        export_data_to_local_storage(
            service=service,
            df=df,
            export_format="parquet",
            override_local_prefix=paths["backup"],
        )

        # Verify backup
        backup_df = load_data_from_local_storage(
            service=service,
            partition_date=partition_date,
            output_format="df",
            override_local_prefix=os.path.dirname(os.path.dirname(paths["backup"])),
        )

        if not verify_dataframes_equal(df, backup_df):
            raise VerificationError("Backup verification failed")

        return OperationResult(
            status=OperationStatus.SUCCESS,
            message=f"Successfully backed up {len(df)} records",
        )

    except VerificationError as e:
        return OperationResult(status=OperationStatus.FAILED, error=e)
    except Exception as e:
        return OperationResult(
            status=OperationStatus.FAILED, error=BackupError(f"Backup failed: {str(e)}")
        )


def create_temp_copy(
    df: pd.DataFrame, service: str, paths: Dict[str, str], partition_date: str
) -> OperationResult:
    """Create and verify temporary copy of data.

    Args:
        df (pd.DataFrame): Data to copy
        service (str): Service name
        paths (Dict[str, str]): Service paths
        partition_date (str): Partition date being processed

    Returns:
        OperationResult: Result of the temporary copy operation

    Behavior:
        1. Exports data to temporary location
        2. Verifies temporary copy integrity
        3. Returns operation status
    """
    try:
        # Create temp copy
        export_data_to_local_storage(
            service=service,
            df=df,
            export_format="parquet",
            override_local_prefix=paths["temp"],
        )

        # Verify temp copy
        temp_df = load_data_from_local_storage(
            service=service,
            partition_date=partition_date,
            output_format="df",
            override_local_prefix=os.path.dirname(os.path.dirname(paths["temp"])),
        )

        if not verify_dataframes_equal(df, temp_df):
            raise VerificationError("Temporary copy verification failed")

        return OperationResult(
            status=OperationStatus.SUCCESS,
            message=f"Successfully created temporary copy of {len(df)} records",
        )

    except Exception as e:
        return OperationResult(
            status=OperationStatus.FAILED,
            error=RepartitionError(f"Temporary copy failed: {str(e)}"),
        )


def export_repartitioned_data(
    df: pd.DataFrame,
    service: str,
    new_service_partition_key: str,
) -> OperationResult:
    """Export data with new partition key.

    Args:
        df (pd.DataFrame): Data to export
        service (str): Service name
        new_service_partition_key (str): New partition key field

    Returns:
        OperationResult: Result of the export operation

    Behavior:
        1. Updates service metadata with new partition key
        2. Exports data with updated metadata
        3. Returns operation status
    """
    try:
        # Update metadata and export
        service_metadata = MAP_SERVICE_TO_METADATA[service].copy()
        service_metadata["timestamp_field"] = new_service_partition_key

        export_data_to_local_storage(
            service=service,
            df=df,
            export_format="parquet",
            override_metadata=service_metadata,
        )

        return OperationResult(
            status=OperationStatus.SUCCESS,
            message=f"Successfully exported {len(df)} records with new partition key",
        )

    except Exception as e:
        return OperationResult(
            status=OperationStatus.FAILED,
            error=RepartitionError(f"Export failed: {str(e)}"),
        )


def repartition_data_for_partition_date(
    service: str,
    partition_date: str,
    new_service_partition_key: str = "preprocessing_timestamp",
) -> OperationResult:
    """Repartition data for a single partition date.

    Args:
        service (str): Name of the service to repartition
        partition_date (str): The partition date to process
        new_service_partition_key (str): Field name to use as the new partition key

    Returns:
        OperationResult: Result of the repartition operation

    Behavior:
        1. Gets paths and loads data
        2. Creates backup and temp copies
        3. Verifies data integrity
        4. Exports repartitioned data
        5. Returns operation status
    """
    paths = None
    try:
        paths = get_service_paths(service, partition_date)
        logger.info(
            f"Processing partition date {partition_date} for service {service}..."
        )

        # Load data
        df = load_data_from_local_storage(
            service=service,
            partition_date=partition_date,
            output_format="df",
        )

        if len(df) == 0:
            return OperationResult(
                status=OperationStatus.SUCCESS,
                message=f"No data found for partition date {partition_date}",
            )

        # Create backup
        export_data_to_local_storage(
            service=service,
            df=df,
            export_format="parquet",
            override_local_prefix=paths["backup"],
        )

        # Verify backup
        backup_df = load_data_from_local_storage(
            service=service,
            partition_date=partition_date,
            output_format="df",
            override_local_prefix=os.path.dirname(os.path.dirname(paths["backup"])),
        )

        if not verify_dataframes_equal(df, backup_df):
            raise VerificationError("Backup verification failed")

        # Create temp copy
        export_data_to_local_storage(
            service=service,
            df=df,
            export_format="parquet",
            override_local_prefix=paths["temp"],
        )

        # Verify temp copy
        temp_df = load_data_from_local_storage(
            service=service,
            partition_date=partition_date,
            output_format="df",
            override_local_prefix=os.path.dirname(os.path.dirname(paths["temp"])),
        )

        if not verify_dataframes_equal(df, temp_df):
            raise VerificationError("Temporary copy verification failed")

        # Delete original after successful backup and temp copy
        if os.path.exists(paths["original"]):
            shutil.rmtree(paths["original"])
            logger.info(f"Deleted original data at {paths['original']}")

        # Export repartitioned data
        service_metadata = MAP_SERVICE_TO_METADATA[service].copy()
        service_metadata["timestamp_field"] = new_service_partition_key

        export_data_to_local_storage(
            service=service,
            df=df,
            export_format="parquet",
            override_metadata=service_metadata,
        )

        return OperationResult(
            status=OperationStatus.SUCCESS,
            message=f"Successfully repartitioned {len(df)} records for {partition_date}",
        )

    except Exception as e:
        return OperationResult(status=OperationStatus.FAILED, error=e, message=str(e))
    finally:
        if paths:
            cleanup_temp_files(paths)


def repartition_data_for_partition_dates(
    start_date: str = "2024-09-28",
    end_date: str = "2025-12-01",
    service: str = "",
    new_service_partition_key: str = "preprocessing_timestamp",
    exclude_partition_dates: List[str] = None,
) -> Dict[str, OperationResult]:
    """Repartition data for multiple partition dates with error handling.

    Args:
        start_date (str): Start date in YYYY-MM-DD format (inclusive)
        end_date (str): End date in YYYY-MM-DD format (inclusive)
        service (str): Name of the service to repartition
        new_service_partition_key (str): Field name to use as the new partition key
        exclude_partition_dates (List[str], optional): List of dates to exclude

    Returns:
        Dict[str, OperationResult]: Dictionary mapping partition dates to their
            operation results

    Raises:
        ValueError: If service name is empty or not found in MAP_SERVICE_TO_METADATA

    Behavior:
        1. Validates service name and existence
        2. Generates list of partition dates to process
        3. Processes each partition date independently
        4. Returns results for all operations
    """
    if not service:
        raise ValueError("Service name is required")

    if service not in MAP_SERVICE_TO_METADATA:
        raise ValueError(f"Unknown service: {service}")

    exclude_partition_dates = exclude_partition_dates or ["2024-10-08"]
    results: Dict[str, OperationResult] = {}

    partition_dates: List[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )

    for partition_date in partition_dates:
        result = repartition_data_for_partition_date(
            service=service,
            partition_date=partition_date,
            new_service_partition_key=new_service_partition_key,
        )
        results[partition_date] = result

        if result.status == OperationStatus.FAILED:
            logger.error(f"Failed to process {partition_date}: {result.error}")
        elif result.status == OperationStatus.SKIPPED:
            logger.warning(f"Skipped {partition_date}: {result.message}")
        else:
            logger.info(f"Successfully processed {partition_date}: {result.message}")

    return results
