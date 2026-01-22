"""Runs the migration process."""

from collections.abc import Callable
import os
from pathlib import Path

import botocore
from tqdm import tqdm

from lib.aws.s3 import S3
from lib.log.logger import get_logger
from scripts.migrate_research_data_to_s3.constants import PREFIXES_TO_MIGRATE
from scripts.migrate_research_data_to_s3.migration_tracker import MigrationTracker

logger = get_logger(__name__)

MB_IN_BYTES = 1024**2


def create_progress_callback(file_path: str, file_size: int) -> Callable[[int], None]:
    """Create a progress callback function for file uploads.

    Args:
        file_path: Path to the file being uploaded
        file_size: Size of the file in bytes

    Returns:
        Callback function for upload progress

    Raises:
        ValueError: If file_size is zero or negative
    """
    if file_size <= 0:
        raise ValueError(
            f"Cannot create progress callback for zero or negative file size: {file_size}"
        )

    uploaded = [0]  # Use list to allow modification in nested function

    def callback(bytes_amount: int) -> None:
        uploaded[0] += bytes_amount
        percent = (uploaded[0] / file_size) * 100
        mb_uploaded = uploaded[0] / MB_IN_BYTES
        mb_total = file_size / MB_IN_BYTES
        logger.debug(
            f"{Path(file_path).name}: {percent:.1f}% ({mb_uploaded:.2f} MB / {mb_total:.2f} MB)"
        )

    return callback


def migrate_file_to_s3(
    local_filepath: str, s3_key: str, s3_client: S3
) -> tuple[bool, str]:
    """Migrates a file to S3.

    Returns:
        tuple[bool, str]: True if the file was migrated successfully, False otherwise.
        Error handling managed downstream.
    """
    try:
        if not os.path.exists(local_filepath):
            raise FileNotFoundError(f"File not found: {local_filepath}")

        file_size = os.path.getsize(local_filepath)
        file_size_mb = file_size / MB_IN_BYTES
        logger.info(f"Migrating file {local_filepath} to S3 ({file_size_mb:.2f} MB)")

        # upload_file automatically handles:
        # - Multipart uploads for files > 8MB
        # - Retries on failures
        # - Progress callbacks
        # - Streaming (doesn't load entire file into memory)
        if s3_client.client is None:
            raise RuntimeError("S3 client not initialized")
        callback = create_progress_callback(local_filepath, file_size)
        s3_client.client.upload_file(
            local_filepath, s3_client.bucket, s3_key, Callback=callback
        )
        logger.info(
            f"Successfully uploaded {local_filepath} to s3://{s3_client.bucket}/{s3_key}"
        )
        return (True, "")
    except (FileNotFoundError, OSError) as e:
        # File system errors
        logger.error(f"File system error uploading {local_filepath} to S3: {e}")
        return (False, str(e))
    except botocore.exceptions.ClientError as e:
        # AWS S3 errors
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        logger.error(f"AWS error uploading {local_filepath} to S3: {error_code} - {e}")
        return (False, f"AWS {error_code}: {str(e)}")
    except (RuntimeError, ValueError) as e:
        # Configuration/validation errors
        logger.error(f"Configuration error uploading {local_filepath} to S3: {e}")
        return (False, str(e))
    except Exception as e:
        logger.critical(
            f"Unexpected error uploading {local_filepath} to S3: {e}",
            exc_info=True,
        )
        raise


def run_migration_for_single_prefix(
    prefix: str, migration_tracker_db: MigrationTracker, s3_client: S3
) -> None:
    """Runs the migration process."""

    files_to_migrate: list[dict[str, str]] = (
        migration_tracker_db.get_files_to_migrate_for_prefix(prefix)
    )

    for file_to_migrate in tqdm(files_to_migrate, desc=f"Migrating {prefix}"):
        local_filepath = file_to_migrate["local_path"]
        s3_key = file_to_migrate["s3_key"]

        # Mark as started before uploading
        migration_tracker_db.mark_started(local_filepath)
        success, error_message = migrate_file_to_s3(local_filepath, s3_key, s3_client)
        if not success:
            migration_tracker_db.mark_failed(local_filepath, error_message)
        else:
            migration_tracker_db.mark_completed(local_filepath)


def run_migration_for_all_prefixes(
    migration_tracker_db: MigrationTracker, s3_client: S3
) -> None:
    """Runs the migration process for all prefixes."""
    for prefix in tqdm(PREFIXES_TO_MIGRATE, desc="Processing prefixes"):
        run_migration_for_single_prefix(prefix, migration_tracker_db, s3_client)


def run_migration_for_prefixes(
    prefixes: list[str],
    migration_tracker_db: MigrationTracker,
    s3_client: S3,
) -> None:
    """Run migration only for the given prefixes."""
    for prefix in tqdm(prefixes, desc="Processing prefixes"):
        run_migration_for_single_prefix(prefix, migration_tracker_db, s3_client)


if __name__ == "__main__":
    migration_tracker_db = MigrationTracker()
    s3_client = S3(create_client_flag=True)
    run_migration_for_all_prefixes(
        migration_tracker_db=migration_tracker_db,
        s3_client=s3_client,
    )
