"""Runs the migration process."""

import os

from tqdm import tqdm

from lib.aws.s3 import S3
from lib.log.logger import get_logger
from scripts.migrate_research_data_to_s3.constants import PREFIXES_TO_MIGRATE
from scripts.migrate_research_data_to_s3.migration_tracker import MigrationTracker

migration_tracker_db = MigrationTracker()

logger = get_logger(__name__)

s3_client = S3(create_client_flag=True)

# TODO: add callback
callback = None


# TODO: introduce batch logic later.
def migrate_file_to_s3(local_filepath: str, s3_key: str) -> tuple[bool, str]:
    """Migrates a file to S3.

    Returns:
        tuple[bool, str]: True if the file was migrated successfully, False otherwise.
        Error handling managed downstream.
    """
    if not os.path.exists(local_filepath):
        raise FileNotFoundError(f"File not found: {local_filepath}")

    file_size = os.path.getsize(local_filepath)
    file_size_mb = file_size / (1024**2)
    logger.info(f"Migrating file {local_filepath} to S3 ({file_size_mb:.2f} MB)")

    try:
        # upload_file automatically handles:
        # - Multipart uploads for files > 8MB
        # - Retries on failures
        # - Progress callbacks
        # - Streaming (doesn't load entire file into memory)
        if s3_client.client is None:
            raise RuntimeError("S3 client not initialized")
        s3_client.client.upload_file(
            local_filepath, s3_client.bucket, s3_key, Callback=callback
        )
        logger.info(
            f"Successfully uploaded {local_filepath} to s3://{os.path.join(s3_client.bucket, s3_key)}"
        )
        return (True, "")
    except Exception as e:
        logger.error(f"Error uploading {local_filepath} to S3: {e}")
        return (False, str(e))


def run_migration_for_single_prefix(prefix: str) -> None:
    """Runs the migration process."""

    files_to_migrate: list[dict[str, str]] = (
        migration_tracker_db.get_files_to_migrate_for_prefix(prefix)
    )

    for file_to_migrate in tqdm(files_to_migrate, desc=f"Migrating {prefix}"):
        local_filepath = file_to_migrate["local_path"]
        s3_key = file_to_migrate["s3_key"]
        success, error_message = migrate_file_to_s3(local_filepath, s3_key)
        if not success:
            migration_tracker_db.mark_failed(local_filepath, error_message)
        else:
            migration_tracker_db.mark_completed(local_filepath)


def run_migration_for_all_prefixes() -> None:
    """Runs the migration process for all prefixes."""
    for prefix in PREFIXES_TO_MIGRATE:
        run_migration_for_single_prefix(prefix)


if __name__ == "__main__":
    pass
