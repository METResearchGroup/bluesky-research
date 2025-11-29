"""Initialize the migration tracking database with the
files to migrate."""

import os
from tqdm import tqdm
from lib.constants import root_local_data_directory
from scripts.migrate_research_data_to_s3.constants import (
    DEFAULT_S3_ROOT_PREFIX,
    PREFIXES_TO_MIGRATE,
)
from scripts.migrate_research_data_to_s3.migration_tracker import MigrationTracker


def get_filepaths_for_local_prefix(local_prefix: str) -> list[str]:
    """Given a local prefix, get all the filepaths for that prefix.

    Args:
        local_prefix: The local prefix to get the filepaths for.
    Returns:
        A list of filepaths for the given prefix.
    """
    full_local_prefix = os.path.join(root_local_data_directory, local_prefix)
    filepaths = []

    # Count total files first for progress bar
    total_files = sum(len(files) for _, _, files in os.walk(full_local_prefix))

    with tqdm(
        total=total_files, desc=f"Discovering files in {local_prefix}", leave=False
    ) as pbar:
        for root, _, files in os.walk(full_local_prefix):
            for file in files:
                full_path = os.path.join(root, file)
                filepaths.append(full_path)
                pbar.update(1)

    return filepaths


def get_s3_key_for_local_filepath(
    local_filepath: str,
    s3_root_prefix: str = DEFAULT_S3_ROOT_PREFIX,
) -> str:
    """Given a local filepath, get the S3 key for that filepath.

    Assumes that local filepath is an absolute path (local = not in AWS)

    Args:
        local_filepath: Absolute path to the local file
        s3_root_prefix: S3 root prefix to prepend to the relative path

    Returns:
        S3 key string with forward slashes

    Raises:
        ValueError: If the file path is not under the root directory
    """
    from pathlib import Path

    local_path = Path(local_filepath)
    root_path = Path(root_local_data_directory)

    try:
        # Get relative path from root
        relative_path = local_path.relative_to(root_path)
    except ValueError:
        raise ValueError(
            f"File path {local_filepath} is not under root directory {root_local_data_directory}"
        )

    # Convert to string with forward slashes (S3 always uses /). Applies
    # to Windows paths, so here for defensibility.
    relative_str = str(relative_path).replace("\\", "/")

    # avoiding os.path.join since it uses the OS-specific path separator,
    # and we want to ensure consistency across platforms (e.g., it would
    # use backslashes on Windows).
    s3_key = f"{s3_root_prefix}/{relative_str}" if s3_root_prefix else relative_str

    return s3_key


def initialize_migration_tracker_db(
    local_prefixes: list[str], migration_tracker_db: MigrationTracker
) -> None:
    """Initialize the migration tracking database with the
    files to migrate."""
    for local_prefix in tqdm(local_prefixes, desc="Processing prefixes"):
        print(f"Initializing migration tracker db for {local_prefix}")
        local_filepaths = get_filepaths_for_local_prefix(local_prefix)
        s3_keys = [get_s3_key_for_local_filepath(fp) for fp in local_filepaths]

        files_to_migrate = [
            {"local_path": fp, "s3_key": s3_key}
            for fp, s3_key in zip(local_filepaths, s3_keys)
        ]
        migration_tracker_db.register_files(files_to_migrate)
        print(
            f"Initialized migration tracker db for {local_prefix} ({len(files_to_migrate)} files)"
        )
    print("Finished initializing migration tracker db")


if __name__ == "__main__":
    prefixes = PREFIXES_TO_MIGRATE
    migration_tracker_db = MigrationTracker()
    initialize_migration_tracker_db(prefixes, migration_tracker_db)
    migration_tracker_db.print_checklist(failed_files_preview_size=10)
