"""Initialize the migration tracking database with the
files to migrate."""

import os
from lib.constants import root_local_data_directory
from scripts.migrate_research_data_to_s3.constants import (
    DEFAULT_S3_ROOT_PREFIX,
    PREFIXES_TO_MIGRATE,
)
from scripts.migrate_research_data_to_s3.migration_tracker import MigrationTracker

migration_tracker_db = MigrationTracker()


def get_filepaths_for_local_prefix(local_prefix: str) -> list[str]:
    """Given a local prefix, get all the filepaths for that prefix.

    Args:
        local_prefix: The local prefix to get the filepaths for.
    Returns:
        A list of filepaths for the given prefix.
    """
    full_local_prefix = os.path.join(root_local_data_directory, local_prefix)
    filepaths = []
    for root, _, files in os.walk(full_local_prefix):
        for file in files:
            full_path = os.path.join(root, file)
            filepaths.append(full_path)
    return filepaths


def get_s3_key_for_local_filepath(
    local_filepath: str,
    s3_root_prefix: str = DEFAULT_S3_ROOT_PREFIX,
) -> str:
    """Given a local filepath, get the S3  for that filepath."""
    # remove everything before the local prefix.
    local_filepath = local_filepath.replace(
        os.path.join(root_local_data_directory, ""), ""
    )
    s3_key = os.path.join(s3_root_prefix, local_filepath)
    return s3_key


def initialize_migration_tracker_db(local_prefixes: list[str]) -> None:
    """Initialize the migration tracking database with the
    files to migrate."""
    for local_prefix in local_prefixes:
        print(f"Initializing migration tracker db for {local_prefix}")
        local_filepaths = get_filepaths_for_local_prefix(local_prefix)
        s3_keys = [get_s3_key_for_local_filepath(fp) for fp in local_filepaths]
        files_to_migrate = [
            {"local_path": fp, "s3_key": s3_key}
            for fp, s3_key in zip(local_filepaths, s3_keys)
        ]
        migration_tracker_db.register_files(files_to_migrate)
        print(f"Initialized migration tracker db for {local_prefix}")
    print("Finished initializing migration tracker db")


if __name__ == "__main__":
    prefixes = PREFIXES_TO_MIGRATE
    initialize_migration_tracker_db(prefixes)
    migration_tracker_db.print_checklist()
