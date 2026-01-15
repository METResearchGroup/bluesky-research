"""Initialize the migration tracking database with the files to migrate.

Special handling:
- `preprocessed_posts` cache files exist under both `firehose/cache` and
  `most_liked/cache`, and there are non-partition artifacts at the top level.
- For archival, we collapse both sources into:
    {DEFAULT_S3_ROOT_PREFIX}/preprocessed_posts/cache/partition_date=YYYY-MM-DD/<file>.parquet
  (i.e., we remove the firehose vs most_liked distinction).
"""

import os
import re
from tqdm import tqdm
from lib.constants import root_local_data_directory
from scripts.migrate_research_data_to_s3.constants import (
    DEFAULT_S3_ROOT_PREFIX,
    PREFIXES_TO_MIGRATE,
)
from scripts.migrate_research_data_to_s3.migration_tracker import MigrationTracker

_PREPROCESSED_POSTS_CACHE_SOURCE_PREFIXES = (
    "preprocessed_posts/firehose/cache",
    "preprocessed_posts/most_liked/cache",
)

_PARTITION_DATE_DIR_RE = re.compile(r"^partition_date=\d{4}-\d{2}-\d{2}$")


def is_valid_partition_date_dirname(dirname: str) -> bool:
    """Returns True if dirname matches partition_date=YYYY-MM-DD."""
    return bool(_PARTITION_DATE_DIR_RE.match(dirname))


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
        for root, dirs, files in os.walk(full_local_prefix):
            # For cache directories, we only want partition_date=YYYY-MM-DD folders
            # directly under the cache root, and we ignore other top-level artifacts
            # (e.g., startTimestamp=... directories).
            if root == full_local_prefix and local_prefix.endswith("/cache"):
                dirs[:] = [d for d in dirs if is_valid_partition_date_dirname(d)]
            for file in files:
                full_path = os.path.join(root, file)
                filepaths.append(full_path)
                pbar.update(1)

    return filepaths


def _iter_preprocessed_posts_cache_parquet_files() -> list[dict[str, str]]:
    """Discover preprocessed_posts cache parquet files from both sources and
    return (local_path, s3_key) mappings with a collapsed destination layout.

    Rules:
    - only include paths under:
        preprocessed_posts/firehose/cache/partition_date=YYYY-MM-DD/*.parquet
        preprocessed_posts/most_liked/cache/partition_date=YYYY-MM-DD/*.parquet
    - ignore any other top-level artifacts in the cache dirs
    - collapse both sources into:
        {DEFAULT_S3_ROOT_PREFIX}/preprocessed_posts/cache/partition_date=YYYY-MM-DD/<filename>.parquet
    - hard-fail if two local files would map to the same S3 key
    """
    mappings: list[dict[str, str]] = []
    s3_key_to_local_path: dict[str, str] = {}

    for src_prefix in _PREPROCESSED_POSTS_CACHE_SOURCE_PREFIXES:
        cache_root = os.path.join(root_local_data_directory, src_prefix)
        if not os.path.isdir(cache_root):
            # If a source doesn't exist, we treat it as empty. This mirrors how
            # the generic initializer behaves with os.walk on missing dirs.
            continue

        for entry in os.listdir(cache_root):
            if not is_valid_partition_date_dirname(entry):
                continue
            partition_dir = os.path.join(cache_root, entry)
            if not os.path.isdir(partition_dir):
                continue

            for fname in os.listdir(partition_dir):
                if not fname.endswith(".parquet"):
                    continue
                local_path = os.path.join(partition_dir, fname)
                if not os.path.isfile(local_path):
                    continue

                # Collapse source-specific paths into a unified destination layout.
                s3_key = (
                    f"{DEFAULT_S3_ROOT_PREFIX}/preprocessed_posts/cache/{entry}/{fname}"
                )

                # Guardrail: prevent silent overwrites in S3 if key collisions occur.
                existing = s3_key_to_local_path.get(s3_key)
                if existing is not None and existing != local_path:
                    raise ValueError(
                        "S3 key collision detected while collapsing preprocessed_posts cache:\n"
                        f"  s3_key: {s3_key}\n"
                        f"  local_path_1: {existing}\n"
                        f"  local_path_2: {local_path}\n"
                        "Refusing to continue (would overwrite in S3)."
                    )
                s3_key_to_local_path[s3_key] = local_path
                mappings.append({"local_path": local_path, "s3_key": s3_key})

    return mappings


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
    preprocessed_posts_cache_initialized = False
    for local_prefix in tqdm(local_prefixes, desc="Processing prefixes"):
        # Special-case: collapse preprocessed_posts cache from both sources.
        if local_prefix in _PREPROCESSED_POSTS_CACHE_SOURCE_PREFIXES:
            if preprocessed_posts_cache_initialized:
                print(
                    f"Skipping {local_prefix}: preprocessed_posts cache already initialized "
                    f"via {_PREPROCESSED_POSTS_CACHE_SOURCE_PREFIXES}"
                )
                continue
            print(
                "Initializing migration tracker db for preprocessed_posts cache "
                f"(collapsed from sources: {_PREPROCESSED_POSTS_CACHE_SOURCE_PREFIXES})"
            )
            files_to_migrate = _iter_preprocessed_posts_cache_parquet_files()
            migration_tracker_db.register_files(files_to_migrate)
            preprocessed_posts_cache_initialized = True
            print(
                "Initialized migration tracker db for preprocessed_posts cache "
                f"({len(files_to_migrate)} files)"
            )
            continue

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
