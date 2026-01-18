import os
from typing import Literal

from lib.validate_parquet import validated_pq_files_within_directory


def create_directory_if_not_exists(path: str) -> None:
    """Creates a directory if it does not exist."""
    dirpath = os.path.dirname(path)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)


def fetch_unique_filepaths_in_directory(
    directory: str, seen_filepaths: set[str] | None = None
) -> tuple[list[str], set[str]]:
    """Fetch unique filepaths in a directory.

    Returns:
        A tuple of (list of unique filepaths, set of seen filepaths).
    """
    new_seen_filepaths: set[str] = seen_filepaths or set()
    unique_filepaths: list[str] = []
    for root, _, files in os.walk(directory):
        for file in files:
            full_path = os.path.join(root, file)
            if full_path not in new_seen_filepaths:
                unique_filepaths.append(full_path)
                new_seen_filepaths.add(full_path)
    return unique_filepaths, new_seen_filepaths


# TODO: move over testing.
# TODO: add stronger typing to "directories" parameter.
def crawl_local_prefix(
    local_prefix: str,
    directories: list[Literal["cache", "active"]] = ["active"],
    validate_pq_files: bool = False,
) -> list[str]:
    """Crawls the local prefix and returns all filepaths.

    For the current format, the prefix would be <service>/<directory = cache / active>
    For the deprecated format, the prefix would be <service>/<source_type = firehose / most_liked>/<directory = cache / active>
    """
    loaded_filepaths: list[str] = []
    seen_filepaths: set[str] = set()

    for directory in directories:
        directory_filepath = os.path.join(local_prefix, directory)
        if validate_pq_files:
            validated_filepaths: list[str] = validated_pq_files_within_directory(
                directory_filepath
            )
            for filepath in validated_filepaths:
                if filepath not in seen_filepaths:
                    loaded_filepaths.append(filepath)
                    seen_filepaths.add(filepath)
        else:
            new_filepaths, new_seen_filepaths = fetch_unique_filepaths_in_directory(
                directory_filepath, seen_filepaths=seen_filepaths
            )
            loaded_filepaths.extend(new_filepaths)
            seen_filepaths.update(new_seen_filepaths)
    return loaded_filepaths
