import os

from lib.db.models import StorageTier
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
def crawl_local_prefix(
    local_prefix: str,
    storage_tiers: list[StorageTier],
    validate_pq_files: bool = False,
) -> list[str]:
    """Crawls the local prefix and returns all filepaths.

    For the current format, the prefix would be <service>/<directory = cache / active>
    For the deprecated format, the prefix would be <service>/<source_type = firehose / most_liked>/<directory = cache / active>
    """
    loaded_filepaths: list[str] = []
    seen_filepaths: set[str] = set()

    for storage_tier in storage_tiers:
        directory_filepath = os.path.join(local_prefix, storage_tier.value)
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


def filter_filepaths_by_date_range(
    filepaths: list[str],
    partition_date: str | None = None,
    start_partition_date: str | None = None,
    end_partition_date: str | None = None,
) -> list[str]:
    """Filter filepaths by date range.

    Can filter by either a single partition_date or a date range
    (start_partition_date to end_partition_date).
    """
    _validate_input_dates_for_filepath_filtering(
        partition_date=partition_date,
        start_partition_date=start_partition_date,
        end_partition_date=end_partition_date,
    )
    if partition_date:
        return _filter_filepaths_by_partition_date(
            filepaths=filepaths,
            partition_date=partition_date,
        )
    if start_partition_date and end_partition_date:
        return _filter_filepaths_by_date_range(
            filepaths=filepaths,
            start_partition_date=start_partition_date,
            end_partition_date=end_partition_date,
        )
    return []


def _validate_input_dates_for_filepath_filtering(
    partition_date: str | None = None,
    start_partition_date: str | None = None,
    end_partition_date: str | None = None,
):
    """Validates invalid input dates for filepath filtering.

    There are three possible invalid input cases:
    - No dates provided
    - One of start_partition_date or end_partition_date is provided, but not both
    - Partition date and one/both of start/end partition dates are provided
    """
    if (
        # no dates provided
        not partition_date and not start_partition_date and not end_partition_date
    ):
        raise ValueError(
            "At least one of partition_date, start_partition_date, or end_partition_date must be provided."
        )
    if (
        # one of start_partition_date or end_partition_date is provided, but not both
        (start_partition_date and not end_partition_date)
        or (end_partition_date and not start_partition_date)
    ):
        raise ValueError(
            "Both start_partition_date and end_partition_date must be provided together."
        )
    if (
        # partition date and one/both of start/end partition dates are provided
        partition_date and (start_partition_date or end_partition_date)
    ):
        raise ValueError(
            "Cannot use partition_date and start_partition_date or end_partition_date together."
        )


def _filter_filepaths_by_partition_date(
    filepaths: list[str],
    partition_date: str,
) -> list[str]:
    """Filter filepaths by partition date."""
    filtered_filepaths: list[str] = []
    for filepath in filepaths:
        file_partition_date = _get_partition_date_from_filepath(filepath)
        if file_partition_date == partition_date:
            filtered_filepaths.append(filepath)
    return filtered_filepaths


def _filter_filepaths_by_date_range(
    filepaths: list[str],
    start_partition_date: str,
    end_partition_date: str,
) -> list[str]:
    """Filter filepaths by date range."""
    filtered_filepaths: list[str] = []
    for filepath in filepaths:
        file_partition_date = _get_partition_date_from_filepath(filepath)
        if (
            file_partition_date
            and start_partition_date <= file_partition_date <= end_partition_date
        ):
            filtered_filepaths.append(filepath)
    return filtered_filepaths


def _get_partition_date_from_filepath(filepath: str) -> str:
    """Get the partition date from a filepath.

    Filepath example:
    - /projects/p32375/bluesky_research_data/ml_inference_perspective_api/
    cache/partition_date=2024-09-29/bbab32f2d9764d52a3d89a7aee014192-0.parquet

    Here, the partition date is "2024-09-29".
    """
    path_parts = filepath.split("/")
    partition_parts = [p for p in path_parts if "partition_date=" in p]
    if len(partition_parts) == 0:
        return ""
    # "partition_date=YYYY-MM-DD" -> "YYYY-MM-DD"
    return partition_parts[0].split("=")[1]
