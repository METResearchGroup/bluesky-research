"""Generic helpers for loading local data."""

import gzip
import json
import os
from typing import Literal, Optional

import pandas as pd

from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger


logger = get_logger(__name__)


def load_jsonl_data(filepath: str) -> list[dict]:
    """Load JSONL data from a file, supporting gzipped files."""
    if filepath.endswith(".gz"):
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            data = [json.loads(line) for line in f]
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            data = [json.loads(line) for line in f]
    return data


def write_jsons_to_local_store(
    source_directory: Optional[str] = None,
    records: Optional[list[dict]] = None,
    export_filepath: str = None,
    compressed: bool = True,
):
    """Writes local JSONs to local store. Writes as a .jsonl file.

    Loads in JSONs from a given directory and writes them to the local storage
    using the export filepath.
    """
    dirpath = os.path.dirname(export_filepath)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

    res: list[dict] = []

    if not source_directory and records:
        res = records
    elif source_directory:
        for file in os.listdir(source_directory):
            if file.endswith(".json"):
                with open(os.path.join(source_directory, file), "r") as f:
                    res.append(json.load(f))
    elif not source_directory and not records:
        raise ValueError("No source data provided.")

    intermediate_filepath = export_filepath
    if compressed:
        intermediate_filepath += ".gz"

    # Write the JSON lines to a file
    if not compressed:
        with open(export_filepath, "w") as f:
            for item in res:
                f.write(json.dumps(item) + "\n")
    else:
        with gzip.open(intermediate_filepath, "wt") as f:
            for item in res:
                f.write(json.dumps(item) + "\n")


def find_files_after_timestamp(base_path: str, target_timestamp_path: str) -> list[str]:
    """Find files after a given timestamp."""
    year, month, day, hour, minute = target_timestamp_path.split("/")
    files_list = []
    for year_dir in os.listdir(base_path):
        if year_dir >= year:
            if year_dir > year:
                # crawl all files, even in subdirectories, and add to list
                # of files
                year_dir_path = os.path.join(base_path, year_dir)
                for root, _, files in os.walk(year_dir_path):
                    for file in files:
                        files_list.append(os.path.join(root, file))
                continue
            else:
                # case where year_dir == year
                months = os.listdir(os.path.join(base_path, year_dir))
                for month_dir in months:
                    # if same year + more recent month, crawl all files.
                    if month_dir > month:
                        # crawl all files, even in subdirectories, and add
                        # to list of files
                        month_dir_path = os.path.join(base_path, year_dir, month_dir)  # noqa
                        for root, _, files in os.walk(month_dir_path):
                            for file in files:
                                files_list.append(os.path.join(root, file))
                    elif month_dir == month:
                        # if same month, check days
                        days = os.listdir(os.path.join(base_path, year_dir, month_dir))
                        for day_dir in days:
                            # if same year + same month + more recent day,
                            # crawl all files.
                            if day_dir > day:
                                # crawl all files, even in subdirectories, and
                                # add to list of files
                                day_dir_path = os.path.join(
                                    base_path, year_dir, month_dir, day_dir
                                )
                                for root, _, files in os.walk(day_dir_path):
                                    for file in files:
                                        files_list.append(os.path.join(root, file))  # noqa
                            elif day_dir == day:
                                # if same day, check hours
                                hours = os.listdir(
                                    os.path.join(
                                        base_path, year_dir, month_dir, day_dir
                                    )  # noqa
                                )
                                for hour_dir in hours:
                                    # if same year + same month + same day +
                                    # more recent hour, crawl all files.
                                    if hour_dir > hour:
                                        # crawl all files, even in
                                        # subdirectories, and add to list
                                        # of files
                                        hour_dir_path = os.path.join(
                                            base_path,
                                            year_dir,
                                            month_dir,
                                            day_dir,
                                            hour_dir,  # noqa
                                        )
                                        for root, _, files in os.walk(hour_dir_path):  # noqa
                                            for file in files:
                                                files_list.append(
                                                    os.path.join(root, file)
                                                )  # noqa
                                    elif hour_dir == hour:
                                        # if same hour, check minutes
                                        minutes = os.listdir(
                                            os.path.join(
                                                base_path,
                                                year_dir,
                                                month_dir,
                                                day_dir,
                                                hour_dir,  # noqa
                                            )
                                        )
                                        for minute_dir in minutes:
                                            # if same year + same month +
                                            # same day + same hour + more
                                            # recent minute, crawl all files.
                                            if minute_dir > minute:
                                                # crawl all files, even in
                                                # subdirectories, and add to
                                                # list of files
                                                minute_dir_path = os.path.join(
                                                    base_path,
                                                    year_dir,
                                                    month_dir,
                                                    day_dir,
                                                    hour_dir,
                                                    minute_dir,  # noqa
                                                )
                                                for root, _, files in os.walk(
                                                    minute_dir_path
                                                ):  # noqa
                                                    for file in files:
                                                        files_list.append(
                                                            os.path.join(root, file)
                                                        )  # noqa

    return files_list


def load_local_data() -> pd.DataFrame:
    pass


def data_is_older_than_lookback(
    start_timestamp: str, end_timestamp: str, lookback_days: int
) -> bool:
    """Returns True if the data is older than the lookback days."""
    return True


def partition_data_by_date(df: pd.DataFrame, timestamp_field: str) -> list[dict]:
    """Partitions data by date.

    Returns a list of dicts of the following format:
    {
        "start_timestamp": str,
        "end_timestamp": str,
        "data": pd.DataFrame
    }
    """
    return []


def export_data_to_local_storage(
    service: str,
    df: pd.DataFrame,
    export_format: Literal["jsonl", "parquet"],
    lookback_days: int,
) -> None:
    """Exports data to local storage.

    Any data older than "lookback_days" will be stored in the "/cache"
    path while any data more recent than "lookback_days" will be stored in
    the "/active" path.

    Receives a generic dataframe and exports it to local storage.
    """
    # metadata needs to include timestamp field so that we can figure out what
    # data is old vs. new
    timestamp_field = MAP_SERVICE_TO_METADATA[service]["timestamp_field"]
    chunked_dfs: list[dict] = partition_data_by_date(
        df=df, timestamp_field=timestamp_field
    )
    for chunk in chunked_dfs:
        local_prefix = MAP_SERVICE_TO_METADATA[service]["local_prefix"]
        start_timestamp = chunk["start_timestamp"]
        end_timestamp = chunk["end_timestamp"]
        file_created_timestamp = generate_current_datetime_str()
        filename = f"startTimestamp={start_timestamp}_endTimestamp={end_timestamp}_fileCreatedTimestamp={file_created_timestamp}.{export_format}"
        if data_is_older_than_lookback(start_timestamp, end_timestamp, lookback_days):
            subfolder = "cache"
        else:
            subfolder = "active"
        # /{root path}/{service-specific path}/{cache / active}/{filename}
        local_export_fp = os.path.join(local_prefix, subfolder, filename)
        if export_format == "jsonl":
            df.to_json(local_export_fp, orient="records", lines=True)
        elif export_format == "parquet":
            partition_cols = MAP_SERVICE_TO_METADATA[service].get(
                "partition_cols", None
            )  # noqa
            df.to_parquet(local_export_fp, index=False, partition_cols=partition_cols)
        logger.info(
            f"Successfully exported {service} data from S3 to local store ({local_export_fp}) as {export_format}"
        )  # noqa


def list_filenames(
    service: str, directories: Literal["cache", "active"] = ["active"]
) -> list[str]:
    """List files in local storage for a given service."""
    local_prefix = MAP_SERVICE_TO_METADATA[service]["local_prefix"]
    res = []
    for directory in directories:
        # TODO: check this implementation.
        fp = os.path.join(local_prefix, directory)
        for root, _, files in os.walk(fp):
            for file in files:
                res.append(os.path.join(root, file))
    return res


def delete_files(filepaths: list[str]) -> None:
    """Delete files from local storage."""
    logger.info(f"Deleting {len(filepaths)} files from local storage...\n\t{filepaths}")
    for filepath in filepaths:
        os.remove(filepath)
    logger.info(f"Successfully deleted {len(filepaths)} files from local storage.")


def load_service_cols(service: str) -> list[str]:
    """Load the columns for a given service."""
    return []


def load_data_from_local_storage(
    service: str,
    directory: Literal["cache", "active"] = "active",
    export_format: Literal["jsonl", "parquet"] = "parquet",
) -> pd.DataFrame:
    """Load data from local storage."""
    filepaths = list_filenames(service=service, directories=[directory])
    if export_format == "jsonl":
        df = pd.read_json(filepaths, orient="records", lines=True)
    elif export_format == "parquet":
        columns: list[str] = load_service_cols(service)
        df = pd.read_parquet(filepaths, columns=columns)
    return df
