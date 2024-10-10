"""Generic helpers for loading local data."""

from datetime import timedelta
import gzip
import json
import os
from typing import Literal, Optional

import pandas as pd
import pyarrow as pa

from lib.constants import (
    current_datetime,
    timestamp_format as DEFAULT_TIMESTAMP_FORMAT,
    default_lookback_days,
)
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger


logger = get_logger(__name__)

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

services_list = [
    "sync_firehose",
    "sync_most_liked_posts",
    "preprocessed_posts",
    "generate_vector_embeddings",
    "calculate_superposters",
    "daily_superposters",
    "ml_inference_perspective_api",
    "ml_inference_sociopolitical",
    "in_network_user_activity",
    "scraped_user_social_network",
    "consolidate_enrichment_integrations",
    "rank_score_feeds",
    "post_scores",
    "study_user_activity",
]


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


def data_is_older_than_lookback(
    end_timestamp: str, lookback_days: int = default_lookback_days
) -> bool:
    """Returns True if the data is older than the lookback days.

    Looks only at the start_timestamp and checks to see if the data is strictly
    older than the lookback days.

    For example, if we have some data from >=2024-05-01, at any point in
    2024-05-01, if our lookback_date is 2024-05-01, then that data is not
    strictly older than the lookback date.

    We err on the conservative side; all data has to be older than the lookback
    for it to be considered old. Since we chunk and group data by day, this should
    lead to a maximum staleness < 24 hours past the threshold. We can make
    this more strict if needed.
    """
    lookback_date = (current_datetime - timedelta(days=lookback_days)).strftime(
        "%Y-%m-%d"
    )
    return end_timestamp < lookback_date


def truncate_string(s: str) -> str:
    """Truncates the string after the first '.' or '+' or 'Z' (whichever comes first)."""
    for delimiter in [".", "+", "Z"]:
        if delimiter in s:
            return s.split(delimiter)[0]
    return s


def partition_data_by_date(
    df: pd.DataFrame, timestamp_field: str, timestamp_format: Optional[str] = None
) -> list[dict]:
    """Partitions data by date.

    Returns a list of dicts of the following format:
    {
        "start_timestamp": str,
        "end_timestamp": str,
        "data": pd.DataFrame
    }

    Transforms the timestamp field to a datetime field and then partitions the
    data by date. Each day's data is stored in a separate dataframe.
    """
    if not timestamp_format:
        timestamp_format = DEFAULT_TIMESTAMP_FORMAT

    # clean timestamp field if relevant.
    df[timestamp_field] = df[timestamp_field].apply(truncate_string)

    # convert to datetime
    df[f"{timestamp_field}_datetime"] = pd.to_datetime(
        df[timestamp_field], format=timestamp_format
    )

    df["partition_date"] = df[f"{timestamp_field}_datetime"].dt.date

    date_groups = df.groupby("partition_date")

    output: list[dict] = []

    for _, group in date_groups:
        # timestamps need to be transformed into the default format.
        start_timestamp = (
            group[f"{timestamp_field}_datetime"]
            .min()
            .strftime(DEFAULT_TIMESTAMP_FORMAT)  # noqa
        )
        end_timestamp = (
            group[f"{timestamp_field}_datetime"]
            .max()
            .strftime(DEFAULT_TIMESTAMP_FORMAT)
        )

        # drop additional grouping columns
        group = group.drop(columns=[f"{timestamp_field}_datetime"])

        # transform 'partition_date' to "YYYY-MM-DD" format.
        group["partition_date"] = (
            group["partition_date"]
            .apply(lambda x: x.strftime("%Y-%m-%d"))
            .astype("string")
        )

        # convert timestamp back to string type.
        group[timestamp_field] = group[timestamp_field].astype("string")

        output.append(
            {
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
                "data": group,
            }
        )

    return output


def export_data_to_local_storage(
    service: str,
    df: pd.DataFrame,
    export_format: Literal["jsonl", "parquet"] = "parquet",
    lookback_days: int = default_lookback_days,
    custom_args: Optional[dict] = None,
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
    timestamp_format = MAP_SERVICE_TO_METADATA[service].get("timestamp_format", None)
    chunked_dfs: list[dict] = partition_data_by_date(
        df=df, timestamp_field=timestamp_field, timestamp_format=timestamp_format
    )
    for chunk in chunked_dfs:
        # processing specific for firehose
        if service == "study_user_activity":
            record_type = custom_args["record_type"]
            local_prefix = MAP_SERVICE_TO_METADATA[service]["subpaths"][record_type]
        elif service == "preprocessed_posts":
            source = custom_args["source"]
            local_prefix = MAP_SERVICE_TO_METADATA[service]["subpaths"][source]
        elif service == "ml_inference_perspective_api":
            source = custom_args["source"]
            local_prefix = MAP_SERVICE_TO_METADATA[service]["subpaths"][source]
        elif service == "ml_inference_sociopolitical":
            source = custom_args["source"]
            local_prefix = MAP_SERVICE_TO_METADATA[service]["subpaths"][source]
        else:
            # generic processing
            local_prefix = MAP_SERVICE_TO_METADATA[service]["local_prefix"]
        start_timestamp: str = chunk["start_timestamp"]
        end_timestamp: str = chunk["end_timestamp"]
        chunk_df: pd.DataFrame = chunk["data"]
        file_created_timestamp = generate_current_datetime_str()
        filename = f"startTimestamp={start_timestamp}_endTimestamp={end_timestamp}_fileCreatedTimestamp={file_created_timestamp}.{export_format}"
        if data_is_older_than_lookback(
            end_timestamp=end_timestamp, lookback_days=lookback_days
        ):
            subfolder = "cache"
        else:
            subfolder = "active"
        # drop extra old column from compactions
        for col in ["row_num", "startTimestamp"]:
            if col in chunk_df.columns:
                chunk_df = chunk_df.drop(columns=[col])
        # /{root path}/{service-specific path}/{cache / active}/{filename}
        folder_path = os.path.join(local_prefix, subfolder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        local_export_fp = os.path.join(folder_path, filename)
        if export_format == "jsonl":
            chunk_df.to_json(local_export_fp, orient="records", lines=True)
        elif export_format == "parquet":
            # by default, we partition on the timestamp field. This will
            # allow us to use predicates when doing reads.
            partition_cols = MAP_SERVICE_TO_METADATA[service].get(
                "partition_cols", "partition_date"
            )  # noqa
            if "partition_date" not in chunk_df.columns:
                chunk_df["partition_date"] = pd.to_datetime(
                    chunk_df[timestamp_field]
                ).dt.date
            # NOTE: we don't use local_export_fp here because we want to
            # partition on the date field, and Parquet will include the partition
            # field name in the file path.
            chunk_df.to_parquet(folder_path, index=False, partition_cols=partition_cols)
        logger.info(
            f"Successfully exported {service} data from S3 to local store ({local_export_fp}) as {export_format}"
        )  # noqa


def get_local_prefixes_for_service(service: str) -> list[str]:
    if service == "preprocessed_posts":
        local_prefixes = []
        subpaths = MAP_SERVICE_TO_METADATA[service]["subpaths"]
        for _, subpath in subpaths.items():
            local_prefixes.append(subpath)
    elif (
        service == "ml_inference_perspective_api"
        or service == "ml_inference_sociopolitical"
    ):
        local_prefixes = []
        subpaths = MAP_SERVICE_TO_METADATA[service]["subpaths"]
        for _, subpath in subpaths.items():
            local_prefixes.append(subpath)
    else:
        local_prefix = MAP_SERVICE_TO_METADATA[service]["local_prefix"]
        if service == "study_user_activity":
            record_type = "post"  # we only fetch the posts from study users.
            local_prefix = MAP_SERVICE_TO_METADATA[service]["subpaths"][record_type]
        local_prefixes = [local_prefix]
    return local_prefixes


def list_filenames(
    service: str, directories: list[Literal["cache", "active"]] = ["active"]
) -> list[str]:
    """List files in local storage for a given service."""
    local_prefixes = get_local_prefixes_for_service(service)
    res = []
    for local_prefix in local_prefixes:
        for directory in directories:
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


def pd_type_to_pa_type(pd_type):
    """Convert pandas dtype to PyArrow type.

    Docs: https://arrow.apache.org/docs/python/api/datatypes.html
    """
    if pd_type == "Int64":
        return pa.int64()
    elif pd_type == "Float64":
        return pa.float64()
    elif pd_type == "bool":
        return pa.bool_()
    elif pd_type == "datetime64[ns]":
        return pa.timestamp("ns")
    elif pd_type == "object":
        return pa.string()  # Treat object as string
    else:
        return pa.string()


def get_service_pa_schema(service: str) -> Optional[pa.Schema]:
    dtypes_map = MAP_SERVICE_TO_METADATA[service].get("dtypes_map", None)
    # we add this here since when we transform the initial loaded dicts to
    # df (prior to writes), we don't have partition_date yet. However, we
    # want this to exist on read.
    if "partition_date" not in dtypes_map:
        dtypes_map["partition_date"] = "string"
    if dtypes_map:
        pa_schema = pa.schema(
            [(col, pd_type_to_pa_type(dtype)) for col, dtype in dtypes_map.items()]
        )
    else:
        pa_schema = None
    return pa_schema


def load_data_from_local_storage(
    service: str,
    directory: Literal["cache", "active"] = "active",
    export_format: Literal["jsonl", "parquet"] = "parquet",
    latest_timestamp: Optional[str] = None,
    use_all_data: bool = False,
) -> pd.DataFrame:
    """Load data from local storage."""
    directories = [directory]
    if use_all_data:
        directories = ["cache", "active"]

    filepaths = list_filenames(service=service, directories=directories)
    if export_format == "jsonl":
        df = pd.read_json(filepaths, orient="records", lines=True)
    elif export_format == "parquet":
        if latest_timestamp:
            # filter at least on the date field.
            # We can't partition data on the timestamp since that's too fine-grained, but we can at least partition on date.
            # This will give at least the subset of data on the correct date.
            latest_timestamp_date = (
                pd.to_datetime(latest_timestamp).date().strftime("%Y-%m-%d")
            )
            filters = [("partition_date", ">=", latest_timestamp_date)]
        else:
            filters = []
        kwargs = {"path": filepaths}
        columns: list[str] = load_service_cols(service)
        schema: Optional[pa.Schema] = get_service_pa_schema(service)
        if columns:
            kwargs["columns"] = columns
        if schema:
            kwargs["schema"] = schema
        if filters:
            kwargs["filters"] = filters
        df = pd.read_parquet(**kwargs)
        if schema:
            # attempt to convert dtypes after the fact. Parquet doesn't preserve the exact same dtypes as pandas so I need to
            # re-convert these after the fact.
            dtypes_map = MAP_SERVICE_TO_METADATA[service].get("dtypes_map", {})
            for col, dtype in dtypes_map.items():
                if dtype == "Int64":
                    df[col] = df[col].astype("Int64")
                elif dtype == "Float64":
                    df[col] = df[col].astype("Float64")
                elif dtype == "bool":
                    df[col] = df[col].astype("bool")
                elif dtype == "object":
                    df[col] = df[col].astype("object")
                elif dtype == "string":
                    df[col] = df[col].astype("string")
    if latest_timestamp:
        logger.info(f"Fetching data after timestamp={latest_timestamp}")
        timestamp_field = MAP_SERVICE_TO_METADATA[service]["timestamp_field"]
        df = df[df[timestamp_field] >= latest_timestamp]
    # drop extra columns that are added in during compaction steps.
    # (these will be added back in during compaction anyways)
    for col in ["row_num", "partition_date", "startTimestamp"]:
        if col in df.columns:
            df = df.drop(columns=[col])
    return df


def _validate_service(service: str) -> bool:
    return service in services_list


def load_latest_data(
    service: str,
    latest_timestamp: Optional[str] = None,
    max_per_source: Optional[int] = None,
) -> pd.DataFrame:
    """Loads the latest preprocessed posts."""
    if not _validate_service(service=service):
        raise ValueError(f"Invalid service: {service}")
    # most services just need the latest preprocessed raw data.
    df = load_data_from_local_storage(
        service="preprocessed_posts", latest_timestamp=latest_timestamp
    )
    if max_per_source:
        # Split the DataFrame by 'source'
        grouped = df.groupby("source")
        # Take the max_per_source for each group and concatenate them
        df = pd.concat([group.head(max_per_source) for _, group in grouped])
    return df
