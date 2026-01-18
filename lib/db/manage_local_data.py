"""Generic helpers for loading local data."""

from datetime import timedelta
import os
from typing import Literal, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from lib.constants import (
    current_datetime,
    timestamp_format as DEFAULT_TIMESTAMP_FORMAT,
    default_lookback_days,
)
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.db.sql.duckdb_wrapper import DuckDB
from lib.datetime_utils import generate_current_datetime_str, truncate_timestamp_string
from lib.log.logger import get_logger
from lib.path_utils import crawl_local_prefix

logger = get_logger(__name__)
duckDB = DuckDB()

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
    "ml_inference_ime",
    "ml_inference_valence_classifier",
    "in_network_user_activity",
    "scraped_user_social_network",
    "consolidate_enrichment_integrations",
    "rank_score_feeds",
    "post_scores",
    "raw_sync",
]

# there are some really weird records that come along and have "created_at"
# dates that are completely implausible. We'll set these to a default partition
# date of 2016-01-01 so that we can at least process the data and then we
# can keep these in one place.
DEFAULT_ERROR_PARTITION_DATE = "2016-01-01"


# TODO: come back to this, as the name + signature doesn't match the docstring
# (docstring implies arbitrary start_timestamp, but the function doesn't
# take one).
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


# NOTE: come back to this after refactoring `partition_data_by_date`, so
# we can see how it's supposed to be used.
def convert_timestamp(x, timestamp_format):
    """Attempts to convert a timestamp to a datetime.

    If the timestamp is not in the correct format, it will be converted to a
    default partition date of 2016-01-01.

    Also checks for the year of the post. Sometimes the timestamp is corrupted
    and the year is before 2024. In this case, we'll log a warning and return
    the default partition date.
    """
    try:
        dt = pd.to_datetime(x, format=timestamp_format)
        if dt.year < 2024:
            # a bit noisy, plus this is an OK default behavior.
            # logger.warning(
            #     f"Timestamp year {dt.year} is before 2024, will try to coerce using {DEFAULT_ERROR_PARTITION_DATE}: {x}."
            # )
            pass
        else:
            return dt
    except Exception as e:
        logger.warning(
            f"Error converting timestamp ({e}), will try to coerce using {DEFAULT_ERROR_PARTITION_DATE}: {x}."
        )
    return pd.to_datetime(DEFAULT_ERROR_PARTITION_DATE, format="%Y-%m-%d")


def partition_data_by_date(
    df: pd.DataFrame,
    timestamp_field: str,
    timestamp_format: Optional[str] = None,
    skip_date_validation: bool = False,
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
    # TODO: need to fix column matching. Don't think these overlap correctly.
    # should process each dtype correctly. Either need the same cols or need
    # to process each dtype separately.
    if not timestamp_format:
        timestamp_format = DEFAULT_TIMESTAMP_FORMAT

    # clean timestamp field if relevant.
    df[timestamp_field] = df[timestamp_field].apply(truncate_timestamp_string)

    try:
        # convert to datetime
        df[f"{timestamp_field}_datetime"] = pd.to_datetime(
            df[timestamp_field], format=timestamp_format
        )
        years = df[f"{timestamp_field}_datetime"].dt.year
        total_invalid_years = sum(1 for year in years if year < 2024)
        if total_invalid_years > 0 and not skip_date_validation:
            raise ValueError(
                f"""
                Some records have years before 2024. This is impossible and an 
                error on Bluesky's part, so we're going to coerce those records
                to a default partition date.
                Total records affected: {total_invalid_years}.
                """
            )
    except Exception as e:
        # sometimes weird records come along. We'll set these, as a default,
        # as being written to a default partition_date.
        logger.warning(f"Error converting timestamp field to datetime: {e}")
        df[f"{timestamp_field}_datetime"] = df[timestamp_field].apply(
            lambda x: convert_timestamp(x, timestamp_format)
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


def _convert_service_name_to_db_name(service: str) -> str:
    """Converts the service name to the version used to define the DB.

    NOTE: tbh normally these two are equivalent, but idk why past me decided
    to change 'preprocess_raw_data' to 'preprocessed_posts'. Tryna be slick ig.

    Anyways, due for a refactoring later.
    """
    if service == "preprocess_raw_data":
        print("Converting 'preprocess_raw_data' to 'preprocessed_posts'...")
        return "preprocessed_posts"
    else:
        return service


def export_data_to_local_storage(
    service: str,
    df: pd.DataFrame,
    export_format: Literal["jsonl", "parquet"] = "parquet",
    lookback_days: int = default_lookback_days,
    custom_args: Optional[dict] = None,
    override_local_prefix: Optional[str] = None,
) -> None:
    """Exports data to local storage.

    Any data older than "lookback_days" will be stored in the "/cache"
    path while any data more recent than "lookback_days" will be stored in
    the "/active" path.

    Args:
        service: Name of the service to export data for
        df: DataFrame containing the data to export
        export_format: Format to export data in ("jsonl" or "parquet")
        lookback_days: Number of days to look back for determining cache vs active
        custom_args: Optional custom arguments for specific services
        override_local_prefix: Optional override for the service's local prefix path

    Receives a generic dataframe and exports it to local storage.
    """
    service = _convert_service_name_to_db_name(service)
    # metadata needs to include timestamp field so that we can figure out what
    # data is old vs. new
    timestamp_field = MAP_SERVICE_TO_METADATA[service]["timestamp_field"]
    timestamp_format = MAP_SERVICE_TO_METADATA[service].get("timestamp_format", None)
    skip_date_validation = MAP_SERVICE_TO_METADATA[service].get(
        "skip_date_validation", False
    )
    # Override skip_date_validation for backfill_sync if custom_args provides a specific source
    # NOTE: This is done because the output formatting for backfill syncs is slightly different
    # from other parts of the pipeline. Should be consolidated at some point.
    if service == "raw_sync" and custom_args and "record_type" in custom_args:
        skip_date_validation = False
    if skip_date_validation:
        chunks = [
            {
                "start_timestamp": generate_current_datetime_str(),
                "end_timestamp": generate_current_datetime_str(),
                "data": df,
            }
        ]
    else:
        chunks: list[dict] = partition_data_by_date(
            df=df,
            timestamp_field=timestamp_field,
            timestamp_format=timestamp_format,
            skip_date_validation=skip_date_validation,
        )
    for chunk in chunks:
        # processing specific for firehose
        if override_local_prefix:
            local_prefix = override_local_prefix
        elif service == "backfill_sync":
            if custom_args and "source" in custom_args:
                local_prefix = MAP_SERVICE_TO_METADATA[service]["subpaths"][
                    custom_args["source"]
                ]
            elif skip_date_validation:
                local_prefix = MAP_SERVICE_TO_METADATA[service]["local_prefix"]
            else:
                if "record_type" in chunk["data"].columns:
                    record_type_groups: dict[str, pd.DataFrame] = {}
                    for record_type, group_df in chunk["data"].groupby("record_type"):
                        record_type_groups[record_type] = group_df

                    for record_type, group_df in record_type_groups.items():
                        logger.info(
                            f"Exporting {record_type} data for service={service} to local storage for raw_sync..."
                        )
                        export_data_to_local_storage(
                            service="raw_sync",
                            df=group_df,
                            export_format=export_format,
                            lookback_days=lookback_days,
                            custom_args={"record_type": record_type},
                        )
                    logger.info(
                        "Completed writing backfill sync data to local storage."
                    )
                    continue
                else:
                    raise ValueError(
                        f"No record_type column found in dataframe for backfill service={service}."
                    )
        elif service == "raw_sync":
            record_type = custom_args["record_type"]
            local_prefix = MAP_SERVICE_TO_METADATA[service]["subpaths"][record_type]
        # elif service == "preprocessed_posts":
        #     source = custom_args["source"]
        #     local_prefix = MAP_SERVICE_TO_METADATA[service]["subpaths"][source]
        # elif service == "ml_inference_perspective_api":
        #     source = custom_args["source"]
        #     local_prefix = MAP_SERVICE_TO_METADATA[service]["subpaths"][source]
        # elif service == "ml_inference_sociopolitical":
        #     source = custom_args["source"]
        #     local_prefix = MAP_SERVICE_TO_METADATA[service]["subpaths"][source]
        else:
            # generic processing
            local_prefix = MAP_SERVICE_TO_METADATA[service]["local_prefix"]
        if service == "backfill_sync" and not (
            skip_date_validation or (custom_args and "source" in custom_args)
        ):
            continue
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
                "partition_cols", ["partition_date"]
            )
            if isinstance(partition_cols, str):
                partition_cols = [partition_cols]

            if chunk_df.empty:
                logger.warning(
                    f"[Service = {service}] Received empty dataframe; skipping parquet export."
                )
                continue

            # Ensure `partition_date` exists and matches the schema contract (string).
            # Derive partition_date from timestamp_field when missing:
            # 1. Extract timestamp field (apply truncate_timestamp_string if it's a string timestamp)
            # 2. Fallback to DEFAULT_ERROR_PARTITION_DATE if timestamp_field is missing
            # 3. Parse to datetime (with format if provided, otherwise auto-detect)
            # 4. Coerce invalid values to NaT, then fill with DEFAULT_ERROR_PARTITION_DATE
            # 5. Convert to stable string format "%Y-%m-%d" for consistent partitioning
            if "partition_date" not in chunk_df.columns:
                logger.warning(
                    f"[Service = {service}] partition_date column missing; deriving from timestamp_field={timestamp_field}"
                )
                if timestamp_field not in chunk_df.columns:
                    logger.warning(
                        f"[Service = {service}] timestamp_field={timestamp_field} not found; using DEFAULT_ERROR_PARTITION_DATE={DEFAULT_ERROR_PARTITION_DATE}"
                    )
                    ts_series = pd.Series(
                        [DEFAULT_ERROR_PARTITION_DATE] * len(chunk_df)
                    )
                else:
                    ts_series = chunk_df[timestamp_field].apply(
                        truncate_timestamp_string
                    )
                if timestamp_format:
                    ts_dt = pd.to_datetime(
                        ts_series, format=timestamp_format, errors="coerce"
                    )
                else:
                    logger.warning(
                        f"[Service = {service}] No timestamp_format provided; using auto-detection for timestamp parsing"
                    )
                    ts_dt = pd.to_datetime(ts_series, errors="coerce")
                ts_dt = ts_dt.fillna(
                    pd.to_datetime(DEFAULT_ERROR_PARTITION_DATE, format="%Y-%m-%d")
                )
                chunk_df["partition_date"] = ts_dt.dt.strftime("%Y-%m-%d").astype(
                    "string"
                )
            else:
                chunk_df["partition_date"] = chunk_df["partition_date"].astype("string")

            if timestamp_field in chunk_df.columns:
                # Keep timestamps stable (we typically store them as strings).
                chunk_df[timestamp_field] = chunk_df[timestamp_field].astype("string")

            # NOTE: we don't use local_export_fp here because we want to
            # partition on the date field, and Parquet will include the partition
            # field name in the file path.
            # Sort partition dates for deterministic selection (previous implementation
            # used iloc[0] which could be non-deterministic). We select the earliest
            # date for logging purposes, though Parquet partitioning handles multiple
            # dates correctly.
            output_partition_dates = sorted(
                chunk_df["partition_date"].dropna().unique()
            )
            if len(output_partition_dates) > 1:
                logger.warning(
                    f"[Service = {service}] Multiple unique partition dates found: {output_partition_dates}. "
                    f"Parquet will partition correctly, but logging assumes single partition date."
                )
            output_partition_date = (
                output_partition_dates[0] if output_partition_dates else "<unknown>"
            )
            logger.info(
                f"[Service = {service}, Partition Date = {output_partition_date}] Exporting n={len(chunk_df)} records to {folder_path}..."
            )
            try:
                # Strongly enforce schema at write-time (avoid pyarrow inference drift).
                schema: Optional[pa.Schema] = get_service_pa_schema(
                    service=service, custom_args=custom_args
                )
                if schema:
                    dtypes_map = (
                        _get_service_dtypes_map(
                            service=service, custom_args=custom_args
                        )
                        or {}
                    )
                    if "partition_date" not in dtypes_map:
                        dtypes_map["partition_date"] = "string"
                    chunk_df = _coerce_df_to_dtypes_map(chunk_df, dtypes_map)
                    table = pa.Table.from_pandas(
                        chunk_df[schema.names],
                        schema=schema,
                        preserve_index=False,
                        safe=True,
                    )
                else:
                    logger.warning(
                        f"[Service = {service}] No PyArrow schema available; skipping schema enforcement. "
                        f"This may lead to schema drift."
                    )
                    table = pa.Table.from_pandas(chunk_df, preserve_index=False)

                pq.write_to_dataset(
                    table=table, root_path=folder_path, partition_cols=partition_cols
                )
            except pa.ArrowException as e:
                logger.error(
                    f"[Service = {service}, Partition Date = {output_partition_date}] "
                    f"PyArrow error during Parquet export: {e}"
                )
                raise
            except ValueError as e:
                logger.error(
                    f"[Service = {service}, Partition Date = {output_partition_date}] "
                    f"Data type coercion error during Parquet export: {e}"
                )
                raise
            except (OSError, IOError) as e:
                logger.error(
                    f"[Service = {service}, Partition Date = {output_partition_date}] "
                    f"File system error during Parquet export to {folder_path}: {e}"
                )
                raise
            except Exception as e:
                logger.error(
                    f"[Service = {service}, Partition Date = {output_partition_date}] "
                    f"Unexpected error exporting data to local storage: {e}"
                )
                raise
        export_path = folder_path if export_format == "parquet" else local_export_fp
        logger.info(
            f"Successfully exported {service} data ({export_path}) as {export_format}"
        )  # noqa


def get_local_prefix_for_service(
    service: str, record_type: Optional[str] = None
) -> str:
    """Get the local prefix for a given service."""
    if service == "raw_sync":
        if record_type:
            return MAP_SERVICE_TO_METADATA[service]["subpaths"][record_type]
        else:
            raise ValueError(f"Record type must be provided for service={service}.")
    else:
        return MAP_SERVICE_TO_METADATA[service]["local_prefix"]


def get_local_prefixes_for_service(service: str) -> list[str]:
    if service == "preprocessed_posts":
        local_prefixes = []
        subpaths = MAP_SERVICE_TO_METADATA[service]["subpaths"]
        for _, subpath in subpaths.items():
            local_prefixes.append(subpath)
    else:
        local_prefix = MAP_SERVICE_TO_METADATA[service]["local_prefix"]
        if service == "raw_sync":
            record_type = "post"  # we only fetch the posts from study users.
            local_prefix = MAP_SERVICE_TO_METADATA[service]["subpaths"][record_type]
        local_prefixes = [local_prefix]
    return local_prefixes


def _get_all_filenames(
    service: str,
    directories: list[Literal["cache", "active"]] = ["active"],
    validate_pq_files: bool = False,
    record_type: Optional[str] = None,
) -> list[str]:
    """Gets all filenames for a given service.

    Uses the current file format of
    - <service>
        - <directory = cache / active>
            - <partition_date = YYYY-MM-DD>
                - <filename>

    Example:
    - /projects/p32375/bluesky_research_data/ml_inference_perspective_api/
    cache/partition_date=2024-09-29/bbab32f2d9764d52a3d89a7aee014192-0.parquet
    """
    root_local_prefix = get_local_prefix_for_service(
        service=service, record_type=record_type
    )

    return crawl_local_prefix(
        local_prefix=root_local_prefix,
        directories=directories,
        validate_pq_files=validate_pq_files,
    )


def _get_all_filenames_deprecated_format(
    service: str,
    directories: list[Literal["cache", "active"]] = ["active"],
    validate_pq_files: bool = False,
    source_types: Optional[list[Literal["firehose", "most_liked"]]] = None,
) -> list[str]:
    """Gets all filenames for a given service.

    Uses the deprecated file format of
    - <service>
        - <source_type = firehose / most_liked>
             - <directory = cache / active>
                - <partition_date = YYYY-MM-DD>
                    - <filename>

    Subdirectories beneath "directory" could vary. Sometimes they have the
    parquet filename as a folder, e.g., "startTimestamp=2024-09-19-11:58:16_endTimestamp=2024-09-19-23:59:59_fileCreatedTimestamp=2024-09-26-18:42:28.parquet"

    In this case, the full filepath is:
        /projects/p32375/bluesky_research_data/ml_inference_perspective_api/
        firehose/cache/
        startTimestamp=2024-09-19-11:58:16_endTimestamp=2024-09-19-23:59:59_fileCreatedTimestamp=2024-09-26-18:42:28.parquet
        /partition_date=2024-09-19/bbb05056707144e28a0b51725ef9940c-0.parquet

    (this came because I thought you could name the .parquet files, but you can't (or it's a hassle to,
    and you don't ever need to do so)).

    Either way, this function looks for the file format listed above and
    returns all the filenames.
    """
    if service not in [
        "preprocessed_posts",
        "ml_inference_perspective_api",
        "ml_inference_sociopolitical",
        "raw_sync",
    ]:
        raise ValueError(f"Service {service} is not supported for deprecated format.")

    root_local_prefix = get_local_prefix_for_service(service)

    if not source_types:
        source_types = ["firehose", "most_liked"]
    else:
        logger.info(f"Loading {service} data for source types: {source_types}")

    local_prefixes = [
        os.path.join(root_local_prefix, source_type) for source_type in source_types
    ]

    loaded_filepaths: list[str] = []

    for local_prefix in local_prefixes:
        loaded_filepaths.extend(
            crawl_local_prefix(
                local_prefix=local_prefix,
                directories=directories,
                validate_pq_files=validate_pq_files,
            )
        )

    return loaded_filepaths


def _validate_filepaths(
    service: str,
    filepaths: list[str],
    partition_date: Optional[str] = None,
    start_partition_date: Optional[str] = None,
    end_partition_date: Optional[str] = None,
) -> list[str]:
    """Validate filepaths."""
    filtered_filepaths: list[str] = []

    if not start_partition_date and not end_partition_date and not partition_date:
        logger.info("No date filters provided, returning all filepaths.")
        return filepaths

    if (start_partition_date and not end_partition_date) or (
        end_partition_date and not start_partition_date
    ):
        raise ValueError(
            "Both start_partition_date and end_partition_date must be provided together."
        )

    # use specific partition date only if the start/end dates aren't provided
    # (they shouldn't be ever jointly provided anyways, since start/end date
    # ranges should only be during backfill operations).
    if partition_date and (start_partition_date or end_partition_date):
        raise ValueError(
            "Cannot use partition_date and start_partition_date or end_partition_date together."
        )

    # partition date is given or start/end dates are provided.
    if partition_date or (start_partition_date and end_partition_date):
        print(
            f"Filtering {len(filepaths)} files in service={service}, "
            f"for {'partition_date=' + partition_date if partition_date else f'date range {start_partition_date} to {end_partition_date}'}"
        )
        for fp in filepaths:
            path_parts = fp.split("/")
            partition_parts = [p for p in path_parts if "partition_date=" in p]
            if not partition_parts:
                continue

            file_partition_date = partition_parts[0].split("=")[1]

            if partition_date:
                if file_partition_date == partition_date:
                    filtered_filepaths.append(fp)
            elif start_partition_date <= file_partition_date <= end_partition_date:
                filtered_filepaths.append(fp)
    return filtered_filepaths


def list_filenames(
    service: str,
    directories: list[Literal["cache", "active"]] = ["active"],
    validate_pq_files: bool = False,
    partition_date: str | None = None,
    start_partition_date: str | None = None,
    end_partition_date: str | None = None,
    override_local_prefix: str | None = None,
    source_types: list[Literal["firehose", "most_liked"]] | None = None,
    custom_args: dict | None = None,
) -> list[str]:
    """List files in local storage for a given service."""

    loaded_filepaths: list[str] = []

    if service == "raw_sync":
        record_type = custom_args["record_type"]
        logger.info(
            f"Getting study user activity data for record_type={record_type}..."
        )
        loaded_filepaths.extend(
            _get_all_filenames(
                service=service,
                directories=directories,
                validate_pq_files=validate_pq_files,
                record_type=record_type,
            )
        )

    elif service in [
        "preprocessed_posts",
        "ml_inference_perspective_api",
        "ml_inference_sociopolitical",
    ]:
        logger.info(
            f"Getting all filenames for service={service} in both current and deprecated formats."
        )
        loaded_filepaths.extend(
            _get_all_filenames_deprecated_format(
                service=service,
                directories=directories,
                validate_pq_files=validate_pq_files,
                source_types=source_types,
            )
        )
        loaded_filepaths.extend(
            _get_all_filenames(
                service=service,
                directories=directories,
                validate_pq_files=validate_pq_files,
            )
        )
    else:
        logger.info(f"Getting all filenames for service={service} in current format.")
        loaded_filepaths.extend(
            _get_all_filenames(
                service=service,
                directories=directories,
                validate_pq_files=validate_pq_files,
            )
        )

    loaded_filepaths = _validate_filepaths(
        service=service,
        filepaths=loaded_filepaths,
        partition_date=partition_date,
        start_partition_date=start_partition_date,
        end_partition_date=end_partition_date,
    )

    if override_local_prefix:
        loaded_filepaths = [
            os.path.join(override_local_prefix, fp) for fp in loaded_filepaths
        ]

    return loaded_filepaths


def delete_files(filepaths: list[str]) -> None:
    """Delete files from local storage."""
    logger.info(f"Deleting {len(filepaths)} files from local storage...\n\t{filepaths}")
    for filepath in filepaths:
        os.remove(filepath)
    logger.info(f"Successfully deleted {len(filepaths)} files from local storage.")


def load_service_cols(service: str) -> list[str]:
    """Load the columns for a given service."""
    return []


def _get_service_dtypes_map(
    service: str, custom_args: Optional[dict] = None
) -> Optional[dict[str, str]]:
    """Return a copy of the configured dtypes map for a service.

    For `raw_sync`, the dtypes map is keyed by `record_type` and must be
    provided via `custom_args`.
    """
    dtypes_map = MAP_SERVICE_TO_METADATA[service].get("dtypes_map", None)
    if custom_args:
        record_type = custom_args.get("record_type", None)
        if record_type:
            dtypes_map = (
                MAP_SERVICE_TO_METADATA[service]
                .get("dtypes_map", {})
                .get(record_type, {})
            )
    if not dtypes_map:
        return None
    # Copy to avoid mutating MAP_SERVICE_TO_METADATA.
    return dict(dtypes_map)


def _coerce_df_to_dtypes_map(
    df: pd.DataFrame, dtypes_map: dict[str, str]
) -> pd.DataFrame:
    """Coerce a DataFrame to the configured dtypes map (strict).

    - Missing columns are created as all-null nullable columns.
    - Extra columns are left as-is (they may be dropped later for schema writes).

    Note: The dtypes_map uses "bool" as the key (for consistency with service_constants.py),
    but pandas nullable boolean dtype is "boolean" (not "bool"). This function automatically
    converts "bool" → "boolean" when creating or coercing columns.
    """
    for col, dtype in dtypes_map.items():
        if col not in df.columns:
            if dtype == "Int64":
                df[col] = pd.Series([pd.NA] * len(df), dtype="Int64")
            elif dtype == "Float64":
                df[col] = pd.Series([pd.NA] * len(df), dtype="Float64")
            elif dtype == "bool":
                # Note: dtypes_map uses "bool" key, but pandas nullable boolean is "boolean"
                df[col] = pd.Series([pd.NA] * len(df), dtype="boolean")
            elif dtype in {"string", "object"}:
                df[col] = pd.Series([pd.NA] * len(df), dtype="string")
            elif dtype == "datetime64[ns]":
                df[col] = pd.Series([pd.NaT] * len(df), dtype="datetime64[ns]")
            else:
                df[col] = pd.Series([pd.NA] * len(df), dtype="string")
            continue

        try:
            if dtype == "Int64":
                df[col] = pd.to_numeric(df[col], errors="raise").astype("Int64")
            elif dtype == "Float64":
                df[col] = pd.to_numeric(df[col], errors="raise").astype("Float64")
            elif dtype == "bool":
                # Note: dtypes_map uses "bool" key, but pandas nullable boolean is "boolean"
                df[col] = df[col].astype("boolean")
            elif dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="raise")
            elif dtype in {"string", "object"}:
                df[col] = df[col].astype("string")
            else:
                df[col] = df[col].astype("string")
        except Exception as e:
            raise ValueError(
                f"Failed to coerce column={col} to dtype={dtype}: {e}"
            ) from e

    return df


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


def get_service_pa_schema(
    service: str, custom_args: Optional[dict] = None
) -> Optional[pa.Schema]:
    dtypes_map = _get_service_dtypes_map(service=service, custom_args=custom_args)
    # we add this here since when we transform the initial loaded dicts to
    # df (prior to writes), we don't have partition_date yet. However, we
    # want this to exist on read.
    if dtypes_map and "partition_date" not in dtypes_map:
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
    export_format: Literal["jsonl", "parquet", "duckdb"] = "parquet",
    partition_date: str | None = None,
    start_partition_date: str | None = None,
    end_partition_date: str | None = None,
    duckdb_query: str | None = None,
    query_metadata: dict | None = None,
    latest_timestamp: str | None = None,
    use_all_data: bool = False,
    validate_pq_files: bool = False,
    override_local_prefix: str | None = None,
    source_types: list[Literal["firehose", "most_liked"]] | None = None,
    custom_args: dict | None = None,
) -> pd.DataFrame:
    """Load data from local storage.

    Args:
        service: Name of the service to load data from
        directory: Which directory to load from ("cache" or "active")
        export_format: Format of the data files ("jsonl", "parquet", or "duckdb")
        partition_date: Specific partition date to load
        start_partition_date: Start of partition date range to load
        end_partition_date: End of partition date range to load
        duckdb_query: SQL query for DuckDB format
        query_metadata: Metadata for DuckDB query
        latest_timestamp: Only load data after this timestamp
        use_all_data: Whether to load from both cache and active directories
        validate_pq_files: Whether to validate parquet files
        override_local_prefix: Optional override for the service's local prefix path
        source_types: Optional list of source types to load.
            Used for deprecated ["firehose", "most_liked"] format.
        custom_args: Optional custom arguments for the service
    """
    service = _convert_service_name_to_db_name(service)
    directories = [directory]
    if use_all_data:
        directories = ["cache", "active"]

    filepaths = list_filenames(
        service=service,
        directories=directories,
        validate_pq_files=validate_pq_files,
        partition_date=partition_date,
        start_partition_date=start_partition_date,
        end_partition_date=end_partition_date,
        override_local_prefix=override_local_prefix,
        source_types=source_types,
        custom_args=custom_args,
    )
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
        schema: Optional[pa.Schema] = get_service_pa_schema(
            service=service, custom_args=custom_args
        )
        if columns:
            kwargs["columns"] = columns
        if schema:
            kwargs["schema"] = schema
        if filters:
            kwargs["filters"] = filters
        df = pd.read_parquet(**kwargs)
        if schema:
            # attempt to convert dtypes after the fact, but only for columns that exist
            if service == "raw_sync":
                record_type = custom_args["record_type"]
                dtypes_map = MAP_SERVICE_TO_METADATA[service]["dtypes_map"][record_type]
            else:
                dtypes_map = MAP_SERVICE_TO_METADATA[service].get("dtypes_map", {})
            for col, dtype in dtypes_map.items():
                if col in df.columns:  # Only convert if column exists
                    try:
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
                    except Exception as e:
                        logger.warning(
                            f"Failed to convert column {col} to type {dtype}: {e}"
                        )
    elif export_format == "duckdb":
        if not duckdb_query or not query_metadata:
            raise ValueError(
                "Must provide a DuckDB query and query metadata when exporting to DuckDB."
            )

        duckdb_query_cols = []
        for table in query_metadata["tables"]:
            duckdb_query_cols.extend(table["columns"])

        df: pd.DataFrame = duckDB.run_query_as_df(
            query=duckdb_query,
            mode="parquet",
            filepaths=filepaths,
            query_metadata=query_metadata,
        )

    if latest_timestamp:
        logger.info(f"Fetching data after timestamp={latest_timestamp}")
        timestamp_field = MAP_SERVICE_TO_METADATA[service]["timestamp_field"]
        df = df[df[timestamp_field] >= latest_timestamp]
    # drop extra columns that are added in during compaction steps.
    # (these will be added back in during compaction anyways)
    cols_to_drop = ["row_num", "partition_date", "startTimestamp"]
    for col in cols_to_drop:
        # don't drop the column if we explicitly query for it.
        if export_format == "duckdb":
            if col in df.columns and col not in duckdb_query_cols:
                df = df.drop(columns=[col])
        else:
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
        groups = [group.head(max_per_source) for _, group in grouped]
        if groups:
            df = pd.concat(groups)
        else:
            return pd.DataFrame()
    return df


def delete_records_for_service(
    service: str,
    directory: Literal["cache", "active"] = "active",
    partition_date: Optional[str] = None,
    start_partition_date: Optional[str] = None,
    end_partition_date: Optional[str] = None,
    validate_pq_files: bool = False,
    override_local_prefix: Optional[str] = None,
    source_types: Optional[list[Literal["firehose", "most_liked"]]] = None,
    custom_args: Optional[dict] = None,
) -> None:
    """Deletes records for a partition service."""
    service = _convert_service_name_to_db_name(service)
    filepaths = list_filenames(
        service=service,
        directories=[directory],
        validate_pq_files=validate_pq_files,
        partition_date=partition_date,
        start_partition_date=start_partition_date,
        end_partition_date=end_partition_date,
        override_local_prefix=override_local_prefix,
        source_types=source_types,
        custom_args=custom_args,
    )
    total_fps_to_delete = len(filepaths)
    if total_fps_to_delete == 0:
        logger.info(
            f"No files to delete for service={service}, partition_date={partition_date}, start_partition_date={start_partition_date}, end_partition_date={end_partition_date}. Exiting."
        )
        return
    delete_records = input(
        f"Are you sure you want to delete {total_fps_to_delete} files for service={service}, partition_date={partition_date}, start_partition_date={start_partition_date}, end_partition_date={end_partition_date}? (y/n)"
    )
    if delete_records != "y":
        logger.info("Aborting deletion.")
        return
    delete_files(filepaths)
