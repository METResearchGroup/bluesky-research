"""Compacts services. Combines all files into a single file or a smaller
set of files.
"""

import os
from typing import Optional

from lib.aws.athena import Athena
from lib.aws.dynamodb import DynamoDB
from lib.aws.s3 import ROOT_BUCKET, S3
from lib.log.logger import get_logger
from lib.helper import generate_current_datetime_str, track_performance
from services.compact_all_services.constants import MAP_SERVICE_TO_METADATA

athena = Athena()
s3 = S3()
dynamodb = DynamoDB()
logger = get_logger(__name__)

# I can use the existing compaction sessions DynamoDB table, since it's still
# technically a compaction session.
dynamodb_table_name = "compaction_sessions"


def generate_service_sql_query(service: str, timestamp: Optional[str] = None) -> str:
    """Creates a SQL query to get the rows for a particular service.

    For some tables, we want to both compact files and dedupe them. For others,
    like the user session logs, each row is already unique, so we just want to
    compact the files.
    """
    # NOTE: I use brackets instead of "get" since the impact of a silent "get"
    # is quite high and could result in lost data.
    skip_deduping = MAP_SERVICE_TO_METADATA[service]["skip_deduping"]
    glue_table_name = MAP_SERVICE_TO_METADATA[service]["glue_table_name"]
    if skip_deduping:
        query = f"SELECT * FROM {glue_table_name}"
    else:
        primary_key = MAP_SERVICE_TO_METADATA[service]["primary_key"]
        timestamp_field = MAP_SERVICE_TO_METADATA[service]["timestamp_field"]
        timestamp_filter = (
            f"AND {timestamp_field} >= '{timestamp}'" if timestamp else ""
        )
        query = f"""
        SELECT *
        FROM (
            SELECT *,
            ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY {timestamp_field} DESC) AS row_num
            FROM {glue_table_name}
        ) ranked
        WHERE row_num = 1
        {timestamp_filter}
        """
    return query


# TODO: write compacted data to a "compacted" filepath and then
# exclude those "compacted" files from the compaction by default
# (and add an option to include them, for when I want to compact all files),
# including all previously compacted files.
def get_service_compaction_session(service: str) -> dict:
    return {}


def insert_service_compaction_session(compaction_session: dict) -> None:
    try:
        dynamodb.insert_item_into_table(
            item=compaction_session, table_name=dynamodb_table_name
        )
        logger.info(
            f"Successfully inserted compaction session: {compaction_session}"  # noqa
        )  # noqa
    except Exception as e:
        logger.error(f"Failed to insert compaction session: {e}")  # noqa
        raise


# NOTE: in the future, if the data is too large, I can write multiple files
# instead of 1. I can just split it and then for each batch, I can do
# something like f"{filename}_{i}.jsonl" where i is the batch number.
def export_compacted_data(service: str, data: list[dict]) -> None:
    """Exports compacted data to S3.

    The compacted data is exported to a "compacted" subdirectory of the
    service's S3 prefix.
    """
    s3_prefix = MAP_SERVICE_TO_METADATA[service]["s3_prefix"]
    timestamp = generate_current_datetime_str()
    filename = f"{timestamp}.jsonl"
    full_s3_prefix = os.path.join(s3_prefix, "compacted", filename)
    s3.write_dicts_jsonl_to_s3(data=data, key=full_s3_prefix)
    logger.info(
        f"Successfully inserted {len(data)} compacted posts for service '{service}' at {full_s3_prefix}"
    )  # noqa


def get_existing_keys(service: str, fetch_all_keys: bool = False) -> list[str]:
    """Gets existing keys for a service.

    If fetch_all_keys is True, then it fetches all keys. Otherwise, it only
    fetches keys that have not been compacted yet. Compacted keys will have a
    "compacted" in the key.
    """
    existing_keys: list[str] = s3.list_keys_given_prefix(
        MAP_SERVICE_TO_METADATA[service]["s3_prefix"]
    )
    if not fetch_all_keys:
        existing_keys = [key for key in existing_keys if "compact" not in key]
    return existing_keys


def delete_keys(keys: list[str]):
    total_keys = len(keys)
    for i, key in enumerate(keys):
        s3.client.delete_object(Bucket=ROOT_BUCKET, Key=key)
        if i % 10 == 0:
            logger.info(f"Deleted {i} keys out of {total_keys}")
    logger.info(f"Deleted {len(keys)} keys")


@track_performance
def compact_single_service(service: str) -> None:
    """Compacts a single service.

    Fetches the S3 keys for that service, then queries Athena for the data for
    that service. Fetches the S3 keys first and then does SELECT * in order to
    avoid accidentally losing data (i.e., if I do SELECT * and then list the
    keys, then there may be new data written to S3 after the SELECT *
    query completes that won't be captured in that result but will be captured
    when I delete the keys, meaning that I lost data).
    """
    # TODO: make this flexible so that I can either compact all data after a certain
    # timestamp or compact all the data after all time.
    latest_service_compaction_session: dict = get_service_compaction_session(service)
    timestamp = latest_service_compaction_session.get("compaction_timestamp", None)
    if timestamp:
        print(f"Compacting data from {service} after {timestamp}")
    query = generate_service_sql_query(service, timestamp)
    existing_keys: list[str] = get_existing_keys(service)
    if existing_keys:
        logger.info(f"Compacting {len(existing_keys)} files for {service}")
    else:
        logger.info(
            f"No files to compact for {service}. Only files, if any, are already compacted"
        )  # noqa
        return
    dtypes_map = MAP_SERVICE_TO_METADATA[service].get("dtypes_map", None)
    df = athena.query_results_as_df(query, dtypes_map=dtypes_map)
    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    logger.info(f"Successfully compacted data for {service}")
    export_compacted_data(service=service, data=df_dicts)
    delete_keys(existing_keys)
    compaction_session = {
        "compaction_timestamp": generate_current_datetime_str(),
        "service": service,
        "num_rows_after_compaction": len(df_dicts),
        "num_files_compacted": len(existing_keys),
        "num_files_deleted": len(existing_keys),
    }
    insert_service_compaction_session(compaction_session=compaction_session)


def do_compact_services() -> None:
    services = [
        "user_session_logs",
        "feed_analytics",
        "post_scores",
        "consolidated_enriched_post_records",
        "ml_inference_perspective_api",
        "ml_inference_sociopolitical",
        # "in_network_user_activity",
        "scraped_user_social_network",
    ]
    for service in services:
        compact_single_service(service)


if __name__ == "__main__":
    do_compact_services()
