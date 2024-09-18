"""Compacts services. Combines all files into a single file or a smaller
set of files.
"""

import os

from lib.aws.athena import Athena
from lib.aws.s3 import ROOT_BUCKET, S3
from lib.log.logger import get_logger
from lib.helper import generate_current_datetime_str, track_performance
from services.compact_all_services.constants import (
    MAP_SERVICE_TO_S3_PREFIX,
    MAP_SERVICE_TO_SQL_QUERY,
)

athena = Athena()
s3 = S3()
logger = get_logger(__name__)


# TODO: write compacted data to a "compacted" filepath and then
# exclude those "compacted" files from the compaction by default
# (and add an option to include them, for when I want to compact all files),
# including all previously compacted files.
def get_service_compaction_session(service: str) -> dict:
    pass


def insert_service_compaction_session(service: str, compaction_session: dict) -> None:
    pass


def export_compacted_data(service: str, data: list[dict]) -> None:
    """Exports compacted data to S3.

    The compacted data is exported to a "compacted" subdirectory of the
    service's S3 prefix.
    """
    s3_prefix = ""  # TODO: get from constants
    full_s3_prefix = os.path.join(s3_prefix, "compacted")
    s3.write_dicts_jsonl_to_s3(data=data, s3_prefix=full_s3_prefix)
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
        MAP_SERVICE_TO_S3_PREFIX[service]
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
    timestamp = latest_service_compaction_session["compaction_timestamp"]
    print(f"Compacting data from {service} after {timestamp}")
    query = MAP_SERVICE_TO_SQL_QUERY[service]
    existing_keys: list[str] = get_existing_keys(service)
    df = athena.query_results_as_df(query)
    df_dicts = df.to_dict(orient="records")
    df_dicts = athena.parse_converted_pandas_dicts(df_dicts)
    logger.info(f"Successfully compacted data for {service}")
    # TODO: add more fields for compaction session, like # of posts before and
    # after, # of files deleted, etc.
    # TODO: should I also do deduping here as well? Maybe? In that case, I
    # need the PK of each table (probably URI tbh). Maybe instead of doing a
    # bunch of map constants, I can do a single service map where each key
    # is the service name and each value is a dict with the service name,
    # the query, the S3 prefix, and the PK
    export_compacted_data(service=service, data=df_dicts)
    delete_keys(existing_keys)
    compaction_session = {
        "compaction_timestamp": generate_current_datetime_str(),
    }
    insert_service_compaction_session(
        service=service, compaction_session=compaction_session
    )


def compact_all_services() -> None:
    pass


def do_compact_services() -> None:
    pass
