"""Backfilling the sync records for users."""

from datetime import datetime
import json
import gc
import os
from typing import Optional
import requests
import multiprocessing
import concurrent.futures
import traceback

from atproto import CAR

from lib.constants import convert_bsky_dt_to_pipeline_dt, timestamp_format
from lib.db.bluesky_models.transformations import (
    TransformedBlock,
    TransformedFollow,
    TransformedLike,
    TransformedPost,
    TransformedRepost,
    TransformedReply,
)
from lib.helper import (
    create_batches,
    track_performance,
    generate_current_datetime_str,
)
from lib.log.logger import get_logger
from services.backfill.sync.constants import (
    default_start_timestamp,
    default_end_timestamp,
    endpoint,
    default_batch_size,
    valid_generic_bluesky_types,
)
from services.backfill.sync.export_data import write_records_to_cache
from services.backfill.sync.models import UserBackfillMetadata

logger = get_logger(__name__)


def get_plc_directory_doc(did: str) -> dict:
    """Get the PLC directory document for a DID.

    Args:
        did: The DID to look up

    Returns:
        The PLC directory document as a dictionary

    See the following links for more information as well as examples:
    - https://web.plc.directory/api/redoc
    - https://web.plc.directory/did/did:plc:w5mjarupsl6ihdrzwgnzdh4y
    - https://internect.info/did/did:plc:w5mjarupsl6ihdrzwgnzdh4y

    Output should look like the following:
    {
        '@context': [
            'https://www.w3.org/ns/did/v1',
            'https://w3id.org/security/multikey/v1',
            'https://w3id.org/security/suites/secp256k1-2019/v1'
        ],
        'alsoKnownAs': ['at://markptorres.bsky.social'],
        'id': 'did:plc:w5mjarupsl6ihdrzwgnzdh4y',
        'service': [
            {
                'id': '#atproto_pds',
                'serviceEndpoint': 'https://puffball.us-east.host.bsky.network',
                'type': 'AtprotoPersonalDataServer'
            }
        ],
        'verificationMethod': [
            {
                'controller': 'did:plc:w5mjarupsl6ihdrzwgnzdh4y',
                'id': 'did:plc:w5mjarupsl6ihdrzwgnzdh4y#atproto',
                'publicKeyMultibase': 'zQ3shrqW7PgHYsfsXrhz4i5eXEUAgWdkpZrqK2gsB1ZBSd9NY',
                'type': 'Multikey'
            }
        ]
    }
    """
    plc_url = f"https://plc.directory/{did}"
    response = requests.get(plc_url)
    return response.json()


def identify_post_type(post: dict):
    """Identifies a post as written by a user.

    By "written by a user", we mean that a post is a standalone post by the user
    and not part of a thread (we count those as replies). Both standalone and
    threaded posts are obviously written by the user.
    """
    post_type = "reply" if "reply" in post else "post"
    return post_type


def identify_record_type(record: dict):
    """Identifies the type of a record.

    The record type is the last part of the record's $type field.
    """
    record_type = record["$type"].split(".")[-1]
    if record_type == "post":
        record_type = identify_post_type(record)
    return record_type


def validate_record_timestamp(
    record: dict,
    start_timestamp: str = default_start_timestamp,
    end_timestamp: str = default_end_timestamp,
) -> bool:
    """Get only the records within the range of the study."""
    record_timestamp = record["createdAt"]
    record_timestamp_pipeline_dt = convert_bsky_dt_to_pipeline_dt(record_timestamp)
    if (
        record_timestamp_pipeline_dt < start_timestamp
        or record_timestamp_pipeline_dt > end_timestamp
    ):
        return False
    return True


def validate_is_valid_generic_bluesky_type(record: dict) -> bool:
    """Checks to see if the record is one of the expected generic Bluesky
    types. If not (e.g., if it's a custom record from another PDS), then
    we'll skip the record.

    Args:
        record: The record to validate

    Returns:
        True if the record is a valid generic Bluesky type, False otherwise

    NOTE: we only check the type of the record itself, we don't check the
    typing of subrecords (e.g., the type of the 'reply' field in a reply
    record), since I've seen that there's sometimes inconsistencies here
    because of Bluesky schema evolution over time.
    """
    return record["$type"] in valid_generic_bluesky_types


def transform_backfilled_record(
    record: dict,
    record_type: str,
    start_timestamp: str,
    end_timestamp: str,
) -> dict:
    """Transform a backfilled record.

    Args:
        record: The record to transform
        record_type: The type of the record

    Returns:
        The transformed record
    """
    record["record_type"] = record_type
    record["synctimestamp"] = convert_bsky_dt_to_pipeline_dt(record["createdAt"])  # noqa
    # for old records, use a different synctimestamp that'll allow
    # use to better partition the data.

    # validate the synctimestamp and if it's not in the range,
    # then set to a default timestamp.
    record_falls_in_study_range: bool = validate_record_timestamp(
        record=record,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    if not record_falls_in_study_range:
        record["synctimestamp"] = assign_default_backfill_synctimestamp(
            synctimestamp=record["synctimestamp"]
        )

    # transform the formats of the fields to be consistent with each other
    # for a given type (for optional fields, these show up only if the
    # record has the field, but I want to enforce consistent schemas).
    try:
        if record_type in ["post", "reply"]:
            embed = record.get("embed", False)
            record["embed"] = json.dumps(embed) if embed else None

            entities = record.get("entities", False)
            record["entities"] = json.dumps(entities) if entities else None

            facets = record.get("facets", False)
            record["facets"] = json.dumps(facets) if facets else None

            langs = record.get("langs", False)
            record["langs"] = ",".join(langs) if langs else None

            tags = record.get("tags", False)
            record["tags"] = ",".join(tags) if tags else None

            labels = record.get("labels", False)
            record["labels"] = json.dumps(labels) if labels else None

            if record_type == "post":
                transformed_record = TransformedPost(**record)
            elif record_type == "reply":
                transformed_record = TransformedReply(**record)
                transformed_record = transformed_record.model_dump()
                transformed_record["reply"] = json.dumps(transformed_record["reply"])
        elif record_type == "repost":
            transformed_record = TransformedRepost(**record)
            transformed_record = transformed_record.model_dump()
            transformed_record["subject"] = json.dumps(transformed_record["subject"])
        elif record_type == "like":
            transformed_record = TransformedLike(**record)
            transformed_record = transformed_record.model_dump()
            transformed_record["subject"] = json.dumps(transformed_record["subject"])
        elif record_type == "follow":
            transformed_record = TransformedFollow(**record)
        elif record_type == "block":
            transformed_record = TransformedBlock(**record)
        if not isinstance(transformed_record, dict):
            return transformed_record.model_dump()
        return transformed_record
    except Exception as e:
        logger.error(f"Error transforming record: {e}")
        logger.error(traceback.format_exc())
        logger.info(f"Record: {record}")
        return record


def send_request_to_pds(did: str, pds_endpoint: str) -> requests.Response:
    """Send a request to the PDS endpoint.

    Args:
        did: The DID of the user
        pds_endpoint: The PDS endpoint to send the request to

    Returns:
        The response object from the request
    """
    root_url = os.path.join(pds_endpoint, "xrpc")
    joined_url = os.path.join(root_url, endpoint)
    full_url = f"{joined_url}?did={did}"
    return requests.get(full_url)


def get_bsky_records_for_user(did: str) -> list[dict]:
    """Get the records for a user.

    Args:
        did: The DID of the user

    Returns:
        The records for the user
    """
    plc_doc = get_plc_directory_doc(did)
    pds_endpoint = plc_doc["service"][0][
        "serviceEndpoint"
    ]  # TODO: verify if this will always work.
    res = send_request_to_pds(did=did, pds_endpoint=pds_endpoint)
    if res.status_code != 200:
        logger.error(f"Error getting CAR file for user {did}: {res.status_code}")
        logger.info(f"res.headers: {res.headers}")
        logger.info(f"res.content: {res.content}")
        logger.info("Returning no records for user.")
        records = []
    else:
        try:
            car_file = CAR.from_bytes(res.content)
            records: list[dict] = [obj for obj in car_file.blocks.values()]
        except Exception as e:
            logger.error(f"Error parsing CAR file for user {did}: {e}")
            logger.info("Returning no records for user.")
            records = []
    return records


def do_backfill_for_user(
    did: str,
    start_timestamp: str = default_start_timestamp,
    end_timestamp: str = default_end_timestamp,
) -> tuple[dict[str, int], dict[str, list[dict]], UserBackfillMetadata]:
    """
    Do backfill for a user.

    Params defined in https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/com/atproto/sync/get_repo.py#L16
    - did: The DID of the repo.
    - since: The revision ('rev') of the repo to create a diff from.

    Args:
        did: The DID of the user to backfill
        start_timestamp: The start timestamp to backfill from
        end_timestamp: The end timestamp to backfill to

    Returns:
        A tuple containing:
        - The count map containing the count of records for each type
        - The record map containing the records for each type
        - Metadata about the user's backfill operation
    """
    type_to_record_map: dict[str, list[dict]] = {}
    type_to_count_map = {}
    total_skipped_records = 0

    # Get PLC document to extract handle and PDS endpoint
    plc_doc = get_plc_directory_doc(did)
    pds_endpoint = plc_doc["service"][0]["serviceEndpoint"]

    # Extract handle from alsoKnownAs
    bluesky_handle = ""
    if "alsoKnownAs" in plc_doc:
        for aka in plc_doc["alsoKnownAs"]:
            if aka.startswith("at://"):
                bluesky_handle = aka.replace("at://", "")
                break

    records: list[dict] = get_bsky_records_for_user(did)
    for record in records:
        if "$type" in record:
            # validate the record type (checking if it's a valid generic Bluesky
            # lexicon record (as opposed to a custom record type)
            # example invalid record: {'$type': 'sh.tangled.graph.follow', 'subject': 'did:plc:iwhuynr6mm6xxuh25o4do2tx', 'createdAt': '2025-03-21T07:40:58Z', 'record_type': 'follow', 'synctimestamp': '2025-04-01-00:00:00'}
            is_valid_generic_bluesky_type = validate_is_valid_generic_bluesky_type(
                record=record
            )
            if not is_valid_generic_bluesky_type:
                logger.info(
                    f"Skipping record type {record['$type']} for user {did} (not a generic Bluesky type): {record}"
                )
                total_skipped_records += 1
                continue

            # validate the record type (assuming that it's a valid Bluesky lexicon record)
            record_type = identify_record_type(record)

            type_to_count_map[record_type] = type_to_count_map.get(record_type, 0) + 1

            transformed_record: dict = transform_backfilled_record(
                record=record,
                record_type=record_type,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
            if record_type in type_to_record_map:
                type_to_record_map[record_type].append(transformed_record)
            else:
                type_to_record_map[record_type] = [transformed_record]

    logger.info(
        f"For user with did={did}, found the following record types and counts: {type_to_count_map}"
    )
    logger.info(
        f"Total records:\t Original: {len(records)}\t Skipped: {total_skipped_records}\t Exported: {len(records) - total_skipped_records}"
    )

    # Create metadata for the user's backfill operation
    user_metadata = create_user_backfill_metadata(
        did=did,
        record_count_map=type_to_count_map,
        bluesky_handle=bluesky_handle,
        pds_service_endpoint=pds_endpoint,
    )

    return type_to_count_map, type_to_record_map, user_metadata


def assign_default_backfill_synctimestamp(synctimestamp: str) -> str:
    """Assign a default synctimestamp to a record.

    Args:
        synctimestamp: The synctimestamp to assign

    Returns:
        The assigned synctimestamp

    We'll assign the record the synctimestamp corresponding to the
    1st or 15th of a given month, whichever is the earliest one that
    comes after a synctimestamp.

    For example, if a record is created on April 2nd, we assign it to April 15th,
    and if it's created April 16th, we assign it to May 1st.

    We also change the time to be 00:00:00.
    """
    synctimestamp_dt = datetime.strptime(synctimestamp, timestamp_format)
    new_hour = 0
    new_minute = 0
    new_second = 0
    try:
        if synctimestamp_dt.day <= 15:
            new_ts = synctimestamp_dt.replace(
                day=15,
                hour=new_hour,
                minute=new_minute,
                second=new_second,
            ).strftime(timestamp_format)
        else:
            current_month = synctimestamp_dt.month
            new_month = current_month + 1
            if new_month > 12:
                new_month = 1
                new_year = synctimestamp_dt.year + 1
            else:
                new_year = synctimestamp_dt.year
            new_ts = synctimestamp_dt.replace(
                year=new_year,
                month=new_month,
                day=1,
                hour=new_hour,
                minute=new_minute,
                second=new_second,
            ).strftime(timestamp_format)
        return new_ts
    except Exception as e:
        logger.error(f"Error assigning default synctimestamp: {e}")
        return synctimestamp


def do_backfill_for_users(
    dids: list[str],
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
) -> tuple[
    dict[str, dict[str, dict]], dict[str, list[dict]], list[UserBackfillMetadata]
]:
    """Performs the backfill for users sequentially.

    Args:
        dids: The list of DIDs to backfill.
        start_timestamp: The start timestamp to backfill from.
        end_timestamp: The end timestamp to backfill to.

    Returns:
        A tuple of three items:
        1. Dictionary mapping DIDs to counts of records by type
        2. Dictionary mapping record types to lists of records
        3. List of UserBackfillMetadata objects for each user
    """
    if not start_timestamp:
        start_timestamp = default_start_timestamp
    if not end_timestamp:
        end_timestamp = default_end_timestamp

    did_to_backfill_counts_map = {}
    type_to_record_full_map = {}
    user_backfill_metadata_list = []

    for did in dids:
        type_to_count_map, type_to_record_map, user_metadata = do_backfill_for_user(
            did, start_timestamp=start_timestamp, end_timestamp=end_timestamp
        )
        did_to_backfill_counts_map[did] = type_to_count_map
        user_backfill_metadata_list.append(user_metadata)

        for record_type, records in type_to_record_map.items():
            if record_type in type_to_record_full_map:
                type_to_record_full_map[record_type].extend(records)
            else:
                type_to_record_full_map[record_type] = records

    return (
        did_to_backfill_counts_map,
        type_to_record_full_map,
        user_backfill_metadata_list,
    )


def do_backfill_for_users_parallel(
    dids: list[str],
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
) -> tuple[
    dict[str, dict[str, dict]], dict[str, list[dict]], list[UserBackfillMetadata]
]:
    """Performs the backfill for users in parallel using multiprocessing.

    This is optimized for compute-bound operations, using ProcessPoolExecutor to
    parallelize across CPU cores.

    Args:
        dids: The list of DIDs to backfill.
        start_timestamp: The start timestamp to backfill from.
        end_timestamp: The end timestamp to backfill to.

    Returns:
        A tuple of three items:
        1. Dictionary mapping DIDs to counts of records by type
        2. Dictionary mapping record types to lists of records
        3. List of UserBackfillMetadata objects for each user
    """
    if not start_timestamp:
        start_timestamp = default_start_timestamp
    if not end_timestamp:
        end_timestamp = default_end_timestamp

    cpu_count = multiprocessing.cpu_count()
    max_workers = min(cpu_count, len(dids))  # Don't use more processes than DIDs

    did_to_backfill_counts_map = {}
    type_to_record_full_map = {}
    user_backfill_metadata_list = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Create futures with enumerated DIDs for progress tracking
        futures = [
            executor.submit(
                do_backfill_for_user,
                did=did,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
            for did in dids
        ]

        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                type_to_count_map, type_to_record_map, user_metadata = future.result()

                # Update counts map
                did = dids[i]  # Get the DID that corresponds to this future
                did_to_backfill_counts_map[did] = type_to_count_map
                user_backfill_metadata_list.append(user_metadata)

                # Update records map
                for record_type, records in type_to_record_map.items():
                    if record_type in type_to_record_full_map:
                        type_to_record_full_map[record_type].extend(records)
                    else:
                        type_to_record_full_map[record_type] = records

            except Exception as e:
                logger.error(f"Error in parallel backfill for DID {dids[i]}: {e}")
                logger.error(traceback.format_exc())
                continue

    return (
        did_to_backfill_counts_map,
        type_to_record_full_map,
        user_backfill_metadata_list,
    )


@track_performance
def run_batched_backfill(
    dids: list[str],
    batch_size: int = default_batch_size,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    run_parallel: bool = True,
) -> dict:
    """Runs the backfill for a list of DIDs in batches.

    Args:
        dids: The list of DIDs to backfill.
        batch_size: The number of DIDs to backfill in each batch.
        start_timestamp: The start timestamp to backfill from.
        end_timestamp: The end timestamp to backfill to.
        run_parallel: Whether to use parallel execution (default: True)

    Returns:
        A dictionary containing information about the backfill run, including
        counts, metadata, and status.
    """
    batches: list[list[str]] = create_batches(batch_list=dids, batch_size=batch_size)
    total_batches = len(batches)
    did_to_backfill_counts_full_map = {}
    all_user_backfill_metadata = []

    backfill_func = (
        do_backfill_for_users_parallel if run_parallel else do_backfill_for_users
    )

    for i, batch in enumerate(batches):
        logger.info(f"Processing batch {i}/{total_batches}")
        did_to_backfill_counts_map, type_to_record_maps, user_metadata_list = (
            backfill_func(
                dids=batch,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        )
        logger.info(
            f"Updating metadata with counts for this batch of {len(batch)} users. Now exporting to cache...."
        )
        did_to_backfill_counts_full_map.update(did_to_backfill_counts_map)
        all_user_backfill_metadata.extend(user_metadata_list)

        if type_to_record_maps:
            write_records_to_cache(
                type_to_record_maps=type_to_record_maps,
                batch_size=batch_size,
            )
        del did_to_backfill_counts_map
        del type_to_record_maps
        gc.collect()

    # Calculate metadata statistics
    total_users = len(dids)
    processed_users = len(all_user_backfill_metadata)

    return {
        "total_batches": total_batches,
        "did_to_backfill_counts_map": did_to_backfill_counts_full_map,
        "total_processed_users": processed_users,
        "total_users": total_users,
        "user_backfill_metadata": all_user_backfill_metadata,
    }


def create_user_backfill_metadata(
    did: str,
    record_count_map: dict[str, int],
    bluesky_handle: str,
    pds_service_endpoint: str,
) -> UserBackfillMetadata:
    """Create metadata for a user's backfill operation.

    Args:
        did: The DID of the user
        record_count_map: Dictionary mapping record types to counts
        bluesky_handle: The Bluesky handle of the user
        pds_service_endpoint: The PDS service endpoint for the user

    Returns:
        A UserBackfillMetadata object containing information about the backfill
    """
    # Calculate total records
    total_records = sum(record_count_map.values())

    # Generate a comma-separated list of record types
    types = ",".join(sorted(record_count_map.keys()))

    # Convert record_count_map to JSON string
    total_records_by_type = json.dumps(record_count_map)

    # Generate current timestamp
    timestamp = generate_current_datetime_str()

    # Create and return the metadata object
    return UserBackfillMetadata(
        did=did,
        bluesky_handle=bluesky_handle,
        types=types,
        total_records=total_records,
        total_records_by_type=total_records_by_type,
        pds_service_endpoint=pds_service_endpoint,
        timestamp=timestamp,
    )


if __name__ == "__main__":
    did = "did:plc:w5mjarupsl6ihdrzwgnzdh4y"
    dids = [
        "did:plc:w5mjarupsl6ihdrzwgnzdh4y",
        # "did:plc:e4itbqoxctxwrrfqgs2rauga",
        # "did:plc:gedsnv7yxi45a4g2gts37vyp",
        # "did:plc:fbnm4hjnzu4qwg3nfjfkdhay",
        # "did:plc:dsnypqaat7r5nw6phtfs6ixw",
    ]
    backfills_map = do_backfill_for_users_parallel(
        dids,
        start_timestamp=default_start_timestamp,
        end_timestamp=default_end_timestamp,
    )
