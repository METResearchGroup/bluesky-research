import gc
import os
from pprint import pprint
from typing import Optional
import requests

from atproto import CAR

from lib.constants import convert_bsky_dt_to_pipeline_dt
from lib.helper import create_batches, track_performance, rate_limit
from lib.log.logger import get_logger
from services.backfill.sync.constants import (
    default_start_timestamp,
    default_end_timestamp,
    endpoint,
    valid_types,
    default_batch_size,
)
from services.backfill.sync.export_data import write_records_to_cache

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


def validate_record_type(
    record: dict,
    record_type: str,
    did: str,
    start_timestamp: str = default_start_timestamp,
    end_timestamp: str = default_end_timestamp,
):
    """Validate the type of a record.

    Args:
        record: The record to validate
        record_type: The type of the record
        did: The DID of the user
        start_timestamp: The start timestamp to filter by
        end_timestamp: The end timestamp to filter by

    Returns:
        True if the record should be included, False otherwise
    """
    if record_type not in valid_types:
        logger.info(f"Skipping record type {record_type} for user {did}")
        return False
    # we want to keep all follows, since it's helpful to have a complete
    # picture of a user's entire social network. However, for the sake
    # of analysis, we'll only care about the delta in follows during the
    # course of the study. Yet let's keep all of this.
    if record_type == "follow":
        return True
    is_valid_timestamp: bool = validate_record_timestamp(
        record=record,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    return is_valid_timestamp


def do_backfill_for_user(
    did: str,
    start_timestamp: str = default_start_timestamp,
    end_timestamp: str = default_end_timestamp,
):
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
        A tuple containing the count map and record map
    """
    plc_doc = get_plc_directory_doc(did)
    pds_endpoint = plc_doc["service"][0][
        "serviceEndpoint"
    ]  # TODO: verify if this will always work.
    root_url = os.path.join(pds_endpoint, "xrpc")
    joined_url = os.path.join(root_url, endpoint)
    full_url = f"{joined_url}?did={did}"
    res = requests.get(full_url)
    car_file = CAR.from_bytes(res.content)
    records: list[dict] = [obj for obj in car_file.blocks.values()]
    type_to_record_map: dict[str, list[dict]] = {}
    type_to_count_map = {}
    total_skipped_records = 0
    for record in records:
        if "$type" in record:
            record_type = identify_record_type(record)
            is_valid_record = validate_record_type(
                record=record,
                record_type=record_type,
                did=did,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
            if not is_valid_record:
                total_skipped_records += 1
                continue
            type_to_count_map[record_type] = type_to_count_map.get(record_type, 0) + 1
            if record_type in type_to_record_map:
                type_to_record_map[record_type].append(record)
            else:
                type_to_record_map[record_type] = [record]
    print(f"For user with did={did}, found the following record types and counts:")
    pprint(type_to_count_map)
    print(f"Total original records: {len(records)}")
    print(f"Total skipped records: {total_skipped_records}")
    print(f"Total exported records: {len(records) - total_skipped_records}")
    return type_to_count_map, type_to_record_map


def transform_backfilled_records_for_export(
    type_to_record_maps: list[dict[str, dict]],
) -> list[dict]:
    """Transforms the backfilled records for export.

    Args:
        type_to_record_maps: A list of dictionaries mapping record types to records.

    Returns:
        A list of dictionaries, where each is a record that was synced. We add
        the "record_type" field to each record, so that we can easilly know
        what type of record it is.
    """
    res = []
    for type_to_record_map in type_to_record_maps:
        for record_type, records in type_to_record_map.items():
            for record in records:
                record["record_type"] = record_type
                record["synctimestamp"] = convert_bsky_dt_to_pipeline_dt(
                    record["createdAt"]
                )  # noqa
                res.append(record)
    return res


# TODO: still need to experiment to see what the rate limit is TBH.
@rate_limit(delay_seconds=5)
def do_backfill_for_users(
    dids: list[str],
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
) -> tuple[dict[str, dict[str, dict]], list[dict[str, dict]]]:
    """Performs the backfill for users.

    Args:
        dids: The list of DIDs to backfill.
        start_timestamp: The start timestamp to backfill from.
        end_timestamp: The end timestamp to backfill to.

    Returns:
        A tuple of two dictionaries. The first dictionary maps DIDs to the
        counts of records of each type that were backfilled. The second
        dictionary maps DIDs to the records of each type that were backfilled.
    """
    if not start_timestamp:
        start_timestamp = default_start_timestamp
    if not end_timestamp:
        end_timestamp = default_end_timestamp
    did_to_backfill_counts_map = {}
    type_to_record_maps = []
    for did in dids:
        type_to_count_map, type_to_record_map = do_backfill_for_user(
            did, start_timestamp=start_timestamp, end_timestamp=end_timestamp
        )
        did_to_backfill_counts_map[did] = type_to_count_map
        type_to_record_maps.append(type_to_record_map)
    return did_to_backfill_counts_map, type_to_record_maps


@track_performance
def run_batched_backfill(
    dids: list[str],
    batch_size: int = default_batch_size,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
) -> dict:
    """Runs the backfill for a list of DIDs in batches.

    Args:
        dids: The list of DIDs to backfill.
        batch_size: The number of DIDs to backfill in each batch.
        start_timestamp: The start timestamp to backfill from.
        end_timestamp: The end timestamp to backfill to.
    """
    batches: list[list[str]] = create_batches(batch_list=dids, batch_size=batch_size)
    total_batches = len(batches)
    did_to_backfill_counts_full_map = {}

    for i, batch in enumerate(batches):
        logger.info(f"Processing batch {i}/{total_batches}")
        did_to_backfill_counts_map, type_to_record_maps = do_backfill_for_users(
            dids=batch,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )
        logger.info(
            f"Updating metadata with counts for this batch of {len(batch)} users. Now exporting to cache...."
        )
        did_to_backfill_counts_full_map.update(did_to_backfill_counts_map)
        transformed_records: list[dict] = transform_backfilled_records_for_export(
            type_to_record_maps=type_to_record_maps,
        )
        if transformed_records:
            write_records_to_cache(
                records=transformed_records,
                batch_size=batch_size,
            )
        del did_to_backfill_counts_map
        del type_to_record_maps
        del transformed_records
        gc.collect()

    return {
        "total_batches": total_batches,
        "did_to_backfill_counts_map": did_to_backfill_counts_full_map,
    }


if __name__ == "__main__":
    did = "did:plc:w5mjarupsl6ihdrzwgnzdh4y"
    dids = [
        "did:plc:w5mjarupsl6ihdrzwgnzdh4y",
        # "did:plc:e4itbqoxctxwrrfqgs2rauga",
        # "did:plc:gedsnv7yxi45a4g2gts37vyp",
        # "did:plc:fbnm4hjnzu4qwg3nfjfkdhay",
        # "did:plc:dsnypqaat7r5nw6phtfs6ixw",
    ]
    backfills_map = do_backfill_for_users(
        dids,
        start_timestamp=default_start_timestamp,
        end_timestamp=default_end_timestamp,
    )
