import os
from pprint import pprint
import requests
from typing import Optional

from lib.constants import convert_bsky_dt_to_pipeline_dt

from atproto import CAR


endpoint = "com.atproto.sync.getRepo"

earliest_record_start_date = "2024-09-27-00:00:00"
latest_record_end_date = "2024-12-02-00:00:00"

valid_types = [
    "block",
    "follow",
    "generator",
    "like",
    "post",
    "profile",
    "reply",
    "repost",
]


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


def validate_record_timestamp(record: dict):
    """Get only the records within the range of the study."""
    record_timestamp = record["createdAt"]
    record_timestamp_pipeline_dt = convert_bsky_dt_to_pipeline_dt(record_timestamp)
    if (
        record_timestamp_pipeline_dt < earliest_record_start_date
        or record_timestamp_pipeline_dt > latest_record_end_date
    ):
        return False
    return True


def do_backfill_for_user(did: str, since: Optional[str] = None):
    """
    Do backfill for a user.

    Params defined in https://github.com/MarshalX/atproto/blob/main/packages/atproto_client/models/com/atproto/sync/get_repo.py#L16
    - did: The DID of the repo.
    - since: The revision ('rev') of the repo to create a diff from.
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
    for record in records:
        if "$type" in record:
            record_type = identify_record_type(record)
            # validate date ranges of records. Exception for profiles,
            # since it's good to have for record-keeping and we expect these
            # to not have a timestamp.
            if record_type != "profile" and not validate_record_timestamp(record):
                continue
            type_to_count_map[record_type] = type_to_count_map.get(record_type, 0) + 1
            if record_type in type_to_record_map:
                type_to_record_map[record_type].append(record)
            else:
                type_to_record_map[record_type] = [record]
    print(f"For user with did={did}, found the following record types and counts:")
    pprint(type_to_count_map)
    return type_to_count_map, type_to_record_map


# TODO: add appropriate rate limiting, so that I don't get throttled
# by the PDSes. Need to check what this rate limit is.
def do_backfill_for_users(dids: list[str]):
    did_to_backfill_map = {}
    for did in dids:
        type_to_count_map, type_to_record_map = do_backfill_for_user(did)
        did_to_backfill_map[did] = {
            "type_to_count_map": type_to_count_map,
            "type_to_record_map": type_to_record_map,
        }
    return did_to_backfill_map


if __name__ == "__main__":
    did = "did:plc:w5mjarupsl6ihdrzwgnzdh4y"
    dids = [
        "did:plc:w5mjarupsl6ihdrzwgnzdh4y",
        # "did:plc:e4itbqoxctxwrrfqgs2rauga",
        # "did:plc:gedsnv7yxi45a4g2gts37vyp",
        # "did:plc:fbnm4hjnzu4qwg3nfjfkdhay",
        # "did:plc:dsnypqaat7r5nw6phtfs6ixw",
    ]
    backfills_map = do_backfill_for_users(dids)
