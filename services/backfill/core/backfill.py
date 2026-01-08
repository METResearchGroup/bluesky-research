"""Backfilling the sync records for users."""

import aiohttp
import json
import os
import requests

from atproto import CAR

from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger
from services.backfill.core.constants import (
    endpoint,
)
from services.backfill.core.models import UserBackfillMetadata

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


async def async_send_request_to_pds(
    did: str, pds_endpoint: str, session: aiohttp.ClientSession
) -> requests.Response:
    """Send a request to the PDS endpoint.

    Args:
        did: The DID of the user
        pds_endpoint: The PDS endpoint to send the request to
    """
    root_url = os.path.join(pds_endpoint, "xrpc")
    joined_url = os.path.join(root_url, endpoint)
    full_url = f"{joined_url}?did={did}"
    return await session.get(full_url)


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


def get_records_from_pds_bytes(pds_bytes: bytes) -> list[dict]:
    """Get the records from the PDS bytes.

    Args:
        pds_bytes: The bytes of the PDS

    Returns:
        The records from the PDS
    """
    car_file = CAR.from_bytes(pds_bytes)
    records: list[dict] = [obj for obj in car_file.blocks.values()]
    return records


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
            records: list[dict] = get_records_from_pds_bytes(res.content)
        except Exception as e:
            logger.error(f"Error parsing CAR file for user {did}: {e}")
            logger.info("Returning no records for user.")
            records = []
    return records


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
