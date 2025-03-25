import os
from pprint import pprint
import requests
from typing import Optional

from atproto import CAR


endpoint = "com.atproto.sync.getRepo"


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
    type_to_count_map = {}
    for record in records:
        if "$type" in record:
            type_to_count_map[record["$type"]] = (
                type_to_count_map.get(record["$type"], 0) + 1
            )
    print(f"For user with did={did}, found the following record types and counts:")
    pprint(type_to_count_map)
    return type_to_count_map


def do_backfill_for_users(dids: list[str]):
    did_to_backfill_map = {}
    for did in dids:
        did_to_backfill_map[did] = do_backfill_for_user(did)
    return did_to_backfill_map


if __name__ == "__main__":
    did = "did:plc:w5mjarupsl6ihdrzwgnzdh4y"
    dids = [
        "did:plc:w5mjarupsl6ihdrzwgnzdh4y",
        "did:plc:e4itbqoxctxwrrfqgs2rauga",
        "did:plc:gedsnv7yxi45a4g2gts37vyp",
        "did:plc:fbnm4hjnzu4qwg3nfjfkdhay",
        "did:plc:dsnypqaat7r5nw6phtfs6ixw",
    ]
    backfills_map = do_backfill_for_users(dids)
    breakpoint()

    # root_url = "https://bsky.social/xrpc/"
    root_url = "https://puffball.us-east.host.bsky.network/xrpc/"
    endpoint = "com.atproto.sync.getRepo"
    joined_url = os.path.join(root_url, endpoint)
    full_url = f"{joined_url}?did={did}"
    res = requests.get(full_url)
    breakpoint()
    # print("Response status code:", res.status_code)
    # print("Response headers:", res.headers)
    # print("Response content:", res.content)
    # print("Response text:", res.text)
    # if res.status_code == 200:
    #     try:
    #         print("Response JSON:", res.json())
    #     except:
    #         print("Could not parse response as JSON")

    from atproto import CAR

    car_file = CAR.from_bytes(res.content)
    print(len(car_file.blocks.values()))
    foo = [obj for obj in car_file.blocks.values()]

    # headers = {"Content-Type": "application/json", "Accept": "application/json"}
    # params = Params(did=did, since=since)
    # res = requests.get(full_url, headers=headers, params=params)
    # breakpoint()
