"""
Hydrate liked post URIs to full post records via PDS or Tap.

Reads liked_post_uris.json from the same directory. With --method pds, calls
get_post_record_given_post_uri for each URI. With --method tap, uses TapRepoSync
to add DIDs and consume events until all posts are received.
"""

import argparse
import asyncio
import json
import os
import sys

from transform.bluesky_helper import get_post_record_given_post_uri

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
from tap_client import TapRepoSync, extract_did_from_post_uri
INPUT_PATH = os.path.join(SCRIPT_DIR, "liked_post_uris.json")
PDS_OUTPUT_PATH = os.path.join(SCRIPT_DIR, "hydrated_posts_pds.json")
TAP_OUTPUT_PATH = os.path.join(SCRIPT_DIR, "hydrated_posts_tap.json")


def _serialize_record_response(response) -> dict | None:
    """Turn GetRecordResponse into a JSON-serializable dict."""
    if response is None:
        return None
    value = response.value
    if hasattr(value, "model_dump"):
        value_dict = value.model_dump()
    elif hasattr(value, "dict"):
        value_dict = value.dict()
    else:
        value_dict = dict(value) if value else None
    return {
        "uri": getattr(response, "uri", None),
        "cid": getattr(response, "cid", None),
        "value": value_dict,
    }


def hydrate_via_pds(uris: list[str]) -> list[dict]:
    """Fetch each post from the PDS and return list of hydrated records."""
    out: list[dict] = []
    for uri in uris:
        resp = get_post_record_given_post_uri(uri)
        rec = _serialize_record_response(resp)
        if rec:
            out.append(rec)
    return out


async def hydrate_via_tap(uris: list[str], timeout_sec: float = 120.0) -> dict[str, dict]:
    """Use Tap to stream repo events and collect posts for the given URIs."""
    dids = list({extract_did_from_post_uri(u) for u in uris})
    tap = TapRepoSync()
    return await tap.wait_for_posts(set(uris), timeout_sec=timeout_sec, dids_to_add=dids)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hydrate liked post URIs via PDS or Tap.")
    parser.add_argument(
        "--method",
        choices=["pds", "tap"],
        default="pds",
        help="Hydration method: pds (API per-URI) or tap (Tap repo sync)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="Timeout in seconds for Tap method (default 120)",
    )
    args = parser.parse_args()

    if not os.path.isfile(INPUT_PATH):
        raise SystemExit(f"Input file not found: {INPUT_PATH}. Run load_raw_likes.py first.")

    with open(INPUT_PATH) as f:
        uris = json.load(f)
    if not uris:
        raise SystemExit("No URIs in input file.")

    if args.method == "pds":
        records = hydrate_via_pds(uris)
        output_path = PDS_OUTPUT_PATH
        with open(output_path, "w") as f:
            json.dump(records, f, indent=2)
        print(f"Wrote {len(records)} hydrated posts to {output_path}")
    else:
        result = asyncio.run(hydrate_via_tap(uris, timeout_sec=args.timeout))
        # Convert to list of {uri, ...record} for consistency with PDS output
        records = [{"uri": uri, **rec} for uri, rec in result.items()]
        output_path = TAP_OUTPUT_PATH
        with open(output_path, "w") as f:
            json.dump(records, f, indent=2)
        print(f"Wrote {len(records)} hydrated posts to {output_path}")


if __name__ == "__main__":
    main()
