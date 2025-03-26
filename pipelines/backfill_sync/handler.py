"""Handler for the backfill sync pipeline."""

import traceback
from typing import Union

from lib.log.logger import get_logger
from services.backfill.main import backfill_records
from services.participant_data.helper import get_all_users

logger = get_logger(__name__)


def process_dids(dids_param: Union[str, list[str]]) -> list[str]:
    """Process the dids parameter to get a list of DIDs.

    Args:
        dids_param: Either "all" to fetch all user DIDs or a comma-separated string of DIDs

    Returns:
        List of DIDs to be processed
    """
    if dids_param == "all":
        # Get all users and extract their DIDs
        users = get_all_users()
        return [user.bluesky_user_did for user in users]
    elif isinstance(dids_param, str) and "," in dids_param:
        # Process comma-separated list of DIDs
        return [did.strip() for did in dids_param.split(",")]
    elif isinstance(dids_param, list):
        # Already a list, just return it
        return dids_param
    else:
        # Single DID as string
        return [dids_param]


def lambda_handler(event, context) -> dict:
    """Lambda handler for backfill sync pipeline.

    Args:
        event: Contains "dids" which can be "all" or a comma-separated list of DIDs
        context: AWS Lambda context

    Returns:
        Dictionary with metadata about the operation
    """
    try:
        if not event:
            event = {"dids": "all"}

        logger.info("Starting backfill sync pipeline.")

        # Process DIDs parameter
        dids_param = event.get("dids", "all")
        dids = process_dids(dids_param)

        logger.info(f"Processing {len(dids)} DIDs for sync.")

        # Create payload for backfill_records
        payload = {"record_type": "sync", "dids": dids}

        # Call backfill_records with the payload
        backfill_records(payload)

        logger.info("Completed backfill sync operation.")

    except Exception as e:
        logger.error(f"Error in backfill sync: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    dids = [
        "did:plc:w5mjarupsl6ihdrzwgnzdh4y",
        "did:plc:e4itbqoxctxwrrfqgs2rauga",
        "did:plc:gedsnv7yxi45a4g2gts37vyp",
        "did:plc:fbnm4hjnzu4qwg3nfjfkdhay",
    ]
    event = {"dids": ",".join(dids)}
    context = {}
    lambda_handler(event, context)
