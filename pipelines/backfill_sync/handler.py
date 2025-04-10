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
        event: Contains:
            - "mode": Either "backfill" or "determine_dids_to_backfill" (default: "backfill")
            - "dids": Can be "all" or a comma-separated list of DIDs (for backfill mode)
            - "start_date", "end_date": Date range for determining DIDs (for determine mode)
        context: AWS Lambda context

    Returns:
        Dictionary with metadata about the operation
    """
    try:
        if not event:
            event = {"mode": "backfill", "dids": "all"}

        mode = event.get("mode", "backfill")
        logger.info(f"Starting backfill sync pipeline in {mode} mode.")

        if mode == "backfill":
            # Process DIDs parameter
            dids_param = event.get("dids", "all")
            dids = process_dids(dids_param)

            logger.info(f"Processing {len(dids)} DIDs for sync.")

            skip_backfill = event.get("skip_backfill", False)

            # Create payload for backfill_records
            payload = {
                "record_type": "sync",
                "dids": dids,
                "skip_backfill": skip_backfill,
            }

            # Call backfill_records with the payload
            backfill_records(payload)

            logger.info("Completed backfill sync operation.")

        elif mode == "determine_dids_to_backfill":
            from services.backfill.sync.determine_dids_to_backfill import (
                main as determine_dids_main,
            )

            # Create payload for determine_dids_to_backfill
            payload = {
                "start_date": event.get("start_date"),
                "end_date": event.get("end_date"),
            }

            # Call determine_dids_to_backfill with the payload
            determine_dids_main(payload=payload)

            logger.info("Completed determining DIDs to backfill.")

        else:
            logger.error(
                f"Invalid mode: {mode}. Must be 'backfill' or 'determine_dids_to_backfill'."
            )
            return {"status": "error", "message": f"Invalid mode: {mode}"}

        return {"status": "success", "mode": mode}

    except Exception as e:
        logger.error(f"Error in backfill sync: {e}")
        logger.error(traceback.format_exc())
        return {"status": "error", "message": str(e)}
