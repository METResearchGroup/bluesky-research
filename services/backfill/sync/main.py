"""Backfill sync data for a given set of users."""

import json
import traceback

from lib.helper import generate_current_datetime_str, track_performance
from lib.log.logger import get_logger
from lib.metadata.models import RunExecutionMetadata
from lib.telemetry.wandb import log_run_to_wandb
from services.backfill.sync.models import UserBackfillMetadata
from services.backfill.sync.session_metadata import write_backfill_metadata_to_db
from services.backfill.sync.constants import service_name
from services.backfill.sync.helper import do_backfills_for_users
from services.backfill.sync.load_data import load_latest_dids_to_backfill_from_queue
from services.write_cache_buffers_to_db.main import write_cache_buffers_to_db


logger = get_logger(__name__)


@log_run_to_wandb(service_name=service_name)
@track_performance
def backfill_sync(payload: dict) -> RunExecutionMetadata:
    """Backfill sync data for a given set of users.

    Args:
        payload: A dictionary containing the following keys:
            - dids: A list of DIDs to backfill
            - load_from_queue: A boolean indicating whether to load the DIDs from
              the queue instead of the payload.
            - start_timestamp: The start timestamp for the backfill
            - end_timestamp: The end timestamp for the backfill
            - skip_backfill: A boolean indicating whether to skip the backfill
            (it will still write the cached data to DB, but it won't query the
            PDSes to do the backfill.)

    Returns:
        A dictionary of metadata about the backfill.
    """
    logger.info("Backfilling sync data")
    dids = payload.get("dids", [])
    load_from_queue = payload.get("load_from_queue", False)
    start_timestamp = payload.get("start_timestamp", None)
    end_timestamp = payload.get("end_timestamp", None)
    skip_backfill = payload.get("skip_backfill", False)

    try:
        if load_from_queue:
            logger.info("Loading DIDs from queue instead of from payload.")
            dids: list[str] = load_latest_dids_to_backfill_from_queue()
            logger.info(f"Loaded {len(dids)} DIDs from queue.")

        if skip_backfill:
            backfill_session_metadata = {
                "backfill_timestamp": generate_current_datetime_str(),
                "event": payload,
                "user_backfill_metadata": [],  # Empty list when skipping backfill
                "did_to_backfill_counts_map": {},  # Empty map when skipping backfill
            }
        else:
            backfill_session_metadata: dict = do_backfills_for_users(
                dids=dids,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                event=payload,
            )

        export_payload = {"service": service_name, "clear_queue": True}
        write_cache_buffers_to_db(payload=export_payload)

        # Extract user metadata list from session metadata
        user_backfill_metadata: list[UserBackfillMetadata] = (
            backfill_session_metadata.pop("user_backfill_metadata", [])
        )
        # we pop this, we want to keep it for some logging stuff but
        # it ends up being too big to insert into DynamoDB
        # (apparently a max 400kb limit on item size).
        backfill_session_metadata.pop("did_to_backfill_counts_map")

        # Create session metadata object
        session_metadata = {
            "service": service_name,
            "timestamp": backfill_session_metadata["backfill_timestamp"],
            "status_code": 200,
            "body": json.dumps(backfill_session_metadata),
            "metadata_table_name": f"{service_name}_metadata",
            "metadata": "",
        }
        transformed_session_metadata = RunExecutionMetadata(**session_metadata)

        # Write both session and user metadata to DB
        write_backfill_metadata_to_db(
            session_backfill_metadata=transformed_session_metadata,
            user_backfill_metadata=user_backfill_metadata,
        )
    except Exception as e:
        logger.error(f"Error backfilling sync data: {e}")
        logger.error(traceback.format_exc())
        session_metadata = {
            "service": service_name,
            "timestamp": generate_current_datetime_str(),
            "status_code": 500,
            "body": json.dumps(f"Error backfilling sync data: {str(e)}"),
            "metadata_table_name": f"{service_name}_metadata",
            "metadata": json.dumps(traceback.format_exc()),
        }
        transformed_session_metadata = RunExecutionMetadata(**session_metadata)
        # Still try to write session metadata even if there was an error
        write_backfill_metadata_to_db(
            session_backfill_metadata=transformed_session_metadata,
            user_backfill_metadata=[],  # Empty list on error
        )
    logger.info("Backfilling sync data complete")
    return transformed_session_metadata


if __name__ == "__main__":
    dids = []
    start_timestamp = ""
    end_timestamp = ""
    payload = {
        "dids": dids,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
    }
    backfill_sync(payload)
