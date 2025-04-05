"""Backfill sync data for a given set of users."""

import json
import traceback

from lib.helper import generate_current_datetime_str, track_performance
from lib.log.logger import get_logger
from lib.metadata.models import RunExecutionMetadata
from lib.telemetry.wandb import log_run_to_wandb
from services.backfill.session_metadata import write_backfill_metadata_to_db
from services.backfill.sync.constants import service_name
from services.backfill.sync.helper import do_backfills_for_users
from services.write_cache_buffers_to_db.main import write_cache_buffers_to_db


logger = get_logger(__name__)


@log_run_to_wandb(service_name=service_name)
@track_performance
def backfill_sync(payload: dict) -> RunExecutionMetadata:
    """Backfill sync data for a given set of users.

    Args:
        payload: A dictionary containing the following keys:
            - dids: A list of DIDs to backfill
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
    start_timestamp = payload.get("start_timestamp", None)
    end_timestamp = payload.get("end_timestamp", None)
    skip_backfill = payload.get("skip_backfill", False)
    try:
        if skip_backfill:
            backfill_session_metadata = {
                "backfill_timestamp": generate_current_datetime_str(),
                "event": payload,
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
        session_metadata = {
            "service": service_name,
            "timestamp": backfill_session_metadata["backfill_timestamp"],
            "status_code": 200,
            "body": json.dumps(backfill_session_metadata),
            "metadata_table_name": f"{service_name}_metadata",
            "metadata": json.dumps(backfill_session_metadata),
        }
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
    write_backfill_metadata_to_db(backfill_metadata=transformed_session_metadata)
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
