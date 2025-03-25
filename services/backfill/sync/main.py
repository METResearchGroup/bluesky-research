"""Backfill sync data for a given set of users."""

import json
import traceback

from lib.helper import generate_current_datetime_str, track_performance
from lib.log.logger import get_logger
from lib.metadata.models import RunExecutionMetadata
from lib.telemetry.wandb import log_run_to_wandb
from services.backfill.session_metadata import write_backfill_metadata_to_db
from services.backfill.sync.helper import do_backfills_for_users
from services.write_cache_buffers_to_db.main import write_cache_buffers_to_db


logger = get_logger(__name__)


@log_run_to_wandb(service_name="backfill_sync")
@track_performance
def backfill_sync(payload: dict) -> RunExecutionMetadata:
    logger.info("Backfilling sync data")
    dids = payload.get("dids", [])
    start_timestamp = payload.get("start_timestamp", "")
    end_timestamp = payload.get("end_timestamp", "")
    skip_backfill = payload.get("skip_backfill", False)
    try:
        if skip_backfill:
            session_metadata = {
                "service": "backfill_sync",
                "timestamp": generate_current_datetime_str(),
                "status_code": 200,
                "body": json.dumps("Backfill sync skipped. Doing writes to DB only."),
                "metadata_table_name": "backfill_sync_metadata",
                "metadata": json.dumps({"skip_backfill": True}),
            }
        else:
            backfill_session_metadata: dict = do_backfills_for_users(
                dids=dids,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                event=payload,
            )
        # TODO: I have the stuff in a "backfill_sync" queue but I need to write
        # it to the "study_user_activity" table. I prob need to update
        # "write_cache_buffers_to_db" with some custom logic (maybe I can
        # just create a custom function and have the handler use that?)
        export_payload = {"service": "backfill_sync", "clear_queue": True}
        write_cache_buffers_to_db(payload=export_payload)
        session_metadata = {
            "service": "backfill_sync",
            "timestamp": backfill_session_metadata["backfill_timestamp"],
            "status_code": 200,
            "body": json.dumps(backfill_session_metadata),
            "metadata_table_name": "backfill_sync_metadata",
            "metadata": json.dumps(backfill_session_metadata),
        }
    except Exception as e:
        logger.error(f"Error backfilling sync data: {e}")
        logger.error(traceback.format_exc())
        session_metadata = {
            "service": "backfill_sync",
            "timestamp": generate_current_datetime_str(),
            "status_code": 500,
            "body": json.dumps(f"Error backfilling sync data: {str(e)}"),
            "metadata_table_name": "backfill_sync_metadata",
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
