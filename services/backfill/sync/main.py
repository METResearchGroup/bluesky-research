"""Backfill sync data for a given set of users."""

import json

from lib.helper import track_performance
from lib.log.logger import get_logger
from lib.metadata.models import RunExecutionMetadata
from lib.telemetry.wandb import log_run_to_wandb
from services.backfill.session_metadata import write_backfill_metadata_to_db
from services.backfill.sync.helper import do_backfills_for_users


logger = get_logger(__name__)


@log_run_to_wandb(service_name="backfill_sync")
@track_performance
def backfill_sync(payload: dict) -> RunExecutionMetadata:
    logger.info("Backfilling sync data")
    dids = payload.get("dids", [])
    start_timestamp = payload.get("start_timestamp", "")
    end_timestamp = payload.get("end_timestamp", "")
    backfill_session_metadata: dict = do_backfills_for_users(
        dids=dids,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        event=payload,
    )
    session_metadata = {
        "service": "backfill_sync",
        "timestamp": backfill_session_metadata["backfill_timestamp"],
        "status_code": 200,
        "body": json.dumps(backfill_session_metadata),
        "metadata_table_name": "backfill_sync_metadata",
        "metadata": json.dumps(backfill_session_metadata),
    }
    transformed_session_metadata = RunExecutionMetadata(**session_metadata)
    write_backfill_metadata_to_db(backfill_metadata=transformed_session_metadata)
    # TODO: write queues to DB. Should prob just do it here no? IDK.
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
