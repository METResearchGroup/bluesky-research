"""Performs backfills for users."""

from typing import Optional

from lib.helper import generate_current_datetime_str, track_performance
from lib.log.logger import get_logger
from services.backfill.sync.backfill import run_batched_backfill
from services.backfill.sync.constants import (
    default_start_timestamp,
    default_end_timestamp,
)

logger = get_logger(__name__)


@track_performance
def do_backfills_for_users(
    dids: list[str],
    start_timestamp: Optional[str] = default_start_timestamp,
    end_timestamp: Optional[str] = default_end_timestamp,
    event: Optional[dict] = None,
):
    # TODO: load previous session (if applicable)

    # TODO: do validation based on previous session (if applicable)

    valid_dids = []

    # do backfills
    backfills_map: dict[str, dict[str, dict]] = run_batched_backfill(dids=valid_dids)
    print(backfills_map)
    # write to DB

    # record backfills metadata
    timestamp = generate_current_datetime_str()
    backfill_session = {
        "backfill_timestamp": timestamp,
        "dids": valid_dids,
        "total_dids": len(valid_dids),
        "event": event,
    }
    return backfill_session


def write_backfill_session_to_db(backfill_session: dict):
    """Writes the backfill session to the database."""
    pass
