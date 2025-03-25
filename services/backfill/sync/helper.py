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


def validate_dids(dids: list[str]) -> list[str]:
    """Validates the DIDs.

    Args:
        dids: The list of DIDs to validate.
    """
    return dids


@track_performance
def do_backfills_for_users(
    dids: list[str],
    start_timestamp: Optional[str] = default_start_timestamp,
    end_timestamp: Optional[str] = default_end_timestamp,
    event: Optional[dict] = None,
):
    """Do backfills for a given set of users."""

    valid_dids: list[str] = validate_dids(dids=dids)
    backfill_metadata: dict = run_batched_backfill(
        dids=valid_dids,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    timestamp = generate_current_datetime_str()
    backfill_session = {
        "backfill_timestamp": timestamp,
        "dids": valid_dids,
        "total_dids": len(valid_dids),
        "total_batches": backfill_metadata["total_batches"],
        "did_to_backfill_counts_map": backfill_metadata["did_to_backfill_counts_map"],
        "event": event,
    }
    return backfill_session
