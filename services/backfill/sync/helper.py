"""Performs backfills for users."""

import re
from typing import Optional

from lib.helper import generate_current_datetime_str, track_performance
from lib.log.logger import get_logger
from services.backfill.sync.backfill import run_batched_backfill

logger = get_logger(__name__)


def validate_dids(dids: list[str]) -> list[str]:
    """Validates the DIDs and ensures they meet required criteria.

    Args:
        dids: The list of DIDs to validate.

    Returns:
        A list of valid, deduplicated DIDs that match the correct format.

    Validation includes:
    1. Deduplication: Removes duplicate DIDs from the list
    2. Format validation: Ensures each DID follows the 'did:plc:[alphanumeric]' format
    3. Logs warning for any DIDs that don't match the expected format
    """
    # Store valid DIDs
    valid_dids = []

    # Regex pattern for valid DIDs (did:plc:alphanumeric)
    did_pattern = re.compile(r"^did:plc:[a-z0-9]+$")

    # Track duplicates and invalid formats
    duplicates = set()
    invalid_formats = []

    # Process each DID
    for did in dids:
        # Skip empty strings
        if not did:
            logger.warning("Empty DID found and skipped")
            continue

        # Check for duplicates
        if did in duplicates:
            logger.info(f"Duplicate DID skipped: {did}")
            continue

        # Validate format
        if not did_pattern.match(did):
            logger.warning(f"Invalid DID format skipped: {did}")
            invalid_formats.append(did)
            continue

        # DID is valid, add to valid list and track as seen
        valid_dids.append(did)
        duplicates.add(did)

    # Log summary stats
    if len(dids) != len(valid_dids):
        logger.info(f"Validated {len(valid_dids)} DIDs from {len(dids)} input DIDs")
        if len(invalid_formats) > 0:
            logger.info(f"Skipped {len(invalid_formats)} DIDs with invalid format")
        if len(dids) - len(invalid_formats) - len(valid_dids) > 0:
            logger.info(
                f"Removed {len(dids) - len(invalid_formats) - len(valid_dids)} duplicate DIDs"
            )

    return valid_dids


@track_performance
def do_backfills_for_users(
    dids: list[str],
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    event: Optional[dict] = None,
) -> dict:
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
