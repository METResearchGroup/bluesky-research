"""Helper scripts for validating data."""

import re
from typing import Optional

from lib.constants import convert_bsky_dt_to_pipeline_dt
from lib.log.logger import get_logger
from services.backfill.core.constants import (
    default_start_timestamp,
    default_end_timestamp,
    valid_generic_bluesky_types,
)
from services.backfill.storage.session_metadata import load_latest_backfilled_users

logger = get_logger(__name__)


def identify_post_type(post: dict):
    """Identifies a post as written by a user.

    By "written by a user", we mean that a post is a standalone post by the user
    and not part of a thread (we count those as replies). Both standalone and
    threaded posts are obviously written by the user.
    """
    post_type = "reply" if "reply" in post else "post"
    return post_type


def identify_record_type(record: dict):
    """Identifies the type of a record.

    The record type is the last part of the record's $type field.
    """
    record_type = record["value"]["$type"].split(".")[-1]
    if record_type == "post":
        record_type = identify_post_type(record)
    return record_type


def validate_record_timestamp(
    record_timestamp: str,
    start_timestamp: str = default_start_timestamp,
    end_timestamp: str = default_end_timestamp,
) -> bool:
    """Get only the records within the range of the study."""
    record_timestamp_pipeline_dt = convert_bsky_dt_to_pipeline_dt(record_timestamp)
    if (
        record_timestamp_pipeline_dt < start_timestamp
        or record_timestamp_pipeline_dt > end_timestamp
    ):
        return False
    return True


def validate_is_valid_generic_bluesky_type(record: dict) -> bool:
    """Checks to see if the record is one of the expected generic Bluesky
    types. If not (e.g., if it's a custom record from another PDS), then
    we'll skip the record.

    Args:
        record: The record to validate

    Returns:
        True if the record is a valid generic Bluesky type, False otherwise

    NOTE: we only check the type of the record itself, we don't check the
    typing of subrecords (e.g., the type of the 'reply' field in a reply
    record), since I've seen that there's sometimes inconsistencies here
    because of Bluesky schema evolution over time.
    """
    return record["value"]["$type"] in valid_generic_bluesky_types


def validate_time_range_record(
    record: dict,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
) -> bool:
    """Validate that the record is within the time range.

    For our use case, we'll currently just look at the range from September 1st,
    2024, to December 2nd, 2024, as this'll encompass the range of posts that are
    most likely to have user engagement related to the study.
    """
    if not start_timestamp:
        start_timestamp = "2024-09-01-00:00:00"
    if not end_timestamp:
        end_timestamp = "2024-12-02-00:00:00"
    record_timestamp = record["value"]["createdAt"]
    record_timestamp_pipeline_dt = convert_bsky_dt_to_pipeline_dt(record_timestamp)
    return (
        record_timestamp_pipeline_dt >= start_timestamp
        and record_timestamp_pipeline_dt <= end_timestamp
    )


def filter_only_valid_bsky_records(record: dict, types_to_sync: list[str]) -> bool:
    """Get only Bluesky records."""
    return record["value"]["$type"] in types_to_sync


def validate_dids(
    dids: list[str], exclude_previously_backfilled_users: bool = True
) -> list[str]:
    """Validates the DIDs and ensures they meet required criteria.

    Args:
        dids: The list of DIDs to validate.
        exclude_previously_backfilled_users: Whether to exclude DIDs that have already been backfilled.

    Returns:
        A list of valid, deduplicated DIDs that match the correct format.

    Validation includes:
    1. Deduplication: Removes duplicate DIDs from the list
    2. Format validation: Ensures each DID follows the 'did:plc:[alphanumeric]' format
    3. Logs warning for any DIDs that don't match the expected format
    """

    if exclude_previously_backfilled_users:
        previously_backfilled_users: list[dict] = load_latest_backfilled_users()
        set_backfilled_dids: set[str] = set(
            [user["did"] for user in previously_backfilled_users]
        )
        dids = [did for did in dids if did not in set_backfilled_dids]

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
