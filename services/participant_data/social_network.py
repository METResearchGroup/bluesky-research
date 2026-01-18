"""Utilities for processing user social network data."""

from collections import defaultdict

import pandas as pd

from lib.db.data_processing import parse_converted_pandas_dicts
from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger

from services.participant_data.models import SocialNetworkRelationshipModel

logger = get_logger(__name__)


def build_user_social_network_map(
    social_network_dicts: list[SocialNetworkRelationshipModel],
) -> dict[str, list[str]]:
    """Build a mapping from study user DIDs to their connection DIDs.

    Transforms social network relationship records into a dictionary
    mapping each study user to their list of connected user DIDs (followers/followees).

    Args:
        social_network_dicts: List of social network relationship records from
            the scraped_user_social_network service. Each record should contain:
            - relationship_to_study_user: "follower" or "follow"
            - follow_did: DID of the user being followed
            - follower_did: DID of the user who is following

    Returns:
        Dictionary mapping study_user_did -> list of connection_dids
    """
    user_to_social_network_map: dict[str, list[str]] = defaultdict(list)

    for row in social_network_dicts:
        relationship = row.relationship_to_study_user

        # Case 1: the social network connection record is for someone who's a
        # follower of a user in the study.
        if relationship == "follower":
            study_user_did = row.follow_did
            connection_did = row.follower_did
        # Case 2: the social network connection record is for someone who's a
        # followee (they're followed by a user in the study) of a user in the study.
        elif relationship == "follow":
            study_user_did = row.follower_did
            connection_did = row.follow_did
        else:
            # This should never happen due to Literal type, but kept for safety
            logger.warning(f"Skipping row with unknown relationship: {row}")
            continue  # Skip if relationship is not recognized

        user_to_social_network_map[study_user_did].append(connection_did)

    return dict(user_to_social_network_map)  # Convert defaultdict to regular dict


def load_user_social_network_map() -> dict[str, list[str]]:
    """Load and process user social network data into a mapping.

    Loads social network relationship data from local storage, processes it,
    and returns a dictionary mapping study user DIDs to their connection DIDs.

    Returns:
        Dictionary mapping study_user_did -> list of connection_dids
    """
    user_social_network_df: pd.DataFrame = load_data_from_local_storage(
        service="scraped_user_social_network",
        latest_timestamp=None,
        storage_tiers=["cache", "active"],
        validate_pq_files=True,
    )
    social_dicts: list[dict] = user_social_network_df.to_dict(orient="records")
    social_dicts = parse_converted_pandas_dicts(social_dicts)

    # Convert dicts to Pydantic models for type safety.
    # Note: we keep models here because downstream logic relies on attribute access.
    social_network_records = [
        SocialNetworkRelationshipModel(**record) for record in social_dicts
    ]
    return build_user_social_network_map(social_network_records)
