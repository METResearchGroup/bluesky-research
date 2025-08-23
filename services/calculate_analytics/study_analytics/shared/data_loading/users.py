"""Shared user data loading functionality for analytics system.

This module provides unified interfaces for loading study user data and
demographic information, eliminating code duplication and ensuring
consistent data handling patterns.
"""

import pandas as pd

from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel

logger = get_logger(__file__)


def load_user_demographic_info() -> pd.DataFrame:
    """Load the user demographic info for the study.

    This function loads all study users and converts them to a DataFrame
    with key demographic information (handle, DID, condition).

    Returns:
        DataFrame containing user demographic information with columns:
        - bluesky_handle: The user's Bluesky handle
        - bluesky_user_did: The user's DID
        - condition: The user's study condition
    """
    try:
        users: list[UserToBlueskyProfileModel] = get_all_users()
        user_df: pd.DataFrame = pd.DataFrame([user.model_dump() for user in users])

        # Drop rows where is_study_user is False to only keep actual study participants
        user_df = user_df[user_df["is_study_user"]]

        # Select only the demographic columns
        user_df = user_df[["bluesky_handle", "bluesky_user_did", "condition"]]

        logger.info(
            f"Successfully loaded demographic info for {len(user_df)} study users"
        )
        return user_df

    except Exception as e:
        logger.error(f"Failed to load user demographic info: {e}")
        raise


def get_user_condition_mapping() -> dict[str, str]:
    """Get a mapping from user DID to study condition.

    Returns:
        Dictionary mapping user DIDs to their study conditions
    """
    try:
        user_df = load_user_demographic_info()
        user_condition_mapping = dict(
            zip(user_df["bluesky_user_did"], user_df["condition"])
        )
        logger.info(
            f"Successfully created condition mapping for {len(user_condition_mapping)} users"
        )
        return user_condition_mapping
    except Exception as e:
        logger.error(f"Failed to create user condition mapping: {e}")
        raise
