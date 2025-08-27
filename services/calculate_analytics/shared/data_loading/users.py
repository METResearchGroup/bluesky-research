"""Shared user data loading functionality for analytics system.

This module provides unified interfaces for loading study user data and
demographic information, eliminating code duplication and ensuring
consistent data handling patterns.
"""

import os
import pandas as pd

from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel
from services.calculate_analytics.shared.constants import shared_assets_directory

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


def load_user_date_to_week_df() -> pd.DataFrame:
    """Load the user date to week mapping from the database."""
    fp = os.path.join(
        shared_assets_directory, "static", "bluesky_per_user_week_assignments.csv"
    )
    df = pd.read_csv(fp)
    df = df[["bluesky_handle", "date", "week_dynamic"]]
    df = df.rename(columns={"week_dynamic": "week"})
    return df


def load_user_data() -> tuple[pd.DataFrame, pd.DataFrame, set[str]]:
    """Loads user data, transformed in the format needed for analysis.

    Returns the following:
    - user_df: DataFrame where each row is a user.
    - user_date_to_week_df: transformed DataFrame where each row is a user + date
    and the week assigned to that date. This requires adding the "bluesky_user_did"
    since the original .csv file loaded doesn't contain the user DIDs and all
    of our transformations are done at the user DID level.
    - valid_study_users_dids: a set of the DIDs for valid study users. We return as
    a set since this helps us quickly filter content during analysis (as opposed
    to essentially regenerating this set during individual transformations whenever
    we need to do filtering).
        - We can have activities that aren't tied to valid study users because (1) we
        may have been tracking a pilot user (in which case we don't want to include)
        their data, or (2) we might be tracking activities from a user who dropped out
        of the study (either voluntary or we ourselves excluded their data or their account
        got banned).
    """

    # load the user data from DynamoDB.
    user_df: pd.DataFrame = load_user_demographic_info()
    user_df = user_df[user_df["is_study_user"]]
    user_df = user_df[["bluesky_handle", "bluesky_user_did", "condition"]]

    # load mapping of user to date and week. This denotes, for each user and date,
    # what week of the study that was for that user. We need this so we know, for a
    # given date, what week to assign their activities and measures to.
    user_date_to_week_df: pd.DataFrame = load_user_date_to_week_df()
    user_handle_to_did_map = {
        bluesky_handle: bluesky_user_did
        for bluesky_handle, bluesky_user_did in zip(
            user_df["bluesky_handle"], user_df["bluesky_user_did"]
        )
    }

    # We have to add the bluesky_user_did to the user + date + week DataFrame
    # since our engagement data (and frankly, any Bluesky activity data that can
    # be linked to a specific user) only contains their DID, not their user
    # handle and the generated `user_date_to_week_df` doesn't originally have that
    # field available.
    user_date_to_week_df["bluesky_user_did"] = user_date_to_week_df[
        "bluesky_handle"
    ].map(user_handle_to_did_map)

    # we base the valid users on the Qualtrics logs. We load the users from
    # DynamoDB but some of those users aren't valid (e.g., they were
    # experimental users, they didn't do the study, etc.). Since
    # user_date_to_week_df is generated using the Qualtrics logs, we treat that
    # as the master log of valid users who participated and completed the study.
    valid_study_user_handles = set(user_date_to_week_df["bluesky_handle"].unique())
    valid_study_users_dids = set(
        [user_handle_to_did_map[handle] for handle in valid_study_user_handles]
    )
    user_df = user_df[user_df["bluesky_user_did"].isin(valid_study_users_dids)]

    return user_df, user_date_to_week_df, valid_study_users_dids
