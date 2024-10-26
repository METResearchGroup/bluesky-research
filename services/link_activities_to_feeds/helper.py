"""Helper code for linking activities to feeds."""

from datetime import datetime, timedelta

import pandas as pd

from lib.constants import timestamp_format
from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger

# how many days to try and fetch data from.
default_lookback_days = 2

logger = get_logger(__name__)


def load_latest_study_user_activities(latest_timestamp: str) -> pd.DataFrame:
    df = load_data_from_local_storage(
        service="aggregated_study_user_activities",
        latest_timestamp=latest_timestamp,
    )
    return df


def load_latest_consolidated_posts(latest_timestamp: str) -> pd.DataFrame:
    consolidated_posts: pd.DataFrame = load_data_from_local_storage(
        service="consolidation",
        latest_timestamp=latest_timestamp,
    )
    return consolidated_posts


def load_post_conversation_traits(
    post_uris: list[str], latest_timestamp: str
) -> pd.DataFrame:
    """Loads the conversation traits of posts.

    Loads the previous consolidated posts and returns the conversation traits of the posts.
    """
    consolidated_posts: pd.DataFrame = load_latest_consolidated_posts(
        latest_timestamp=latest_timestamp
    )
    post_conversation_traits: pd.DataFrame = consolidated_posts.loc[
        consolidated_posts["uri"].isin(post_uris),
        [
            "uri",
            "llm_model_name",
            "sociopolitical_was_successfully_labeled",
            "sociopolitical_reason",
            "sociopolitical_label_timestamp",
            "is_sociopolitical",
            "political_ideology_label",
            "perspective_was_successfully_labeled",
            "perspective_reason",
            "perspective_label_timestamp",
            "prob_toxic",
            "prob_constructive",
        ],
    ].reset_index(drop=True)
    return post_conversation_traits


def load_post_ime_scores(post_uris: list[str], latest_timestamp: str) -> pd.DataFrame:
    """Loads the IM scores of posts."""
    latest_ime_scores: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_ime",
        latest_timestamp=latest_timestamp,
    )
    latest_ime_scores = latest_ime_scores.loc[
        latest_ime_scores["uri"].isin(post_uris),
    ].reset_index(drop=True)
    return latest_ime_scores


def link_activities_to_feeds(lookback_days: int = default_lookback_days):
    """Links activities to feeds."""
    latest_timestamp: str = (datetime.now() - timedelta(days=lookback_days)).strftime(
        timestamp_format
    )
    latest_study_user_activities: pd.DataFrame = load_latest_study_user_activities(
        latest_timestamp=latest_timestamp
    )
    post_uris: list[str] = latest_study_user_activities["uri"].tolist()
    post_conversation_traits: pd.DataFrame = load_post_conversation_traits(
        post_uris=post_uris, latest_timestamp=latest_timestamp
    )
    post_ime_scores: pd.DataFrame = load_post_ime_scores(
        post_uris=post_uris, latest_timestamp=latest_timestamp
    )
    logger.info(f"Loaded {len(post_conversation_traits)} post conversation traits.")
    logger.info(f"Loaded {len(post_ime_scores)} post IM scores.")


if __name__ == "__main__":
    link_activities_to_feeds()
