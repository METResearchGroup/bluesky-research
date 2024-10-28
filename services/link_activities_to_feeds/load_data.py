"""Loads latest data for mapping activities to feeds."""

from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import track_performance
from lib.log.logger import get_logger

import pandas as pd

logger = get_logger(__name__)


def load_latest_study_user_activities(latest_timestamp: str) -> pd.DataFrame:
    df = load_data_from_local_storage(
        service="aggregated_study_user_activities",
        latest_timestamp=latest_timestamp,
    )
    # remove default user.
    df = df[df["author_did"] != "default"]
    return df


@track_performance
def load_latest_consolidated_posts(latest_timestamp: str) -> pd.DataFrame:
    consolidated_posts: pd.DataFrame = load_data_from_local_storage(
        service="consolidated_enriched_post_records",
        latest_timestamp=latest_timestamp,
    )
    return consolidated_posts


@track_performance
def load_post_conversation_traits(
    post_uris: list[str], latest_timestamp: str
) -> pd.DataFrame:
    """Loads the conversation traits of posts.

    Loads the previous consolidated posts and returns the conversation traits of the posts.
    """
    consolidated_posts: pd.DataFrame = load_latest_consolidated_posts(
        latest_timestamp=latest_timestamp
    )
    logger.info(f"Loaded {len(consolidated_posts)} total consolidated posts.")
    post_conversation_traits: pd.DataFrame = consolidated_posts.loc[
        consolidated_posts["uri"].isin(post_uris),
        [
            "uri",
            "author_did",
            "llm_model_name",
            "sociopolitical_was_successfully_labeled",
            "sociopolitical_reason",
            "sociopolitical_label_timestamp",
            "political_ideology_label",
            "perspective_was_successfully_labeled",
            "perspective_reason",
            "perspective_label_timestamp",
            "prob_toxic",
            "prob_constructive",
        ],
    ].reset_index(drop=True)
    return post_conversation_traits


@track_performance
def load_post_ime_scores(post_uris: list[str], latest_timestamp: str) -> pd.DataFrame:
    """Loads the IM scores of posts."""
    latest_ime_scores: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_ime",
        latest_timestamp=latest_timestamp,
    )
    logger.info(f"Loaded {len(latest_ime_scores)} total IME scores.")
    latest_ime_scores = latest_ime_scores.loc[
        latest_ime_scores["uri"].isin(post_uris),
    ].reset_index(drop=True)
    return latest_ime_scores
