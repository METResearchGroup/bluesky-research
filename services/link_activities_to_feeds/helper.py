"""Helper code for linking activities to feeds."""

import pandas as pd

from lib.log.logger import get_logger

# how many days to try and fetch data from.
default_lookback_days = 2

logger = get_logger(__name__)


def load_latest_study_user_activities() -> pd.DataFrame:
    pass


def load_latest_consolidated_posts(
    lookback_days: int = default_lookback_days,
) -> pd.DataFrame:
    pass


def load_post_conversation_traits(post_uris: list[str]) -> pd.DataFrame:
    """Loads the conversation traits of posts.

    Loads the previous consolidated posts and returns the conversation traits of the posts.
    """
    consolidated_posts: pd.DataFrame = load_latest_consolidated_posts()
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


def load_post_ime_scores(post_uris: list[str]) -> pd.DataFrame:
    """Loads the IM scores of posts."""
    pass


def link_activities_to_feeds():
    """Links activities to feeds."""

    latest_study_user_activities: pd.DataFrame = load_latest_study_user_activities()
    post_uris = latest_study_user_activities["uri"].tolist()
    post_conversation_traits: pd.DataFrame = load_post_conversation_traits(post_uris)
    post_ime_scores: pd.DataFrame = load_post_ime_scores(post_uris)
    logger.info(f"Loaded {len(post_conversation_traits)} post conversation traits.")
    logger.info(f"Loaded {len(post_ime_scores)} post IM scores.")
    pass


if __name__ == "__main__":
    link_activities_to_feeds()
