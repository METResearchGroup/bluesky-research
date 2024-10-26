"""Helper code for linking activities to feeds."""

from datetime import datetime, timedelta
import json

import pandas as pd

from lib.constants import timestamp_format
from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import track_performance
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


default_post_conversation_dict = {
    "uri": "",
    "llm_model_name": "",
    "sociopolitical_was_successfully_labeled": "",
    "sociopolitical_reason": "",
    "sociopolitical_label_timestamp": "",
    "political_ideology_label": "",
    "perspective_was_successfully_labeled": "",
    "perspective_reason": "",
    "perspective_label_timestamp": "",
    "prob_toxic": "",
    "prob_constructive": "",
}

default_post_ime_score_dict = {
    "uri": "",
    "text": "",
    "prob_emotion": "",
    "prob_intergroup": "",
    "prob_moral": "",
    "prob_other": "",
    "label_emotion": "",
    "label_intergroup": "",
    "label_moral": "",
    "label_other": "",
    "source": "",
    "label_timestamp": "",
}

@track_performance
def get_user_session_logs_with_feeds_shown(
    latest_study_user_activities: pd.DataFrame
) -> tuple[pd.DataFrame, set[str]]:
    """Gets the user session logs with feeds shown, loaded from JSON."""
    user_session_logs = latest_study_user_activities[
        latest_study_user_activities["data_type"] == "user_session_log"
    ]
    user_session_log_data: list[dict] = user_session_logs["data"].apply(json.loads)
    
    # list of lists of dicts, corresponding to lists of feeds shown.
    feeds_shown: list[list[dict]] = [
        json.loads(data["feed"]) for data in user_session_log_data
    ]

    user_session_logs["data"] = feeds_shown

    uris_of_posts_shown_in_feeds: set[str] = set()
    for feed in feeds_shown:
        for post in feed:
            uris_of_posts_shown_in_feeds.add(post["post"])

    return user_session_logs, uris_of_posts_shown_in_feeds


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
    post_conversation_traits: pd.DataFrame = consolidated_posts.loc[
        consolidated_posts["uri"].isin(post_uris),
        [
            "uri",
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
    latest_ime_scores = latest_ime_scores.loc[
        latest_ime_scores["uri"].isin(post_uris),
    ].reset_index(drop=True)
    return latest_ime_scores


@track_performance
def enrich_feed_posts_with_attributes(
    user_session_logs_with_feeds: pd.DataFrame,
    post_conversation_traits: pd.DataFrame,
    post_ime_scores: pd.DataFrame
) -> list[list[dict]]:
    """Given the posts in the feeds, enrich them with their conversation traits
    and IM scores."""
    feeds = user_session_logs_with_feeds["data"].tolist()
    updated_feeds: list[list[dict]] = []

    post_ime_score_map: dict[str, dict] = {
        post["uri"]: {
            "uri": post["uri"],
            "text": post["text"],
            "prob_emotion": post["prob_emotion"],
            "prob_intergroup": post["prob_intergroup"],
            "prob_moral": post["prob_moral"],
            "prob_other": post["prob_other"],
            "label_emotion": post["label_emotion"],
            "label_intergroup": post["label_intergroup"],
            "label_moral": post["label_moral"],
            "label_other": post["label_other"],
            "source": post["source"],
            "label_timestamp": post["label_timestamp"],
        }
        for post in post_ime_scores.to_dict(orient="records")
    }

    post_conversation_trait_map: dict[str, dict] = {
        post["uri"]: {
            "uri": post["uri"],
            "llm_model_name": post["llm_model_name"],
            "sociopolitical_was_successfully_labeled": post["sociopolitical_was_successfully_labeled"],
            "sociopolitical_reason": post["sociopolitical_reason"],
            "sociopolitical_label_timestamp": post["sociopolitical_label_timestamp"],
            "political_ideology_label": post["political_ideology_label"],
            "perspective_was_successfully_labeled": post["perspective_was_successfully_labeled"],
            "perspective_reason": post["perspective_reason"],
            "perspective_label_timestamp": post["perspective_label_timestamp"],
            "prob_toxic": post["prob_toxic"],
            "prob_constructive": post["prob_constructive"],
        }
        for post in post_conversation_traits.to_dict(orient="records")
    }

    total_posts: int = 0
    total_posts_not_in_post_ime_scores: int = 0
    total_posts_not_in_post_conversation_traits: int = 0

    for feed in feeds:
        updated_feed: list[dict] = []
        for post in feed:
            post_ime_score_dict = post_ime_score_map.get(post["uri"], default_post_ime_score_dict)
            if not post_ime_score_dict["uri"]:
                total_posts_not_in_post_ime_scores += 1
            post_conversation_trait_dict = post_conversation_trait_map.get(post["uri"], default_post_conversation_dict)
            if not post_conversation_trait_dict["uri"]:
                total_posts_not_in_post_conversation_traits += 1
            updated_feed.append({
                **post,
                **post_ime_score_dict,
                **post_conversation_trait_dict,
            })
            total_posts += 1
        updated_feeds.append(updated_feed)

    logger.info(f"Total posts from feeds: {total_posts}")
    logger.info(f"Total posts without post IME scores: {total_posts_not_in_post_ime_scores}/{total_posts}")
    logger.info(f"Total posts without post conversation traits (toxicity/constructiveness): {total_posts_not_in_post_conversation_traits}/{total_posts}")

    return updated_feeds

@track_performance
def map_activities_to_feeds(
    latest_study_user_activities: pd.DataFrame,
    user_session_logs_with_feeds: pd.DataFrame
) -> pd.DataFrame:
    """Maps activities (e.g., likes/posts/follows/shares) to feeds."""
    mapped_df = pd.DataFrame()
    return mapped_df


@track_performance
def link_activities_to_feeds(lookback_days: int = default_lookback_days):
    """Links activities to feeds.

    Requires that we load study user activities, enrich the posts that were shown
    in the feeds, and then map the activities to the feeds.
    """
    latest_timestamp: str = (datetime.now() - timedelta(days=lookback_days)).strftime(
        timestamp_format
    )
    latest_study_user_activities: pd.DataFrame = load_latest_study_user_activities(
        latest_timestamp=latest_timestamp
    )

    # fetch the user session logs and separate them out from the study user activities
    # so that they can be processed separately. Also load the URIs of the posts
    # that have been shown in the feeds.
    user_session_logs_with_feeds, uris_of_posts_shown_in_feeds = get_user_session_logs_with_feeds_shown(
        latest_study_user_activities=latest_study_user_activities
    )
    latest_study_user_activities = latest_study_user_activities[
        latest_study_user_activities["data_type"] != "user_session_log"
    ].reset_index(drop=True)

    logger.info(f"Loaded {len(latest_study_user_activities)} study user activities.")
    logger.info(f"Loaded {len(user_session_logs_with_feeds)} user session logs with feeds shown.")

    post_conversation_traits: pd.DataFrame = load_post_conversation_traits(
        post_uris=uris_of_posts_shown_in_feeds,
        latest_timestamp=latest_timestamp
    )
    logger.info(f"Loaded {len(post_conversation_traits)} post conversation traits.")
    post_ime_scores: pd.DataFrame = load_post_ime_scores(
        post_uris=uris_of_posts_shown_in_feeds,
        latest_timestamp=latest_timestamp
    )
    logger.info(f"Loaded {len(post_ime_scores)} post IM scores.")

    enriched_feeds: list[list[dict]] = enrich_feed_posts_with_attributes(
        user_session_logs_with_feeds=user_session_logs_with_feeds,
        post_conversation_traits=post_conversation_traits,
        post_ime_scores=post_ime_scores
    )

    user_session_logs_with_feeds["enriched_feeds"] = enriched_feeds

    mapped_df: pd.DataFrame = map_activities_to_feeds(
        latest_study_user_activities=latest_study_user_activities,
        user_session_logs_with_feeds=user_session_logs_with_feeds
    )

    breakpoint()


if __name__ == "__main__":
    link_activities_to_feeds()
