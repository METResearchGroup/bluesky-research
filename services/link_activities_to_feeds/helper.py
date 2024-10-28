"""Helper code for linking activities to feeds."""

from datetime import datetime, timedelta

import pandas as pd

from lib.constants import timestamp_format
from lib.helper import track_performance
from lib.log.logger import get_logger

from services.link_activities_to_feeds.constants import default_lookback_days
from services.link_activities_to_feeds.load_data import (
    load_latest_study_user_activities,
    load_post_conversation_traits,
    load_post_ime_scores,
)
from services.link_activities_to_feeds.map_activities_to_feeds import (
    map_activities_to_feeds,
)
from services.link_activities_to_feeds.process_data import (
    get_user_session_logs_with_feeds_shown,
    process_user_session_logs,
)

logger = get_logger(__name__)

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
def enrich_feed_posts_with_attributes(
    user_session_logs_with_feeds: pd.DataFrame,
    post_conversation_traits: pd.DataFrame,
    post_ime_scores: pd.DataFrame,
) -> list[list[dict]]:
    """Given the posts in the feeds, enrich them with their conversation traits
    and IM scores."""
    feeds = user_session_logs_with_feeds["feed_shown"].tolist()
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
            "author_did": post["author_did"],
            "llm_model_name": post["llm_model_name"],
            "sociopolitical_was_successfully_labeled": post[
                "sociopolitical_was_successfully_labeled"
            ],
            "sociopolitical_reason": post["sociopolitical_reason"],
            "sociopolitical_label_timestamp": post["sociopolitical_label_timestamp"],
            "political_ideology_label": post["political_ideology_label"],
            "perspective_was_successfully_labeled": post[
                "perspective_was_successfully_labeled"
            ],
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
            try:
                post_ime_score_dict = post_ime_score_map.get(
                    post["post"], default_post_ime_score_dict
                )
            except Exception as e:
                print(f"Error: {e}")
                breakpoint()
            if not post_ime_score_dict["uri"]:
                total_posts_not_in_post_ime_scores += 1
            post_conversation_trait_dict = post_conversation_trait_map.get(
                post["post"], default_post_conversation_dict
            )
            if not post_conversation_trait_dict["uri"]:
                total_posts_not_in_post_conversation_traits += 1
            updated_feed.append(
                {
                    **post,
                    **post_ime_score_dict,
                    **post_conversation_trait_dict,
                }
            )
            total_posts += 1
        updated_feeds.append(updated_feed)

    logger.info(f"Total posts from feeds: {total_posts}")
    logger.info(
        f"Total posts without post IME scores: {total_posts_not_in_post_ime_scores}/{total_posts}"
    )
    logger.info(
        f"Total posts without post conversation traits (toxicity/constructiveness): {total_posts_not_in_post_conversation_traits}/{total_posts}"
    )

    return updated_feeds


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
    logger.info(f"Loaded {len(latest_study_user_activities)} study user activities.")

    # fetch the user session logs and separate them out from the study user activities
    # so that they can be processed separately. Also load the URIs of the posts
    # that have been shown in the feeds.
    user_session_logs_with_feeds, uris_of_posts_shown_in_feeds = (
        get_user_session_logs_with_feeds_shown(
            latest_study_user_activities=latest_study_user_activities
        )
    )

    latest_study_user_activities = latest_study_user_activities[
        latest_study_user_activities["data_type"] != "user_session_log"
    ].reset_index(drop=True)

    logger.info(f"Loaded {len(latest_study_user_activities)} study user activities.")
    logger.info(
        f"Loaded {len(user_session_logs_with_feeds)} user session logs with feeds shown."
    )

    # load enrichment data for the posts shown in the feeds.
    post_conversation_traits: pd.DataFrame = load_post_conversation_traits(
        post_uris=uris_of_posts_shown_in_feeds, latest_timestamp=latest_timestamp
    )
    logger.info(
        f"Loaded {len(post_conversation_traits)} posts with conversation traits."
    )
    post_ime_scores: pd.DataFrame = load_post_ime_scores(
        post_uris=uris_of_posts_shown_in_feeds, latest_timestamp=latest_timestamp
    )
    logger.info(f"Loaded {len(post_ime_scores)} post IM scores.")

    user_session_logs_with_feeds: pd.DataFrame = process_user_session_logs(
        user_session_logs_with_feeds=user_session_logs_with_feeds
    )

    # enrich the user session logs with enriched data about each post that
    # was shown in the feed.
    enriched_feeds: list[list[dict]] = enrich_feed_posts_with_attributes(
        user_session_logs_with_feeds=user_session_logs_with_feeds,
        post_conversation_traits=post_conversation_traits,
        post_ime_scores=post_ime_scores,
    )

    user_session_logs_with_feeds["enriched_feeds"] = enriched_feeds

    map_activities_to_feeds(
        latest_study_user_activities=latest_study_user_activities,
        user_session_logs_with_feeds=user_session_logs_with_feeds,
    )


if __name__ == "__main__":
    link_activities_to_feeds()
