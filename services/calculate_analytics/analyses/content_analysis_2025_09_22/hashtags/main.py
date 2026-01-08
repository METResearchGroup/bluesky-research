"""Hashtag analysis."""

import pickle
import gzip
import os

import pandas as pd

from lib.datetime_utils import get_partition_dates, generate_current_datetime_str
from lib.log.logger import get_logger
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
    exclude_partition_dates,
)
from services.calculate_analytics.shared.data_loading.labels import (
    load_sociopolitical_labels_by_uris,
)
from services.calculate_analytics.shared.data_loading.users import load_user_data
from services.calculate_analytics.shared.data_loading.feeds import (
    get_all_post_uris_used_in_feeds,
    get_post_uris_used_in_feeds_per_user_per_day,
)
from services.calculate_analytics.analyses.content_analysis_2025_09_22.hashtags.model import (
    analyze_hashtags_for_posts,
)
from services.calculate_analytics.analyses.content_analysis_2025_09_22.hashtags.transform import (
    aggregate_hashtags_by_condition_and_pre_post,
)
from services.calculate_analytics.analyses.content_analysis_2025_09_22.hashtags.visualize import (
    create_all_visualizations,
)

# Configuration constants
TOP_N = 30  # Number of top hashtags to analyze

logger = get_logger(__file__)
current_dir = os.path.dirname(os.path.abspath(__file__))


def do_setup():
    """Setup steps for analysis."""

    # load users and partition dates.
    try:
        user_df, _, valid_study_users_dids = load_user_data()
        partition_dates: list[str] = get_partition_dates(
            start_date=STUDY_START_DATE,
            end_date=STUDY_END_DATE,
            exclude_partition_dates=exclude_partition_dates,  # feed generation was broken on this date.
        )
    except Exception as e:
        logger.error(f"Failed to load user data and/or partition dates: {e}")
        raise

    # load feeds: per-user, per-date, get list of URIs of posts in feeds.
    try:
        user_to_content_in_feeds: dict[str, dict[str, set[str]]] = (
            get_post_uris_used_in_feeds_per_user_per_day(
                valid_study_users_dids=valid_study_users_dids
            )
        )
    except Exception as e:
        logger.error(f"Failed to get feeds per user: {e}")
        raise

    # load posts as hashmap of URI to text.
    try:
        feed_content_uris: set[str] = get_all_post_uris_used_in_feeds(
            user_to_content_in_feeds=user_to_content_in_feeds
        )
        uri_to_text: dict[str, str] = {}
        for partition_date in partition_dates:
            preprocessed_posts: pd.DataFrame = load_sociopolitical_labels_by_uris(
                uris=feed_content_uris, partition_date=partition_date
            )
            for row in preprocessed_posts.itertuples():
                if (
                    bool(row.was_successfully_labeled)
                    and bool(row.is_sociopolitical)
                    and (row.uri not in uri_to_text)
                ):
                    uri_to_text[row.uri] = row.text

        logger.info(f"Loaded {len(uri_to_text)} texts for Hashtag Analysis.")
    except Exception as e:
        logger.error(f"Failed to get posts for Hashtag Analysis: {e}")
        raise

    return {
        "user_df": user_df,
        "user_to_content_in_feeds": user_to_content_in_feeds,
        "uri_to_text": uri_to_text,
    }


def export_user_posts(user_to_content_in_feeds, uri_to_text):
    """Exports user posts to local storage as compressed pickle files."""
    timestamp = generate_current_datetime_str()
    export_dir = os.path.join(current_dir, timestamp)

    # Create the export directory if it doesn't exist
    os.makedirs(export_dir, exist_ok=True)

    user_to_content_feeds_fp = os.path.join(
        export_dir, "user_to_content_in_feeds.pkl.gz"
    )
    uri_to_text_fp = os.path.join(export_dir, "uri_to_text.pkl.gz")

    with gzip.open(user_to_content_feeds_fp, "wb") as f:
        pickle.dump(user_to_content_in_feeds, f)
    with gzip.open(uri_to_text_fp, "wb") as f:
        pickle.dump(uri_to_text, f)


def do_hashtag_analysis_and_export_results(
    uri_to_text: dict[str, str],
    user_df: pd.DataFrame,
    user_to_content_in_feeds: dict[str, dict[str, set[str]]],
):
    """Hashtag analysis and export results."""
    # get hashtags for all posts
    try:
        uri_to_hashtags: dict[str, dict] = analyze_hashtags_for_posts(uri_to_text)
    except Exception as e:
        logger.error(f"Failed to analyze hashtags for posts: {e}")
        raise

    # aggregate hashtags by condition and pre/post-election
    try:
        aggregated_data = aggregate_hashtags_by_condition_and_pre_post(
            uri_to_hashtags, user_df, user_to_content_in_feeds, top_n=TOP_N
        )
    except Exception as e:
        logger.error(
            f"Failed to aggregate hashtags by condition and pre/post-election: {e}"
        )
        raise

    # create visualizations
    try:
        output_dir = create_all_visualizations(aggregated_data)
        logger.info(f"Visualizations created in: {output_dir}")
    except Exception as e:
        logger.error(f"Failed to create visualizations: {e}")
        raise

    return uri_to_hashtags, aggregated_data


def main():
    try:
        setup_objs = do_setup()
        user_df: pd.DataFrame = setup_objs["user_df"]
        user_to_content_in_feeds: dict[str, dict[str, set[str]]] = setup_objs[
            "user_to_content_in_feeds"
        ]
        uri_to_text: dict[str, str] = setup_objs["uri_to_text"]
    except Exception as e:
        logger.error(f"Failed to setup: {e}")
        raise

    try:
        export_user_posts(
            user_to_content_in_feeds=user_to_content_in_feeds,
            uri_to_text=uri_to_text,
        )
    except Exception as e:
        logger.error(f"Failed to export user posts: {e}")
        raise

    try:
        do_hashtag_analysis_and_export_results(
            uri_to_text=uri_to_text,
            user_df=user_df,
            user_to_content_in_feeds=user_to_content_in_feeds,
        )
    except Exception as e:
        logger.error(f"Failed to do aggregations and export results: {e}")
        raise


if __name__ == "__main__":
    main()
