"""Main script for running NER on feed posts dataset."""

import json
import os

import pandas as pd

from lib.helper import generate_current_datetime_str, get_partition_dates
from lib.log.logger import get_logger
from ml_tooling.ner.model import get_entities_for_posts
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
    exclude_partition_dates,
)
from services.calculate_analytics.shared.data_loading.feeds import (
    get_all_post_uris_used_in_feeds,
    get_post_uris_used_in_feeds_per_user_per_day,
)
from services.calculate_analytics.shared.data_loading.posts import (
    load_preprocessed_posts_by_uris,
)
from services.calculate_analytics.shared.data_loading.users import load_user_data
from services.calculate_analytics.analyses.content_analysis_2025_09_22.ner.transform import (
    aggregate_entities_by_condition_and_pre_post,
)
from services.calculate_analytics.analyses.content_analysis_2025_09_22.ner.visualize import (
    create_all_visualizations,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str: str = generate_current_datetime_str()

logger = get_logger(__file__)


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
            preprocessed_posts: pd.DataFrame = load_preprocessed_posts_by_uris(
                uris=feed_content_uris, partition_date=partition_date
            )
            for row in preprocessed_posts.itertuples():
                if row.uri not in uri_to_text:
                    uri_to_text[row.uri] = row.text

        logger.info(f"Loaded {len(uri_to_text)} texts for NER.")
    except Exception as e:
        logger.error(f"Failed to get posts for NER: {e}")
        raise

    return {
        "user_df": user_df,
        "user_to_content_in_feeds": user_to_content_in_feeds,
        "uri_to_text": uri_to_text,
    }


def export_user_posts(user_to_content_in_feeds, uri_to_text):
    """Exports user posts to local storage."""
    timestamp = generate_current_datetime_str()
    export_dir = os.path.join(current_dir, timestamp)

    # Create the export directory if it doesn't exist
    os.makedirs(export_dir, exist_ok=True)

    user_to_content_feeds_fp = os.path.join(export_dir, "user_to_content_in_feeds.json")
    uri_to_text_fp = os.path.join(export_dir, "uri_to_text.json")

    with open(user_to_content_feeds_fp, "w") as f:
        json.dump(user_to_content_in_feeds, f)
    with open(uri_to_text_fp, "w") as f:
        json.dump(uri_to_text, f)


def do_ner_and_export_results(
    uri_to_text: dict[str, str],
    user_df: pd.DataFrame,
    user_to_content_in_feeds: dict[str, dict[str, set[str]]],
):
    """Extract entities for all posts and create visualizations."""
    # get entities for all posts.
    try:
        uri_to_entities_map: dict[str, list[dict[str, str]]] = get_entities_for_posts(
            uri_to_text
        )
        logger.info(f"Extracted entities for {len(uri_to_entities_map)} posts")
    except Exception as e:
        logger.error(f"Failed to get entities for posts: {e}")
        raise

    # Aggregate entities by condition and pre/post-election
    try:
        aggregated_data = aggregate_entities_by_condition_and_pre_post(
            uri_to_entities_map=uri_to_entities_map,
            user_df=user_df,
            user_to_content_in_feeds=user_to_content_in_feeds,
            top_n=20,
            election_cutoff_date="2024-11-05",
        )
        logger.info("Successfully aggregated entities by condition and election period")
    except Exception as e:
        logger.error(f"Failed to aggregate entities: {e}")
        raise

    # Create visualizations
    try:
        output_dir = create_all_visualizations(aggregated_data)
        logger.info(f"Visualizations created successfully at: {output_dir}")
    except Exception as e:
        logger.error(f"Failed to create visualizations: {e}")
        raise

    return uri_to_entities_map, aggregated_data


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
        do_ner_and_export_results(
            uri_to_text=uri_to_text,
            user_df=user_df,
            user_to_content_in_feeds=user_to_content_in_feeds,
        )
    except Exception as e:
        logger.error(f"Failed to do aggregations and export results: {e}")
        raise


if __name__ == "__main__":
    main()
