"""Main script for fitting TF-IDF models on our data"""

import os

from lib.helper import generate_current_datetime_str, get_partition_dates
from lib.log.logger import get_logger
from ml_tooling.tf_idf.model import fit_model
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
    exclude_partition_dates,
)
from services.calculate_analytics.shared.data_loading.feeds import (
    get_all_post_uris_used_in_feeds,
    get_post_uris_used_in_feeds_per_user_per_day,
)
from services.calculate_analytics.shared.data_loading.users import load_user_data

current_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str: str = generate_current_datetime_str()

logger = get_logger(__file__)


def do_setup():
    """Setup steps for analysis."""

    # load users and partition dates.
    try:
        user_df, user_date_to_week_df, valid_study_users_dids = load_user_data()
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

    # get sociopolitical posts
    def get_sociopolitical_post_uris(
        partition_dates: list[str],
        filter_by_sociopolitical_filter: bool,
        is_sociopolitical_filter: bool,
    ):
        # NOTE: placeholder for actual sociopolitical post filtering.
        # Use the params to make intent explicit (and keep static analysis clean).
        if not filter_by_sociopolitical_filter:
            return set()
        if not is_sociopolitical_filter:
            return set()
        return set()

    # NOTE: we just need
    sociopolitical_post_uris: set[str] = get_sociopolitical_post_uris(
        partition_dates=partition_dates,
        filter_by_sociopolitical_filter=True,
        is_sociopolitical_filter=True,
    )

    # filter user_to_content_in_feeds to only include sociopolitical posts
    user_to_content_in_feeds = {
        user: {
            date: posts.intersection(sociopolitical_post_uris)
            for date, posts in user_posts.items()
        }
        for user, user_posts in user_to_content_in_feeds.items()
    }

    # load labels for feed content
    try:
        feed_content_uris: set[str] = get_all_post_uris_used_in_feeds(
            user_to_content_in_feeds=user_to_content_in_feeds
        )
    except Exception as e:
        logger.error(f"Failed to get labels for feed content: {e}")
        raise

    # now that we have the post URIs of sociopolitical posts used in feeds, we
    # can load the actual text of the posts.
    # Each dict has keys "uri", "text"]
    def load_posts(uris: set[str]):
        return []

    try:
        posts: list[dict[str, str]] = load_posts(uris=feed_content_uris)
    except Exception as e:
        logger.error(f"Failed to load posts: {e}")
        raise

    return {
        "posts": posts,
        "user_df": user_df,
        "user_date_to_week_df": user_date_to_week_df,
        "user_to_content_in_feeds": user_to_content_in_feeds,
        "partition_dates": partition_dates,
    }


def prepare_results_for_visualization():
    pass


# NOTE: need a URI -> doc_id mapping.
def fit_model_and_export_results(
    posts: list[dict[str, str]],
):
    post_texts: list[str] = [post["text"] for post in posts]

    def preprocess_texts(texts: list[str]):
        return texts

    preprocessed_texts = preprocess_texts(post_texts)
    _, fitted_sparse_matrix = fit_model(preprocessed_texts)
    # NOTE: now what?
    prepare_results_for_visualization()


def main():
    """Execute the steps required for fitting a global TF-IDF model on our data
    and then splicing it by condition and pre/post election periods for downstream
    visualization and analysis."""
    try:
        setup_objs = do_setup()
    except Exception as e:
        logger.error(f"Failed to setup: {e}")
        raise

    try:
        fit_model_and_export_results(posts=setup_objs["posts"])
    except Exception as e:
        logger.error(
            f"Failed to fit TF-IDF model and prepare results for visualization: {e}"
        )
        raise


if __name__ == "__main__":
    main()
