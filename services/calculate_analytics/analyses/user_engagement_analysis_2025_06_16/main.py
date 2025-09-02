"""
Main script for user engagement analysis.
"""

import os

import pandas as pd

from lib.helper import generate_current_datetime_str, get_partition_dates
from lib.log.logger import get_logger
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
)
from services.calculate_analytics.shared.data_loading.engagement import (
    get_content_engaged_with_per_user,
    get_engaged_content,
)
from services.calculate_analytics.shared.data_loading.labels import (
    get_all_labels_for_posts,
)
from services.calculate_analytics.shared.data_loading.users import (
    load_user_data,
)
from services.calculate_analytics.shared.analysis.content_analysis import (
    get_daily_engaged_content_per_user_metrics,
    get_weekly_content_per_user_metrics,
    transform_daily_content_per_user_metrics,
    transform_weekly_content_per_user_metrics,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
current_datetime_str: str = generate_current_datetime_str()
engaged_content_daily_aggregated_results_export_fp = os.path.join(
    current_dir,
    "results",
    f"daily_content_label_proportions_per_user_{current_datetime_str}.csv",
)
engaged_content_weekly_aggregated_results_export_fp = os.path.join(
    current_dir,
    "results",
    f"weekly_content_label_proportions_per_user_{current_datetime_str}.csv",
)

logger = get_logger(__file__)


def do_setup():
    """Setup steps for analysis. Includes:
    - 1. Loading users
    - 2. Loading content that users engaged with. Then we transform it to a
    format suited for our analysis.
    - 3. Loading the labels for the content that users engaged with. Then we
    transform it to a format suited for our analysis.
    """
    # load users and partition dates.
    try:
        user_df, user_date_to_week_df, valid_study_users_dids = load_user_data()
        partition_dates: list[str] = get_partition_dates(
            start_date=STUDY_START_DATE,
            end_date=STUDY_END_DATE,
        )
    except Exception as e:
        logger.error(f"Failed to load user data and/or partition dates: {e}")
        raise

    # get all content engaged with by study users, keyed on the URI of the post.
    # We do it in this way so that we can track all the ways that a given post is engaged
    # and by who. This is required since multiple users can engage with the same post, in
    # multiple different ways (e.g., one user likes a post, one shares it, etc.) and
    # the same user can even engage with one post in multiple ways (e.g., a user can both
    # retweet and like a post)

    try:
        engaged_content: dict[str, list[dict]] = get_engaged_content(
            valid_study_users_dids=valid_study_users_dids
        )
        logger.info(
            f"Total number of posts engaged with in some way: {len(engaged_content)}"
        )  # noqa
    except Exception as e:
        logger.error(f"Failed to get engaged content: {e}")
        raise

    # get all the content engaged with by user, keyed on user DID.
    # keyed on user, value = another dict, with key = date, value = engagements (like/post/repost/reply)
    # The idea here is we can know, for each user, on which days they engaged with content
    # on Bluesky, and for each of those days, what they did those days.

    try:
        user_to_content_engaged_with: dict[str, dict] = (
            get_content_engaged_with_per_user(engaged_content=engaged_content)
        )
        logger.info(
            f"Total number of users who engaged with Bluesky content at some point: {len(user_to_content_engaged_with)}"
        )  # noqa
    except Exception as e:
        logger.error(f"Failed to get content engaged with per user: {e}")
        raise

    # now that we have the engagement data loaded and we've organized it on a
    # per-user, per-day level, we now load the labels for that engagement data.
    try:
        engaged_content_uris = list(engaged_content.keys())
        labels_for_engaged_content: dict[str, dict] = get_all_labels_for_posts(
            post_uris=engaged_content_uris, partition_dates=partition_dates
        )
        logger.info(
            f"Loaded {len(labels_for_engaged_content)} labels for {len(engaged_content_uris)} posts engaged with."
        )  # noqa
    except Exception as e:
        logger.error(f"Failed to get labels for engaged content: {e}")
        raise

    return {
        "user_df": user_df,
        "user_date_to_week_df": user_date_to_week_df,
        "user_to_content_engaged_with": user_to_content_engaged_with,
        "labels_for_engaged_content": labels_for_engaged_content,
        "partition_dates": partition_dates,
    }


def do_aggregations_and_export_results(
    user_df: pd.DataFrame,
    user_date_to_week_df: pd.DataFrame,
    user_to_content_engaged_with: dict[str, dict],
    labels_for_engaged_content: dict[str, dict],
    partition_dates: list[str],
):
    """Perform aggregated analyses and export results.

    Aggregations are done at two levels:
    - (1) daily
    - (2) weekly.
    """

    # (1) Daily aggregations
    logger.info("[Daily analysis] Getting per-user, per-day content label metrics...")

    try:
        user_per_day_content_label_metrics: dict[
            str, dict[str, dict[str, float | None]]
        ] = get_daily_engaged_content_per_user_metrics(
            user_to_content_engaged_with=user_to_content_engaged_with,
            labels_for_engaged_content=labels_for_engaged_content,
        )
    except Exception as e:
        logger.error(f"Failed to get daily engaged content per user metrics: {e}")
        raise

    try:
        transformed_per_user_per_day_content_label_metrics: pd.DataFrame = (
            transform_daily_content_per_user_metrics(
                user_per_day_content_label_metrics=user_per_day_content_label_metrics,
                users=user_df,
                partition_dates=partition_dates,
            )
        )
    except Exception as e:
        logger.error(f"Failed to transform daily engaged content per user metrics: {e}")
        raise

    try:
        logger.info(
            f"[Daily analysis] Exporting per-user, per-day content label metrics to {engaged_content_daily_aggregated_results_export_fp}..."
        )
        os.makedirs(
            os.path.dirname(engaged_content_daily_aggregated_results_export_fp),
            exist_ok=True,
        )
        transformed_per_user_per_day_content_label_metrics.to_csv(
            engaged_content_daily_aggregated_results_export_fp, index=False
        )
        logger.info(
            f"[Daily analysis] Exported per-user, per-day content label metrics to {engaged_content_daily_aggregated_results_export_fp}..."
        )
    except Exception as e:
        logger.error(f"Failed to export daily engaged content per user metrics: {e}")
        raise

    # (2) Weekly aggregations.
    logger.info("[Weekly analysis] Getting per-user, per-week content label metrics...")

    try:
        user_per_week_content_label_metrics: dict[
            str, dict[str, dict[str, float | None]]
        ] = get_weekly_content_per_user_metrics(
            user_per_day_content_label_metrics=user_per_day_content_label_metrics,
            user_date_to_week_df=user_date_to_week_df,
        )
    except Exception as e:
        logger.error(f"Failed to get weekly engaged content per user metrics: {e}")
        raise

    try:
        transformed_user_per_week_content_label_metrics: pd.DataFrame = (
            transform_weekly_content_per_user_metrics(
                user_per_week_content_label_metrics=user_per_week_content_label_metrics,
                users=user_df,
                user_date_to_week_df=user_date_to_week_df,
            )
        )
    except Exception as e:
        logger.error(
            f"Failed to transform weekly engaged content per user metrics: {e}"
        )
        raise

    try:
        logger.info(
            f"[Weekly analysis] Exporting per-user, per-week content label metrics to {engaged_content_weekly_aggregated_results_export_fp}..."
        )
        os.makedirs(
            os.path.dirname(engaged_content_weekly_aggregated_results_export_fp),
            exist_ok=True,
        )
        transformed_user_per_week_content_label_metrics.to_csv(
            engaged_content_weekly_aggregated_results_export_fp, index=False
        )
        logger.info(
            f"[Weekly analysis] Exported per-user, per-day content label metrics to {engaged_content_weekly_aggregated_results_export_fp}..."
        )
    except Exception as e:
        logger.error(f"Failed to export weekly engaged content per user metrics: {e}")
        raise


def main():
    """Execute the steps required for doing analysis of the content that users engaged
    with during the study, both at the daily and weekly aggregation levels."""

    try:
        setup_objs = do_setup()

        user_df = setup_objs["user_df"]
        user_date_to_week_df = setup_objs["user_date_to_week_df"]
        user_to_content_engaged_with = setup_objs["user_to_content_engaged_with"]
        labels_for_engaged_content = setup_objs["labels_for_engaged_content"]
        partition_dates = setup_objs["partition_dates"]
    except Exception as e:
        logger.error(f"Failed to setup: {e}")
        raise

    try:
        do_aggregations_and_export_results(
            user_df=user_df,
            user_date_to_week_df=user_date_to_week_df,
            user_to_content_engaged_with=user_to_content_engaged_with,
            labels_for_engaged_content=labels_for_engaged_content,
            partition_dates=partition_dates,
        )
    except Exception as e:
        logger.error(f"Failed to do aggregations and export results: {e}")
        raise


if __name__ == "__main__":
    main()
