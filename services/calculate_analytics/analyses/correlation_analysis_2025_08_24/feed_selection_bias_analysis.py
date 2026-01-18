"""
Feed Selection Bias Analysis for Toxicity-Constructiveness Study

This script implements the feed selection bias analysis to investigate whether algorithmic
selection in feed generation creates artificial correlations between toxicity and constructiveness
scores. The analysis compares correlation patterns between posts used in feeds and the baseline
population to identify potential selection biases.

Research Question: "Assuming the baseline analysis comes out clean, look at the correlation of
toxicity x constructiveness on all posts used in feeds, to see if there's anything in the
algorithmic selection that causes this bias."

The analysis will:
- Load posts used in feeds data locally (manageable volume)
- Calculate correlations between toxicity and constructiveness for feed-selected posts
- Compare correlation patterns with baseline analysis results
- Implement bias detection metrics and statistical analysis
- Generate comparison reports and visualizations
- Identify whether feed selection algorithms introduce artificial correlations

This feed bias analysis serves as Phase 2 of the correlation investigation project and
depends on the completion of baseline correlation analysis (Phase 1).
"""

import os

import pandas as pd

from services.calculate_analytics.shared.analysis.correlations import (
    calculate_correlations,
    write_correlation_results,
)
from services.calculate_analytics.shared.data_loading.feeds import (
    map_users_to_posts_used_in_feeds,
)
from services.calculate_analytics.shared.data_loading.users import (
    get_user_condition_mapping,
)
from services.calculate_analytics.shared.data_loading.labels import (
    get_perspective_api_labels_for_posts,
)
from services.calculate_analytics.shared.constants import (
    STUDY_START_DATE,
    STUDY_END_DATE,
    exclude_partition_dates,
)
from lib.log.logger import get_logger
from lib.datetime_utils import get_partition_dates, generate_current_datetime_str

logger = get_logger(__name__)
output_dir = os.path.join(os.path.dirname(__file__), "results")


def accumulate_posts_used_in_feeds(
    partition_dates: list[str],
    user_condition_mapping: dict[str, str],
) -> dict[str, set[str]]:
    """
    Accumulate posts used in feeds for each condition.
    """
    condition_to_post_uris: dict[str, set[str]] = {}
    for partition_date in partition_dates:
        users_to_posts_used_in_feeds: dict[str, set[str]] = (
            map_users_to_posts_used_in_feeds(partition_date)
        )
        invalid_users = set()
        for user, post_uris in users_to_posts_used_in_feeds.items():
            try:
                condition = user_condition_mapping[user]
                if condition not in condition_to_post_uris:
                    condition_to_post_uris[condition] = set()
                condition_to_post_uris[condition].update(post_uris)
            except Exception:
                invalid_users.add(user)
        # expected that some wont' be found (maybe ~30) due to test users who
        # we added during our pilot.
        logger.info(
            f"Total invalid users for partition date {partition_date}: {len(invalid_users)}"
        )
    return condition_to_post_uris


def main():
    """
    Main execution function for feed selection bias analysis.
    """
    logger.info("Starting feed selection bias analysis")

    table_columns = ["uri", "partition_date", "prob_toxic", "prob_constructive"]
    table_columns_str = ", ".join(table_columns)
    query = (
        f"SELECT {table_columns_str} "
        f"FROM ml_inference_perspective_api "
        f"WHERE prob_toxic IS NOT NULL "
        f"AND prob_constructive IS NOT NULL "
    ).strip()

    # load users.
    user_condition_mapping: dict[str, str] = get_user_condition_mapping()

    # load posts used in feeds. Accumulate post URIs used in feeds for each condition.
    partition_dates: list[str] = get_partition_dates(
        start_date=STUDY_START_DATE,
        end_date=STUDY_END_DATE,
        exclude_partition_dates=exclude_partition_dates,
    )
    condition_to_post_uris: dict[str, set[str]] = accumulate_posts_used_in_feeds(
        partition_dates=partition_dates,
        user_condition_mapping=user_condition_mapping,
    )
    for condition, post_uris in condition_to_post_uris.items():
        logger.info(f"[Condition '{condition}']: {len(post_uris)} posts used in feeds")

    if not condition_to_post_uris:
        logger.error("No posts used in feeds found")
        return

    data = []
    for condition, uris in condition_to_post_uris.items():
        logger.info(
            f"[Condition '{condition}']: Loading Perspective API labels for {len(uris)} posts used in feeds"
        )
        for uri in uris:
            data.append((condition, uri))

    posts_df = pd.DataFrame(data, columns=["condition", "uri"])

    # load perspective API labels.
    df: pd.DataFrame = get_perspective_api_labels_for_posts(
        posts=posts_df,
        lookback_start_date=STUDY_START_DATE,
        lookback_end_date=STUDY_END_DATE,
        duckdb_query=query,
        query_metadata={
            "tables": [
                {"name": "ml_inference_perspective_api", "columns": table_columns}
            ]
        },
        export_format="duckdb",
    )
    if df.empty:
        logger.error("No Perspective API labels found for posts used in feeds")
        return

    timestamp = generate_current_datetime_str()

    # Result 1: Get correlations for all posts used in feeds.
    all_results = calculate_correlations(df, "prob_toxic", "prob_constructive")
    write_correlation_results(
        all_results,
        output_dir,
        f"all_posts_used_in_feeds_correlations_{timestamp}.json",
    )

    # Result 2: For each condition, get correlations for posts used in feeds.
    for condition, post_uris in condition_to_post_uris.items():
        results = calculate_correlations(
            df[df["uri"].isin(post_uris)], "prob_toxic", "prob_constructive"
        )
        write_correlation_results(
            results,
            output_dir,
            f"{condition}_posts_used_in_feeds_correlations_{timestamp}.json",
        )

    logger.info("Feed selection bias analysis complete")


if __name__ == "__main__":
    main()
