"""Module for content analysis."""

import pandas as pd


# TODO: should be the logic in get_per_user_feed_averages_for_partition_date
# (but no need to add partition_date) - this can be used for both feed content
# analysis and engagement content analysis.


# this should calculate the averages and proportions for each of the labels.
def calculate_content_label_metrics(posts_df: pd.DataFrame) -> pd.DataFrame:
    pass


def get_feed_content_per_user_per_day_metrics():
    pass


def transform_feed_content_per_user_per_day_metrics():
    pass


def get_engaged_content_per_user_per_day_metrics(
    user_to_content_engaged_with: dict[str, dict], labels_for_engaged_content: dict
):
    pass


def transform_engaged_content_per_user_per_day_metrics():
    pass


def get_engaged_content_per_user_per_week_metrics():
    pass


def transform_engaged_content_per_user_per_week_metrics():
    pass
