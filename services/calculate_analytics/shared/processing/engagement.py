"""Engagement analysis functions for analytics processing.

This module provides reusable functions for calculating user engagement metrics
and aggregating engagement data across different record types.
"""

from typing import Dict, List

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.shared.config import get_config

logger = get_logger(__file__)


def get_num_records_per_user_per_day(record_type: str) -> Dict[str, Dict[str, int]]:
    """Get all records of a specific type for study users.

    Args:
        record_type: The type of record to get (like, post, follow, repost)

    Returns:
        Dictionary with structure:
        {
            "did": {
                "2024-10-01": 10,
                "2024-10-02": 20,
                ...
            },
            ...
        }

        The dates returned for a user are only the dates for which there are records.
    """
    config = get_config()
    study_config = config.study

    custom_args = {"record_type": record_type}
    df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        start_partition_date=study_config.start_date_inclusive,
        end_partition_date=study_config.end_date_inclusive,
        custom_args=custom_args,
    )

    df = df.drop_duplicates(subset=["uri"])
    df["partition_date"] = pd.to_datetime(df["synctimestamp"]).dt.date

    # Group by author and partition_date to get the number of records per user per day
    column_name = f"num_{record_type}s"
    records_per_user_per_day: List[Dict] = (
        df.groupby(["author", "partition_date"])
        .size()
        .reset_index(name=column_name)
        .to_dict(orient="records")
    )

    result = {}

    for row in records_per_user_per_day:
        author = row["author"]
        date = row["partition_date"].strftime("%Y-%m-%d")
        num_records = row[column_name]

        if author not in result:
            result[author] = {}

        result[author][date] = num_records

    return result


def aggregate_metrics_per_user_per_day(
    users: List[Dict], partition_dates: List[str]
) -> Dict[str, Dict[str, Dict[str, int]]]:
    """Get all engagement metrics for users per day.

    Args:
        users: List of user dictionaries
        partition_dates: List of partition dates to process

    Returns:
        Dictionary with structure:
        {
            "<handle>": {
                "2024-10-01": {
                    "num_likes": 10,
                    "num_posts": 20,
                    "num_follows": 30,
                    "num_reposts": 40,
                },
                ...
            },
            ...
        }

        Imputes zeros where relevant.
    """
    num_likes_per_user_per_day = get_num_records_per_user_per_day("like")
    num_posts_per_user_per_day = get_num_records_per_user_per_day("post")
    num_follows_per_user_per_day = get_num_records_per_user_per_day("follow")
    num_reposts_per_user_per_day = get_num_records_per_user_per_day("repost")

    result = {}

    for user in users:
        handle = user["bluesky_handle"]
        result[handle] = {}

        for partition_date in partition_dates:
            result[handle][partition_date] = {
                "num_likes": num_likes_per_user_per_day.get(handle, {}).get(
                    partition_date, 0
                ),
                "num_posts": num_posts_per_user_per_day.get(handle, {}).get(
                    partition_date, 0
                ),
                "num_follows": num_follows_per_user_per_day.get(handle, {}).get(
                    partition_date, 0
                ),
                "num_reposts": num_reposts_per_user_per_day.get(handle, {}).get(
                    partition_date, 0
                ),
            }

    return result


def get_engagement_summary_per_user(
    users: List[Dict], partition_dates: List[str]
) -> pd.DataFrame:
    """Get engagement summary for each user across all partition dates.

    Args:
        users: List of user dictionaries
        partition_dates: List of partition dates to process

    Returns:
        DataFrame with engagement summary per user
    """
    engagement_data = aggregate_metrics_per_user_per_day(users, partition_dates)

    summary_rows = []
    for handle, date_data in engagement_data.items():
        total_likes = sum(date_data[date]["num_likes"] for date in date_data)
        total_posts = sum(date_data[date]["num_posts"] for date in date_data)
        total_follows = sum(date_data[date]["num_follows"] for date in date_data)
        total_reposts = sum(date_data[date]["num_reposts"] for date in date_data)

        summary_rows.append(
            {
                "bluesky_handle": handle,
                "total_likes": total_likes,
                "total_posts": total_posts,
                "total_follows": total_follows,
                "total_reposts": total_reposts,
                "total_engagements": total_likes
                + total_posts
                + total_follows
                + total_reposts,
            }
        )

    return pd.DataFrame(summary_rows)


def calculate_engagement_rates(
    users: List[Dict], partition_dates: List[str]
) -> pd.DataFrame:
    """Calculate engagement rates (engagements per day) for each user.

    Args:
        users: List of user dictionaries
        partition_dates: List of partition dates to process

    Returns:
        DataFrame with engagement rates per user
    """
    engagement_data = aggregate_metrics_per_user_per_day(users, partition_dates)

    rate_rows = []
    for handle, date_data in engagement_data.items():
        total_engagements = sum(sum(date_data[date].values()) for date in date_data)
        active_days = len([date for date in date_data if any(date_data[date].values())])

        if active_days > 0:
            engagement_rate = total_engagements / active_days
        else:
            engagement_rate = 0.0

        rate_rows.append(
            {
                "bluesky_handle": handle,
                "total_engagements": total_engagements,
                "active_days": active_days,
                "engagement_rate": engagement_rate,
            }
        )

    return pd.DataFrame(rate_rows)
