"""For the feeds, calculates the proportion of posts that came from the "most-liked"
source as compared to the firehose."""

import os
from typing import Literal
import pandas as pd

from lib.helper import (
    calculate_start_end_date_for_lookback,
    generate_current_datetime_str,
    get_partition_dates,
)
from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.load_data.load_data import (
    load_preprocessed_posts_by_source,
)
from services.fetch_posts_used_in_feeds.helper import (
    load_feeds_from_local_storage,
    load_feed_from_json_str,
    get_posts_used_in_feeds,
)

current_filedir = os.path.dirname(os.path.abspath(__file__))

logger = get_logger(__name__)


def load_feeds_by_conditions(
    partition_date: str, conditions: list[str]
) -> pd.DataFrame:
    feeds_df: pd.DataFrame = load_feeds_from_local_storage(partition_date)
    filtered_feeds_df = feeds_df[feeds_df["condition"].isin(conditions)]
    return filtered_feeds_df


def get_uris_for_feeds_by_conditions(
    partition_date: str, conditions: list[str]
) -> set[str]:
    feeds_df: pd.DataFrame = load_feeds_by_conditions(partition_date, conditions)
    feeds: list[str] = feeds_df["feed"].tolist()
    loaded_feeds: list[list[dict]] = [load_feed_from_json_str(feed) for feed in feeds]
    posts_used_in_feeds_df: pd.DataFrame = get_posts_used_in_feeds(
        feeds=loaded_feeds, partition_date=partition_date
    )
    uris: set[str] = set(posts_used_in_feeds_df["uri"].tolist())
    return uris


def calculate_posts_used_in_feeds_by_source(
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
    source: Literal["most_liked", "firehose"],
) -> dict:
    """Gets the posts from the engagement and treatment conditions and checks
    to see how many of them use posts from the "most-liked" source.
    """
    try:
        uris_used_in_feeds: set[str] = get_uris_for_feeds_by_conditions(
            partition_date=partition_date,
            conditions=["engagement", "representative_diversification"],
        )
    except Exception as e:
        # should error out on 2024-10-08 due to server crash.
        logger.error(f"Error getting URIs for feeds by conditions: {e}")
        uris_used_in_feeds = set()
    total_posts_used_in_feeds = len(uris_used_in_feeds)
    most_liked_posts: pd.DataFrame = load_preprocessed_posts_by_source(
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
        source=source,
    )
    most_liked_posts_uris: set[str] = set(most_liked_posts["uri"].tolist())
    most_liked_posts_uris_used_in_feeds: set[str] = most_liked_posts_uris.intersection(
        uris_used_in_feeds
    )
    total_most_liked_posts = len(most_liked_posts_uris_used_in_feeds)
    total_firehose_posts = total_posts_used_in_feeds - total_most_liked_posts
    try:
        prop_most_liked_posts = round(
            total_most_liked_posts / total_posts_used_in_feeds, 2
        )
    except ZeroDivisionError:
        # should error out on 2024-10-08 due to server crash, and
        # possibly 2024-10-09 from the same reason.
        prop_most_liked_posts = 0
    prop_firehose_posts = 1 - prop_most_liked_posts
    return {
        "date": partition_date,
        "total_firehose_posts": total_firehose_posts,
        "total_most_liked_posts": total_most_liked_posts,
        "total_posts_used_in_feeds": total_posts_used_in_feeds,
        "prop_most_liked_posts": prop_most_liked_posts,
        "prop_firehose_posts": prop_firehose_posts,
    }


def calculate_prop_most_liked_posts_per_day_for_study() -> pd.DataFrame:
    start_date = "2024-09-28"
    end_date = "2024-12-01"
    # exclude_partition_dates = ["2024-10-08"]
    exclude_partition_dates = []
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )
    res = []
    for partition_date in partition_dates:
        lookback_start_date, lookback_end_date = calculate_start_end_date_for_lookback(
            partition_date=partition_date,
            # num_days_lookback=5,
            num_days_lookback=8,
            min_lookback_date="2024-09-28",
        )
        posts_used_in_feeds_by_source_dict = calculate_posts_used_in_feeds_by_source(
            partition_date=partition_date,
            lookback_start_date=lookback_start_date,
            lookback_end_date=lookback_end_date,
            source="most_liked",
        )
        res.append(posts_used_in_feeds_by_source_dict)
    df = pd.DataFrame(res)
    df = df.sort_values("date", ascending=True)
    df.to_csv(
        os.path.join(
            current_filedir,
            f"feed_proportion_per_source_{generate_current_datetime_str()}.csv",
        )
    )
    return df


if __name__ == "__main__":
    calculate_prop_most_liked_posts_per_day_for_study()
    # get_uris_for_feeds_by_conditions(
    #     partition_date="2024-10-01",
    #     conditions=["engagement", "representative_diversification"],
    # )
