"""For the feeds, calculates the proportion of posts that came from the "most-liked"
source as compared to the firehose."""

import gc
import os
from typing import Literal

import numpy as np
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
) -> dict:
    """Gets the URIs for the feeds."""
    feeds_df: pd.DataFrame = load_feeds_by_conditions(partition_date, conditions)
    condition_to_feeds_dict = {}
    for condition in conditions:
        condition_to_feeds_dict[condition] = []

    # for each feed, get the URIs.
    for _, row in feeds_df.iterrows():
        condition = row["condition"]
        feed = row["feed"]
        loaded_feed = load_feed_from_json_str(feed)
        posts_used_in_feeds_df: pd.DataFrame = get_posts_used_in_feeds(
            feeds=[loaded_feed], partition_date=partition_date, silence_logs=True
        )
        uris: set[str] = set(posts_used_in_feeds_df["uri"].tolist())
        condition_to_feeds_dict[condition].append(uris)

    for condition, feeds in condition_to_feeds_dict.items():
        print(f"For date={partition_date}, condition={condition}:")
        print(f"Total number of feeds: {len(feeds)}")

    return condition_to_feeds_dict


def calculate_posts_used_in_feeds_by_source(
    partition_date: str,
    lookback_start_date: str,
    lookback_end_date: str,
    source: Literal["most_liked", "firehose"],
) -> dict:
    """Gets the posts from the engagement and treatment conditions and checks
    to see how many of them use posts from the "most-liked" source.

    We're doing two things here:
    - Getting base rates (how many total # of posts came from the most-liked
    source versus firehose source)
    - Per-feed rates (how many of each feed came from each source).
    """
    try:
        uris_used_in_feeds: dict = get_uris_for_feeds_by_conditions(
            partition_date=partition_date,
            conditions=["engagement", "representative_diversification"],
        )
    except Exception as e:
        # should error out on 2024-10-08 due to server crash.
        logger.error(f"Error getting URIs for feeds by conditions: {e}")
        uris_used_in_feeds = {}
    all_uris_used_in_feeds = set()
    for condition, feeds in uris_used_in_feeds.items():
        for feed in feeds:
            all_uris_used_in_feeds.update(feed)
    total_posts_used_in_feeds = len(all_uris_used_in_feeds)
    most_liked_posts: pd.DataFrame = load_preprocessed_posts_by_source(
        partition_date=partition_date,
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
        source=source,
    )
    most_liked_posts_uris: set[str] = set(most_liked_posts["uri"].tolist())

    # for each condition, it will have a list of feeds (the feeds from users
    # in the condition), and for each feed, it will have a list of URIs (the
    # URIs of the posts used in the feed). We want to find the proportion of
    # URIs in each feed that come from the "most-liked" source.
    condition_to_prop_most_liked_posts_used_in_feeds: dict[str, list[float]] = {}
    for condition, feeds in uris_used_in_feeds.items():
        condition_to_prop_most_liked_posts_used_in_feeds[condition] = []
        for feed in feeds:
            total_overlap_posts = len(most_liked_posts_uris.intersection(feed))
            prop_most_liked_posts = total_overlap_posts / len(feed)
            condition_to_prop_most_liked_posts_used_in_feeds[condition].append(
                prop_most_liked_posts
            )

    # now we average out across every feed to get the average proportion of
    # each feed that comes from the "most-liked" source.
    proportions_most_liked_posts_per_feed = []
    for condition, props in condition_to_prop_most_liked_posts_used_in_feeds.items():
        proportions_most_liked_posts_per_feed.extend(props)

    average_prop_most_liked_posts_per_feed = np.mean(
        proportions_most_liked_posts_per_feed
    )
    average_prop_firehose_posts_per_feed = 1 - average_prop_most_liked_posts_per_feed

    # we now add in base rate info: how many total posts (across all feeds)
    # came from each source and how big was the base pool of posts used to
    # generate feeds.
    most_liked_posts_uris_used_in_feeds: set[str] = most_liked_posts_uris.intersection(
        all_uris_used_in_feeds
    )
    total_most_liked_posts = len(most_liked_posts_uris_used_in_feeds)
    total_firehose_posts = total_posts_used_in_feeds - total_most_liked_posts

    del most_liked_posts_uris_used_in_feeds
    del uris_used_in_feeds
    del all_uris_used_in_feeds
    del most_liked_posts
    del most_liked_posts_uris
    del condition_to_prop_most_liked_posts_used_in_feeds
    del proportions_most_liked_posts_per_feed
    gc.collect()

    result = {
        "date": partition_date,
        "Total firehose posts used across all feeds": total_firehose_posts,
        "Total most-liked posts used across all feeds": total_most_liked_posts,
        "Total posts used across all feeds": total_posts_used_in_feeds,
        "Average proportion of most-liked posts per feed": average_prop_most_liked_posts_per_feed,
        "Average proportion of firehose posts per feed": average_prop_firehose_posts_per_feed,
    }
    print(f"Metrics for date={partition_date}: {result}")
    return result


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
