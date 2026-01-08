"""Given files containing the feeds, load the posts used in the feeds.

Currently we have the feeds saved locally. However, we'll want to use this in
the future to automatically get the posts used in the feeds as soon as we
generate the feeds (as part of the feed generation service, as the last step).

We do this on a day-by-day basis because:
- Today (2025-02-02), we're doing this for all past feeds, and with 1M feeds,
each with 50-100 posts, it can be quite memory-intensive.
- In the future, we'll likely want to run this as a daily cron job anyways,
where we take the feeds from the previous day and get the posts used in those
feeds (perhaps as the start of a daily analytics pipeline).
"""

import json

import pandas as pd

from lib.db.manage_local_data import (
    export_data_to_local_storage,
    load_data_from_local_storage,
)
from lib.datetime_utils import get_partition_dates
from lib.log.logger import get_logger
from services.fetch_posts_used_in_feeds.models import PostInFeedModel

logger = get_logger(__file__)


def load_feed_from_json_str(feed_json_str: str) -> list[dict]:
    """Load the feed from the json string.

    Some of the feeds are doubly-encoded as json-strings, so might need
    to be decoded twice.
    """
    res = json.loads(feed_json_str)
    if isinstance(res, list):
        return res
    else:
        res = json.loads(res)
        if isinstance(res, list):
            return res
        else:
            raise ValueError(f"Invalid feed: {res}")


def load_feeds_from_local_storage(partition_date: str) -> pd.DataFrame:
    """Load the feeds from the local storage."""
    logger.info(f"Loading feeds for partition date {partition_date}...")
    df = load_data_from_local_storage(
        service="generated_feeds",
        directory="cache",
        partition_date=partition_date,
    )
    logger.info(f"Loaded {len(df)} feeds for partition date {partition_date}.")
    return df


def get_posts_used_in_feeds(
    feeds: list[list[dict]], partition_date: str, silence_logs: bool = False
) -> pd.DataFrame:
    post_uris: list[str] = [post["item"] for feed in feeds for post in feed]
    deduped_post_uris: list[str] = list(set(post_uris))
    total_post_uris = len(post_uris)
    total_deduped_post_uris = len(deduped_post_uris)
    percentage_unique = total_deduped_post_uris / total_post_uris * 100
    if not silence_logs:
        logger.info(
            f"Found {total_deduped_post_uris} unique post URIs (out of {total_post_uris})."
        )
        logger.info(f"Found {percentage_unique:.2f}% unique post URIs.")
    posts: list[PostInFeedModel] = [
        PostInFeedModel(uri=uri, partition_date=partition_date)
        for uri in deduped_post_uris
    ]
    post_df: pd.DataFrame = pd.DataFrame([post.model_dump() for post in posts])
    return post_df


def get_posts_used_in_feeds_for_partition_date(partition_date: str) -> pd.DataFrame:
    df = load_feeds_from_local_storage(partition_date)
    feeds: list[str] = df["feed"].tolist()
    loaded_feeds: list[list[dict]] = [load_feed_from_json_str(feed) for feed in feeds]
    return get_posts_used_in_feeds(feeds=loaded_feeds, partition_date=partition_date)


def get_and_export_posts_used_in_feeds_for_partition_date(partition_date: str):
    """Get the posts used in the feeds for a given partition date and export to local storage."""
    logger.info(f"Processing posts from feeds with partition date {partition_date}...")

    post_df: pd.DataFrame = get_posts_used_in_feeds_for_partition_date(
        partition_date=partition_date
    )
    export_data_to_local_storage(
        service="fetch_posts_used_in_feeds",
        df=post_df,
        export_format="parquet",
    )
    logger.info(
        f"Exported {len(post_df)} posts to fetch_posts_used_in_feeds for partition date {partition_date}."
    )


def get_and_export_posts_used_in_feeds_for_partition_dates(
    start_date: str = "2024-09-28",
    end_date: str = "2025-12-01",
    exclude_partition_dates: list[str] = ["2024-10-08"],
):
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )

    for partition_date in partition_dates:
        get_and_export_posts_used_in_feeds_for_partition_date(
            partition_date=partition_date
        )
    logger.info("Finished with exporting posts across partition dates.")


if __name__ == "__main__":
    get_and_export_posts_used_in_feeds_for_partition_dates()
