"""Gets the posts that were shown each day, across all feeds."""

import json
import os

import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))


def get_posts_from_feed(feed: str) -> set[str]:
    feed_dicts = json.loads(feed)
    if not isinstance(feed_dicts, list):
        # some of them didn't get serialized correclty so they're json-dumped
        # json-strings, so we need to load them again
        feed_dicts = json.loads(feed_dicts)
    if not isinstance(feed_dicts, list):
        raise ValueError(f"Feed {feed} is not a list")
    try:
        return {feed_dict["item"] for feed_dict in feed_dicts}
    except Exception as e:
        raise ValueError(f"Error getting posts from feed {feed}: {e}")


def main():
    print("Getting posts in feeds per day...")
    feeds: pd.DataFrame = pd.read_parquet(
        os.path.join(current_dir, "consolidated_feeds.parquet")
    )
    feeds = feeds.sort_values("partition_date", ascending=True)
    feeds_by_partition_date: dict[str, pd.DataFrame] = {}
    for partition_date, df in feeds.groupby("partition_date"):
        feeds_by_partition_date[partition_date] = df
    posts_per_day: dict[str, set[str]] = {}
    for partition_date, df in feeds_by_partition_date.items():
        print(f"Processing {partition_date}...")
        posts_in_feeds = df.apply(lambda row: get_posts_from_feed(row["feed"]), axis=1)
        posts_in_feeds = set.union(*posts_in_feeds)
        posts_per_day[partition_date] = posts_in_feeds
        print(f"Found {len(posts_in_feeds)} posts in {partition_date}")
    export_filepath = os.path.join(current_dir, "posts_in_feeds_per_day.json")
    with open(export_filepath, "w") as f:
        json.dump({date: list(posts) for date, posts in posts_per_day.items()}, f)
    print(f"Exported posts per day to {export_filepath}")


if __name__ == "__main__":
    main()
