"""Gets the consolidated and enriched posts that ended up in the feeds."""

from datetime import datetime, timedelta
import gc
import json
import os

from lib.db.manage_local_data import load_data_from_local_storage
from scripts.analytics.helper import get_partition_dates

analytics_data_path = "/projects/p32375/bluesky_research_data/analytics"
export_prefix = os.path.join(analytics_data_path, "consolidated_posts")
num_days_to_lookahead = 7
excluded_partition_dates = ["2024-10-09"]  # server outage


def get_feed_posts_to_consider(
    partition_date: str,
    posts_in_feeds_per_day_dict: dict[str, set[str]],
    lookahead_days: int = num_days_to_lookahead,
) -> set[str]:
    """Get the feeds that could have had posts from the given partition date.

    Returns a set of feed IDs.
    """
    res: set[str] = set()
    for day in range(lookahead_days):
        partition_date_to_check = (
            datetime.strptime(partition_date, "%Y-%m-%d") + timedelta(days=day)
        ).strftime("%Y-%m-%d")
        if partition_date_to_check not in posts_in_feeds_per_day_dict:
            # this is OK for some dates. For example, on the first set of
            # feeds, we want to consider posts that appeared before them,
            # so we'll be looking for feeds with that partition date and that
            # will be empty, since partition_date refers to the posts partition
            # date, not the date of the feeds.
            print(f"No feeds for partition date={partition_date_to_check}")
        feed_ids = posts_in_feeds_per_day_dict.get(partition_date_to_check, set())
        res.update(feed_ids)
    return res


def main():
    print("Getting consolidated posts that were used in feeds...")
    with open(os.path.join(analytics_data_path, "posts_in_feeds_per_day.json")) as f:
        posts_in_feeds_per_day_dict = json.loads(f.read())
    # get the partition dates to load posts from. This should be a few days
    # before the earliest feeds since it'll take posts from the previous days.
    partition_dates = get_partition_dates(
        start_date="2024-09-26", end_date="2024-12-01"
    )
    # partition_dates = ["2024-09-27", "2024-09-28", "2024-09-29", "2024-09-30"]
    for current_partition_date in partition_dates:
        if current_partition_date in excluded_partition_dates:
            continue
        consolidated_enriched_posts_df = load_data_from_local_storage(
            service="consolidated_enriched_post_records",
            directory="cache",
            partition_date=current_partition_date,
        )
        print(
            f"Loaded {len(consolidated_enriched_posts_df)} consolidated enriched posts for partition date={current_partition_date}"
        )
        feed_ids_to_consider: set[str] = get_feed_posts_to_consider(
            partition_date=current_partition_date,
            posts_in_feeds_per_day_dict=posts_in_feeds_per_day_dict,
        )
        print(
            f"Number of feed IDs to consider (from feeds with lookahead days={num_days_to_lookahead}): {len(feed_ids_to_consider)}"
        )
        filtered_df = consolidated_enriched_posts_df[
            consolidated_enriched_posts_df["uri"].isin(feed_ids_to_consider)
        ]
        print(
            f"Filtered {len(consolidated_enriched_posts_df)} posts down to {len(filtered_df)} posts that were actually used in feeds"
        )
        if "partition_date" not in filtered_df.columns:
            filtered_df["partition_date"] = current_partition_date
        filtered_df.to_parquet(
            export_prefix, index=False, partition_cols=["partition_date"]
        )
        print(
            f"Exported partitioned parquet file to {os.path.join(export_prefix, f'partition_date={current_partition_date}.parquet')}"
        )
        del consolidated_enriched_posts_df
        gc.collect()
        del filtered_df
        gc.collect()


if __name__ == "__main__":
    main()
