"""Link posts to feeds."""

import json
from typing import Optional

import pandas as pd

from lib.db.manage_local_data import (
    export_data_to_local_storage,
    load_data_from_local_storage,
)
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.helper import get_partition_dates
from lib.log.logger import get_logger

default_start_date = "2024-09-29"  # TODO:  check if this or 09/28
default_end_date = "2024-10-08"

logger = get_logger(__file__)


def load_feeds_for_partition_date(partition_date: str) -> pd.DataFrame:
    """Load the feeds for a given partition date."""
    feeds_df: pd.DataFrame = load_data_from_local_storage(
        service="generated_feeds",
        directory="cache",
        partition_date=partition_date,
    )
    logger.info(f"Loaded {len(feeds_df)} feeds for partition date {partition_date}")
    return feeds_df


# TODO: I need the enriched, hydrated posts for the partition date.
# Ideally that's in enrich_hydrate_posts.py though I need to double-check that
# implementation. We can filter more here too. Here, I'm going to assume
# that the output has the enriched, hydrated versions of all the posts
# that have appeared in feeds for the partition date.
def load_candidate_posts_for_partition_date(partition_date: str) -> pd.DataFrame:
    """Load the candidate posts for a given partition date."""
    candidate_posts_df: pd.DataFrame = load_data_from_local_storage(
        service="enriched_hydrated_posts",
        directory="cache",
        partition_date=partition_date,
    )
    logger.info(
        f"Loaded {len(candidate_posts_df)} candidate posts for partition date {partition_date}"
    )
    return candidate_posts_df


def link_posts_to_feeds(
    candidate_posts_df: pd.DataFrame,
    feeds_df: pd.DataFrame,
) -> pd.DataFrame:
    """Link the posts to the feeds.

    Naive implementation of looping through all the rows and then just
    doing json.loads() on the feed columns and iterating through all the
    posts in each feed to get the hydrated posts. Not sure if there's a more
    clever version, but this works for now and the sizes should be small
    enough for it to not matter?
    """
    post_uri_to_post_map: dict[str, dict] = {
        post["uri"]: post for post in candidate_posts_df.to_dict(orient="records")
    }

    logger.info(
        f"Linking {len(feeds_df)} feeds to {len(candidate_posts_df)} candidate posts"
    )

    linked_feeds: list[dict] = []

    for feed_record in feeds_df.to_dict(orient="records"):
        # NOTE: if it proves more complicated (e.g,. doubly-json-dumped), use
        # helper function that I wrote before (I think in the migration script?)
        feed_posts: list[dict] = json.loads(feed_record["feed"])
        hydrated_feed_posts: list[dict] = []

        for post_dict in feed_posts:
            post_uri = post_dict["item"]
            if post_uri in post_uri_to_post_map:
                hydrated_feed_posts.append(post_uri_to_post_map[post_uri])
            else:
                # NOTE: this should never happen, so it's something worth
                # flagging if it does (at least, this would imply that there's
                # a post whose preprocessed, enriched, hydrated version doesn't
                # exist in the candidate posts, which shouldn't happen?).
                logger.warning(f"Post URI {post_uri} not found in candidate posts.")

        feed_record["hydrated_feed"] = json.dumps(hydrated_feed_posts)
        feed_record.pop("feed")
        linked_feeds.append(feed_record)

    dtypes_map = MAP_SERVICE_TO_METADATA["generated_feeds"]["dtypes_map"]
    dtypes_map.pop("feed")
    dtypes_map["hydrated_feed"] = "string"

    linked_df: pd.DataFrame = pd.DataFrame(linked_feeds, dtypes=dtypes_map)

    logger.info(f"Resulting linked dataframe has {len(linked_df)} rows")
    return linked_df


def link_posts_to_feeds_for_partition_date(partition_date: str) -> None:
    """Link the posts to the feeds for a given partition date."""
    logger.info(f"Linking posts to feeds for partition date {partition_date}")

    candidate_posts_df: pd.DataFrame = load_candidate_posts_for_partition_date(
        partition_date=partition_date
    )
    feeds_df: pd.DataFrame = load_feeds_for_partition_date(
        partition_date=partition_date
    )
    linked_df: pd.DataFrame = link_posts_to_feeds(
        candidate_posts_df=candidate_posts_df,
        feeds_df=feeds_df,
    )

    # TODO:  check if this is correct. Check constants file (e.g., for correct 'timestamp' field)
    export_data_to_local_storage(
        service="linked_posts_to_feeds",
        df=linked_df,
        export_format="parquet",
    )
    logger.info(f"Completed linking posts to feeds for partition date {partition_date}")


def main(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    if not start_date:
        start_date = default_start_date
    if not end_date:
        end_date = default_end_date
    partition_dates = get_partition_dates(start_date, end_date)
    for partition_date in partition_dates:
        link_posts_to_feeds_for_partition_date(partition_date)
    logger.info(f"Completed linking posts to feeds for {start_date} to {end_date}")


if __name__ == "__main__":
    main()
