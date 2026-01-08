"""Gets the posts that were liked by study users.

We load the raw posts that were liked by study users (we got these posts from
backfilling the PDS endpoints). We take these raw posts and push them to the
queue in order to preprocess them.

We take likes from a certain date (e.g., "2024-10-08") and then load posts from
a lookback period (e.g., 10 days before "2024-10-08") and then we try to match
likes to posts.

We then write these posts to local storage.
"""

import json

import pandas as pd

from lib.db.manage_local_data import (
    load_data_from_local_storage,
    export_data_to_local_storage,
)
from lib.datetime_utils import get_partition_dates
from lib.log.logger import get_logger
from services.backfill.posts_used_in_feeds.load_data import (
    calculate_start_end_date_for_lookback,
)
from services.get_posts_liked_by_study_users.constants import (
    default_num_days_lookback,
    default_min_lookback_date,
    service_name,
)


logger = get_logger(__name__)


# TODO: should also load reposts and replies, not just posts?
def load_raw_posts_with_lookback(lookback_start_date: str, lookback_end_date: str):
    """Load the raw posts with lookback."""
    active_df = load_data_from_local_storage(
        service="raw_sync",
        directory="active",
        custom_args={"record_type": "post"},
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    cache_df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        custom_args={"record_type": "post"},
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    df = pd.concat([active_df, cache_df])
    return df


def load_replies_with_lookback(lookback_start_date: str, lookback_end_date: str):
    """Load the replies with lookback."""
    active_df = load_data_from_local_storage(
        service="raw_sync",
        directory="active",
        custom_args={"record_type": "reply"},
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    cache_df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        custom_args={"record_type": "reply"},
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    df = pd.concat([active_df, cache_df])
    return df


def load_reposts_with_lookback(lookback_start_date: str, lookback_end_date: str):
    """Load the reposts with lookback."""
    active_df = load_data_from_local_storage(
        service="raw_sync",
        directory="active",
        custom_args={"record_type": "repost"},
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    cache_df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        custom_args={"record_type": "repost"},
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    df = pd.concat([active_df, cache_df])
    return df


def load_likes_for_partition_date(partition_date: str):
    """Load the likes for a given partition date."""
    active_df = load_data_from_local_storage(
        service="raw_sync",
        directory="active",
        custom_args={"record_type": "like"},
        start_partition_date=partition_date,
        end_partition_date=partition_date,
    )
    cache_df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        custom_args={"record_type": "like"},
        start_partition_date=partition_date,
        end_partition_date=partition_date,
    )
    df = pd.concat([active_df, cache_df])
    return df


def load_likes_for_lookback_range(lookback_start_date: str, lookback_end_date: str):
    """Load the likes for a given lookback range."""
    active_df = load_data_from_local_storage(
        service="raw_sync",
        directory="active",
        custom_args={"record_type": "like"},
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    cache_df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        custom_args={"record_type": "like"},
        start_partition_date=lookback_start_date,
        end_partition_date=lookback_end_date,
    )
    df = pd.concat([active_df, cache_df])
    return df


def load_raw_posts_for_likes_from_partition_date(partition_date: str):
    """Load the raw posts, replies, and reposts to check against likes from a
    given partition date."""

    lookback_start_date, lookback_end_date = calculate_start_end_date_for_lookback(
        partition_date=partition_date,
        num_days_lookback=default_num_days_lookback,
        min_lookback_date=default_min_lookback_date,
    )

    # load likes for a partition date.
    likes_df = load_likes_for_partition_date(partition_date)

    # load raw posts from a partition date.
    raw_posts_df = load_raw_posts_with_lookback(
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
    )

    raw_replies_df = load_replies_with_lookback(
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
    )

    raw_reposts_df = load_reposts_with_lookback(
        lookback_start_date=lookback_start_date,
        lookback_end_date=lookback_end_date,
    )

    liked_post_uris: set[str] = set(
        [json.loads(subject)["uri"] for subject in likes_df["subject"]]
    )

    breakpoint()

    # TODO: filter raw_posts_df.
    filtered_raw_posts_df = raw_posts_df[raw_posts_df["uri"].isin(liked_post_uris)]
    filtered_raw_replies_df = raw_replies_df[
        raw_replies_df["uri"].isin(liked_post_uris)
    ]
    filtered_raw_reposts_df = raw_reposts_df[
        raw_reposts_df["uri"].isin(liked_post_uris)
    ]

    # TODO: can I combine them like this? Prob not TBH.
    filtered_raw_posts_df = pd.concat(
        [filtered_raw_posts_df, filtered_raw_replies_df, filtered_raw_reposts_df]
    )

    total_likes = len(likes_df)
    total_raw_posts = len(raw_posts_df)
    total_filtered_raw_posts = len(filtered_raw_posts_df)
    prop_likes_matched = total_filtered_raw_posts / total_likes
    logger.info(
        f"""(Partition date: {partition_date}): Loaded {total_likes} likes, {total_raw_posts} raw posts, and matched {total_filtered_raw_posts} raw posts to likes.
        Proportion of likes matched: {prop_likes_matched}."""
    )
    return filtered_raw_posts_df


# TODO: only need to write the URIs of the posts and then partition on synctimestamp,
# so that we can load the posts from 'raw_sync' and filter them by the URIs from
# the 'get_posts_liked_by_study_users' service. Since they'll be partitioned by
# the same field (synctimestamp), this should work.
def get_and_export_liked_posts_for_partition_date(partition_date: str):
    """Get and export the liked posts for a given partition date."""

    raw_posts_liked_by_study_users_df = load_raw_posts_for_likes_from_partition_date(
        partition_date=partition_date,
    )
    logger.info(
        f"(Partition date: {partition_date}): Loaded {len(raw_posts_liked_by_study_users_df)} raw posts liked by users on {partition_date}."
    )
    export_data_to_local_storage(
        df=raw_posts_liked_by_study_users_df,
        service=service_name,
        directory="active",
        partition_date=partition_date,
    )
    logger.info(
        f"(Partition date: {partition_date}): Exported {len(raw_posts_liked_by_study_users_df)} raw posts to {service_name}."
    )


def get_and_export_liked_posts_for_partition_dates(
    start_date: str = "2024-09-28",
    end_date: str = "2025-12-01",
    exclude_partition_dates: list[str] = ["2024-10-08"],
):
    """Get and export the liked posts for a given partition date."""
    partition_dates = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )
    for partition_date in partition_dates:
        get_and_export_liked_posts_for_partition_date(partition_date)


if __name__ == "__main__":
    likes = load_likes_for_lookback_range(
        lookback_start_date="2024-09-28",
        lookback_end_date="2024-12-01",
    )
    # TODO: get the URIs and export, then load these into `backfill_endpoint_worker`
    # and then use that to filter the records.
    breakpoint()
