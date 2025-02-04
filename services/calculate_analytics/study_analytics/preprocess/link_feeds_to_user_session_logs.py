"""Link feeds to user session logs."""

import json
from typing import Optional

import pandas as pd

from lib.db.manage_local_data import (
    export_data_to_local_storage,
    load_data_from_local_storage,
)
from lib.helper import calculate_start_end_date_for_lookback, get_partition_dates
from lib.log.logger import get_logger

num_days_lookback = 1
min_lookback_date = "2024-09-29"  # different than other `min_lookback_date` due to users not logging in until after feeds were generated.
default_start_date = "2024-09-29"
default_end_date = "2024-10-08"

logger = get_logger(__file__)


def load_user_session_logs_for_partition_date(partition_date: str) -> pd.DataFrame:
    """Load the user session logs for a given partition date."""
    user_session_logs_df: pd.DataFrame = load_data_from_local_storage(
        service="generated_user_session_logs",
        directory="cache",
        export_format="parquet",
        partition_date=partition_date,
    )
    logger.info(
        f"Loaded {len(user_session_logs_df)} user session logs for partition date {partition_date}"
    )
    return user_session_logs_df


def map_users_to_sorted_feed_ids(feeds_df: pd.DataFrame) -> dict[str, list[str]]:
    """Map users to their sorted feed IDs."""
    return {
        user: group["feed_id"].sort_values(ascending=True).tolist()
        for user, group in feeds_df.groupby("user")
    }


def get_timestamp_from_feed_id(feed_id: str) -> str:
    """Returns the timestamp from a feed ID.

    Example: "did:plc:nayl5gjinnc3qrws7dd2aht2::2024-09-30-12:33:47"
    Returns: "2024-09-30-12:33:47"
    """
    return feed_id.split("::")[1]


def get_latest_feed_id_for_user(
    user: str,
    timestamp: str,
    map_user_to_sorted_feed_ids: dict[str, list[str]],
) -> str:
    """Get the latest feed ID for a user before a given timestamp.

    If the user doesn't have any feeds before the timestamp, return None.
    """
    user_feed_ids = map_user_to_sorted_feed_ids[user]
    latest_feed_id = None
    for feed_id in user_feed_ids:
        feed_timestamp = get_timestamp_from_feed_id(feed_id)
        if feed_timestamp <= timestamp:
            latest_feed_id = feed_id
        else:
            break
    return latest_feed_id


def load_candidate_feeds_for_partition_date(partition_date: str) -> pd.DataFrame:
    """For a given partition date, load the candidate feeds that could've been
    shown.

    Given that we updated the feeds every few hours, we just need to load
    feeds from that day plus the previous day (to account for boundary
    conditions).
    """
    start_date, end_date = calculate_start_end_date_for_lookback(
        partition_date, num_days_lookback, min_lookback_date
    )
    feeds_df: pd.DataFrame = load_data_from_local_storage(
        service="generated_feeds",
        directory="cache",
        export_format="parquet",
        start_partition_date=start_date,
        end_partition_date=end_date,
    )
    logger.info(
        f"Loaded {len(feeds_df)} candidate feeds for partition date {partition_date}"
    )
    return feeds_df


def link_feeds_to_user_session_logs(
    user_session_logs_df: pd.DataFrame,
    candidate_feeds_df: pd.DataFrame,
) -> pd.DataFrame:
    """Link the feeds to the user session logs.

    Logic:
    1. For each user session log, find the latest feed before the user session log timestamp.
    If it exists, map the user session log to that feed.
    2. If it can't find a user's feed before the session log timestamp, that means
    that the user was seeing the default feed. Find the latest default feed before
    the session log timestamp and map the user session log to that feed.
    """
    logger.info(
        f"Linking {len(user_session_logs_df)} user session logs to {len(candidate_feeds_df)} candidate feeds"
    )

    logs_with_feed_ids_df: pd.DataFrame = user_session_logs_df[
        user_session_logs_df["feed_id"].notna()
    ]
    logs_without_feed_ids_df: pd.DataFrame = user_session_logs_df[
        user_session_logs_df["feed_id"].isna()
    ]

    logger.info(f"User session logs with feed IDs: {logs_with_feed_ids_df.shape[0]}")
    logger.info(
        f"User session logs without feed IDs: {logs_without_feed_ids_df.shape[0]}"
    )

    map_user_to_sorted_feed_ids: dict[str, list[str]] = map_users_to_sorted_feed_ids(
        feeds_df=candidate_feeds_df
    )

    # For logs without feed IDs, find the latest feed before each log timestamp
    mapped_feed_ids: list[str] = []
    for idx, (_, row) in enumerate(logs_without_feed_ids_df.iterrows()):
        if idx % 1000 == 0:
            logger.info(f"Processing log {idx} of {logs_without_feed_ids_df.shape[0]}")
        user = row["user_did"]
        timestamp = row["timestamp"]

        # Case 1: If the user has no feeds, they've been seeing the default feed.
        # Map them to the latest default feed before their login timestamp.
        if user not in map_user_to_sorted_feed_ids:
            logger.info(f"User {user} has no feeds")
            latest_feed_id: Optional[str] = get_latest_feed_id_for_user(
                user="default",
                timestamp=timestamp,
                map_user_to_sorted_feed_ids=map_user_to_sorted_feed_ids,
            )
            mapped_feed_ids.append(latest_feed_id)
        else:
            # Case 2: (generic case) Find the latest feed before the user session
            # log timestamp.
            latest_feed_id: Optional[str] = get_latest_feed_id_for_user(
                user=user,
                timestamp=timestamp,
                map_user_to_sorted_feed_ids=map_user_to_sorted_feed_ids,
            )
            if latest_feed_id:
                mapped_feed_ids.append(latest_feed_id)
            else:
                # Case 3: No feed found for user before their session log timestamp.
                # This means they were logging on before we made feeds for them.
                # That means that they saw the default feed.
                logger.info(
                    f"No feed found for user {user} before timestamp {timestamp}"
                )
                latest_feed_id: Optional[str] = get_latest_feed_id_for_user(
                    user="default",
                    timestamp=timestamp,
                    map_user_to_sorted_feed_ids=map_user_to_sorted_feed_ids,
                )
                mapped_feed_ids.append(latest_feed_id)

    logs_without_feed_ids_df["feed_id"] = mapped_feed_ids

    # Combine the dataframes
    enriched_logs_df = pd.concat([logs_with_feed_ids_df, logs_without_feed_ids_df])
    enriched_logs_df = enriched_logs_df.sort_values(by="timestamp", ascending=True)
    logger.info(f"Resulting linked dataframe has {len(enriched_logs_df)} rows")

    return enriched_logs_df


def impute_missing_default_feeds(
    user_session_logs: pd.DataFrame,
    feeds: pd.DataFrame,
) -> pd.DataFrame:
    """Imputes missing default feeds for user session logs.

    For these, it'll be denoted with a "backfill" cursor. We want to impute
    what the feed would have been if we had the actual feed.
    """
    backfilled_user_session_logs: pd.DataFrame = user_session_logs[
        user_session_logs["cursor"] == "backfill"
    ]
    regular_user_session_logs: pd.DataFrame = user_session_logs[
        user_session_logs["cursor"] != "backfill"
    ]
    logger.info(
        f"Backfilled user session logs: {backfilled_user_session_logs.shape[0]}"
    )
    logger.info(f"Regular user session logs: {regular_user_session_logs.shape[0]}")

    default_feeds: pd.DataFrame = feeds[feeds["user"] == "default"]
    map_default_feed_id_to_feed: dict[str, dict] = {
        row["feed_id"]: json.loads(row["feed"]) for _, row in default_feeds.iterrows()
    }

    backfilled_feeds: list[dict] = []

    for idx, (_, row) in enumerate(backfilled_user_session_logs.iterrows()):
        if idx % 1000 == 0:
            logger.info(
                f"Processing log {idx} of {backfilled_user_session_logs.shape[0]}"
            )
        feed_id = row["feed_id"]
        feed = map_default_feed_id_to_feed[feed_id]
        backfilled_feeds.append(feed)

    backfilled_user_session_logs["feed"] = backfilled_feeds
    combined_user_session_logs: pd.DataFrame = pd.concat(
        [regular_user_session_logs, backfilled_user_session_logs]
    )
    return combined_user_session_logs


def link_feeds_to_user_session_logs_for_partition_date(partition_date: str) -> None:
    """For a given partition date, link the feeds to the user session logs."""
    logger.info(
        f"Linking feeds to user session logs for partition date {partition_date}"
    )
    user_session_logs_df: pd.DataFrame = load_user_session_logs_for_partition_date(
        partition_date=partition_date
    )
    candidate_feeds_df: pd.DataFrame = load_candidate_feeds_for_partition_date(
        partition_date=partition_date
    )
    linked_df: pd.DataFrame = link_feeds_to_user_session_logs(
        user_session_logs_df=user_session_logs_df,
        candidate_feeds_df=candidate_feeds_df,
    )
    linked_df: pd.DataFrame = impute_missing_default_feeds(
        user_session_logs_df=linked_df,
        candidate_feeds_df=candidate_feeds_df,
    )

    export_data_to_local_storage(
        service="linked_feeds_to_user_session_logs",
        df=linked_df,
        export_format="parquet",
    )

    logger.info(
        f"Exported linked feeds to user session logs to local storage for partition date {partition_date}"
    )


def main(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    if start_date is None:
        start_date = default_start_date
    if end_date is None:
        end_date = default_end_date
    partition_dates = get_partition_dates(start_date, end_date)
    for partition_date in partition_dates:
        link_feeds_to_user_session_logs_for_partition_date(partition_date)
    logger.info(
        f"Completed linking feeds to user session logs for {start_date} to {end_date}"
    )


if __name__ == "__main__":
    main()
