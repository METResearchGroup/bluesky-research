"""Maps user session logs to feeds.

Logic:
1. Finds the latest feed before the user session log timestamp.
2. Maps the user session log to the feed.
3. If it can't find a user's feed before the session log timestamp, that means
that the user was seeing the default feed. Find the latest default feed before
the session log timestamp and map the user session log to that feed.
"""

import json
import os

import pandas as pd

from lib.helper import track_performance

current_dir = os.path.dirname(os.path.abspath(__file__))


def get_sorted_feed_ids(feeds: pd.DataFrame) -> dict[str, list[str]]:
    """Returns a dictionary mapping user to their sorted feed IDs.

    Args:
        feeds: DataFrame containing feed data with 'user' and 'feed_id' columns

    Returns:
        Dictionary mapping user to list of feed IDs sorted ascending
    """
    return {
        user: group["feed_id"].sort_values(ascending=True).tolist()
        for user, group in feeds.groupby("user")
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
    user_feed_ids = map_user_to_sorted_feed_ids[user]
    latest_feed_id = None
    for feed_id in user_feed_ids:
        feed_timestamp = get_timestamp_from_feed_id(feed_id)
        if feed_timestamp <= timestamp:
            latest_feed_id = feed_id
        else:
            break
    return latest_feed_id


def enrich_user_session_logs_with_feed_ids(
    user_session_logs: pd.DataFrame,
    map_user_to_sorted_feed_ids: dict[str, list[str]],
) -> pd.DataFrame:
    """Enrich the user session logs with feed IDs, if they don't have
    the feed IDs.

    The latest user session logs will have the feed IDs, but some of the older
    ones will not. For the ones that do not, we need to map them to the correct
    feed ID.
    """
    # Split into logs with and without feed IDs
    logs_with_feed_ids = user_session_logs[user_session_logs["feed_id"].notna()]
    logs_without_feed_ids = user_session_logs[user_session_logs["feed_id"].isna()]

    print(f"Logs with feed IDs: {logs_with_feed_ids.shape[0]}")
    print(f"Logs without feed IDs: {logs_without_feed_ids.shape[0]}")

    # For logs without feed IDs, find the latest feed before each log timestamp
    mapped_feed_ids: list[str] = []
    for idx, (_, row) in enumerate(logs_without_feed_ids.iterrows()):
        if idx % 1000 == 0:
            print(f"Processing log {idx} of {logs_without_feed_ids.shape[0]}")
        user = row["user_did"]
        timestamp = row["timestamp"]

        # Case 1: If the user has no feeds, they've been seeing the default feed.
        # Map them to the latest default feed before their login timestamp.
        if user not in map_user_to_sorted_feed_ids:
            print(f"User {user} has no feeds")
            latest_feed_id = get_latest_feed_id_for_user(
                user="default",
                timestamp=timestamp,
                map_user_to_sorted_feed_ids=map_user_to_sorted_feed_ids,
            )
            mapped_feed_ids.append(latest_feed_id)
        else:
            # Case 2: (generic case) Find the latest feed before the user session
            # log timestamp.
            latest_feed_id = get_latest_feed_id_for_user(
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
                print(f"No feed found for user {user} before timestamp {timestamp}")
                latest_feed_id = get_latest_feed_id_for_user(
                    user="default",
                    timestamp=timestamp,
                    map_user_to_sorted_feed_ids=map_user_to_sorted_feed_ids,
                )
                mapped_feed_ids.append(latest_feed_id)

    logs_without_feed_ids["feed_id"] = mapped_feed_ids

    # Combine the dataframes
    enriched_logs = pd.concat([logs_with_feed_ids, logs_without_feed_ids])

    return enriched_logs


def impute_missing_default_feeds(
    user_session_logs: pd.DataFrame,
    feeds: pd.DataFrame,
) -> pd.DataFrame:
    """Imputes missing default feeds for user session logs.

    For these, it'll be denoted with a "backfill" cursor. We want to impute
    what the feed would have been if we had the actual feed.
    """
    backfilled_user_session_logs = user_session_logs[
        user_session_logs["cursor"] == "backfill"
    ]
    regular_user_session_logs = user_session_logs[
        user_session_logs["cursor"] != "backfill"
    ]
    print(f"Backfilled user session logs: {backfilled_user_session_logs.shape[0]}")
    print(f"Regular user session logs: {regular_user_session_logs.shape[0]}")

    default_feeds = feeds[feeds["user"] == "default"]
    map_default_feed_id_to_feed = {
        row["feed_id"]: json.loads(row["feed"]) for _, row in default_feeds.iterrows()
    }

    backfilled_feeds = []

    for idx, (_, row) in enumerate(backfilled_user_session_logs.iterrows()):
        if idx % 1000 == 0:
            print(f"Processing log {idx} of {backfilled_user_session_logs.shape[0]}")
        feed_id = row["feed_id"]
        feed = map_default_feed_id_to_feed[feed_id]
        backfilled_feeds.append(feed)

    backfilled_user_session_logs["feed"] = backfilled_feeds
    combined_user_session_logs = pd.concat(
        [regular_user_session_logs, backfilled_user_session_logs]
    )
    return combined_user_session_logs


@track_performance
def main():
    print("Starting enrichment of user session logs with feed IDs")
    feeds: pd.DataFrame = pd.read_parquet(
        os.path.join(current_dir, "consolidated_feeds.parquet")
    )
    user_session_logs: pd.DataFrame = pd.read_parquet(
        os.path.join(current_dir, "consolidated_user_session_logs.parquet")
    )
    # restrict user session logs to starting 09/29 UTC time.
    user_session_logs = user_session_logs[
        user_session_logs["timestamp"] >= "2024-09-29"
    ]

    map_user_to_sorted_feed_ids: dict[str, list[str]] = get_sorted_feed_ids(feeds)  # noqa
    mapped_user_session_logs: pd.DataFrame = enrich_user_session_logs_with_feed_ids(
        user_session_logs, map_user_to_sorted_feed_ids
    )
    mapped_user_session_logs = impute_missing_default_feeds(
        mapped_user_session_logs, feeds
    )
    mapped_user_session_logs.to_parquet(
        os.path.join(current_dir, "mapped_user_session_logs.parquet")
    )
    print("Completed enrichment of user session logs with feed IDs")


if __name__ == "__main__":
    # fp = os.path.join(current_dir, "mapped_user_session_logs.parquet")
    # df = pd.read_parquet(fp)
    # breakpoint()
    main()
