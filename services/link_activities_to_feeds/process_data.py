"""Processes the data that's loaded for mapping activities to feeds."""

from datetime import datetime, timedelta
import json

import pandas as pd

from lib.aws.dynamodb import DynamoDB
from lib.constants import timestamp_format
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.link_activities_to_feeds.constants import default_lookback_days

logger = get_logger(__name__)

dynamodb = DynamoDB()


@track_performance
def get_user_session_logs_with_feeds_shown(
    latest_study_user_activities: pd.DataFrame,
) -> tuple[pd.DataFrame, set[str]]:
    """Gets the user session logs with feeds shown, loaded from JSON."""
    user_session_logs = latest_study_user_activities[
        latest_study_user_activities["data_type"] == "user_session_log"
    ]
    user_session_log_data: list[dict] = user_session_logs["data"].apply(json.loads)

    # list of lists of dicts, corresponding to lists of feeds shown.
    feeds_shown: list[list[dict]] = [
        json.loads(data["feed"]) for data in user_session_log_data
    ]

    user_session_logs["feed_shown"] = feeds_shown
    user_session_logs["cursor"] = user_session_log_data.apply(lambda x: x["cursor"])

    uris_of_posts_shown_in_feeds: set[str] = set()
    for feed in feeds_shown:
        for post in feed:
            uris_of_posts_shown_in_feeds.add(post["post"])

    return user_session_logs, uris_of_posts_shown_in_feeds


@track_performance
def backfill_feed_generation_timestamp(
    user_session_logs_with_feeds: pd.DataFrame,
    lookback_days: int = default_lookback_days + 1,  # to have some buffer.
) -> list[str]:
    """Backfills the feed generation timestamp for user session logs.

    Fetches the latest feed generation timestamps from the metadata in DynamoDB
    and then checks to see which feed generation timestamp is the closest
    to the user session log's activity timestamp (but occurs before it).
    """

    start_timestamp = (datetime.now() - timedelta(days=lookback_days)).strftime(
        "%Y-%m-%d"
    )

    feed_generation_session_metadata: list[dict] = dynamodb.get_all_items_from_table(
        table_name="rank_score_feed_sessions"
    )

    filtered_feed_generation_session_metadata: list[dict] = [
        session_metadata
        for session_metadata in feed_generation_session_metadata
        if session_metadata["feed_generation_timestamp"]["S"] >= start_timestamp
    ]

    # sort metadata in ascending order.
    filtered_feed_generation_session_metadata.sort(
        key=lambda x: x["feed_generation_timestamp"]["S"]
    )  # noqa

    # sort user_session_logs_with_feeds by activity_timestamp
    df = user_session_logs_with_feeds.sort_values("activity_timestamp").reset_index(
        drop=True
    )

    df["feed_generation_timestamp"] = None

    # assign the feed generation timestamp to the user session log if the
    # activity timestamp is greater than or equal to the feed generation timestamp.
    # The latest feed generation timestamp is assigned to the user session log since
    # this is the version of the feed that the users saw.
    for session_metadata in filtered_feed_generation_session_metadata:
        feed_generation_timestamp = session_metadata["feed_generation_timestamp"]["S"]
        df.loc[
            df["activity_timestamp"] >= feed_generation_timestamp,
            "feed_generation_timestamp",
        ] = feed_generation_timestamp

    return df["feed_generation_timestamp"].tolist()


def add_supplementary_fields_to_user_session_logs(
    user_session_logs_with_feeds: pd.DataFrame,
) -> pd.DataFrame:
    """Adds supplementary fields to user session logs."""
    if "feed_generation_timestamp" not in user_session_logs_with_feeds.columns:
        user_session_logs_with_feeds["feed_generation_timestamp"] = (
            backfill_feed_generation_timestamp(
                user_session_logs_with_feeds=user_session_logs_with_feeds
            )
        )

    if "set_of_post_uris_in_feed" not in user_session_logs_with_feeds.columns:
        user_session_logs_with_feeds["set_of_post_uris_in_feed"] = (
            user_session_logs_with_feeds[
                "feed_shown"
            ].apply(lambda x: set(post["post"] for post in x))
        )

    return user_session_logs_with_feeds


def assign_session_hash(timestamp: str, window_period_minutes: int) -> str:
    """Assigns a session hash to a user session log based on the timestamp.

    The hash will be a compound string of the date plus the window period
    number plus the bucket that the hour/minute combination falls into if
    the window period is used.

    For example, if the window period is 5 minutes, then a post from
    12:39am would be in the 8th bucket (between 12:35am and 12:40am).

    If the post is from 2024-10-01, then the hash would be
    2024_10_01_5_8, corresponding to:
        - 2024-10-01 (date)
        - 5 (window period in minutes)
        - 8 (bucket number)

    Note that this uses a fixed window period instead of a sliding window period.
    As a result, for example, in a 5 minute period, 12:39am and 12:41am will
    be in separate windows.
    """
    # Parse the timestamp into a datetime object
    dt = datetime.strptime(timestamp, timestamp_format)

    # Extract date components
    date_str = dt.strftime("%Y_%m_%d")

    # Calculate which bucket the time falls into based on window period
    minutes_since_midnight = dt.hour * 60 + dt.minute
    bucket_number = minutes_since_midnight // window_period_minutes

    # Combine components into hash string
    session_hash = f"{date_str}_{window_period_minutes}_{bucket_number}"

    return session_hash


def split_user_session_logs_by_sliding_window(
    user_session_logs_df: pd.DataFrame, window_period_minutes: int
) -> list[pd.DataFrame]:
    """Splits user session logs into multiple dataframes, each corresponding to a
    single sliding window period.

    Uses a sliding window approach where any rows within window_period_minutes of each
    other will be grouped together. For example, if window_period_minutes=5:
    - A row at 12:39 and a row at 12:42 will be grouped together since they're within 5 min
    - A row at 12:39 and a row at 12:45 will be in separate groups since they're 6 min apart

    Returns a list of dataframes, where each dataframe contains rows that fall within
    the same sliding window period.

    This makes the following assumptions:
    - The first user session log in a window is the first request for that feed
    (we have no way to 100% check this, but if there are multiple feed requests,
    it's reasonable to assume that the first one is the initial request and the
    others are paginated requests).
    - The last request in a series of paginated requests will have the eof
    cursor.

    We know that all feeds will be 4 unique feed requests long (feeds are
    guaranteed to have length of 100, and 4 requests are made to fetch all of
    them), so we can actually work backwards from the last request to piece
    together the full feed.

    (note: this only works if we have 4 requests per feed. We'll have to
    figure out a more clever way of tracking if the # of requests changes.
    One way of doing this is prepend future user session logs with 'start_',
    so we know which request was first).

    Otherwise, this approach is a reasonable shorthand.

    As a first pass, we can just iterate through each user session log and then
    stop when we hit the eof cursor, and assume that everything between eof
    cursors corresponds to the same feed. We need to do this within a sliding
    window, since not all users scroll through the end of their feeds. This
    does introduce an edge case of half a feed showing up in one window
    and the other half in another window, but that's probably fine and my
    testing shows that it's much more common for users to not scroll through
    their feeds completely (i.e., never hit the 'eof' cursor) than for this
    edge case to happen.

    So, within a given sliding window interval, we can assume that all of the
    user session logs before an eof cursor are part of the same feed.

    The algorithm works as follows:
    - Sort the user session logs by timestamp.
    - Initialize a current group with the first user session log.
    - Initialize a window start time with the timestamp of the first user session log.
    - For the user session logs within that window, we do the following:
        - If it's an eof cursor, we assume that everything up to and including
        that cursor is part of that same feed.
        - Otherwise, we add to the current group and wait for the next eof cursor.
    - Once we've iterated across that window, we treat whatever is left (if we
    haven't hit an eof cursor) as its own feed.
    - We then move our pointer to the next user session log outside of this
    window and repeat the process.
    """
    if len(user_session_logs_df) == 0:
        return []

    # Sort logs by timestamp
    sorted_logs = user_session_logs_df.sort_values("activity_timestamp").reset_index(
        drop=True
    )

    result_groups = []
    current_group = []
    current_window_start = None

    for idx, row in sorted_logs.iterrows():
        current_timestamp = datetime.strptime(
            row["activity_timestamp"], "%Y-%m-%d-%H:%M:%S"
        )

        # Start new window if this is first row or outside current window
        if current_window_start is None:
            current_window_start = current_timestamp
            current_group = [row]
            continue

        time_diff = (current_timestamp - current_window_start).total_seconds() / 60

        if time_diff <= window_period_minutes:
            # Within window - add to current group
            current_group.append(row)

            # If this is an eof cursor, save current group and start new one
            if "eof" in str(row["cursor"]):
                result_groups.append(pd.DataFrame(current_group))
                current_group = []

        else:
            # Outside window - save current group if not empty
            if current_group:
                result_groups.append(pd.DataFrame(current_group))

            # Start new window
            current_window_start = current_timestamp
            current_group = [row]

    # Add final group if not empty
    if current_group:
        result_groups.append(pd.DataFrame(current_group))

    return result_groups


def consolidate_paginated_user_session_logs(
    session_logs_df: pd.DataFrame,
) -> pd.DataFrame:
    """Consolidates (and dedupes) paginated requests for the same feed."""
    if len(session_logs_df) == 1:
        return session_logs_df
    else:
        start_timestamp = min(session_logs_df["activity_timestamp"])
        insert_timestamp = min(session_logs_df["insert_timestamp"])
        cursor = (
            max(session_logs_df["cursor"])
            if "eof" not in session_logs_df["cursor"].values
            else "eof"
        )
        full_feed = []
        seen_post_uris = set()

        # order session logs by activity timestamp ascending.
        session_logs_df = session_logs_df.sort_values("activity_timestamp").reset_index(
            drop=True
        )

        # loop through each session log and add the posts from the feed
        # to the full feed if the posts aren't in the seen post uris
        for feed in session_logs_df["feed_shown"]:
            for post in feed:
                if post["post"] not in seen_post_uris:
                    full_feed.append(post)
                    seen_post_uris.add(post["post"])

        # create a new dataframe that has the consolidated into
        res = {
            "author_did": max(
                session_logs_df["author_did"]
            ),  # should be 1 unique value anyways
            "author_handle": max(
                session_logs_df["author_handle"]
            ),  # should be 1 unique value anyways
            "data_type": "user_session_logs",
            "data": json.dumps(full_feed),
            "activity_timestamp": start_timestamp,
            "insert_timestamp": insert_timestamp,
            "feed_shown": full_feed,
            "cursor": cursor,
        }
        consolidated_paginated_session_log_df = pd.DataFrame([res])
        return consolidated_paginated_session_log_df


def consolidate_user_session_log_paginated_requests(
    user_session_logs_with_feeds: pd.DataFrame, window_period_minutes: int = 5
) -> pd.DataFrame:
    """Consolidates paginated requests for the same feed.

    Bluesky only fetches 30 results for a feed at a time, so if a user
    scrolls through far enough, they'll incur multiple requests for the same
    feed. This function collapses them together.

    It assumes that the requests for a user within a given window period are
    in fact paginated requests for the same feed. It dedupes those requests
    and then creates a single user session log that has the initial timestamp
    of the first request and then the full feed of posts shown (by concatenating
    the posts across multiple paginated requests).

    It does so by grouping the user session logs by user and then assigning each
    user session log row to a context window that's the size of the window
    period. Then it groups by that context window and concatenates the user session
    logs together. We assume that a user is unlikely to log into the app multiple
    times in a 2-minute window, for example, so this is a safe assumption. Plus
    we don't really want to count intervals that short as separate events
    anyways. Plus the posts that a person will see won't differ in a
    sufficiently short window.

    NOTE: I think I should implement this as a sliding window instead
    of a fixed window period.
    """
    output_session_logs: list[pd.DataFrame] = []

    # group by user, and then assign each user session log row to a context
    # window that's double the size of the window period. Then group by that
    # context window and concatenate the user session logs together.

    # iterate through each user and their session logs.
    for _, user_session_logs_df in user_session_logs_with_feeds.groupby("author_did"):
        # split up user session logs by sliding window approach
        split_user_session_logs: list[pd.DataFrame] = (
            split_user_session_logs_by_sliding_window(
                user_session_logs_df=user_session_logs_df,
                window_period_minutes=window_period_minutes,
            )
        )
        for session_logs_df in split_user_session_logs:
            consolidated_session_logs_df = consolidate_paginated_user_session_logs(
                session_logs_df
            )
            output_session_logs.append(consolidated_session_logs_df)
    logger.info(
        f"After consolidating paginated user session logs, there are {len(output_session_logs)} user session logs (out of {len(user_session_logs_with_feeds)} originally)."
    )  # noqa
    return pd.concat(output_session_logs, ignore_index=True)


def process_user_session_logs(
    user_session_logs_with_feeds: pd.DataFrame,
) -> pd.DataFrame:
    """Processes user session logs with feeds shown."""
    user_session_logs_with_feeds: pd.DataFrame = (
        consolidate_user_session_log_paginated_requests(
            user_session_logs_with_feeds=user_session_logs_with_feeds
        )
    )

    user_session_logs_with_feeds: pd.DataFrame = (
        add_supplementary_fields_to_user_session_logs(
            user_session_logs_with_feeds=user_session_logs_with_feeds
        )
    )

    return user_session_logs_with_feeds
