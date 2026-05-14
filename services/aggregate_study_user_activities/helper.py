"""Helper service for fetching and processing all the study user activities
in one large table."""

from __future__ import annotations

from datetime import timedelta
import os

import pandas as pd

from lib.aws.athena import Athena
from lib.constants import current_datetime, current_datetime_str, partition_date_format
from lib.datetime_utils import get_partition_dates
from lib.db.manage_local_data import export_data_to_local_storage
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users

from services.aggregate_study_user_activities.aggregation_core import (
    author_did_series_for_follows,
    copy_dtypes_without_partition,
    finalize_activity_df,
    json_object_per_row,
)


default_lookback_days = 1

logger = get_logger(__name__)
athena = Athena()

users = get_all_users()
map_author_did_to_author_handle = {
    user.bluesky_user_did: user.bluesky_handle for user in users
}

cols_to_keep = [
    "author_did",
    "author_handle",
    "data_type",
    "data",
    "activity_timestamp",
]

# JSON keys for activity payloads (order is stable serialization order).
FOLLOW_ACTIVITY_JSON_COLUMNS = [
    "follow_did",
    "follow_handle",
    "follow_url",
    "follower_did",
    "follower_handle",
    "follower_url",
    "relationship_to_study_user",
]
POST_ACTIVITY_JSON_COLUMNS = [
    "uri",
    "cid",
    "indexed_at",
    "author_did",
    "author_handle",
    "created_at",
    "text",
    "embed",
    "entities",
    "facets",
    "labels",
    "langs",
    "reply_parent",
    "reply_root",
    "tags",
    "synctimestamp",
    "url",
    "source",
    "like_count",
    "reply_count",
    "repost_count",
]
SESSION_LOG_JSON_COLUMNS = ["cursor", "limit", "feed_length", "feed"]


def get_valid_partition_date_directory(
    local_prefix: str, partition_date: str
) -> str | None:
    """Return active or cache partition directory path, or None if missing."""
    expected_active_directory = os.path.join(
        local_prefix, "active", f"partition_date={partition_date}"
    )
    expected_cache_directory = os.path.join(
        local_prefix, "cache", f"partition_date={partition_date}"
    )

    if os.path.exists(expected_active_directory):
        return expected_active_directory
    if os.path.exists(expected_cache_directory):
        return expected_cache_directory
    return None


def _load_parquet_for_partition(
    local_prefix: str,
    partition_date: str,
    *,
    warn_missing_directory: str,
    warn_empty_parquet: str,
    dtypes_map: dict,
) -> pd.DataFrame:
    valid_directory = get_valid_partition_date_directory(local_prefix, partition_date)
    if not valid_directory:
        logger.warning(warn_missing_directory)
        return pd.DataFrame(columns=cols_to_keep)

    df: pd.DataFrame = pd.read_parquet(valid_directory)
    if len(df) == 0:
        logger.warning(warn_empty_parquet)
        return pd.DataFrame(columns=cols_to_keep)

    dtypes_trimmed = copy_dtypes_without_partition(dtypes_map)
    return df.astype(dtypes_trimmed)


def aggregate_latest_user_likes(partition_date: str) -> pd.DataFrame:
    """Collects the latest user likes for the given partition date."""
    df = _load_parquet_for_partition(
        MAP_SERVICE_TO_METADATA["study_user_likes"]["local_prefix"],
        partition_date,
        warn_missing_directory=(
            f"No valid user likes directory found for partition date: {partition_date}"
        ),
        warn_empty_parquet=f"No likes found for partition date: {partition_date}",
        dtypes_map=MAP_SERVICE_TO_METADATA["study_user_likes"]["dtypes_map"],
    )
    if df.empty:
        return df
    return finalize_activity_df(
        df,
        df["author"],
        df["author"].map(map_author_did_to_author_handle),  # type: ignore
        "like",
        df["record"],
        df["synctimestamp"],
        cols_to_keep,
    )


def aggregate_latest_user_follows(partition_date: str) -> pd.DataFrame:
    """Collects the latest user follows for the given partition date."""
    df = _load_parquet_for_partition(
        MAP_SERVICE_TO_METADATA["scraped_user_social_network"]["local_prefix"],
        partition_date,
        warn_missing_directory=(
            f"No valid user follows directory found for partition date: {partition_date}"
        ),
        warn_empty_parquet=(
            f"No user follows found for partition date: {partition_date}"
        ),
        dtypes_map=MAP_SERVICE_TO_METADATA["scraped_user_social_network"]["dtypes_map"],
    )
    if df.empty:
        return df

    author_did = author_did_series_for_follows(df)
    return finalize_activity_df(
        df,
        author_did,
        author_did.map(map_author_did_to_author_handle),  # type: ignore
        "follow",
        json_object_per_row(df, FOLLOW_ACTIVITY_JSON_COLUMNS),
        df["insert_timestamp"],
        cols_to_keep,
    )


# NOTE: we changed MAP_SERVICE_TO_METADATA to remove the "study_user_activity"
# key so this function (and anything referencing MAP_SERVICE_TO_METADATA["study_user_activity"])
# is currently deprecated.
def aggregate_latest_user_posts(partition_date: str) -> pd.DataFrame:
    """Collects the latest user posts for the given partition date."""
    df = _load_parquet_for_partition(
        MAP_SERVICE_TO_METADATA["study_user_activity"]["subpaths"]["post"],
        partition_date,
        warn_missing_directory=(
            f"No valid user posts directory found for partition date: {partition_date}"
        ),
        warn_empty_parquet=(
            f"No user posts found for partition date: {partition_date}"
        ),
        dtypes_map=MAP_SERVICE_TO_METADATA["study_user_activity"]["dtypes_map"],
    )
    if df.empty:
        return df
    return finalize_activity_df(
        df,
        df["author_did"],
        df["author_did"].map(map_author_did_to_author_handle),  # type: ignore
        "post",
        json_object_per_row(df, POST_ACTIVITY_JSON_COLUMNS),
        df["synctimestamp"],
        cols_to_keep,
    )


def aggregate_latest_user_likes_on_user_posts(partition_date: str) -> pd.DataFrame:
    """Collects the latest user likes on user posts for the given partition date."""
    df = _load_parquet_for_partition(
        MAP_SERVICE_TO_METADATA["study_user_like_on_user_post"]["local_prefix"],
        partition_date,
        warn_missing_directory=(
            f"No valid likes on user posts directory found for partition date: {partition_date}"
        ),
        warn_empty_parquet=(
            f"No likes on user posts found for partition date: {partition_date}"
        ),
        dtypes_map=MAP_SERVICE_TO_METADATA["study_user_like_on_user_post"][
            "dtypes_map"
        ],
    )
    if df.empty:
        return df
    return finalize_activity_df(
        df,
        df["author"],
        df["author"].map(map_author_did_to_author_handle),  # type: ignore
        "like_on_user_post",
        df["record"],
        df["synctimestamp"],
        cols_to_keep,
    )


def aggregate_latest_user_reply_to_user_posts(partition_date: str) -> pd.DataFrame:
    """Collects the latest user replies to user posts for the given partition date."""
    df = _load_parquet_for_partition(
        MAP_SERVICE_TO_METADATA["study_user_reply_to_user_post"]["local_prefix"],
        partition_date,
        warn_missing_directory=(
            f"No valid replies to user posts directory found for partition date: {partition_date}"
        ),
        warn_empty_parquet=(
            f"No replies to user posts found for partition date: {partition_date}"
        ),
        dtypes_map=MAP_SERVICE_TO_METADATA["study_user_reply_to_user_post"][
            "dtypes_map"
        ],
    )
    if df.empty:
        return df
    return finalize_activity_df(
        df,
        "",
        "",
        "reply_to_user_post",
        "",
        "",
        cols_to_keep,
    )


def aggregate_latest_user_session_logs(partition_date: str) -> pd.DataFrame:
    """Exports the latest user session logs for the given partition date."""
    query = f"SELECT * FROM user_session_logs WHERE partition_date = '{partition_date}'"
    df: pd.DataFrame = athena.query_results_as_df(query)

    if len(df) == 0:
        logger.warning(
            f"No user session logs found for partition date: {partition_date}"
        )
        return pd.DataFrame(columns=cols_to_keep)

    data = json_object_per_row(df, SESSION_LOG_JSON_COLUMNS)
    return finalize_activity_df(
        df,
        df["user_did"],
        df["user_did"].map(map_author_did_to_author_handle),  # type: ignore
        "user_session_log",
        data,
        df["timestamp"],
        cols_to_keep,
    )


def aggregate_latest_user_activities(partition_date: str) -> pd.DataFrame:
    """Collects the latest user activities for the given partition date."""
    latest_user_likes: pd.DataFrame = aggregate_latest_user_likes(partition_date)
    latest_user_follows: pd.DataFrame = aggregate_latest_user_follows(partition_date)
    latest_user_posts: pd.DataFrame = aggregate_latest_user_posts(partition_date)
    latest_user_likes_on_user_posts: pd.DataFrame = (
        aggregate_latest_user_likes_on_user_posts(partition_date)
    )
    latest_user_session_logs: pd.DataFrame = aggregate_latest_user_session_logs(
        partition_date
    )
    latest_activities: pd.DataFrame = pd.concat(
        [
            latest_user_likes,
            latest_user_follows,
            latest_user_posts,
            latest_user_likes_on_user_posts,
            latest_user_session_logs,
        ]
    )
    return latest_activities


def export_latest_user_activities(
    activities_df: pd.DataFrame,
    partition_date: str,
) -> None:
    """Exports the latest user activities for the given partition date."""
    activities_df["insert_timestamp"] = current_datetime_str
    activities_df["partition_date"] = partition_date
    dtypes_map = MAP_SERVICE_TO_METADATA["aggregated_study_user_activities"][
        "dtypes_map"
    ]
    activities_df = activities_df.astype(dtypes_map)
    logger.info(
        f"Exporting {len(activities_df)} rows to local storage for partition date = {partition_date}."
    )
    logger.info(
        f"Counts of each activity type: {activities_df['data_type'].value_counts()}"
    )
    export_data_to_local_storage(
        service="aggregated_study_user_activities",
        df=activities_df,
        export_format="parquet",
    )


def main():
    lookback_days = default_lookback_days
    end_date = (current_datetime - timedelta(days=1)).strftime(partition_date_format)
    start_date = (current_datetime - timedelta(days=lookback_days)).strftime(
        partition_date_format
    )
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=[],
    )[::-1]
    logger.info(f"Aggregating all user activities from dates: {partition_dates}")
    for partition_date in partition_dates:
        logger.info("*" * 10)
        logger.info(
            f"Aggregating latest user activities for partition date: {partition_date}"
        )
        latest_activities_df: pd.DataFrame = aggregate_latest_user_activities(
            partition_date
        )
        logger.info(
            f"Finished aggregating latest user activities for partition date: {partition_date}. Exporting..."
        )
        export_latest_user_activities(
            activities_df=latest_activities_df,
            partition_date=partition_date,
        )
        logger.info(
            f"Finished exporting latest user activities for partition date: {partition_date}"
        )
        logger.info("*" * 10)


if __name__ == "__main__":
    main()
