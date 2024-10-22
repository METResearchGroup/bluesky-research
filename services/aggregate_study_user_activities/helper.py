"""Helper service for fetching and processing all the study user activities
in one large table."""

from datetime import timedelta
import json
import os

import pandas as pd

from lib.constants import current_datetime, current_datetime_str
from lib.db.manage_local_data import export_data_to_local_storage
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users


default_lookback_days = 1

logger = get_logger(__name__)

users = get_all_users()
map_author_did_to_author_handle = {
    user.bluesky_user_did: user.bluesky_handle for user in users
}


def generate_partition_dates(lookback_days: int = default_lookback_days) -> list[str]:
    """Generates the partition dates for the given lookback days."""
    partition_dates = []
    # exclude current day
    for i in range(1, lookback_days + 1):
        partition_date = (current_datetime - timedelta(days=i)).strftime("%Y-%m-%d")
        partition_dates.append(partition_date)
    return partition_dates


def get_valid_partition_date_directory(local_prefix: str, partition_date: str) -> str:
    """Gets the valid partition date directory for the given partition date.

    First, checks the 'active' directory to see if the expected partition date
    is there, and if not, checks the 'cache' directory.
    """
    expected_active_directory = os.path.join(
        local_prefix, "active", f"partition_date={partition_date}"
    )
    expected_cache_directory = os.path.join(
        local_prefix, "cache", f"partition_date={partition_date}"
    )

    if os.path.exists(expected_active_directory):
        valid_directory = expected_active_directory
    elif os.path.exists(expected_cache_directory):
        valid_directory = expected_cache_directory
    else:
        raise ValueError(
            f"No valid partition date directory found for partition date: {partition_date}"
        )

    return valid_directory


def aggregate_latest_user_likes(partition_date: str) -> pd.DataFrame:
    """Collects the latest user likes for the given partition date."""

    valid_directory = get_valid_partition_date_directory(
        local_prefix=MAP_SERVICE_TO_METADATA["study_user_likes"]["local_prefix"],
        partition_date=partition_date,
    )

    if not valid_directory:
        raise ValueError(
            f"No valid user likes directory found for partition date: {partition_date}"
        )

    df: pd.DataFrame = pd.read_parquet(valid_directory)
    dtypes_map = MAP_SERVICE_TO_METADATA["study_user_likes"]["dtypes_map"]
    df = df.astype(dtypes_map)

    df["author_did"] = df["author"]
    df["author_handle"] = df["author"].map(map_author_did_to_author_handle)
    df["data_type"] = "like"
    df["data"] = df["record"]
    df["activity_timestamp"] = df["synctimestamp"]
    return df


def aggregate_latest_user_follows(partition_date: str) -> pd.DataFrame:
    """Collects the latest user follows for the given partition date."""
    valid_directory = get_valid_partition_date_directory(
        local_prefix=MAP_SERVICE_TO_METADATA["scraped_user_social_network"][
            "local_prefix"
        ],
        partition_date=partition_date,
    )

    if not valid_directory:
        raise ValueError(
            f"No valid user follows directory found for partition date: {partition_date}"
        )

    df: pd.DataFrame = pd.read_parquet(valid_directory)
    # TODO: add back in. Missing "partition_date" column? Probably uncompacted local data TBH.
    # TODO: should test on Quest, which will have the partition dates.
    # dtypes_map = MAP_SERVICE_TO_METADATA["scraped_user_social_network"]["dtypes_map"]
    # df = df.astype(dtypes_map)

    df["author_did"] = df.apply(
        lambda row: row["follow_did"]
        if row["relationship_to_study_user"] == "follower"
        else row["follower_did"],
        axis=1,
    )
    df["author_handle"] = df["author_did"].map(map_author_did_to_author_handle)
    df["data_type"] = "follow"
    df["data"] = df.apply(
        lambda row: json.dumps(
            {
                "follow_did": row["follow_did"]
                if pd.notna(row["follow_did"])
                else None,
                "follow_handle": row["follow_handle"]
                if pd.notna(row["follow_handle"])
                else None,
                "follow_url": row["follow_url"]
                if pd.notna(row["follow_url"])
                else None,
                "follower_did": row["follower_did"]
                if pd.notna(row["follower_did"])
                else None,
                "follower_handle": row["follower_handle"]
                if pd.notna(row["follower_handle"])
                else None,
                "follower_url": row["follower_url"]
                if pd.notna(row["follower_url"])
                else None,
                "relationship_to_study_user": row["relationship_to_study_user"],
            }
        ),
        axis=1,
    )
    df["activity_timestamp"] = df["synctimestamp"]
    return df


def aggregate_latest_user_posts(partition_date: str) -> pd.DataFrame:
    """Collects the latest user posts for the given partition date."""
    valid_directory = get_valid_partition_date_directory(
        local_prefix=MAP_SERVICE_TO_METADATA["study_user_activity"]["local_prefix"],
        partition_date=partition_date,
    )

    if not valid_directory:
        raise ValueError(
            f"No valid user posts directory found for partition date: {partition_date}"
        )

    df: pd.DataFrame = pd.read_parquet(valid_directory)
    dtypes_map = MAP_SERVICE_TO_METADATA["study_user_activity"]["dtypes_map"]
    df = df.astype(dtypes_map)

    df["author_did"] = ""
    df["author_handle"] = ""
    df["data_type"] = "post"
    df["data"] = ""
    df["activity_timestamp"] = ""
    return df


def aggregate_latest_user_likes_on_user_posts(partition_date: str) -> pd.DataFrame:
    """Collects the latest user likes on user posts for the given partition date."""
    valid_directory = get_valid_partition_date_directory(
        local_prefix=MAP_SERVICE_TO_METADATA["study_user_like_on_user_post"][
            "local_prefix"
        ],
        partition_date=partition_date,
    )

    if not valid_directory:
        raise ValueError(
            f"No valid likes on user posts directory found for partition date: {partition_date}"
        )

    df: pd.DataFrame = pd.read_parquet(valid_directory)
    dtypes_map = MAP_SERVICE_TO_METADATA["study_user_like_on_user_post"]["dtypes_map"]
    df = df.astype(dtypes_map)

    df["author_did"] = ""
    df["author_handle"] = ""
    df["data_type"] = "like_on_user_post"
    df["data"] = ""
    df["activity_timestamp"] = ""
    return df


# TODO: these still need to be migrated and compacted I think, and then
# also added to compaction and data snapshot logics.
def aggregate_latest_user_reply_to_user_posts(partition_date: str) -> pd.DataFrame:
    """Collects the latest user replies to user posts for the given partition date."""
    valid_directory = get_valid_partition_date_directory(
        local_prefix=MAP_SERVICE_TO_METADATA["study_user_reply_to_user_post"][
            "local_prefix"
        ],
        partition_date=partition_date,
    )

    if not valid_directory:
        raise ValueError(
            f"No valid replies to user posts directory found for partition date: {partition_date}"
        )

    df: pd.DataFrame = pd.read_parquet(valid_directory)
    dtypes_map = MAP_SERVICE_TO_METADATA["study_user_reply_to_user_post"]["dtypes_map"]
    df = df.astype(dtypes_map)

    df["author_did"] = ""
    df["author_handle"] = ""
    df["data_type"] = "reply_to_user_post"
    df["data"] = ""
    df["activity_timestamp"] = ""
    return df


def aggregate_latest_user_activities(partition_date: str) -> pd.DataFrame:
    """Collects the latest user activities for the given partition date."""
    latest_user_likes = aggregate_latest_user_likes(partition_date)
    latest_user_follows = aggregate_latest_user_follows(partition_date)
    latest_user_posts = aggregate_latest_user_posts(partition_date)
    latest_user_likes_on_user_posts = aggregate_latest_user_likes_on_user_posts(
        partition_date
    )
    # latest_user_reply_to_user_posts = aggregate_latest_user_reply_to_user_posts(
    #     partition_date
    # )
    latest_activities = pd.concat(
        [
            latest_user_likes,
            latest_user_follows,
            latest_user_posts,
            latest_user_likes_on_user_posts,
            # latest_user_reply_to_user_posts,
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
    export_data_to_local_storage(
        service="aggregated_study_user_activities",
        df=activities_df,
        export_format="parquet",
    )


def main():
    partition_dates: list[str] = generate_partition_dates(
        lookback_days=default_lookback_days
    )
    partition_dates = ["2024-10-10"]  # TODO: remove.
    for partition_date in partition_dates:
        logger.info("*" * 10)
        logger.info(
            f"Aggregating latest user activities for partition date: {partition_date}"
        )
        latest_activities_df = aggregate_latest_user_activities(partition_date)
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
