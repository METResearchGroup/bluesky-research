"""Helper service for fetching and processing all the study user activities
in one large table."""

from datetime import timedelta
import json
import os

import pandas as pd

from lib.aws.athena import Athena
from lib.constants import current_datetime, current_datetime_str
from lib.db.manage_local_data import export_data_to_local_storage
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users


default_lookback_days = 1

logger = get_logger(__name__)
athena = Athena()

users = get_all_users()
map_author_did_to_author_handle = {
    user.bluesky_user_did: user.bluesky_handle for user in users
}

cols_to_keep = ["author_did", "author_handle", "data_type", "data", "activity_timestamp"]


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
        # raise ValueError(
        #     f"No valid partition date directory found for partition date: {partition_date}"
        # )
        return None

    return valid_directory


def aggregate_latest_user_likes(partition_date: str) -> pd.DataFrame:
    """Collects the latest user likes for the given partition date."""

    valid_directory = get_valid_partition_date_directory(
        local_prefix=MAP_SERVICE_TO_METADATA["study_user_likes"]["local_prefix"],
        partition_date=partition_date,
    )

    if not valid_directory:
        logger.warning(
            f"No valid user likes directory found for partition date: {partition_date}"
        )
        df = pd.DataFrame(columns=cols_to_keep)
        return df

    df: pd.DataFrame = pd.read_parquet(valid_directory)
    if len(df) == 0:
        logger.warning(
            f"No likes found for partition date: {partition_date}"
        )
        df = pd.DataFrame(columns=cols_to_keep)

    dtypes_map = MAP_SERVICE_TO_METADATA["study_user_likes"]["dtypes_map"]
    if "partition_date" in dtypes_map:
        dtypes_map.pop("partition_date")
    df = df.astype(dtypes_map)

    df["author_did"] = df["author"]
    df["author_handle"] = df["author"].map(map_author_did_to_author_handle)
    df["data_type"] = "like"
    df["data"] = df["record"]
    df["activity_timestamp"] = df["synctimestamp"]

    df = df[cols_to_keep]
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
        logger.warning(
            f"No valid user follows directory found for partition date: {partition_date}"
        )
        df = pd.DataFrame(columns=cols_to_keep)
        return df

    df: pd.DataFrame = pd.read_parquet(valid_directory)
    if len(df) == 0:
        logger.warning(
            f"No user follows found for partition date: {partition_date}"
        )
        df = pd.DataFrame(columns=cols_to_keep)

    # making a note to convert the dtypes here to make sure that there's no
    # implicit string -> complex dtype conversion happening
    dtypes_map = MAP_SERVICE_TO_METADATA["scraped_user_social_network"]["dtypes_map"]
    if "partition_date" in dtypes_map:
        dtypes_map.pop("partition_date")
    df = df.astype(dtypes_map)

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
    df["activity_timestamp"] = df["insert_timestamp"]

    df = df[cols_to_keep]
    return df


def aggregate_latest_user_posts(partition_date: str) -> pd.DataFrame:
    """Collects the latest user posts for the given partition date."""
    valid_directory = get_valid_partition_date_directory(
        local_prefix=MAP_SERVICE_TO_METADATA["study_user_activity"]["subpaths"]["post"],
        partition_date=partition_date,
    )

    if not valid_directory:
        logger.warning(
            f"No valid user posts directory found for partition date: {partition_date}"
        )
        df = pd.DataFrame(columns=cols_to_keep)
        return df

    df: pd.DataFrame = pd.read_parquet(valid_directory)
    if len(df) == 0:
        logger.warning(
            f"No user posts found for partition date: {partition_date}"
        )
        df = pd.DataFrame(columns=cols_to_keep)

    # making a note to convert the dtypes here to make sure that there's no
    # implicit string -> complex dtype conversion happening
    dtypes_map = MAP_SERVICE_TO_METADATA["study_user_activity"]["dtypes_map"]
    if "partition_date" in dtypes_map:
        dtypes_map.pop("partition_date")
    df = df.astype(dtypes_map)

    df["author_did"] = df["author_did"]
    df["author_handle"] = df["author_did"].map(map_author_did_to_author_handle)
    df["data_type"] = "post"
    df["data"] = df.apply(
        lambda row: json.dumps(
            {
                "uri": row["uri"] if pd.notna(row["uri"]) else None,
                "cid": row["cid"] if pd.notna(row["cid"]) else None,
                "indexed_at": row["indexed_at"] if pd.notna(row["indexed_at"]) else None,
                "author_did": row["author_did"] if pd.notna(row["author_did"]) else None,
                "author_handle": row["author_handle"] if pd.notna(row["author_handle"]) else None,
                "created_at": row["created_at"] if pd.notna(row["created_at"]) else None,
                "text": row["text"] if pd.notna(row["text"]) else None,
                "embed": row["embed"] if pd.notna(row["embed"]) else None,
                "entities": row["entities"] if pd.notna(row["entities"]) else None,
                "facets": row["facets"] if pd.notna(row["facets"]) else None,
                "labels": row["labels"] if pd.notna(row["labels"]) else None,
                "langs": row["langs"] if pd.notna(row["langs"]) else None,
                "reply_parent": row["reply_parent"] if pd.notna(row["reply_parent"]) else None,
                "reply_root": row["reply_root"] if pd.notna(row["reply_root"]) else None,
                "tags": row["tags"] if pd.notna(row["tags"]) else None,
                "synctimestamp": row["synctimestamp"] if pd.notna(row["synctimestamp"]) else None,
                "url": row["url"] if pd.notna(row["url"]) else None,
                "source": row["source"] if pd.notna(row["source"]) else None,
                "like_count": row["like_count"] if pd.notna(row["like_count"]) else None,
                "reply_count": row["reply_count"] if pd.notna(row["reply_count"]) else None,
                "repost_count": row["repost_count"] if pd.notna(row["repost_count"]) else None,
            }
        ),
        axis=1,
    )
    df["activity_timestamp"] = df["synctimestamp"]

    df = df[cols_to_keep]
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
        logger.warning(
            f"No valid likes on user posts directory found for partition date: {partition_date}"
        )
        df = pd.DataFrame(columns=cols_to_keep)
        return df

    df: pd.DataFrame = pd.read_parquet(valid_directory)
    if len(df) == 0:
        logger.warning(
            f"No likes on user posts found for partition date: {partition_date}"
        )
        df = pd.DataFrame(columns=cols_to_keep)
    
    # making a note to convert the dtypes here to make sure that there's no
    # implicit string -> complex dtype conversion happening
    dtypes_map = MAP_SERVICE_TO_METADATA["study_user_like_on_user_post"]["dtypes_map"]
    if "partition_date" in dtypes_map:
        dtypes_map.pop("partition_date")
    df = df.astype(dtypes_map)

    df["author_did"] = df["author"]
    df["author_handle"] = df["author"].map(map_author_did_to_author_handle)
    df["data_type"] = "like_on_user_post"
    df["data"] = df["record"]
    df["activity_timestamp"] = df["synctimestamp"]

    df = df[cols_to_keep]
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
        logger.warning(
            f"No valid replies to user posts directory found for partition date: {partition_date}"
        )
        df = pd.DataFrame(columns=cols_to_keep)
        return df

    df: pd.DataFrame = pd.read_parquet(valid_directory)

    if len(df) == 0:
        logger.warning(
            f"No replies to user posts found for partition date: {partition_date}"
        )
        df = pd.DataFrame(columns=cols_to_keep)

    dtypes_map = MAP_SERVICE_TO_METADATA["study_user_reply_to_user_post"]["dtypes_map"]
    df = df.astype(dtypes_map)

    df["author_did"] = ""
    df["author_handle"] = ""
    df["data_type"] = "reply_to_user_post"
    df["data"] = ""
    df["activity_timestamp"] = ""

    df = df[cols_to_keep]
    return df


def aggregate_latest_user_session_logs(partition_date: str) -> None:
    """Exports the latest user session logs for the given partition date."""
    query = f"SELECT * FROM user_session_logs WHERE partition_date = '{partition_date}'"
    df: pd.DataFrame = athena.query_results_as_df(query)

    if len(df) == 0:
        logger.warning(
            f"No user session logs found for partition date: {partition_date}"
        )
        df = pd.DataFrame(columns=cols_to_keep)

    df["author_did"] = df["user_did"]
    df["author_handle"] = df["user_did"].map(map_author_did_to_author_handle)
    df["data_type"] = "user_session_log"
    # NOTE: is this going to be too much all at once? Unsure, should be OK.
    # looks to be ~6MB for 2700 rows. For creating a "one big table" for
    # analytics, this is probably fine.
    df["data"] = df.apply(
        lambda row: json.dumps(
            {
                "cursor": row["cursor"] if pd.notna(row["cursor"]) else None,
                "limit": row["limit"] if pd.notna(row["limit"]) else None,
                "feed_length": row["feed_length"] if pd.notna(row["feed_length"]) else None,
                "feed": row["feed"] if pd.notna(row["feed"]) else None,
            }
        ),
        axis=1
    )
    df["activity_timestamp"] = df["timestamp"]

    df = df[cols_to_keep]
    return df


def aggregate_latest_user_activities(partition_date: str) -> pd.DataFrame:
    """Collects the latest user activities for the given partition date."""
    latest_user_likes: pd.DataFrame = aggregate_latest_user_likes(partition_date)
    latest_user_follows: pd.DataFrame = aggregate_latest_user_follows(partition_date)
    latest_user_posts: pd.DataFrame = aggregate_latest_user_posts(partition_date)
    latest_user_likes_on_user_posts: pd.DataFrame = aggregate_latest_user_likes_on_user_posts(
        partition_date
    )
    latest_user_session_logs: pd.DataFrame = aggregate_latest_user_session_logs(partition_date)
    # latest_user_reply_to_user_posts = aggregate_latest_user_reply_to_user_posts(
    #     partition_date
    # )
    latest_activities: pd.DataFrame = pd.concat(
        [
            latest_user_likes,
            latest_user_follows,
            latest_user_posts,
            latest_user_likes_on_user_posts,
            latest_user_session_logs,
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
    partition_dates: list[str] = generate_partition_dates(
        lookback_days=lookback_days
    )
    logger.info(f"Aggregating all user activities from dates: {partition_dates}")
    breakpoint()
    for partition_date in partition_dates:
        logger.info("*" * 10)
        logger.info(
            f"Aggregating latest user activities for partition date: {partition_date}"
        )
        latest_activities_df: pd.DataFrame = aggregate_latest_user_activities(partition_date)
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
