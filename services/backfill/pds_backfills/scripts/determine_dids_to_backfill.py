"""Script to determine DIDs to backfill, based on the data that currently
exist in the database.

Currently what we want is:
- The DIDs of the posts that were reposted.
- The DIDs of the posts that were responded to.
- The DIDs of the posts that were liked.

We also have stubbed methods here for if we want to backfill posts or follows.
"""

import json
import os
import sqlite3

import pandas as pd

from lib.constants import study_start_date, study_end_date
from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger
from lib.db.manage_local_data import load_data_from_local_storage
from lib.db.queue import Queue
from lib.db.service_constants import MAP_SERVICE_TO_METADATA
from services.backfill.pds_backfills.core.constants import input_queue_name
from services.backfill.pds_backfills.storage.session_metadata import (
    load_latest_backfilled_users,
)
from transform.transform_raw_data import get_author_did_from_post_uri

logger = get_logger(__name__)

queue = Queue(queue_name=input_queue_name, create_new_queue=True)

subpaths: dict[str, str] = MAP_SERVICE_TO_METADATA["raw_sync"]["subpaths"]

current_dir = os.path.dirname(os.path.abspath(__file__))
sqlite_db_path = os.path.join(current_dir, "dids_to_backfill.sqlite")


def get_records(
    record_type: str,
    query: str,
    columns: list[str],
    start_date_inclusive: str,
    end_date_inclusive: str,
) -> pd.DataFrame:
    """Get records from raw_sync for a given record type."""
    custom_args = {"record_type": record_type}
    active_df = load_data_from_local_storage(
        service="raw_sync",
        storage_tiers=["active"],
        duckdb_query=query,
        query_metadata={"tables": [{"name": "raw_sync", "columns": columns}]},
        custom_args=custom_args,
        start_partition_date=start_date_inclusive,
        end_partition_date=end_date_inclusive,
    )
    cache_df = load_data_from_local_storage(
        service="raw_sync",
        storage_tiers=["cache"],
        duckdb_query=query,
        query_metadata={"tables": [{"name": "raw_sync", "columns": columns}]},
        custom_args=custom_args,
        start_partition_date=start_date_inclusive,
        end_partition_date=end_date_inclusive,
    )
    df = pd.concat([active_df, cache_df]).reset_index()
    return df


def get_dids_from_posts(
    start_date_inclusive: str,
    end_date_inclusive: str,
) -> set[str]:
    """Get DIDs from posts."""
    # NOTE: no need to implement yet, but stubbed here for future reference.
    # (e.g., if we want to backfill posts in the future).
    return set()


def get_dids_from_reposts(
    start_date_inclusive: str,
    end_date_inclusive: str,
) -> set[str]:
    """Get DIDs from reposts."""
    query = "SELECT subject FROM raw_sync"
    columns = ["subject"]
    reposts_df = get_records(
        record_type="repost",
        query=query,
        columns=columns,
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )

    # JSON-dumped dicts of the actual posts that were liked.
    logger.info(f"Total number of reposts loaded: {len(reposts_df)}")
    reposted_posts: list[str] = reposts_df["subject"].unique()
    uris_of_reposted_posts: list[str] = [
        json.loads(reposted_post)["uri"] for reposted_post in reposted_posts
    ]
    dids_of_reposted_posts: list[str] = [
        get_author_did_from_post_uri(post_uri) for post_uri in uris_of_reposted_posts
    ]
    dids_of_reposted_posts = set(dids_of_reposted_posts)
    logger.info(
        f"Total number of unique DIDs from posts that were reposted: {len(dids_of_reposted_posts)}"
    )

    return dids_of_reposted_posts


def get_dids_from_replies(
    start_date_inclusive: str,
    end_date_inclusive: str,
) -> set[str]:
    """Get DIDs from replies."""
    query = "SELECT reply FROM raw_sync"
    columns = ["reply"]
    replies_df = get_records(
        record_type="reply",
        query=query,
        columns=columns,
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )

    # JSON-dumped dicts of the actual posts that were liked.
    logger.info(f"Total number of replies loaded: {len(replies_df)}")
    replies: list[str] = replies_df["reply"].unique()

    # parent = post that a reply is replying to.
    # root = first post in the thread.
    # if a post is replying to one post, and that is the entire thread, then
    # parent == root.
    dids_of_parent_posts: set[str] = set()
    dids_of_root_posts: set[str] = set()

    for reply in replies:
        reply_dict = json.loads(reply)
        parent_post_uri = reply_dict["parent"]["uri"]
        parent_post_did = get_author_did_from_post_uri(parent_post_uri)
        root_post_uri = reply_dict["root"]["uri"]
        root_post_did = get_author_did_from_post_uri(root_post_uri)
        dids_of_parent_posts.add(parent_post_did)
        dids_of_root_posts.add(root_post_did)

    dids_of_posts_replied_to: set[str] = dids_of_parent_posts | dids_of_root_posts
    logger.info(
        f"Total number of unique DIDs from posts that were replied to: {len(dids_of_posts_replied_to)}"
    )

    return dids_of_posts_replied_to


def get_dids_from_likes(
    start_date_inclusive: str,
    end_date_inclusive: str,
) -> set[str]:
    """Get DIDs from likes."""
    query = "SELECT subject FROM raw_sync"
    columns = ["subject"]
    likes_df = get_records(
        record_type="like",
        query=query,
        columns=columns,
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )

    # JSON-dumped dicts of the actual posts that were liked.
    logger.info(f"Total number of likes loaded: {len(likes_df)}")
    liked_posts: list[str] = likes_df["subject"].unique()
    uris_of_liked_posts: list[str] = [
        json.loads(liked_post)["uri"] for liked_post in liked_posts
    ]
    dids_of_liked_posts: list[str] = [
        get_author_did_from_post_uri(post_uri) for post_uri in uris_of_liked_posts
    ]
    dids_of_liked_posts = set(dids_of_liked_posts)
    logger.info(
        f"Total number of unique DIDs from posts that were liked: {len(dids_of_liked_posts)}"
    )

    return dids_of_liked_posts


def get_dids_from_follows(
    start_date_inclusive: str,
    end_date_inclusive: str,
) -> set[str]:
    """Get DIDs from follows."""
    # NOTE: no need to implement yet, but stubbed here for future reference.
    # (e.g., if we want to backfill follows-of-follows in the future).
    return set()


def get_dids_to_backfill(
    start_date_inclusive: str = study_start_date,
    end_date_inclusive: str = study_end_date,
) -> set[str]:
    """Get DIDs to backfill."""
    dids_from_posts: set[str] = get_dids_from_posts(
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )
    dids_from_reposts: set[str] = get_dids_from_reposts(
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )
    dids_from_replies: set[str] = get_dids_from_replies(
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )
    dids_from_likes: set[str] = get_dids_from_likes(
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )
    dids_from_follows: set[str] = get_dids_from_follows(
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )

    dids_to_backfill: set[str] = (
        dids_from_posts
        | dids_from_reposts
        | dids_from_replies
        | dids_from_likes
        | dids_from_follows
    )

    logger.info(f"Total DIDs to backfill: {len(dids_to_backfill)}")
    return dids_to_backfill


def write_dids_to_local_db(dids: set[str]):
    """Write DIDs to local DB.

    Args:
        dids: Set of DIDs to write to the local SQLite database.
    """
    # Get the current directory and create the full path to the database

    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    cursor.execute("CREATE TABLE IF NOT EXISTS dids (did TEXT PRIMARY KEY)")

    # Insert the DIDs
    for did in dids:
        cursor.execute("INSERT OR REPLACE INTO dids (did) VALUES (?)", (did,))

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    logger.info(f"Wrote {len(dids)} DIDs to local database at {sqlite_db_path}")


def main(payload: dict):
    """Main function to determine DIDs to backfill.

    Loads previously backfilled users, loads all the DIDs in the database,
    and then returns the DIDs that have not been backfilled yet.
    """
    start_date = payload.get("start_date", study_start_date)
    end_date = payload.get("end_date", study_end_date)

    previously_backfilled_dids: list[dict] = load_latest_backfilled_users()

    logger.info(
        f"Total number of previously backfilled DIDs: {len(previously_backfilled_dids)}"
    )
    previously_backfilled_dids_set: set[str] = set(
        [user["did"] for user in previously_backfilled_dids]
    )

    dids_to_backfill: set[str] = get_dids_to_backfill(
        start_date_inclusive=start_date,
        end_date_inclusive=end_date,
    )
    logger.info(
        f"Total number of DIDs to backfill (prior to filtering): {len(dids_to_backfill)}"
    )
    filtered_dids_to_backfill: set[str] = (
        dids_to_backfill - previously_backfilled_dids_set
    )
    logger.info(f"Total number of DIDs to backfill: {len(filtered_dids_to_backfill)}")

    items = [{"did": did} for did in filtered_dids_to_backfill]

    write_dids_to_local_db(dids=filtered_dids_to_backfill)

    queue.batch_add_items_to_queue(items=items)

    metadata = {
        "total_dids_to_backfill": len(dids_to_backfill),
        "timestamp": generate_current_datetime_str(),
        "backfill_type": "sync",
    }

    logger.info(metadata)


if __name__ == "__main__":
    payload = {
        "start_date": study_start_date,
        "end_date": study_end_date,
    }
    main(payload=payload)
