"""Script to determine DIDs to backfill, based on the data that currently
exist in the database."""

import pandas as pd

from lib.constants import study_start_date, study_end_date
from lib.helper import generate_current_datetime_str
from lib.log.logger import get_logger
from lib.db.manage_local_data import load_data_from_local_storage
from lib.db.queue import Queue
from lib.db.service_constants import MAP_SERVICE_TO_METADATA

logger = get_logger(__name__)

queue_name = "input_backfill_sync"
queue = Queue(queue_name=queue_name, create_new_queue=True)

subpaths: dict[str, str] = MAP_SERVICE_TO_METADATA["raw_sync"]["subpaths"]


def get_dids(
    record_type: str,
    start_date_inclusive: str,
    end_date_inclusive: str,
) -> set[str]:
    """Get DIDs from raw_sync for a given record type."""
    custom_args = {"record_type": record_type}
    query = "SELECT did FROM raw_sync"
    active_df = load_data_from_local_storage(
        service="raw_sync",
        directory="active",
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={"tables": [{"name": "raw_sync", "columns": ["did"]}]},
        custom_args=custom_args,
        start_partition_date=start_date_inclusive,
        end_partition_date=end_date_inclusive,
    )
    cache_df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={"tables": [{"name": "raw_sync", "columns": ["did"]}]},
        custom_args=custom_args,
        start_partition_date=start_date_inclusive,
        end_partition_date=end_date_inclusive,
    )
    df = pd.concat([active_df, cache_df])
    dids = set(df["did"].unique())
    return dids


def get_dids_from_posts(
    start_date_inclusive: str,
    end_date_inclusive: str,
) -> set[str]:
    """Get DIDs from posts."""
    dids_from_posts = get_dids(
        record_type="post",
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )
    logger.info(f"Total number of DIDs from posts: {len(dids_from_posts)}")
    return dids_from_posts


def get_dids_from_reposts(
    start_date_inclusive: str,
    end_date_inclusive: str,
) -> set[str]:
    """Get DIDs from reposts."""
    dids_from_reposts = get_dids(
        record_type="repost",
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )
    logger.info(f"Total number of DIDs from reposts: {len(dids_from_reposts)}")
    return dids_from_reposts


def get_dids_from_replies(
    start_date_inclusive: str,
    end_date_inclusive: str,
) -> set[str]:
    """Get DIDs from replies."""
    dids_from_replies = get_dids(
        record_type="reply",
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )
    logger.info(f"Total number of DIDs from replies: {len(dids_from_replies)}")
    return dids_from_replies


def get_dids_from_likes(
    start_date_inclusive: str,
    end_date_inclusive: str,
) -> set[str]:
    """Get DIDs from likes."""
    dids_from_likes = get_dids(
        record_type="like",
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )
    logger.info(f"Total number of DIDs from likes: {len(dids_from_likes)}")
    return dids_from_likes


def get_dids_from_follows(
    start_date_inclusive: str,
    end_date_inclusive: str,
) -> set[str]:
    """Get DIDs from follows."""
    dids_from_follows = get_dids(
        record_type="follow",
        start_date_inclusive=start_date_inclusive,
        end_date_inclusive=end_date_inclusive,
    )
    logger.info(f"Total number of DIDs from follows: {len(dids_from_follows)}")
    return dids_from_follows


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
    return dids_to_backfill


def main(payload: dict):
    """Main function to determine DIDs to backfill.

    Loads previously backfilled users, loads all the DIDs in the database,
    and then returns the DIDs that have not been backfilled yet.
    """
    start_date = payload.get("start_date", study_start_date)
    end_date = payload.get("end_date", study_end_date)
    # previously_backfilled_dids: list[dict] = load_latest_backfilled_users()
    previously_backfilled_dids = []
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
    queue.batch_add_items_to_queue(items=items)

    metadata = {
        "total_dids_to_backfill": len(dids_to_backfill),
        "timestamp": generate_current_datetime_str(),
        "backfill_type": "sync",
    }

    logger.info(metadata)


if __name__ == "__main__":
    main()
