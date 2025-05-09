"""Preprocesses raw sync data per day."""

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.db.queue import Queue
from lib.helper import get_partition_dates
from lib.log.logger import get_logger

logger = get_logger(__name__)

queue = Queue(queue_name="input_preprocess_raw_data", create_new_queue=True)


def load_posts_from_db(partition_date: str) -> pd.DataFrame:
    """Loads posts from the database, in the `raw_sync` path,
    for a given date."""
    posts_df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        partition_date=partition_date,
        custom_args={"record_type": "post"},
    )
    # Deduplicate posts based on URI to ensure we have unique records
    posts_df = posts_df.drop_duplicates(subset=["uri"], keep="first")
    return posts_df


def load_replies_from_db(partition_date: str) -> pd.DataFrame:
    """Loads replies from the database, in the `raw_sync` path,
    for a given date."""
    replies_df = load_data_from_local_storage(
        service="raw_sync",
        directory="cache",
        partition_date=partition_date,
        custom_args={"record_type": "reply"},
    )
    # Deduplicate replies based on URI to ensure we have unique records
    replies_df = replies_df.drop_duplicates(subset=["uri"], keep="first")
    return replies_df


def load_raw_posts_from_db_for_date(partition_date: str) -> pd.DataFrame:
    """Loads posts and replies from the database, in the `raw_sync` path,
    for a given date."""
    posts_df = load_posts_from_db(partition_date=partition_date)
    replies_df = load_replies_from_db(partition_date=partition_date)
    df = pd.concat([posts_df, replies_df])
    return df


def preprocess_raw_data_for_date(partition_date: str) -> pd.DataFrame:
    """Preprocesses raw data for a given date."""
    raw_posts: pd.DataFrame = load_raw_posts_from_db_for_date(
        partition_date=partition_date
    )
    return raw_posts


def insert_raw_data_into_queue(
    raw_posts: pd.DataFrame,
    partition_date: str,
) -> None:
    """Inserts raw data into the preprocess_raw_data input queue."""
    queue.batch_add_items_to_queue(
        items=raw_posts.to_dict(orient="records"),
        metadata={
            "partition_date": partition_date,
            "total_items": len(raw_posts),
        },
    )


def load_raw_data_to_preprocess_for_date_range(
    start_date: str, end_date: str
) -> pd.DataFrame:
    """Preprocesses raw data for a given date range."""
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
    )
    logger.info(f"Loading raw data from {start_date} to {end_date}")
    for partition_date in partition_dates:
        logger.info(f"Loading raw data for partition date: {partition_date}")
        raw_posts: pd.DataFrame = load_raw_posts_from_db_for_date(
            partition_date=partition_date
        )
        insert_raw_data_into_queue(raw_posts=raw_posts, partition_date=partition_date)
        logger.info(f"Inserted raw data for partition date: {partition_date}")
    logger.info(f"Finished loading raw data from {start_date} to {end_date}")


def main():
    """Preprocesses raw data for a given date range.

    Does the following:
    1. Loads the raw posts and replies for a given set of dates.
    2. Pushes them to the preprocess_raw_data input queue.
    """
    start_date = "2024-09-01"
    end_date = "2024-12-01"
    load_raw_data_to_preprocess_for_date_range(start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    main()
