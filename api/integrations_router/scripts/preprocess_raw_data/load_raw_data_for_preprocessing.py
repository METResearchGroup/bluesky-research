"""Preprocesses raw sync data per day."""

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage


def load_posts_from_db(partition_date: str) -> pd.DataFrame:
    """Loads posts from the database, in the `raw_sync` path,
    for a given date."""
    posts_df = load_data_from_local_storage(
        service="raw_sync",
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


def main():
    """Preprocesses raw data for a given date range.

    Does the following:
    1. Loads the raw posts and replies for a given set of dates.
    2. Pushes them to the preprocess_raw_data input queue.
    """
    pass


if __name__ == "__main__":
    main()
