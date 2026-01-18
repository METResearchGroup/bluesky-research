"""Helper functions for getting author to average toxicity and outrage."""

from concurrent.futures import ThreadPoolExecutor
import os

import pandas as pd

from lib.log.logger import get_logger
from lib.db.manage_local_data import (
    load_data_from_local_storage,
    export_data_to_local_storage,
)

logger = get_logger(__file__)

service_name = "author_to_average_toxicity_outrage"


def get_author_to_average_toxicity_outrage(partition_date: str) -> pd.DataFrame:
    """Gets author to average toxicity and outrage for a given partition date.

    Returns a pandas dataframe with the following columns:
    - author_did: str
    - preprocessing_timestamp: str
    - total_labeled_posts: int
    - average_toxicity: float
    - average_outrage: float

    Three-phase algorithm:
    1. Load the labeled posts for the given partition date. Get the post URIs.
    2. Use the post URIs to get the preprocessed posts. Load the URI, author_did,
    and preprocessing_timestamp for the preprocessed posts.
    3. Join the two dataframes on the URI column and for each author_did,
    calculate the following:
        - author_did: str
        - preprocessing_timestamp: str (from preprocessed_posts)
        - total_labeled_posts: int
        - average_toxicity: float
        - average_outrage: float
    4. Return a pandas dataframe with the above columns.
    """
    ### 1. Load the labeled posts for the given partition date. Get the post URIs. ###
    # load labeled posts (here, Perspective API labels, since this is what has
    # the toxicity and outrage metrics).
    perspective_api_table_columns = [
        "uri",
        "text",
        "prob_toxic",
        "prob_moral_outrage",
    ]
    perspective_api_table_columns_str = ", ".join(perspective_api_table_columns)
    perspective_api_sort_filter = ""
    perspective_api_query = (
        f"SELECT {perspective_api_table_columns_str} "
        f"FROM ml_inference_perspective_api "
        f"WHERE text IS NOT NULL "
        f"AND text != '' "
        f"{perspective_api_sort_filter}"
    ).strip()
    perspective_api_query_metadata = {
        "tables": [
            {
                "name": "ml_inference_perspective_api",
                "columns": perspective_api_table_columns,
            }
        ]
    }
    perspective_api_labels_df: pd.DataFrame = load_data_from_local_storage(
        service="ml_inference_perspective_api",
        directory="cache",
        partition_date=partition_date,
        duckdb_query=perspective_api_query,
        query_metadata=perspective_api_query_metadata,
        export_format="duckdb",
    )
    # drop the text column.
    perspective_api_labels_df.drop(columns=["text"], inplace=True)

    perspective_api_labels_uris = set(perspective_api_labels_df["uri"])

    ### 2. Use the post URIs to get the preprocessed posts. Load the URI and author_did
    # for the preprocessed posts. ###

    # load preprocessed posts.
    preprocessed_posts_table_columns = [
        "author_did",
        "text",
        "uri",
        "preprocessing_timestamp",
    ]
    preprocessed_posts_table_columns_str = ", ".join(preprocessed_posts_table_columns)
    preprocessed_posts_sort_filter = ""
    preprocessed_posts_query = (
        f"SELECT {preprocessed_posts_table_columns_str} "
        f"FROM preprocessed_posts "
        f"WHERE text IS NOT NULL "
        f"AND text != '' "
        f"{preprocessed_posts_sort_filter}"
    ).strip()
    preprocessed_posts_query_metadata = {
        "tables": [
            {"name": "preprocessed_posts", "columns": preprocessed_posts_table_columns}
        ]
    }

    preprocessed_posts_df: pd.DataFrame = load_data_from_local_storage(
        service="preprocessed_posts",
        directory="cache",
        partition_date=partition_date,
        duckdb_query=preprocessed_posts_query,
        query_metadata=preprocessed_posts_query_metadata,
        export_format="duckdb",
    )
    # drop the text column and filter URIs.
    preprocessed_posts_df.drop(columns=["text"], inplace=True)
    preprocessed_posts_df = preprocessed_posts_df[
        preprocessed_posts_df["uri"].isin(perspective_api_labels_uris)
    ]

    # join the two dataframes on the URI column.
    joined_df = pd.merge(
        preprocessed_posts_df, perspective_api_labels_df, on="uri", how="inner"
    )

    # Add partition_date back since we're grouping by it but not loading it from DB
    joined_df["partition_date"] = partition_date

    # Handle preprocessing_timestamp - it should only come from preprocessed_posts
    if "preprocessing_timestamp_x" in joined_df.columns:
        joined_df.rename(
            columns={"preprocessing_timestamp_x": "preprocessing_timestamp"},
            inplace=True,
        )
    if "preprocessing_timestamp_y" in joined_df.columns:
        joined_df.drop(columns=["preprocessing_timestamp_y"], inplace=True)

    # Remove any duplicate rows based on all columns (in case of exact duplicates)
    joined_df.drop_duplicates(inplace=True)
    joined_df.reset_index(drop=True, inplace=True)

    # If no data after join, return empty dataframe with correct columns
    if len(joined_df) == 0:
        logger.warning(f"No data after join for partition date {partition_date}")
        return pd.DataFrame(
            columns=[
                "author_did",
                "preprocessing_timestamp",
                "average_toxicity",
                "average_outrage",
                "total_labeled_posts",
            ]
        )

    # group by author_did and partition_date, keeping the first preprocessing_timestamp
    # and calculating the average toxicity and outrage as well as the total number of posts.
    joined_df = (
        joined_df.groupby(["author_did", "partition_date"])
        .agg(
            {
                "prob_toxic": "mean",
                "prob_moral_outrage": "mean",
                "uri": "count",
                "preprocessing_timestamp": "first",  # Take the first preprocessing_timestamp for each group
            }
        )
        .reset_index()
    )
    joined_df.rename(columns={"uri": "total_labeled_posts"}, inplace=True)
    joined_df.rename(
        columns={
            "prob_toxic": "average_toxicity",
            "prob_moral_outrage": "average_outrage",
        },
        inplace=True,
    )
    joined_df.reset_index(drop=True, inplace=True)

    return joined_df


def get_and_export_author_to_average_toxicity_outrage_for_partition_date(
    partition_date: str,
):
    """Gets author to average toxicity and outrage for a given partition date
    and exports to local storage."""
    author_to_average_toxicity_outrage_df: pd.DataFrame = (
        get_author_to_average_toxicity_outrage(partition_date)
    )
    logger.info(
        f"(Partition date: {partition_date}): Loaded and calculated average toxicity and outrage for {len(author_to_average_toxicity_outrage_df)} authors on {partition_date}."
    )
    export_data_to_local_storage(
        service=service_name,
        df=author_to_average_toxicity_outrage_df,
        export_format="parquet",
    )
    logger.info(
        f"(Partition date: {partition_date}): Exported {len(author_to_average_toxicity_outrage_df)} authors to {service_name}."
    )


def get_and_export_daily_author_to_average_toxicity_outrage_sequential(
    partition_dates: list[str],
):
    """Sequential version of getting author to average toxicity and outrage per day,
    processing one partition date at a time."""
    completed = 0
    for partition_date in partition_dates:
        if completed % 10 == 0:
            logger.info(
                f"Processed {completed}/{len(partition_dates)} partition dates..."
            )
        try:
            get_and_export_author_to_average_toxicity_outrage_for_partition_date(
                partition_date
            )
        except Exception as e:
            logger.error(f"Error processing partition date {partition_date}: {e}")
        completed += 1

    logger.info(f"Processed {completed}/{len(partition_dates)} partition dates...")
    logger.info("Finished processing all partition dates.")


def get_and_export_daily_author_to_average_toxicity_outrage_parallel(
    partition_dates: list[str],
):
    """Parallel version of getting author to average toxicity and outrage per day,
    using multithreading with one thread per partition date."""
    max_workers = None
    if max_workers is None:
        max_workers = min(32, (os.cpu_count() or 1) + 4)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_partition_date = {
            executor.submit(
                get_and_export_author_to_average_toxicity_outrage_for_partition_date,
                (partition_date),
            ): partition_date
            for partition_date in partition_dates
        }
        completed = 0
        for future in future_to_partition_date:
            if completed % 10 == 0:
                logger.info(
                    f"Processed {completed}/{len(partition_dates)} partition dates..."
                )
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error in thread: {e}")
            completed += 1

    logger.info(f"Processed {completed}/{len(partition_dates)} partition dates...")
    logger.info("Finished processing all partition dates.")


def get_and_export_daily_author_to_average_toxicity_outrage(
    partition_dates: list[str], mode: str = "sequential"
):
    """Gets author to average toxicity and outrage per day for multiple partition dates.

    Args:
        partition_dates: List of partition dates to process
        mode: Processing mode - "sequential" (default) or "parallel"
    """
    if mode == "sequential":
        logger.info("Running in sequential mode (single-threaded)")
        get_and_export_daily_author_to_average_toxicity_outrage_sequential(
            partition_dates
        )
    elif mode == "parallel":
        logger.info("Running in parallel mode (multithreaded)")
        get_and_export_daily_author_to_average_toxicity_outrage_parallel(
            partition_dates
        )
    else:
        raise ValueError(f"Invalid mode '{mode}'. Must be 'sequential' or 'parallel'.")
