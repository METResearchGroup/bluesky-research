"""Loading data for backfilling."""

from typing import Literal, Optional

import pandas as pd

from lib.constants import (
    partition_date_format,
    timestamp_format,
)
from lib.datetime_utils import convert_bsky_dt_to_pipeline_dt
from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import track_performance
from lib.log.logger import get_logger

INTEGRATIONS_LIST = [
    "ml_inference_perspective_api",
    "ml_inference_sociopolitical",
    "ml_inference_ime",
    "ml_inference_valence_classifier",
    "ml_inference_intergroup",
]

default_table_columns = ["uri", "text", "preprocessing_timestamp"]

logger = get_logger(__file__)


def load_preprocessed_posts(
    start_date: str = "",
    end_date: str = "",
    sorted_by_partition_date: bool = False,
    ascending: bool = False,
    table_columns: Optional[list[str]] = None,
    output_format: Literal["list", "df"] = "list",
    convert_ts_fields: bool = False,
) -> list[dict] | pd.DataFrame:
    """Load the preprocessed posts.

    Args:
        start_date (str): Start date in YYYY-MM-DD format (inclusive)
        end_date (str): End date in YYYY-MM-DD format (inclusive)
        sorted_by_partition_date (bool): Whether to sort the posts by partition date
        ascending (bool): Whether to sort the posts in ascending order
        table_columns (Optional[list[str]]): Columns to load from the table
        output_format (Literal["list", "df"]): Format to return the data in
    """
    if not start_date or not end_date:
        raise ValueError("start_date and end_date must be provided")

    if table_columns is None:
        table_columns = (
            default_table_columns.copy()
        )  # Make a copy to avoid modifying the default

    # Only add partition_date if sorting and it's not already in columns
    if sorted_by_partition_date and "partition_date" not in table_columns:
        table_columns.append("partition_date")

    sort_direction = "ASC" if ascending else "DESC"
    sort_filter = (
        f"ORDER BY partition_date {sort_direction}" if sorted_by_partition_date else ""
    )

    table_columns_str = ", ".join(table_columns)
    query = (
        f"SELECT {table_columns_str} "
        f"FROM preprocessed_posts "
        f"WHERE text IS NOT NULL "
        f"AND text != '' "
        f"{sort_filter}"
    ).strip()

    cached_df: pd.DataFrame = load_data_from_local_storage(
        service="preprocessed_posts",
        directory="cache",
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={
            "tables": [{"name": "preprocessed_posts", "columns": table_columns}]
        },
        start_partition_date=start_date,
        end_partition_date=end_date,
    )
    logger.info(f"Loaded {len(cached_df)} posts from cache")
    active_df: pd.DataFrame = load_data_from_local_storage(
        service="preprocessed_posts",
        directory="active",
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={
            "tables": [{"name": "preprocessed_posts", "columns": table_columns}]
        },
        start_partition_date=start_date,
        end_partition_date=end_date,
    )
    logger.info(f"Loaded {len(active_df)} posts from active")
    df = pd.concat([cached_df, active_df]).drop_duplicates(subset="uri", keep="first")
    print(f"Columns in cached_df: {cached_df.columns}")
    print(f"Columns in active_df: {active_df.columns}")
    print(f"Columns in df: {df.columns}")
    print(f"Index names in cached_df: {cached_df.index.names}")
    print(f"Index names in active_df: {active_df.index.names}")
    print(f"Index names in df: {df.index.names}")
    logger.info(f"Loaded {len(df)} posts from cache and active")

    if convert_ts_fields:
        logger.info(
            "Converting timestamp fields to consolidated pipeline-friendly string formats."
        )
        for ts_col in ["partition_date", "preprocessing_timestamp"]:
            if ts_col in df.columns:  # Check if column exists
                if ts_col == "partition_date":
                    df[ts_col] = df[ts_col].dt.strftime(partition_date_format)
                else:
                    if pd.api.types.is_datetime64_any_dtype(df[ts_col]):
                        df[ts_col] = df[ts_col].dt.strftime(timestamp_format)
                    else:
                        df[ts_col] = df[ts_col].apply(convert_bsky_dt_to_pipeline_dt)
    if output_format == "list":
        return df.to_dict(orient="records")
    return df


def load_service_post_uris(
    service: str,
    id_field: str = "uri",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> set[str]:
    """Load the post URIs of all posts for a service.

    Args:
        service (str): Name of the service
        id_field (str): Name of the ID field to use
        start_date (Optional[str]): Start date in YYYY-MM-DD format (inclusive)
        end_date (Optional[str]): End date in YYYY-MM-DD format (inclusive)
    """
    query = f"SELECT {id_field} FROM {service}"
    logger.info(f"Loading {service} post URIs from local storage")
    cached_df: pd.DataFrame = load_data_from_local_storage(
        service=service,
        directory="cache",
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={"tables": [{"name": service, "columns": [id_field]}]},
        start_partition_date=start_date,
        end_partition_date=end_date,
    )
    logger.info(f"Loaded {len(cached_df)} post URIs from cache")
    active_df: pd.DataFrame = load_data_from_local_storage(
        service=service,
        directory="active",
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={"tables": [{"name": service, "columns": [id_field]}]},
        start_partition_date=start_date,
        end_partition_date=end_date,
    )
    logger.info(f"Loaded {len(active_df)} post URIs from active")
    df = pd.concat([cached_df, active_df]).drop_duplicates(
        subset=id_field, keep="first"
    )
    logger.info(f"Loaded {len(df)} unique post URIs from cache and active")
    return set(df[id_field])


@track_performance
def load_posts_to_backfill(
    integrations: list[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict[str, list[dict]]:
    """Given an integration, return the URIs of the posts to be backfilled.

    Args:
        integrations (list[str]): List of integrations to backfill for
        start_date (Optional[str]): Start date in YYYY-MM-DD format (inclusive)
        end_date (Optional[str]): End date in YYYY-MM-DD format (inclusive)

    Returns:
        dict[str, list[dict]]: Dictionary mapping valid integration names to lists of posts
    """
    # Filter out invalid integrations
    valid_integrations = [
        i for i in (integrations or INTEGRATIONS_LIST) if i in INTEGRATIONS_LIST
    ]

    if not valid_integrations:
        return {}  # Return empty dict if no valid integrations

    total_posts: list[dict] = load_preprocessed_posts(
        start_date=start_date,
        end_date=end_date,
        sorted_by_partition_date=False,
        ascending=False,
        table_columns=default_table_columns,
        output_format="list",
    )

    posts_to_backfill_by_integration: dict[str, list[dict]] = {}
    for integration in valid_integrations:
        integration_post_uris: set[str] = load_service_post_uris(
            service=integration,
            start_date=start_date,
            end_date=end_date,
        )
        posts_to_backfill_by_integration[integration] = [
            post for post in total_posts if post["uri"] not in integration_post_uris
        ]

    return posts_to_backfill_by_integration
