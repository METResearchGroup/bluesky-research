"""Loading data for backfilling."""

from typing import Optional

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import track_performance
from lib.log.logger import get_logger

INTEGRATIONS_LIST = [
    "ml_inference_perspective_api",
    "ml_inference_sociopolitical",
    "ml_inference_ime",
]

logger = get_logger(__file__)


def load_preprocessed_posts(
    start_date: Optional[str] = None, end_date: Optional[str] = None
) -> list[dict]:
    """Load the preprocessed posts.

    Args:
        start_date (Optional[str]): Start date in YYYY-MM-DD format (inclusive)
        end_date (Optional[str]): End date in YYYY-MM-DD format (inclusive)
    """
    query = """
        SELECT uri, text FROM preprocessed_posts
        WHERE text IS NOT NULL
        AND text != ''
    """

    cached_df: pd.DataFrame = load_data_from_local_storage(
        service="preprocessed_posts",
        directory="cache",
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={
            "tables": [{"name": "preprocessed_posts", "columns": ["uri", "text"]}]
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
            "tables": [{"name": "preprocessed_posts", "columns": ["uri", "text"]}]
        },
    )
    logger.info(f"Loaded {len(active_df)} posts from active")
    df = pd.concat([cached_df, active_df])
    logger.info(f"Loaded {len(df)} posts from cache and active")
    return df.to_dict(orient="records")


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
    )
    logger.info(f"Loaded {len(active_df)} post URIs from active")
    df = pd.concat([cached_df, active_df])
    logger.info(f"Loaded {len(df)} post URIs from cache and active")
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
    """
    integrations_to_backfill = INTEGRATIONS_LIST if not integrations else integrations
    total_posts: list[dict] = load_preprocessed_posts(
        start_date=start_date, end_date=end_date
    )
    posts_to_backfill_by_integration: dict[str, list[dict]] = {}
    for integration in integrations_to_backfill:
        integration_post_uris: set[str] = load_service_post_uris(
            service=integration, start_date=start_date, end_date=end_date
        )
        posts_to_backfill_by_integration[integration] = [
            post for post in total_posts if post["uri"] not in integration_post_uris
        ]
    return posts_to_backfill_by_integration
