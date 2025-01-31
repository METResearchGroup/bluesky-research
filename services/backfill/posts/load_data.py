"""Loading data for backfilling."""

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


def load_preprocessed_posts() -> list[dict]:
    """Load the preprocessed posts."""
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


def load_service_post_uris(service: str, id_field: str = "uri") -> set[str]:
    """Load the post URIs of all posts for a service."""
    query = f"SELECT {id_field} FROM {service}"
    logger.info(f"Loading {service} post URIs from local storage")
    cached_df: pd.DataFrame = load_data_from_local_storage(
        service=service,
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={"tables": [{"name": service, "columns": [id_field]}]},
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
def load_posts_to_backfill(integrations: list[str]) -> dict[str, dict]:
    """Given an integration, return the URIs of the posts to be backfilled.

    If no integration is provided, return dicts of the posts to be backfilled,
    mapped by the integration to backfill them for.

    For example:
    {
        "ml_inference_perspective_api": [
            {"uri": "123", "text": "..."},
            {"uri": "456", "text": "..."},
        ],
        "ml_inference_sociopolitical": [
            {"uri": "789", "text": "..."},
        ],
    }
    """
    integrations_to_backfill = INTEGRATIONS_LIST if not integrations else integrations
    total_posts: list[dict] = load_preprocessed_posts()
    posts_to_backfill_by_integration: dict[str, list[dict]] = {}
    for integration in integrations_to_backfill:
        integration_post_uris: set[str] = load_service_post_uris(service=integration)
        posts_to_backfill_by_integration[integration] = [
            post for post in total_posts if post["uri"] not in integration_post_uris
        ]
    return posts_to_backfill_by_integration
