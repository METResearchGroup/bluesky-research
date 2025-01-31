"""Loading data for backfilling."""

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import track_performance

INTEGRATIONS_LIST = [
    "ml_inference_perspective_api",
    "ml_inference_sociopolitical",
    "ml_inference_ime",
]


def load_preprocessed_posts() -> list[dict]:
    """Load the preprocessed posts."""
    query = """
        SELECT uri, text FROM preprocessed_posts
        WHERE text IS NOT NULL
        AND text != ''
    """
    df: pd.DataFrame = load_data_from_local_storage(
        service="preprocessed_posts",
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={
            "tables": [{"name": "preprocessed_posts", "columns": ["uri", "text"]}]
        },
    )
    return df.to_dict(orient="records")


def load_service_post_uris(service: str, id_field: str = "uri") -> set[str]:
    """Load the post URIs of all posts for a service."""
    query = f"SELECT {id_field} FROM {service}"
    df: pd.DataFrame = load_data_from_local_storage(
        service=service,
        export_format="duckdb",
        duckdb_query=query,
        query_metadata={"tables": [{"name": service, "columns": [id_field]}]},
    )
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
