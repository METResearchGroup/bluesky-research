"""Loading data for backfilling."""

from typing import Optional

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage

INTEGRATIONS_LIST = [
    "ml_inference_perspective_api",
    "ml_inference_sociopolitical",
    "ml_inference_ime",
]


def load_service_post_uris(service: str, id_field: str = "uri") -> set[str]:
    """Load the post URIs of all posts for a service."""
    query = f"SELECT {id_field} FROM {service};"
    df: pd.DataFrame = load_data_from_local_storage(
        service=service,
        export_format="duckdb",
        duckdb_query=query,
    )
    return set(df[id_field])


def load_posts_to_backfill(integration: Optional[str] = None) -> dict[str, set[str]]:
    """Given an integration, return the URIs of the posts to be backfilled.

    If no integration is provided, return the URIs of all posts to be backfilled,
    mapped by the integration to backfill them for.
    """
    total_post_uris = load_service_post_uris(service="preprocessed_posts")
    integration_to_post_uris_map = {}
    for integration in INTEGRATIONS_LIST:
        integration_post_uris: set[str] = load_service_post_uris(integration)
        integration_to_post_uris_map[integration] = integration_post_uris
    posts_to_backfill_by_integration = {
        integration: total_post_uris - integration_post_uris
        for integration in INTEGRATIONS_LIST
    }
    return posts_to_backfill_by_integration
