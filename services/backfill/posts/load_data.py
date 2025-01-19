"""Loading data for backfilling."""

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.helper import RUN_MODE, track_performance

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
        query_metadata={"tables": [{"name": service, "columns": [id_field]}]},
    )
    return set(df[id_field])


@track_performance
def load_posts_to_backfill(integrations: list[str]) -> dict[str, set[str]]:
    """Given an integration, return the URIs of the posts to be backfilled.

    If no integration is provided, return the URIs of all posts to be backfilled,
    mapped by the integration to backfill them for.
    """
    integrations_to_backfill = INTEGRATIONS_LIST if not integrations else integrations
    if RUN_MODE == "local":
        # skip tables that aren't available in local data
        tables_to_skip = ["ml_inference_ime"]
        integrations_to_backfill = [
            integration
            for integration in integrations_to_backfill
            if integration not in tables_to_skip
        ]

    total_post_uris: set[str] = load_service_post_uris(service="preprocessed_posts")
    posts_to_backfill_by_integration: dict[str, set[str]] = {}
    for integration in integrations_to_backfill:
        integration_post_uris: set[str] = load_service_post_uris(service=integration)
        posts_to_backfill_by_integration[integration] = (
            total_post_uris - integration_post_uris
        )
    return posts_to_backfill_by_integration
