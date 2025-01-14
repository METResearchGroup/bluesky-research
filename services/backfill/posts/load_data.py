"""Loading data for backfilling."""

from typing import Optional


def load_preprocessed_post_uris() -> set[str]:
    """Load the post URIs of all preprocessed posts.

    Returns a set of the post URIs, so they can be easily compared
    to other sets of URIs.
    """
    pass


def load_perspective_api_post_uris() -> set[str]:
    pass


def load_integration_post_uris(integration: str, id_field: str = "uri") -> set[str]:
    pass


def load_posts_to_backfill(integration: Optional[str] = None) -> dict[str, set[str]]:
    """Given an integration, return the URIs of the posts to be backfilled.

    If no integration is provided, return the URIs of all posts to be backfilled,
    mapped by the integration to backfill them for.
    """
    return {}
