"""Service for backfilling posts."""

from services.backfill.posts.load_data import load_posts_to_backfill


def backfill_posts(payload: dict):
    posts_to_backfill: dict[str, set[str]] = load_posts_to_backfill(
        payload.get("integration")
    )
    for integration, post_uris in posts_to_backfill.items():
        pass
