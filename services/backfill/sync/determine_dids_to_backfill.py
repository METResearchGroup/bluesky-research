"""Script to determine DIDs to backfill, based on the data that currently
exist in the database."""

from lib.log.logger import get_logger
from services.backfill.sync.session_metadata import load_latest_backfilled_users

logger = get_logger(__name__)


def get_dids_from_posts() -> set[str]:
    dids_from_posts = set()
    logger.info(f"Total number of DIDs from posts: {len(dids_from_posts)}")
    return dids_from_posts


def get_dids_from_reposts() -> set[str]:
    dids_from_reposts = set()
    logger.info(f"Total number of DIDs from reposts: {len(dids_from_reposts)}")
    return dids_from_reposts


def get_dids_from_replies() -> set[str]:
    dids_from_replies = set()
    logger.info(f"Total number of DIDs from replies: {len(dids_from_replies)}")
    return dids_from_replies


def get_dids_from_likes() -> set[str]:
    dids_from_likes = set()
    logger.info(f"Total number of DIDs from likes: {len(dids_from_likes)}")
    return dids_from_likes


def get_dids_from_follows() -> set[str]:
    dids_from_follows = set()
    logger.info(f"Total number of DIDs from follows: {len(dids_from_follows)}")
    return dids_from_follows


def get_dids_to_backfill() -> set[str]:
    dids_from_posts: set[str] = get_dids_from_posts()
    dids_from_reposts: set[str] = get_dids_from_reposts()
    dids_from_replies: set[str] = get_dids_from_replies()
    dids_from_likes: set[str] = get_dids_from_likes()
    dids_from_follows: set[str] = get_dids_from_follows()

    dids_to_backfill: set[str] = (
        dids_from_posts
        | dids_from_reposts
        | dids_from_replies
        | dids_from_likes
        | dids_from_follows
    )
    return dids_to_backfill


def main():
    previously_backfilled_dids: list[dict] = load_latest_backfilled_users()
    logger.info(
        f"Total number of previously backfilled DIDs: {len(previously_backfilled_dids)}"
    )
    previously_backfilled_dids_set: set[str] = set(
        [user["did"] for user in previously_backfilled_dids]
    )

    dids_to_backfill: set[str] = get_dids_to_backfill()
    logger.info(
        f"Total number of DIDs to backfill (prior to filtering): {len(dids_to_backfill)}"
    )
    filtered_dids_to_backfill: set[str] = (
        dids_to_backfill - previously_backfilled_dids_set
    )
    logger.info(f"Total number of DIDs to backfill: {len(filtered_dids_to_backfill)}")


if __name__ == "__main__":
    main()
