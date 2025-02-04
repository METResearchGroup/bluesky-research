"""Backfill pipeline for posts that were used in feeds."""

from api.integrations_router.main import route_and_run_integration_request
from lib.db.queue import Queue
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.backfill.posts.load_data import INTEGRATIONS_LIST
from services.backfill.posts_used_in_feeds.helper import (
    backfill_posts_used_in_feed_for_partition_dates,
)


logger = get_logger(__file__)


# TODO: Add args, like in "backfill_posts"
@track_performance
def backfill_posts_used_in_feeds(payload: dict):
    """Backfill pipeline for posts that were used in feeds.

    The logic is as follows:
    - For each day, we load in the posts that were used in feeds for that date.
    - The "base pool" of available posts for feeds are the posts that were synced
    in the previous "X" # of days (e.g., 5 days). Therefore, we load in the
    posts that were synced from those days.
    - We then match the posts used in feeds to the base pool of posts.
    - We then load in the hydrated versions of those posts.
    - We then check if the posts have any integration data. If not, we add them to
    the queue.
    - We then (optionally) run the integrations.
    """
    posts_to_backfill: dict[str, list[dict]] = {}

    breakpoint()

    # Determine which integrations to process
    specified_integrations = payload.get("integration")
    integrations_to_process = (
        specified_integrations
        if specified_integrations is not None
        else INTEGRATIONS_LIST
    )

    if payload.get("add_posts_to_queue"):
        posts_to_backfill: dict[str, list[dict]] = (
            backfill_posts_used_in_feed_for_partition_dates(
                start_date=payload.get("start_date"),
                end_date=payload.get("end_date"),
                exclude_partition_dates=payload.get("exclude_partition_dates"),
            )
        )
        # Only add posts for specified integrations
        for integration in integrations_to_process:
            if integration in posts_to_backfill:
                post_dicts = posts_to_backfill[integration]
                logger.info(
                    f"Adding {len(post_dicts)} posts for {integration} to backfill queue..."
                )
                queue = Queue(queue_name=f"input_{integration}", create_new_queue=True)
                queue.batch_add_items_to_queue(items=post_dicts, metadata=None)
    else:
        logger.info("Skipping adding posts to queue...")

    if payload.get("run_integrations"):
        logger.info("Running integrations...")
        logger.info(
            f"Backfilling for the following integrations: {integrations_to_process}"
        )
        for integration in integrations_to_process:
            integration_kwargs = payload.get("integration_kwargs", {}).get(
                integration, {}
            )
            _ = route_and_run_integration_request(
                {
                    "service": integration,
                    "payload": {"run_type": "backfill", **integration_kwargs},
                    "metadata": {},
                }
            )
    else:
        logger.info("Skipping integrations...")
    logger.info("Backfilling posts complete.")


if __name__ == "__main__":
    backfill_posts_used_in_feeds()
