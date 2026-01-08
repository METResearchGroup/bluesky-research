"""Helper functions for the posts_used_in_feeds backfill."""

from typing import Optional

from lib.db.queue import Queue
from lib.datetime_utils import get_partition_dates
from lib.log.logger import get_logger
from services.backfill.posts_used_in_feeds.load_data import load_posts_to_backfill

logger = get_logger(__file__)


def backfill_posts_used_in_feed_for_partition_date(
    partition_date: str,
    integrations_to_process: list[str],
):
    posts_to_backfill: dict[str, list[dict]] = load_posts_to_backfill(
        partition_date=partition_date,
        integrations=integrations_to_process,
    )
    context = {"total": len(posts_to_backfill)}
    logger.info(
        f"Loaded {len(posts_to_backfill)} posts to backfill for partition date {partition_date}.",
        context=context,
    )

    if len(posts_to_backfill) == 0:
        logger.info(f"No posts to backfill for partition date {partition_date}.")
        return

    for integration, post_dicts in posts_to_backfill.items():
        logger.info(
            f"Adding {len(post_dicts)} posts for {integration} to backfill queue..."
        )
        queue = Queue(queue_name=f"input_{integration}", create_new_queue=True)
        queue.batch_add_items_to_queue(items=post_dicts, metadata=None)
    logger.info("Adding posts to queue complete.")


def backfill_posts_used_in_feed_for_partition_dates(
    start_date: str,
    end_date: str,
    integrations_to_process: list[str],
    exclude_partition_dates: Optional[list[str]] = None,
):
    """Backfill posts used in feed for a range of partition dates."""
    partition_dates: list[str] = get_partition_dates(
        start_date=start_date,
        end_date=end_date,
        exclude_partition_dates=exclude_partition_dates,
    )

    for partition_date in partition_dates:
        logger.info(
            f"Backfilling posts used in feed for partition date {partition_date}..."
        )
        backfill_posts_used_in_feed_for_partition_date(
            partition_date,
            integrations_to_process=integrations_to_process,
        )
        logger.info(
            f"Finished backfilling posts used in feed for partition date {partition_date}."
        )
    logger.info(
        f"Finished backfilling posts used in feed from partition dates {start_date} to {end_date}."
    )
