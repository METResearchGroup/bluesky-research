"""Service for backfilling posts."""

from api.integrations_router.main import route_and_run_integration_request
from lib.db.queue import Queue
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.backfill.posts.load_data import (
    INTEGRATIONS_LIST,
    load_posts_to_backfill,
)

logger = get_logger(__file__)


@track_performance
def backfill_posts(payload: dict):
    """Backfills posts by adding them to integration queues and optionally running the integrations.

    Args:
        payload (dict): Configuration for the backfill process. Expected format:
            {
                "add_posts_to_queue" (bool): Whether to add posts to integration queues
                "run_integrations" (bool): Whether to run the integrations after queueing
                "integration" (Optional[list[str]]): List of specific integrations to backfill.
                    If not provided, will backfill for all integrations.
            }

    The function performs two main steps:
    1. If add_posts_to_queue=True, loads posts that haven't been processed by each integration
       and adds them to the respective integration queues
    2. If run_integrations=True, triggers the integration services to process the queued posts
    """
    posts_to_backfill: dict[str, set[str]] = {}
    if payload.get("add_posts_to_queue"):
        posts_to_backfill: dict[str, set[str]] = load_posts_to_backfill(
            payload.get("integration")
        )
        context = {"total": len(posts_to_backfill)}
        logger.info(
            f"Loaded {len(posts_to_backfill)} posts to backfill.", context=context
        )

        if len(posts_to_backfill) == 0:
            logger.info("No posts to backfill. Exiting...")
            return

        for integration, post_uris in posts_to_backfill.items():
            logger.info(
                f"Adding {len(post_uris)} posts for {integration} to backfill queue..."
            )  # noqa
            queue = Queue(queue_name=f"input_{integration}", create_new_queue=True)
            payloads = [{"uri": uri} for uri in post_uris]
            queue.batch_add_items_to_queue(items=payloads, metadata=None)
        logger.info("Adding posts to queue complete.")
    else:
        logger.info("Skipping adding posts to queue...")

    if payload.get("run_integrations"):
        logger.info("Running integrations...")
        # if we tried to load posts to backfill, but none were found, skip.
        # Else, set as default to backfill all integrations.
        if payload.get("add_posts_to_queue"):
            integrations_to_backfill = posts_to_backfill.keys()
        else:
            integrations_to_backfill = INTEGRATIONS_LIST
        for integration in integrations_to_backfill:
            _ = route_and_run_integration_request(
                {
                    "service": integration,
                    "payload": {"run_type": "backfill"},
                    "metadata": {},
                }
            )
    else:
        logger.info("Skipping integrations...")
    logger.info("Backfilling posts complete.")


if __name__ == "__main__":
    # payload = {"integration": ["ml_inference_perspective_api"]}
    payload = {"integration": None}
    backfill_posts(payload)
