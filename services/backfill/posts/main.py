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
                "start_date" (Optional[str]): Start date in YYYY-MM-DD format (inclusive)
                "end_date" (Optional[str]): End date in YYYY-MM-DD format (inclusive)
            }

    The function performs two main steps:
    1. If add_posts_to_queue=True, loads posts that haven't been processed by each integration
       and adds them to the respective integration queues
    2. If run_integrations=True, triggers the integration services to process the queued posts

    Examples:
        # Example 1: Only add posts to queue for Perspective API integration
        payload = {
            "add_posts_to_queue": True,
            "run_integrations": False,
            "integration": ["ml_inference_perspective_api"]
        }
        # This will load unprocessed posts and add them to the Perspective API queue,
        # but won't run the integration service.

        # Example 2: Run all integrations on existing queued posts
        payload = {
            "add_posts_to_queue": False,
            "run_integrations": True
        }
        # This will run all integration services on posts already in their queues,
        # without adding new posts.

        # Example 3: Full backfill for all integrations
        payload = {
            "add_posts_to_queue": True,
            "run_integrations": True
        }
        # This will load unprocessed posts for all integrations, add them to their
        # respective queues, and then run all integration services.

        # Example 4: Targeted backfill for specific integrations
        payload = {
            "add_posts_to_queue": True,
            "run_integrations": True,
            "integration": ["ml_inference_perspective_api", "ml_inference_sociopolitical"]
        }
        # This will load and process posts only for the Perspective API and
        # sociopolitical inference integrations.
    """
    posts_to_backfill: dict[str, list[dict]] = {}
    if payload.get("add_posts_to_queue"):
        posts_to_backfill: dict[str, list[dict]] = load_posts_to_backfill(
            payload.get("integration"),
            start_date=payload.get("start_date"),
            end_date=payload.get("end_date"),
        )
        context = {"total": len(posts_to_backfill)}
        logger.info(
            f"Loaded {len(posts_to_backfill)} posts to backfill.", context=context
        )

        if len(posts_to_backfill) == 0:
            logger.info("No posts to backfill. Exiting...")
            return

        for integration, post_dicts in posts_to_backfill.items():
            logger.info(
                f"Adding {len(post_dicts)} posts for {integration} to backfill queue..."
            )  # noqa
            queue = Queue(queue_name=f"input_{integration}", create_new_queue=True)
            queue.batch_add_items_to_queue(items=post_dicts, metadata=None)
        logger.info("Adding posts to queue complete.")
    else:
        logger.info("Skipping adding posts to queue...")

    if payload.get("run_integrations"):
        logger.info("Running integrations...")
        # if we tried to load posts to backfill, but none were found, skip.
        # Else, set as default to backfill all integrations.
        if payload.get("add_posts_to_queue"):
            integrations_to_backfill = posts_to_backfill.keys()
        elif payload.get("integration"):
            integrations_to_backfill = payload.get("integration")
        else:
            integrations_to_backfill = INTEGRATIONS_LIST
        logger.info(
            f"Backfilling for the following integrations: {integrations_to_backfill}"
        )
        for integration in integrations_to_backfill:
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
    # payload = {"integration": ["ml_inference_perspective_api"]}
    payload = {"integration": None}
    backfill_posts(payload)
