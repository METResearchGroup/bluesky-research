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
            queue = Queue(queue_name=integration, create_new_queue=True)
            payloads = [{"uri": uri} for uri in post_uris]
            queue.batch_add_items_to_queue(payloads)
        logger.info("Adding posts to queue complete.")
    else:
        logger.info("Skipping adding posts to queue...")

    if payload.get("run_integrations"):
        logger.info("Running integrations...")
        if not posts_to_backfill:
            integrations_to_backfill = INTEGRATIONS_LIST
        else:
            integrations_to_backfill = posts_to_backfill.keys()
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
