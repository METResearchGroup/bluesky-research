from services.backfill.posts.main import backfill_posts
from services.backfill.posts_used_in_feeds.main import backfill_posts_used_in_feeds
from services.backfill.core.main import backfill_sync


def backfill_records(payload: dict):
    """Backfills records of different types based on the provided payload.

    Args:
        payload (dict): Configuration for the backfill process. Expected format depends on record_type:

            For record_type="posts":
            {
                "record_type": "posts",
                "add_posts_to_queue" (bool): Whether to add posts to integration queues
                "run_integrations" (bool): Whether to run the integrations after queueing
                "integration" (Optional[list[str]]): List of specific integrations to backfill.
                    If not provided, will backfill for all integrations.
            }

            For other record types:
            {
                "record_type": "<record_type>",
                # Additional payload parameters will be documented as other record types are added
            }

    Example:
        payload = {
            "record_type": "posts",
            "add_posts_to_queue": True,
            "run_integrations": True,
            "integration": ["ml_inference_perspective_api"]  # Optional
        }
    """
    record_type = payload.get("record_type")
    if record_type == "posts":
        backfill_posts(payload)
    elif record_type == "posts_used_in_feeds":
        backfill_posts_used_in_feeds(payload)
    elif record_type == "sync":
        backfill_sync(payload)
    else:
        raise ValueError(f"Unsupported record type: {record_type}")
