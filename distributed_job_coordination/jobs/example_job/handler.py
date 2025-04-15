"""Example handler job."""

from lib.db.queue import Queue
from lib.log.logger import get_logger

logger = get_logger(__name__)


def compute_text_length(text: str) -> int:
    return len(text)


def do_computation(items: list[dict]) -> list[dict]:
    return [
        {
            "id": item["id"],
            "text_length": compute_text_length(item["text"]),
            "group_id": item["group_id"],
        }
        for item in items
    ]


def main(event: dict) -> None:
    items = event["items"]
    queue: Queue = event["task_output_queue"]
    result = do_computation(items)
    queue.batch_add_items_to_queue(result)
    logger.info(f"Added {len(result)} items to queue.")
    return result


def handler(event: dict) -> None:
    """
    Example job handler.
    """
    return main(event)
