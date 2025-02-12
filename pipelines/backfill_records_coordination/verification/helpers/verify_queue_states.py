"""Verification script for checking queue states."""

import json

from lib.db.queue import Queue


def verify_queue_zero_state(queue: Queue) -> bool:
    """Verify that a queue is empty.

    This function checks if the specified queue has zero items, which is useful
    for verifying initial states or completed processing.

    Args:
        queue (Queue): The queue instance to verify

    Returns:
        bool: True if queue is empty, False otherwise
    """
    queue_counts = queue.get_queue_length()
    print(f"Queue {queue.queue_name} has {queue_counts} items.")
    return queue_counts == 0


def verify_queue_non_zero_state(queue: Queue) -> bool:
    """Verify that a queue contains items.

    This function checks if the specified queue has at least one item, which is
    useful for verifying that items were successfully added to the queue.

    Args:
        queue (Queue): The queue instance to verify

    Returns:
        bool: True if queue contains items, False if empty
    """
    queue_counts = queue.get_queue_length()
    print(f"Queue {queue.queue_name} has {queue_counts} items.")
    return queue_counts > 0


def get_total_records_in_queue(queue: Queue) -> int:
    """Get total number of individual records in pending queue items.

    This function counts the total number of actual records across all pending
    queue items by summing their batch sizes. Since queue items can contain
    batches of records, this provides the true count of pending records.

    Args:
        queue (Queue): The queue instance to check

    Returns:
        int: Total number of individual records in pending queue items
    """
    query = "SELECT metadata FROM queue WHERE status = 'pending';"
    res = queue.run_query(query)
    total_records = 0
    for record in res:
        metadata = json.loads(record["metadata"])
        total_records += metadata["actual_batch_size"]
    print(f"Queue {queue.queue_name} has {total_records} records.")
    return total_records
