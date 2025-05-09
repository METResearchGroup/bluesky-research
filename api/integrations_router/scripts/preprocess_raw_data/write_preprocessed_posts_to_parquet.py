"""Clears the SQLite caches for the preprocessed posts and writes
them to parquet files."""

from lib.db.queue import Queue
from services.write_cache_buffers_to_db.helper import write_cache_buffer_queue_to_db


def main():
    """Clears the SQLite caches for the preprocessed posts and writes
    them to Queueet files.

    Then pushes to the integration
    """
    service = "preprocess_raw_data"
    clear_queue = False
    write_cache_buffer_queue_to_db(service=service, clear_queue=clear_queue)

    # Push to the integration queues.
    preprocessed_posts_queue = Queue(queue_name="output_preprocess_raw_data")

    items = preprocessed_posts_queue.load_dict_items_from_queue(limit=None)

    print(
        f"Loaded {len(items)} items from the preprocessed posts queue, to push to the queues."
    )

    integration_queues = [
        Queue(queue_name="input_ml_inference_perspective_api"),
        Queue(queue_name="input_ml_inference_sociopolitical"),
        Queue(queue_name="input_ml_inference_ime"),
    ]

    for integration_queue in integration_queues:
        print(
            f"Pushing {len(items)} items to the {integration_queue.queue_name} queue..."
        )
        integration_queue.batch_add_items_to_queue(items=items)


if __name__ == "__main__":
    main()
