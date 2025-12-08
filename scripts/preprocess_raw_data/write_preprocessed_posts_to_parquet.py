"""Clears the SQLite caches for the preprocessed posts and writes
them to parquet files."""

from lib.db.queue import Queue
from services.write_cache_buffers_to_db.helper import write_cache_buffer_queue_to_db


def main():
    """Clears the SQLite caches for the preprocessed posts and writes
    them to parquet files.

    Then pushes to the integration
    """

    # NOTE: no need to write to parquet here if it's already written to the DB,
    # worth double-checking that the DB is correct.
    service = "preprocess_raw_data"
    clear_queue = False
    write_cache_buffer_queue_to_db(service=service, clear_queue=clear_queue)

    # Push to the integration queues.
    preprocessed_posts_queue = Queue(queue_name="output_preprocess_raw_data")

    items = preprocessed_posts_queue.load_dict_items_from_queue(limit=None)

    # temp patch to fix preprocessing_timestamp and filtered_at to use synctimestamp
    new_items = []
    for item in items:
        item["preprocessing_timestamp"] = item["synctimestamp"]
        item["filtered_at"] = item["synctimestamp"]
        new_items.append(item)

    items = new_items

    print(
        f"Loaded {len(items)} items from the preprocessed posts queue, to push to the queues."
    )

    integration_queues = [
        Queue(queue_name="input_ml_inference_perspective_api", create_new_queue=True),
        Queue(queue_name="input_ml_inference_sociopolitical", create_new_queue=True),
        Queue(queue_name="input_ml_inference_ime", create_new_queue=True),
    ]

    for integration_queue in integration_queues:
        print(
            f"Pushing {len(items)} items to the {integration_queue.queue_name} queue..."
        )
        integration_queue.batch_add_items_to_queue(items=items)


if __name__ == "__main__":
    main()
