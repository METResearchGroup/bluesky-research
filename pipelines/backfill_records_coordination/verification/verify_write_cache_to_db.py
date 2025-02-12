"""Prints queue counts for output queues.

Prints the following:
1. Whether each output queue has items
2. The total number of records in each output queue

Should be run before and after the write_cache step to verify counts.
"""

from lib.db.queue import Queue
from pipelines.backfill_records_coordination.verification.helpers.verify_queue_states import (
    get_total_records_in_queue,
)


output_ml_inference_perspective_api_queue = Queue(
    queue_name="output_ml_inference_perspective_api"
)

output_ml_inference_sociopolitical_queue = Queue(
    queue_name="output_ml_inference_sociopolitical"
)

output_ml_inference_ime_queue = Queue(queue_name="output_ml_inference_ime")


def main():
    for queue in [
        output_ml_inference_perspective_api_queue,
        output_ml_inference_sociopolitical_queue,
        output_ml_inference_ime_queue,
    ]:
        total_records = get_total_records_in_queue(queue)
        print(f"Total records in {queue.queue_name}: {total_records}")


if __name__ == "__main__":
    main()
