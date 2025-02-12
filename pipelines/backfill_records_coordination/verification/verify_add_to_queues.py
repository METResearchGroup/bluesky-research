"""Verifies that we've successfully added records to the queues.

Checks for the following:
1. The input queues are not empty.
2. The output queues are empty.
3. The total number of records in the input queues is correct.

Should be run after inserting records into the integration queue(s).
"""

from lib.db.queue import Queue
from pipelines.backfill_records_coordination.verification.helpers.verify_queue_states import (
    verify_queue_non_zero_state,
    verify_queue_zero_state,
    get_total_records_in_queue,
)


input_ml_inference_perspective_api_queue = Queue(
    queue_name="input_ml_inference_perspective_api"
)

output_ml_inference_perspective_api_queue = Queue(
    queue_name="output_ml_inference_perspective_api"
)

input_ml_inference_sociopolitical_queue = Queue(
    queue_name="input_ml_inference_sociopolitical"
)

output_ml_inference_sociopolitical_queue = Queue(
    queue_name="output_ml_inference_sociopolitical"
)

input_ml_inference_ime_queue = Queue(queue_name="input_ml_inference_ime")

output_ml_inference_ime_queue = Queue(queue_name="output_ml_inference_ime")


def main():
    assert verify_queue_non_zero_state(input_ml_inference_perspective_api_queue)
    assert verify_queue_zero_state(output_ml_inference_perspective_api_queue)
    assert verify_queue_non_zero_state(input_ml_inference_sociopolitical_queue)
    assert verify_queue_zero_state(output_ml_inference_sociopolitical_queue)
    assert verify_queue_non_zero_state(input_ml_inference_ime_queue)
    assert verify_queue_zero_state(output_ml_inference_ime_queue)

    for queue in [
        input_ml_inference_perspective_api_queue,
        input_ml_inference_sociopolitical_queue,
        input_ml_inference_ime_queue,
    ]:
        total_records = get_total_records_in_queue(queue)
        print(f"Total records in {queue.queue_name}: {total_records}")
