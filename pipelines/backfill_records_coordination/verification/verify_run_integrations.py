"""Verifies that we've successfully run the integrations.

Checks for the following:
1. The input queues are empty.
2. The output queues are not empty.
3. The total number of records in the output queues is correct.

Should be run after running the integration(s).
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
    assert verify_queue_zero_state(input_ml_inference_perspective_api_queue)
    assert verify_queue_non_zero_state(output_ml_inference_perspective_api_queue)
    assert verify_queue_zero_state(input_ml_inference_sociopolitical_queue)
    assert verify_queue_non_zero_state(output_ml_inference_sociopolitical_queue)
    assert verify_queue_zero_state(input_ml_inference_ime_queue)
    assert verify_queue_non_zero_state(output_ml_inference_ime_queue)

    for queue in [
        output_ml_inference_perspective_api_queue,
        output_ml_inference_sociopolitical_queue,
        output_ml_inference_ime_queue,
    ]:
        total_records = get_total_records_in_queue(queue)
        print(f"Total records in {queue.queue_name}: {total_records}")
