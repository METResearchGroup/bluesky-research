"""Verifies the state of integration queues at different stages.

Checks for the following states:
1. Start: Input queues not empty, output queues empty
2. Mid: Print queue counts and totals for each integration
3. End: Input queues empty, output queues not empty
"""

import click
from lib.db.queue import Queue
from pipelines.backfill_records_coordination.verification.helpers.verify_queue_states import (
    verify_queue_non_zero_state,
    verify_queue_zero_state,
    verify_only_invalid_records_in_queue,
    get_total_records_in_queue,
)
from lib.log.logger import get_logger

logger = get_logger(__name__)

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


def verify_start_state():
    """Verify that input queues have records and output queues are empty."""
    assert verify_queue_non_zero_state(input_ml_inference_perspective_api_queue)
    assert verify_queue_zero_state(output_ml_inference_perspective_api_queue)
    assert verify_queue_non_zero_state(input_ml_inference_sociopolitical_queue)
    assert verify_queue_zero_state(output_ml_inference_sociopolitical_queue)
    assert verify_queue_non_zero_state(input_ml_inference_ime_queue)
    assert verify_queue_zero_state(output_ml_inference_ime_queue)


def print_mid_state():
    """Print counts for all queues and total records per integration."""
    # Perspective API queues
    input_p = get_total_records_in_queue(input_ml_inference_perspective_api_queue)
    output_p = get_total_records_in_queue(output_ml_inference_perspective_api_queue)
    logger.info(
        f"Perspective API - Input queue: {input_p}, Output queue: {output_p}, Total records: {input_p + output_p}",
        context={
            "input_queue": input_p,
            "output_queue": output_p,
            "total_records": input_p + output_p,
        },
    )

    # Sociopolitical queues
    input_s = get_total_records_in_queue(input_ml_inference_sociopolitical_queue)
    output_s = get_total_records_in_queue(output_ml_inference_sociopolitical_queue)
    logger.info(
        f"Sociopolitical - Input queue: {input_s}, Output queue: {output_s}, Total records: {input_s + output_s}",
        context={
            "input_queue": input_s,
            "output_queue": output_s,
            "total_records": input_s + output_s,
        },
    )

    # IME queues
    input_i = get_total_records_in_queue(input_ml_inference_ime_queue)
    output_i = get_total_records_in_queue(output_ml_inference_ime_queue)
    logger.info(
        f"IME - Input queue: {input_i}, Output queue: {output_i}, Total records: {input_i + output_i}",
        context={
            "input_queue": input_i,
            "output_queue": output_i,
            "total_records": input_i + output_i,
        },
    )


def verify_end_state():
    """Verify that input queues only have invalid records (if any) and output
    queues have records.

    This checks to see if the integration classifies all the records that it
    was supposed to do.
    """
    assert verify_only_invalid_records_in_queue(
        input_ml_inference_perspective_api_queue
    )
    assert verify_queue_non_zero_state(output_ml_inference_perspective_api_queue)
    assert verify_only_invalid_records_in_queue(input_ml_inference_sociopolitical_queue)
    assert verify_queue_non_zero_state(output_ml_inference_sociopolitical_queue)
    assert verify_only_invalid_records_in_queue(input_ml_inference_ime_queue)
    assert verify_queue_non_zero_state(output_ml_inference_ime_queue)


@click.command()
@click.option(
    "--state",
    type=click.Choice(["start", "mid", "end"]),
    required=True,
    help="State to verify: start, mid, or end",
)
def main(state):
    if state == "start":
        verify_start_state()
    elif state == "mid":
        print_mid_state()
    else:  # end
        verify_end_state()


if __name__ == "__main__":
    main()
