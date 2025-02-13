"""Verifies the state of queues during write cache step.

Checks for the following states:
1. Start: Input queues empty, output queues not empty
2. Mid: Print output queue counts
3. End: Output queues not empty, verify counts
"""

import click
from lib.db.queue import Queue
from lib.log.logger import get_logger
from pipelines.backfill_records_coordination.verification.helpers.verify_queue_states import (
    verify_queue_non_zero_state,
    verify_queue_zero_state,
    get_total_records_in_queue,
)

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
    """Verify that input queues are empty and output queues have records."""
    # Verify input queues are empty
    assert verify_queue_zero_state(input_ml_inference_perspective_api_queue)
    assert verify_queue_zero_state(input_ml_inference_sociopolitical_queue)
    assert verify_queue_zero_state(input_ml_inference_ime_queue)

    # Verify output queues have records and print counts
    assert verify_queue_non_zero_state(output_ml_inference_perspective_api_queue)
    assert verify_queue_non_zero_state(output_ml_inference_sociopolitical_queue)
    assert verify_queue_non_zero_state(output_ml_inference_ime_queue)

    print_queue_counts()


def print_queue_counts():
    """Print counts for all output queues."""
    perspective_count = get_total_records_in_queue(
        output_ml_inference_perspective_api_queue
    )
    sociopolitical_count = get_total_records_in_queue(
        output_ml_inference_sociopolitical_queue
    )
    ime_count = get_total_records_in_queue(output_ml_inference_ime_queue)

    logger.info(
        f"Perspective API output queue count: {perspective_count}",
        context={"queue_count": perspective_count},
    )
    logger.info(
        f"Sociopolitical output queue count: {sociopolitical_count}",
        context={"queue_count": sociopolitical_count},
    )
    logger.info(
        f"IME output queue count: {ime_count}",
        context={"queue_count": ime_count},
    )


def verify_end_state():
    """Verify that output queues still have records."""
    print_queue_counts()

    assert verify_queue_non_zero_state(output_ml_inference_perspective_api_queue)
    assert verify_queue_non_zero_state(output_ml_inference_sociopolitical_queue)
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
        print_queue_counts()
    else:  # end
        verify_end_state()


if __name__ == "__main__":
    main()
