"""Verifies the state of queues during write cache step.

Checks for the following states:
1. Start: Input queues empty, output queues not empty, verify storage counts
2. Mid: Same as start
3. End: Input queues empty, output queues empty, verify storage counts
"""

import click
from lib.db.queue import Queue
from lib.log.logger import get_logger
from pipelines.backfill_records_coordination.verification.helpers.verify_queue_states import (
    verify_queue_non_zero_state,
    verify_queue_zero_state,
    verify_only_invalid_records_in_queue,
    get_total_records_in_queue,
)
from pipelines.backfill_records_coordination.verification.helpers.verify_storage import (
    verify_storage,
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


def print_queue_counts():
    """Print counts for all input and output queues."""
    # Input queue counts
    perspective_input_count = get_total_records_in_queue(
        input_ml_inference_perspective_api_queue
    )
    sociopolitical_input_count = get_total_records_in_queue(
        input_ml_inference_sociopolitical_queue
    )
    ime_input_count = get_total_records_in_queue(input_ml_inference_ime_queue)

    logger.info(
        f"Perspective API input queue count: {perspective_input_count}",
        context={"queue_count": perspective_input_count},
    )
    logger.info(
        f"Sociopolitical input queue count: {sociopolitical_input_count}",
        context={"queue_count": sociopolitical_input_count},
    )
    logger.info(
        f"IME input queue count: {ime_input_count}",
        context={"queue_count": ime_input_count},
    )

    # Output queue counts
    perspective_output_count = get_total_records_in_queue(
        output_ml_inference_perspective_api_queue
    )
    sociopolitical_output_count = get_total_records_in_queue(
        output_ml_inference_sociopolitical_queue
    )
    ime_output_count = get_total_records_in_queue(output_ml_inference_ime_queue)

    logger.info(
        f"Perspective API output queue count: {perspective_output_count}",
        context={"queue_count": perspective_output_count},
    )
    logger.info(
        f"Sociopolitical output queue count: {sociopolitical_output_count}",
        context={"queue_count": sociopolitical_output_count},
    )
    logger.info(
        f"IME output queue count: {ime_output_count}",
        context={"queue_count": ime_output_count},
    )


def verify_start_or_mid_state():
    """Verify start/mid state and print storage info."""
    # Verify input queues are empty
    assert verify_only_invalid_records_in_queue(
        input_ml_inference_perspective_api_queue
    )
    assert verify_only_invalid_records_in_queue(input_ml_inference_sociopolitical_queue)
    assert verify_only_invalid_records_in_queue(input_ml_inference_ime_queue)

    # Verify output queues have records
    assert verify_queue_non_zero_state(output_ml_inference_perspective_api_queue)
    assert verify_queue_non_zero_state(output_ml_inference_sociopolitical_queue)
    assert verify_queue_non_zero_state(output_ml_inference_ime_queue)

    print_queue_counts()

    # Verify storage for each integration
    verify_storage("ml_inference_perspective_api")
    verify_storage("ml_inference_sociopolitical")
    verify_storage("ml_inference_ime")


def verify_end_state():
    """Verify end state and print storage info."""
    # Verify input queues only have invalid records
    assert verify_only_invalid_records_in_queue(
        input_ml_inference_perspective_api_queue
    )
    assert verify_only_invalid_records_in_queue(input_ml_inference_sociopolitical_queue)
    assert verify_only_invalid_records_in_queue(input_ml_inference_ime_queue)

    # Verify output queues are empty
    assert verify_queue_zero_state(output_ml_inference_perspective_api_queue)
    assert verify_queue_zero_state(output_ml_inference_sociopolitical_queue)
    assert verify_queue_zero_state(output_ml_inference_ime_queue)

    print_queue_counts()


@click.command()
@click.option(
    "--state",
    type=click.Choice(["start", "mid", "end"]),
    required=True,
    help="State to verify: start, mid, or end",
)
def main(state):
    if state == "start" or state == "mid":
        verify_start_or_mid_state()
    else:  # end
        verify_end_state()


if __name__ == "__main__":
    main()
