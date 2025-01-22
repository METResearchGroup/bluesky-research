"""Insert dummy records into the integration queues.

Inserts new dummy records into the queues and verifies that the backfill
actually works as intended.

Triggers the backfill pipelines with "add_posts_to_queue" set to False,
since we're adding it here manually, and then triggers the integrations.
"""

import hashlib
import time

from lib.db.queue import Queue
from pipelines.backfill_records_coordination.handler import lambda_handler

from faker import Faker

DEFAULT_NUM_DUMMY_RECORDS = 100

fake = Faker()


def generate_dummy_record() -> dict:
    """Generates a single dummy record."""
    unique_input = f"{time.time()}_{fake.random_number()}"
    uri = hashlib.sha256(unique_input.encode()).hexdigest()
    text = fake.text(max_nb_chars=280)  # Twitter-like length
    return {"uri": uri, "text": text}


def generate_dummy_records(num_records: int) -> list[dict]:
    """Generates a list of dummy records."""
    records: list[dict] = [generate_dummy_record() for _ in range(num_records)]
    return records


def insert_dummy_records_into_queue(records: list[dict], integration: str) -> None:
    """Inserts dummy records into the queue."""
    queue = Queue(queue_name=f"input_{integration}", create_new_queue=True)
    queue.batch_add_items_to_queue(items=records, metadata=None)


def trigger_backfill_pipeline(integrations: list[str]) -> None:
    """Triggers backfill pipeline for the given integrations."""
    payload = {
        "record_type": "post",
        "add_posts_to_queue": False,
        "run_integrations": True,
        "integration": integrations,
        "integration_kwargs": {
            "perspective_api": {
                "backfill_period": None,
                "backfill_duration": None,
                "run_classification": True,
            }
        },
    }
    lambda_handler(payload, None)


if __name__ == "__main__":
    generate_dummy_records_payloads = [
        {
            "integration": "ml_inference_perspective_api",
            "num_records": 100,
        },
        {
            "integration": "ml_inference_sociopolitical",
            "num_records": 100,
        },
        {
            "integration": "ml_inference_ime",
            "num_records": 100,
        },
    ]
    for payload in generate_dummy_records_payloads:
        records = generate_dummy_records(payload["num_records"])
        insert_dummy_records_into_queue(records, payload["integration"])
        trigger_backfill_pipeline([payload["integration"]])
