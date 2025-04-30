from lib.db.queue import Queue
from services.backfill.core.constants import input_queue_name

queue = Queue(queue_name=input_queue_name, create_new_queue=True)


def load_latest_dids_to_backfill_from_queue() -> list[str]:
    latest_payloads: list[dict] = queue.load_dict_items_from_queue(
        limit=None,
        status="pending",
    )
    dids_to_backfill: list[str] = [payload["dids"] for payload in latest_payloads]
    return dids_to_backfill
