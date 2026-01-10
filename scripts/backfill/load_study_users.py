"""Logic for loading study users and then writing to SQLite.

Exports to SQLite the list of user DIDs, in the form:

[
    {
        "dids": [
            "did:plc:123",
            "did:plc:456",
            ...
        ]
    }
]

Each row in the SQLite DB is a list of user DIDs.

"""

import os

from lib.db.queue import Queue
from lib.batching_utils import create_batches
from lib.log.logger import get_logger
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel

logger = get_logger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))


def main():
    users: list[UserToBlueskyProfileModel] = get_all_users()
    users: list[dict] = [user.model_dump() for user in users]
    user_dids = [user["bluesky_user_did"] for user in users]

    batch_user_dids = create_batches(user_dids, 100)

    items: list[dict] = [{"dids": user_dids} for user_dids in batch_user_dids]
    queue_path = os.path.join(current_dir, "backfill_study_users.sqlite")
    queue = Queue(
        queue_name="backfill_study_users",
        create_new_queue=True,
        temp_queue=True,
        temp_queue_path=queue_path,
    )
    queue.batch_add_items_to_queue(items=items)
    logger.info(f"Wrote {len(user_dids)} study users to queue")


if __name__ == "__main__":
    main()
