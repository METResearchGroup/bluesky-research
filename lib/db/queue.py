"""Generic class for implementing queues.

Under the hood, uses SQLite to implement queues.

Each queue will have their own SQLite instance in order to scale
each queue independently.
"""

import json
import os
import sqlite3
from typing import Optional

from pydantic import BaseModel, Field, validator
import typing_extensions as te

from lib.helper import BSKY_DATA_DIR, generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__file__)

root_db_path = os.path.join(BSKY_DATA_DIR, "queue")
if not os.path.exists(root_db_path):
    logger.info(f"Creating new directory for queue data at {root_db_path}...")
    os.makedirs(root_db_path)

existing_sqlite_dbs = [
    file for file in os.listdir(root_db_path) if file.endswith(".db")
]

DEFAULT_PRIMARY_KEY = "uri"


class QueueItem(BaseModel):
    """Represents a single item in the queue table.

    Attributes:
        id: Auto-incrementing primary key
        payload: JSON-serialized data for this queue item
        created_at: Timestamp when this item was added to the queue
        status: Current status of this item. One of:
            - 'pending': Not yet processed
            - 'processing': Currently being processed
            - 'completed': Successfully processed
            - 'failed': Processing failed
    """

    id: Optional[int] = Field(
        default=None,
        description="The auto-incrementing primary key for this queue item.",
    )
    payload: str = Field(
        default="", description="The JSON-serialized data for this queue item."
    )
    created_at: str = Field(
        default_factory=generate_current_datetime_str,
        description="The timestamp when the queue item was created.",
    )
    status: te.Literal["pending", "processing", "completed", "failed"] = Field(
        default="pending",
        description="The current status of the queue item. One of: 'pending', 'processing', 'completed', 'failed'.",
    )

    @validator("payload")
    def model_validator(cls, v):
        """Validate the payload field."""
        if not v:
            raise ValueError("Payload cannot be empty")
        try:
            json.loads(v)
        except json.JSONDecodeError:
            raise ValueError("Payload must be a valid JSON string")
        return v


class Queue:
    def __init__(
        self,
        queue_name: str,
        create_new_queue: bool = False,
        primary_key: Optional[str] = DEFAULT_PRIMARY_KEY,
    ):
        self.queue_name = queue_name
        self.queue_table_name = "queue"
        self.primary_key = primary_key
        self.db_path = os.path.join(root_db_path, f"{queue_name}.db")
        if not os.path.exists(self.db_path):
            if create_new_queue:
                logger.info(f"Creating new SQLite DB for queue {queue_name}...")
                self._init_queue_db()
            else:
                raise ValueError(
                    f"DB for queue {queue_name} doesn't exist. Need to pass in `create_new_queue` if creating a new queue is intended."
                )
        else:
            logger.info(f"Loading existing SQLite DB for queue {queue_name}...")
            count = self.get_queue_length()
            logger.info(f"Current queue size: {count} items")

    def __repr__(self):
        return f"Queue(name={self.queue_name}, db_path={self.db_path})"

    def __str__(self):
        return f"Queue(name={self.queue_name}, db_path={self.db_path})"

    def _init_queue_db(self):
        """Initialize queue database with unique constraint on primary_key from payload."""
        with sqlite3.connect(self.db_path) as conn:
            # Create the main table with an additional column for the primary key
            conn.execute(f"""
                CREATE TABLE {self.queue_table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT,
                    payload_key TEXT UNIQUE,  -- This will store the extracted primary key
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending'
                )
            """)
            # Create an index on payload_key for better performance
            conn.execute(f"""
                CREATE UNIQUE INDEX idx_payload_key 
                ON {self.queue_table_name}(payload_key)
            """)

    def _extract_primary_key(self, payload: dict) -> str:
        """Extract the primary key value from the payload."""
        try:
            if isinstance(payload, str):
                payload = json.loads(payload)
            return str(payload[self.primary_key])
        except (KeyError, json.JSONDecodeError) as e:
            raise ValueError(
                f"Could not extract primary key '{self.primary_key}' from payload: {e}"
            )

    def get_queue_length(self) -> int:
        """Get total number of items in queue."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {self.queue_table_name}")
            return cursor.fetchone()[0]  # Ensure we return an integer, not None

    def add_item_to_queue(self, payload: dict) -> Optional[QueueItem]:
        """Add single item to queue, skipping if primary key already exists."""
        if not payload:
            raise ValueError("Payload cannot be empty")

        json_payload = json.dumps(payload)
        payload_key = self._extract_primary_key(payload)
        queue_item = QueueItem(payload=json_payload)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    f"""
                    INSERT INTO {self.queue_table_name} 
                    (payload, payload_key, created_at, status) 
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(payload_key) DO NOTHING
                    RETURNING id
                    """,
                    (
                        queue_item.payload,
                        payload_key,
                        queue_item.created_at,
                        queue_item.status,
                    ),
                )
                result = cursor.fetchone()

                if result is None:
                    logger.debug(
                        f"Skipped duplicate item with {self.primary_key}: {payload_key}"
                    )
                    return None

                queue_item.id = result[0]
                return queue_item

        except sqlite3.IntegrityError:
            logger.debug(
                f"Skipped duplicate item with {self.primary_key}: {payload_key}"
            )
            return None

    def batch_add_item_to_queue(self, items: list[dict]) -> list[QueueItem]:
        """Add multiple items to queue, skipping duplicates."""
        queue_items = []
        for item in items:
            if not item:
                raise ValueError("Payload cannot be empty")
            queue_item = self.add_item_to_queue(item)
            if queue_item is not None:
                queue_items.append(queue_item)
        return queue_items

    def remove_item_from_queue(self) -> Optional[QueueItem]:
        """Remove and return the next available item from the queue.

        Returns:
            QueueItem: The next pending item in the queue, or None if queue is empty
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                WITH next_item AS (
                    SELECT id, payload, created_at, status
                    FROM queue 
                    WHERE status = 'pending'
                    ORDER BY created_at ASC 
                    LIMIT 1
                )
                UPDATE queue
                SET status = 'processing'
                WHERE id IN (SELECT id FROM next_item)
                RETURNING id, payload, created_at, status
            """)

            row = cursor.fetchone()
            if not row:
                count = self.get_queue_length()
                logger.info(f"Queue is empty. Total items in queue: {count}")
                return None

            item = QueueItem(
                id=row[0], payload=row[1], created_at=row[2], status="processing"
            )

            count = self.get_queue_length()
            logger.info(f"Queue size after remove: {count} items")
            return item

    def batch_remove_items_from_queue(self, limit: int) -> list[QueueItem]:
        """Remove multiple items from queue."""
        items = []
        for _ in range(limit):
            item = self.remove_item_from_queue()
            if item is None:
                break
            items.append(item)
        return items
