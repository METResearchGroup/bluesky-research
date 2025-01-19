"""Generic class for implementing queues.

Under the hood, uses SQLite to implement queues.

Each queue will have their own SQLite instance in order to scale
each queue independently.
"""

import json
import os
import sqlite3
import time
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
        """Initialize queue database with WAL mode and optimized settings."""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            # Enable WAL mode before creating tables
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")  # 30 second timeout
            conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
            conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory mapped I/O
            conn.execute("PRAGMA page_size=4096")

            # Create the main table with an additional column for the primary key
            conn.execute(f"""
                CREATE TABLE {self.queue_table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT,
                    payload_key TEXT UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending'
                )
            """)
            # Create an index on payload_key for better performance
            conn.execute(f"""
                CREATE UNIQUE INDEX idx_payload_key 
                ON {self.queue_table_name}(payload_key)
            """)

            # Ensure WAL mode is persisted
            conn.execute("PRAGMA journal_mode=WAL")
            conn.commit()

    def _get_connection(self) -> sqlite3.Connection:
        """Get an optimized SQLite connection with retry logic."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)

        # Configure connection for optimal performance
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA cache_size=-64000")
        conn.execute("PRAGMA mmap_size=268435456")
        conn.execute("PRAGMA temp_store=MEMORY")

        return conn

    def get_queue_length(self) -> int:
        """Get total number of items in queue with retry logic."""
        max_retries = 3
        retry_delay = 1.0  # seconds

        for attempt in range(max_retries):
            try:
                with self._get_connection() as conn:
                    cursor = conn.execute(
                        f"SELECT COUNT(*) FROM {self.queue_table_name}"
                    )
                    return cursor.fetchone()[0]
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(
                        f"Database locked, retrying in {retry_delay} seconds... "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                raise

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
        """Add multiple items to queue, processing in chunks for memory efficiency."""
        CHUNK_SIZE = 1000
        all_queue_items = []
        total_chunks = len(items) // CHUNK_SIZE

        # Process items in chunks
        for i in range(0, len(items), CHUNK_SIZE):
            if i % 50 == 0:
                logger.info(f"Processing chunk {i // CHUNK_SIZE} of {total_chunks}")
            chunk = items[i : i + CHUNK_SIZE]
            chunk_items = self._batch_add_chunk(chunk)
            all_queue_items.extend(chunk_items)

        return all_queue_items

    def _batch_add_chunk(self, items: list[dict]) -> list[QueueItem]:
        """Add multiple items to queue with retry logic."""
        if not items:
            return []

        insert_data = []
        payload_map = {}

        for item in items:
            if not item:
                raise ValueError("Payload cannot be empty")

            json_payload = json.dumps(item)
            payload_key = self._extract_primary_key(item)
            created_at = generate_current_datetime_str()
            status = "pending"

            insert_data.append((json_payload, payload_key, created_at, status))
            payload_map[payload_key] = (json_payload, created_at, status)

        max_retries = 3
        retry_delay = 1.0

        for attempt in range(max_retries):
            try:
                with self._get_connection() as conn:
                    # First perform the batch insert
                    placeholders = ",".join(["(?, ?, ?, ?)"] * len(insert_data))
                    conn.execute(
                        f"""
                        INSERT OR IGNORE INTO {self.queue_table_name}
                        (payload, payload_key, created_at, status)
                        VALUES {placeholders}
                        """,
                        [val for tup in insert_data for val in tup],
                    )

                    # Then retrieve the IDs for successfully inserted items
                    payload_keys = list(payload_map.keys())
                    placeholders = ",".join(["?"] * len(payload_keys))
                    cursor = conn.execute(
                        f"""
                        SELECT id, payload_key, created_at 
                        FROM {self.queue_table_name}
                        WHERE payload_key IN ({placeholders})
                        """,
                        payload_keys,
                    )

                    # Create QueueItems only for successfully inserted items
                    queue_items = []
                    for row in cursor.fetchall():
                        id_, payload_key, db_created_at = row
                        payload, _, status = payload_map[payload_key]
                        queue_item = QueueItem(
                            id=id_,
                            payload=payload,
                            created_at=db_created_at,
                            status=status,
                        )
                        queue_items.append(queue_item)

                    return queue_items

            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    logger.warning(
                        f"Database locked, retrying in {retry_delay} seconds... "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                raise

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

    def _get_sqlite_version(self) -> tuple:
        """Get the SQLite version as a tuple of integers."""
        with sqlite3.connect(self.db_path):
            version_str = sqlite3.sqlite_version
        return tuple(map(int, version_str.split(".")))

    def _supports_returning(self) -> bool:
        """Check if SQLite version supports RETURNING clause."""
        return self._get_sqlite_version() >= (3, 35, 0)
