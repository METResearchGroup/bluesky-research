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

DEFAULT_BATCH_SIZE = 1000


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
    def __init__(self, queue_name: str, create_new_queue: bool = False):
        self.queue_name = queue_name
        self.queue_table_name = "queue"
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
            conn.execute("PRAGMA page_size=8192")  # Double the current size

            # Add compression if you're using SQLite 3.38.0 or later
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA zip_compression=true")  # Enable compression
            except sqlite3.OperationalError:
                # Older SQLite version, compression not available
                pass

            # Create the main table with an additional column for the primary key
            conn.execute(f"""
                CREATE TABLE {self.queue_table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending'
                )
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

    def add_item_to_queue(self, payload: dict, metadata: dict = None) -> None:
        """Add single item to queue."""
        if not payload:
            raise ValueError("Payload cannot be empty")
        if metadata is None:
            metadata = {}

        json_payload = json.dumps(payload)
        metadata_dict = {**metadata, "batch_size": 1, "actual_batch_size": 1}
        json_metadata = json.dumps(metadata_dict)

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    f"""
                    INSERT INTO {self.queue_table_name} 
                    (payload, metadata, created_at, status) 
                    VALUES (?, ?, ?, ?)
                    RETURNING id
                    """,
                    (
                        json_payload,
                        json_metadata,
                        generate_current_datetime_str(),
                        "pending",
                    ),
                )
        except Exception as e:
            logger.error(f"Error adding item to queue: {e}")
            raise e

    def _create_batched_chunks(
        self,
        chunks: list[list[dict]],
        batch_size: int,
        metadata: dict = None,
    ) -> list[tuple[str, str, str, str]]:
        """Out of the chunks, create a batch of chunks that will be written to the queue."""
        batched_chunks: list[tuple[str, str, str, str]] = []
        if metadata is None:
            metadata = {}
        for chunk in chunks:
            payload = json.dumps(chunk)
            created_at = generate_current_datetime_str()
            status = "pending"
            metadata_dict = {
                **metadata,
                "batch_size": batch_size,
                "actual_batch_size": len(chunk),
            }
            json_metadata = json.dumps(metadata_dict)
            batched_chunks.append((payload, json_metadata, created_at, status))
        return batched_chunks

    def batch_add_items_to_queue(
        self,
        items: list[dict],
        metadata: dict = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        """Add multiple items to queue, processing in chunks for memory
        efficiency."""
        if metadata is None:
            metadata = {}
        chunks: list[list[dict]] = [
            items[i : i + batch_size] for i in range(0, len(items), batch_size)
        ]
        batched_chunks: list[tuple[str, str, str, str]] = self._create_batched_chunks(
            chunks=chunks, batch_size=batch_size, metadata=metadata
        )

        logger.info(f"Writing {len(batched_chunks)} items to DB...")

        # write to DB
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                f"""
                INSERT INTO {self.queue_table_name} (payload, metadata, created_at, status)
                VALUES (?, ?, ?, ?)
                """,
                batched_chunks,
            )

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

    def batch_remove_items_from_queue(
        self, limit: Optional[int] = None
    ) -> list[QueueItem]:
        """Remove multiple items from queue."""
        items = []
        if limit is None:
            limit = self.get_queue_length()
        for _ in range(limit):
            item = self.remove_item_from_queue()
            if item is None:
                break
            items.append(item)
        return items

    def load_items_from_queue(
        self,
        limit: Optional[int] = None,
        status: Optional[str] = None,
        min_id: Optional[int] = None,
        min_timestamp: Optional[str] = None,
    ) -> list[QueueItem]:
        """Load multiple items from queue.

        Supports a variety of filters:
        - status: filter by status
        - min_id: filter to grab all rows whose autoincremented id is greater
        than the provided id
        - min_timestamp: filter to grab all rows whose created_at is greater
        than the provided timestamp

        When "limit" is provided, it will return the first "limit" number of items
        that match the filters.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            query = "SELECT id, payload, created_at, status FROM queue"
            conditions = []
            params = []

            if status:
                conditions.append("status = ?")
                params.append(status)

            if min_id:
                conditions.append("id > ?")
                params.append(min_id)

            if min_timestamp:
                conditions.append("created_at > ?")
                params.append(min_timestamp)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            if limit:
                query += " LIMIT ?"
                params.append(limit)

            cursor.execute(query, params)
            rows = cursor.fetchall()

            items = []
            for row in rows:
                item = QueueItem(
                    id=row[0], payload=row[1], created_at=row[2], status=row[3]
                )
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
