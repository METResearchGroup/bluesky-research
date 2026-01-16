"""Generic class for implementing queues.

Under the hood, uses SQLite to implement queues.

Each queue will have their own SQLite instance in order to scale
each queue independently.
"""

import aiosqlite
import json
import os
import sqlite3
import time
from typing import Iterable, Optional
import re

from pydantic import BaseModel, Field, field_validator
import typing_extensions as te

from lib.db.queue_constants import NAME_TO_QUEUE_NAME_MAP
from lib.load_env_vars import EnvVarsContainer
from lib.datetime_utils import generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__file__)

bsky_data_dir = EnvVarsContainer.get_env_var("BSKY_DATA_DIR")
if not bsky_data_dir:
    raise ValueError("BSKY_DATA_DIR must be set to use lib.db.queue")

root_db_path = os.path.join(bsky_data_dir, "queue")
if not os.path.exists(root_db_path):
    logger.info(f"Creating new directory for queue data at {root_db_path}...")
    # exist_ok=True handles TOCTOU race condition when parallel test workers
    # concurrently import this module and attempt to create the directory
    os.makedirs(root_db_path, exist_ok=True)

existing_sqlite_dbs = [
    file for file in os.listdir(root_db_path) if file.endswith(".db")
]

DEFAULT_BATCH_CHUNK_SIZE = 1000
DEFAULT_BATCH_WRITE_SIZE = 25
INPUT_QUEUE_PREFIX = "input_"
OUTPUT_QUEUE_PREFIX = "output_"

_SQLITE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_sqlite_identifier(identifier: str) -> str:
    """Safely quote a SQLite identifier (table/column name).

    SQLite parameterization does not support identifiers, so we validate with a
    strict allowlist and then quote.
    """
    if not _SQLITE_IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Invalid SQLite identifier: {identifier!r}")
    return f'"{identifier}"'


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
    metadata: str = Field(
        default="", description="The JSON-serialized metadata for this queue item."
    )
    created_at: str = Field(
        default_factory=generate_current_datetime_str,
        description="The timestamp when the queue item was created.",
    )
    status: te.Literal["pending", "processing", "completed", "failed"] = Field(
        default="pending",
        description="The current status of the queue item. One of: 'pending', 'processing', 'completed', 'failed'.",
    )

    @field_validator("payload")
    @classmethod
    def model_validator(cls, v: str) -> str:
        """Validate the payload field.

        Args:
            v: The payload value to validate

        Returns:
            The validated payload string

        Raises:
            ValueError: If payload is empty or not valid JSON
        """
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
        temp_queue: bool = False,
        temp_queue_path: str = None,
    ):
        """Initialize a Queue instance.

        Args:
            queue_name: Name of the queue
            create_new_queue: If True, creates a new queue if it doesn't exist
            temp_queue: If True, the queue is meant to be a temporary queue so
            we don't validate the DB path or name.
            temp_queue_path: If provided, the queue is meant to be a temporary
            queue so we don't validate the DB path or name.
        """
        # Special handling for test queues
        if queue_name.startswith("test_"):
            self.queue_name = queue_name
        elif temp_queue:
            self.queue_name = queue_name
        else:
            self.queue_name = NAME_TO_QUEUE_NAME_MAP[queue_name]

        self.queue_table_name = "queue"
        self._queue_table_sql = _quote_sqlite_identifier(self.queue_table_name)

        if temp_queue and temp_queue_path:
            self.db_path = temp_queue_path
        else:
            self.db_path = os.path.join(root_db_path, f"{queue_name}.db")

        if os.path.exists(self.db_path) and create_new_queue:
            logger.info(
                f"DB for queue {queue_name} already exists. Not overwriting, using existing DB..."
            )
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

    async def _async_get_connection(self) -> sqlite3.Connection:
        """Get an optimized SQLite connection with retry logic."""
        conn = await aiosqlite.connect(self.db_path)

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
                        f"SELECT COUNT(*) FROM {self._queue_table_sql}"  # nosec B608
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

    def add_item_to_queue(self, payload: dict, metadata: Optional[dict] = None) -> None:
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
                query = f"""
                    INSERT INTO {self._queue_table_sql}
                    (payload, metadata, created_at, status)
                    VALUES (?, ?, ?, ?)
                    RETURNING id
                    """
                conn.execute(
                    query,
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
        metadata: Optional[dict] = None,
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
        metadata: Optional[dict] = None,
        batch_size: Optional[int] = DEFAULT_BATCH_CHUNK_SIZE,
        batch_write_size: Optional[int] = DEFAULT_BATCH_WRITE_SIZE,
    ) -> None:
        """Add multiple items to queue, processing in chunks for memory
        efficiency.

        Split chunks into further batches. e.g., with batch_size = 1000,
        batch_write_size = 25, then if we have 50,000 items, it will be split
        into 50 minibatches of 1,000 items each, and then it will be split into
        2 batches of 25 minibatches each.

        See https://markptorres.com/research/2025-01-31-effectiveness-of-sqlite
        for writeup.
        """
        if metadata is None:
            metadata = {}
        chunks: list[list[dict]] = [
            items[i : i + batch_size] for i in range(0, len(items), batch_size)
        ]
        minibatch_chunks: list[tuple[str, str, str, str]] = self._create_batched_chunks(
            chunks=chunks, batch_size=batch_size, metadata=metadata
        )

        batch_chunks: list[list[tuple[str, str, str, str]]] = [
            minibatch_chunks[i : i + batch_write_size]
            for i in range(0, len(minibatch_chunks), batch_write_size)
        ]

        total_items = len(items)
        total_batches = len(batch_chunks)
        total_minibatches = len(minibatch_chunks)

        logger.info(
            f"Writing {total_items} items as {total_minibatches} minibatches to DB."
        )
        logger.info(
            f"Writing {total_minibatches} minibatches to DB as {total_batches} batches..."
        )

        for i, batch_chunk in enumerate(batch_chunks):
            if i % 10 == 0:
                logger.info(f"Processing batch {i + 1}/{total_batches}...")
            with sqlite3.connect(self.db_path) as conn:
                query = f"""
                    INSERT INTO {self._queue_table_sql} (payload, metadata, created_at, status)
                    VALUES (?, ?, ?, ?)
                    """
                conn.executemany(
                    query,
                    batch_chunk,
                )
            conn.commit()

    async def async_batch_add_items_to_queue(
        self,
        items: list[dict],
        metadata: Optional[dict] = None,
        batch_size: Optional[int] = DEFAULT_BATCH_CHUNK_SIZE,
        batch_write_size: Optional[int] = DEFAULT_BATCH_WRITE_SIZE,
    ) -> None:
        """(Async) add multiple items to queue, processing in chunks for memory
        efficiency.

        Split chunks into further batches. e.g., with batch_size = 1000,
        batch_write_size = 25, then if we have 50,000 items, it will be split
        into 50 minibatches of 1,000 items each, and then it will be split into
        2 batches of 25 minibatches each.

        See https://markptorres.com/research/2025-01-31-effectiveness-of-sqlite
        for writeup.
        """
        if metadata is None:
            metadata = {}
        chunks: list[list[dict]] = [
            items[i : i + batch_size] for i in range(0, len(items), batch_size)
        ]
        minibatch_chunks: list[tuple[str, str, str, str]] = self._create_batched_chunks(
            chunks=chunks, batch_size=batch_size, metadata=metadata
        )

        batch_chunks: list[list[tuple[str, str, str, str]]] = [
            minibatch_chunks[i : i + batch_write_size]
            for i in range(0, len(minibatch_chunks), batch_write_size)
        ]

        total_items = len(items)
        total_batches = len(batch_chunks)
        total_minibatches = len(minibatch_chunks)

        logger.info(
            f"Writing {total_items} items as {total_minibatches} minibatches to DB."
        )
        logger.info(
            f"Writing {total_minibatches} minibatches to DB as {total_batches} batches..."
        )

        for i, batch_chunk in enumerate(batch_chunks):
            if i % 10 == 0:
                logger.info(f"Processing batch {i + 1}/{total_batches}...")
            conn = await self._async_get_connection()
            try:
                query = f"""
                    INSERT INTO {self._queue_table_sql} (payload, metadata, created_at, status)
                    VALUES (?, ?, ?, ?)
                    """
                await conn.executemany(
                    query,
                    batch_chunk,
                )
                await conn.commit()
            except Exception as e:
                logger.error(f"Failed to write batch {i+1}/{total_batches}: {e}")
                # Consider whether to re-raise or continue with next batch
                raise  # Re-raising will abort the entire operation
            finally:
                await conn.close()

    def remove_item_from_queue(self) -> Optional[QueueItem]:
        """Remove and return the next available item from the queue.

        Returns:
            QueueItem: The next pending item in the queue, or None if queue is empty
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                WITH next_item AS (
                    SELECT id, payload, metadata, created_at, status
                    FROM queue 
                    WHERE status = 'pending'
                    ORDER BY created_at ASC 
                    LIMIT 1
                )
                UPDATE queue
                SET status = 'processing'
                WHERE id IN (SELECT id FROM next_item)
                RETURNING id, payload, metadata, created_at, status
            """)

            row = cursor.fetchone()
            if not row:
                count = self.get_queue_length()
                logger.info(f"Queue is empty. Total items in queue: {count}")
                return None

            item = QueueItem(
                id=row[0],
                payload=row[1],
                metadata=row[2],
                created_at=row[3],
                status="processing",
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

    def batch_delete_items_by_ids(self, ids: Iterable[int]) -> int:
        """Delete multiple items from queue by their ids.

        Args:
            ids: List of queue item IDs to delete

        Returns:
            int: Number of items actually deleted
        """
        ids_list = list(ids)
        if not ids_list:
            return 0
        with sqlite3.connect(self.db_path) as conn:
            placeholders = ",".join("?" for _ in ids_list)
            query = f"DELETE FROM {self._queue_table_sql} WHERE id IN ({placeholders})"
            cursor = conn.execute(query, tuple(ids_list))
            deleted_count = cursor.rowcount
            logger.info(f"Deleted {deleted_count} items from queue.")
            return deleted_count

    def clear_queue(self) -> int:
        """Delete all items from the queue.

        Returns:
            int: Number of items deleted
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(f"DELETE FROM {self._queue_table_sql}")  # nosec B608
            deleted_count = cursor.rowcount
            logger.info(f"Cleared {deleted_count} items from queue {self.queue_name}")
            return deleted_count

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
        than the provided id. Strictly greater than.
        - min_timestamp: filter to grab all rows whose created_at is greater
        than the provided timestamp. Strictly greater than.

        When "limit" is provided, it will return the first "limit" number of items
        that match the filters.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            query = "SELECT id, payload, metadata, created_at, status FROM queue"
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
                    id=row[0],
                    payload=row[1],
                    metadata=row[2],
                    created_at=row[3],
                    status=row[4],
                )
                items.append(item)

            return items

    def load_dict_items_metadata_from_queue(self) -> list[dict]:
        """Load the metadata from the queue."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT id, metadata FROM queue")
            rows = cursor.fetchall()
            return [row[1] for row in rows]

    def load_dict_items_from_queue(
        self,
        limit: Optional[int] = None,
        status: Optional[str] = None,
        min_id: Optional[int] = None,
        min_timestamp: Optional[str] = None,
    ) -> list[dict]:
        """Loads the latest queue items from the queue and returns them
        as hydrated JSON dictionaries. Each row in the queue is batch-specific
        but we return these as a list of dictionaries where each row is a
        single record (as opposed to a batch of records).

        Each row is a JSON-dumped batch of payloads. For example, for a
        batch size of 100 posts, one row might be a JSON-dumped string of
        100 posts. Each row shares the same metadata. We want to not only
        return the posts, but also the metadata, so that we can link these
        posts to which batch row they originally came from.

        Also does deduplication of payloads (in case it happens, though it
        is highly unlikely).
        """
        latest_queue_items: list[QueueItem] = self.load_items_from_queue(
            limit=limit, status=status, min_id=min_id, min_timestamp=min_timestamp
        )
        latest_payload_batch_strings: list[str] = []
        latest_payload_batch_ids: list[str] = []
        latest_payload_batch_metadata: list[str] = []
        for item in latest_queue_items:
            latest_payload_batch_strings.append(item.payload)
            latest_payload_batch_ids.append(item.id)
            latest_payload_batch_metadata.append(item.metadata)

        # Initialize accumulator for all payload dictionaries.
        latest_payloads: list[dict] = []
        for (
            payload_string,
            payload_id,
            payload_metadata,
        ) in zip(
            latest_payload_batch_strings,
            latest_payload_batch_ids,
            latest_payload_batch_metadata,
        ):
            payloads: list[dict] = json.loads(payload_string)
            for payload in payloads:
                payload["batch_id"] = payload_id
                payload["batch_metadata"] = payload_metadata
            latest_payloads.extend(payloads)

        # deduplicate payload dictionaries
        unique_payloads = []
        seen = set()
        for payload in latest_payloads:
            # Create a canonical string representation of the payload
            canonical = json.dumps(payload, sort_keys=True)
            if canonical not in seen:
                seen.add(canonical)
                unique_payloads.append(payload)

        return unique_payloads

    def _get_sqlite_version(self) -> tuple:
        """Get the SQLite version as a tuple of integers."""
        with sqlite3.connect(self.db_path):
            version_str = sqlite3.sqlite_version
        return tuple(map(int, version_str.split(".")))

    def _supports_returning(self) -> bool:
        """Check if SQLite version supports RETURNING clause."""
        return self._get_sqlite_version() >= (3, 35, 0)

    def run_query(self, query: str, params: Optional[tuple] = None) -> list[dict]:
        """Execute a SQL query and return results as a list of dictionaries.

        This method safely executes read-only SQL queries against the queue database
        and returns the results as a list of dictionaries where each dictionary
        represents a row with column names as keys.

        The method implements several security measures:
        - Only SELECT queries are allowed
        - Unsafe operations (INSERT, UPDATE, etc.) are blocked
        - Multiple statements are rejected
        - Parameters are properly bound to prevent SQL injection
        - Transactions are isolated

        Args:
            query: The SQL query to execute. Must be a single SELECT statement.
            params: Optional tuple of parameters to bind to the query for safe
                   value substitution.

        Returns:
            List of dictionaries containing the query results, where each dict
            represents a row with column names as keys. Column types are preserved:
            - INTEGER columns return int
            - TEXT columns return str
            - NULL values return None
            Unicode characters are fully supported in both queries and results.

        Raises:
            ValueError: If the query:
                - Is not a SELECT statement
                - Contains unsafe operations
                - Contains multiple statements
            sqlite3.Error: If there is a database error during execution
        """
        if not query or not isinstance(query, str):
            raise ValueError("Query must be a non-empty string")

        # Basic safety checks
        query_lower = query.lower().strip()
        if not query_lower.startswith("select"):
            raise ValueError("Only SELECT queries are allowed")

        # Check for unsafe keywords, but only match whole words
        unsafe_keywords = ["insert", "update", "delete", "drop", "alter", "create"]
        query_words = set(query_lower.split())
        if any(keyword in query_words for keyword in unsafe_keywords):
            raise ValueError(f"Query contains unsafe operations: {query}")

        # Check for multiple statements
        if ";" in query_lower:
            # Allow semicolons in string literals and subqueries
            stripped_query = query_lower
            # Remove string literals
            stripped_query = re.sub(r"'[^']*'", "", stripped_query)
            stripped_query = re.sub(r'"[^"]*"', "", stripped_query)
            # Remove comments
            stripped_query = re.sub(r"--.*$", "", stripped_query, flags=re.MULTILINE)
            stripped_query = re.sub(r"/\*.*?\*/", "", stripped_query, flags=re.DOTALL)

            if ";" in stripped_query:
                raise ValueError("Multiple SQL statements are not allowed")

        try:
            with self._get_connection() as conn:
                # Set timeout to prevent long-running queries
                conn.execute("PRAGMA busy_timeout = 30000")  # 30 second timeout

                cursor = conn.execute(query, params or ())

                # Get column names from cursor description
                columns = [desc[0] for desc in cursor.description]

                # Convert rows to dictionaries while preserving types
                results = []
                for row in cursor.fetchall():
                    row_dict = {}
                    for i, value in enumerate(row):
                        row_dict[columns[i]] = value
                    results.append(row_dict)

                return results
        except sqlite3.Error as e:
            logger.error(f"Database error executing query: {e}")
            raise

    async def close(self) -> None:
        """Close the queue.

        This method is a placeholder to provide a consistent API.
        Since we don't maintain persistent connections in this class,
        this method is a no-op.

        We sometimes run into an edge case during garbage collection when
        Python is cleaning up objects. Having this API here helps with it.
        """
        pass

    def delete_queue(self):
        """Delete the queue."""
        os.remove(self.db_path)
        logger.info(f"Deleted queue {self.queue_name} at {self.db_path}")


def get_input_queue_name(integration_name: str) -> str:
    """Get the input queue name for a given integration name."""
    return f"{INPUT_QUEUE_PREFIX}{integration_name}"


def get_input_queue_for_integration(integration_name: str) -> Queue:
    """Get the input queue for a given integration name."""
    return Queue(
        queue_name=get_input_queue_name(integration_name),
        create_new_queue=True,
    )


def get_output_queue_name(integration_name: str) -> str:
    """Get the output queue name for a given integration name."""
    return f"{OUTPUT_QUEUE_PREFIX}{integration_name}"


def get_output_queue_for_integration(integration_name: str) -> Queue:
    """Get the output queue for a given integration name."""
    return Queue(
        queue_name=get_output_queue_name(integration_name),
        create_new_queue=True,
    )
