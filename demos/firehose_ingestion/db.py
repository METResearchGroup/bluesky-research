"""SQLite database interface for Firehose records.

This module provides a high-performance database interface for storing and 
retrieving Firehose records from Bluesky. It uses SQLite with WAL mode and
other optimizations for efficient concurrent access and high throughput.
"""

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field

from lib.helper import BSKY_DATA_DIR, generate_current_datetime_str
from lib.log.logger import get_logger

logger = get_logger(__file__)

# Define the root path for firehose databases
root_db_path = os.path.join(BSKY_DATA_DIR, "firehose")
if not os.path.exists(root_db_path):
    logger.info(f"Creating new directory for firehose data at {root_db_path}...")
    os.makedirs(root_db_path)


class FirehoseRecord(BaseModel):
    """Represents a single record from the Bluesky firehose.

    Attributes:
        did: Repository DID identifier
        time_us: Timestamp in microseconds
        kind: Kind of event (commit, identity, account)
        commit_data: JSON commit data
        collection: Collection type (e.g. app.bsky.feed.post)
        created_at: Timestamp when this record was added to the database
    """
    did: str
    time_us: str
    kind: str
    commit_data: str  # JSONB (stored as string)
    collection: str
    created_at: str = Field(
        default_factory=generate_current_datetime_str,
        description="Timestamp when the record was added to the database"
    )


@dataclass
class DatabaseStats:
    """Statistics about database operations.

    Attributes:
        total_records: Total number of records in the database
        total_write_time: Total time spent writing records in seconds
        avg_write_time: Average time per record write in milliseconds
        sync_time: Time spent on sync operations in seconds
    """
    total_records: int = 0
    total_write_time: float = 0.0
    avg_write_time: float = 0.0
    sync_time: float = 0.0


class FirehoseDB:
    """High-performance SQLite database for storing Bluesky firehose records."""

    def __init__(self, db_name: str, create_new_db: bool = False):
        """Initialize a FirehoseDB instance.

        Args:
            db_name: Name of the database
            create_new_db: If True, creates a new database if it doesn't exist
        """
        self.db_name = db_name
        self.table_name = "firehose_records"
        self.db_path = os.path.join(root_db_path, f"{db_name}.db")
        self.stats = DatabaseStats()

        if os.path.exists(self.db_path) and create_new_db:
            logger.info(
                f"DB for firehose {db_name} already exists. Not overwriting, using existing DB..."
            )
        if not os.path.exists(self.db_path):
            if create_new_db:
                logger.info(f"Creating new SQLite DB for firehose {db_name}...")
                self._init_db()
            else:
                raise ValueError(
                    f"DB for firehose {db_name} doesn't exist. Need to pass in `create_new_db` if creating a new database is intended."
                )
        else:
            logger.info(f"Loading existing SQLite DB for firehose {db_name}...")
            count = self.get_record_count()
            logger.info(f"Current record count: {count} records")

    def __repr__(self):
        return f"FirehoseDB(name={self.db_name}, db_path={self.db_path})"

    def __str__(self):
        return f"FirehoseDB(name={self.db_name}, db_path={self.db_path})"

    def _init_db(self):
        """Initialize database with WAL mode and optimized settings."""
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            # Enable WAL mode before creating tables
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")  # 30 second timeout
            conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
            conn.execute("PRAGMA mmap_size=268435456")  # 256MB memory mapped I/O
            conn.execute("PRAGMA page_size=8192")  # 8KB page size

            # Add compression if SQLite version supports it
            try:
                conn.execute("PRAGMA zip_compression=true")  # Enable compression
            except sqlite3.OperationalError:
                # Older SQLite version, compression not available
                pass

            # Create the main table
            conn.execute(f"""
                CREATE TABLE {self.table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    did TEXT NOT NULL,
                    time_us TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    commit_data TEXT NOT NULL,
                    collection TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)

            # Create indexes for faster queries
            conn.execute(f"CREATE INDEX idx_{self.table_name}_did ON {self.table_name}(did)")
            conn.execute(f"CREATE INDEX idx_{self.table_name}_collection ON {self.table_name}(collection)")
            conn.execute(f"CREATE INDEX idx_{self.table_name}_time_us ON {self.table_name}(time_us)")
            conn.execute(f"CREATE INDEX idx_{self.table_name}_created_at ON {self.table_name}(created_at)")

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

    def get_record_count(self) -> int:
        """Get total number of records in the database with retry logic."""
        max_retries = 3
        retry_delay = 1.0  # seconds

        for attempt in range(max_retries):
            try:
                with self._get_connection() as conn:
                    cursor = conn.execute(
                        f"SELECT COUNT(*) FROM {self.table_name}"
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

    def add_record(self, record: Union[FirehoseRecord, Dict]) -> float:
        """Add a single record to the database.

        Args:
            record: A FirehoseRecord object or dictionary with record data

        Returns:
            Time taken to insert the record in seconds

        Raises:
            ValueError: If the record is empty or invalid
        """
        if isinstance(record, dict):
            # Convert dict to FirehoseRecord if needed
            record = FirehoseRecord(**record)

        start_time = time.time()
        
        # First sync operation - measure separately
        sync_start = time.time()
        with self._get_connection() as conn:
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        sync_time = time.time() - sync_start
        self.stats.sync_time += sync_time
        
        # Record insertion
        try:
            with self._get_connection() as conn:
                conn.execute(
                    f"""
                    INSERT INTO {self.table_name} 
                    (did, time_us, kind, commit_data, collection, created_at) 
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.did,
                        record.time_us,
                        record.kind,
                        record.commit_data,
                        record.collection,
                        generate_current_datetime_str(),
                    ),
                )
        except Exception as e:
            logger.error(f"Error adding record to database: {e}")
            raise e

        elapsed = time.time() - start_time
        
        # Update stats
        self.stats.total_records += 1
        self.stats.total_write_time += elapsed
        self.stats.avg_write_time = (self.stats.total_write_time * 1000) / self.stats.total_records
        
        return elapsed

    def batch_add_records(
        self, 
        records: List[Union[FirehoseRecord, Dict]], 
        batch_size: int = 1000,
        batch_write_size: int = 25
    ) -> float:
        """Add multiple records to the database, processing in chunks for memory efficiency.

        Args:
            records: List of FirehoseRecord objects or dictionaries
            batch_size: Size of each batch chunk
            batch_write_size: Number of chunks to write in a single transaction

        Returns:
            Time taken to insert all records in seconds
        """
        if not records:
            return 0.0

        start_time = time.time()
        
        # Convert dicts to FirehoseRecord objects if needed
        formatted_records = []
        for record in records:
            if isinstance(record, dict):
                record = FirehoseRecord(**record)
            formatted_records.append((
                record.did,
                record.time_us,
                record.kind,
                record.commit_data,
                record.collection,
                generate_current_datetime_str(),
            ))
        
        # Split into chunks
        chunks = [
            formatted_records[i:i + batch_size] 
            for i in range(0, len(formatted_records), batch_size)
        ]
        
        # Further split chunks into batches
        batch_chunks = [
            chunks[i:i + batch_write_size]
            for i in range(0, len(chunks), batch_write_size)
        ]
        
        total_records = len(formatted_records)
        total_batches = len(batch_chunks)
        total_chunks = len(chunks)
        
        logger.info(f"Writing {total_records} records as {total_chunks} chunks in {total_batches} batches...")
        
        # First sync operation - measure separately
        sync_start = time.time()
        with self._get_connection() as conn:
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        sync_time = time.time() - sync_start
        self.stats.sync_time += sync_time

        # Process each batch
        for i, batch in enumerate(batch_chunks):
            if i % 10 == 0:
                logger.info(f"Processing batch {i + 1}/{total_batches}...")
            
            with self._get_connection() as conn:
                for chunk in batch:
                    conn.executemany(
                        f"""
                        INSERT INTO {self.table_name} 
                        (did, time_us, kind, commit_data, collection, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        chunk,
                    )
            
        elapsed = time.time() - start_time
        
        # Update stats
        self.stats.total_records += total_records
        self.stats.total_write_time += elapsed
        self.stats.avg_write_time = (self.stats.total_write_time * 1000) / self.stats.total_records
        
        return elapsed

    def get_records(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        kind: Optional[str] = None,
        collection: Optional[str] = None,
        did: Optional[str] = None,
        min_time_us: Optional[str] = None,
        min_id: Optional[int] = None,
        min_timestamp: Optional[str] = None,
    ) -> List[FirehoseRecord]:
        """Get records from the database with various filtering options.

        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            kind: Filter by event kind (commit, identity, account)
            collection: Filter by collection type
            did: Filter by repository DID
            min_time_us: Filter records with time_us greater than this value
            min_id: Filter records with ID greater than this value
            min_timestamp: Filter records created after this timestamp

        Returns:
            List of FirehoseRecord objects matching the criteria
        """
        with self._get_connection() as conn:
            query = f"""
                SELECT id, did, time_us, kind, commit_data, collection, created_at 
                FROM {self.table_name}
            """
            
            conditions = []
            params = []
            
            if kind:
                conditions.append("kind = ?")
                params.append(kind)
                
            if collection:
                conditions.append("collection = ?")
                params.append(collection)
                
            if did:
                conditions.append("did = ?")
                params.append(did)
                
            if min_time_us:
                conditions.append("time_us > ?")
                params.append(min_time_us)
                
            if min_id:
                conditions.append("id > ?")
                params.append(min_id)
                
            if min_timestamp:
                conditions.append("created_at > ?")
                params.append(min_timestamp)
                
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
                
            query += " ORDER BY id ASC"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
                
            if offset:
                query += " OFFSET ?"
                params.append(offset)
                
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
            
            records = []
            for row in rows:
                record = FirehoseRecord(
                    did=row[1],
                    time_us=row[2],
                    kind=row[3],
                    commit_data=row[4],
                    collection=row[5],
                    created_at=row[6],
                )
                records.append(record)
                
            return records

    def clear_database(self) -> int:
        """Delete all records from the database.
        
        Returns:
            Number of records deleted
        """
        with self._get_connection() as conn:
            cursor = conn.execute(f"DELETE FROM {self.table_name}")
            deleted_count = cursor.rowcount
            logger.info(f"Cleared {deleted_count} records from database {self.db_name}")
            
            # Reset stats
            self.stats = DatabaseStats()
            
            return deleted_count

    def get_stats(self) -> DatabaseStats:
        """Get current database statistics.
        
        Returns:
            DatabaseStats object with current statistics
        """
        # Make sure the stats are up-to-date with the actual record count
        self.stats.total_records = self.get_record_count()
        return self.stats
