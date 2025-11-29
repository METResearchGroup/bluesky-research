from enum import Enum
import os
import sqlite3


from lib.helper import generate_current_datetime_str
from lib.log.logger import Logger

logger = Logger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
MIGRATION_TRACKER_DB_PATH = os.path.join(current_dir, "migration_tracker.db")


class MigrationStatus(str, Enum):
    """Status tracker for migration tracking."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class MigrationTracker:
    """Tracks migration progress using SQLite database."""

    def __init__(self, db_path: str = MIGRATION_TRACKER_DB_PATH):
        """Initialize the migration tracker.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS migration_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    local_path TEXT NOT NULL UNIQUE,
                    s3_key TEXT NOT NULL,
                    file_size_bytes INTEGER,
                    status TEXT NOT NULL DEFAULT 'pending',
                    started_at TEXT,
                    completed_at TEXT,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create index for faster status queries
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_status 
                ON migration_files(status)
            """)

            conn.commit()
            logger.info(f"Initialized migration tracker database: {self.db_path}")

    def register_files(
        self,
        files: list[dict[str, str]],
    ) -> None:
        """Register files to be migrated.

        Args:
            files: List of dicts with 'local_path' and 's3_key'
        """
        logger.info(f"Registering {len(files)} files for migration")
        with sqlite3.connect(self.db_path) as conn:
            for file_info in files:
                local_path = file_info["local_path"]
                s3_key = file_info["s3_key"]

                if not os.path.exists(local_path):
                    raise FileNotFoundError(f"File not found: {local_path}")

                file_size = os.path.getsize(local_path)

                try:
                    conn.execute(
                        """
                        INSERT INTO migration_files 
                        (local_path, s3_key, file_size_bytes, status)
                        VALUES (?, ?, ?, ?)
                    """,
                        (local_path, s3_key, file_size, MigrationStatus.PENDING.value),
                    )
                except sqlite3.IntegrityError:
                    logger.debug(f"File already registered: {local_path}")

            conn.commit()
            logger.info(f"Registered {len(files)} files for migration")

    def mark_started(self, local_path: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE migration_files
                SET status = ?, started_at = ?, updated_at = ?
                WHERE local_path = ?
            """,
                (
                    MigrationStatus.IN_PROGRESS.value,
                    generate_current_datetime_str(),
                    generate_current_datetime_str(),
                    local_path,
                ),
            )
            conn.commit()
            logger.info(f"Marked file as started: {local_path}")

    def mark_completed(self, local_path: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE migration_files
                SET status = ?, completed_at = ?, updated_at = ?
                WHERE local_path = ?
            """,
                (
                    MigrationStatus.COMPLETED.value,
                    generate_current_datetime_str(),
                    generate_current_datetime_str(),
                    local_path,
                ),
            )
            conn.commit()
            logger.info(f"Marked file as completed: {local_path}")

    def mark_failed(self, local_path: str, error_message: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE migration_files
                SET status = ?, completed_at = ?, updated_at = ?, error_message = ?
                WHERE local_path = ?
            """,
                (
                    MigrationStatus.FAILED.value,
                    generate_current_datetime_str(),
                    generate_current_datetime_str(),
                    error_message,
                    local_path,
                ),
            )
            conn.commit()
            logger.info(f"Marked file as failed: {local_path}")

    def get_pending_files(self) -> list[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT local_path, s3_key, file_size_bytes
                FROM migration_files
                WHERE status = ?
                ORDER BY created_at ASC
            """,
                (MigrationStatus.PENDING.value,),
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_in_progress_files(self) -> list[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT local_path, s3_key, file_size_bytes
                FROM migration_files
                WHERE status = ?
                ORDER BY created_at ASC
            """,
                (MigrationStatus.IN_PROGRESS.value,),
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_failed_files(self) -> list[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT local_path, s3_key, file_size_bytes, error_message
                FROM migration_files
                WHERE status = ?
                ORDER BY created_at ASC
            """,
                (MigrationStatus.FAILED.value,),
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_files_to_migrate_for_prefix(self, prefix: str) -> list[dict]:
        """Get files to migrate for a given prefix.

        Args:
            prefix: The prefix to get files for. Does a LIKE query on the local_path column.
        Returns:
            A list of dicts with 'local_path', 's3_key', and 'file_size_bytes'.
        """
        if not prefix:
            raise ValueError("Prefix cannot be empty")

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT local_path, s3_key
                FROM migration_files
                WHERE status = ? AND local_path LIKE ?
            """,
                (MigrationStatus.PENDING.value, f"%{prefix}%"),
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_summary(self) -> dict:
        """Get migration summary statistics."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT 
                    status,
                    COUNT(*) as count,
                    SUM(file_size_bytes) as total_size_bytes
                FROM migration_files
                GROUP BY status
            """)

            summary = {}
            for row in cursor.fetchall():
                summary[row["status"]] = {
                    "count": row["count"],
                    "total_size_mb": (row["total_size_bytes"] or 0) / (1024**2),
                }

            # Get overall stats
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending
                FROM migration_files
            """)
            overall = dict(cursor.fetchone())

            return {"overall": overall, "by_status": summary}

    def print_checklist(self, failed_files_preview_size: int = 10) -> None:
        """Print a human-readable checklist of migration status."""
        summary = self.get_summary()
        overall = summary["overall"]

        print("\n" + "=" * 60)
        print("MIGRATION PROGRESS CHECKLIST")
        print("=" * 60)
        print(f"Total files: {overall['total']}")
        print(f"✅ Completed: {overall['completed']}")
        print(f"❌ Failed: {overall['failed']}")
        print(f"⏳ Pending: {overall['pending']}")
        print(f"🔄 In Progress: {overall.get('in_progress', 0)}")

        if overall["total"] > 0:
            progress_pct = (overall["completed"] / overall["total"]) * 100
            print(f"\nProgress: {progress_pct:.1f}%")

        print("=" * 60 + "\n")

        # Show failed files if any
        failed = self.get_failed_files()
        if failed:
            print(f"\n❌ Failed Files ({len(failed)}):")
            for f in failed[:failed_files_preview_size]:
                print(f"  - {f['local_path']}: {f['error_message']}")
            if len(failed) > failed_files_preview_size:
                print(f"  ... and {len(failed) - failed_files_preview_size} more")
