"""Tests for scripts.migrate_research_data_to_s3.migration_tracker module."""

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

from scripts.migrate_research_data_to_s3.migration_tracker import (
    MigrationStatus,
    MigrationTracker,
)


class TestMigrationTracker:
    """Tests for MigrationTracker class."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database file for testing."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        yield db_path
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)

    @pytest.fixture
    def tracker(self, temp_db_path):
        """Create a MigrationTracker instance with a temporary database."""
        return MigrationTracker(db_path=temp_db_path)

    @pytest.fixture
    def temp_file(self):
        """Create a temporary file for testing."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("test content")
            temp_path = f.name
        yield temp_path
        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)

    def test_init_creates_database_schema(self, temp_db_path):
        """Test that initialization creates the database schema."""
        tracker = MigrationTracker(db_path=temp_db_path)
        
        # Verify table exists
        with sqlite3.connect(temp_db_path) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='migration_files'"
            )
            assert cursor.fetchone() is not None

    def test_init_creates_indexes(self, temp_db_path):
        """Test that initialization creates all required indexes."""
        tracker = MigrationTracker(db_path=temp_db_path)
        
        with sqlite3.connect(temp_db_path) as conn:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
            indexes = [row[0] for row in cursor.fetchall()]
            
            assert "idx_status" in indexes
            assert "idx_local_path" in indexes
            assert "idx_s3_key" in indexes
            assert "idx_status_local_path" in indexes

    def test_register_files_adds_pending_files(self, tracker, temp_file):
        """Test that register_files adds files with pending status."""
        files = [
            {"local_path": temp_file, "s3_key": "s3/test/file.txt"}
        ]
        tracker.register_files(files)
        
        pending = tracker.get_pending_files()
        assert len(pending) == 1
        assert pending[0]["local_path"] == temp_file
        assert pending[0]["s3_key"] == "s3/test/file.txt"

    def test_register_files_skips_empty_files(self, tracker, temp_file):
        """Test that register_files marks empty files as skipped."""
        # Create empty file
        empty_file = temp_file + ".empty"
        Path(empty_file).touch()
        
        try:
            files = [
                {"local_path": temp_file, "s3_key": "s3/test/file.txt"},
                {"local_path": empty_file, "s3_key": "s3/test/empty.txt"},
            ]
            tracker.register_files(files)
            
            pending = tracker.get_pending_files()
            assert len(pending) == 1
            assert pending[0]["local_path"] == temp_file
            
            # Check skipped status
            with sqlite3.connect(tracker.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT status FROM migration_files WHERE local_path = ?",
                    (empty_file,)
                )
                row = cursor.fetchone()
                assert row is not None
                assert row["status"] == MigrationStatus.SKIPPED.value
        finally:
            if os.path.exists(empty_file):
                os.unlink(empty_file)

    def test_register_files_raises_on_missing_file(self, tracker):
        """Test that register_files raises FileNotFoundError for missing files."""
        files = [
            {"local_path": "/nonexistent/file.txt", "s3_key": "s3/test/file.txt"}
        ]
        
        with pytest.raises(FileNotFoundError):
            tracker.register_files(files)

    def test_register_files_handles_duplicates(self, tracker, temp_file):
        """Test that register_files handles duplicate registrations gracefully."""
        files = [
            {"local_path": temp_file, "s3_key": "s3/test/file.txt"}
        ]
        
        # Register twice
        tracker.register_files(files)
        tracker.register_files(files)  # Should not raise
        
        # Should only have one entry
        pending = tracker.get_pending_files()
        assert len(pending) == 1

    def test_mark_started_updates_status(self, tracker, temp_file):
        """Test that mark_started updates file status to in_progress."""
        files = [{"local_path": temp_file, "s3_key": "s3/test/file.txt"}]
        tracker.register_files(files)
        
        tracker.mark_started(temp_file)
        
        in_progress = tracker.get_in_progress_files()
        assert len(in_progress) == 1
        assert in_progress[0]["local_path"] == temp_file
        
        # Pending should be empty
        pending = tracker.get_pending_files()
        assert len(pending) == 0

    def test_mark_completed_updates_status(self, tracker, temp_file):
        """Test that mark_completed updates file status to completed."""
        files = [{"local_path": temp_file, "s3_key": "s3/test/file.txt"}]
        tracker.register_files(files)
        tracker.mark_started(temp_file)
        tracker.mark_completed(temp_file)
        
        # Check status in database
        with sqlite3.connect(tracker.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT status FROM migration_files WHERE local_path = ?",
                (temp_file,)
            )
            row = cursor.fetchone()
            assert row is not None
            assert row["status"] == MigrationStatus.COMPLETED.value

    def test_mark_failed_updates_status_with_error(self, tracker, temp_file):
        """Test that mark_failed updates file status and stores error message."""
        files = [{"local_path": temp_file, "s3_key": "s3/test/file.txt"}]
        tracker.register_files(files)
        tracker.mark_started(temp_file)
        
        error_message = "Test error message"
        tracker.mark_failed(temp_file, error_message)
        
        failed = tracker.get_failed_files()
        assert len(failed) == 1
        assert failed[0]["local_path"] == temp_file
        assert failed[0]["error_message"] == error_message

    def test_get_files_to_migrate_for_prefix_includes_pending_and_in_progress(
        self, tracker, temp_file, tmp_path
    ):
        """Test that get_files_to_migrate_for_prefix includes both pending and in_progress files."""
        # Patch root_local_data_directory to point to tmp_path
        import scripts.migrate_research_data_to_s3.migration_tracker as mt_module
        original_root = mt_module.root_local_data_directory
        mt_module.root_local_data_directory = str(tmp_path)
        
        try:
            # Create files in a test directory structure
            test_dir = tmp_path / "test_prefix"
            test_dir.mkdir()
            
            file1 = test_dir / "file1.txt"
            file1.write_text("content1")
            file2 = test_dir / "file2.txt"
            file2.write_text("content2")
            
            files = [
                {"local_path": str(file1), "s3_key": "s3/test/file1.txt"},
                {"local_path": str(file2), "s3_key": "s3/test/file2.txt"},
            ]
            tracker.register_files(files)
            
            # Mark one as in_progress
            tracker.mark_started(str(file1))
            
            # Get files for prefix (relative to root_local_data_directory)
            files_to_migrate = tracker.get_files_to_migrate_for_prefix("test_prefix")
            
            # Should include both pending and in_progress
            assert len(files_to_migrate) == 2
            paths = {f["local_path"] for f in files_to_migrate}
            assert str(file1) in paths
            assert str(file2) in paths
        finally:
            mt_module.root_local_data_directory = original_root

    def test_get_files_to_migrate_for_prefix_raises_on_empty_prefix(self, tracker):
        """Test that get_files_to_migrate_for_prefix raises ValueError for empty prefix."""
        with pytest.raises(ValueError, match="Prefix cannot be empty"):
            tracker.get_files_to_migrate_for_prefix("")

    def test_get_summary_returns_correct_statistics(self, tracker, temp_file):
        """Test that get_summary returns correct statistics."""
        files = [
            {"local_path": temp_file, "s3_key": "s3/test/file.txt"}
        ]
        tracker.register_files(files)
        tracker.mark_started(temp_file)
        tracker.mark_completed(temp_file)
        
        summary = tracker.get_summary()
        
        assert "overall" in summary
        assert "by_status" in summary
        assert summary["overall"]["total"] == 1
        assert summary["overall"]["completed"] == 1
        assert summary["by_status"]["completed"]["count"] == 1

    def test_get_status_counts_for_prefix_returns_all_statuses(
        self, tracker, temp_file, tmp_path
    ):
        """Test that get_status_counts_for_prefix returns counts for all statuses."""
        # Patch root_local_data_directory to point to tmp_path
        import scripts.migrate_research_data_to_s3.migration_tracker as mt_module
        original_root = mt_module.root_local_data_directory
        mt_module.root_local_data_directory = str(tmp_path)
        
        try:
            # Create files in test directory
            test_dir = tmp_path / "test_prefix"
            test_dir.mkdir()
            
            file1 = test_dir / "file1.txt"
            file1.write_text("content1")
            file2 = test_dir / "file2.txt"
            file2.write_text("content2")
            
            files = [
                {"local_path": str(file1), "s3_key": "s3/test/file1.txt"},
                {"local_path": str(file2), "s3_key": "s3/test/file2.txt"},
            ]
            tracker.register_files(files)
            
            tracker.mark_started(str(file1))
            tracker.mark_completed(str(file1))
            tracker.mark_failed(str(file2), "test error")
            
            # Get status counts for prefix (relative to root_local_data_directory)
            status_counts = tracker.get_status_counts_for_prefix("test_prefix")
            
            # Should have all statuses, even if count is 0
            assert "pending" in status_counts
            assert "in_progress" in status_counts
            assert "completed" in status_counts
            assert "failed" in status_counts
            assert "skipped" in status_counts
            assert status_counts["completed"] == 1
            assert status_counts["failed"] == 1
        finally:
            mt_module.root_local_data_directory = original_root

    def test_check_constraint_enforces_valid_status(self, temp_db_path):
        """Test that CHECK constraint prevents invalid status values."""
        tracker = MigrationTracker(db_path=temp_db_path)
        
        # Try to insert invalid status
        with sqlite3.connect(temp_db_path) as conn:
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    """
                    INSERT INTO migration_files 
                    (local_path, s3_key, status)
                    VALUES (?, ?, ?)
                    """,
                    ("/test/path.txt", "s3/test.txt", "invalid_status"),
                )
                conn.commit()

    def test_print_checklist_output(self, tracker, temp_file, capsys):
        """Test that print_checklist produces expected output."""
        files = [
            {"local_path": temp_file, "s3_key": "s3/test/file.txt"}
        ]
        tracker.register_files(files)
        tracker.mark_completed(temp_file)
        
        tracker.print_checklist()
        
        captured = capsys.readouterr()
        assert "MIGRATION PROGRESS CHECKLIST" in captured.out
        assert "Total files:" in captured.out
        assert "Completed:" in captured.out

    def test_get_failed_files_includes_error_message(self, tracker, temp_file):
        """Test that get_failed_files includes error_message in results."""
        files = [{"local_path": temp_file, "s3_key": "s3/test/file.txt"}]
        tracker.register_files(files)
        tracker.mark_started(temp_file)
        
        error_msg = "Connection timeout"
        tracker.mark_failed(temp_file, error_msg)
        
        failed = tracker.get_failed_files()
        assert len(failed) == 1
        assert "error_message" in failed[0]
        assert failed[0]["error_message"] == error_msg

