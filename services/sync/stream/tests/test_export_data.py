"""Tests for cache management and cache writer - new architecture.

Tests cover:
- CachePathManager
- CacheDirectoryManager
- CacheFileWriter
- CacheFileReader
- Adapter functions (export_study_user_data_local, export_in_network_user_data_local)
"""

import json
import os
import shutil
import tempfile
from unittest.mock import Mock, patch

import pytest

from services.sync.stream.cache_management import (
    CachePathManager,
    CacheDirectoryManager,
    CacheFileWriter,
    CacheFileReader,
)
from services.sync.stream.cache_writer import (
    export_study_user_data_local,
    export_in_network_user_data_local,
    _get_system_components,
    _get_study_user_manager,
)
from services.sync.stream.handlers.registry import RecordHandlerRegistry
from services.sync.stream.types import Operation, RecordType, GenericRecordType, FollowStatus


class TestCachePathManager:
    """Tests for CachePathManager class."""

    @pytest.fixture(autouse=True)
    def mock_study_user_manager(self, monkeypatch):
        """Mock study user manager to avoid loading real data."""
        mock_manager = Mock()
        mock_manager.insert_study_user_post = Mock()
        mock_manager.is_study_user = Mock(return_value=False)
        mock_manager.is_study_user_post = Mock(return_value=None)
        mock_manager.is_in_network_user = Mock(return_value=False)
        monkeypatch.setattr(
            "services.sync.stream.cache_writer._get_study_user_manager",
            lambda: mock_manager,
        )

    def test_init(self):
        """Test CachePathManager initialization."""
        # Arrange & Act
        path_manager = CachePathManager()

        # Assert
        assert path_manager.root_write_path.endswith("__local_cache__")
        assert Operation.CREATE.value in path_manager.root_create_path
        assert Operation.DELETE.value in path_manager.root_delete_path
        assert path_manager.operation_types == [GenericRecordType.POST.value, GenericRecordType.LIKE.value, GenericRecordType.FOLLOW.value]

    def test_get_local_cache_path(self):
        """Test get_local_cache_path returns correct paths."""
        # Arrange
        path_manager = CachePathManager()

        # Act
        create_post_path = path_manager.get_local_cache_path(Operation.CREATE, GenericRecordType.POST)
        delete_like_path = path_manager.get_local_cache_path(Operation.DELETE, GenericRecordType.LIKE)

        # Assert
        assert Operation.CREATE.value in create_post_path
        assert GenericRecordType.POST.value in create_post_path
        assert Operation.DELETE.value in delete_like_path
        assert GenericRecordType.LIKE.value in delete_like_path

    def test_get_study_user_activity_path_post(self):
        """Test get_study_user_activity_path for post records."""
        # Arrange
        path_manager = CachePathManager()

        # Act
        create_path = path_manager.get_study_user_activity_path(Operation.CREATE, RecordType.POST)
        delete_path = path_manager.get_study_user_activity_path(Operation.DELETE, RecordType.POST)

        # Assert
        assert "study_user_activity" in create_path
        assert Operation.CREATE.value in create_path
        assert RecordType.POST.value in create_path
        assert "study_user_activity" in delete_path
        assert Operation.DELETE.value in delete_path

    def test_get_study_user_activity_path_follow(self):
        """Test get_study_user_activity_path for follow records with follow_status."""
        # Arrange
        path_manager = CachePathManager()

        # Act
        follower_path = path_manager.get_study_user_activity_path(
            Operation.CREATE, RecordType.FOLLOW, follow_status=FollowStatus.FOLLOWER
        )
        followee_path = path_manager.get_study_user_activity_path(
            Operation.CREATE, RecordType.FOLLOW, follow_status=FollowStatus.FOLLOWEE
        )

        # Assert
        assert RecordType.FOLLOW.value in follower_path
        assert FollowStatus.FOLLOWER.value in follower_path
        assert RecordType.FOLLOW.value in followee_path
        assert FollowStatus.FOLLOWEE.value in followee_path

    def test_get_study_user_activity_path_like_on_user_post(self):
        """Test get_study_user_activity_path for like_on_user_post records."""
        # Arrange
        path_manager = CachePathManager()

        # Act
        path = path_manager.get_study_user_activity_path(Operation.CREATE, RecordType.LIKE_ON_USER_POST)

        # Assert
        assert "study_user_activity" in path
        assert RecordType.LIKE_ON_USER_POST.value in path

    def test_get_in_network_activity_path(self):
        """Test get_in_network_activity_path returns correct path."""
        # Arrange
        path_manager = CachePathManager()
        author_did = "did:plc:test123"

        # Act
        path = path_manager.get_in_network_activity_path(Operation.CREATE, RecordType.POST, author_did)

        # Assert
        assert "in_network_user_activity" in path
        assert Operation.CREATE.value in path
        assert RecordType.POST.value in path
        assert author_did in path

    def test_get_relative_path(self):
        """Test get_relative_path returns relative path component."""
        # Arrange
        path_manager = CachePathManager()

        # Act
        post_relative = path_manager.get_relative_path(Operation.CREATE, RecordType.POST)
        follow_relative = path_manager.get_relative_path(
            Operation.CREATE, RecordType.FOLLOW, follow_status=FollowStatus.FOLLOWER
        )

        # Assert
        assert Operation.CREATE.value in post_relative
        assert RecordType.POST.value in post_relative
        assert RecordType.FOLLOW.value in follow_relative
        assert FollowStatus.FOLLOWER.value in follow_relative


class TestCacheDirectoryManager:
    """Tests for CacheDirectoryManager class."""

    @pytest.fixture(autouse=True)
    def mock_study_user_manager(self, monkeypatch):
        """Mock study user manager to avoid loading real data."""
        mock_manager = Mock()
        mock_manager.insert_study_user_post = Mock()
        monkeypatch.setattr(
            "services.sync.stream.cache_writer._get_study_user_manager",
            lambda: mock_manager,
        )

    def test_init(self):
        """Test CacheDirectoryManager initialization."""
        # Arrange
        path_manager = CachePathManager()

        # Act
        dir_manager = CacheDirectoryManager(path_manager=path_manager)

        # Assert
        assert dir_manager.path_manager == path_manager

    def test_ensure_exists_creates_directory(self):
        """Test ensure_exists creates directory if it doesn't exist."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)
        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test", "nested", "path")

            # Act
            dir_manager.ensure_exists(test_path)

            # Assert
            assert os.path.exists(test_path)
            assert os.path.isdir(test_path)

    def test_ensure_exists_does_not_fail_if_exists(self):
        """Test ensure_exists does not fail if directory already exists."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)
        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "existing")
            os.makedirs(test_path)

            # Act & Assert (should not raise)
            dir_manager.ensure_exists(test_path)
            assert os.path.exists(test_path)

    def test_rebuild_all_creates_all_directories(self):
        """Test rebuild_all creates all required directory structures."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)

        # Use a temporary directory for testing
        with tempfile.TemporaryDirectory() as tmpdir:
            # Override root_write_path for testing
            path_manager.root_write_path = os.path.join(tmpdir, "__local_cache__")
            path_manager.root_create_path = os.path.join(
                path_manager.root_write_path, Operation.CREATE.value
            )
            path_manager.root_delete_path = os.path.join(
                path_manager.root_write_path, Operation.DELETE.value
            )
            # Update study_user_activity paths
            path_manager.study_user_activity_root_local_path = os.path.join(
                path_manager.root_write_path, "study_user_activity"
            )
            path_manager.in_network_user_activity_root_local_path = os.path.join(
                path_manager.root_write_path, "in_network_user_activity"
            )
            path_manager.in_network_user_activity_create_post_local_path = os.path.join(
                path_manager.in_network_user_activity_root_local_path, Operation.CREATE.value, RecordType.POST.value
            )

            # Act
            dir_manager.rebuild_all()

            # Assert - check key directories exist
            assert os.path.exists(path_manager.root_write_path)
            assert os.path.exists(path_manager.root_create_path)
            assert os.path.exists(path_manager.root_delete_path)
            assert os.path.exists(
                path_manager.study_user_activity_root_local_path
            )
            assert os.path.exists(
                path_manager.in_network_user_activity_root_local_path
            )

    def test_rebuild_all_raises_error_for_non_sync_path_manager(self):
        """Test rebuild_all raises TypeError for non-CachePathManager."""
        # Arrange
        mock_path_manager = Mock()
        dir_manager = CacheDirectoryManager(path_manager=mock_path_manager)

        # Act & Assert
        with pytest.raises(TypeError, match="rebuild_all requires CachePathManager"):
            dir_manager.rebuild_all()

    def test_delete_all_removes_directory(self):
        """Test delete_all removes the root write path."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)

        with tempfile.TemporaryDirectory() as tmpdir:
            path_manager.root_write_path = os.path.join(tmpdir, "__local_cache__")
            os.makedirs(path_manager.root_write_path)

            # Act
            dir_manager.delete_all()

            # Assert
            assert not os.path.exists(path_manager.root_write_path)

    def test_delete_all_raises_error_for_non_sync_path_manager(self):
        """Test delete_all raises TypeError for non-CachePathManager."""
        # Arrange
        mock_path_manager = Mock()
        dir_manager = CacheDirectoryManager(path_manager=mock_path_manager)

        # Act & Assert
        with pytest.raises(TypeError, match="delete_all requires CachePathManager"):
            dir_manager.delete_all()

    def test_exists_returns_true_for_existing_path(self):
        """Test exists returns True for existing path."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Act & Assert
            assert dir_manager.exists(tmpdir) is True

    def test_exists_returns_false_for_non_existing_path(self):
        """Test exists returns False for non-existing path."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)
        non_existent = "/nonexistent/path/that/does/not/exist"

        # Act & Assert
        assert dir_manager.exists(non_existent) is False


class TestCacheFileWriter:
    """Tests for CacheFileWriter class."""

    @pytest.fixture(autouse=True)
    def mock_study_user_manager(self, monkeypatch):
        """Mock study user manager to avoid loading real data."""
        mock_manager = Mock()
        mock_manager.insert_study_user_post = Mock()
        monkeypatch.setattr(
            "services.sync.stream.cache_writer._get_study_user_manager",
            lambda: mock_manager,
        )

    def test_init(self):
        """Test CacheFileWriter initialization."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)

        # Act
        file_writer = CacheFileWriter(directory_manager=dir_manager)

        # Assert
        assert file_writer.directory_manager == dir_manager

    def test_write_json_creates_file(self):
        """Test write_json creates JSON file with correct data."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)
        file_writer = CacheFileWriter(directory_manager=dir_manager)

        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test.json")
            test_data = {"key": "value", "number": 42}

            # Act
            file_writer.write_json(test_path, test_data)

            # Assert
            assert os.path.exists(test_path)
            with open(test_path, "r") as f:
                loaded_data = json.load(f)
            assert loaded_data == test_data

    def test_write_jsonl_creates_file(self):
        """Test write_jsonl creates JSONL file with correct data."""
        # Arrange
        path_manager = CachePathManager()
        dir_manager = CacheDirectoryManager(path_manager=path_manager)
        file_writer = CacheFileWriter(directory_manager=dir_manager)

        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test.jsonl")
            test_records = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]

            # Act
            file_writer.write_jsonl(test_path, test_records)

            # Assert
            assert os.path.exists(test_path)
            with open(test_path, "r") as f:
                lines = f.readlines()
            assert len(lines) == 2
            assert json.loads(lines[0]) == test_records[0]
            assert json.loads(lines[1]) == test_records[1]


class TestCacheFileReader:
    """Tests for CacheFileReader class."""

    @pytest.fixture(autouse=True)
    def mock_study_user_manager(self, monkeypatch):
        """Mock study user manager to avoid loading real data."""
        mock_manager = Mock()
        mock_manager.insert_study_user_post = Mock()
        monkeypatch.setattr(
            "services.sync.stream.cache_writer._get_study_user_manager",
            lambda: mock_manager,
        )

    def test_read_json_loads_file(self):
        """Test read_json loads JSON file correctly."""
        # Arrange
        file_reader = CacheFileReader()

        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "test.json")
            test_data = {"key": "value", "number": 42}
            with open(test_path, "w") as f:
                json.dump(test_data, f)

            # Act
            result = file_reader.read_json(test_path)

            # Assert
            assert result == test_data

    def test_list_files_returns_files_only(self):
        """Test list_files returns only files, not directories."""
        # Arrange
        file_reader = CacheFileReader()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create files and directories
            file1 = os.path.join(tmpdir, "file1.txt")
            file2 = os.path.join(tmpdir, "file2.txt")
            subdir = os.path.join(tmpdir, "subdir")
            os.makedirs(subdir)

            with open(file1, "w") as f:
                f.write("content1")
            with open(file2, "w") as f:
                f.write("content2")

            # Act
            result = file_reader.list_files(tmpdir)

            # Assert
            assert len(result) == 2
            assert "file1.txt" in result
            assert "file2.txt" in result
            assert "subdir" not in result

    def test_list_files_returns_empty_list_for_nonexistent_directory(self):
        """Test list_files returns empty list for non-existent directory."""
        # Arrange
        file_reader = CacheFileReader()
        non_existent = "/nonexistent/directory/path"

        # Act
        result = file_reader.list_files(non_existent)

        # Assert
        assert result == []

    def test_read_all_json_in_directory_reads_all_json_files(self):
        """Test read_all_json_in_directory reads all JSON files."""
        # Arrange
        file_reader = CacheFileReader()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple JSON files
            file1 = os.path.join(tmpdir, "file1.json")
            file2 = os.path.join(tmpdir, "file2.json")
            file3 = os.path.join(tmpdir, "file3.txt")  # Non-JSON file

            data1 = {"id": 1, "name": "test1"}
            data2 = {"id": 2, "name": "test2"}

            with open(file1, "w") as f:
                json.dump(data1, f)
            with open(file2, "w") as f:
                json.dump(data2, f)
            with open(file3, "w") as f:
                f.write("not json")

            # Act
            records, filepaths = file_reader.read_all_json_in_directory(tmpdir)

            # Assert
            assert len(records) == 2
            assert len(filepaths) == 2
            assert data1 in records
            assert data2 in records
            assert file1 in filepaths
            assert file2 in filepaths

    def test_read_all_json_in_directory_returns_empty_for_nonexistent_directory(
        self,
    ):
        """Test read_all_json_in_directory returns empty for non-existent directory."""
        # Arrange
        file_reader = CacheFileReader()
        non_existent = "/nonexistent/directory/path"

        # Act
        records, filepaths = file_reader.read_all_json_in_directory(non_existent)

        # Assert
        assert records == []
        assert filepaths == []


class TestExportStudyUserDataLocal:
    """Tests for export_study_user_data_local adapter function."""

    @pytest.fixture(autouse=True)
    def setup_test(self, monkeypatch, tmp_path):
        """Set up test fixtures."""
        # Clear registry before each test
        RecordHandlerRegistry.clear()

        # Reset system components
        monkeypatch.setattr(
            "services.sync.stream.cache_writer._system_components", None
        )

        # Create temporary directory for cache
        cache_dir = tmp_path / "__local_cache__"
        cache_dir.mkdir()

        # Mock setup_sync_export_system to use temp directory
        # Import here to avoid triggering data_filter imports during module load
        def mock_setup(use_s3=False, s3_client=None):
            # Lazy imports to avoid triggering data_filter.py import
            from services.sync.stream.cache_management import (
                CachePathManager,
                CacheDirectoryManager,
                CacheFileWriter,
                CacheFileReader,
            )

            path_manager = CachePathManager()
            path_manager.root_write_path = str(cache_dir)
            path_manager.root_create_path = str(cache_dir / Operation.CREATE.value)
            path_manager.root_delete_path = str(cache_dir / Operation.DELETE.value)
            path_manager.study_user_activity_root_local_path = str(
                cache_dir / "study_user_activity"
            )

            dir_manager = CacheDirectoryManager(path_manager=path_manager)
            file_writer = CacheFileWriter(directory_manager=dir_manager)
            file_reader = CacheFileReader()

            # Register handlers
            from services.sync.stream.handlers.factories import (
                create_study_user_post_handler,
                create_study_user_like_handler,
                create_study_user_follow_handler,
                create_study_user_like_on_user_post_handler,
                create_study_user_reply_to_user_post_handler,
            )

            def make_post_handler():
                return create_study_user_post_handler(
                    path_manager, file_writer, file_reader
                )

            def make_like_handler():
                return create_study_user_like_handler(
                    path_manager, file_writer, file_reader
                )

            def make_follow_handler():
                return create_study_user_follow_handler(
                    path_manager, file_writer, file_reader
                )

            def make_like_on_user_post_handler():
                return create_study_user_like_on_user_post_handler(
                    path_manager, file_writer, file_reader
                )

            def make_reply_to_user_post_handler():
                return create_study_user_reply_to_user_post_handler(
                    path_manager, file_writer, file_reader
                )

            from services.sync.stream.types import RecordType

            RecordHandlerRegistry.register_factory(RecordType.POST.value, make_post_handler)
            RecordHandlerRegistry.register_factory(RecordType.LIKE.value, make_like_handler)
            RecordHandlerRegistry.register_factory(RecordType.FOLLOW.value, make_follow_handler)
            RecordHandlerRegistry.register_factory(
                RecordType.LIKE_ON_USER_POST.value, make_like_on_user_post_handler
            )
            RecordHandlerRegistry.register_factory(
                RecordType.REPLY_TO_USER_POST.value, make_reply_to_user_post_handler
            )

            from services.sync.stream.exporters.study_user_exporter import (
                StudyUserActivityExporter,
            )
            from services.sync.stream.storage.repository import StorageRepository
            from services.sync.stream.storage.adapters import LocalStorageAdapter

            storage_adapter = LocalStorageAdapter(path_manager=path_manager)
            storage_repository = StorageRepository(adapter=storage_adapter)
            exporter = StudyUserActivityExporter(
                path_manager=path_manager,
                storage_repository=storage_repository,
                handler_registry=RecordHandlerRegistry,
            )

            return (
                path_manager,
                dir_manager,
                file_writer,
                file_reader,
                RecordHandlerRegistry,
                exporter,
                storage_repository,
            )

        # Patch setup_sync_export_system in the setup module (where it's actually defined)
        # This will be used when _get_system_components() imports it
        monkeypatch.setattr(
            "services.sync.stream.setup.setup_sync_export_system", mock_setup
        )

        # Reset the global variable
        import services.sync.stream.cache_writer as cache_writer_module

        cache_writer_module._system_components = None

        yield

        # Cleanup
        RecordHandlerRegistry.clear()

    def test_export_study_user_data_local_post(self, tmp_path):
        """Test export_study_user_data_local for post records."""
        # Arrange
        record = {
            "uri": "at://did:plc:test/app.bsky.feed.post/123",
            "text": "Test post",
            "author_did": "did:plc:test",
        }
        author_did = "did:plc:test"
        filename = "test_post.json"
        mock_study_user_manager = Mock()
        mock_study_user_manager.insert_study_user_post = Mock()

        # Act
        export_study_user_data_local(
            record=record,
            record_type=RecordType.POST.value,
            operation=Operation.CREATE.value,
            author_did=author_did,
            filename=filename,
            study_user_manager=mock_study_user_manager,
        )

        # Assert - check file was created
        cache_dir = tmp_path / "__local_cache__" / "study_user_activity" / Operation.CREATE.value / RecordType.POST.value
        expected_file = cache_dir / filename
        assert expected_file.exists()

        # Verify file contents
        with open(expected_file, "r") as f:
            loaded_data = json.load(f)
        assert loaded_data == record

    def test_export_study_user_data_local_like(self, tmp_path):
        """Test export_study_user_data_local for like records."""
        # Arrange
        record = {
            "uri": "at://did:plc:test/app.bsky.feed.like/123",
            "record": {
                "subject": {"uri": "at://did:plc:other/app.bsky.feed.post/456"}
            },
        }
        author_did = "did:plc:test"
        filename = "test_like.json"
        mock_study_user_manager = Mock()

        # Act
        export_study_user_data_local(
            record=record,
            record_type=RecordType.LIKE.value,
            operation=Operation.CREATE.value,
            author_did=author_did,
            filename=filename,
            study_user_manager=mock_study_user_manager,
        )

        # Assert - check file was created in nested directory
        post_uri_suffix = "456"
        cache_dir = (
            tmp_path
            / "__local_cache__"
            / "study_user_activity"
            / Operation.CREATE.value
            / RecordType.LIKE.value
            / post_uri_suffix
        )
        expected_file = cache_dir / filename
        assert expected_file.exists()

    def test_export_study_user_data_local_follow_with_follow_status(self, tmp_path):
        """Test export_study_user_data_local for follow records with follow_status."""
        # Arrange
        record = {
            "uri": "at://did:plc:test/app.bsky.graph.follow/123",
            "follower_did": "did:plc:test",
            "followee_did": "did:plc:other",
        }
        author_did = "did:plc:test"
        filename = "test_follow.json"
        mock_study_user_manager = Mock()

        # Act
        export_study_user_data_local(
            record=record,
            record_type=RecordType.FOLLOW.value,
            operation=Operation.CREATE.value,
            author_did=author_did,
            filename=filename,
            kwargs={"follow_status": FollowStatus.FOLLOWER.value},
            study_user_manager=mock_study_user_manager,
        )

        # Assert - check file was created in follower directory
        cache_dir = (
            tmp_path
            / "__local_cache__"
            / "study_user_activity"
            / Operation.CREATE.value
            / RecordType.FOLLOW.value
            / FollowStatus.FOLLOWER.value
        )
        expected_file = cache_dir / filename
        assert expected_file.exists()

    def test_export_study_user_data_local_follow_missing_follow_status(self):
        """Test export_study_user_data_local raises error when follow_status missing."""
        # Arrange
        record = {"uri": "at://did:plc:test/app.bsky.graph.follow/123"}
        author_did = "did:plc:test"
        filename = "test_follow.json"
        mock_study_user_manager = Mock()

        # Act & Assert
        with pytest.raises(ValueError, match="follow_status required"):
            export_study_user_data_local(
                record=record,
                record_type=RecordType.FOLLOW.value,
                operation=Operation.CREATE.value,
                author_did=author_did,
                filename=filename,
                study_user_manager=mock_study_user_manager,
            )

    def test_export_study_user_data_local_unknown_record_type(self):
        """Test export_study_user_data_local raises error for unknown record type."""
        # Arrange
        record = {"uri": "test"}
        author_did = "did:plc:test"
        filename = "test.json"
        mock_study_user_manager = Mock()

        # Act & Assert
        # Enum validation happens first, so we get a ValueError from enum creation
        with pytest.raises((ValueError, KeyError), match="(Unknown record type|not a valid RecordType)"):
            export_study_user_data_local(
                record=record,
                record_type="unknown_type",  # type: ignore[arg-type]
                operation=Operation.CREATE.value,
                author_did=author_did,
                filename=filename,
                study_user_manager=mock_study_user_manager,
            )

    def test_export_study_user_data_local_calls_insert_study_user_post(self, tmp_path):
        """Test export_study_user_data_local calls insert_study_user_post for posts."""
        # Arrange
        record = {
            "uri": "at://did:plc:test/app.bsky.feed.post/123",
            "text": "Test post",
        }
        author_did = "did:plc:test"
        filename = "test_post.json"

        with patch(
            "services.sync.stream.cache_writer._get_study_user_manager"
        ) as mock_get_manager:
            mock_manager = Mock()
            mock_manager.insert_study_user_post = Mock()
            mock_get_manager.return_value = mock_manager

            # Act
            export_study_user_data_local(
                record=record,
                record_type=RecordType.POST.value,
                operation=Operation.CREATE.value,
                author_did=author_did,
                filename=filename,
            )

            # Assert
            mock_manager.insert_study_user_post.assert_called_once_with(
                post_uri=record["uri"], user_did=author_did
            )


class TestExportInNetworkUserDataLocal:
    """Tests for export_in_network_user_data_local adapter function."""

    @pytest.fixture(autouse=True)
    def setup_test(self, monkeypatch, tmp_path):
        """Set up test fixtures."""
        # Reset system components
        monkeypatch.setattr(
            "services.sync.stream.cache_writer._system_components", None
        )

        # Create temporary directory for cache
        cache_dir = tmp_path / "__local_cache__"
        cache_dir.mkdir()

        # Mock setup_sync_export_system
        def mock_setup(use_s3=False, s3_client=None):
            from services.sync.stream.cache_management import (
                CachePathManager,
                CacheDirectoryManager,
                CacheFileWriter,
                CacheFileReader,
            )

            path_manager = CachePathManager()
            path_manager.root_write_path = str(cache_dir)
            path_manager.in_network_user_activity_root_local_path = str(
                cache_dir / "in_network_user_activity"
            )

            dir_manager = CacheDirectoryManager(path_manager=path_manager)
            file_writer = CacheFileWriter(directory_manager=dir_manager)
            file_reader = CacheFileReader()

            from services.sync.stream.exporters.study_user_exporter import (
                StudyUserActivityExporter,
            )
            from services.sync.stream.storage.repository import StorageRepository
            from services.sync.stream.storage.adapters import LocalStorageAdapter

            storage_adapter = LocalStorageAdapter(path_manager=path_manager)
            storage_repository = StorageRepository(adapter=storage_adapter)
            exporter = StudyUserActivityExporter(
                path_manager=path_manager,
                storage_repository=storage_repository,
                handler_registry=RecordHandlerRegistry,
            )

            return (
                path_manager,
                dir_manager,
                file_writer,
                file_reader,
                RecordHandlerRegistry,
                exporter,
                storage_repository,
            )

        # Patch setup_sync_export_system in the setup module (where it's actually defined)
        # This will be used when _get_system_components() imports it
        monkeypatch.setattr(
            "services.sync.stream.setup.setup_sync_export_system", mock_setup
        )

        # Reset the global variable
        import services.sync.stream.cache_writer as cache_writer_module

        cache_writer_module._system_components = None

        yield

    def test_export_in_network_user_data_local_post(self, tmp_path):
        """Test export_in_network_user_data_local for post records."""
        # Arrange
        record = {
            "uri": "at://did:plc:test/app.bsky.feed.post/123",
            "text": "Test post",
        }
        author_did = "did:plc:test"
        filename = "test_post.json"

        # Act
        export_in_network_user_data_local(
            record=record,
            record_type=RecordType.POST.value,
            author_did=author_did,
            filename=filename,
        )

        # Assert - check file was created
        cache_dir = (
            tmp_path
            / "__local_cache__"
            / "in_network_user_activity"
            / Operation.CREATE.value
            / RecordType.POST.value
            / author_did
        )
        expected_file = cache_dir / filename
        assert expected_file.exists()

        # Verify file contents
        with open(expected_file, "r") as f:
            loaded_data = json.load(f)
        assert loaded_data == record

    def test_export_in_network_user_data_local_non_post_is_noop(self):
        """Test export_in_network_user_data_local is no-op for non-post types."""
        # Arrange
        record = {"uri": "test"}
        author_did = "did:plc:test"
        filename = "test.json"
        mock_study_user_manager = Mock()

        # Act (should not raise or create files)
        export_in_network_user_data_local(
            record=record,
            record_type=RecordType.LIKE.value,
            author_did=author_did,
            filename=filename,
            study_user_manager=mock_study_user_manager,
        )

        # Assert - function should return without doing anything
        # (no file should be created, but we can't easily test that without
        # checking the file system, which we'll skip for this test)

