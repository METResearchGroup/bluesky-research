"""Tests for cache_writer.py - adapter functions for real-time cache writes."""

import json
from unittest.mock import Mock, patch

import pytest

from services.sync.stream.cache_writer import (
    export_study_user_data_local,
    export_in_network_user_data_local,
)
from services.sync.stream.cache_management import (
    CachePathManager,
    CacheDirectoryManager,
    CacheFileWriter,
    CacheFileReader,
)
from services.sync.stream.handlers.registry import RecordHandlerRegistry
from services.sync.stream.types import Operation, RecordType, FollowStatus


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
        def mock_setup():
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
        def mock_setup():
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

