"""Helper utilities for pytest functions."""
import copy
import logging
import os
from typing import Optional
from unittest.mock import Mock, patch

import pytest

from services.sync.stream.tests.mock_firehose_data import (
    mock_user_dids, mock_post_uri_to_user_did_map, mock_follow_records,
    mock_like_records, mock_post_records
)


def pytest_configure(config):
    """Configure pytest hooks - runs before any test modules are imported.
    
    This patches get_study_user_manager before data_filter.py can initialize
    study_user_manager at module level, preventing real initialization during tests.
    """
    # Patch at the source before any imports happen
    # This must happen before data_filter.py is imported
    patch(
        "services.participant_data.study_users.get_study_user_manager",
        get_mock_study_manager,
    ).start()


def clean_path(path: str):
    if os.path.exists(path):
        os.remove(path)


class MockStudyUserManager:
    """Mock class for the StudyUserManager singleton."""

    def __init__(self):
        self.study_users_dids_set = mock_user_dids
        self.post_uri_to_study_user_did_map = mock_post_uri_to_user_did_map
        self.in_network_user_dids_set = {}

    def is_study_user(self, user_did: str) -> bool:
        """Mock function for checking if a user is a study user."""
        return user_did in self.study_users_dids_set

    def is_study_user_post(self, post_uri: str) -> Optional[str]:
        """Mock function for checking if a post is from a study user."""
        return self.post_uri_to_study_user_did_map.get(post_uri, None)

    def is_in_network_user(self, user_did: str) -> bool:
        """Mock function for checking if a user is in the in-network user DIDs."""
        return user_did in self.in_network_user_dids_set


class MockS3():
    """Mock patch class of the `S3` utility class."""

    def __init__(self):
        super().__init__()
        self.write_dict_json_to_s3 = Mock()
        self.write_local_jsons_to_s3 = Mock()
        self.create_partition_key_based_on_timestamp = Mock(
            return_value="year=2024/month=08/day=01/hour=20/minute=39"
        )


# class MockLogger():
#     """Mock class of the `Logger` utility class."""

#     def __init__(self, name: str = "test"):
#         self.name = name
#         self.log = Mock()
#         self.info = Mock()
#         self.error = Mock()

class MockLogger:
    """Mock class of the `Logger` utility class that captures logs for testing."""

    def __init__(self, name: str = "test"):
        self.name = name
        self.records: list[logging.LogRecord] = []
        
    def info(self, message: str, **kwargs) -> None:
        record = logging.LogRecord(
            name=self.name,
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=message,
            args=(),
            exc_info=None
        )
        self.records.append(record)
        
    def error(self, message: str, **kwargs) -> None:
        record = logging.LogRecord(
            name=self.name,
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg=message,
            args=(),
            exc_info=None
        )
        self.records.append(record)
        
    def log(self, message: str, **kwargs) -> None:
        self.info(message, **kwargs)

    def get_logs(self) -> list[str]:
        """Helper method to get all logged messages."""
        return [record.msg for record in self.records]

mock_s3 = MockS3()
mock_logger = MockLogger()
mock_study_manager = MockStudyUserManager()


def get_mock_study_manager(load_from_aws: bool = False):
    """Mock function for get_study_user_manager that accepts load_from_aws parameter."""
    return mock_study_manager


def get_mock_logger():
    return mock_logger


@pytest.fixture(autouse=True)
def mock_logger_fixture(monkeypatch):
    return MockLogger()


@pytest.fixture(autouse=True)
def mock_study_user_manager(monkeypatch):
    """Comprehensive fixture that patches all StudyUserManager dependencies.
    
    This fixture ensures:
    - Singleton instance is reset before each test
    - get_study_user_manager returns a mock
    - _load_in_network_user_dids returns empty set (no Parquet loading)
    - load_data_from_local_storage returns empty DataFrame (no Parquet loading)
    - _write_post_uri_to_study_user_did_map_to_s3 does nothing (no S3 writes)
    """
    import pandas as pd
    from services.participant_data.study_users import StudyUserManager
    
    # Reset singleton instance before each test to prevent state leakage
    StudyUserManager._instance = None
    
    # Patch get_study_user_manager at the source so setup_sync_export_system uses the mock
    monkeypatch.setattr(
        "services.participant_data.study_users.get_study_user_manager",
        get_mock_study_manager
    )
    
    # Patch _load_in_network_user_dids to avoid Parquet loading issues
    monkeypatch.setattr(
        "services.participant_data.study_users.StudyUserManager._load_in_network_user_dids",
        lambda self, is_local=True, is_refresh=False: set()
    )
    
    # Patch load_data_from_local_storage to avoid Parquet loading issues
    monkeypatch.setattr(
        "lib.db.manage_local_data.load_data_from_local_storage",
        lambda **kwargs: pd.DataFrame()
    )
    
    # Patch S3 write to avoid AWS errors
    monkeypatch.setattr(
        "services.participant_data.study_users.StudyUserManager._write_post_uri_to_study_user_did_map_to_s3",
        lambda self: None
    )


@pytest.fixture(autouse=True)
def mock_s3_fixture(monkeypatch):
    # Only patch s3 if it exists (for backward compatibility with old code)
    # Since export_data.py no longer exists, we skip S3 patching
    # patch partition key on export.
    monkeypatch.setattr(
        "lib.aws.s3.S3.create_partition_key_based_on_timestamp",  # noqa
        mock_s3.create_partition_key_based_on_timestamp
    )
    # patch the `generate_current_datetime_str` since this'll determine the
    # compressed file's name.
    monkeypatch.setattr(
        "lib.helper.generate_current_datetime_str",
        lambda: "2024-08-01-20:39:38"
    )


@pytest.fixture
def cleanup_files(request):
    files_to_cleanup = []

    def add_file(filepath):
        files_to_cleanup.append(filepath)

    def cleanup():
        for filepath in files_to_cleanup:
            clean_path(filepath)
            print(f"Deleted {filepath}")

    request.addfinalizer(cleanup)
    return add_file


# create fixtures for each mock records list, to avoid different tests
# collidiing with each other.
@pytest.fixture
def mock_follow_records_fixture():
    return copy.deepcopy(mock_follow_records)


@pytest.fixture
def mock_like_records_fixture():
    return copy.deepcopy(mock_like_records)


@pytest.fixture
def mock_post_records_fixture():
    return copy.deepcopy(mock_post_records)


@pytest.fixture
def path_manager():
    """Create a CachePathManager for testing."""
    from services.sync.stream.cache_management import CachePathManager
    return CachePathManager()


@pytest.fixture
def sync_export_context(mock_study_user_manager):
    """Create a SyncExportContext for testing."""
    from services.sync.stream.setup import setup_sync_export_system
    
    # Create context using the normal setup
    # The mock_study_user_manager fixture ensures get_study_user_manager returns the mock
    # Tests will use the actual cache paths (which is fine for testing)
    context = setup_sync_export_system()
    
    return context
