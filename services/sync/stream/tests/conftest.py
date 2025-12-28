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


@pytest.fixture(scope="session", autouse=True)
def patch_get_study_user_manager():
    """Session-scoped fixture that patches get_study_user_manager for all tests.
    
    This patches get_study_user_manager before any modules can initialize
    study_user_manager at module level, preventing real initialization during tests.
    The patch is automatically stopped after the test session completes.
    """
    # Create patcher and start it
    patcher = patch(
        "services.participant_data.study_users.get_study_user_manager",
        get_mock_study_manager,
    )
    patcher.start()
    
    # Yield to allow tests to run
    yield
    
    # Stop the patcher in teardown
    patcher.stop()


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
    - get_all_users returns empty list (no DynamoDB calls)
    - _load_study_user_dids returns empty set (no DynamoDB calls)
    - _load_in_network_user_dids returns empty set (no Parquet loading)
    - load_data_from_local_storage returns empty DataFrame (no Parquet loading)
    - _write_post_uri_to_study_user_did_map_to_s3 does nothing (no S3 writes)
    """
    import pandas as pd
    from services.participant_data.study_users import StudyUserManager
    
    # Reset singleton instance before each test to prevent state leakage
    StudyUserManager._instance = None
    
    # Patch get_study_user_manager at the source so setup_cache_write_system uses the mock
    monkeypatch.setattr(
        "services.participant_data.study_users.get_study_user_manager",
        get_mock_study_manager
    )
    
    # Patch get_all_users to avoid DynamoDB calls
    # This is critical because _load_study_user_dids() calls get_all_users()
    monkeypatch.setattr(
        "services.participant_data.helper.get_all_users",
        lambda test_mode=False: []
    )
    
    # Patch _load_study_user_dids to avoid DynamoDB calls via get_all_users
    monkeypatch.setattr(
        "services.participant_data.study_users.StudyUserManager._load_study_user_dids",
        lambda self: set()
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
    """Mock S3 and datetime utilities for consistent test behavior.
    
    Patches:
    - S3.create_partition_key_based_on_timestamp: Returns consistent partition key
    - generate_current_datetime_str: Returns consistent timestamp for file naming
    """
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
def cache_write_context(mock_study_user_manager):
    """Create a CacheWriteContext for testing."""
    from services.sync.stream.core.setup import setup_cache_write_system
    
    # Create context using the normal setup
    # The mock_study_user_manager fixture ensures get_study_user_manager returns the mock
    # Tests will use the actual cache paths (which is fine for testing)
    context = setup_cache_write_system()
    
    return context


@pytest.fixture
def batch_export_context(mock_study_user_manager):
    """Create a BatchExportContext for testing."""
    from services.sync.stream.core.setup import create_batch_export_context
    
    # Create context using the normal setup
    # The mock_study_user_manager fixture ensures get_study_user_manager returns the mock
    # Tests will use the actual cache paths (which is fine for testing)
    context = create_batch_export_context()
    
    return context




@pytest.fixture
def dir_manager(path_manager):
    """Create a CacheDirectoryManager for testing."""
    from services.sync.stream.cache_management import CacheDirectoryManager
    return CacheDirectoryManager(path_manager=path_manager)


@pytest.fixture
def file_utilities(dir_manager):
    """Create a FileUtilities instance for testing."""
    from services.sync.stream.cache_management import FileUtilities
    return FileUtilities(directory_manager=dir_manager)


# Aliases for readability in tests that emphasize read vs write operations
file_writer = file_utilities
file_reader = file_utilities


@pytest.fixture
def mock_storage_repository():
    """Create a mock storage repository for testing."""
    from unittest.mock import MagicMock
    return MagicMock()


@pytest.fixture
def mock_service_metadata(monkeypatch):
    """Mock MAP_SERVICE_TO_METADATA to avoid KeyError in tests."""
    from lib.db import service_constants
    
    # Use monkeypatch.setitem to ensure pytest restores original state after tests
    # Ensure required services exist in MAP_SERVICE_TO_METADATA
    if "study_user_activity" not in service_constants.MAP_SERVICE_TO_METADATA:
        monkeypatch.setitem(
            service_constants.MAP_SERVICE_TO_METADATA,
            "study_user_activity",
            {"dtypes_map": {}}
        )
    if "scraped_user_social_network" not in service_constants.MAP_SERVICE_TO_METADATA:
        monkeypatch.setitem(
            service_constants.MAP_SERVICE_TO_METADATA,
            "scraped_user_social_network",
            {"dtypes_map": {}}
        )
    if "in_network_user_activity" not in service_constants.MAP_SERVICE_TO_METADATA:
        monkeypatch.setitem(
            service_constants.MAP_SERVICE_TO_METADATA,
            "in_network_user_activity",
            {"dtypes_map": {}}
        )
    
    return service_constants.MAP_SERVICE_TO_METADATA


@pytest.fixture
def patched_export_dataframe(monkeypatch, mock_service_metadata):
    """Patch BaseActivityExporter._export_dataframe to handle missing columns gracefully.
    
    NOTE: This fixture duplicates logic from BaseActivityExporter._export_dataframe.
    This duplication exists as a workaround to handle missing columns in test metadata.
    This can be removed once:
    - Test metadata (MAP_SERVICE_TO_METADATA) is complete, OR
    - Production code handles missing columns gracefully
    
    Until then, this ensures tests don't fail due to missing dtype mappings.
    """
    import pandas as pd
    from services.sync.stream.exporters.base import BaseActivityExporter
    from lib.helper import generate_current_datetime_str
    from lib.constants import timestamp_format
    
    def patched_export(self, data, service, record_type=None):
        if not data:
            return
        dtypes_map = mock_service_metadata.get(service, {}).get("dtypes_map", {})
        df = pd.DataFrame(data)
        df["synctimestamp"] = generate_current_datetime_str()
        df["partition_date"] = pd.to_datetime(
            df["synctimestamp"], format=timestamp_format
        ).dt.date
        # Only apply dtypes_map for columns that exist
        if dtypes_map:
            existing_cols = {k: v for k, v in dtypes_map.items() if k in df.columns}
            if existing_cols:
                df = df.astype(existing_cols)
        custom_args = {}
        if record_type:
            custom_args["record_type"] = record_type
        self.storage_repository.export_dataframe(
            df=df,
            service=service,
            record_type=record_type,
            custom_args=custom_args if custom_args else None,
        )
    
    monkeypatch.setattr(BaseActivityExporter, "_export_dataframe", patched_export)
    return patched_export
