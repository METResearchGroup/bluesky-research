"""Helper utilities for pytest functions."""
import copy
import logging
import os
from typing import Optional
from unittest.mock import Mock

import pytest

from services.sync.stream.tests.mock_firehose_data import (
    mock_user_dids, mock_post_uri_to_user_did_map, mock_follow_records,
    mock_like_records, mock_post_records
)


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


def get_mock_study_manager():
    return mock_study_manager


def get_mock_logger():
    return mock_logger


@pytest.fixture(autouse=True)
def mock_logger_fixture(monkeypatch):
    return MockLogger()


@pytest.fixture(autouse=True)
def mock_study_user_manager(monkeypatch):
    monkeypatch.setattr(
        "services.sync.stream.data_filter.study_user_manager.is_study_user",
        mock_study_manager.is_study_user
    )
    monkeypatch.setattr(
        "services.sync.stream.data_filter.study_user_manager.is_study_user_post",  # noqa
        mock_study_manager.is_study_user_post
    )
    monkeypatch.setattr(
        "services.sync.stream.data_filter.get_study_user_manager",
        get_mock_study_manager
    )
    monkeypatch.setattr(
        "services.participant_data.study_users.get_study_user_manager",
        get_mock_study_manager
    )
    monkeypatch.setattr(
        "services.sync.stream.data_filter.study_user_manager",
        mock_study_manager
    )


@pytest.fixture(autouse=True)
def mock_s3_fixture(monkeypatch):
    monkeypatch.setattr("services.sync.stream.export_data.s3", mock_s3)
    # patch study user activity writes
    monkeypatch.setattr(
        "services.sync.stream.export_data.s3.write_dict_json_to_s3",
        mock_s3.write_dict_json_to_s3
    )
    # patch general firehose writes
    monkeypatch.setattr(
        "services.sync.stream.export_data.s3.write_local_jsons_to_s3",
        mock_s3.write_local_jsons_to_s3
    )
    # patch partition key on export.
    # monkeypatch.setattr(
    #     "services.sync.stream.export_data.s3.create_partition_key_based_on_timestamp", # noqa
    #     "year=2024/month=08/day=01/hour=20/minute=39"
    # )
    monkeypatch.setattr(
        "lib.aws.s3.S3.create_partition_key_based_on_timestamp",  # noqa
        mock_s3.create_partition_key_based_on_timestamp
    )
    # patch the `generate_current_datetime_str` since this'll determine the
    # compressed file's name.
    monkeypatch.setattr(
        "services.sync.stream.export_data.generate_current_datetime_str",
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
