"""Shared fixtures for repository tests."""

import pytest
from unittest.mock import Mock

from services.backfill.models import PostToEnqueueModel
from services.backfill.repositories.adapters import LocalStorageAdapter, S3Adapter
from services.backfill.repositories.base import BackfillDataAdapter


@pytest.fixture
def local_storage_adapter():
    """Create LocalStorageAdapter instance."""
    return LocalStorageAdapter()


@pytest.fixture
def s3_adapter():
    """Create S3Adapter instance."""
    return S3Adapter()


@pytest.fixture
def mock_adapter():
    """Mock BackfillDataAdapter."""
    return Mock(spec=BackfillDataAdapter)


@pytest.fixture
def repository(mock_adapter):
    """Create BackfillDataRepository with mocked adapter."""
    from services.backfill.repositories.repository import BackfillDataRepository
    return BackfillDataRepository(adapter=mock_adapter)


@pytest.fixture
def sample_posts():
    """Sample posts for testing."""
    return [
        PostToEnqueueModel(
            uri="test_uri_1",
            text="test_text_1",
            preprocessing_timestamp="2024-01-01T00:00:00",
        ),
        PostToEnqueueModel(
            uri="test_uri_2",
            text="test_text_2",
            preprocessing_timestamp="2024-01-02T00:00:00",
        ),
    ]


@pytest.fixture
def sample_date_range():
    """Sample date range for testing."""
    return {
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
    }


@pytest.fixture
def sample_records():
    """Sample records for testing."""
    return [
        {"id": 1, "data": "record1"},
        {"id": 2, "data": "record2"},
    ]
