"""Shared fixtures for service tests."""

import pytest
from unittest.mock import Mock

from services.backfill.models import PostToEnqueueModel
from services.backfill.repositories.repository import BackfillDataRepository
from services.backfill.services.backfill_data_loader_service import (
    BackfillDataLoaderService,
)
from services.backfill.services.queue_manager_service import QueueManagerService


@pytest.fixture
def mock_repository():
    """Mock BackfillDataRepository."""
    return Mock(spec=BackfillDataRepository)


@pytest.fixture
def mock_data_loader():
    """Mock BackfillDataLoaderService."""
    return Mock(spec=BackfillDataLoaderService)


@pytest.fixture
def mock_queue_manager():
    """Mock QueueManagerService."""
    return Mock(spec=QueueManagerService)


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


@pytest.fixture
def sample_queue_ids():
    """Sample queue IDs for testing."""
    return [1, 2, 3, 4, 5]
