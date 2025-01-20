"""Tests for run.py."""

import pytest
from unittest.mock import Mock, patch

from api.integrations_router.models import (
    IntegrationRequest,
    IntegrationResponse,
    RunExecutionMetadata
)
from api.integrations_router.run import (
    run_integration_service,
    run_integration_request
)


@pytest.fixture
def mock_service_fn():
    """Mock service function that returns execution metadata."""
    return Mock(return_value={
        "service": "test_service",
        "timestamp": "2024-03-14T12:00:00Z", 
        "status_code": 200,
        "body": "Test successful",
        "metadata_table_name": "test_table",
        "metadata": "{}"
    })


@pytest.fixture
def mock_write_metadata():
    """Mock function for writing metadata to DynamoDB."""
    return Mock()


@pytest.fixture
def test_request():
    """Sample integration request."""
    return IntegrationRequest(
        service="test_service",
        payload={"key": "value"},
        metadata={}
    )


def test_run_integration_service(test_request, mock_service_fn):
    """Test run_integration_service function."""
    with patch(
        "api.integrations_router.run.MAP_INTEGRATION_REQUEST_TO_SERVICE",
        {"test_service": mock_service_fn}
    ):
        result = run_integration_service(test_request)
        
        assert isinstance(result, RunExecutionMetadata)
        assert result.service == "test_service"
        assert result.timestamp == "2024-03-14T12:00:00Z"
        assert result.status_code == 200
        assert result.body == "Test successful"
        
        mock_service_fn.assert_called_once_with(key="value")


def test_run_integration_request(test_request, mock_service_fn, mock_write_metadata):
    """Test run_integration_request function."""
    with patch(
        "api.integrations_router.run.MAP_INTEGRATION_REQUEST_TO_SERVICE",
        {"test_service": mock_service_fn}
    ), patch(
        "api.integrations_router.run.write_execution_metadata_to_db",
        mock_write_metadata
    ):
        result = run_integration_request(test_request)
        
        assert isinstance(result, IntegrationResponse)
        assert result.service == "test_service"
        assert result.timestamp == "2024-03-14T12:00:00Z"
        assert result.status_code == 200
        assert result.body == "Test successful"
        
        mock_service_fn.assert_called_once_with(key="value")
        mock_write_metadata.assert_called_once()


def test_run_integration_service_invalid_service(test_request):
    """Test run_integration_service with invalid service name."""
    with patch(
        "api.integrations_router.run.MAP_INTEGRATION_REQUEST_TO_SERVICE",
        {}
    ):
        with pytest.raises(KeyError):
            run_integration_service(test_request)


def test_run_integration_request_write_failure(test_request, mock_service_fn):
    """Test run_integration_request when writing metadata fails."""
    mock_write_metadata = Mock(side_effect=Exception("DB Write Failed"))
    
    with patch(
        "api.integrations_router.run.MAP_INTEGRATION_REQUEST_TO_SERVICE",
        {"test_service": mock_service_fn}
    ), patch(
        "api.integrations_router.run.write_execution_metadata_to_db",
        mock_write_metadata
    ):
        with pytest.raises(Exception) as exc_info:
            run_integration_request(test_request)
        
        assert str(exc_info.value) == "DB Write Failed"
        mock_service_fn.assert_called_once_with(key="value")
        mock_write_metadata.assert_called_once()
