"""Tests for invoker.py.

Needs to not be named "test_main.py", since for some reason that exact naming
causes pytest to not run these tests (it can't discover this file?).
"""

import pytest
from unittest.mock import Mock, patch

from lib.pipeline_invocation.models import IntegrationRequest
from lib.pipeline_invocation.invoker import parse_integration_request, invoke_pipeline_handler
from lib.metadata.models import RunExecutionMetadata


@pytest.fixture
def test_request_dict():
    """Sample integration request dictionary."""
    return {
        "service": "test_service",
        "payload": {"key": "value"},
        "metadata": {}
    }


@pytest.fixture
def mock_run_integration_request():
    """Mock for run_integration_request function."""
    return Mock(return_value=RunExecutionMetadata(
        service="test_service",
        timestamp="2024-03-14T12:00:00Z",
        status_code=200,
        body="Test successful",
        metadata_table_name="test_table",
        metadata="{}"
    ))


def test_parse_integration_request(test_request_dict):
    """Test parse_integration_request function."""
    result = parse_integration_request(test_request_dict)
    
    assert isinstance(result, IntegrationRequest)
    assert result.service == "test_service"
    assert result.payload == {"key": "value"}
    assert result.metadata == {}


def test_parse_integration_request_invalid():
    """Test parse_integration_request with invalid input."""
    invalid_request = {"invalid_key": "value"}
    
    with pytest.raises(ValueError):
        parse_integration_request(invalid_request)


def test_invoke_pipeline_handler(test_request_dict, mock_run_integration_request):
    """Test invoke_pipeline_handler function."""
    with patch(
        "lib.pipeline_invocation.invoker.run_integration_request",
        mock_run_integration_request
    ):
        result = invoke_pipeline_handler(
            service="test_service",
            payload={"key": "value"},
            request_metadata={}
        )
        
        assert isinstance(result, RunExecutionMetadata)
        assert result.service == "test_service"
        assert result.timestamp == "2024-03-14T12:00:00Z"
        assert result.status_code == 200
        assert result.body == "Test successful"
        assert result.metadata_table_name == "test_table"
        assert result.metadata == "{}"
        
        mock_run_integration_request.assert_called_once()
        actual_request = mock_run_integration_request.call_args[1]["request"]
        assert isinstance(actual_request, IntegrationRequest)
        assert actual_request.service == "test_service"
        assert actual_request.payload == {"key": "value"}

