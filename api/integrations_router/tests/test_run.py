"""Tests for run.py."""

import pytest
from unittest.mock import Mock, patch

from api.integrations_router.models import (
    IntegrationRequest,
    IntegrationResponse,
    RunExecutionMetadata
)
from api.integrations_router.run import (
    load_latest_run_metadata,
    run_integration_service,
    run_integration_request
)


class TestLoadLatestRunMetadata:
    """Tests for load_latest_run_metadata function.
    
    This class tests the function that retrieves the most recent run metadata
    from DynamoDB for a given service.
    """

    @pytest.fixture
    def mock_dynamodb(self):
        """Mock DynamoDB client and its query_items_by_inference_type method."""
        with patch("api.integrations_router.run.dynamodb") as mock_db:
            yield mock_db

    def test_ml_inference_perspective_api_service(self, mock_dynamodb):
        """Test retrieving metadata for perspective API service.
        
        Expected input:
        - service = "ml_inference_perspective_api"
        
        Expected behavior:
        - Should query DynamoDB with inference_type="perspective_api"
        - Should return dict with most recent timestamp
        """
        mock_items = [
            {
                "service": {"S": "ml_inference_perspective_api"},
                "timestamp": {"S": "2024-01-02"},
                "status_code": {"N": "200"},
                "body": {"S": "success"},
                "metadata_table_name": {"S": "test_table"},
                "metadata": {"S": "{}"}
            },
            {
                "service": {"S": "ml_inference_perspective_api"},
                "timestamp": {"S": "2024-01-01"},
                "status_code": {"N": "200"},
                "body": {"S": "success"},
                "metadata_table_name": {"S": "test_table"},
                "metadata": {"S": "{}"}
            }
        ]
        mock_dynamodb.query_items_by_inference_type.return_value = mock_items

        result = load_latest_run_metadata("ml_inference_perspective_api")
        
        mock_dynamodb.query_items_by_inference_type.assert_called_once_with(
            table_name="ml_inference_labeling_sessions",
            inference_type="perspective_api"
        )
        assert isinstance(result, dict)
        assert result["timestamp"]["S"] == "2024-01-02"

    def test_ml_inference_sociopolitical_service(self, mock_dynamodb):
        """Test retrieving metadata for LLM service.
        
        Expected input:
        - service = "ml_inference_sociopolitical"
        
        Expected behavior:
        - Should query DynamoDB with inference_type="llm"
        - Should return dict with most recent timestamp
        """
        mock_items = [
            {
                "service": {"S": "ml_inference_sociopolitical"},
                "timestamp": {"S": "2024-01-01"},
                "status_code": {"N": "200"},
                "body": {"S": "success"},
                "metadata_table_name": {"S": "test_table"},
                "metadata": {"S": "{}"}
            }
        ]
        mock_dynamodb.query_items_by_inference_type.return_value = mock_items

        result = load_latest_run_metadata("ml_inference_sociopolitical")
        
        mock_dynamodb.query_items_by_inference_type.assert_called_once_with(
            table_name="ml_inference_labeling_sessions",
            inference_type="llm"
        )
        assert isinstance(result, dict)
        assert result["timestamp"]["S"] == "2024-01-01"

    def test_ml_inference_ime_service(self, mock_dynamodb):
        """Test retrieving metadata for IME service.
        
        Expected input:
        - service = "ml_inference_ime"
        
        Expected behavior:
        - Should query DynamoDB with inference_type="ime"
        - Should return dict with most recent timestamp
        """
        mock_items = [
            {
                "service": {"S": "ml_inference_ime"},
                "timestamp": {"S": "2024-01-01"},
                "status_code": {"N": "200"},
                "body": {"S": "success"},
                "metadata_table_name": {"S": "test_table"},
                "metadata": {"S": "{}"}
            }
        ]
        mock_dynamodb.query_items_by_inference_type.return_value = mock_items

        result = load_latest_run_metadata("ml_inference_ime")
        
        mock_dynamodb.query_items_by_inference_type.assert_called_once_with(
            table_name="ml_inference_labeling_sessions",
            inference_type="ime"
        )
        assert isinstance(result, dict)
        assert result["timestamp"]["S"] == "2024-01-01"

    def test_non_ml_inference_service(self, mock_dynamodb):
        """Test retrieving metadata for non-ML inference service.
        
        Expected input:
        - service = "other_service"
        
        Expected behavior:
        - Should not query DynamoDB
        - Should return empty dict
        """
        result = load_latest_run_metadata("other_service")
        
        mock_dynamodb.query_items_by_inference_type.assert_not_called()
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_no_items_found(self, mock_dynamodb):
        """Test behavior when no items are found in DynamoDB.
        
        Expected input:
        - service = "ml_inference_perspective_api"
        - DynamoDB returns empty list
        
        Expected behavior:
        - Should return empty dict when no items found
        """
        mock_dynamodb.query_items_by_inference_type.return_value = []

        result = load_latest_run_metadata("ml_inference_perspective_api")
        
        assert isinstance(result, dict)
        assert len(result) == 0


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
        "api.integrations_router.run.write_run_metadata_to_db",
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
        "api.integrations_router.run.write_run_metadata_to_db",
        mock_write_metadata
    ):
        with pytest.raises(Exception) as exc_info:
            run_integration_request(test_request)
        
        assert str(exc_info.value) == "DB Write Failed"
        mock_service_fn.assert_called_once_with(key="value")
        mock_write_metadata.assert_called_once()
