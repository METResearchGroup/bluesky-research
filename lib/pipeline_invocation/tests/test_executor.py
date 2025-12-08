"""Tests for executor.py."""

import pytest
from unittest.mock import Mock, patch

from lib.pipeline_invocation.models import IntegrationRequest
from lib.pipeline_invocation.executor import (
    load_latest_run_metadata,
    run_integration_service,
    run_integration_request
)
from lib.metadata.models import RunExecutionMetadata


class TestLoadLatestRunMetadata:
    """Tests for load_latest_run_metadata function.
    
    This class tests the function that retrieves the most recent run metadata
    from DynamoDB for a given service.
    """

    @pytest.fixture
    def mock_dynamodb(self):
        """Mock DynamoDB client and its query_items_by_service method."""
        with patch("lib.pipeline_invocation.executor.dynamodb") as mock_db:
            yield mock_db

    @pytest.mark.parametrize("service,timestamp,mock_items", [
        (
            "ml_inference_perspective_api",
            "2024-01-02",
            [
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
        ),
        (
            "ml_inference_sociopolitical",
            "2024-01-01",
            [
                {
                    "service": {"S": "ml_inference_sociopolitical"},
                    "timestamp": {"S": "2024-01-01"},
                    "status_code": {"N": "200"},
                    "body": {"S": "success"},
                    "metadata_table_name": {"S": "test_table"},
                    "metadata": {"S": "{}"}
                }
            ]
        ),
        (
            "ml_inference_ime",
            "2024-01-01",
            [
                {
                    "service": {"S": "ml_inference_ime"},
                    "timestamp": {"S": "2024-01-01"},
                    "status_code": {"N": "200"},
                    "body": {"S": "success"},
                    "metadata_table_name": {"S": "test_table"},
                    "metadata": {"S": "{}"}
                }
            ]
        ),
    ])
    def test_ml_inference_service_metadata_retrieval(self, mock_dynamodb, service, timestamp, mock_items):
        """Test retrieving metadata for ML inference services.
        
        Expected input:
        - service: Service name to query
        
        Expected behavior:
        - Should query DynamoDB with service name
        - Should return dict with most recent timestamp
        """
        mock_dynamodb.query_items_by_service.return_value = mock_items

        result = load_latest_run_metadata(service)
        
        mock_dynamodb.query_items_by_service.assert_called_once_with(
            table_name="integration_run_metadata",
            service=service
        )
        assert isinstance(result, dict)
        assert result["timestamp"]["S"] == timestamp

    def test_non_ml_inference_service(self, mock_dynamodb):
        """Test retrieving metadata for non-ML inference service.
        
        Expected input:
        - service = "other_service"
        
        Expected behavior:
        - Should query DynamoDB but return empty dict when no items found
        """
        mock_dynamodb.query_items_by_service.return_value = []
        
        result = load_latest_run_metadata("other_service")
        
        mock_dynamodb.query_items_by_service.assert_called_once_with(
            table_name="integration_run_metadata",
            service="other_service"
        )
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
        mock_dynamodb.query_items_by_service.return_value = []

        result = load_latest_run_metadata("ml_inference_perspective_api")
        
        mock_dynamodb.query_items_by_service.assert_called_once_with(
            table_name="integration_run_metadata",
            service="ml_inference_perspective_api"
        )
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
def mock_wandb_logger():
    """Mock WandB logger."""
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
        "lib.pipeline_invocation.executor.PipelineHandlerRegistry.get_handler",
        return_value=mock_service_fn
    ):
        result = run_integration_service(test_request)
        
        assert isinstance(result, RunExecutionMetadata)
        assert result.service == "test_service"
        assert result.timestamp == "2024-03-14T12:00:00Z"
        assert result.status_code == 200
        assert result.body == "Test successful"
        
        mock_service_fn.assert_called_once_with(event={"key": "value"}, context=None)


def test_run_integration_request(
    test_request, 
    mock_service_fn, 
    mock_write_metadata,
    mock_wandb_logger
):
    """Test run_integration_request function."""
    with patch(
        "lib.pipeline_invocation.executor.PipelineHandlerRegistry.get_handler",
        return_value=mock_service_fn
    ):
        result = run_integration_request(
            test_request,
            wandb_logger=mock_wandb_logger,
            write_metadata=mock_write_metadata
        )
        
        assert isinstance(result, RunExecutionMetadata)
        assert result.service == "test_service"
        assert result.timestamp == "2024-03-14T12:00:00Z"
        assert result.status_code == 200
        assert result.body == "Test successful"
        
        mock_service_fn.assert_called_once_with(event={"key": "value"}, context=None)
        mock_write_metadata.assert_called_once()
        mock_wandb_logger.assert_called_once()


def test_run_integration_service_invalid_service(test_request):
    """Test run_integration_service with invalid service name."""
    with patch(
        "lib.pipeline_invocation.executor.PipelineHandlerRegistry.get_handler",
        side_effect=KeyError("Unknown service name: test_service")
    ):
        with pytest.raises(KeyError):
            run_integration_service(test_request)


def test_run_integration_request_write_failure(
    test_request, 
    mock_service_fn,
    mock_wandb_logger
):
    """Test run_integration_request when writing metadata fails."""
    from lib.pipeline_invocation.errors import MetadataWriteError
    mock_write_metadata = Mock(side_effect=MetadataWriteError("test_service", Exception("DB Write Failed")))
    
    with patch(
        "lib.pipeline_invocation.executor.PipelineHandlerRegistry.get_handler",
        return_value=mock_service_fn
    ):
        with pytest.raises(MetadataWriteError):
            run_integration_request(
                test_request,
                wandb_logger=mock_wandb_logger,
                write_metadata=mock_write_metadata
            )

        mock_service_fn.assert_called_once_with(event={"key": "value"}, context=None)
        mock_write_metadata.assert_called_once()

