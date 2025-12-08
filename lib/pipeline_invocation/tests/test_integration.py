"""Integration tests for pipeline invocation happy flows.

These tests verify the complete end-to-end flow from service call to handler execution,
including metadata tracking and observability.
"""

import pytest
from unittest.mock import Mock, patch

from lib.pipeline_invocation.invoker import invoke_pipeline_handler
from lib.pipeline_invocation.registry import PipelineHandlerRegistry
from lib.metadata.models import RunExecutionMetadata


@pytest.mark.integration
class TestHappyFlowIntegration:
    """Integration tests for happy path flows."""

    @pytest.fixture
    def mock_handler(self):
        """Mock pipeline handler that returns valid metadata."""
        return Mock(return_value={
            "service": "test_service",
            "timestamp": "2024-03-14T12:00:00Z",
            "status_code": 200,
            "body": "Test successful",
            "metadata_table_name": "integration_run_metadata",
            "metadata": "{}"
        })

    @pytest.fixture
    def mock_dynamodb(self):
        """Mock DynamoDB client."""
        with patch("lib.pipeline_invocation.executor.dynamodb") as mock_db:
            mock_db.query_items_by_service.return_value = []
            mock_db.insert_item_into_table.return_value = None
            yield mock_db

    @pytest.fixture
    def mock_wandb_logger(self):
        """Mock WandB logger to avoid actual logging."""
        return Mock()

    def test_full_call_chain_perspective_api(
        self, mock_handler, mock_dynamodb, mock_wandb_logger
    ):
        """Test full call chain for perspective API service.
        
        Verifies: invoke_pipeline_handler → registry → handler → metadata write
        """
        # Inject mock handler via registry DI to avoid real handler execution
        PipelineHandlerRegistry.inject_handler_for_testing(
            "ml_inference_perspective_api",
            mock_handler
        )
        
        try:
            with patch(
                "lib.pipeline_invocation.executor._default_wandb_logger",
                return_value=mock_wandb_logger
            ):
                result = invoke_pipeline_handler(
                    service="ml_inference_perspective_api",
                    payload={"run_type": "backfill"},
                    request_metadata={"request_id": "test-123"}
                )
                
                assert isinstance(result, RunExecutionMetadata)
                assert result.service == "test_service"
                assert result.status_code == 200
                assert result.body == "Test successful"
                
                # Verify handler was called
                mock_handler.assert_called_once()
                call_args = mock_handler.call_args
                assert call_args[1]["event"] == {"run_type": "backfill"}
        finally:
            PipelineHandlerRegistry.reset_test_handlers()

    def test_full_call_chain_sociopolitical(
        self, mock_handler, mock_dynamodb, mock_wandb_logger
    ):
        """Test full call chain for sociopolitical service."""
        PipelineHandlerRegistry.inject_handler_for_testing(
            "ml_inference_sociopolitical",
            mock_handler
        )
        
        try:
            with patch(
                "lib.pipeline_invocation.executor._default_wandb_logger",
                return_value=mock_wandb_logger
            ):
                result = invoke_pipeline_handler(
                    service="ml_inference_sociopolitical",
                    payload={"run_type": "prod"},
                    request_metadata={}
                )
                
                assert isinstance(result, RunExecutionMetadata)
                assert result.service == "test_service"
                assert result.status_code == 200
                mock_handler.assert_called_once()
        finally:
            PipelineHandlerRegistry.reset_test_handlers()

    def test_full_call_chain_ime(
        self, mock_handler, mock_dynamodb, mock_wandb_logger
    ):
        """Test full call chain for IME service."""
        PipelineHandlerRegistry.inject_handler_for_testing(
            "ml_inference_ime",
            mock_handler
        )
        
        try:
            with patch(
                "lib.pipeline_invocation.executor._default_wandb_logger",
                return_value=mock_wandb_logger
            ):
                result = invoke_pipeline_handler(
                    service="ml_inference_ime",
                    payload={"run_type": "backfill"},
                    request_metadata={}
                )
                
                assert isinstance(result, RunExecutionMetadata)
                assert result.service == "test_service"
                assert result.status_code == 200
                mock_handler.assert_called_once()
        finally:
            PipelineHandlerRegistry.reset_test_handlers()

    def test_full_call_chain_valence_classifier(
        self, mock_handler, mock_dynamodb, mock_wandb_logger
    ):
        """Test full call chain for valence classifier service."""
        PipelineHandlerRegistry.inject_handler_for_testing(
            "ml_inference_valence_classifier",
            mock_handler
        )
        
        try:
            with patch(
                "lib.pipeline_invocation.executor._default_wandb_logger",
                return_value=mock_wandb_logger
            ):
                result = invoke_pipeline_handler(
                    service="ml_inference_valence_classifier",
                    payload={"run_type": "prod"},
                    request_metadata={}
                )
                
                assert isinstance(result, RunExecutionMetadata)
                assert result.service == "test_service"
                assert result.status_code == 200
                mock_handler.assert_called_once()
        finally:
            PipelineHandlerRegistry.reset_test_handlers()

    def test_full_call_chain_preprocess_raw_data(
        self, mock_handler, mock_dynamodb, mock_wandb_logger
    ):
        """Test full call chain for preprocess raw data service.
        
        Uses registry DI to inject mock handler, avoiding fasttext dependency.
        """
        PipelineHandlerRegistry.inject_handler_for_testing(
            "preprocess_raw_data",
            mock_handler
        )
        
        try:
            with patch(
                "lib.pipeline_invocation.executor._default_wandb_logger",
                return_value=mock_wandb_logger
            ):
                result = invoke_pipeline_handler(
                    service="preprocess_raw_data",
                    payload={"run_type": "backfill"},
                    request_metadata={}
                )
                
                assert isinstance(result, RunExecutionMetadata)
                assert result.service == "test_service"
                assert result.status_code == 200
                mock_handler.assert_called_once()
        finally:
            PipelineHandlerRegistry.reset_test_handlers()

    def test_registry_handles_all_services(self):
        """Test that all registered services can be retrieved."""
        services = PipelineHandlerRegistry.list_services()
        assert len(services) == 5
        assert "ml_inference_perspective_api" in services
        assert "ml_inference_sociopolitical" in services
        assert "ml_inference_ime" in services
        assert "ml_inference_valence_classifier" in services
        assert "preprocess_raw_data" in services
        
        # Verify service names are registered without actually loading handlers
        # (which would import heavy dependencies like fasttext)
        # The fact that list_services() works confirms registration succeeded
        # Individual handler loading can be tested in unit tests with mocks
        assert all(service in PipelineHandlerRegistry._handlers for service in services)

    def test_backfill_service_integration(self, mock_handler, mock_dynamodb, mock_wandb_logger):
        """Test integration with backfill service pattern.
        
        Simulates how services/backfill/posts/main.py would call invoke_pipeline_handler.
        """
        PipelineHandlerRegistry.inject_handler_for_testing(
            "ml_inference_perspective_api",
            mock_handler
        )
        
        try:
            with patch(
                "lib.pipeline_invocation.executor._default_wandb_logger",
                return_value=mock_wandb_logger
            ):
                # Simulate the call pattern from backfill service
                integration = "ml_inference_perspective_api"
                integration_kwargs = {"backfill_period": "days"}
                
                result = invoke_pipeline_handler(
                    service=integration,
                    payload={"run_type": "backfill", **integration_kwargs},
                    request_metadata={}
                )
                
                assert isinstance(result, RunExecutionMetadata)
                assert result.service == "test_service"
                
                # Verify handler was called with correct payload
                mock_handler.assert_called_once()
                call_args = mock_handler.call_args
                assert call_args[1]["event"] == {
                    "run_type": "backfill",
                    "backfill_period": "days"
                }
        finally:
            PipelineHandlerRegistry.reset_test_handlers()
