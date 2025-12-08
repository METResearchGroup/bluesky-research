"""Integration tests for pipeline invocation happy flows.

These tests verify the complete end-to-end flow from service call to handler execution,
including metadata tracking and observability.
"""

import pytest
from unittest.mock import Mock, patch

from lib.pipeline_invocation.invoker import invoke_pipeline_handler
from lib.pipeline_invocation.registry import PipelineHandlerRegistry


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

    @pytest.mark.parametrize("service,payload,request_metadata", [
        ("ml_inference_perspective_api", {"run_type": "backfill"}, {"request_id": "test-123"}),
        ("ml_inference_sociopolitical", {"run_type": "prod"}, {}),
        ("ml_inference_ime", {"run_type": "backfill"}, {}),
        ("ml_inference_valence_classifier", {"run_type": "prod"}, {}),
        ("preprocess_raw_data", {"run_type": "backfill"}, {}),
    ])
    def test_full_call_chain(self, mock_handler, service, payload, request_metadata):
        """Test full call chain for registered services.
        
        Verifies: invoke_pipeline_handler → registry → handler → metadata write
        """
        from lib.metadata.models import RunExecutionMetadata
        
        # Inject mock handler via registry DI to avoid real handler execution
        PipelineHandlerRegistry.inject_handler_for_testing(
            service,
            mock_handler
        )
        
        try:
            with patch(
                "lib.pipeline_invocation.executor._default_wandb_logger",
                return_value=Mock()
            ):
                result = invoke_pipeline_handler(
                    service=service,
                    payload=payload,
                    request_metadata=request_metadata
                )
                
                assert isinstance(result, RunExecutionMetadata)
                assert result.service == "test_service"
                assert result.status_code == 200
                assert result.body == "Test successful"
                
                # Verify handler was called
                mock_handler.assert_called_once()
                call_args = mock_handler.call_args
                assert call_args[1]["event"] == payload
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

    def test_backfill_service_integration(self, mock_handler):
        """Test integration with backfill service pattern.
        
        Simulates how services/backfill/posts/main.py would call invoke_pipeline_handler.
        """
        from lib.metadata.models import RunExecutionMetadata
        
        PipelineHandlerRegistry.inject_handler_for_testing(
            "ml_inference_perspective_api",
            mock_handler
        )
        
        try:
            with patch(
                "lib.pipeline_invocation.executor._default_wandb_logger",
                return_value=Mock()
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
