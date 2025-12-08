"""Tests for registry.py."""

import pytest

from lib.pipeline_invocation.registry import PipelineHandlerRegistry


def test_list_services():
    """Test list_services returns all registered services."""
    services = PipelineHandlerRegistry.list_services()
    assert isinstance(services, list)
    assert "ml_inference_perspective_api" in services
    assert "ml_inference_sociopolitical" in services
    assert "ml_inference_ime" in services
    assert "ml_inference_valence_classifier" in services
    assert "preprocess_raw_data" in services


def test_get_handler_valid_service():
    """Test get_handler with valid service name."""
    handler = PipelineHandlerRegistry.get_handler("ml_inference_perspective_api")
    assert callable(handler)


def test_get_handler_invalid_service():
    """Test get_handler with invalid service name raises KeyError."""
    with pytest.raises(KeyError) as exc_info:
        PipelineHandlerRegistry.get_handler("invalid_service")
    
    assert "Unknown service name: invalid_service" in str(exc_info.value)

