"""Tests for metrics collection."""
import asyncio
from datetime import datetime
import json
import time

import pytest

from lib.telemetry.metrics import MetricsCollector

def test_metrics_collector_initialization():
    """Test that MetricsCollector initializes correctly."""
    collector = MetricsCollector()
    assert collector.process is not None

def test_get_snapshot():
    """Test that _get_snapshot returns valid metrics."""
    collector = MetricsCollector()
    snapshot = collector._get_snapshot()
    
    # Check snapshot structure
    assert snapshot.timestamp is not None
    assert isinstance(snapshot.memory_mb, float)
    assert isinstance(snapshot.cpu_percent, float)
    assert isinstance(snapshot.io_counters, dict)
    
    # Validate timestamp format
    datetime.fromisoformat(snapshot.timestamp)  # Should not raise error

def test_measure_context_manager():
    """Test the measure context manager captures metrics correctly."""
    collector = MetricsCollector()
    
    with collector.measure("test_operation", tags={"test": "true"}):
        # Simulate some work
        time.sleep(0.1)
    
    metrics = collector.last_metrics
    
    # Check metrics structure
    assert metrics["operation"] == "test_operation"
    assert metrics["tags"] == {"test": "true"}
    
    # Check timing
    assert isinstance(metrics["timing"]["duration_seconds"], float)
    assert metrics["timing"]["duration_seconds"] >= 0.1  # Should be at least our sleep time
    assert "start_time" in metrics["timing"]
    assert "end_time" in metrics["timing"]
    
    # Check memory metrics
    assert "start_mb" in metrics["memory"]
    assert "end_mb" in metrics["memory"]
    assert "delta_mb" in metrics["memory"]
    assert isinstance(metrics["memory"]["delta_mb"], float)
    
    # Check CPU metrics
    assert "percent" in metrics["cpu"]
    assert isinstance(metrics["cpu"]["percent"], float)
    
    # Check IO metrics
    assert "start" in metrics["io"]
    assert "end" in metrics["io"]
    assert "delta" in metrics["io"]
    assert isinstance(metrics["io"]["delta"], dict)

def test_measure_with_no_tags():
    """Test the measure context manager works with no tags."""
    collector = MetricsCollector()
    
    with collector.measure("test_operation"):
        pass
    
    assert collector.last_metrics["tags"] == {}

def test_measure_captures_changes():
    """Test that the measure context manager captures metric changes."""
    collector = MetricsCollector()
    data = []
    
    with collector.measure("memory_allocation"):
        # Create some memory allocation
        for _ in range(1000000):
            data.append(1)
    
    metrics = collector.last_metrics
    
    # Memory should have increased
    assert metrics["memory"]["delta_mb"] > 0

def test_metrics_serialization():
    """Test that metrics can be serialized to JSON."""
    collector = MetricsCollector()
    
    with collector.measure("test_operation"):
        pass
    
    # Verify metrics can be serialized to JSON
    json_str = json.dumps(collector.last_metrics)
    parsed = json.loads(json_str)
    
    assert parsed["operation"] == "test_operation"

@pytest.mark.asyncio
async def test_measure_with_async():
    """Test the measure context manager works with async code."""
    collector = MetricsCollector()
    
    async def async_operation():
        await asyncio.sleep(0.1)
    
    with collector.measure("async_operation"):
        await async_operation()
    
    assert collector.last_metrics["operation"] == "async_operation"
    assert collector.last_metrics["timing"]["duration_seconds"] >= 0.1 