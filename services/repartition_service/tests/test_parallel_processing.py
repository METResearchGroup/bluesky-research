"""Tests for parallel_processing.py.

This test suite verifies the functionality of parallel processing implementation:
- Configuration handling
- Chunk processing
- Progress monitoring
- Error recovery
- Result aggregation
"""

import multiprocessing
import pytest
from unittest.mock import patch, MagicMock
import time

from services.repartition_service.parallel_processing import (
    ParallelConfig,
    process_date_chunk,
    monitor_progress,
    recover_failed_chunks,
    repartition_data_for_partition_dates_parallel,
)
from services.repartition_service.helper import (
    OperationResult,
    OperationStatus,
    RepartitionError,
)

# Mock service metadata for testing
MOCK_SERVICE_METADATA = {
    "test_service": {
        "local_prefix": "/data/test_service",
        "timestamp_field": "created_at",
    }
}

@pytest.fixture(autouse=True)
def mock_service_metadata():
    """Automatically mock service metadata for all tests."""
    with patch("services.repartition_service.parallel_processing.MAP_SERVICE_TO_METADATA", MOCK_SERVICE_METADATA):
        yield


class TestParallelConfig:
    """Tests for ParallelConfig class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = ParallelConfig()
        assert config.max_workers == 4
        assert config.chunk_size == 10
        assert config.timeout == 3600
        assert config.progress_interval == 5

    def test_custom_values(self):
        """Test custom configuration values."""
        config = ParallelConfig(
            max_workers=2,
            chunk_size=5,
            timeout=1800,
            progress_interval=10,
        )
        assert config.max_workers == 2
        assert config.chunk_size == 5
        assert config.timeout == 1800
        assert config.progress_interval == 10


class TestProcessDateChunk:
    """Tests for process_date_chunk function."""

    @patch("services.repartition_service.parallel_processing.repartition_data_for_partition_date")
    def test_successful_chunk_processing(self, mock_repartition):
        """Test successful processing of a date chunk."""
        dates = ["2024-01-01", "2024-01-02"]
        shared_state = multiprocessing.Value("i", 0)
        mock_repartition.return_value = OperationResult(
            status=OperationStatus.SUCCESS,
            message="Success",
        )

        results = process_date_chunk(
            dates=dates,
            service="test_service",
            new_service_partition_key="created_at",
            shared_state=shared_state,
        )

        assert len(results) == 2
        assert all(r.status == OperationStatus.SUCCESS for r in results.values())
        assert shared_state.value == 2

    @patch("services.repartition_service.parallel_processing.repartition_data_for_partition_date")
    def test_mixed_results_chunk_processing(self, mock_repartition):
        """Test chunk processing with mixed success and failure."""
        dates = ["2024-01-01", "2024-01-02"]
        shared_state = multiprocessing.Value("i", 0)
        mock_repartition.side_effect = [
            OperationResult(status=OperationStatus.SUCCESS, message="Success"),
            OperationResult(
                status=OperationStatus.FAILED,
                error=RepartitionError("Test error"),
            ),
        ]

        results = process_date_chunk(
            dates=dates,
            service="test_service",
            new_service_partition_key="created_at",
            shared_state=shared_state,
        )

        assert len(results) == 2
        assert results[dates[0]].status == OperationStatus.SUCCESS
        assert results[dates[1]].status == OperationStatus.FAILED
        assert shared_state.value == 2


class TestMonitorProgress:
    """Tests for monitor_progress function."""

    def test_progress_monitoring(self):
        """Test progress monitoring with completion."""
        shared_state = multiprocessing.Value("i", 0)
        stop_event = multiprocessing.Event()
        total_dates = 2

        with patch("services.repartition_service.parallel_processing.logger") as mock_logger:
            # Set up a separate process for monitoring
            monitor = multiprocessing.Process(
                target=monitor_progress,
                args=(shared_state, total_dates, 0.1, stop_event),  # Reduce interval for testing
            )
            monitor.start()

            # Simulate progress
            with shared_state.get_lock():
                shared_state.value = 1
            # Let the monitor update
            time.sleep(0.2)  # Wait for at least one update
            # Complete progress
            with shared_state.get_lock():
                shared_state.value = 2
            # Let the monitor detect completion
            time.sleep(0.2)
            # Signal monitor to stop
            stop_event.set()
            # Wait for monitor to finish
            monitor.join(timeout=1)

            assert not monitor.is_alive()
            # Verify that logger.info was called with progress messages
            mock_logger.info.assert_called()
            progress_calls = [
                call for call in mock_logger.info.call_args_list
                if "Progress:" in str(call)
            ]
            assert len(progress_calls) > 0, "No progress messages were logged"


class TestRecoverFailedChunks:
    """Tests for recover_failed_chunks function."""

    @patch("services.repartition_service.parallel_processing.repartition_data_for_partition_date")
    def test_successful_recovery(self, mock_repartition):
        """Test successful recovery of failed dates."""
        failed_dates = ["2024-01-01"]
        mock_repartition.return_value = OperationResult(
            status=OperationStatus.SUCCESS,
            message="Success",
        )

        results = recover_failed_chunks(
            failed_dates=failed_dates,
            service="test_service",
            new_service_partition_key="created_at",
        )

        assert len(results) == 1
        assert results[failed_dates[0]].status == OperationStatus.SUCCESS
        assert mock_repartition.call_count == 1

    @patch("services.repartition_service.parallel_processing.repartition_data_for_partition_date")
    def test_failed_recovery_with_retries(self, mock_repartition):
        """Test failed recovery with multiple retry attempts."""
        failed_dates = ["2024-01-01"]
        mock_repartition.return_value = OperationResult(
            status=OperationStatus.FAILED,
            error=RepartitionError("Test error"),
        )

        results = recover_failed_chunks(
            failed_dates=failed_dates,
            service="test_service",
            new_service_partition_key="created_at",
            max_retries=2,
        )

        assert len(results) == 1
        assert results[failed_dates[0]].status == OperationStatus.FAILED
        assert isinstance(results[failed_dates[0]].error, RepartitionError)
        assert mock_repartition.call_count == 2


class TestRepartitionDataForPartitionDatesParallel:
    """Tests for repartition_data_for_partition_dates_parallel function."""

    @patch("services.repartition_service.parallel_processing.get_partition_dates")
    @patch("concurrent.futures.ProcessPoolExecutor")
    def test_successful_parallel_processing(self, mock_executor, mock_get_dates):
        """Test successful parallel processing of multiple dates."""
        # Setup
        dates = ["2024-01-01", "2024-01-02"]
        mock_get_dates.return_value = dates
        
        mock_future = MagicMock()
        mock_future.result.return_value = {
            "2024-01-01": OperationResult(status=OperationStatus.SUCCESS),
            "2024-01-02": OperationResult(status=OperationStatus.SUCCESS),
        }
        
        mock_executor_instance = MagicMock()
        mock_executor_instance.__enter__.return_value.submit.return_value = mock_future
        mock_executor.return_value = mock_executor_instance

        # Test
        results = repartition_data_for_partition_dates_parallel(
            start_date="2024-01-01",
            end_date="2024-01-02",
            service="test_service",
            parallel_config=ParallelConfig(max_workers=2, chunk_size=1),
        )

        assert len(results) == 2
        assert all(r.status == OperationStatus.SUCCESS for r in results.values())

    @patch("services.repartition_service.parallel_processing.get_partition_dates")
    def test_empty_date_range(self, mock_get_dates):
        """Test handling of empty date range."""
        mock_get_dates.return_value = []

        results = repartition_data_for_partition_dates_parallel(
            start_date="2024-01-01",
            end_date="2024-01-02",
            service="test_service",
        )

        assert len(results) == 0

    def test_invalid_service(self):
        """Test handling of invalid service name."""
        with pytest.raises(ValueError, match="Service name is required"):
            repartition_data_for_partition_dates_parallel(
                start_date="2024-01-01",
                end_date="2024-01-02",
                service="",
            )

    @patch("services.repartition_service.parallel_processing.MAP_SERVICE_TO_METADATA", {})
    def test_unknown_service(self):
        """Test handling of unknown service name."""
        with pytest.raises(ValueError, match="Unknown service"):
            repartition_data_for_partition_dates_parallel(
                start_date="2024-01-01",
                end_date="2024-01-02",
                service="unknown_service",
            ) 