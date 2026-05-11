"""Tests for multiprocessing orchestration in parallel_processing."""

from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pytest

from services.repartition_service.helper import OperationResult, OperationStatus
from services.repartition_service.parallel_processing import (
    ParallelConfig,
    process_date_chunk,
    recover_failed_chunks,
    repartition_data_for_partition_dates_parallel,
)

MOCK_META = {
    "test_service": {
        "local_prefix": "/data/test_service",
        "timestamp_field": "preprocessing_timestamp",
    },
}


@pytest.fixture(autouse=True)
def mock_map_parallel():
    with patch(
        "services.repartition_service.parallel_processing.MAP_SERVICE_TO_METADATA", MOCK_META
    ):
        yield


class FakeSharedCounter:
    __slots__ = ("value",)

    def __init__(self, start: int = 0) -> None:
        self.value = start

    def get_lock(self):
        return nullcontext()


def test_process_date_chunk_increment_and_results():
    shared = FakeSharedCounter(0)

    with patch(
        "services.repartition_service.parallel_processing.repartition_data_for_partition_date",
        return_value=OperationResult(OperationStatus.SUCCESS),
    ) as repart_mock:
        merged = process_date_chunk(
            ["2024-01-01", "2024-01-02"],
            service="svc",
            new_service_partition_key="indexed_at",
            shared_state=shared,
        )

    assert shared.value == 2
    assert repart_mock.call_count == 2
    assert merged["2024-01-01"].status == OperationStatus.SUCCESS
    assert merged["2024-01-02"].status == OperationStatus.SUCCESS


@pytest.mark.parametrize("service", ["", "unknown_service"])
def test_parallel_raises_for_bad_service(service):
    with pytest.raises(ValueError):
        repartition_data_for_partition_dates_parallel(
            start_date="2024-01-01",
            end_date="2024-01-02",
            service=service,
            new_service_partition_key="preprocessing_timestamp",
        )


@patch(
    "services.repartition_service.parallel_processing.get_partition_dates",
    return_value=[],
)
def test_parallel_returns_empty_when_no_dates(_mock_dates):
    out = repartition_data_for_partition_dates_parallel(
        start_date="2024-01-01",
        end_date="2024-01-02",
        service="test_service",
    )
    assert out == {}


@patch(
    "services.repartition_service.parallel_processing.recover_failed_chunks",
)
@patch("concurrent.futures.as_completed")
@patch("concurrent.futures.ProcessPoolExecutor")
@patch("services.repartition_service.parallel_processing.multiprocessing")
@patch(
    "services.repartition_service.parallel_processing.get_partition_dates",
    return_value=["2024-01-01"],
)
def test_parallel_retries_failed_chunks(
    _mock_dates,
    mock_mp,
    mock_executor_cls,
    mock_as_completed,
    mock_recover,
):
    def fake_value(*args, **kwargs):  # noqa: ANN001, ANN002
        return FakeSharedCounter(0)

    mock_mp.Value.side_effect = fake_value
    mock_mp.Event.return_value = MagicMock()
    mock_monitor_proc = MagicMock()
    mock_mp.Process.return_value = mock_monitor_proc

    mock_recover.return_value = {
        "2024-01-01": OperationResult(OperationStatus.SUCCESS),
    }

    failed = OperationResult(
        OperationStatus.FAILED,
        error=ValueError("simulated partition failure"),
    )
    failing_future = MagicMock()
    failing_future.result.return_value = {"2024-01-01": failed}

    mock_executor_instance = MagicMock()
    mock_executor_instance.submit.return_value = failing_future
    mock_executor_instance.__enter__.return_value = mock_executor_instance
    mock_executor_instance.__exit__.return_value = None
    mock_executor_cls.return_value = mock_executor_instance
    mock_as_completed.return_value = [failing_future]

    results = repartition_data_for_partition_dates_parallel(
        start_date="2024-01-01",
        end_date="2024-01-02",
        service="test_service",
        parallel_config=ParallelConfig(chunk_size=1, max_workers=2),
    )

    mock_recover.assert_called_once()
    recover_positional, recover_named = mock_recover.call_args
    assert recover_positional[0] == ["2024-01-01"]
    assert recover_positional[1] == "test_service"
    assert recover_positional[2] == "preprocessing_timestamp"

    mock_monitor_proc.join.assert_called()
    mock_monitor_proc.start.assert_called()

    assert results["2024-01-01"].status == OperationStatus.SUCCESS


@patch(
    "services.repartition_service.parallel_processing.repartition_data_for_partition_date",
)
def test_recover_failed_chunks_retries(mock_repartition):
    mock_repartition.side_effect = [
        OperationResult(OperationStatus.FAILED, error=RuntimeError("transient")),
        OperationResult(OperationStatus.SUCCESS, message="ok"),
    ]

    results = recover_failed_chunks(
        ["2024-01-01"], service="test_service", new_service_partition_key="preprocessing_timestamp"
    )

    assert mock_repartition.call_count >= 2
    assert results["2024-01-01"].status == OperationStatus.SUCCESS
