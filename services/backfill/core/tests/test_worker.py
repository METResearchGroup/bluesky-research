import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
import pandas as pd

import aiohttp

import services.backfill.core.worker as worker_mod

@pytest.fixture
def mock_config():
    # Minimal config mock
    class TimeRange:
        start_date = "2023-01-01T00:00:00Z"
        end_date = "2023-12-31T23:59:59Z"
    config = MagicMock()
    config.record_types = ["post", "repost"]
    config.time_range = TimeRange()
    return config

@pytest.fixture
def mock_session():
    return MagicMock(spec=aiohttp.ClientSession)

@pytest.fixture
def mock_cpu_pool():
    return MagicMock()

@pytest.fixture
def dids():
    return ["did:plc:123", "did:plc:456"]

@pytest.fixture
def worker_instance(dids, mock_session, mock_cpu_pool, mock_config):
    with patch("services.backfill.core.worker.get_write_queues") as mock_get_queues:
        mock_queue = MagicMock()
        mock_get_queues.return_value = {
            "output_results_queue": mock_queue,
            "output_deadletter_queue": mock_queue,
        }
        return worker_mod.PDSEndpointWorker(
            dids=dids,
            session=mock_session,
            cpu_pool=mock_cpu_pool,
            config=mock_config,
        )

@pytest.mark.asyncio
async def test_init_worker_queue(worker_instance):
    await worker_instance._init_worker_queue()
    assert worker_instance.temp_work_queue.qsize() == len(worker_instance.dids)

@pytest.mark.asyncio
async def test_filter_records(worker_instance):
    # Only valid records in date range should be returned
    with patch("services.backfill.core.worker.filter_only_valid_bsky_records", return_value=True), \
         patch("services.backfill.core.worker.validate_time_range_record", return_value=True):
        records = [{"foo": "bar"} for _ in range(5)]
        filtered = worker_instance._filter_records(records)
        assert len(filtered) == 5

@pytest.mark.asyncio
async def test_fetch_paginated_records_success(worker_instance, mock_session):
    # Simulate two pages, then no cursor
    mock_response1 = MagicMock()
    mock_response1.status = 200
    mock_response1.json = AsyncMock(return_value={"records": [{"id": 1}], "cursor": "abc"})
    mock_response1.headers = {"ratelimit-remaining": "100"}
    mock_response2 = MagicMock()
    mock_response2.status = 200
    mock_response2.json = AsyncMock(return_value={"records": [{"id": 2}], "cursor": None})
    mock_response2.headers = {"ratelimit-remaining": "99"}

    with patch("services.backfill.core.worker.AtpAgent") as MockAgent, \
         patch("services.backfill.core.worker.convert_bsky_dt_to_pipeline_dt", return_value="2023-01-01T00:00:00Z"):
        agent = MockAgent.return_value
        agent.async_list_records = AsyncMock(side_effect=[mock_response1, mock_response2])
        records = await worker_instance._fetch_paginated_records(
            did="did:plc:123",
            record_type="post",
            session=mock_session,
            retry_count=0,
        )
        assert records == [{"id": 1}, {"id": 2}]
        assert agent.async_list_records.call_count == 2

@pytest.mark.asyncio
async def test_fetch_paginated_records_400(worker_instance, mock_session):
    # Simulate 400 error (deleted/suspended)
    mock_response = MagicMock()
    mock_response.status = 400
    mock_response.reason = "Bad Request"
    mock_response.json = AsyncMock(return_value={})
    mock_response.headers = {"ratelimit-remaining": "100"}

    with patch("services.backfill.core.worker.AtpAgent") as MockAgent:
        agent = MockAgent.return_value
        agent.async_list_records = AsyncMock(return_value=mock_response)
        records = await worker_instance._fetch_paginated_records(
            did="did:plc:123",
            record_type="post",
            session=mock_session,
            retry_count=0,
        )
        assert records == []

@pytest.mark.asyncio
async def test_process_did_success(worker_instance, mock_session):
    # Patch _fetch_paginated_records to return dummy records
    with patch.object(worker_instance, "_fetch_paginated_records", AsyncMock(return_value=[{"foo": "bar"}])), \
         patch.object(worker_instance, "_filter_records", return_value=[{"foo": "bar"}]):
        await worker_instance._process_did(
            did="did:plc:123",
            session=mock_session,
            retry_count=0,
        )
        # Should have put something in results queue
        assert not worker_instance.temp_results_queue.empty()

@pytest.mark.asyncio
async def test_process_did_deadletter(worker_instance, mock_session):
    # Patch _fetch_paginated_records to return empty (simulate deleted/inactive)
    with patch.object(worker_instance, "_fetch_paginated_records", AsyncMock(return_value=[])):
        await worker_instance._process_did(
            did="did:plc:123",
            session=mock_session,
            retry_count=0,
        )
        # Should have put something in deadletter queue
        assert not worker_instance.temp_deadletter_queue.empty()

@pytest.mark.asyncio
async def test_process_did_max_retries(worker_instance, mock_session):
    await worker_instance._process_did(
        did="did:plc:123",
        session=mock_session,
        retry_count=worker_instance.max_retries + 1,
    )
    assert not worker_instance.temp_deadletter_queue.empty()

@pytest.mark.asyncio
async def test_start_and_shutdown(worker_instance):
    with patch.object(worker_instance, "_init_worker_queue", AsyncMock()), \
         patch.object(worker_instance, "_writer", AsyncMock()), \
         patch.object(worker_instance, "_write_queue_to_db", AsyncMock()), \
         patch("asyncio.create_task", side_effect=lambda coro: AsyncMock()):
        await worker_instance.start()
        worker_instance._writer_task = AsyncMock()
        worker_instance._db_task = AsyncMock()
        with patch.object(worker_instance.output_results_queue, "close", AsyncMock()), \
             patch.object(worker_instance.output_deadletter_queue, "close", AsyncMock()):
            await worker_instance.shutdown()

@pytest.mark.asyncio
async def test_worker_loop(worker_instance):
    await worker_instance.temp_work_queue.put({"did": "did:plc:123"})
    with patch.object(worker_instance, "_process_did", AsyncMock()):
        await worker_instance._worker_loop()
        assert worker_instance.temp_work_queue.empty()

@pytest.mark.asyncio
async def test_wait_done(worker_instance):
    worker_instance._producer_group = AsyncMock()
    with patch.object(worker_instance.temp_work_queue, "join", AsyncMock()), \
         patch.object(worker_instance.temp_results_queue, "join", AsyncMock()), \
         patch.object(worker_instance.temp_deadletter_queue, "join", AsyncMock()), \
         patch("asyncio.get_running_loop", return_value=MagicMock()), \
         patch.object(worker_instance, "_sync_persist_to_db", return_value={}), \
         patch.object(worker_instance, "write_backfill_metadata_to_db", return_value=None), \
         patch.object(worker_instance.output_results_queue, "delete_queue", return_value=None), \
         patch.object(worker_instance.output_deadletter_queue, "delete_queue", return_value=None):
        await worker_instance.wait_done()

def test_sync_flush_buffers(worker_instance):
    result_buffer = [{"did": "did:plc:123"}]
    deadletter_buffer = [{"did": "did:plc:456"}]
    with patch.object(worker_instance.output_results_queue, "batch_add_items_to_queue") as mock_results, \
         patch.object(worker_instance.output_deadletter_queue, "batch_add_items_to_queue") as mock_deadletter:
        worker_instance._sync_flush_buffers(result_buffer, deadletter_buffer)
        mock_results.assert_called_once()
        mock_deadletter.assert_called_once()

def test_estimate_parquet_write_size(worker_instance):
    df = pd.DataFrame([{"a": 1, "b": 2} for _ in range(10)])
    size = worker_instance._estimate_parquet_write_size("post", df, 2, 10)
    assert isinstance(size, float)

@pytest.mark.asyncio
async def test_persist_to_db(worker_instance):
    with patch.object(worker_instance.output_results_queue, "load_dict_items_from_queue", return_value=[
        {"did": "did:plc:123", "content": [{"record_type": "post"}], "batch_id": "1"}
    ]), \
         patch.object(worker_instance.output_deadletter_queue, "load_dict_items_from_queue", return_value=[]), \
         patch.object(worker_instance, "_filter_records", return_value=[{"record_type": "post"}]), \
         patch("services.backfill.core.worker.transform_backfilled_record", return_value={"record_type": "post"}), \
         patch("services.backfill.core.worker.postprocess_backfilled_record", return_value={"record_type": "post"}), \
         patch.object(worker_instance.output_results_queue, "batch_delete_items_by_ids"), \
         patch.object(worker_instance.output_deadletter_queue, "batch_delete_items_by_ids"), \
         patch("services.backfill.core.worker.export_data_to_local_storage"):
        result = await worker_instance.persist_to_db()
        assert isinstance(result, dict)

def test_write_backfill_metadata_to_db(worker_instance):
    with patch("services.backfill.core.worker.write_backfill_metadata_to_db") as mock_write:
        worker_instance.write_backfill_metadata_to_db({"did:plc:123": {"post": 1, "repost": 0, "reply": 0}})
        mock_write.assert_called_once()
