"""Tests for jetstream_connector.py.

This test suite verifies the functionality of the JetstreamConnector class,
with a focus on the end timestamp logic in the listen_until_count method.
"""

import asyncio
import json
import time
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch, call

import pytest
import websockets

from lib.constants import timestamp_format
from services.sync.jetstream.jetstream_connector import (
    JetstreamConnector,
    PUBLIC_INSTANCES,
)
from services.sync.jetstream.helper import timestamp_to_unix_microseconds, unix_microseconds_to_date


class TestJetstreamConnector:
    """Tests for the JetstreamConnector class.
    
    This class tests the functionality of the JetstreamConnector,
    including the end timestamp logic in the listen_until_count method.
    """

    def setup_method(self):
        """Set up test fixtures."""
        self.connector = JetstreamConnector(queue_name="test_queue", batch_size=10)
        
        # Mock the queue
        self.connector.queue = MagicMock()
        self.connector.queue.get_queue_length.return_value = 0
        self.connector.queue.batch_add_items_to_queue.return_value = None
        
        # Sample message for testing
        self.sample_message = {
            "did": "did:plc:test123",
            "time_us": "1672531200000000",  # 2023-01-01
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.feed.post",
                "data": {"text": "Test post"}
            }
        }
        
        # Sample record for testing
        self.sample_record = Mock()
        self.sample_record.to_queue_item.return_value = {"payload": self.sample_message}

    def test_get_public_instances(self):
        """Test get_public_instances method.
        
        Expected behavior:
        - Should return the list of public instances defined in PUBLIC_INSTANCES
        """
        instances = JetstreamConnector.get_public_instances()
        assert instances == PUBLIC_INSTANCES
        assert isinstance(instances, list)
        assert len(instances) > 0

    def test_generate_uri(self):
        """Test generate_uri method.
        
        Expected behavior:
        - Should format parameters correctly
        - Should generate correct WebSocket URI
        - Should handle lists and simple parameters
        """
        # Test with a simple parameter
        params = {"wantedCollections": ["app.bsky.feed.post"]}
        instance = PUBLIC_INSTANCES[0]
        uri = self.connector.generate_uri(instance, params)
        assert uri == f"wss://{instance}/subscribe?wantedCollections=app.bsky.feed.post"
        
        # Test with multiple values for a parameter and other parameters
        params = {
            "wantedCollections": ["app.bsky.feed.post", "app.bsky.feed.like"],
            "cursor": "1672531200000000"
        }
        uri = self.connector.generate_uri(instance, params)
        assert "wantedCollections=app.bsky.feed.post" in uri
        assert "wantedCollections=app.bsky.feed.like" in uri
        assert "cursor=1672531200000000" in uri
        
        # Test with invalid instance
        with pytest.raises(ValueError):
            self.connector.generate_uri("not-a-valid-instance", params)

    def test_extract_record_from_message(self):
        """Test extract_record_from_message method.
        
        Expected behavior:
        - Should correctly extract fields from a valid message
        - Should return None for invalid messages
        - Should track collections seen
        """
        # Patch the JetstreamRecord constructor
        with patch("services.sync.jetstream.jetstream_connector.JetstreamRecord") as mock_record:
            mock_record.return_value = self.sample_record
            
            # Test with valid message
            result = self.connector.extract_record_from_message(self.sample_message)
            assert result == self.sample_record
            
            # Test collection tracking
            assert "app.bsky.feed.post" in self.connector.collections_seen
            
            # Test with invalid message format
            invalid_msgs = [
                None,
                {},
                {"missing": "required_fields"},
                {"did": "test", "time_us": "123", "kind": "unknown"},
            ]
            
            for msg in invalid_msgs:
                result = self.connector.extract_record_from_message(msg)
                assert result is None

    def test_store_message(self):
        """Test store_message method.
        
        Expected behavior:
        - Should extract record and add to pending_records
        - Should flush when batch size is reached
        - Should return True for success, False for failure
        """
        # Patch extract_record_from_message
        with patch.object(self.connector, 'extract_record_from_message') as mock_extract:
            mock_extract.return_value = self.sample_record
            
            # Test successful storage
            result = self.connector.store_message(self.sample_message)
            assert result is True
            assert len(self.connector.pending_records) == 1
            assert self.connector.records_stored == 1
            
            # Mock extract_record_from_message to return None (invalid message)
            mock_extract.return_value = None
            
            # Test failed storage
            result = self.connector.store_message({"invalid": "message"})
            assert result is False
            assert len(self.connector.pending_records) == 1  # Unchanged
            assert self.connector.records_stored == 1  # Unchanged
    
    def test_flush_pending_records(self):
        """Test _flush_pending_records method.
        
        Expected behavior:
        - Should add pending records to queue
        - Should clear pending_records after flushing
        - Should handle exceptions gracefully
        """
        # Set up pending records
        self.connector.pending_records = [{"payload": "record1"}, {"payload": "record2"}]
        self.connector.collections_seen = {"collection1", "collection2"}
        
        # Test normal flush
        self.connector._flush_pending_records()
        
        # Verify queue was called with records and metadata
        self.connector.queue.batch_add_items_to_queue.assert_called_once()
        call_args = self.connector.queue.batch_add_items_to_queue.call_args
        assert call_args[1]["items"] == [{"payload": "record1"}, {"payload": "record2"}]
        assert "metadata" in call_args[1]
        assert "collections" in call_args[1]["metadata"]
        assert set(call_args[1]["metadata"]["collections"]) == {"collection1", "collection2"}
        
        # Verify pending_records was cleared
        assert len(self.connector.pending_records) == 0
        
        # Test with no pending records
        self.connector.queue.batch_add_items_to_queue.reset_mock()
        self.connector._flush_pending_records()
        self.connector.queue.batch_add_items_to_queue.assert_not_called()
        
        # Test with exception
        self.connector.pending_records = [{"payload": "record1"}]
        self.connector.queue.batch_add_items_to_queue.side_effect = Exception("Test error")
        
        # Should not raise exception
        self.connector._flush_pending_records()
        
        # Records should still be in pending_records
        assert len(self.connector.pending_records) == 1

    @pytest.mark.asyncio
    async def test_listen_until_count_basic(self):
        """Test basic functionality of listen_until_count.
        
        Expected behavior:
        - Should connect to the specified instance
        - Should receive and process messages until target count is reached
        - Should return statistics about the ingestion process
        """
        # Mock websockets.connect
        mock_websocket = AsyncMock()
        mock_websocket.__aenter__.return_value = mock_websocket
        
        # Set up mock to return 5 messages
        message_times = ["1672531200000000", "1672531300000000", "1672531400000000", 
                         "1672531500000000", "1672531600000000"]
        
        messages = []
        for i, time_us in enumerate(message_times):
            message = self.sample_message.copy()
            message["time_us"] = time_us
            messages.append(json.dumps(message))
        
        mock_websocket.recv.side_effect = messages
        
        # Mock extract_record_from_message to return sample_record and add to collections_seen
        def mock_extract_side_effect(message_dict):
            # Add to collections_seen as a side effect
            self.connector.collections_seen.add("app.bsky.feed.post")
            return self.sample_record
        
        mock_extract = MagicMock(side_effect=mock_extract_side_effect)
        
        with patch.object(self.connector, 'extract_record_from_message', mock_extract), \
             patch('websockets.connect', return_value=mock_websocket), \
             patch('services.sync.jetstream.jetstream_connector.unix_microseconds_to_date', return_value="2023-01-01"):
            
            # Run listen_until_count with target of 5 records
            stats = await self.connector.listen_until_count(
                instance=PUBLIC_INSTANCES[0],
                wanted_collections=["app.bsky.feed.post"],
                target_count=5,
                max_time=60,
            )
            
            # Verify statistics
            assert stats["records_stored"] == 5
            assert stats["messages_received"] == 5
            assert stats["target_reached"] is True
            assert stats["latest_cursor"] == "1672531600000000"
            assert "app.bsky.feed.post" in stats["collections"]

    @pytest.mark.asyncio
    async def test_listen_until_count_with_end_timestamp(self):
        """Test listen_until_count with end_timestamp.
        
        Expected behavior:
        - Should stop when reaching a cursor >= end_timestamp
        - Should set end_cursor_reached to True in stats
        """
        # Mock websockets.connect
        mock_websocket = AsyncMock()
        mock_websocket.__aenter__.return_value = mock_websocket
        
        # Set up mock to return messages with increasing timestamps
        # Jan 1, 2023 -> Jan 5, 2023
        message_times = [
            "1672531200000000",  # Jan 1, 2023
            "1672617600000000",  # Jan 2, 2023
            "1672704000000000",  # Jan 3, 2023
            "1672790400000000",  # Jan 4, 2023
            "1672876800000000",  # Jan 5, 2023
        ]
        
        messages = []
        for i, time_us in enumerate(message_times):
            message = self.sample_message.copy()
            message["time_us"] = time_us
            messages.append(json.dumps(message))
        
        mock_websocket.recv.side_effect = messages
        
        # Set end_timestamp to Jan 3, 2023
        end_timestamp = "2023-01-03"
        end_cursor = timestamp_to_unix_microseconds(end_timestamp)
        
        # Mock extract_record_from_message to return sample_record and add to collections_seen
        def mock_extract_side_effect(message_dict):
            # Add to collections_seen as a side effect
            self.connector.collections_seen.add("app.bsky.feed.post")
            return self.sample_record
        
        mock_extract = MagicMock(side_effect=mock_extract_side_effect)
        
        with patch.object(self.connector, 'extract_record_from_message', mock_extract), \
             patch('websockets.connect', return_value=mock_websocket), \
             patch('services.sync.jetstream.jetstream_connector.unix_microseconds_to_date') as mock_to_date, \
             patch('services.sync.jetstream.jetstream_connector.timestamp_to_unix_microseconds', return_value=end_cursor):
            
            # Map timestamps to dates for the mock
            date_mapping = {
                int(message_times[0]): "2023-01-01",
                int(message_times[1]): "2023-01-02",
                int(message_times[2]): "2023-01-03",
                int(message_times[3]): "2023-01-04",
                int(message_times[4]): "2023-01-05",
            }
            
            # Mock unix_microseconds_to_date
            mock_to_date.side_effect = lambda x: date_mapping.get(x, "unknown")
            
            # Run listen_until_count with end_timestamp
            stats = await self.connector.listen_until_count(
                instance=PUBLIC_INSTANCES[0],
                wanted_collections=["app.bsky.feed.post"],
                target_count=10,  # More than we'll get
                max_time=60,
                end_timestamp=end_timestamp,
            )
            
            # We should have 3 records stored, but 4 messages received
            # (one extra message to detect we're past the end timestamp)
            assert stats["records_stored"] == 3
            assert stats["messages_received"] == 4
            assert "app.bsky.feed.post" in stats["collections"]
            assert stats["end_cursor_reached"] is True

    @pytest.mark.asyncio
    async def test_listen_until_count_date_tracking(self):
        """Test date tracking in listen_until_count.
        
        Expected behavior:
        - Should track date changes as processing progresses
        - Should log date changes
        """
        # Mock websockets.connect
        mock_websocket = AsyncMock()
        mock_websocket.__aenter__.return_value = mock_websocket
        
        # Set up mock to return messages with dates spanning multiple days
        message_times = [
            "1672531200000000",  # Jan 1, 2023
            "1672531300000000",  # Jan 1, 2023
            "1672617600000000",  # Jan 2, 2023
            "1672617700000000",  # Jan 2, 2023
            "1672704000000000",  # Jan 3, 2023
        ]
        
        messages = []
        for i, time_us in enumerate(message_times):
            message = self.sample_message.copy()
            message["time_us"] = time_us
            messages.append(json.dumps(message))
        
        mock_websocket.recv.side_effect = messages
        
        # Mock extract_record_from_message to return sample_record
        mock_extract = MagicMock(return_value=self.sample_record)
        
        with patch.object(self.connector, 'extract_record_from_message', mock_extract), \
             patch('websockets.connect', return_value=mock_websocket), \
             patch('services.sync.jetstream.jetstream_connector.logger') as mock_logger, \
             patch('services.sync.jetstream.jetstream_connector.unix_microseconds_to_date') as mock_to_date:
            
            # Map timestamps to dates for the mock
            date_mapping = {
                int(message_times[0]): "2023-01-01",
                int(message_times[1]): "2023-01-01",
                int(message_times[2]): "2023-01-02",
                int(message_times[3]): "2023-01-02",
                int(message_times[4]): "2023-01-03",
            }
            
            # Mock unix_microseconds_to_date
            mock_to_date.side_effect = lambda x: date_mapping.get(x, "unknown")
            
            # Add collection to collections_seen (normally done by extract_record_from_message)
            self.connector.collections_seen.add("app.bsky.feed.post")
            
            # Run listen_until_count
            stats = await self.connector.listen_until_count(
                instance=PUBLIC_INSTANCES[0],
                wanted_collections=["app.bsky.feed.post"],
                target_count=5,
                max_time=60,
            )
            
            # Should have processed all 5 messages
            assert stats["records_stored"] == 5
            assert stats["messages_received"] == 5
            assert stats["current_date"] == "2023-01-03"
            
            # Verify date change logging
            # Should log date changes from Jan 1 -> Jan 2 and Jan 2 -> Jan 3
            date_change_calls = [
                call for call in mock_logger.info.call_args_list 
                if "Date change:" in str(call)
            ]
            assert len(date_change_calls) == 2
            assert "2023-01-01 -> 2023-01-02" in str(date_change_calls[0])
            assert "2023-01-02 -> 2023-01-03" in str(date_change_calls[1])
            
            # Verify extract_record_from_message was called for each message
            assert mock_extract.call_count == 5

    @pytest.mark.asyncio
    async def test_listen_until_count_with_cursor(self):
        """Test listen_until_count with a starting cursor.
        
        Expected behavior:
        - Should include the cursor in the URI parameters
        - Should initialize current_date based on the cursor
        """
        # Mock websockets.connect
        mock_websocket = AsyncMock()
        mock_websocket.__aenter__.return_value = mock_websocket
        
        # Set up mock to return one message
        mock_websocket.recv.side_effect = [json.dumps(self.sample_message)]
        
        # Define a cursor for January 2, 2023
        cursor = "1672617600000000"  # Jan 2, 2023
        
        with patch.object(self.connector, 'store_message', return_value=True), \
             patch('websockets.connect', return_value=mock_websocket), \
             patch('services.sync.jetstream.jetstream_connector.unix_microseconds_to_date') as mock_to_date, \
             patch.object(self.connector, 'generate_uri') as mock_generate_uri:
            
            # Mock unix_microseconds_to_date to return date for cursor
            mock_to_date.return_value = "2023-01-02"
            
            # Run listen_until_count with cursor
            await self.connector.listen_until_count(
                instance=PUBLIC_INSTANCES[0],
                wanted_collections=["app.bsky.feed.post"],
                target_count=1,
                max_time=60,
                cursor=cursor,
            )
            
            # Verify cursor was included in parameters
            mock_generate_uri.assert_called_once()
            params = mock_generate_uri.call_args[0][1]
            assert params["cursor"] == cursor
            
            # Verify current_date was initialized
            assert self.connector.current_date == "2023-01-02"

    @pytest.mark.asyncio
    async def test_listen_until_count_error_handling(self):
        """Test error handling in listen_until_count.
        
        Expected behavior:
        - Should handle WebSocket connection errors
        - Should handle JSON parsing errors
        - Should handle general exceptions
        - Should always flush pending records and return stats
        """
        # Mock websockets.connect
        mock_websocket = AsyncMock()
        mock_websocket.__aenter__.return_value = mock_websocket
        
        # Set up mock to return 3 messages, including an invalid one
        mock_websocket.recv.side_effect = [
            json.dumps(self.sample_message),
            "invalid json",
            json.dumps(self.sample_message),
            websockets.exceptions.ConnectionClosed(None, None),  # Simulate connection closed
        ]
        
        # Mock store_message to raise exception on 3rd message
        original_store_message = self.connector.store_message
        store_call_count = 0
        
        def mock_store_with_error(message):
            nonlocal store_call_count
            store_call_count += 1
            if store_call_count == 2:
                raise Exception("Test error in store_message")
            return original_store_message(message)
        
        with patch.object(self.connector, 'store_message', side_effect=mock_store_with_error), \
             patch('websockets.connect', return_value=mock_websocket), \
             patch.object(self.connector, '_flush_pending_records') as mock_flush:
            
            # Run listen_until_count
            stats = await self.connector.listen_until_count(
                instance=PUBLIC_INSTANCES[0],
                wanted_collections=["app.bsky.feed.post"],
                target_count=10,
                max_time=60,
            )
            
            # Should have received 3 messages but only stored 1
            assert stats["messages_received"] == 3
            assert stats["records_stored"] == 1
            
            # Verify pending records were flushed
            mock_flush.assert_called_once()

    @pytest.mark.asyncio
    async def test_listen_until_count_max_time(self):
        """Test max_time limit in listen_until_count.
        
        Expected behavior:
        - Should stop after max_time is reached
        - Should not reach target_count
        """
        # Mock websockets.connect
        mock_websocket = AsyncMock()
        mock_websocket.__aenter__.return_value = mock_websocket
        
        # Set up mock to return many messages
        mock_websocket.recv.side_effect = [
            json.dumps(self.sample_message) for _ in range(100)
        ]
        
        # Mock time to advance by 1 second per message
        original_time = time.time
        time_call_count = 0
        start_time = original_time()
        
        def mock_time():
            nonlocal time_call_count
            current_time = start_time + time_call_count * 0.5
            time_call_count += 1
            return current_time
        
        with patch.object(self.connector, 'store_message', return_value=True), \
             patch('websockets.connect', return_value=mock_websocket), \
             patch('time.time', side_effect=mock_time):
            
            # Run listen_until_count with short max_time (2 seconds)
            stats = await self.connector.listen_until_count(
                instance=PUBLIC_INSTANCES[0],
                wanted_collections=["app.bsky.feed.post"],
                target_count=100,  # More than we'll get in 2 seconds
                max_time=2,
            )
            
            # Should have processed fewer than target_count
            assert stats["records_stored"] < 100
            assert stats["target_reached"] is False
            
            # Should have run for approximately max_time
            assert stats["total_time"] >= 2
            assert stats["total_time"] < 4  # Allow some flexibility 