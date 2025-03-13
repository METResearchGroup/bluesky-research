"""Tests for jetstream_cli.py.

This test suite verifies the functionality of the BlueskyStream CLI tool,
with a focus on command-line parsing and parameter handling.
"""

import asyncio
from unittest.mock import patch, MagicMock, call, AsyncMock

import click
import pytest
from click.testing import CliRunner

from services.sync.jetstream.jetstream_cli import stream_bluesky
from services.sync.jetstream.constants import VALID_COLLECTIONS
from services.sync.jetstream.jetstream_connector import PUBLIC_INSTANCES


class TestJetstreamCli:
    """Tests for the jetstream_cli.py command-line interface.
    
    This class tests the functionality of the CLI tool for connecting
    to the Bluesky firehose via Jetstream.
    """

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner(mix_stderr=False)
        
        # Set up mock stats return value
        self.mock_stats = {
            "records_stored": 100,
            "messages_received": 120,
            "total_time": 10.5,
            "records_per_second": 9.5,
            "collections": ["app.bsky.feed.post"],
            "target_reached": True,
            "latest_cursor": "1672531200000000",
            "queue_length": 100,
            "current_date": "2023-01-01",
            "end_cursor_reached": False,
        }
        
    def test_default_options(self):
        """Test CLI with default options.
        
        Expected behavior:
        - Should use default values for all parameters
        - Should call listen_until_count with these defaults
        """
        with patch('services.sync.jetstream.jetstream_cli.JetstreamConnector') as mock_connector_cls, \
             patch('asyncio.get_event_loop') as mock_loop:
            
            # Set up the mock connector
            mock_connector = MagicMock()
            mock_connector_cls.return_value = mock_connector
            
            # Set up the mock event loop
            mock_coroutine = AsyncMock()
            mock_coroutine.return_value = self.mock_stats
            mock_connector.listen_until_count.return_value = mock_coroutine
            
            mock_loop_instance = MagicMock()
            mock_loop.return_value = mock_loop_instance
            mock_loop_instance.run_until_complete.return_value = self.mock_stats
            
            # Run the CLI with default options, capture output
            with patch('services.sync.jetstream.jetstream_cli.logger') as mock_logger:
                result = self.runner.invoke(stream_bluesky, [], catch_exceptions=False)
                
                # Should exit successfully
                assert result.exit_code == 0
                
                # Should create connector with default queue_name and batch_size
                mock_connector_cls.assert_called_once_with(queue_name="jetstream_sync", batch_size=100)
                
                # Should call listen_until_count with default parameters, including end_timestamp=None
                from unittest.mock import ANY
                mock_connector.listen_until_count.assert_called_once_with(
                    instance=PUBLIC_INSTANCES[0],
                    wanted_collections=ANY,
                    target_count=1000,
                    max_time=300,
                    cursor=None,
                    wanted_dids=None,
                    end_timestamp=None
                )
                
                # Should run the coroutine
                mock_loop_instance.run_until_complete.assert_called_once_with(mock_coroutine)
                
                # Debug print to see what was called
                call_args_list = [str(call) for call in mock_logger.info.call_args_list]
                # Just verify the call was made with some string formatting
                assert any("Records stored" in str(call) for call in mock_logger.info.call_args_list)
                
    def test_custom_collections(self):
        """Test CLI with custom collections.
        
        Expected behavior:
        - Should pass the specified collections to listen_until_count
        """
        with patch('services.sync.jetstream.jetstream_cli.JetstreamConnector') as mock_connector_cls, \
             patch('asyncio.get_event_loop') as mock_loop:
            
            # Set up the mock connector
            mock_connector = MagicMock()
            mock_connector_cls.return_value = mock_connector
            
            # Set up the mock event loop
            mock_coroutine = AsyncMock()
            mock_coroutine.return_value = self.mock_stats
            mock_connector.listen_until_count.return_value = mock_coroutine
            
            mock_loop_instance = MagicMock()
            mock_loop.return_value = mock_loop_instance
            mock_loop_instance.run_until_complete.return_value = self.mock_stats
            
            # Run the CLI with custom collections
            result = self.runner.invoke(stream_bluesky, [
                "-c", "app.bsky.feed.like", 
                "-c", "app.bsky.graph.follow"
            ], catch_exceptions=False)
            
            # Should exit successfully
            assert result.exit_code == 0
            
            # Should call listen_until_count with specified collections
            mock_connector.listen_until_count.assert_called_once()
            call_args = mock_connector.listen_until_count.call_args
            assert set(call_args[1]["wanted_collections"]) == {
                "app.bsky.feed.like", 
                "app.bsky.graph.follow"
            }
            
    def test_custom_dids(self):
        """Test CLI with custom DIDs.
        
        Expected behavior:
        - Should pass the specified DIDs to listen_until_count
        """
        with patch('services.sync.jetstream.jetstream_cli.JetstreamConnector') as mock_connector_cls, \
             patch('asyncio.get_event_loop') as mock_loop:
            
            # Set up the mock connector
            mock_connector = MagicMock()
            mock_connector_cls.return_value = mock_connector
            
            # Set up the mock event loop
            mock_coroutine = AsyncMock()
            mock_coroutine.return_value = self.mock_stats
            mock_connector.listen_until_count.return_value = mock_coroutine
            
            mock_loop_instance = MagicMock()
            mock_loop.return_value = mock_loop_instance
            mock_loop_instance.run_until_complete.return_value = self.mock_stats
            
            # Run the CLI with custom DIDs
            result = self.runner.invoke(stream_bluesky, [
                "-d", "did:plc:abc123", 
                "-d", "did:plc:def456"
            ], catch_exceptions=False)
            
            # Should exit successfully
            assert result.exit_code == 0
            
            # Should call listen_until_count with specified DIDs
            mock_connector.listen_until_count.assert_called_once()
            call_args = mock_connector.listen_until_count.call_args
            assert call_args[1]["wanted_dids"] == ("did:plc:abc123", "did:plc:def456")
            
    def test_start_timestamp(self):
        """Test CLI with start_timestamp.
        
        Expected behavior:
        - Should convert timestamp to cursor
        - Should pass cursor to listen_until_count
        """
        with patch('services.sync.jetstream.jetstream_cli.JetstreamConnector') as mock_connector_cls, \
             patch('asyncio.get_event_loop') as mock_loop, \
             patch('services.sync.jetstream.jetstream_cli.timestamp_to_unix_microseconds') as mock_to_unix:
            
            # Set up the mock connector
            mock_connector = MagicMock()
            mock_connector_cls.return_value = mock_connector
            
            # Set up the mock event loop
            mock_coroutine = AsyncMock()
            mock_coroutine.return_value = self.mock_stats
            mock_connector.listen_until_count.return_value = mock_coroutine
            
            mock_loop_instance = MagicMock()
            mock_loop.return_value = mock_loop_instance
            mock_loop_instance.run_until_complete.return_value = self.mock_stats
            
            # Mock timestamp conversion
            mock_to_unix.return_value = 1672531200000000  # Jan 1, 2023
            
            # Run the CLI with start_timestamp
            result = self.runner.invoke(stream_bluesky, [
                "-s", "2023-01-01"
            ], catch_exceptions=False)
            
            # Should exit successfully
            assert result.exit_code == 0
            
            # Should convert timestamp
            mock_to_unix.assert_called_once_with("2023-01-01")
            
            # Should call listen_until_count with cursor
            mock_connector.listen_until_count.assert_called_once()
            call_args = mock_connector.listen_until_count.call_args
            assert call_args[1]["cursor"] == "1672531200000000"
            
    def test_custom_queue_and_batch_size(self):
        """Test CLI with custom queue_name and batch_size.
        
        Expected behavior:
        - Should initialize connector with specified queue_name and batch_size
        """
        with patch('services.sync.jetstream.jetstream_cli.JetstreamConnector') as mock_connector_cls, \
             patch('asyncio.get_event_loop') as mock_loop:
            
            # Set up the mock connector
            mock_connector = MagicMock()
            mock_connector_cls.return_value = mock_connector
            
            # Set up the mock event loop
            mock_coroutine = AsyncMock()
            mock_coroutine.return_value = self.mock_stats
            mock_connector.listen_until_count.return_value = mock_coroutine
            
            mock_loop_instance = MagicMock()
            mock_loop.return_value = mock_loop_instance
            mock_loop_instance.run_until_complete.return_value = self.mock_stats
            
            # Run the CLI with custom queue and batch size
            result = self.runner.invoke(stream_bluesky, [
                "--queue-name", "custom_queue",
                "--batch-size", "50"
            ], catch_exceptions=False)
            
            # Should exit successfully
            assert result.exit_code == 0
            
            # Should create connector with specified queue_name and batch_size
            mock_connector_cls.assert_called_once_with(queue_name="custom_queue", batch_size=50)
            
    def test_num_records_and_max_time(self):
        """Test CLI with custom num_records and max_time.
        
        Expected behavior:
        - Should pass specified values to listen_until_count
        """
        with patch('services.sync.jetstream.jetstream_cli.JetstreamConnector') as mock_connector_cls, \
             patch('asyncio.get_event_loop') as mock_loop:
            
            # Set up the mock connector
            mock_connector = MagicMock()
            mock_connector_cls.return_value = mock_connector
            
            # Set up the mock event loop
            mock_coroutine = AsyncMock()
            mock_coroutine.return_value = self.mock_stats
            mock_connector.listen_until_count.return_value = mock_coroutine
            
            mock_loop_instance = MagicMock()
            mock_loop.return_value = mock_loop_instance
            mock_loop_instance.run_until_complete.return_value = self.mock_stats
            
            # Run the CLI with custom num_records and max_time
            result = self.runner.invoke(stream_bluesky, [
                "-n", "500",
                "-t", "600"
            ], catch_exceptions=False)
            
            # Should exit successfully
            assert result.exit_code == 0
            
            # Should call listen_until_count with specified values
            mock_connector.listen_until_count.assert_called_once()
            call_args = mock_connector.listen_until_count.call_args
            assert call_args[1]["target_count"] == 500
            assert call_args[1]["max_time"] == 600
            
    def test_invalid_collection(self):
        """Test CLI with invalid collection.
        
        Expected behavior:
        - Should fail with an error code
        """
        result = self.runner.invoke(stream_bluesky, [
            "-c", "invalid.collection"
        ], catch_exceptions=True)

        # Should exit with an error
        assert result.exit_code != 0

    def test_invalid_instance(self):
        """Test CLI with invalid instance.
        
        Expected behavior:
        - Should fail with an error code
        """
        result = self.runner.invoke(stream_bluesky, [
            "-i", "invalid.instance"
        ], catch_exceptions=True)

        # Should exit with an error
        assert result.exit_code != 0

    def test_invalid_timestamp(self):
        """Test CLI with invalid timestamp.
        
        Expected behavior:
        - Should fail with an error code
        """
        result = self.runner.invoke(stream_bluesky, [
            "-s", "2023/01/01"  # Wrong format
        ], catch_exceptions=True)

        # Should exit with an error
        assert result.exit_code != 0

    def test_end_timestamp_handling(self):
        """Test CLI with end_timestamp parameter.
        
        Expected behavior:
        - Should pass end_timestamp to listen_until_count
        - Should report end_cursor_reached in output if True
        """
        with patch('services.sync.jetstream.jetstream_cli.JetstreamConnector') as mock_connector_cls, \
             patch('asyncio.get_event_loop') as mock_loop:
            
            # Set up the mock connector
            mock_connector = MagicMock()
            mock_connector_cls.return_value = mock_connector
            
            # Set up the mock event loop with stats indicating end_cursor_reached
            mock_stats_with_end_reached = self.mock_stats.copy()
            mock_stats_with_end_reached["end_cursor_reached"] = True
            
            mock_coroutine = AsyncMock()
            mock_coroutine.return_value = mock_stats_with_end_reached
            mock_connector.listen_until_count.return_value = mock_coroutine
            
            mock_loop_instance = MagicMock()
            mock_loop.return_value = mock_loop_instance
            mock_loop_instance.run_until_complete.return_value = mock_stats_with_end_reached
            
            # Run the CLI with end_timestamp, capture stderr and mock logger
            with patch('services.sync.jetstream.jetstream_cli.logger') as mock_logger:
                result = self.runner.invoke(stream_bluesky, [
                    "--end-timestamp", "2023-01-31"
                ], catch_exceptions=False)
                
                # Should exit successfully
                assert result.exit_code == 0
                
                # Should call listen_until_count with end_timestamp
                mock_connector.listen_until_count.assert_called_once()
                call_args = mock_connector.listen_until_count.call_args
                assert call_args[1]["end_timestamp"] == "2023-01-31"
                
                # Just verify the call was made with some string containing the relevant message
                assert any("Stopped because end timestamp was reached" in str(call) for call in mock_logger.info.call_args_list)
                
    def test_user_handles_warning(self):
        """Test CLI with user handles.
        
        Expected behavior:
        - Should warn that user handles are not yet implemented
        """
        with patch('services.sync.jetstream.jetstream_cli.JetstreamConnector') as mock_connector_cls, \
             patch('asyncio.get_event_loop') as mock_loop:
            
            # Set up the mock connector
            mock_connector = MagicMock()
            mock_connector_cls.return_value = mock_connector
            
            # Set up the mock event loop
            mock_coroutine = AsyncMock()
            mock_coroutine.return_value = self.mock_stats
            mock_connector.listen_until_count.return_value = mock_coroutine
            
            mock_loop_instance = MagicMock()
            mock_loop.return_value = mock_loop_instance
            mock_loop_instance.run_until_complete.return_value = self.mock_stats
            
            # Run the CLI with user handles, capture stderr and mock logger
            with patch('services.sync.jetstream.jetstream_cli.logger') as mock_logger:
                result = self.runner.invoke(stream_bluesky, [
                    "-u", "alice.bsky.social,bob.bsky.social"
                ], catch_exceptions=False)
                
                # Should exit successfully
                assert result.exit_code == 0
                
                # Just verify the call was made with some string containing the relevant message
                assert any("not yet implemented" in str(call) for call in mock_logger.info.call_args_list)
                
    def test_error_handling(self):
        """Test CLI error handling.
        
        Expected behavior:
        - Should catch and log exceptions
        - Should exit with non-zero code
        """
        with patch('services.sync.jetstream.jetstream_cli.JetstreamConnector') as mock_connector_cls, \
             patch('asyncio.get_event_loop') as mock_loop:
            
            # Set up the mock connector to raise an exception
            mock_connector = MagicMock()
            mock_connector_cls.return_value = mock_connector
            
            # Set up the mock event loop to raise an exception
            mock_loop_instance = MagicMock()
            mock_loop.return_value = mock_loop_instance
            mock_loop_instance.run_until_complete.side_effect = Exception("Test error")
            
            # Run the CLI which should now encounter an error, capture stderr and mock logger
            with patch('services.sync.jetstream.jetstream_cli.logger') as mock_logger:
                result = self.runner.invoke(stream_bluesky, [], catch_exceptions=True)
                
                # Should exit with an error
                assert result.exit_code != 0
                
                # Just verify the call was made with some string containing the relevant message
                assert any("Error during ingestion" in str(call) for call in mock_logger.error.call_args_list) 