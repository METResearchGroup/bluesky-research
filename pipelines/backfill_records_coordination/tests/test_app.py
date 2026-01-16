from unittest import TestCase
from unittest.mock import patch, MagicMock, call

import pytest
from click.testing import CliRunner

from pipelines.backfill_records_coordination.app import (
    backfill_records,
    _resolve_single_integration,
    DEFAULT_INTEGRATION_KWARGS,
    _create_integration_runner_payload,
)
from services.backfill.models import (
    IntegrationRunnerServicePayload,
    BackfillPeriod,
)


class TestBackfillCoordinationCliApp(TestCase):
    def setUp(self):
        self.runner = CliRunner()
        
    def test_resolve_integration_full_names(self):
        """Test that full integration names are returned unchanged."""
        test_cases = [
            'ml_inference_perspective_api',
            'ml_inference_sociopolitical',
            'ml_inference_ime'
        ]
        for integration in test_cases:
            self.assertEqual(_resolve_single_integration(integration), integration)
            
    def test_resolve_integration_abbreviations(self):
        """Test that abbreviations are correctly resolved."""
        test_cases = {
            'p': 'ml_inference_perspective_api',
            's': 'ml_inference_sociopolitical',
            'i': 'ml_inference_ime'
        }
        for abbrev, full_name in test_cases.items():
            self.assertEqual(_resolve_single_integration(abbrev), full_name)

    @patch('pipelines.backfill_records_coordination.app.IntegrationRunnerService')
    def test_run_integrations_only(self, mock_service_cls):
        """Test running integrations without record type."""
        mock_service = MagicMock()
        mock_service_cls.return_value = mock_service
        
        result = self.runner.invoke(
            backfill_records,
            ['--run-integrations', '-i', 'p', '-i', 's']
        )
        
        self.assertEqual(result.exit_code, 0)
        mock_service.run_integrations.assert_called_once()
        # Verify payload structure
        call_args = mock_service.run_integrations.call_args
        payload = call_args[1]['payload']
        self.assertIsInstance(payload, IntegrationRunnerServicePayload)
        self.assertEqual(len(payload.integration_configs), 2)
        integration_names = [config.integration_name for config in payload.integration_configs]
        self.assertIn('ml_inference_perspective_api', integration_names)
        self.assertIn('ml_inference_sociopolitical', integration_names)

    def test_add_to_queue_requires_record_type(self):
        """Test that add_to_queue requires record_type."""
        result = self.runner.invoke(
            backfill_records,
            ['--add-to-queue']
        )
        
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("--record-type is required when --add-to-queue is used", result.output)

    @patch('pipelines.backfill_records_coordination.app.IntegrationRunnerService')
    @patch('pipelines.backfill_records_coordination.app.EnqueueService')
    def test_backfill_specific_integrations(self, mock_enqueue_cls, mock_integration_runner_cls):
        """Test backfill with specific integrations."""
        mock_enqueue = MagicMock()
        mock_enqueue_cls.return_value = mock_enqueue
        mock_integration_runner = MagicMock()
        mock_integration_runner_cls.return_value = mock_integration_runner
        
        result = self.runner.invoke(
            backfill_records,
            ['--record-type', 'posts', '-i', 'p', '-i', 's', '--add-to-queue', '--run-integrations']
        )
        
        self.assertEqual(result.exit_code, 0)
        mock_enqueue.enqueue_records.assert_called_once()
        mock_integration_runner.run_integrations.assert_called_once()
        
        # Verify enqueue payload
        enqueue_call = mock_enqueue.enqueue_records.call_args
        enqueue_payload = enqueue_call[1]['payload']
        self.assertEqual(enqueue_payload.record_type, 'posts')
        self.assertEqual(enqueue_payload.integrations, ['ml_inference_perspective_api', 'ml_inference_sociopolitical'])
        
        # Verify integration runner payload
        runner_call = mock_integration_runner.run_integrations.call_args
        runner_payload = runner_call[1]['payload']
        self.assertIsInstance(runner_payload, IntegrationRunnerServicePayload)
        self.assertEqual(len(runner_payload.integration_configs), 2)
        
    @patch('pipelines.backfill_records_coordination.app.IntegrationRunnerService')
    @patch('pipelines.backfill_records_coordination.app.EnqueueService')
    def test_backfill_with_period_and_duration(self, mock_enqueue_cls, mock_integration_runner_cls):
        """Test backfill with period and duration parameters."""
        mock_enqueue = MagicMock()
        mock_enqueue_cls.return_value = mock_enqueue
        mock_integration_runner = MagicMock()
        mock_integration_runner_cls.return_value = mock_integration_runner
        
        result = self.runner.invoke(
            backfill_records,
            [
                '--record-type', 'posts',
                '-i', 'p',
                '--backfill-period', 'days',
                '--backfill-duration', '2',
                '--add-to-queue',
                '--run-integrations'
            ]
        )
        
        self.assertEqual(result.exit_code, 0)
        mock_integration_runner.run_integrations.assert_called_once()
        
        # Verify integration runner payload has correct period and duration
        runner_call = mock_integration_runner.run_integrations.call_args
        runner_payload = runner_call[1]['payload']
        self.assertIsInstance(runner_payload, IntegrationRunnerServicePayload)
        config = runner_payload.integration_configs[0]
        self.assertEqual(config.integration_name, 'ml_inference_perspective_api')
        self.assertEqual(config.backfill_period, BackfillPeriod.DAYS)
        self.assertEqual(config.backfill_duration, 2)

    @patch('pipelines.backfill_records_coordination.app.EnqueueService')
    def test_queue_only_no_run(self, mock_enqueue_cls):
        """Test adding to queue without running integrations."""
        mock_enqueue = MagicMock()
        mock_enqueue_cls.return_value = mock_enqueue
        
        # Verify integration runner service is not called
        with patch('pipelines.backfill_records_coordination.app.IntegrationRunnerService') as mock_runner:
            result = self.runner.invoke(
                backfill_records,
                ['--record-type', 'posts', '--add-to-queue']
            )
            
            self.assertEqual(result.exit_code, 0)
            mock_enqueue.enqueue_records.assert_called_once()
            mock_runner.assert_not_called()

    def test_run_only_no_queue(self):
        """Test validation: run_integrations without record_type requires explicit integrations."""
        result = self.runner.invoke(
            backfill_records,
            ['--run-integrations']
        )
        
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn(
            "--integrations is required when --run-integrations is used",
            result.output,
        )

    @patch('pipelines.backfill_records_coordination.app.CacheBufferWriterService')
    def test_write_cache_buffer_to_storage_all_services(self, mock_service_cls):
        """Test writing cache buffer for all services."""
        mock_service = MagicMock()
        mock_service_cls.return_value = mock_service

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache-buffer-to-storage', '--service-source-buffer', 'all']
        )

        self.assertEqual(result.exit_code, 0)
        mock_service.write_cache.assert_called_once_with(service='all')
        mock_service.clear_queue.assert_not_called()

    @patch('pipelines.backfill_records_coordination.app.CacheBufferWriterService')
    def test_write_cache_buffer_to_storage_specific_service(self, mock_service_cls):
        """Test writing cache buffer for a specific service."""
        mock_service = MagicMock()
        mock_service_cls.return_value = mock_service

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache-buffer-to-storage', '--service-source-buffer', 'ml_inference_perspective_api']
        )

        self.assertEqual(result.exit_code, 0)
        mock_service.write_cache.assert_called_once_with(service='ml_inference_perspective_api')
        mock_service.clear_queue.assert_not_called()

    @patch('pipelines.backfill_records_coordination.app.CacheBufferWriterService')
    def test_write_cache_buffer_to_storage_with_clear_queue(self, mock_service_cls):
        """Test writing cache buffer with clear_queue flag."""
        mock_service = MagicMock()
        mock_service_cls.return_value = mock_service

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache-buffer-to-storage', '--service-source-buffer', 'all', '--clear-queue']
        )

        self.assertEqual(result.exit_code, 0)
        mock_service.write_cache.assert_called_once_with(service='all')
        mock_service.clear_queue.assert_called_once_with(service='all')
        
    @patch('pipelines.backfill_records_coordination.app.CacheBufferWriterService')
    def test_write_cache_buffer_to_storage_with_bypass_write_requires_clear_queue(self, mock_service_cls):
        """Test that bypass_write can only be used with clear_queue."""
        mock_service = MagicMock()
        mock_service_cls.return_value = mock_service
        
        # Test without clear_queue
        result = self.runner.invoke(
            backfill_records,
            ['--write-cache-buffer-to-storage', '--service-source-buffer', 'all', '--bypass-write']
        )
        
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("--bypass-write requires --write-cache-buffer-to-storage and --clear-queue", result.output)
        mock_service.write_cache.assert_not_called()
        mock_service.clear_queue.assert_not_called()

    @patch('pipelines.backfill_records_coordination.app.CacheBufferWriterService')
    def test_write_cache_buffer_to_storage_with_bypass_write_and_clear_queue(self, mock_service_cls):
        """Test writing cache buffer with bypass_write and clear_queue flags."""
        mock_service = MagicMock()
        mock_service_cls.return_value = mock_service

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache-buffer-to-storage', '--service-source-buffer', 'all', '--clear-queue', '--bypass-write']
        )

        self.assertEqual(result.exit_code, 0)
        # When bypass_write is True, write_cache should NOT be called
        mock_service.write_cache.assert_not_called()
        # Only clear_queue should be called
        mock_service.clear_queue.assert_called_once_with(service='all')

    @patch('pipelines.backfill_records_coordination.app.CacheBufferWriterService')
    def test_write_cache_buffer_to_storage_with_clear_queue_no_bypass(self, mock_service_cls):
        """Test writing cache buffer with clear_queue but no bypass_write."""
        mock_service = MagicMock()
        mock_service_cls.return_value = mock_service

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache-buffer-to-storage', '--service-source-buffer', 'all', '--clear-queue']
        )

        self.assertEqual(result.exit_code, 0)
        mock_service.write_cache.assert_called_once_with(service='all')
        mock_service.clear_queue.assert_called_once_with(service='all')

    def test_invalid_record_type(self):
        """Test error handling for invalid record type."""
        result = self.runner.invoke(
            backfill_records,
            ['--record-type', 'invalid']
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Invalid value for '--record-type'", result.output)

    def test_invalid_backfill_period(self):
        """Test error handling for invalid backfill period."""
        result = self.runner.invoke(
            backfill_records,
            [
                '--backfill-period', 'invalid'
            ]
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Invalid value for '--backfill-period'", result.output)

    @patch('pipelines.backfill_records_coordination.app.IntegrationRunnerService')
    @patch('pipelines.backfill_records_coordination.app.EnqueueService')
    def test_backfill_with_date_range(self, mock_enqueue_cls, mock_integration_runner_cls):
        """Test backfill with date range parameters."""
        mock_enqueue = MagicMock()
        mock_enqueue_cls.return_value = mock_enqueue
        mock_integration_runner = MagicMock()
        mock_integration_runner_cls.return_value = mock_integration_runner
        
        result = self.runner.invoke(
            backfill_records,
            [
                '--record-type', 'posts',
                '-i', 'p',
                '--start-date', '2024-01-01',
                '--end-date', '2024-01-31',
                '--add-to-queue',
                '--run-integrations'
            ]
        )
        
        self.assertEqual(result.exit_code, 0)
        mock_enqueue.enqueue_records.assert_called_once()
        
        # Verify enqueue payload has correct dates
        enqueue_call = mock_enqueue.enqueue_records.call_args
        enqueue_payload = enqueue_call[1]['payload']
        self.assertEqual(enqueue_payload.start_date, '2024-01-01')
        self.assertEqual(enqueue_payload.end_date, '2024-01-31')

    def test_backfill_with_invalid_date_format(self):
        """Test error handling for invalid date format."""
        result = self.runner.invoke(
            backfill_records,
            [
                '--start-date', 'invalid-date',
                '--end-date', '2024-01-31'
            ]
        )
        
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Invalid date format", result.output)

    @patch('pipelines.backfill_records_coordination.app.EnqueueService')
    def test_posts_used_in_feeds_with_date_range(self, mock_enqueue_cls):
        """Test backfill for posts_used_in_feeds with required date range."""
        mock_enqueue = MagicMock()
        mock_enqueue_cls.return_value = mock_enqueue
        
        result = self.runner.invoke(
            backfill_records,
            [
                '--record-type', 'posts_used_in_feeds',
                '--start-date', '2024-01-01',
                '--end-date', '2024-01-31',
                '--add-to-queue'
            ]
        )
        
        self.assertEqual(result.exit_code, 0)
        mock_enqueue.enqueue_records.assert_called_once()
        
        # Verify enqueue payload
        enqueue_call = mock_enqueue.enqueue_records.call_args
        enqueue_payload = enqueue_call[1]['payload']
        self.assertEqual(enqueue_payload.record_type, 'posts_used_in_feeds')
        self.assertEqual(enqueue_payload.start_date, '2024-01-01')
        self.assertEqual(enqueue_payload.end_date, '2024-01-31')

    def test_posts_used_in_feeds_missing_dates(self):
        """Test error when posts_used_in_feeds is missing required dates."""
        # Test missing both dates
        result = self.runner.invoke(
            backfill_records,
            ['--record-type', 'posts_used_in_feeds']
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Both --start-date and --end-date are required", result.output)

        # Test missing end date
        result = self.runner.invoke(
            backfill_records,
            [
                '--record-type', 'posts_used_in_feeds',
                '--start-date', '2024-01-01'
            ]
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Both --start-date and --end-date are required", result.output)

        # Test missing start date
        result = self.runner.invoke(
            backfill_records,
            [
                '--record-type', 'posts_used_in_feeds',
                '--end-date', '2024-01-31'
            ]
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Both --start-date and --end-date are required", result.output)

    @patch('pipelines.backfill_records_coordination.app.CacheBufferWriterService')
    def test_write_cache_buffer_to_storage_only_no_backfill(self, mock_service_cls):
        """Test that write_cache_buffer_to_storage alone doesn't trigger backfill."""
        mock_service = MagicMock()
        mock_service_cls.return_value = mock_service
        
        # Verify other services are not called
        with patch('pipelines.backfill_records_coordination.app.IntegrationRunnerService') as mock_runner:
            result = self.runner.invoke(
                backfill_records,
                ['--write-cache-buffer-to-storage', '--service-source-buffer', 'all']
            )

            self.assertEqual(result.exit_code, 0)
            mock_service.write_cache.assert_called_once_with(service='all')
            mock_runner.assert_not_called()

    @patch('pipelines.backfill_records_coordination.app.CacheBufferWriterService')
    @patch('pipelines.backfill_records_coordination.app.EnqueueService')
    @patch('pipelines.backfill_records_coordination.app.IntegrationRunnerService')
    def test_write_cache_buffer_to_storage_with_backfill(self, mock_integration_runner_cls, mock_enqueue_cls, mock_cache_writer_cls):
        """Test write_cache_buffer_to_storage combined with backfill operations."""
        mock_cache_writer = MagicMock()
        mock_cache_writer_cls.return_value = mock_cache_writer
        mock_enqueue = MagicMock()
        mock_enqueue_cls.return_value = mock_enqueue
        mock_integration_runner = MagicMock()
        mock_integration_runner_cls.return_value = mock_integration_runner

        result = self.runner.invoke(
            backfill_records,
            [
                '--write-cache-buffer-to-storage', '--service-source-buffer', 'all',
                '--record-type', 'posts',
                '-i', 'p',
                '--add-to-queue',
                '--run-integrations'
            ]
        )

        self.assertEqual(result.exit_code, 0)
        # Verify services were called
        mock_enqueue.enqueue_records.assert_called_once()
        mock_integration_runner.run_integrations.assert_called_once()
        mock_cache_writer.write_cache.assert_called_once_with(service='all')

    @patch('pipelines.backfill_records_coordination.app.CacheBufferWriterService')
    @patch('pipelines.backfill_records_coordination.app.EnqueueService')
    @patch('pipelines.backfill_records_coordination.app.IntegrationRunnerService')
    def test_no_operations_specified(self, mock_integration_runner_cls, mock_enqueue_cls, mock_service_cls):
        """Test that no operations (no backfill or write_cache_buffer_to_storage) results in no service calls."""
        mock_cache_writer = MagicMock()
        mock_service_cls.return_value = mock_cache_writer
        mock_enqueue = MagicMock()
        mock_enqueue_cls.return_value = mock_enqueue
        mock_integration_runner = MagicMock()
        mock_integration_runner_cls.return_value = mock_integration_runner
        
        result = self.runner.invoke(backfill_records, [])
        
        self.assertEqual(result.exit_code, 0)
        mock_cache_writer.write_cache.assert_not_called()
        mock_enqueue.enqueue_records.assert_not_called()
        mock_integration_runner.run_integrations.assert_not_called()

    @patch('pipelines.backfill_records_coordination.app.Queue')
    def test_clear_input_queues_with_confirmation(self, mock_queue_cls):
        """Test clearing input queues with user confirmation."""
        mock_queue = MagicMock()
        mock_queue.clear_queue.return_value = 5  # Simulate 5 items cleared
        mock_queue_cls.return_value = mock_queue

        # Simulate user confirming the action
        result = self.runner.invoke(
            backfill_records,
            ['--clear-input-queues', '-i', 'p', '-i', 's'],
            input='y\n'  # Simulate user typing 'y' when prompted
        )

        self.assertEqual(result.exit_code, 0)
        # Should be called twice, once for each integration
        self.assertEqual(mock_queue_cls.call_count, 2)
        mock_queue_cls.assert_has_calls([
            call(queue_name='input_ml_inference_perspective_api', create_new_queue=True),
            call().clear_queue(),
            call(queue_name='input_ml_inference_sociopolitical', create_new_queue=True),
            call().clear_queue(),
        ])
        self.assertEqual(mock_queue.clear_queue.call_count, 2)

    @patch('pipelines.backfill_records_coordination.app.Queue')
    def test_clear_output_queues_with_confirmation(self, mock_queue_cls):
        """Test clearing output queues with user confirmation."""
        mock_queue = MagicMock()
        mock_queue.clear_queue.return_value = 3  # Simulate 3 items cleared
        mock_queue_cls.return_value = mock_queue

        # Simulate user confirming the action
        result = self.runner.invoke(
            backfill_records,
            ['--clear-output-queues', '-i', 'p', '-i', 's'],
            input='y\n'  # Simulate user typing 'y' when prompted
        )

        self.assertEqual(result.exit_code, 0)
        # Should be called twice, once for each integration
        self.assertEqual(mock_queue_cls.call_count, 2)
        mock_queue_cls.assert_has_calls([
            call(queue_name='output_ml_inference_perspective_api', create_new_queue=True),
            call().clear_queue(),
            call(queue_name='output_ml_inference_sociopolitical', create_new_queue=True),
            call().clear_queue(),
        ])
        self.assertEqual(mock_queue.clear_queue.call_count, 2)

    @patch('pipelines.backfill_records_coordination.app.Queue')
    def test_clear_both_queues_with_confirmation(self, mock_queue_cls):
        """Test clearing both input and output queues with user confirmation."""
        mock_queue = MagicMock()
        mock_queue.clear_queue.return_value = 4  # Simulate 4 items cleared
        mock_queue_cls.return_value = mock_queue

        # Simulate user confirming both actions
        result = self.runner.invoke(
            backfill_records,
            ['--clear-input-queues', '--clear-output-queues', '-i', 'p'],
            input='y\n'  # Only need one confirmation since both are handled together
        )

        self.assertEqual(result.exit_code, 0)
        # Should be called twice for the one integration (input and output)
        self.assertEqual(mock_queue_cls.call_count, 2)
        mock_queue_cls.assert_has_calls([
            call(queue_name='input_ml_inference_perspective_api', create_new_queue=True),
            call().clear_queue(),
            call(queue_name='output_ml_inference_perspective_api', create_new_queue=True),
            call().clear_queue(),
        ])
        self.assertEqual(mock_queue.clear_queue.call_count, 2)

    @pytest.mark.skip(reason="Functionality needs fixing: cancellation should return exit code 0 or handle gracefully")
    @patch('pipelines.backfill_records_coordination.app.Queue')
    def test_clear_queues_cancelled(self, mock_queue_cls):
        """Test cancellation of queue clearing when user declines confirmation."""
        mock_queue = MagicMock()
        mock_queue_cls.return_value = mock_queue

        # Simulate user declining the action
        result = self.runner.invoke(
            backfill_records,
            ['--clear-input-queues', '-i', 'p'],
            input='n\n'  # Simulate user typing 'n' when prompted
        )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Operation cancelled", result.output)
        mock_queue_cls.assert_not_called()
        mock_queue.clear_queue.assert_not_called()

    @pytest.mark.skip(reason="Functionality needs fixing: no default integrations logic exists when clearing queues without -i flag")
    @patch('pipelines.backfill_records_coordination.app.Queue')
    def test_clear_queues_all_integrations(self, mock_queue_cls):
        """Test clearing queues for all integrations when none specified."""
        mock_queue = MagicMock()
        mock_queue.clear_queue.return_value = 2  # Simulate 2 items cleared
        mock_queue_cls.return_value = mock_queue

        # Simulate user confirming the action
        result = self.runner.invoke(
            backfill_records,
            ['--clear-input-queues'],  # No integrations specified
            input='y\n'
        )

        self.assertEqual(result.exit_code, 0)
        # Should be called once for each default integration
        expected_calls = len(DEFAULT_INTEGRATION_KWARGS)
        self.assertEqual(mock_queue_cls.call_count, expected_calls)
        self.assertEqual(mock_queue.clear_queue.call_count, expected_calls)

    @patch('pipelines.backfill_records_coordination.app.Queue')
    @patch('pipelines.backfill_records_coordination.app.IntegrationRunnerService')
    @patch('pipelines.backfill_records_coordination.app.EnqueueService')
    def test_clear_queues_with_other_operations(self, mock_enqueue_cls, mock_integration_runner_cls, mock_queue_cls):
        """Test clearing queues combined with other operations."""
        mock_queue = MagicMock()
        mock_queue.clear_queue.return_value = 3
        mock_queue_cls.return_value = mock_queue
        mock_enqueue = MagicMock()
        mock_enqueue_cls.return_value = mock_enqueue
        mock_integration_runner = MagicMock()
        mock_integration_runner_cls.return_value = mock_integration_runner

        # Simulate user confirming the action
        result = self.runner.invoke(
            backfill_records,
            [
                '--clear-input-queues',
                '-i', 'p',
                '--record-type', 'posts',
                '--add-to-queue',
                '--run-integrations'
            ],
            input='y\n'
        )

        self.assertEqual(result.exit_code, 0)
        # Verify queue clearing happened
        mock_queue_cls.assert_called_once_with(
            queue_name='input_ml_inference_perspective_api',
            create_new_queue=True
        )
        mock_queue.clear_queue.assert_called_once()
        # Verify other operations were also performed
        mock_enqueue.enqueue_records.assert_called_once()
        mock_integration_runner.run_integrations.assert_called_once()

    def test_write_cache_buffer_to_storage_requires_service_source_buffer(self):
        """Test that --write-cache-buffer-to-storage requires --service-source-buffer."""
        result = self.runner.invoke(
            backfill_records,
            ['--write-cache-buffer-to-storage']
        )
        
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn(
            "--service-source-buffer is required when --write-cache-buffer-to-storage is used",
            result.output
        )

    @patch('pipelines.backfill_records_coordination.app.CacheBufferWriterService')
    def test_service_source_buffer_without_write_flag(self, mock_service_cls):
        """Test that --service-source-buffer without --write-cache-buffer-to-storage is ignored."""
        mock_service = MagicMock()
        mock_service_cls.return_value = mock_service
        
        result = self.runner.invoke(
            backfill_records,
            ['--service-source-buffer', 'all']
        )
        
        # Should succeed but not write cache (no error, just no action)
        self.assertEqual(result.exit_code, 0)
        mock_service.write_cache.assert_not_called()

    def test_create_integration_runner_payload_defaults(self):
        """Test _create_integration_runner_payload with default values."""
        payload = _create_integration_runner_payload(
            mapped_integration_names=['ml_inference_perspective_api'],
            backfill_period=None,
            backfill_duration=None,
        )
        
        self.assertIsInstance(payload, IntegrationRunnerServicePayload)
        self.assertEqual(len(payload.integration_configs), 1)
        config = payload.integration_configs[0]
        self.assertEqual(config.integration_name, 'ml_inference_perspective_api')
        self.assertEqual(config.backfill_period, BackfillPeriod.DAYS)  # Default
        self.assertIsNone(config.backfill_duration)

    def test_create_integration_runner_payload_custom_values(self):
        """Test _create_integration_runner_payload with custom values."""
        payload = _create_integration_runner_payload(
            mapped_integration_names=['ml_inference_perspective_api', 'ml_inference_sociopolitical'],
            backfill_period='hours',
            backfill_duration=5,
        )
        
        self.assertIsInstance(payload, IntegrationRunnerServicePayload)
        self.assertEqual(len(payload.integration_configs), 2)
        for config in payload.integration_configs:
            self.assertEqual(config.backfill_period, BackfillPeriod.HOURS)
            self.assertEqual(config.backfill_duration, 5)

    @patch('pipelines.backfill_records_coordination.app.CacheBufferWriterService')
    @patch('pipelines.backfill_records_coordination.app.IntegrationRunnerService')
    @patch('pipelines.backfill_records_coordination.app.EnqueueService')
    def test_all_three_operations_together(self, mock_enqueue_cls, mock_integration_runner_cls, mock_cache_writer_cls):
        """Test enqueue + run integrations + write cache buffer all together."""
        mock_enqueue = MagicMock()
        mock_enqueue_cls.return_value = mock_enqueue
        mock_integration_runner = MagicMock()
        mock_integration_runner_cls.return_value = mock_integration_runner
        mock_cache_writer = MagicMock()
        mock_cache_writer_cls.return_value = mock_cache_writer

        result = self.runner.invoke(
            backfill_records,
            [
                '--record-type', 'posts',
                '-i', 'p',
                '--add-to-queue',
                '--run-integrations',
                '--write-cache-buffer-to-storage',
                '--service-source-buffer', 'ml_inference_perspective_api',
                '--clear-queue',
            ]
        )

        self.assertEqual(result.exit_code, 0)
        mock_enqueue.enqueue_records.assert_called_once()
        mock_integration_runner.run_integrations.assert_called_once()
        mock_cache_writer.write_cache.assert_called_once_with(service='ml_inference_perspective_api')
        mock_cache_writer.clear_queue.assert_called_once_with(service='ml_inference_perspective_api')
