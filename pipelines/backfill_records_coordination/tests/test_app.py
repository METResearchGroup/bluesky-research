from unittest import TestCase
from unittest.mock import patch, Mock, MagicMock, call

from click.testing import CliRunner

from pipelines.backfill_records_coordination.app import (
    backfill_records,
    resolve_integration,
    DEFAULT_INTEGRATION_KWARGS,
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
            self.assertEqual(resolve_integration(integration), integration)
            
    def test_resolve_integration_abbreviations(self):
        """Test that abbreviations are correctly resolved."""
        test_cases = {
            'p': 'ml_inference_perspective_api',
            's': 'ml_inference_sociopolitical',
            'i': 'ml_inference_ime'
        }
        for abbrev, full_name in test_cases.items():
            self.assertEqual(resolve_integration(abbrev), full_name)

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    def test_run_integrations_only(self, mock_handler):
        """Test running integrations without record type."""
        mock_handler.return_value = {"statusCode": 200}
        
        result = self.runner.invoke(
            backfill_records,
            ['--run-integrations', '-i', 'p', '-i', 's']
        )
        
        self.assertEqual(result.exit_code, 0)
        mock_handler.assert_called_once_with(
            {
                "payload": {
                    "record_type": None,
                    "add_posts_to_queue": False,
                    "run_integrations": True,
                    "integration": [
                        "ml_inference_perspective_api",
                        "ml_inference_sociopolitical"
                    ],
                    "integration_kwargs": {
                        "ml_inference_perspective_api": {
                            "backfill_period": None,
                            "backfill_duration": None
                        },
                        "ml_inference_sociopolitical": {
                            "backfill_period": None,
                            "backfill_duration": None
                        }
                    },
                    "start_date": None,
                    "end_date": None
                }
            },
            None
        )

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    def test_add_to_queue_requires_record_type(self, mock_handler):
        """Test that add_to_queue requires record_type."""
        result = self.runner.invoke(
            backfill_records,
            ['--add-to-queue']
        )
        
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("--record-type is required when --add-to-queue is used", result.output)
        mock_handler.assert_not_called()

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    def test_backfill_specific_integrations(self, mock_handler):
        """Test backfill with specific integrations."""
        mock_handler.return_value = {"statusCode": 200}
        
        result = self.runner.invoke(
            backfill_records,
            ['--record-type', 'posts', '-i', 'p', '-i', 's', '--add-to-queue', '--run-integrations']
        )
        
        self.assertEqual(result.exit_code, 0)
        mock_handler.assert_called_once_with(
            {
                "payload": {
                    "record_type": "posts",
                    "add_posts_to_queue": True,
                    "run_integrations": True,
                    "integration": [
                        "ml_inference_perspective_api",
                        "ml_inference_sociopolitical"
                    ],
                    "integration_kwargs": {
                        "ml_inference_perspective_api": {
                            "backfill_period": None,
                            "backfill_duration": None
                        },
                        "ml_inference_sociopolitical": {
                            "backfill_period": None,
                            "backfill_duration": None
                        }
                    },
                    "start_date": None,
                    "end_date": None
                }
            },
            None
        )
        
    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    def test_backfill_with_period_and_duration(self, mock_handler):
        """Test backfill with period and duration parameters."""
        mock_handler.return_value = {"statusCode": 200}
        
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
        mock_handler.assert_called_once_with(
            {
                "payload": {
                    "record_type": "posts",
                    "add_posts_to_queue": True,
                    "run_integrations": True,
                    "integration": ["ml_inference_perspective_api"],
                    "integration_kwargs": {
                        "ml_inference_perspective_api": {
                            "backfill_period": "days",
                            "backfill_duration": 2
                        }
                    },
                    "start_date": None,
                    "end_date": None
                }
            },
            None
        )

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    def test_queue_only_no_run(self, mock_handler):
        """Test adding to queue without running integrations."""
        mock_handler.return_value = {"statusCode": 200}
        
        result = self.runner.invoke(
            backfill_records,
            ['--record-type', 'posts', '--add-to-queue']
        )
        
        self.assertEqual(result.exit_code, 0)
        mock_handler.assert_called_once_with(
            {
                "payload": {
                    "record_type": "posts",
                    "add_posts_to_queue": True,
                    "run_integrations": False,
                    "integration_kwargs": {
                        "ml_inference_perspective_api": {
                            "backfill_period": None,
                            "backfill_duration": None
                        },
                        "ml_inference_sociopolitical": {
                            "backfill_period": None,
                            "backfill_duration": None
                        },
                        "ml_inference_ime": {
                            "backfill_period": None,
                            "backfill_duration": None,
                        },
                        "ml_inference_valence_classifier": {
                            "backfill_period": None,
                            "backfill_duration": None,
                        },
                        "ml_inference_intergroup": {
                            "backfill_period": None,
                            "backfill_duration": None,
                        }
                    },
                    "start_date": None,
                    "end_date": None
                }
            },
            None
        )
        
    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    def test_run_only_no_queue(self, mock_handler):
        """Test validation: run_integrations without record_type requires explicit integrations."""
        mock_handler.return_value = {"statusCode": 200}
        
        result = self.runner.invoke(
            backfill_records,
            ['--run-integrations']
        )
        
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn(
            "--integrations is required when --run-integrations is used",
            result.output,
        )
        mock_handler.assert_not_called()

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    @patch('pipelines.backfill_records_coordination.app.write_cache_handler')
    def test_write_cache_all_services(self, mock_write_cache, mock_handler):
        """Test writing cache for all services."""
        mock_write_cache.return_value = {"statusCode": 200}

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache', 'all']
        )

        self.assertEqual(result.exit_code, 0)
        mock_write_cache.assert_called_once_with(
            {"payload": {
                "service": "all", 
                "clear_queue": False,
                "bypass_write": False
            }},
            None
        )

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    @patch('pipelines.backfill_records_coordination.app.write_cache_handler')
    def test_write_cache_specific_service(self, mock_write_cache, mock_handler):
        """Test writing cache for a specific service."""
        mock_write_cache.return_value = {"statusCode": 200}

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache', 'ml_inference_perspective_api']
        )

        self.assertEqual(result.exit_code, 0)
        mock_write_cache.assert_called_once_with(
            {"payload": {
                "service": "ml_inference_perspective_api", 
                "clear_queue": False,
                "bypass_write": False
            }},
            None
        )

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    @patch('pipelines.backfill_records_coordination.app.write_cache_handler')
    def test_write_cache_with_clear_queue(self, mock_write_cache, mock_handler):
        """Test writing cache with clear_queue flag."""
        mock_handler.return_value = {"statusCode": 200}
        mock_write_cache.return_value = {"statusCode": 200}

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache', 'all', '--clear-queue']
        )

        self.assertEqual(result.exit_code, 0)
        mock_write_cache.assert_called_once_with(
            {"payload": {
                "service": "all", 
                "clear_queue": True,
                "bypass_write": False
            }},
            None
        )
        
    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    @patch('pipelines.backfill_records_coordination.app.write_cache_handler')
    def test_write_cache_with_bypass_write_requires_clear_queue(self, mock_write_cache, mock_handler):
        """Test that bypass_write can only be used with clear_queue."""
        # Test without clear_queue
        result = self.runner.invoke(
            backfill_records,
            ['--write-cache', 'all', '--bypass-write']
        )
        
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("--bypass-write requires --write-cache and --clear-queue", result.output)
        mock_write_cache.assert_not_called()

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    @patch('pipelines.backfill_records_coordination.app.write_cache_handler')
    def test_write_cache_with_bypass_write_and_clear_queue(self, mock_write_cache, mock_handler):
        """Test writing cache with bypass_write and clear_queue flags."""
        mock_write_cache.return_value = {"statusCode": 200}

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache', 'all', '--clear-queue', '--bypass-write']
        )

        self.assertEqual(result.exit_code, 0)
        mock_write_cache.assert_called_once_with(
            {"payload": {
                "service": "all", 
                "clear_queue": True,
                "bypass_write": True
            }},
            None
        )

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    @patch('pipelines.backfill_records_coordination.app.write_cache_handler')
    def test_write_cache_with_clear_queue_no_bypass(self, mock_write_cache, mock_handler):
        """Test writing cache with clear_queue but no bypass_write."""
        mock_write_cache.return_value = {"statusCode": 200}

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache', 'all', '--clear-queue']
        )

        self.assertEqual(result.exit_code, 0)
        mock_write_cache.assert_called_once_with(
            {"payload": {
                "service": "all", 
                "clear_queue": True,
                "bypass_write": False
            }},
            None
        )

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

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    def test_backfill_with_date_range(self, mock_handler):
        """Test backfill with date range parameters."""
        mock_handler.return_value = {"statusCode": 200}
        
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
        mock_handler.assert_called_once_with(
            {
                "payload": {
                    "record_type": "posts",
                    "add_posts_to_queue": True,
                    "run_integrations": True,
                    "integration_kwargs": {
                        "ml_inference_perspective_api": {
                            "backfill_period": None,
                            "backfill_duration": None
                        }
                    },
                    "integration": ["ml_inference_perspective_api"],
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31"
                }
            },
            None
        )

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

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    def test_posts_used_in_feeds_with_date_range(self, mock_handler):
        """Test backfill for posts_used_in_feeds with required date range."""
        mock_handler.return_value = {"statusCode": 200}
        
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
        mock_handler.assert_called_once_with(
            {
                "payload": {
                    "record_type": "posts_used_in_feeds",
                    "add_posts_to_queue": True,
                    "run_integrations": False,
                    "integration_kwargs": {
                        "ml_inference_perspective_api": {
                            "backfill_period": None,
                            "backfill_duration": None
                        },
                        "ml_inference_sociopolitical": {
                            "backfill_period": None,
                            "backfill_duration": None
                        },
                        "ml_inference_ime": {
                            "backfill_period": None,
                            "backfill_duration": None,
                        },
                        "ml_inference_valence_classifier": {
                            "backfill_period": None,
                            "backfill_duration": None,
                        },
                        "ml_inference_intergroup": {
                            "backfill_period": None,
                            "backfill_duration": None,
                        }
                    },
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31"
                }
            },
            None
        )

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

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    @patch('pipelines.backfill_records_coordination.app.write_cache_handler')
    def test_write_cache_only_no_backfill(self, mock_write_cache, mock_handler):
        """Test that write_cache alone doesn't trigger backfill."""
        mock_write_cache.return_value = {"statusCode": 200}

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache', 'all']
        )

        self.assertEqual(result.exit_code, 0)
        mock_handler.assert_not_called()
        mock_write_cache.assert_called_once_with(
            {"payload": {
                "service": "all", 
                "clear_queue": False,
                "bypass_write": False
            }},
            None
        )

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    @patch('pipelines.backfill_records_coordination.app.write_cache_handler')
    def test_write_cache_with_backfill(self, mock_write_cache, mock_handler):
        """Test write_cache combined with backfill operations."""
        mock_handler.return_value = {"statusCode": 200}
        mock_write_cache.return_value = {"statusCode": 200}

        result = self.runner.invoke(
            backfill_records,
            [
                '--write-cache', 'all',
                '--record-type', 'posts',
                '-i', 'p',
                '--add-to-queue',
                '--run-integrations'
            ]
        )

        self.assertEqual(result.exit_code, 0)
        # Verify both handlers were called
        mock_handler.assert_called_once()
        mock_write_cache.assert_called_once_with(
            {"payload": {
                "service": "all", 
                "clear_queue": False,
                "bypass_write": False
            }},
            None
        )

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    def test_no_operations_specified(self, mock_handler):
        """Test that no operations (no backfill or write_cache) results in no handler calls."""
        result = self.runner.invoke(backfill_records, [])
        
        self.assertEqual(result.exit_code, 0)
        mock_handler.assert_not_called()

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
    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    def test_clear_queues_with_other_operations(self, mock_handler, mock_queue_cls):
        """Test clearing queues combined with other operations."""
        mock_queue = MagicMock()
        mock_queue.clear_queue.return_value = 3
        mock_queue_cls.return_value = mock_queue
        mock_handler.return_value = {"statusCode": 200}

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
        mock_handler.assert_called_once()
