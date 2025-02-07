from unittest import TestCase
from unittest.mock import patch, Mock

from click.testing import CliRunner

from pipelines.backfill_records_coordination.app import backfill_records, resolve_integration

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
                            "backfill_duration": None,
                            "run_classification": True
                        },
                        "ml_inference_sociopolitical": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
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
                            "backfill_duration": None,
                            "run_classification": True
                        },
                        "ml_inference_sociopolitical": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
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
                            "backfill_duration": 2,
                            "run_classification": True
                        }
                    },
                    "start_date": None,
                    "end_date": None
                }
            },
            None
        )

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    def test_backfill_no_classification(self, mock_handler):
        """Test backfill with classification disabled."""
        mock_handler.return_value = {"statusCode": 200}
        
        result = self.runner.invoke(
            backfill_records,
            [
                '--record-type', 'posts',
                '-i', 'p',
                '--add-to-queue',
                '--run-integrations',
                '--no-run-classification'
            ]
        )
        
        expected_args = {
            "payload": {
                "record_type": "posts",
                "add_posts_to_queue": True,
                "run_integrations": True,
                "integration_kwargs": {
                    "ml_inference_perspective_api": {
                        "backfill_period": None,
                        "backfill_duration": None,
                        "run_classification": False
                    }
                },
                "integration": ["ml_inference_perspective_api"],
                "start_date": None,
                "end_date": None
            }
        }
        self.assertEqual(result.exit_code, 0)
        mock_handler.assert_called_once_with(expected_args, None)
        
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
                            "backfill_duration": None,
                            "run_classification": True
                        },
                        "ml_inference_sociopolitical": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
                        },
                        "ml_inference_ime": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
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
        """Test running integrations without adding to queue."""
        mock_handler.return_value = {"statusCode": 200}
        
        result = self.runner.invoke(
            backfill_records,
            ['--run-integrations']
        )
        
        self.assertEqual(result.exit_code, 0)
        mock_handler.assert_called_once_with(
            {
                "payload": {
                    "record_type": None,
                    "add_posts_to_queue": False,
                    "run_integrations": True,
                    "integration_kwargs": {
                        "ml_inference_perspective_api": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
                        },
                        "ml_inference_sociopolitical": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
                        },
                        "ml_inference_ime": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
                        }
                    },
                    "start_date": None,
                    "end_date": None
                }
            },
            None
        )

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    @patch('pipelines.backfill_records_coordination.app.write_cache_handler')
    def test_write_cache_all_services(self, mock_write_cache, mock_handler):
        """Test writing cache for all services."""
        mock_handler.return_value = {"statusCode": 200}
        mock_write_cache.return_value = {"statusCode": 200}

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache', 'all']
        )

        self.assertEqual(result.exit_code, 0)
        mock_write_cache.assert_called_once_with(
            {"payload": {"service": "all"}},
            None
        )

    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    @patch('pipelines.backfill_records_coordination.app.write_cache_handler')
    def test_write_cache_specific_service(self, mock_write_cache, mock_handler):
        """Test writing cache for a specific service."""
        mock_handler.return_value = {"statusCode": 200}
        mock_write_cache.return_value = {"statusCode": 200}

        result = self.runner.invoke(
            backfill_records,
            ['--write-cache', 'ml_inference_perspective_api']
        )

        self.assertEqual(result.exit_code, 0)
        mock_write_cache.assert_called_once_with(
            {"payload": {"service": "ml_inference_perspective_api"}},
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
                            "backfill_duration": None,
                            "run_classification": True
                        },
                        "ml_inference_sociopolitical": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
                        },
                        "ml_inference_ime": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
                        }
                    },
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
                '--end-date', '2024-01-31'
            ]
        )
        
        self.assertEqual(result.exit_code, 0)
        mock_handler.assert_called_once_with(
            {
                "payload": {
                    "record_type": "posts_used_in_feeds",
                    "add_posts_to_queue": False,
                    "run_integrations": False,
                    "integration_kwargs": {
                        "ml_inference_perspective_api": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
                        },
                        "ml_inference_sociopolitical": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
                        },
                        "ml_inference_ime": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
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
