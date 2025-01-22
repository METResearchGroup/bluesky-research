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
    def test_backfill_all_integrations(self, mock_handler):
        """Test backfill with default settings (all integrations)."""
        mock_handler.return_value = {"statusCode": 200}
        
        result = self.runner.invoke(backfill_records, ['--record-type', 'posts'])
        
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
                        }
                    }
                }
            },
            None
        )
        
    @patch('pipelines.backfill_records_coordination.app.lambda_handler')
    def test_backfill_specific_integrations(self, mock_handler):
        """Test backfill with specific integrations."""
        mock_handler.return_value = {"statusCode": 200}
        
        result = self.runner.invoke(
            backfill_records,
            ['--record-type', 'posts', '-i', 'p', '-i', 's']
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
                        }
                    }
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
                '--backfill-duration', '2'
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
                    }
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
                "integration": ["ml_inference_perspective_api"]
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
            ['--record-type', 'posts', '--no-run-integrations']
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
                        }
                    }
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
            ['--record-type', 'posts', '--no-add-to-queue']
        )
        
        self.assertEqual(result.exit_code, 0)
        mock_handler.assert_called_once_with(
            {
                "payload": {
                    "record_type": "posts",
                    "add_posts_to_queue": False,
                    "run_integrations": True,
                    "integration_kwargs": {
                        "ml_inference_perspective_api": {
                            "backfill_period": None,
                            "backfill_duration": None,
                            "run_classification": True
                        }
                    }
                }
            },
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
                '--record-type', 'posts',
                '--backfill-period', 'invalid'
            ]
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Invalid value for '--backfill-period'", result.output) 
