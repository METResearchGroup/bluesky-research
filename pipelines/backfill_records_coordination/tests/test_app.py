from unittest import TestCase
from unittest.mock import patch, Mock
from click.testing import CliRunner
from pipelines.backfill_records_coordination.app import run_integration, resolve_integration

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

    @patch('pipelines.backfill_records_coordination.app.route_and_run_integration_request')
    def test_cli_with_full_names(self, mock_route_and_run):
        """Test CLI with full integration names."""
        mock_response = Mock()
        mock_response.statusCode = 200
        mock_route_and_run.return_value = mock_response
        
        test_cases = [
            'ml_inference_perspective_api',
            'ml_inference_sociopolitical',
            'ml_inference_ime'
        ]
        
        for integration in test_cases:
            result = self.runner.invoke(run_integration, ['--integration', integration])
            self.assertEqual(result.exit_code, 0)
            self.assertIn(f"Integration {integration} completed with status: 200", result.output)
            
    @patch('pipelines.backfill_records_coordination.app.route_and_run_integration_request')
    def test_cli_with_abbreviations(self, mock_route_and_run):
        """Test CLI with abbreviated integration names."""
        mock_response = Mock()
        mock_response.statusCode = 200
        mock_route_and_run.return_value = mock_response
        
        test_cases = ['p', 's', 'i']
        
        for abbrev in test_cases:
            result = self.runner.invoke(run_integration, ['--integration', abbrev])
            self.assertEqual(result.exit_code, 0)
            full_name = resolve_integration(abbrev)
            self.assertIn(f"Integration {full_name} completed with status: 200", result.output)
            
    def test_cli_with_invalid_option(self):
        """Test CLI with invalid integration option."""
        result = self.runner.invoke(run_integration, ['--integration', 'invalid'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Invalid value for '--integration'", result.output) 
