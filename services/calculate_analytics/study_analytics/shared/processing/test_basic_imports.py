"""Basic import validation tests for processing modules.

This module tests that all processing functions can be imported and
basic functionality works without requiring external data or configuration.
"""

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

# Test imports
try:
    from services.calculate_analytics.study_analytics.shared.processing.features import (
        calculate_feature_averages,
        calculate_feature_proportions,
        calculate_political_averages,
        calculate_valence_averages,
    )
    from services.calculate_analytics.study_analytics.shared.processing.thresholds import (
        map_date_to_static_week,
        map_date_to_dynamic_week,
        get_latest_survey_timestamp_within_period,
        get_week_threshold_for_user_dynamic,
    )
    from services.calculate_analytics.study_analytics.shared.processing.engagement import (
        get_engagement_summary_per_user,
        calculate_engagement_rates,
    )
    from services.calculate_analytics.study_analytics.shared.processing.utils import (
        calculate_probability_threshold_proportions,
        calculate_label_proportions,
        safe_mean,
        safe_sum,
        validate_probability_series,
    )

    IMPORTS_SUCCESSFUL = True
except ImportError as e:
    print(f"Import error: {e}")
    IMPORTS_SUCCESSFUL = False


class TestProcessingImports(unittest.TestCase):
    """Test that all processing modules can be imported successfully."""

    def test_imports_successful(self):
        """Test that all required modules can be imported."""
        self.assertTrue(
            IMPORTS_SUCCESSFUL, "All processing modules should import successfully"
        )

    def test_features_module_functions_exist(self):
        """Test that features module functions exist and are callable."""
        if not IMPORTS_SUCCESSFUL:
            self.skipTest("Imports failed")

        # Test that functions exist
        self.assertTrue(callable(calculate_feature_averages))
        self.assertTrue(callable(calculate_feature_proportions))
        self.assertTrue(callable(calculate_political_averages))
        self.assertTrue(callable(calculate_valence_averages))

    def test_thresholds_module_functions_exist(self):
        """Test that thresholds module functions exist and are callable."""
        if not IMPORTS_SUCCESSFUL:
            self.skipTest("Imports failed")

        # Test that functions exist
        self.assertTrue(callable(map_date_to_static_week))
        self.assertTrue(callable(map_date_to_dynamic_week))
        self.assertTrue(callable(get_latest_survey_timestamp_within_period))
        self.assertTrue(callable(get_week_threshold_for_user_dynamic))

    def test_engagement_module_functions_exist(self):
        """Test that engagement module functions exist and are callable."""
        if not IMPORTS_SUCCESSFUL:
            self.skipTest("Imports failed")

        # Test that functions exist
        self.assertTrue(callable(get_engagement_summary_per_user))
        self.assertTrue(callable(calculate_engagement_rates))

    def test_utils_module_functions_exist(self):
        """Test that utils module functions exist and are callable."""
        if not IMPORTS_SUCCESSFUL:
            self.skipTest("Imports failed")

        # Test that functions exist
        self.assertTrue(callable(calculate_probability_threshold_proportions))
        self.assertTrue(callable(calculate_label_proportions))
        self.assertTrue(callable(safe_mean))
        self.assertTrue(callable(safe_sum))
        self.assertTrue(callable(validate_probability_series))


class TestUtilsFunctions(unittest.TestCase):
    """Test utility functions with mock data."""

    def setUp(self):
        """Set up test data."""
        if not IMPORTS_SUCCESSFUL:
            self.skipTest("Imports failed")

        # Create test probability series
        self.prob_series = pd.Series([0.1, 0.3, 0.7, 0.9, 0.2, 0.8])
        self.empty_series = pd.Series([])
        self.invalid_series = pd.Series([0.1, 1.5, -0.2, 0.8])  # Invalid probabilities

    def test_calculate_probability_threshold_proportions(self):
        """Test probability threshold proportion calculation."""
        # Test with valid threshold
        proportion_05 = calculate_probability_threshold_proportions(
            self.prob_series, 0.5
        )
        self.assertIsInstance(proportion_05, float)
        self.assertGreaterEqual(proportion_05, 0.0)
        self.assertLessEqual(proportion_05, 1.0)

        # Test with empty series
        proportion_empty = calculate_probability_threshold_proportions(
            self.empty_series, 0.5
        )
        self.assertEqual(proportion_empty, 0.0)

        # Test with high threshold
        proportion_08 = calculate_probability_threshold_proportions(
            self.prob_series, 0.8
        )
        self.assertIsInstance(proportion_08, float)

    def test_safe_mean(self):
        """Test safe mean calculation."""
        # Test with valid series
        mean_val = safe_mean(self.prob_series)
        self.assertIsInstance(mean_val, float)
        self.assertAlmostEqual(mean_val, 0.5, places=1)

        # Test with empty series
        mean_empty = safe_mean(self.empty_series, default=-1.0)
        self.assertEqual(mean_empty, -1.0)

        # Test with default value
        mean_default = safe_mean(self.empty_series, default=42.0)
        self.assertEqual(mean_default, 42.0)

    def test_safe_sum(self):
        """Test safe sum calculation."""
        # Test with valid series
        sum_val = safe_sum(self.prob_series)
        self.assertIsInstance(sum_val, float)
        self.assertAlmostEqual(sum_val, 3.0, places=1)

        # Test with empty series
        sum_empty = safe_sum(self.empty_series, default=-1.0)
        self.assertEqual(sum_empty, -1.0)

    def test_validate_probability_series(self):
        """Test probability series validation."""
        # Test with valid series
        is_valid = validate_probability_series(self.prob_series)
        self.assertTrue(is_valid)

        # Test with empty series
        is_valid_empty = validate_probability_series(self.empty_series)
        self.assertFalse(is_valid_empty)

        # Test with invalid series
        is_valid_invalid = validate_probability_series(self.invalid_series)
        self.assertFalse(is_valid_invalid)

    def test_calculate_label_proportions(self):
        """Test label proportion calculation."""
        # Create test label series
        label_series = pd.Series(["left", "right", "left", "moderate", "right"])
        label_values = ["left", "right", "moderate"]

        proportions = calculate_label_proportions(label_series, label_values)

        # Test structure
        self.assertIsInstance(proportions, dict)
        self.assertIn("prop_is_left", proportions)
        self.assertIn("prop_is_right", proportions)
        self.assertIn("prop_is_moderate", proportions)

        # Test values
        self.assertAlmostEqual(proportions["prop_is_left"], 0.4, places=1)
        self.assertAlmostEqual(proportions["prop_is_right"], 0.4, places=1)
        self.assertAlmostEqual(proportions["prop_is_moderate"], 0.2, places=1)

        # Test with empty series
        proportions_empty = calculate_label_proportions(self.empty_series, label_values)
        self.assertEqual(proportions_empty["prop_is_left"], 0.0)
        self.assertEqual(proportions_empty["prop_is_right"], 0.0)
        self.assertEqual(proportions_empty["prop_is_moderate"], 0.0)


class TestFeaturesFunctions(unittest.TestCase):
    """Test features functions with mock data."""

    def setUp(self):
        """Set up test data."""
        if not IMPORTS_SUCCESSFUL:
            self.skipTest("Imports failed")

        # Create test posts DataFrame
        self.posts_df = pd.DataFrame(
            {
                "prob_toxic": [0.1, 0.8, 0.3],
                "prob_compassion": [0.9, 0.2, 0.8],
                "is_sociopolitical": [0.8, 0.9, 0.7],
                "political_ideology_label": ["left", "right", "moderate"],
                "valence_label": ["positive", "negative", "neutral"],
            }
        )

    @patch("services.calculate_analytics.study_analytics.shared.config.get_config")
    def test_calculate_political_averages(self, mock_get_config):
        """Test political averages calculation."""
        # Mock configuration
        mock_config = MagicMock()
        mock_get_config.return_value = mock_config

        # Test function
        political_averages = calculate_political_averages(self.posts_df)

        # Test structure
        self.assertIsInstance(political_averages, dict)
        self.assertIn("avg_is_political", political_averages)
        self.assertIn("avg_is_political_left", political_averages)
        self.assertIn("avg_is_political_right", political_averages)
        self.assertIn("avg_is_political_moderate", political_averages)

        # Test values
        self.assertAlmostEqual(political_averages["avg_is_political"], 0.8, places=1)
        self.assertAlmostEqual(
            political_averages["avg_is_political_left"], 1 / 3, places=2
        )
        self.assertAlmostEqual(
            political_averages["avg_is_political_right"], 1 / 3, places=2
        )
        self.assertAlmostEqual(
            political_averages["avg_is_political_moderate"], 1 / 3, places=2
        )

    def test_calculate_valence_averages(self):
        """Test valence averages calculation."""
        valence_averages = calculate_valence_averages(self.posts_df)

        # Test structure
        self.assertIsInstance(valence_averages, dict)
        self.assertIn("avg_is_positive", valence_averages)
        self.assertIn("avg_is_neutral", valence_averages)
        self.assertIn("avg_is_negative", valence_averages)

        # Test values
        self.assertAlmostEqual(valence_averages["avg_is_positive"], 1 / 3, places=2)
        self.assertAlmostEqual(valence_averages["avg_is_neutral"], 1 / 3, places=2)
        self.assertAlmostEqual(valence_averages["avg_is_negative"], 1 / 3, places=2)


class TestThresholdsFunctions(unittest.TestCase):
    """Test thresholds functions with mock data."""

    def setUp(self):
        """Set up test data."""
        if not IMPORTS_SUCCESSFUL:
            self.skipTest("Imports failed")

        self.survey_timestamps = ["2024-10-10", "2024-10-20", "2024-10-30"]

    def test_get_latest_survey_timestamp_within_period(self):
        """Test survey timestamp within period calculation."""
        # Test with valid period
        latest = get_latest_survey_timestamp_within_period(
            self.survey_timestamps, "2024-10-05", "2024-10-25"
        )
        self.assertEqual(latest, "2024-10-20")

        # Test with no timestamps in period
        latest_none = get_latest_survey_timestamp_within_period(
            self.survey_timestamps, "2024-11-01", "2024-11-30"
        )
        self.assertIsNone(latest_none)

        # Test with empty timestamps
        latest_empty = get_latest_survey_timestamp_within_period(
            [], "2024-10-05", "2024-10-25"
        )
        self.assertIsNone(latest_empty)

    def test_map_date_to_dynamic_week(self):
        """Test dynamic week mapping."""
        # Test with valid date
        week = map_date_to_dynamic_week("2024-10-25", self.survey_timestamps)
        self.assertEqual(week, 3)

        # Test with date beyond thresholds
        week_beyond = map_date_to_dynamic_week("2024-11-05", self.survey_timestamps)
        self.assertIsNone(week_beyond)

        # Test with first week
        week_first = map_date_to_dynamic_week("2024-10-05", self.survey_timestamps)
        self.assertEqual(week_first, 1)


def run_import_tests():
    """Run all import validation tests."""
    print("Running Processing Module Import Validation Tests...")
    print("=" * 60)

    # Create test suite
    test_suite = unittest.TestSuite()

    # Add test classes
    test_suite.addTest(unittest.makeSuite(TestProcessingImports))
    test_suite.addTest(unittest.makeSuite(TestUtilsFunctions))
    test_suite.addTest(unittest.makeSuite(TestFeaturesFunctions))
    test_suite.addTest(unittest.makeSuite(TestThresholdsFunctions))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Print summary
    print("\n" + "=" * 60)
    print("Import Validation Test Results:")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"  {test}: {traceback}")

    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"  {test}: {traceback}")

    success = len(result.failures) == 0 and len(result.errors) == 0
    print(f"\nOverall Result: {'✅ PASSED' if success else '❌ FAILED'}")

    return success


if __name__ == "__main__":
    success = run_import_tests()
    exit(0 if success else 1)
