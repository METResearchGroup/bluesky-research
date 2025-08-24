"""Pipeline framework testing suite.

This module provides comprehensive tests for the ABC-based pipeline framework,
including unit tests, integration tests, and mock-based testing.
"""

import unittest
from unittest.mock import Mock, patch

import pandas as pd

from services.calculate_analytics.study_analytics.shared.pipelines import (
    BaseResearchPipeline,
    BaseFeedAnalysisPipeline,
    FeedAnalysisPipeline,
    WeeklyThresholdsPipeline,
    EngagementAnalysisPipeline,
    PipelineOrchestrator,
    PipelineState,
    PipelineResult,
    PipelineError,
)


class TestBaseResearchPipeline(unittest.TestCase):
    """Test cases for BaseResearchPipeline abstract base class."""

    def setUp(self):
        """Set up test fixtures."""

        # Create a concrete implementation for testing
        class TestPipeline(BaseResearchPipeline):
            def setup(self):
                if "fail_setup" in self.config:
                    raise PipelineError("Setup failed", self.name, "setup")

            def execute(self):
                if "fail_execute" in self.config:
                    raise PipelineError("Execution failed", self.name, "execute")

                return PipelineResult(
                    success=True, data={"test": "data"}, metadata={"test": True}
                )

            def cleanup(self):
                if "fail_cleanup" in self.config:
                    raise PipelineError("Cleanup failed", self.name, "cleanup")

            def validate(self):
                if "fail_validation" in self.config:
                    return False
                return True

        self.TestPipeline = TestPipeline

    def test_pipeline_initialization(self):
        """Test pipeline initialization with default configuration."""
        pipeline = self.TestPipeline("test_pipeline")

        self.assertEqual(pipeline.name, "test_pipeline")
        self.assertEqual(pipeline.state, PipelineState.PENDING)
        self.assertEqual(pipeline.config, {})
        self.assertIsNone(pipeline.start_time)
        self.assertIsNone(pipeline.end_time)
        self.assertIsNone(pipeline.execution_time)
        self.assertIsNone(pipeline.error)
        self.assertEqual(pipeline.metadata, {})

    def test_pipeline_initialization_with_config(self):
        """Test pipeline initialization with custom configuration."""
        config = {"param1": "value1", "param2": "value2"}
        pipeline = self.TestPipeline("test_pipeline", config=config)

        self.assertEqual(pipeline.config, config)

    def test_pipeline_setup_failure(self):
        """Test pipeline setup failure handling."""
        config = {"fail_setup": True}
        pipeline = self.TestPipeline("test_pipeline", config=config)

        result = pipeline.run()

        self.assertFalse(result.success)
        self.assertEqual(pipeline.state, PipelineState.FAILED)
        self.assertIsNotNone(pipeline.error)
        self.assertIn("Setup failed", result.error)

    def test_pipeline_execution_failure(self):
        """Test pipeline execution failure handling."""
        config = {"fail_execute": True}
        pipeline = self.TestPipeline("test_pipeline", config=config)

        result = pipeline.run()

        self.assertFalse(result.success)
        self.assertEqual(pipeline.state, PipelineState.FAILED)
        self.assertIsNotNone(pipeline.error)
        self.assertIn("Execution failed", result.error)

    def test_pipeline_validation_failure(self):
        """Test pipeline validation failure handling."""
        config = {"fail_validation": True}
        pipeline = self.TestPipeline("test_pipeline", config=config)

        result = pipeline.run()

        self.assertFalse(result.success)
        self.assertEqual(pipeline.state, PipelineState.FAILED)
        self.assertIsNotNone(pipeline.error)
        self.assertIn("validation failed", result.error)

    def test_pipeline_successful_execution(self):
        """Test successful pipeline execution."""
        pipeline = self.TestPipeline("test_pipeline")

        result = pipeline.run()

        self.assertTrue(result.success)
        self.assertEqual(pipeline.state, PipelineState.COMPLETED)
        self.assertIsNone(pipeline.error)
        self.assertIsNotNone(pipeline.start_time)
        self.assertIsNotNone(pipeline.end_time)
        self.assertIsNotNone(pipeline.execution_time)
        self.assertEqual(result.data, {"test": "data"})
        self.assertEqual(result.metadata, {"test": True})

    def test_pipeline_cleanup_failure(self):
        """Test pipeline cleanup failure handling."""
        config = {"fail_cleanup": True}
        pipeline = self.TestPipeline("test_pipeline", config=config)

        result = pipeline.run()

        # Pipeline should still complete successfully even if cleanup fails
        self.assertTrue(result.success)
        self.assertEqual(pipeline.state, PipelineState.COMPLETED)
        # Cleanup error should be logged but not affect result

    def test_pipeline_status(self):
        """Test pipeline status retrieval."""
        pipeline = self.TestPipeline("test_pipeline")

        status = pipeline.get_status()

        expected_keys = [
            "name",
            "state",
            "start_time",
            "end_time",
            "execution_time",
            "error",
            "metadata",
        ]
        for key in expected_keys:
            self.assertIn(key, status)

        self.assertEqual(status["name"], "test_pipeline")
        self.assertEqual(status["state"], PipelineState.PENDING.value)

    def test_pipeline_cancellation(self):
        """Test pipeline cancellation."""
        pipeline = self.TestPipeline("test_pipeline")

        # Set pipeline to running state
        pipeline.state = PipelineState.RUNNING

        # Cancel pipeline
        pipeline.cancel()

        self.assertEqual(pipeline.state, PipelineState.CANCELLED)

    def test_pipeline_cancellation_not_running(self):
        """Test pipeline cancellation when not running."""
        pipeline = self.TestPipeline("test_pipeline")

        # Cancel pipeline when not running
        pipeline.cancel()

        # State should remain unchanged
        self.assertEqual(pipeline.state, PipelineState.PENDING)


class TestBaseFeedAnalysisPipeline(unittest.TestCase):
    """Test cases for BaseFeedAnalysisPipeline abstract base class."""

    def setUp(self):
        """Set up test fixtures."""

        # Create a concrete implementation for testing
        class TestFeedPipeline(BaseFeedAnalysisPipeline):
            def setup(self):
                pass

            def execute(self):
                return PipelineResult(success=True, data=pd.DataFrame())

            def cleanup(self):
                pass

            def validate(self):
                return True

            def load_feed_data(self, partition_date):
                return {"user1": pd.DataFrame({"col1": [1, 2, 3]})}

            def calculate_feed_features(self, posts_df, user):
                return {"user": user, "feature1": 1.0}

            def aggregate_feed_results(self, user_results):
                return pd.DataFrame(user_results)

        self.TestFeedPipeline = TestFeedPipeline

    def test_feed_pipeline_initialization(self):
        """Test feed pipeline initialization."""
        pipeline = self.TestFeedPipeline("test_feed_pipeline")

        self.assertEqual(pipeline.name, "test_feed_pipeline")
        self.assertEqual(pipeline.feature_config, {})
        self.assertEqual(pipeline.study_config, {})

    def test_feed_pipeline_initialization_with_config(self):
        """Test feed pipeline initialization with configuration."""
        config = {"features": {"feature1": "value1"}, "study": {"study1": "value1"}}
        pipeline = self.TestFeedPipeline("test_feed_pipeline", config=config)

        self.assertEqual(pipeline.feature_config, {"feature1": "value1"})
        self.assertEqual(pipeline.study_config, {"study1": "value1"})

    def test_validate_feed_data_empty(self):
        """Test feed data validation with empty DataFrame."""
        pipeline = self.TestFeedPipeline("test_pipeline")

        empty_df = pd.DataFrame()
        result = pipeline.validate_feed_data(empty_df)

        self.assertFalse(result)

    def test_validate_feed_data_missing_columns(self):
        """Test feed data validation with missing required columns."""
        pipeline = self.TestFeedPipeline("test_pipeline")
        pipeline.feature_config = {"required_columns": ["col1", "col2"]}

        df = pd.DataFrame({"col1": [1, 2, 3]})  # Missing col2
        result = pipeline.validate_feed_data(df)

        self.assertFalse(result)

    def test_validate_feed_data_valid(self):
        """Test feed data validation with valid data."""
        pipeline = self.TestFeedPipeline("test_pipeline")
        pipeline.feature_config = {"required_columns": ["col1"]}

        df = pd.DataFrame({"col1": [1, 2, 3]})
        result = pipeline.validate_feed_data(df)

        self.assertTrue(result)


class TestFeedAnalysisPipeline(unittest.TestCase):
    """Test cases for FeedAnalysisPipeline concrete implementation."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock the configuration and data loading dependencies
        self.config_patcher = patch(
            "services.calculate_analytics.study_analytics.shared.pipelines.feed_analysis.get_config"
        )
        self.data_loading_patcher = patch(
            "services.calculate_analytics.study_analytics.shared.pipelines.feed_analysis.get_hydrated_feed_posts_per_user"
        )
        self.processing_patcher = patch(
            "services.calculate_analytics.study_analytics.shared.pipelines.feed_analysis.calculate_feature_averages"
        )

        self.mock_config = self.config_patcher.start()
        self.mock_data_loading = self.data_loading_patcher.start()
        self.mock_processing = self.processing_patcher.start()

        # Setup mock configuration
        mock_config_obj = Mock()
        mock_config_obj.features = Mock()
        mock_config_obj.features.toxicity_features = ["toxic", "severe_toxic"]
        mock_config_obj.study = Mock()
        mock_config_obj.study.wave_1_study_start_date_inclusive = "2024-09-30"
        mock_config_obj.study.wave_2_study_end_date_inclusive = "2024-12-01"

        self.mock_config.return_value = mock_config_obj

        # Setup mock data loading
        self.mock_data_loading.return_value = {
            "user1": pd.DataFrame(
                {"prob_toxic": [0.1, 0.2, 0.3], "prob_severe_toxic": [0.05, 0.1, 0.15]}
            )
        }

        # Setup mock processing
        self.mock_processing.return_value = {
            "user": "user1",
            "avg_prob_toxic": 0.2,
            "avg_prob_severe_toxic": 0.1,
        }

    def tearDown(self):
        """Clean up test fixtures."""
        self.config_patcher.stop()
        self.data_loading_patcher.stop()
        self.processing_patcher.stop()

    def test_feed_pipeline_initialization(self):
        """Test feed analysis pipeline initialization."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")

        self.assertEqual(pipeline.name, "test_feed_pipeline")
        self.assertIn("exclude_partition_dates", pipeline.config)
        self.assertIn("default_label_threshold", pipeline.config)
        self.assertIn("load_unfiltered_posts", pipeline.config)

    def test_feed_pipeline_setup_success(self):
        """Test successful pipeline setup."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")

        # Setup should not raise any exceptions
        pipeline.setup()

        self.assertEqual(pipeline.state, PipelineState.PENDING)

    def test_feed_pipeline_setup_missing_config(self):
        """Test pipeline setup with missing configuration."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")
        pipeline.config = {}  # Clear default config

        with self.assertRaises(PipelineError):
            pipeline.setup()

    def test_feed_pipeline_setup_missing_feature_config(self):
        """Test pipeline setup with missing feature configuration."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")

        # Mock missing feature configuration
        mock_config_obj = Mock()
        mock_config_obj.features = Mock()
        del mock_config_obj.features.toxicity_features
        self.mock_config.return_value = mock_config_obj

        with self.assertRaises(PipelineError):
            pipeline.setup()

    def test_feed_pipeline_execute_no_partition_date(self):
        """Test pipeline execution without setting partition date."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")
        pipeline.setup()

        with self.assertRaises(PipelineError):
            pipeline.execute()

    def test_feed_pipeline_execute_success(self):
        """Test successful pipeline execution."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")
        pipeline.setup()
        pipeline.set_partition_date("2024-10-15")

        result = pipeline.execute()

        self.assertTrue(result.success)
        self.assertIsNotNone(result.data)
        self.assertIn("partition_date", result.metadata)
        self.assertIn("users_processed", result.metadata)

    def test_feed_pipeline_cleanup(self):
        """Test pipeline cleanup."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")
        pipeline.setup()

        # Set some state
        pipeline.current_partition_date = "2024-10-15"
        pipeline.user_results = [{"user": "user1"}]
        pipeline.final_results = pd.DataFrame([{"user": "user1"}])
        pipeline.metadata = {"test": True}

        pipeline.cleanup()

        self.assertIsNone(pipeline.current_partition_date)
        self.assertEqual(pipeline.user_results, [])
        self.assertIsNone(pipeline.final_results)
        self.assertEqual(pipeline.metadata, {})

    def test_feed_pipeline_validation_success(self):
        """Test successful pipeline validation."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")
        pipeline.setup()

        # Set valid results
        pipeline.final_results = pd.DataFrame(
            {"user": ["user1", "user2"], "user_did": ["user1", "user2"]}
        )

        result = pipeline.validate()
        self.assertTrue(result)

    def test_feed_pipeline_validation_no_results(self):
        """Test pipeline validation with no results."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")
        pipeline.setup()

        result = pipeline.validate()
        self.assertFalse(result)

    def test_feed_pipeline_validation_missing_columns(self):
        """Test pipeline validation with missing required columns."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")
        pipeline.setup()

        # Set results with missing columns
        pipeline.final_results = pd.DataFrame(
            {
                "user": ["user1", "user2"]
                # Missing user_did column
            }
        )

        result = pipeline.validate()
        self.assertFalse(result)

    def test_feed_pipeline_validation_duplicate_users(self):
        """Test pipeline validation with duplicate users."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")
        pipeline.setup()

        # Set results with duplicate users
        pipeline.final_results = pd.DataFrame(
            {
                "user": ["user1", "user1"],  # Duplicate user
                "user_did": ["user1", "user1"],
            }
        )

        result = pipeline.validate()
        self.assertTrue(result)  # Should pass but log warning

    def test_set_partition_date(self):
        """Test setting partition date."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")

        pipeline.set_partition_date("2024-10-15")

        self.assertEqual(pipeline.current_partition_date, "2024-10-15")

    def test_get_partition_dates(self):
        """Test getting available partition dates."""
        pipeline = FeedAnalysisPipeline("test_feed_pipeline")

        dates = pipeline.get_partition_dates()

        self.assertIsInstance(dates, list)
        self.assertGreater(len(dates), 0)
        # Should exclude the configured excluded date
        self.assertNotIn("2024-10-08", dates)


class TestWeeklyThresholdsPipeline(unittest.TestCase):
    """Test cases for WeeklyThresholdsPipeline concrete implementation."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock the configuration and data loading dependencies
        self.config_patcher = patch(
            "services.calculate_analytics.study_analytics.shared.pipelines.weekly_thresholds.get_config"
        )
        self.data_loading_patcher = patch(
            "services.calculate_analytics.study_analytics.shared.pipelines.weekly_thresholds.load_user_demographic_info"
        )
        self.processing_patcher = patch(
            "services.calculate_analytics.study_analytics.shared.pipelines.weekly_thresholds.get_week_thresholds_per_user_static"
        )

        self.mock_config = self.config_patcher.start()
        self.mock_data_loading = self.data_loading_patcher.start()
        self.mock_processing = self.processing_patcher.start()

        # Setup mock configuration
        mock_config_obj = Mock()
        mock_config_obj.study = Mock()
        mock_config_obj.study.wave_1_study_start_date_inclusive = "2024-09-30"
        mock_config_obj.study.wave_2_study_end_date_inclusive = "2024-12-01"
        mock_config_obj.weeks = Mock()
        mock_config_obj.weeks.wave_1_week_start_dates_inclusive = ["2024-09-30"]

        self.mock_config.return_value = mock_config_obj

        # Setup mock data loading
        self.mock_data_loading.return_value = pd.DataFrame(
            {
                "bluesky_handle": ["user1", "user2"],
                "bluesky_user_did": ["user1", "user2"],
                "condition": ["wave1", "wave2"],
                "is_study_user": [True, True],
            }
        )

        # Setup mock processing
        self.mock_processing.return_value = pd.DataFrame(
            {
                "bluesky_handle": ["user1", "user2"],
                "wave": [1, 2],
                "date": ["2024-10-01", "2024-10-01"],
                "week_static": [1, 1],
            }
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.config_patcher.stop()
        self.data_loading_patcher.stop()
        self.processing_patcher.stop()

    def test_weekly_thresholds_pipeline_initialization(self):
        """Test weekly thresholds pipeline initialization."""
        pipeline = WeeklyThresholdsPipeline("test_thresholds_pipeline")

        self.assertEqual(pipeline.name, "test_thresholds_pipeline")
        self.assertIn("exclude_partition_dates", pipeline.config)
        self.assertIn("calculate_static", pipeline.config)
        self.assertIn("calculate_dynamic", pipeline.config)

    def test_weekly_thresholds_pipeline_setup_success(self):
        """Test successful pipeline setup."""
        pipeline = WeeklyThresholdsPipeline("test_thresholds_pipeline")

        # Setup should not raise any exceptions
        pipeline.setup()

        self.assertEqual(pipeline.state, PipelineState.PENDING)

    def test_weekly_thresholds_pipeline_setup_missing_config(self):
        """Test pipeline setup with missing configuration."""
        pipeline = WeeklyThresholdsPipeline("test_thresholds_pipeline")
        pipeline.config = {}  # Clear default config

        with self.assertRaises(PipelineError):
            pipeline.setup()

    def test_weekly_thresholds_pipeline_setup_missing_study_config(self):
        """Test pipeline setup with missing study configuration."""
        pipeline = WeeklyThresholdsPipeline("test_thresholds_pipeline")

        # Mock missing study configuration
        mock_config_obj = Mock()
        mock_config_obj.study = Mock()
        del mock_config_obj.study.wave_1_study_start_date_inclusive
        self.mock_config.return_value = mock_config_obj

        with self.assertRaises(PipelineError):
            pipeline.setup()

    def test_weekly_thresholds_pipeline_execute_success(self):
        """Test successful pipeline execution."""
        pipeline = WeeklyThresholdsPipeline("test_thresholds_pipeline")
        pipeline.setup()

        result = pipeline.execute()

        self.assertTrue(result.success)
        self.assertIsNotNone(result.data)
        self.assertIn("users_processed", result.metadata)
        self.assertIn("static_thresholds_count", result.metadata)

    def test_weekly_thresholds_pipeline_cleanup(self):
        """Test pipeline cleanup."""
        pipeline = WeeklyThresholdsPipeline("test_thresholds_pipeline")
        pipeline.setup()

        # Set some state
        pipeline.user_demographics = pd.DataFrame({"user": ["user1"]})
        pipeline.static_thresholds = pd.DataFrame({"user": ["user1"]})
        pipeline.final_results = pd.DataFrame({"user": ["user1"]})
        pipeline.metadata = {"test": True}

        pipeline.cleanup()

        self.assertIsNone(pipeline.user_demographics)
        self.assertIsNone(pipeline.static_thresholds)
        self.assertIsNone(pipeline.final_results)
        self.assertEqual(pipeline.metadata, {})

    def test_weekly_thresholds_pipeline_validation_success(self):
        """Test successful pipeline validation."""
        pipeline = WeeklyThresholdsPipeline("test_thresholds_pipeline")
        pipeline.setup()

        # Set valid results
        pipeline.final_results = pd.DataFrame(
            {
                "bluesky_handle": ["user1", "user2"],
                "wave": [1, 2],
                "date": ["2024-10-01", "2024-10-01"],
            }
        )

        result = pipeline.validate()
        self.assertTrue(result)

    def test_weekly_thresholds_pipeline_validation_no_results(self):
        """Test pipeline validation with no results."""
        pipeline = WeeklyThresholdsPipeline("test_thresholds_pipeline")
        pipeline.setup()

        result = pipeline.validate()
        self.assertFalse(result)

    def test_weekly_thresholds_pipeline_validation_missing_columns(self):
        """Test pipeline validation with missing required columns."""
        pipeline = WeeklyThresholdsPipeline("test_thresholds_pipeline")
        pipeline.setup()

        # Set results with missing columns
        pipeline.final_results = pd.DataFrame(
            {
                "bluesky_handle": ["user1", "user2"]
                # Missing wave and date columns
            }
        )

        result = pipeline.validate()
        self.assertFalse(result)

    def test_weekly_thresholds_pipeline_validation_invalid_waves(self):
        """Test pipeline validation with invalid wave values."""
        pipeline = WeeklyThresholdsPipeline("test_thresholds_pipeline")
        pipeline.setup()

        # Set results with invalid wave values
        pipeline.final_results = pd.DataFrame(
            {
                "bluesky_handle": ["user1", "user2"],
                "wave": [1, 3],  # Invalid wave value
                "date": ["2024-10-01", "2024-10-01"],
            }
        )

        result = pipeline.validate()
        self.assertFalse(result)

    def test_weekly_thresholds_pipeline_validation_invalid_weeks(self):
        """Test pipeline validation with invalid week values."""
        pipeline = WeeklyThresholdsPipeline("test_thresholds_pipeline")
        pipeline.setup()

        # Set results with invalid week values
        pipeline.final_results = pd.DataFrame(
            {
                "bluesky_handle": ["user1", "user2"],
                "wave": [1, 2],
                "date": ["2024-10-01", "2024-10-01"],
                "week_static": [1, 9],  # Invalid week value
            }
        )

        result = pipeline.validate()
        self.assertFalse(result)


class TestEngagementAnalysisPipeline(unittest.TestCase):
    """Test cases for EngagementAnalysisPipeline concrete implementation."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock the configuration and data loading dependencies
        self.config_patcher = patch(
            "services.calculate_analytics.study_analytics.shared.pipelines.engagement_analysis.get_config"
        )
        self.data_loading_patcher = patch(
            "services.calculate_analytics.study_analytics.shared.pipelines.engagement_analysis.load_study_users"
        )
        self.processing_patcher = patch(
            "services.calculate_analytics.study_analytics.shared.pipelines.engagement_analysis.get_engagement_summary_per_user"
        )

        self.mock_config = self.config_patcher.start()
        self.mock_data_loading = self.data_loading_patcher.start()
        self.mock_processing = self.processing_patcher.start()

        # Setup mock configuration
        mock_config_obj = Mock()
        mock_config_obj.study = Mock()
        mock_config_obj.study.wave_1_study_start_date_inclusive = "2024-09-30"
        mock_config_obj.study.wave_2_study_end_date_inclusive = "2024-12-01"

        self.mock_config.return_value = mock_config_obj

        # Setup mock data loading
        self.mock_data_loading.return_value = [
            {"user": "user1", "id": "1"},
            {"user": "user2", "id": "2"},
        ]

        # Setup mock processing
        self.mock_processing.return_value = pd.DataFrame(
            {
                "user": ["user1", "user2"],
                "total_likes": [10, 20],
                "total_posts": [5, 10],
            }
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.config_patcher.stop()
        self.data_loading_patcher.stop()
        self.processing_patcher.stop()

    def test_engagement_analysis_pipeline_initialization(self):
        """Test engagement analysis pipeline initialization."""
        pipeline = EngagementAnalysisPipeline("test_engagement_pipeline")

        self.assertEqual(pipeline.name, "test_engagement_pipeline")
        self.assertIn("exclude_partition_dates", pipeline.config)
        self.assertIn("include_likes", pipeline.config)
        self.assertIn("include_posts", pipeline.config)

    def test_engagement_analysis_pipeline_setup_success(self):
        """Test successful pipeline setup."""
        pipeline = EngagementAnalysisPipeline("test_engagement_pipeline")

        # Setup should not raise any exceptions
        pipeline.setup()

        self.assertEqual(pipeline.state, PipelineState.PENDING)

    def test_engagement_analysis_pipeline_setup_missing_config(self):
        """Test pipeline setup with missing configuration."""
        pipeline = EngagementAnalysisPipeline("test_engagement_pipeline")
        pipeline.config = {}  # Clear default config

        with self.assertRaises(PipelineError):
            pipeline.setup()

    def test_engagement_analysis_pipeline_setup_missing_study_config(self):
        """Test pipeline setup with missing study configuration."""
        pipeline = EngagementAnalysisPipeline("test_engagement_pipeline")

        # Mock missing study configuration
        mock_config_obj = Mock()
        mock_config_obj.study = Mock()
        del mock_config_obj.study.wave_1_study_start_date_inclusive
        self.mock_config.return_value = mock_config_obj

        with self.assertRaises(PipelineError):
            pipeline.setup()

    def test_engagement_analysis_pipeline_execute_success(self):
        """Test successful pipeline execution."""
        pipeline = EngagementAnalysisPipeline("test_engagement_pipeline")
        pipeline.setup()

        result = pipeline.execute()

        self.assertTrue(result.success)
        self.assertIsNotNone(result.data)
        self.assertIn("users_processed", result.metadata)
        self.assertIn("partition_dates", result.metadata)

    def test_engagement_analysis_pipeline_cleanup(self):
        """Test pipeline cleanup."""
        pipeline = EngagementAnalysisPipeline("test_engagement_pipeline")
        pipeline.setup()

        # Set some state
        pipeline.users = [{"user": "user1"}]
        pipeline.partition_dates = ["2024-10-01"]
        pipeline.final_results = {"users": [{"user": "user1"}]}
        pipeline.metadata = {"test": True}

        pipeline.cleanup()

        self.assertIsNone(pipeline.users)
        self.assertIsNone(pipeline.partition_dates)
        self.assertIsNone(pipeline.final_results)
        self.assertEqual(pipeline.metadata, {})

    def test_engagement_analysis_pipeline_validation_success(self):
        """Test successful pipeline validation."""
        pipeline = EngagementAnalysisPipeline("test_engagement_pipeline")
        pipeline.setup()

        # Set valid results
        pipeline.final_results = {
            "users": [{"user": "user1"}],
            "partition_dates": ["2024-10-01"],
            "engagement_summary": pd.DataFrame(
                {"user": ["user1"], "total_likes": [10]}
            ),
        }

        result = pipeline.validate()
        self.assertTrue(result)

    def test_engagement_analysis_pipeline_validation_no_results(self):
        """Test pipeline validation with no results."""
        pipeline = EngagementAnalysisPipeline("test_engagement_pipeline")
        pipeline.setup()

        result = pipeline.validate()
        self.assertFalse(result)

    def test_engagement_analysis_pipeline_validation_missing_users(self):
        """Test pipeline validation with missing users."""
        pipeline = EngagementAnalysisPipeline("test_engagement_pipeline")
        pipeline.setup()

        # Set results with missing users
        pipeline.final_results = {
            "partition_dates": ["2024-10-01"]
            # Missing users
        }

        result = pipeline.validate()
        self.assertFalse(result)

    def test_engagement_analysis_pipeline_validation_missing_partition_dates(self):
        """Test pipeline validation with missing partition dates."""
        pipeline = EngagementAnalysisPipeline("test_engagement_pipeline")
        pipeline.setup()

        # Set results with missing partition dates
        pipeline.final_results = {
            "users": [{"user": "user1"}]
            # Missing partition_dates
        }

        result = pipeline.validate()
        self.assertFalse(result)

    def test_engagement_analysis_pipeline_validation_invalid_engagement_summary(self):
        """Test pipeline validation with invalid engagement summary."""
        pipeline = EngagementAnalysisPipeline("test_engagement_pipeline")
        pipeline.setup()

        # Set results with invalid engagement summary
        pipeline.final_results = {
            "users": [{"user": "user1"}],
            "partition_dates": ["2024-10-01"],
            "engagement_summary": "invalid",  # Should be DataFrame
        }

        result = pipeline.validate()
        self.assertFalse(result)

    def test_engagement_analysis_pipeline_validation_missing_required_columns(self):
        """Test pipeline validation with missing required columns in engagement summary."""
        pipeline = EngagementAnalysisPipeline("test_engagement_pipeline")
        pipeline.setup()

        # Set results with missing required columns
        pipeline.final_results = {
            "users": [{"user": "user1"}],
            "partition_dates": ["2024-10-01"],
            "engagement_summary": pd.DataFrame(
                {
                    "total_likes": [10]
                    # Missing user column
                }
            ),
        }

        result = pipeline.validate()
        self.assertFalse(result)


class TestPipelineOrchestrator(unittest.TestCase):
    """Test cases for PipelineOrchestrator."""

    def setUp(self):
        """Set up test fixtures."""
        self.orchestrator = PipelineOrchestrator("test_orchestrator")

        # Create mock pipelines
        self.mock_pipeline1 = Mock()
        self.mock_pipeline1.name = "pipeline1"
        self.mock_pipeline1.run.return_value = PipelineResult(
            success=True, data={"result": "data1"}, execution_time=1.0
        )

        self.mock_pipeline2 = Mock()
        self.mock_pipeline2.name = "pipeline2"
        self.mock_pipeline2.run.return_value = PipelineResult(
            success=True, data={"result": "data2"}, execution_time=2.0
        )

    def test_orchestrator_initialization(self):
        """Test orchestrator initialization."""
        self.assertEqual(self.orchestrator.name, "test_orchestrator")
        self.assertEqual(self.orchestrator.pipelines, {})
        self.assertEqual(self.orchestrator.execution_history, [])
        self.assertEqual(self.orchestrator.running_pipelines, {})

    def test_register_pipeline(self):
        """Test pipeline registration."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)

        self.assertIn("pipeline1", self.orchestrator.pipelines)
        self.assertEqual(self.orchestrator.pipelines["pipeline1"], self.mock_pipeline1)

    def test_unregister_pipeline(self):
        """Test pipeline unregistration."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.unregister_pipeline("pipeline1")

        self.assertNotIn("pipeline1", self.orchestrator.pipelines)

    def test_execute_pipeline_success(self):
        """Test successful pipeline execution."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)

        result = self.orchestrator.execute_pipeline("pipeline1")

        self.assertTrue(result.success)
        self.assertEqual(result.pipeline_name, "pipeline1")
        self.assertIsNotNone(result.result)
        self.assertEqual(len(self.orchestrator.execution_history), 1)

    def test_execute_pipeline_not_registered(self):
        """Test executing unregistered pipeline."""
        with self.assertRaises(ValueError):
            self.orchestrator.execute_pipeline("unregistered_pipeline")

    def test_execute_pipeline_with_config_override(self):
        """Test pipeline execution with configuration override."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)

        config_override = {"param1": "value1"}
        result = self.orchestrator.execute_pipeline("pipeline1", config=config_override)

        # Check that config was updated
        self.mock_pipeline1.config.update.assert_called_with(config_override)
        self.assertTrue(result.success)

    def test_execute_pipelines_sequential_success(self):
        """Test successful sequential pipeline execution."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.register_pipeline(self.mock_pipeline2)

        results = self.orchestrator.execute_pipelines_sequential(
            ["pipeline1", "pipeline2"]
        )

        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.success for r in results))
        self.assertEqual(len(self.orchestrator.execution_history), 2)

    def test_execute_pipelines_sequential_with_configs(self):
        """Test sequential execution with configuration overrides."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.register_pipeline(self.mock_pipeline2)

        configs = {"pipeline1": {"param1": "value1"}, "pipeline2": {"param2": "value2"}}

        results = self.orchestrator.execute_pipelines_sequential(
            ["pipeline1", "pipeline2"], configs=configs
        )

        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.success for r in results))

    def test_execute_pipelines_parallel(self):
        """Test parallel pipeline execution (currently falls back to sequential)."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.register_pipeline(self.mock_pipeline2)

        results = self.orchestrator.execute_pipelines_parallel(
            ["pipeline1", "pipeline2"]
        )

        self.assertEqual(len(results), 2)
        self.assertTrue(all(r.success for r in results))

    def test_get_pipeline_status(self):
        """Test getting pipeline status."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)

        status = self.orchestrator.get_pipeline_status("pipeline1")

        self.assertIsNotNone(status)
        self.mock_pipeline1.get_status.assert_called_once()

    def test_get_pipeline_status_not_found(self):
        """Test getting status of unregistered pipeline."""
        status = self.orchestrator.get_pipeline_status("unregistered_pipeline")

        self.assertIsNone(status)

    def test_get_all_pipeline_statuses(self):
        """Test getting all pipeline statuses."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.register_pipeline(self.mock_pipeline2)

        statuses = self.orchestrator.get_all_pipeline_statuses()

        self.assertEqual(len(statuses), 2)
        self.assertIn("pipeline1", statuses)
        self.assertIn("pipeline2", statuses)

    def test_get_execution_summary_empty(self):
        """Test execution summary with no executions."""
        summary = self.orchestrator.get_execution_summary()

        self.assertEqual(summary["total_executions"], 0)
        self.assertEqual(summary["successful_executions"], 0)
        self.assertEqual(summary["failed_executions"], 0)
        self.assertEqual(summary["success_rate"], 0.0)

    def test_get_execution_summary_with_executions(self):
        """Test execution summary with executions."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.execute_pipeline("pipeline1")

        summary = self.orchestrator.get_execution_summary()

        self.assertEqual(summary["total_executions"], 1)
        self.assertEqual(summary["successful_executions"], 1)
        self.assertEqual(summary["failed_executions"], 0)
        self.assertEqual(summary["success_rate"], 1.0)

    def test_cancel_pipeline(self):
        """Test pipeline cancellation."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.running_pipelines["pipeline1"] = self.mock_pipeline1

        result = self.orchestrator.cancel_pipeline("pipeline1")

        self.assertTrue(result)
        self.mock_pipeline1.cancel.assert_called_once()

    def test_cancel_pipeline_not_running(self):
        """Test cancelling non-running pipeline."""
        result = self.orchestrator.cancel_pipeline("pipeline1")

        self.assertFalse(result)

    def test_cancel_all_pipelines(self):
        """Test cancelling all running pipelines."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.register_pipeline(self.mock_pipeline2)
        self.orchestrator.running_pipelines["pipeline1"] = self.mock_pipeline1
        self.orchestrator.running_pipelines["pipeline2"] = self.mock_pipeline2

        self.orchestrator.cancel_all_pipelines()

        self.assertEqual(len(self.orchestrator.running_pipelines), 0)

    def test_clear_execution_history(self):
        """Test clearing execution history."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.execute_pipeline("pipeline1")

        self.assertGreater(len(self.orchestrator.execution_history), 0)

        self.orchestrator.clear_execution_history()

        self.assertEqual(len(self.orchestrator.execution_history), 0)

    def test_export_execution_history_json(self):
        """Test exporting execution history to JSON."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.execute_pipeline("pipeline1")

        filepath = self.orchestrator.export_execution_history(format="json")

        self.assertIsInstance(filepath, str)
        self.assertTrue(filepath.endswith(".json"))

    def test_export_execution_history_csv(self):
        """Test exporting execution history to CSV."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.execute_pipeline("pipeline1")

        filepath = self.orchestrator.export_execution_history(format="csv")

        self.assertIsInstance(filepath, str)
        self.assertTrue(filepath.endswith(".csv"))

    def test_export_execution_history_excel(self):
        """Test exporting execution history to Excel."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.execute_pipeline("pipeline1")

        filepath = self.orchestrator.export_execution_history(format="excel")

        self.assertIsInstance(filepath, str)
        self.assertTrue(filepath.endswith(".xlsx"))

    def test_export_execution_history_invalid_format(self):
        """Test exporting execution history with invalid format."""
        self.orchestrator.register_pipeline(self.mock_pipeline1)
        self.orchestrator.execute_pipeline("pipeline1")

        with self.assertRaises(ValueError):
            self.orchestrator.export_execution_history(format="invalid")

    def test_export_execution_history_no_history(self):
        """Test exporting execution history when none exists."""
        with self.assertRaises(ValueError):
            self.orchestrator.export_execution_history(format="json")


class TestPipelineResult(unittest.TestCase):
    """Test cases for PipelineResult dataclass."""

    def test_pipeline_result_initialization(self):
        """Test PipelineResult initialization with default values."""
        result = PipelineResult(success=True)

        self.assertTrue(result.success)
        self.assertIsNone(result.data)
        self.assertIsNone(result.error)
        self.assertIsNone(result.execution_time)
        self.assertIsNone(result.metadata)
        self.assertIsNone(result.timestamp)

    def test_pipeline_result_initialization_with_values(self):
        """Test PipelineResult initialization with all values."""
        import datetime

        data = {"test": "data"}
        metadata = {"test": True}
        timestamp = datetime.datetime.now()

        result = PipelineResult(
            success=True, data=data, metadata=metadata, timestamp=timestamp
        )

        self.assertTrue(result.success)
        self.assertEqual(result.data, data)
        self.assertEqual(result.metadata, metadata)
        self.assertEqual(result.timestamp, timestamp)


class TestPipelineError(unittest.TestCase):
    """Test cases for PipelineError exception."""

    def test_pipeline_error_initialization(self):
        """Test PipelineError initialization."""
        error = PipelineError("Test error message", "test_pipeline", "test_stage")

        self.assertEqual(error.message, "Test error message")
        self.assertEqual(error.pipeline_name, "test_pipeline")
        self.assertEqual(error.stage, "test_stage")

    def test_pipeline_error_string_representation(self):
        """Test PipelineError string representation."""
        error = PipelineError("Test error message", "test_pipeline", "test_stage")

        error_str = str(error)
        self.assertIn("test_pipeline", error_str)
        self.assertIn("test_stage", error_str)
        self.assertIn("Test error message", error_str)


if __name__ == "__main__":
    # Run all tests
    unittest.main(verbosity=2)
