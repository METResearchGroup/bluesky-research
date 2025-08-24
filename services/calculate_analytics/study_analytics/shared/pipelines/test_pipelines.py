"""Pipeline framework testing suite.

This module provides comprehensive tests for the simple pipeline framework,
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
        self.assertIn("Pipeline validation failed", result.error)

    def test_pipeline_cleanup_failure(self):
        """Test pipeline cleanup failure handling."""
        config = {"fail_cleanup": True}
        pipeline = self.TestPipeline("test_pipeline", config=config)

        result = pipeline.run()

        # Cleanup failure should not affect successful execution
        self.assertTrue(result.success)
        self.assertEqual(pipeline.state, PipelineState.COMPLETED)

    def test_pipeline_successful_execution(self):
        """Test successful pipeline execution."""
        pipeline = self.TestPipeline("test_pipeline")

        result = pipeline.run()

        self.assertTrue(result.success)
        self.assertEqual(pipeline.state, PipelineState.COMPLETED)
        self.assertIsNotNone(pipeline.start_time)
        self.assertIsNotNone(pipeline.end_time)
        self.assertIsNotNone(pipeline.execution_time)
        self.assertEqual(result.data, {"test": "data"})
        self.assertEqual(result.metadata, {"test": True})

    def test_pipeline_state_transitions(self):
        """Test pipeline state transitions during execution."""
        pipeline = self.TestPipeline("test_pipeline")

        # Initial state
        self.assertEqual(pipeline.state, PipelineState.PENDING)

        # Start execution
        pipeline.state = PipelineState.RUNNING
        self.assertEqual(pipeline.state, PipelineState.RUNNING)

        # Complete execution
        pipeline.state = PipelineState.COMPLETED
        self.assertEqual(pipeline.state, PipelineState.COMPLETED)

    def test_pipeline_get_status(self):
        """Test pipeline status retrieval."""
        pipeline = self.TestPipeline("test_pipeline")

        status = pipeline.get_status()

        self.assertEqual(status["name"], "test_pipeline")
        self.assertEqual(status["state"], PipelineState.PENDING.value)
        self.assertIsNone(status["start_time"])
        self.assertIsNone(status["end_time"])
        self.assertIsNone(status["execution_time"])
        self.assertIsNone(status["error"])
        self.assertEqual(status["metadata"], {})

    def test_pipeline_cancel(self):
        """Test pipeline cancellation."""
        pipeline = self.TestPipeline("test_pipeline")

        # Set to running state
        pipeline.state = PipelineState.RUNNING

        # Cancel pipeline
        pipeline.cancel()

        self.assertEqual(pipeline.state, PipelineState.CANCELLED)

    def test_pipeline_cancel_not_running(self):
        """Test pipeline cancellation when not running."""
        pipeline = self.TestPipeline("test_pipeline")

        # Cancel pipeline that's not running
        pipeline.cancel()

        # State should remain unchanged
        self.assertEqual(pipeline.state, PipelineState.PENDING)


class TestBaseFeedAnalysisPipeline(unittest.TestCase):
    """Test cases for BaseFeedAnalysisPipeline abstract base class."""

    def setUp(self):
        """Set up test fixtures."""

        class TestFeedPipeline(BaseFeedAnalysisPipeline):
            def setup(self):
                pass

            def execute(self):
                return PipelineResult(
                    success=True,
                    data={"feed_data": "test"},
                    metadata={"feed_analysis": True},
                )

            def cleanup(self):
                pass

            def validate(self):
                return True

            def load_feed_data(self, partition_date):
                return {"user1": pd.DataFrame({"post": ["test post"]})}

            def calculate_feed_features(self, posts_df, user):
                return {"user": user, "feature": "test"}

            def aggregate_feed_results(self, user_results):
                return pd.DataFrame(user_results)

        self.TestFeedPipeline = TestFeedPipeline

    def test_feed_pipeline_initialization(self):
        """Test feed pipeline initialization."""
        config = {
            "features": {"required_columns": ["post"]},
            "study": {"wave": "wave1"},
        }
        pipeline = self.TestFeedPipeline("test_feed_pipeline", config=config)

        self.assertEqual(pipeline.name, "test_feed_pipeline")
        self.assertEqual(pipeline.feature_config, {"required_columns": ["post"]})
        self.assertEqual(pipeline.study_config, {"wave": "wave1"})

    def test_feed_data_validation_success(self):
        """Test successful feed data validation."""
        config = {"features": {"required_columns": ["post"]}}
        pipeline = self.TestFeedPipeline("test_feed_pipeline", config=config)

        # Valid data
        valid_df = pd.DataFrame({"post": ["test post 1", "test post 2"]})
        result = pipeline.validate_feed_data(valid_df)

        self.assertTrue(result)

    def test_feed_data_validation_empty(self):
        """Test feed data validation with empty DataFrame."""
        config = {"features": {"required_columns": ["post"]}}
        pipeline = self.TestFeedPipeline("test_feed_pipeline", config=config)

        # Empty data
        empty_df = pd.DataFrame()
        result = pipeline.validate_feed_data(empty_df)

        self.assertFalse(result)

    def test_feed_data_validation_missing_columns(self):
        """Test feed data validation with missing required columns."""
        config = {"features": {"required_columns": ["post", "user"]}}
        pipeline = self.TestFeedPipeline("test_feed_pipeline", config=config)

        # Missing required columns
        invalid_df = pd.DataFrame({"post": ["test post"]})
        result = pipeline.validate_feed_data(invalid_df)

        self.assertFalse(result)

    def test_feed_pipeline_execution(self):
        """Test feed pipeline execution."""
        pipeline = self.TestFeedPipeline("test_feed_pipeline")

        result = pipeline.run()

        self.assertTrue(result.success)
        self.assertEqual(result.data, {"feed_data": "test"})
        self.assertEqual(result.metadata, {"feed_analysis": True})


class TestFeedAnalysisPipeline(unittest.TestCase):
    """Test cases for FeedAnalysisPipeline concrete implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.pipeline = FeedAnalysisPipeline("test_feed_analysis")

    @patch(
        "services.calculate_analytics.study_analytics.shared.pipelines.feed_analysis.get_config"
    )
    def test_pipeline_initialization(self, mock_get_config):
        """Test pipeline initialization."""
        # Mock configuration
        mock_config = Mock()
        mock_config.features = Mock()
        mock_config.features.toxicity_features = ["prob_toxic"]
        mock_config.study = Mock()
        mock_get_config.return_value = mock_config

        pipeline = FeedAnalysisPipeline("test_pipeline")

        self.assertEqual(pipeline.name, "test_pipeline")
        self.assertIn("exclude_partition_dates", pipeline.config)
        self.assertIn("default_label_threshold", pipeline.config)

    def test_set_partition_date(self):
        """Test setting partition date."""
        partition_date = "2024-10-15"
        self.pipeline.set_partition_date(partition_date)

        self.assertEqual(self.pipeline.current_partition_date, partition_date)

    def test_setup_validation(self):
        """Test pipeline setup validation."""
        # Test with valid configuration
        try:
            self.pipeline.setup()
            # Should not raise exception
        except Exception as e:
            self.fail(f"Setup should not fail with valid config: {e}")

    def test_setup_missing_config(self):
        """Test pipeline setup with missing configuration."""
        # Remove required config
        self.pipeline.config = {}

        with self.assertRaises(PipelineError):
            self.pipeline.setup()


class TestWeeklyThresholdsPipeline(unittest.TestCase):
    """Test cases for WeeklyThresholdsPipeline concrete implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.pipeline = WeeklyThresholdsPipeline("test_weekly_thresholds")

    def test_pipeline_initialization(self):
        """Test pipeline initialization."""
        self.assertEqual(self.pipeline.name, "test_weekly_thresholds")
        self.assertIsInstance(self.pipeline.config, dict)

    def test_set_time_period(self):
        """Test setting time period."""
        start_date = "2024-10-01"
        end_date = "2024-10-31"

        self.pipeline.set_time_period(start_date, end_date)

        self.assertEqual(self.pipeline.start_date, start_date)
        self.assertEqual(self.pipeline.end_date, end_date)


class TestEngagementAnalysisPipeline(unittest.TestCase):
    """Test cases for EngagementAnalysisPipeline concrete implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.pipeline = EngagementAnalysisPipeline("test_engagement_analysis")

    def test_pipeline_initialization(self):
        """Test pipeline initialization."""
        self.assertEqual(self.pipeline.name, "test_engagement_analysis")
        self.assertIsInstance(self.pipeline.config, dict)

    def test_set_analysis_period(self):
        """Test setting analysis period."""
        start_date = "2024-10-01"
        end_date = "2024-10-31"

        self.pipeline.set_analysis_period(start_date, end_date)

        self.assertEqual(self.pipeline.start_date, start_date)
        self.assertEqual(self.pipeline.end_date, end_date)


class TestPipelineResult(unittest.TestCase):
    """Test cases for PipelineResult dataclass."""

    def test_pipeline_result_creation(self):
        """Test PipelineResult creation."""
        result = PipelineResult(
            success=True,
            data={"test": "data"},
            execution_time=1.5,
            metadata={"test": True},
        )

        self.assertTrue(result.success)
        self.assertEqual(result.data, {"test": "data"})
        self.assertEqual(result.execution_time, 1.5)
        self.assertEqual(result.metadata, {"test": True})
        self.assertIsNone(result.error)
        self.assertIsNone(result.timestamp)

    def test_pipeline_result_with_error(self):
        """Test PipelineResult creation with error."""
        result = PipelineResult(
            success=False, error="Test error message", execution_time=0.5
        )

        self.assertFalse(result.success)
        self.assertEqual(result.error, "Test error message")
        self.assertEqual(result.execution_time, 0.5)
        self.assertIsNone(result.data)
        self.assertIsNone(result.metadata)
        self.assertIsNone(result.timestamp)


class TestPipelineError(unittest.TestCase):
    """Test cases for PipelineError exception."""

    def test_pipeline_error_creation(self):
        """Test PipelineError creation."""
        error = PipelineError("Test error", "test_pipeline", "setup")

        self.assertEqual(error.message, "Test error")
        self.assertEqual(error.pipeline_name, "test_pipeline")
        self.assertEqual(error.stage, "setup")

    def test_pipeline_error_string_representation(self):
        """Test PipelineError string representation."""
        error = PipelineError("Test error", "test_pipeline", "setup")
        error_str = str(error)

        self.assertIn("test_pipeline", error_str)
        self.assertIn("setup", error_str)
        self.assertIn("Test error", error_str)


class TestPipelineState(unittest.TestCase):
    """Test cases for PipelineState enum."""

    def test_pipeline_state_values(self):
        """Test pipeline state enum values."""
        self.assertEqual(PipelineState.PENDING.value, "pending")
        self.assertEqual(PipelineState.RUNNING.value, "running")
        self.assertEqual(PipelineState.COMPLETED.value, "completed")
        self.assertEqual(PipelineState.FAILED.value, "failed")
        self.assertEqual(PipelineState.CANCELLED.value, "cancelled")

    def test_pipeline_state_enumeration(self):
        """Test pipeline state enumeration."""
        states = list(PipelineState)

        self.assertIn(PipelineState.PENDING, states)
        self.assertIn(PipelineState.RUNNING, states)
        self.assertIn(PipelineState.COMPLETED, states)
        self.assertIn(PipelineState.FAILED, states)
        self.assertIn(PipelineState.CANCELLED, states)


if __name__ == "__main__":
    unittest.main()
