"""Abstract base classes for analytics pipeline framework.

This module defines the core interfaces that all analytics pipelines must implement,
ensuring consistency and standardization across the analytics system.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd

from lib.log.logger import get_logger


class PipelineState(Enum):
    """Pipeline execution states."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class PipelineResult:
    """Result of pipeline execution."""

    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None


class PipelineError(Exception):
    """Base exception for pipeline errors."""

    def __init__(self, message: str, pipeline_name: str, stage: str):
        self.message = message
        self.pipeline_name = pipeline_name
        self.stage = stage
        super().__init__(f"[{pipeline_name}] {stage}: {message}")


class BaseResearchPipeline(ABC):
    """Abstract base class for all research pipelines.

    This class defines the core interface that all analytics pipelines must implement.
    It provides lifecycle management, state tracking, and error handling.
    """

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """Initialize the pipeline.

        Args:
            name: Unique name for this pipeline instance
            config: Configuration dictionary for pipeline parameters
        """
        self.name = name
        self.config = config or {}
        self.state = PipelineState.PENDING
        self.logger = get_logger(f"pipeline.{name}")
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.execution_time: Optional[float] = None
        self.error: Optional[str] = None
        self.metadata: Dict[str, Any] = {}

    @abstractmethod
    def setup(self) -> None:
        """Setup pipeline resources and validate configuration.

        This method should:
        - Validate configuration parameters
        - Initialize required resources
        - Perform any pre-execution setup

        Raises:
            PipelineError: If setup fails
        """
        pass

    @abstractmethod
    def execute(self) -> PipelineResult:
        """Execute the main pipeline logic.

        This method should:
        - Load required data
        - Perform processing operations
        - Generate outputs
        - Handle errors gracefully

        Returns:
            PipelineResult with execution results

        Raises:
            PipelineError: If execution fails
        """
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """Clean up pipeline resources.

        This method should:
        - Release any acquired resources
        - Clean up temporary files
        - Reset pipeline state
        """
        pass

    @abstractmethod
    def validate(self) -> bool:
        """Validate pipeline outputs and results.

        This method should:
        - Verify output data quality
        - Check for expected patterns
        - Validate against business rules

        Returns:
            True if validation passes, False otherwise
        """
        pass

    def run(self) -> PipelineResult:
        """Run the complete pipeline lifecycle.

        This method orchestrates the complete pipeline execution:
        1. Setup
        2. Execute
        3. Validate
        4. Cleanup

        Returns:
            PipelineResult with final execution results
        """
        try:
            self.logger.info(f"Starting pipeline: {self.name}")
            self.state = PipelineState.RUNNING
            self.start_time = datetime.now()

            # Setup phase
            self.logger.info("Setting up pipeline")
            self.setup()

            # Execute phase
            self.logger.info("Executing pipeline")
            result = self.execute()

            # Validation phase
            self.logger.info("Validating pipeline results")
            if not self.validate():
                raise PipelineError(
                    "Pipeline validation failed", self.name, "validation"
                )

            # Success
            self.state = PipelineState.COMPLETED
            self.end_time = datetime.now()
            if self.start_time and self.end_time:
                self.execution_time = (self.end_time - self.start_time).total_seconds()

            self.logger.info(
                f"Pipeline completed successfully in {self.execution_time:.2f}s"
            )
            return result

        except Exception as e:
            self.state = PipelineState.FAILED
            self.end_time = datetime.now()
            if self.start_time and self.end_time:
                self.execution_time = (self.end_time - self.start_time).total_seconds()

            error_msg = str(e)
            self.error = error_msg
            self.logger.error(f"Pipeline failed: {error_msg}")

            # Cleanup on failure
            try:
                self.cleanup()
            except Exception as cleanup_error:
                self.logger.error(f"Cleanup failed: {cleanup_error}")

            return PipelineResult(
                success=False,
                error=error_msg,
                execution_time=self.execution_time,
                timestamp=self.end_time,
            )

    def get_status(self) -> Dict[str, Any]:
        """Get current pipeline status.

        Returns:
            Dictionary with current pipeline state and metadata
        """
        return {
            "name": self.name,
            "state": self.state.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "execution_time": self.execution_time,
            "error": self.error,
            "metadata": self.metadata,
        }

    def cancel(self) -> None:
        """Cancel pipeline execution.

        This method should be implemented by subclasses to handle
        graceful cancellation of running operations.
        """
        if self.state == PipelineState.RUNNING:
            self.state = PipelineState.CANCELLED
            self.logger.info("Pipeline cancellation requested")
            # Subclasses should implement cancellation logic


class BaseFeedAnalysisPipeline(BaseResearchPipeline):
    """Abstract base class for feed analysis pipelines.

    This class extends BaseResearchPipeline with feed-specific functionality
    and provides common methods for feed analysis operations.
    """

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """Initialize the feed analysis pipeline.

        Args:
            name: Unique name for this pipeline instance
            config: Configuration dictionary for pipeline parameters
        """
        super().__init__(name, config)
        self.feature_config = self.config.get("features", {})
        self.study_config = self.config.get("study", {})

    @abstractmethod
    def load_feed_data(self, partition_date: str) -> Dict[str, pd.DataFrame]:
        """Load feed data for a specific partition date.

        Args:
            partition_date: Date string in YYYY-MM-DD format

        Returns:
            Dictionary mapping user IDs to their feed posts DataFrames

        Raises:
            PipelineError: If data loading fails
        """
        pass

    @abstractmethod
    def calculate_feed_features(
        self, posts_df: pd.DataFrame, user: str
    ) -> Dict[str, Any]:
        """Calculate features from feed posts for a specific user.

        Args:
            posts_df: DataFrame containing user's feed posts
            user: User identifier

        Returns:
            Dictionary containing calculated features
        """
        pass

    @abstractmethod
    def aggregate_feed_results(
        self, user_results: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """Aggregate results from multiple users into a single DataFrame.

        Args:
            user_results: List of user result dictionaries

        Returns:
            DataFrame with aggregated results
        """
        pass

    def validate_feed_data(self, posts_df: pd.DataFrame) -> bool:
        """Validate feed data quality.

        Args:
            posts_df: DataFrame to validate

        Returns:
            True if data is valid, False otherwise
        """
        if posts_df.empty:
            self.logger.warning("Empty posts DataFrame")
            return False

        required_columns = self.feature_config.get("required_columns", [])
        missing_columns = [
            col for col in required_columns if col not in posts_df.columns
        ]

        if missing_columns:
            self.logger.warning(f"Missing required columns: {missing_columns}")
            return False

        return True
