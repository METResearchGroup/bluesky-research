"""Feed analysis pipeline implementation.

This module implements the FeedAnalysisPipeline class that converts the
existing feed_analytics.py logic into a standardized pipeline framework.
"""

from typing import Any, Dict, List, Optional

import pandas as pd

from services.calculate_analytics.study_analytics.shared.config import get_config
from services.calculate_analytics.study_analytics.load_data.load_data import (
    get_hydrated_feed_posts_per_user,
)
from services.calculate_analytics.study_analytics.shared.processing.features import (
    calculate_feature_averages,
    calculate_feature_proportions,
    calculate_political_averages,
    calculate_valence_averages,
)
from services.calculate_analytics.study_analytics.shared.pipelines.base import (
    BaseFeedAnalysisPipeline,
    PipelineError,
    PipelineResult,
)


class FeedAnalysisPipeline(BaseFeedAnalysisPipeline):
    """Pipeline for analyzing feed content and calculating user averages.

    This pipeline implements the functionality from the original feed_analytics.py
    script, providing a standardized interface for feed analysis operations.
    """

    def __init__(
        self, name: str = "feed_analysis", config: Optional[Dict[str, Any]] = None
    ):
        """Initialize the feed analysis pipeline.

        Args:
            name: Name for this pipeline instance
            config: Configuration dictionary for pipeline parameters
        """
        super().__init__(name, config)

        # Load configuration with fallbacks for testing
        try:
            self.config_obj = get_config()
            self.feature_config = getattr(self.config_obj, "features", None)
            self.study_config = getattr(self.config_obj, "study", None)
        except Exception as e:
            self.logger.warning(f"Failed to load configuration: {e}")
            self.config_obj = None
            self.feature_config = None
            self.study_config = None

        # Set default configuration
        self.default_config = {
            "exclude_partition_dates": ["2024-10-08"],
            "default_label_threshold": 0.5,
            "load_unfiltered_posts": True,
        }
        self.config.update(self.default_config)

        # Pipeline state
        self.current_partition_date: Optional[str] = None
        self.user_results: List[Dict[str, Any]] = []
        self.final_results: Optional[pd.DataFrame] = None

    def setup(self) -> None:
        """Setup pipeline resources and validate configuration."""
        self.logger.info("Setting up feed analysis pipeline")

        # Validate required configuration
        required_config = ["exclude_partition_dates", "default_label_threshold"]
        missing_config = [key for key in required_config if key not in self.config]

        if missing_config:
            raise PipelineError(
                f"Missing required configuration: {missing_config}", self.name, "setup"
            )

        # Validate feature configuration (optional for testing)
        if self.feature_config and not hasattr(
            self.feature_config, "toxicity_features"
        ):
            self.logger.warning(
                "Feature configuration missing toxicity_features - using defaults"
            )
        elif not self.feature_config:
            self.logger.info("No feature configuration loaded - using default settings")

        self.logger.info("Feed analysis pipeline setup completed")

    def execute(self) -> PipelineResult:
        """Execute the feed analysis pipeline.

        This method implements the main logic from feed_analytics.py:
        1. Load feed data for the partition date
        2. Calculate features for each user
        3. Aggregate results into a DataFrame

        Returns:
            PipelineResult with execution results
        """
        try:
            if not self.current_partition_date:
                raise PipelineError(
                    "No partition date set for execution", self.name, "execute"
                )

            self.logger.info(
                f"Executing feed analysis for partition date: {self.current_partition_date}"
            )

            # Load feed data
            self.logger.info("Loading feed data")
            user_posts_map = self.load_feed_data(self.current_partition_date)

            if not user_posts_map:
                self.logger.warning("No feed data loaded")
                return PipelineResult(
                    success=True,
                    data=pd.DataFrame(),
                    metadata={
                        "partition_date": self.current_partition_date,
                        "users_processed": 0,
                    },
                )

            # Process each user's feed
            self.logger.info(f"Processing {len(user_posts_map)} users")
            self.user_results = []

            for user, posts_df in user_posts_map.items():
                try:
                    # Validate feed data
                    if not self.validate_feed_data(posts_df):
                        self.logger.warning(f"Skipping user {user} due to invalid data")
                        continue

                    # Calculate features
                    user_result = self.calculate_feed_features(posts_df, user)
                    self.user_results.append(user_result)

                except Exception as e:
                    self.logger.error(f"Error processing user {user}: {e}")
                    # Continue with other users
                    continue

            # Aggregate results
            self.logger.info("Aggregating results")
            self.final_results = self.aggregate_feed_results(self.user_results)

            # Update metadata
            self.metadata.update(
                {
                    "partition_date": self.current_partition_date,
                    "users_processed": len(self.user_results),
                    "total_users_available": len(user_posts_map),
                    "success_rate": len(self.user_results) / len(user_posts_map)
                    if user_posts_map
                    else 0.0,
                }
            )

            self.logger.info(
                f"Feed analysis completed successfully. Processed {len(self.user_results)} users"
            )

            return PipelineResult(
                success=True,
                data=self.final_results,
                metadata=self.metadata,
                timestamp=self.start_time,
            )

        except Exception as e:
            raise PipelineError(
                f"Feed analysis execution failed: {str(e)}", self.name, "execute"
            )

    def cleanup(self) -> None:
        """Clean up pipeline resources."""
        self.logger.info("Cleaning up feed analysis pipeline")

        # Clear data references
        self.current_partition_date = None
        self.user_results = []
        self.final_results = None

        # Clear metadata
        self.metadata.clear()

    def validate(self) -> bool:
        """Validate pipeline outputs and results."""
        if self.final_results is None:
            self.logger.error("No final results to validate")
            return False

        if self.final_results.empty:
            self.logger.warning("Final results are empty")
            return True  # Empty results might be valid

        # Check required columns
        required_columns = ["user", "user_did"]
        missing_columns = [
            col for col in required_columns if col not in self.final_results.columns
        ]

        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False

        # Check data types
        if not pd.api.types.is_string_dtype(self.final_results["user"]):
            self.logger.error("User column is not string type")
            return False

        # Check for duplicate users
        if self.final_results["user"].duplicated().any():
            self.logger.warning("Duplicate users found in results")
            return False

        self.logger.info("Feed analysis validation passed")
        return True

    def load_feed_data(self, partition_date: str) -> Dict[str, pd.DataFrame]:
        """Load feed data for a specific partition date.

        Args:
            partition_date: Date string in YYYY-MM-DD format

        Returns:
            Dictionary mapping user IDs to their feed posts DataFrames

        Raises:
            PipelineError: If data loading fails
        """
        try:
            load_unfiltered_posts = self.config.get("load_unfiltered_posts", True)

            self.logger.info(f"Loading feed data for partition date: {partition_date}")
            user_posts_map = get_hydrated_feed_posts_per_user(
                partition_date=partition_date,
                load_unfiltered_posts=load_unfiltered_posts,
            )

            self.logger.info(f"Loaded feed data for {len(user_posts_map)} users")
            return user_posts_map

        except Exception as e:
            raise PipelineError(
                f"Failed to load feed data: {str(e)}", self.name, "load_feed_data"
            )

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
        try:
            # Calculate basic feature averages
            feature_averages = calculate_feature_averages(posts_df, user)

            # Calculate feature proportions
            threshold = self.config.get("default_label_threshold", 0.5)
            feature_proportions = calculate_feature_proportions(
                posts_df, user, threshold
            )

            # Calculate political averages
            political_averages = calculate_political_averages(posts_df)

            # Calculate valence averages
            valence_averages = calculate_valence_averages(posts_df)

            # Combine all results
            user_result = {
                "user": user,
                "user_did": user,  # Assuming user_did is same as user for now
                **feature_averages,
                **feature_proportions,
                **political_averages,
                **valence_averages,
            }

            return user_result

        except Exception as e:
            self.logger.error(f"Error calculating features for user {user}: {e}")
            # Return minimal result with error indication
            return {"user": user, "user_did": user, "error": str(e)}

    def aggregate_feed_results(
        self, user_results: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """Aggregate results from multiple users into a single DataFrame.

        Args:
            user_results: List of user result dictionaries

        Returns:
            DataFrame with aggregated results
        """
        if not user_results:
            return pd.DataFrame()

        # Convert to DataFrame
        results_df = pd.DataFrame(user_results)

        # Handle any error results
        error_rows = results_df[results_df["error"].notna()]
        if not error_rows.empty:
            self.logger.warning(f"Found {len(error_rows)} users with errors")
            # Remove error rows for now
            results_df = results_df[results_df["error"].isna()].drop(columns=["error"])

        # Ensure proper column ordering
        if not results_df.empty:
            # Put user columns first
            user_columns = ["user", "user_did"]
            other_columns = [
                col for col in results_df.columns if col not in user_columns
            ]
            results_df = results_df[user_columns + other_columns]

        return results_df

    def set_partition_date(self, partition_date: str) -> None:
        """Set the partition date for analysis.

        Args:
            partition_date: Date string in YYYY-MM-DD format
        """
        self.current_partition_date = partition_date
        self.logger.info(f"Set partition date to: {partition_date}")

    def get_partition_dates(self) -> List[str]:
        """Get available partition dates for analysis.

        Returns:
            List of available partition dates
        """
        # This would typically come from configuration or data discovery
        # For now, return a default range
        start_date = self.study_config.get(
            "wave_1_study_start_date_inclusive", "2024-09-30"
        )
        end_date = self.study_config.get(
            "wave_2_study_end_date_inclusive", "2024-12-01"
        )

        # Generate date range (simplified)
        import pandas as pd

        date_range = pd.date_range(start=start_date, end=end_date, freq="D")

        # Filter out excluded dates
        exclude_dates = self.config.get("exclude_partition_dates", [])
        available_dates = [
            date.strftime("%Y-%m-%d")
            for date in date_range
            if date.strftime("%Y-%m-%d") not in exclude_dates
        ]

        return available_dates
