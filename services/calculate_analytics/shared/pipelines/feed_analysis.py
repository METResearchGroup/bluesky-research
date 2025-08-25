"""Feed analysis implementation.

This module implements the FeedAnalyzer class that provides feed analysis
functionality using simple, direct method execution instead of complex
pipeline orchestration.
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
    BaseAnalyzer,
)


class FeedAnalyzer(BaseAnalyzer):
    """Analyzer for feed content and user averages.

    This class implements the functionality from the original feed_analytics.py
    script, providing a simple interface for feed analysis operations.
    """

    def __init__(
        self, name: str = "feed_analysis", config: Optional[Dict[str, Any]] = None
    ):
        """Initialize the feed analyzer.

        Args:
            name: Name for this analyzer instance
            config: Configuration dictionary for analyzer parameters
        """
        super().__init__(name, config)

        # Load configuration with fallbacks for testing
        try:
            self.config_obj = get_config()
            self.feature_config = getattr(self.config_obj, "features", None)
            self.study_config = getattr(self.config_obj, "study", None)
        except Exception as e:
            self.log_warning(f"Failed to load configuration: {e}")
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

    def analyze_partition_date(self, partition_date: str) -> pd.DataFrame:
        """Analyze feed data for a specific partition date.

        This method implements the main logic from feed_analytics.py:
        1. Load feed data for the partition date
        2. Calculate features for each user
        3. Aggregate results into a single DataFrame

        Args:
            partition_date: Date string in YYYY-MM-DD format

        Returns:
            DataFrame with aggregated feed analysis results

        Raises:
            ValueError: If required configuration is missing
        """
        self.log_execution("analyze_partition_date", partition_date=partition_date)

        # Validate configuration
        required_config = ["exclude_partition_dates", "default_label_threshold"]
        self.validate_config(required_config)

        # Check if partition date should be excluded
        if partition_date in self.config["exclude_partition_dates"]:
            self.log_warning(
                f"Partition date {partition_date} is excluded from analysis"
            )
            return pd.DataFrame()

        try:
            # Load feed data
            posts_data = self._load_feed_data(partition_date)
            if not posts_data:
                self.log_warning(
                    f"No feed data found for partition date {partition_date}"
                )
                return pd.DataFrame()

            # Calculate features for each user
            user_results = []
            for user, posts_df in posts_data.items():
                if self._validate_feed_data(posts_df):
                    features = self._calculate_features(posts_df, user)
                    user_results.append(features)

            # Aggregate results
            if user_results:
                results = self._aggregate_results(user_results)
                self.log_info(
                    f"Successfully analyzed {len(user_results)} users for {partition_date}"
                )
                return results
            else:
                self.log_warning(
                    f"No valid user results for partition date {partition_date}"
                )
                return pd.DataFrame()

        except Exception as e:
            self.log_error(f"Error analyzing partition date {partition_date}: {e}")
            raise

    def _load_feed_data(self, partition_date: str) -> Dict[str, pd.DataFrame]:
        """Load feed data for a specific partition date.

        Args:
            partition_date: Date string in YYYY-MM-DD format

        Returns:
            Dictionary mapping user IDs to their feed posts DataFrames
        """
        self.log_info(f"Loading feed data for partition date: {partition_date}")

        try:
            posts_data = get_hydrated_feed_posts_per_user(
                partition_date=partition_date,
                load_unfiltered_posts=self.config["load_unfiltered_posts"],
            )

            if not posts_data:
                self.log_warning(
                    f"No feed data returned for partition date {partition_date}"
                )
                return {}

            self.log_info(f"Loaded feed data for {len(posts_data)} users")
            return posts_data

        except Exception as e:
            self.log_error(f"Failed to load feed data for {partition_date}: {e}")
            raise

    def _calculate_features(self, posts_df: pd.DataFrame, user: str) -> Dict[str, Any]:
        """Calculate features from feed posts for a specific user.

        Args:
            posts_df: DataFrame containing user's feed posts
            user: User identifier

        Returns:
            Dictionary containing calculated features
        """
        try:
            # Calculate feature averages
            feature_averages = calculate_feature_averages(
                posts_df, self.feature_config, self.config["default_label_threshold"]
            )

            # Calculate feature proportions
            feature_proportions = calculate_feature_proportions(
                posts_df, self.feature_config, self.config["default_label_threshold"]
            )

            # Calculate political averages
            political_averages = calculate_political_averages(
                posts_df, self.feature_config, self.config["default_label_threshold"]
            )

            # Calculate valence averages
            valence_averages = calculate_valence_averages(
                posts_df, self.feature_config, self.config["default_label_threshold"]
            )

            # Combine all features
            user_features = {
                "user": user,
                "partition_date": posts_df["partition_date"].iloc[0]
                if not posts_df.empty
                else None,
                **feature_averages,
                **feature_proportions,
                **political_averages,
                **valence_averages,
            }

            return user_features

        except Exception as e:
            self.log_error(f"Failed to calculate features for user {user}: {e}")
            raise

    def _aggregate_results(self, user_results: List[Dict[str, Any]]) -> pd.DataFrame:
        """Aggregate results from multiple users into a single DataFrame.

        Args:
            user_results: List of user result dictionaries

        Returns:
            DataFrame with aggregated results
        """
        try:
            if not user_results:
                return pd.DataFrame()

            # Convert to DataFrame
            results_df = pd.DataFrame(user_results)

            # Ensure user column is first
            if "user" in results_df.columns:
                cols = ["user"] + [col for col in results_df.columns if col != "user"]
                results_df = results_df[cols]

            self.log_info(f"Aggregated results for {len(user_results)} users")
            return results_df

        except Exception as e:
            self.log_error(f"Failed to aggregate results: {e}")
            raise

    def _validate_feed_data(self, posts_df: pd.DataFrame) -> bool:
        """Validate feed data quality.

        Args:
            posts_df: DataFrame to validate

        Returns:
            True if data is valid, False otherwise
        """
        if posts_df.empty:
            self.log_warning("Empty posts DataFrame")
            return False

        # Check for required columns if feature config is available
        if self.feature_config and hasattr(self.feature_config, "toxicity_features"):
            required_columns = ["partition_date", "text"]  # Basic required columns
            if not self.validate_data(posts_df, required_columns):
                return False

        return True

    def get_analysis_summary(self, partition_date: str) -> Dict[str, Any]:
        """Get a summary of analysis results for a partition date.

        Args:
            partition_date: Date string in YYYY-MM-DD format

        Returns:
            Dictionary with analysis summary information
        """
        try:
            results = self.analyze_partition_date(partition_date)

            summary = {
                "partition_date": partition_date,
                "total_users": len(results) if not results.empty else 0,
                "columns_analyzed": list(results.columns) if not results.empty else [],
                "analysis_timestamp": pd.Timestamp.now().isoformat(),
            }

            return summary

        except Exception as e:
            self.log_error(f"Failed to get analysis summary for {partition_date}: {e}")
            return {
                "partition_date": partition_date,
                "error": str(e),
                "analysis_timestamp": pd.Timestamp.now().isoformat(),
            }
