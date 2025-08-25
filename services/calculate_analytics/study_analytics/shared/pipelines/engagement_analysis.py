"""Engagement analysis implementation.

This module implements the EngagementAnalyzer class that provides engagement
analysis functionality using simple, direct method execution instead of complex
pipeline orchestration.
"""

from typing import Any, Dict, List, Optional

import pandas as pd

from services.calculate_analytics.study_analytics.shared.config import get_config
from services.calculate_analytics.study_analytics.shared.data_loading.users import (
    load_user_demographic_info,
)
from services.calculate_analytics.study_analytics.shared.processing.engagement import (
    get_num_records_per_user_per_day,
    aggregate_metrics_per_user_per_day,
    get_engagement_summary_per_user,
)
from services.calculate_analytics.study_analytics.shared.pipelines.base import (
    BaseAnalyzer,
)


class EngagementAnalyzer(BaseAnalyzer):
    """Analyzer for user engagement metrics.

    This class implements the functionality from the original
    get_aggregate_metrics.py script, providing a simple interface
    for engagement analysis operations.
    """

    def __init__(
        self, name: str = "engagement_analysis", config: Optional[Dict[str, Any]] = None
    ):
        """Initialize the engagement analyzer.

        Args:
            name: Name for this analyzer instance
            config: Configuration dictionary for analyzer parameters
        """
        super().__init__(name, config)

        # Load configuration with fallbacks for testing
        try:
            self.config_obj = get_config()
            self.study_config = getattr(self.config_obj, "study", None)
        except Exception as e:
            self.log_warning(f"Failed to load configuration: {e}")
            self.config_obj = None
            self.study_config = None

        # Set default configuration
        self.default_config = {
            "exclude_partition_dates": ["2024-10-08"],
            "include_likes": True,
            "include_posts": True,
            "include_follows": True,
            "include_reposts": True,
            "calculate_rates": True,
            "calculate_summaries": True,
        }
        self.config.update(self.default_config)

    def analyze_period(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Analyze engagement metrics for a specific time period.

        This method implements the main logic from get_aggregate_metrics.py:
        1. Load user demographic information
        2. Calculate daily metrics for each user
        3. Aggregate metrics and generate summaries

        Args:
            start_date: Start date string in YYYY-MM-DD format
            end_date: End date string in YYYY-MM-DD format

        Returns:
            Dictionary containing engagement analysis results

        Raises:
            ValueError: If required configuration is missing
        """
        self.log_execution("analyze_period", start_date=start_date, end_date=end_date)

        # Validate configuration
        required_config = ["exclude_partition_dates"]
        self.validate_config(required_config)

        try:
            # Load user demographics
            users = self._load_user_demographics()
            if not users:
                self.log_warning("No user demographics loaded")
                return {}

            # Generate partition dates
            partition_dates = self._generate_partition_dates(start_date, end_date)
            if not partition_dates:
                self.log_warning(
                    f"No valid partition dates between {start_date} and {end_date}"
                )
                return {}

            # Calculate daily metrics
            daily_metrics = self._calculate_daily_metrics(users, partition_dates)
            if not daily_metrics:
                self.log_warning("No daily metrics calculated")
                return {}

            # Generate engagement summary
            engagement_summary = self._generate_engagement_summary(daily_metrics)

            # Compile final results
            results = {
                "daily_metrics": daily_metrics,
                "engagement_summary": engagement_summary,
                "analysis_period": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "total_days": len(partition_dates),
                },
                "users_analyzed": len(users),
            }

            self.log_info(
                f"Successfully analyzed engagement for {len(users)} users over {len(partition_dates)} days"
            )
            return results

        except Exception as e:
            self.log_error(
                f"Error analyzing engagement for period {start_date} to {end_date}: {e}"
            )
            raise

    def _load_user_demographics(self) -> List[Dict[str, Any]]:
        """Load user demographic information.

        Returns:
            List of user demographic dictionaries
        """
        self.log_info("Loading user demographic information")

        try:
            users = load_user_demographic_info()

            if not users:
                self.log_warning("No user demographics returned")
                return []

            self.log_info(f"Loaded demographics for {len(users)} users")
            return users

        except Exception as e:
            self.log_error(f"Failed to load user demographics: {e}")
            raise

    def _generate_partition_dates(self, start_date: str, end_date: str) -> List[str]:
        """Generate list of partition dates between start and end dates.

        Args:
            start_date: Start date string in YYYY-MM-DD format
            end_date: End date string in YYYY-MM-DD format

        Returns:
            List of partition date strings
        """
        try:
            # Generate date range
            date_range = pd.date_range(start=start_date, end=end_date, freq="D")

            # Filter out excluded dates
            exclude_dates = self.config["exclude_partition_dates"]
            partition_dates = [
                date.strftime("%Y-%m-%d")
                for date in date_range
                if date.strftime("%Y-%m-%d") not in exclude_dates
            ]

            self.log_info(f"Generated {len(partition_dates)} partition dates")
            return partition_dates

        except Exception as e:
            self.log_error(f"Failed to generate partition dates: {e}")
            raise

    def _calculate_daily_metrics(
        self, users: List[Dict[str, Any]], partition_dates: List[str]
    ) -> Dict[str, Any]:
        """Calculate daily metrics for each user.

        Args:
            users: List of user demographic dictionaries
            partition_dates: List of partition date strings

        Returns:
            Dictionary containing daily metrics for each user
        """
        try:
            daily_metrics = {}

            for user in users:
                user_id = user.get("user_id")
                if not user_id:
                    continue

                try:
                    # Get number of records per user per day
                    user_daily_counts = get_num_records_per_user_per_day(
                        user_id, partition_dates, self.config
                    )

                    if user_daily_counts:
                        daily_metrics[user_id] = user_daily_counts

                except Exception as e:
                    self.log_warning(
                        f"Failed to calculate daily metrics for user {user_id}: {e}"
                    )
                    continue

            self.log_info(f"Calculated daily metrics for {len(daily_metrics)} users")
            return daily_metrics

        except Exception as e:
            self.log_error(f"Failed to calculate daily metrics: {e}")
            raise

    def _generate_engagement_summary(
        self, daily_metrics: Dict[str, Any]
    ) -> pd.DataFrame:
        """Generate engagement summary from daily metrics.

        Args:
            daily_metrics: Dictionary containing daily metrics for each user

        Returns:
            DataFrame with engagement summary per user
        """
        try:
            if not daily_metrics:
                return pd.DataFrame()

            # Aggregate metrics per user per day
            aggregated_metrics = aggregate_metrics_per_user_per_day(daily_metrics)

            # Generate engagement summary
            engagement_summary = get_engagement_summary_per_user(aggregated_metrics)

            self.log_info(
                f"Generated engagement summary for {len(engagement_summary)} users"
            )
            return engagement_summary

        except Exception as e:
            self.log_error(f"Failed to generate engagement summary: {e}")
            raise

    def get_engagement_summary(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Get engagement summary for a specific time period.

        Args:
            start_date: Start date string in YYYY-MM-DD format
            end_date: End date string in YYYY-MM-DD format

        Returns:
            DataFrame with engagement summary
        """
        try:
            results = self.analyze_period(start_date, end_date)
            return results.get("engagement_summary", pd.DataFrame())

        except Exception as e:
            self.log_error(f"Failed to get engagement summary: {e}")
            return pd.DataFrame()

    def get_daily_metrics(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Get daily metrics for a specific time period.

        Args:
            start_date: Start date string in YYYY-MM-DD format
            end_date: End date string in YYYY-MM-DD format

        Returns:
            Dictionary containing daily metrics
        """
        try:
            results = self.analyze_period(start_date, end_date)
            return results.get("daily_metrics", {})

        except Exception as e:
            self.log_error(f"Failed to get daily metrics: {e}")
            return {}
