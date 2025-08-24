"""Engagement analysis pipeline implementation.

This module implements the EngagementAnalysisPipeline class that converts the
existing get_aggregate_metrics.py logic into a standardized pipeline framework.
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
    BaseResearchPipeline,
    PipelineError,
    PipelineResult,
)


class EngagementAnalysisPipeline(BaseResearchPipeline):
    """Pipeline for analyzing user engagement metrics.

    This pipeline implements the functionality from the original
    get_aggregate_metrics.py script, providing a standardized
    interface for engagement analysis operations.
    """

    def __init__(
        self, name: str = "engagement_analysis", config: Optional[Dict[str, Any]] = None
    ):
        """Initialize the engagement analysis pipeline.

        Args:
            name: Name for this pipeline instance
            config: Configuration dictionary for pipeline parameters
        """
        super().__init__(name, config)

        # Load configuration with fallbacks for testing
        try:
            self.config_obj = get_config()
            self.study_config = getattr(self.config_obj, "study", None)
        except Exception as e:
            self.logger.warning(f"Failed to load configuration: {e}")
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

        # Pipeline state
        self.users: Optional[List[Dict[str, Any]]] = None
        self.partition_dates: Optional[List[str]] = None
        self.daily_metrics: Optional[Dict[str, Any]] = None
        self.engagement_summary: Optional[pd.DataFrame] = None
        self.final_results: Optional[Dict[str, Any]] = None

    def setup(self) -> None:
        """Setup pipeline resources and validate configuration."""
        self.logger.info("Setting up engagement analysis pipeline")

        # Validate required configuration
        required_config = ["exclude_partition_dates"]
        missing_config = [key for key in required_config if key not in self.config]

        if missing_config:
            raise PipelineError(
                f"Missing required configuration: {missing_config}", self.name, "setup"
            )

        # Validate study configuration (optional for testing)
        if self.study_config and not hasattr(
            self.study_config, "wave_1_study_start_date_inclusive"
        ):
            self.logger.warning(
                "Study configuration missing wave dates - using defaults"
            )
        elif not self.study_config:
            self.logger.info("No study configuration loaded - using default settings")

        self.logger.info("Engagement analysis pipeline setup completed")

    def execute(self) -> PipelineResult:
        """Execute the engagement analysis pipeline.

        This method implements the main logic from get_aggregate_metrics.py:
        1. Load study users
        2. Generate partition dates
        3. Calculate daily engagement metrics
        4. Generate engagement summaries
        5. Combine results into final output

        Returns:
            PipelineResult with execution results
        """
        try:
            self.logger.info("Executing engagement analysis pipeline")

            # Load study users
            self.logger.info("Loading study users")
            self.users = self.load_study_users()

            if not self.users:
                self.logger.warning("No study users loaded")
                return PipelineResult(
                    success=True, data={}, metadata={"users_processed": 0}
                )

            self.logger.info(f"Loaded {len(self.users)} study users")

            # Generate partition dates
            self.logger.info("Generating partition dates")
            self.partition_dates = self.generate_partition_dates()

            if not self.partition_dates:
                self.logger.warning("No partition dates generated")
                return PipelineResult(
                    success=True,
                    data={},
                    metadata={"users_processed": len(self.users), "partition_dates": 0},
                )

            self.logger.info(f"Generated {len(self.partition_dates)} partition dates")

            # Calculate daily engagement metrics
            if (
                self.config.get("include_likes", True)
                or self.config.get("include_posts", True)
                or self.config.get("include_follows", True)
                or self.config.get("include_reposts", True)
            ):
                self.logger.info("Calculating daily engagement metrics")
                self.daily_metrics = self.calculate_daily_engagement_metrics()
                self.logger.info("Daily engagement metrics calculated")

            # Generate engagement summaries
            if self.config.get("calculate_summaries", True):
                self.logger.info("Generating engagement summaries")
                self.engagement_summary = self.generate_engagement_summaries()
                self.logger.info("Engagement summaries generated")

            # Combine results
            self.logger.info("Combining engagement analysis results")
            self.final_results = self.combine_engagement_results()

            # Update metadata
            self.metadata.update(
                {
                    "users_processed": len(self.users),
                    "partition_dates": len(self.partition_dates),
                    "daily_metrics_calculated": self.daily_metrics is not None,
                    "engagement_summaries_calculated": self.engagement_summary
                    is not None,
                }
            )

            self.logger.info("Engagement analysis pipeline completed successfully")

            return PipelineResult(
                success=True,
                data=self.final_results,
                metadata=self.metadata,
                timestamp=self.start_time,
            )

        except Exception as e:
            raise PipelineError(
                f"Engagement analysis execution failed: {str(e)}", self.name, "execute"
            )

    def cleanup(self) -> None:
        """Clean up pipeline resources."""
        self.logger.info("Cleaning up engagement analysis pipeline")

        # Clear data references
        self.users = None
        self.partition_dates = None
        self.daily_metrics = None
        self.engagement_summary = None
        self.final_results = None

        # Clear metadata
        self.metadata.clear()

    def validate(self) -> bool:
        """Validate pipeline outputs and results."""
        if self.final_results is None:
            self.logger.error("No final results to validate")
            return False

        # Check that we have basic results
        if not self.final_results:
            self.logger.warning("Final results are empty")
            return True  # Empty results might be valid

        # Validate users
        if "users" not in self.final_results:
            self.logger.error("Missing users in final results")
            return False

        # Validate partition dates
        if "partition_dates" not in self.final_results:
            self.logger.error("Missing partition dates in final results")
            return False

        # Validate daily metrics if present
        if (
            "daily_metrics" in self.final_results
            and self.final_results["daily_metrics"]
        ):
            daily_metrics = self.final_results["daily_metrics"]
            if not isinstance(daily_metrics, dict):
                self.logger.error("Daily metrics should be a dictionary")
                return False

        # Validate engagement summary if present
        if (
            "engagement_summary" in self.final_results
            and self.final_results["engagement_summary"] is not None
        ):
            engagement_summary = self.final_results["engagement_summary"]
            if not isinstance(engagement_summary, pd.DataFrame):
                self.logger.error("Engagement summary should be a DataFrame")
                return False

            if not engagement_summary.empty:
                # Check required columns
                required_columns = ["user"]
                missing_columns = [
                    col
                    for col in required_columns
                    if col not in engagement_summary.columns
                ]

                if missing_columns:
                    self.logger.error(
                        f"Missing required columns in engagement summary: {missing_columns}"
                    )
                    return False

        self.logger.info("Engagement analysis validation passed")
        return True

    def load_study_users(self) -> List[Dict[str, Any]]:
        """Load study users for engagement analysis.

        Returns:
            List of user dictionaries

        Raises:
            PipelineError: If data loading fails
        """
        try:
            self.logger.info("Loading study users")

            # Use the available user demographic function and convert to list of dicts
            user_df = load_user_demographic_info()
            users = []

            for _, row in user_df.iterrows():
                user_dict = {
                    "user": row["bluesky_handle"],
                    "user_did": row["bluesky_user_did"],
                    "condition": row["condition"],
                }
                users.append(user_dict)

            self.logger.info(f"Loaded {len(users)} study users")
            return users

        except Exception as e:
            raise PipelineError(
                f"Failed to load study users: {str(e)}", self.name, "load_study_users"
            )

    def generate_partition_dates(self) -> List[str]:
        """Generate partition dates for engagement analysis.

        Returns:
            List of partition date strings in YYYY-MM-DD format
        """
        try:
            # Get study date range from configuration
            start_date = self.study_config.get(
                "wave_1_study_start_date_inclusive", "2024-09-30"
            )
            end_date = self.study_config.get(
                "wave_2_study_end_date_inclusive", "2024-12-01"
            )

            # Generate date range
            date_range = pd.date_range(start=start_date, end=end_date, freq="D")

            # Convert to string format
            all_dates = [date.strftime("%Y-%m-%d") for date in date_range]

            # Filter out excluded dates
            exclude_dates = self.config.get("exclude_partition_dates", [])
            partition_dates = [date for date in all_dates if date not in exclude_dates]

            self.logger.info(
                f"Generated {len(partition_dates)} partition dates from {start_date} to {end_date}"
            )
            return partition_dates

        except Exception as e:
            self.logger.error(f"Error generating partition dates: {e}")
            return []

    def calculate_daily_engagement_metrics(self) -> Dict[str, Any]:
        """Calculate daily engagement metrics for all users.

        Returns:
            Dictionary containing daily engagement metrics
        """
        try:
            if not self.users or not self.partition_dates:
                self.logger.warning(
                    "No users or partition dates available for daily metrics calculation"
                )
                return {}

            daily_metrics = {}

            # Calculate metrics for each record type if enabled
            record_types = []
            if self.config.get("include_likes", True):
                record_types.append("like")
            if self.config.get("include_posts", True):
                record_types.append("post")
            if self.config.get("include_follows", True):
                record_types.append("follow")
            if self.config.get("include_reposts", True):
                record_types.append("repost")

            # Get daily metrics for each record type
            for record_type in record_types:
                try:
                    self.logger.info(f"Calculating daily metrics for {record_type}")
                    metrics = get_num_records_per_user_per_day(record_type)
                    daily_metrics[record_type] = metrics
                except Exception as e:
                    self.logger.error(
                        f"Error calculating daily metrics for {record_type}: {e}"
                    )
                    daily_metrics[record_type] = {}

            # Calculate aggregate metrics if enabled
            if self.config.get("calculate_rates", True):
                try:
                    self.logger.info("Calculating aggregate metrics per user per day")
                    aggregate_metrics = aggregate_metrics_per_user_per_day(
                        self.users, self.partition_dates
                    )
                    daily_metrics["aggregate"] = aggregate_metrics
                except Exception as e:
                    self.logger.error(f"Error calculating aggregate metrics: {e}")
                    daily_metrics["aggregate"] = {}

            self.logger.info(
                f"Daily engagement metrics calculated for {len(record_types)} record types"
            )
            return daily_metrics

        except Exception as e:
            self.logger.error(f"Error calculating daily engagement metrics: {e}")
            return {}

    def generate_engagement_summaries(self) -> Optional[pd.DataFrame]:
        """Generate engagement summaries for all users.

        Returns:
            DataFrame with engagement summaries, or None if calculation fails
        """
        try:
            if not self.users or not self.partition_dates:
                self.logger.warning(
                    "No users or partition dates available for engagement summary calculation"
                )
                return None

            self.logger.info("Generating engagement summaries")
            engagement_summary = get_engagement_summary_per_user(
                self.users, self.partition_dates
            )

            if engagement_summary is not None and not engagement_summary.empty:
                self.logger.info(
                    f"Generated engagement summaries for {len(engagement_summary)} users"
                )
            else:
                self.logger.warning("Generated empty engagement summary")

            return engagement_summary

        except Exception as e:
            self.logger.error(f"Error generating engagement summaries: {e}")
            return None

    def combine_engagement_results(self) -> Dict[str, Any]:
        """Combine all engagement analysis results.

        Returns:
            Dictionary containing all engagement analysis results
        """
        try:
            results = {
                "users": self.users,
                "partition_dates": self.partition_dates,
            }

            # Add daily metrics if available
            if self.daily_metrics:
                results["daily_metrics"] = self.daily_metrics

            # Add engagement summary if available
            if self.engagement_summary is not None:
                results["engagement_summary"] = self.engagement_summary

            # Add configuration information
            results["configuration"] = {
                "include_likes": self.config.get("include_likes", True),
                "include_posts": self.config.get("include_posts", True),
                "include_follows": self.config.get("include_follows", True),
                "include_reposts": self.config.get("include_reposts", True),
                "calculate_rates": self.config.get("calculate_rates", True),
                "calculate_summaries": self.config.get("calculate_summaries", True),
            }

            self.logger.info("Combined engagement analysis results")
            return results

        except Exception as e:
            self.logger.error(f"Error combining engagement results: {e}")
            return {}

    def get_engagement_metrics_for_user(self, user_id: str) -> Dict[str, Any]:
        """Get engagement metrics for a specific user.

        Args:
            user_id: User identifier

        Returns:
            Dictionary containing user's engagement metrics
        """
        if not self.final_results or "daily_metrics" not in self.final_results:
            return {}

        daily_metrics = self.final_results["daily_metrics"]
        user_metrics = {}

        # Extract metrics for the specific user
        for record_type, metrics in daily_metrics.items():
            if user_id in metrics:
                user_metrics[record_type] = metrics[user_id]
            else:
                user_metrics[record_type] = {}

        return user_metrics

    def get_engagement_summary_for_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get engagement summary for a specific user.

        Args:
            user_id: User identifier

        Returns:
            Dictionary containing user's engagement summary, or None if not found
        """
        if not self.final_results or "engagement_summary" not in self.final_results:
            return None

        engagement_summary = self.final_results["engagement_summary"]
        if engagement_summary.empty:
            return None

        user_summary = engagement_summary[engagement_summary["user"] == user_id]
        if user_summary.empty:
            return None

        return user_summary.iloc[0].to_dict()

    def export_engagement_data(self, output_path: str, format: str = "csv") -> None:
        """Export engagement data to file.

        Args:
            output_path: Path to output file
            format: Output format (csv, json, parquet)
        """
        if not self.final_results:
            self.logger.warning("No results to export")
            return

        if format == "csv":
            self.final_results.to_csv(output_path, index=False)
        elif format == "json":
            self.final_results.to_json(output_path, orient="records")
        elif format == "parquet":
            self.final_results.to_parquet(output_path, index=False)
        else:
            raise ValueError(f"Unsupported format: {format}")

        self.logger.info(f"Exported engagement data to {output_path}")

    def set_analysis_period(self, start_date: str, end_date: str) -> None:
        """Set the analysis period for engagement analysis.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        """
        self.start_date = start_date
        self.end_date = end_date
        self.logger.info(f"Set analysis period: {start_date} to {end_date}")
