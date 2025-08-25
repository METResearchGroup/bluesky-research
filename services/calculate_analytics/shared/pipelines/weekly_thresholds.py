"""Weekly thresholds analysis implementation.

This module implements the WeeklyThresholdsAnalyzer class that provides weekly
thresholds calculation functionality using simple, direct method execution
instead of complex pipeline orchestration.
"""

from typing import Any, Dict, Optional

import pandas as pd

from lib.log.logger import get_logger
from services.calculate_analytics.study_analytics.shared.config import get_config
from services.calculate_analytics.study_analytics.shared.data_loading.users import (
    load_user_demographic_info,
)
from services.calculate_analytics.study_analytics.shared.processing.thresholds import (
    get_week_thresholds_per_user_static,
    get_week_thresholds_per_user_dynamic,
)
from services.calculate_analytics.study_analytics.shared.pipelines.base import (
    BaseAnalyzer,
)

logger = get_logger(__name__)


class WeeklyThresholdsAnalyzer(BaseAnalyzer):
    """Analyzer for calculating weekly thresholds for study users.

    This class implements the functionality from the original
    calculate_weekly_thresholds_per_user.py script, providing a simple interface
    for weekly threshold calculations.
    """

    def __init__(
        self, name: str = "weekly_thresholds", config: Optional[Dict[str, Any]] = None
    ):
        """Initialize the weekly thresholds analyzer.

        Args:
            name: Name for this analyzer instance
            config: Configuration dictionary for analyzer parameters
        """
        super().__init__(name, config)

        # Load configuration with fallbacks for testing
        try:
            self.config_obj = get_config()
            self.study_config = getattr(self.config_obj, "study", None)
            self.week_config = getattr(self.config_obj, "weeks", None)
        except Exception as e:
            self.log_warning(f"Failed to load configuration: {e}")
            self.config_obj = None
            self.study_config = None
            self.week_config = None

        # Set default configuration
        self.default_config = {
            "exclude_partition_dates": ["2024-10-08"],
            "calculate_static": True,
            "calculate_dynamic": True,
            "include_wave_info": True,
        }
        self.config.update(self.default_config)

    def calculate_thresholds(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Calculate weekly thresholds for a specific time period.

        This method implements the main logic from calculate_weekly_thresholds_per_user.py:
        1. Load user demographic information
        2. Calculate static thresholds for each user
        3. Calculate dynamic thresholds for each user
        4. Combine results into final output

        Args:
            start_date: Start date string in YYYY-MM-DD format
            end_date: End date string in YYYY-MM-DD format

        Returns:
            Dictionary containing weekly threshold results

        Raises:
            ValueError: If required configuration is missing
        """
        self.log_execution(
            "calculate_thresholds", start_date=start_date, end_date=end_date
        )

        # Validate configuration
        required_config = [
            "exclude_partition_dates",
            "calculate_static",
            "calculate_dynamic",
        ]
        self.validate_config(required_config)

        try:
            # Load user demographics
            user_demographics = self._load_user_demographics()
            if user_demographics.empty:
                self.log_warning("No user demographics loaded")
                return {}

            # Calculate static thresholds if enabled
            static_thresholds = None
            if self.config["calculate_static"]:
                static_thresholds = self._calculate_static_thresholds(user_demographics)

            # Calculate dynamic thresholds if enabled
            dynamic_thresholds = None
            if self.config["calculate_dynamic"]:
                dynamic_thresholds = self._calculate_dynamic_thresholds(
                    user_demographics
                )

            # Combine results
            results = self._combine_threshold_results(
                static_thresholds, dynamic_thresholds, start_date, end_date
            )

            self.log_info(
                f"Successfully calculated thresholds for {len(user_demographics)} users"
            )
            return results

        except Exception as e:
            self.log_error(
                f"Error calculating thresholds for period {start_date} to {end_date}: {e}"
            )
            raise

    def _load_user_demographics(self) -> pd.DataFrame:
        """Load user demographic information.

        Returns:
            DataFrame with user demographic information
        """
        self.log_info("Loading user demographic information")

        try:
            user_demographics = load_user_demographic_info()

            if user_demographics.empty:
                self.log_warning("No user demographics returned")
                return pd.DataFrame()

            self.log_info(f"Loaded demographics for {len(user_demographics)} users")
            return user_demographics

        except Exception as e:
            self.log_error(f"Failed to load user demographics: {e}")
            raise

    def _calculate_static_thresholds(
        self, user_demographics: pd.DataFrame
    ) -> pd.DataFrame:
        """Calculate static thresholds for all users.

        Args:
            user_demographics: DataFrame with user demographic information

        Returns:
            DataFrame with static thresholds per user
        """
        try:
            self.log_info("Calculating static thresholds")

            static_thresholds = get_week_thresholds_per_user_static(user_demographics)

            if static_thresholds is not None and not static_thresholds.empty:
                self.log_info(
                    f"Calculated static thresholds for {len(static_thresholds)} users"
                )
                return static_thresholds
            else:
                self.log_warning("No static thresholds calculated")
                return pd.DataFrame()

        except Exception as e:
            self.log_error(f"Failed to calculate static thresholds: {e}")
            raise

    def _calculate_dynamic_thresholds(
        self, user_demographics: pd.DataFrame
    ) -> pd.DataFrame:
        """Calculate dynamic thresholds for all users.

        Args:
            user_demographics: DataFrame with user demographic information

        Returns:
            DataFrame with dynamic thresholds per user
        """
        try:
            self.log_info("Calculating dynamic thresholds")

            dynamic_thresholds = get_week_thresholds_per_user_dynamic(user_demographics)

            if dynamic_thresholds is not None and not dynamic_thresholds.empty:
                self.log_info(
                    f"Calculated dynamic thresholds for {len(dynamic_thresholds)} users"
                )
                return dynamic_thresholds
            else:
                self.log_warning("No dynamic thresholds calculated")
                return pd.DataFrame()

        except Exception as e:
            self.log_error(f"Failed to calculate dynamic thresholds: {e}")
            raise

    def _combine_threshold_results(
        self,
        static_thresholds: Optional[pd.DataFrame],
        dynamic_thresholds: Optional[pd.DataFrame],
        start_date: str,
        end_date: str,
    ) -> Dict[str, Any]:
        """Combine threshold results into final output.

        Args:
            static_thresholds: DataFrame with static thresholds
            dynamic_thresholds: DataFrame with dynamic thresholds
            start_date: Start date of analysis period
            end_date: End date of analysis period

        Returns:
            Dictionary containing combined threshold results
        """
        try:
            results = {
                "analysis_period": {
                    "start_date": start_date,
                    "end_date": end_date,
                },
                "configuration": {
                    "calculate_static": self.config["calculate_static"],
                    "calculate_dynamic": self.config["calculate_dynamic"],
                    "include_wave_info": self.config["include_wave_info"],
                },
            }

            # Add static thresholds if available
            if static_thresholds is not None and not static_thresholds.empty:
                results["static_thresholds"] = static_thresholds
                results["static_thresholds_count"] = len(static_thresholds)

            # Add dynamic thresholds if available
            if dynamic_thresholds is not None and not dynamic_thresholds.empty:
                results["dynamic_thresholds"] = dynamic_thresholds
                results["dynamic_thresholds_count"] = len(dynamic_thresholds)

            # Add summary statistics
            results["summary"] = {
                "total_users": len(static_thresholds)
                if static_thresholds is not None
                else 0,
                "static_thresholds_available": static_thresholds is not None
                and not static_thresholds.empty,
                "dynamic_thresholds_available": dynamic_thresholds is not None
                and not dynamic_thresholds.empty,
            }

            self.log_info("Successfully combined threshold results")
            return results

        except Exception as e:
            self.log_error(f"Failed to combine threshold results: {e}")
            raise

    def get_static_thresholds(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Get static thresholds for a specific time period.

        Args:
            start_date: Start date string in YYYY-MM-DD format
            end_date: End date string in YYYY-MM-DD format

        Returns:
            DataFrame with static thresholds
        """
        try:
            results = self.calculate_thresholds(start_date, end_date)
            return results.get("static_thresholds", pd.DataFrame())

        except Exception as e:
            self.log_error(f"Failed to get static thresholds: {e}")
            return pd.DataFrame()

    def get_dynamic_thresholds(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Get dynamic thresholds for a specific time period.

        Args:
            start_date: Start date string in YYYY-MM-DD format
            end_date: End date string in YYYY-MM-DD format

        Returns:
            DataFrame with dynamic thresholds
        """
        try:
            results = self.calculate_thresholds(start_date, end_date)
            return results.get("dynamic_thresholds", pd.DataFrame())

        except Exception as e:
            self.log_error(f"Failed to get dynamic thresholds: {e}")
            return pd.DataFrame()

    def get_thresholds_summary(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Get a summary of threshold calculations for a specific time period.

        Args:
            start_date: Start date string in YYYY-MM-DD format
            end_date: End date string in YYYY-MM-DD format

        Returns:
            Dictionary with threshold summary information
        """
        try:
            results = self.calculate_thresholds(start_date, end_date)

            summary = {
                "analysis_period": results.get("analysis_period", {}),
                "configuration": results.get("configuration", {}),
                "summary": results.get("summary", {}),
                "analysis_timestamp": pd.Timestamp.now().isoformat(),
            }

            return summary

        except Exception as e:
            self.log_error(f"Failed to get thresholds summary: {e}")
            return {
                "analysis_period": {"start_date": start_date, "end_date": end_date},
                "error": str(e),
                "analysis_timestamp": pd.Timestamp.now().isoformat(),
            }
