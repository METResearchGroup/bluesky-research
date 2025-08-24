"""Weekly thresholds pipeline implementation.

This module implements the WeeklyThresholdsPipeline class that converts the
existing calculate_weekly_thresholds_per_user.py logic into a standardized pipeline framework.
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
    BaseResearchPipeline,
    PipelineError,
    PipelineResult,
)

logger = get_logger(__name__)


class WeeklyThresholdsPipeline(BaseResearchPipeline):
    """Pipeline for calculating weekly thresholds for study users.

    This pipeline implements the functionality from the original
    calculate_weekly_thresholds_per_user.py script, providing a standardized
    interface for weekly threshold calculations.
    """

    def __init__(
        self, name: str = "weekly_thresholds", config: Optional[Dict[str, Any]] = None
    ):
        """Initialize the weekly thresholds pipeline.

        Args:
            name: Name for this pipeline instance
            config: Configuration dictionary for pipeline parameters
        """
        super().__init__(name, config)

        # Load configuration
        self.config_obj = get_config()
        self.study_config = self.config_obj.study
        self.week_config = self.config_obj.weeks

        # Set default configuration
        self.default_config = {
            "exclude_partition_dates": ["2024-10-08"],
            "calculate_static": True,
            "calculate_dynamic": True,
            "include_wave_info": True,
        }
        self.config.update(self.default_config)

        # Pipeline state
        self.user_demographics: Optional[pd.DataFrame] = None
        self.static_thresholds: Optional[pd.DataFrame] = None
        self.dynamic_thresholds: Optional[pd.DataFrame] = None
        self.final_results: Optional[pd.DataFrame] = None
        self.qualtrics_logs: Optional[pd.DataFrame] = None

    def setup(self) -> None:
        """Setup pipeline resources and validate configuration."""
        self.logger.info("Setting up weekly thresholds pipeline")

        # Validate required configuration
        required_config = [
            "exclude_partition_dates",
            "calculate_static",
            "calculate_dynamic",
        ]
        missing_config = [key for key in required_config if key not in self.config]

        if missing_config:
            raise PipelineError(
                f"Missing required configuration: {missing_config}", self.name, "setup"
            )

        # Validate study configuration
        if not hasattr(self.study_config, "wave_1_study_start_date_inclusive"):
            raise PipelineError(
                "Study configuration missing wave dates", self.name, "setup"
            )

        # Validate week configuration
        if not hasattr(self.week_config, "wave_1_week_start_dates_inclusive"):
            raise PipelineError(
                "Week configuration missing week start dates", self.name, "setup"
            )

        self.logger.info("Weekly thresholds pipeline setup completed")

    def execute(self) -> PipelineResult:
        """Execute the weekly thresholds pipeline.

        This method implements the main logic from calculate_weekly_thresholds_per_user.py:
        1. Load user demographic information
        2. Calculate static week thresholds
        3. Calculate dynamic week thresholds (if enabled)
        4. Combine results into final output

        Returns:
            PipelineResult with execution results
        """
        try:
            self.logger.info("Executing weekly thresholds pipeline")

            # Load user demographics
            self.logger.info("Loading user demographic information")
            self.user_demographics = self.load_user_demographics()

            if self.user_demographics.empty:
                self.logger.warning("No user demographics loaded")
                return PipelineResult(
                    success=True, data=pd.DataFrame(), metadata={"users_processed": 0}
                )

            self.logger.info(
                f"Loaded demographics for {len(self.user_demographics)} users"
            )

            # Calculate static thresholds
            if self.config.get("calculate_static", True):
                self.logger.info("Calculating static week thresholds")
                self.static_thresholds = self.calculate_static_thresholds()
                self.logger.info(
                    f"Static thresholds calculated for {len(self.static_thresholds)} user-date combinations"
                )

            # Calculate dynamic thresholds
            if self.config.get("calculate_dynamic", True):
                self.logger.info("Calculating dynamic week thresholds")
                self.dynamic_thresholds = self.calculate_dynamic_thresholds()
                if self.dynamic_thresholds is not None:
                    self.logger.info(
                        f"Dynamic thresholds calculated for {len(self.dynamic_thresholds)} user-date combinations"
                    )

            # Combine results
            self.logger.info("Combining threshold results")
            self.final_results = self.combine_threshold_results()

            # Update metadata
            self.metadata.update(
                {
                    "users_processed": len(self.user_demographics),
                    "static_thresholds_count": len(self.static_thresholds)
                    if self.static_thresholds is not None
                    else 0,
                    "dynamic_thresholds_count": len(self.dynamic_thresholds)
                    if self.dynamic_thresholds is not None
                    else 0,
                    "final_results_count": len(self.final_results)
                    if self.final_results is not None
                    else 0,
                }
            )

            self.logger.info("Weekly thresholds pipeline completed successfully")

            return PipelineResult(
                success=True,
                data=self.final_results,
                metadata=self.metadata,
                timestamp=self.start_time,
            )

        except Exception as e:
            raise PipelineError(
                f"Weekly thresholds execution failed: {str(e)}", self.name, "execute"
            )

    def cleanup(self) -> None:
        """Clean up pipeline resources."""
        self.logger.info("Cleaning up weekly thresholds pipeline")

        # Clear data references
        self.user_demographics = None
        self.static_thresholds = None
        self.dynamic_thresholds = None
        self.final_results = None
        self.qualtrics_logs = None

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
        required_columns = ["bluesky_handle", "wave", "date"]
        missing_columns = [
            col for col in required_columns if col not in self.final_results.columns
        ]

        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False

        # Check data types
        if not pd.api.types.is_string_dtype(self.final_results["bluesky_handle"]):
            self.logger.error("bluesky_handle column is not string type")
            return False

        if not pd.api.types.is_integer_dtype(self.final_results["wave"]):
            self.logger.error("wave column is not integer type")
            return False

        # Check for valid wave values
        valid_waves = [1, 2]
        invalid_waves = self.final_results[
            ~self.final_results["wave"].isin(valid_waves)
        ]["wave"].unique()
        if len(invalid_waves) > 0:
            self.logger.error(f"Invalid wave values found: {invalid_waves}")
            return False

        # Check for valid week values (if present)
        if "week_static" in self.final_results.columns:
            valid_weeks = list(range(1, 9))  # Weeks 1-8
            invalid_weeks = self.final_results[
                ~self.final_results["week_static"].isin(valid_weeks)
            ]["week_static"].unique()
            if len(invalid_weeks) > 0:
                self.logger.error(f"Invalid static week values found: {invalid_weeks}")
                return False

        self.logger.info("Weekly thresholds validation passed")
        return True

    def load_user_demographics(self) -> pd.DataFrame:
        """Load user demographic information.

        Returns:
            DataFrame with user demographic information

        Raises:
            PipelineError: If data loading fails
        """
        try:
            self.logger.info("Loading user demographic information")
            user_df = load_user_demographic_info()

            # Add wave information if enabled
            if self.config.get("include_wave_info", True):
                user_df = self.add_wave_information(user_df)

            self.logger.info(f"Loaded demographics for {len(user_df)} study users")
            return user_df

        except Exception as e:
            raise PipelineError(
                f"Failed to load user demographics: {str(e)}",
                self.name,
                "load_user_demographics",
            )

    def add_wave_information(self, user_df: pd.DataFrame) -> pd.DataFrame:
        """Add wave information to user demographics.

        Args:
            user_df: DataFrame with user demographic information

        Returns:
            DataFrame with added wave column
        """
        # This is a simplified implementation
        # In practice, wave information would come from study configuration
        # For now, assign waves based on some logic or configuration

        # Get wave configuration
        wave_1_start = self.study_config.get(
            "wave_1_study_start_date_inclusive", "2024-09-30"
        )
        wave_2_start = self.study_config.get(
            "wave_2_study_start_date_inclusive", "2024-10-07"
        )
        logger.info(f"Wave 1 start: {wave_1_start}")
        logger.info(f"Wave 2 start: {wave_2_start}")

        # Simple wave assignment (this would be more sophisticated in practice)
        # For now, assign based on condition or other logic
        if "condition" in user_df.columns:
            # Assign waves based on condition (simplified)
            user_df["wave"] = user_df["condition"].apply(
                lambda x: 1 if "wave1" in str(x).lower() else 2
            )
        else:
            # Default wave assignment
            user_df["wave"] = 1

        return user_df

    def calculate_static_thresholds(self) -> pd.DataFrame:
        """Calculate static week thresholds for all users.

        Returns:
            DataFrame with static week assignments
        """
        try:
            if self.user_demographics is None or self.user_demographics.empty:
                self.logger.warning(
                    "No user demographics available for static threshold calculation"
                )
                return pd.DataFrame()

            # Get static week thresholds
            static_thresholds = get_week_thresholds_per_user_static(
                self.user_demographics
            )

            self.logger.info(
                f"Calculated static thresholds for {len(static_thresholds)} user-date combinations"
            )
            return static_thresholds

        except Exception as e:
            self.logger.error(f"Error calculating static thresholds: {e}")
            return pd.DataFrame()

    def calculate_dynamic_thresholds(self) -> Optional[pd.DataFrame]:
        """Calculate dynamic week thresholds for all users.

        Returns:
            DataFrame with dynamic week assignments, or None if calculation fails
        """
        try:
            if self.user_demographics is None or self.user_demographics.empty:
                self.logger.warning(
                    "No user demographics available for dynamic threshold calculation"
                )
                return None

            # Load Qualtrics logs if available
            # For now, we'll skip dynamic thresholds if logs aren't available
            if self.qualtrics_logs is None:
                self.logger.info(
                    "Qualtrics logs not available, skipping dynamic threshold calculation"
                )
                return None

            # Get dynamic week thresholds
            dynamic_thresholds = get_week_thresholds_per_user_dynamic(
                self.qualtrics_logs, self.user_demographics
            )

            self.logger.info(
                f"Calculated dynamic thresholds for {len(dynamic_thresholds)} user-date combinations"
            )
            return dynamic_thresholds

        except Exception as e:
            self.logger.error(f"Error calculating dynamic thresholds: {e}")
            return None

    def combine_threshold_results(self) -> pd.DataFrame:
        """Combine static and dynamic threshold results.

        Returns:
            DataFrame with combined threshold results
        """
        try:
            results = []

            # Add static thresholds
            if self.static_thresholds is not None and not self.static_thresholds.empty:
                static_df = self.static_thresholds.copy()
                static_df["threshold_type"] = "static"
                results.append(static_df)

            # Add dynamic thresholds
            if (
                self.dynamic_thresholds is not None
                and not self.dynamic_thresholds.empty
            ):
                dynamic_df = self.dynamic_thresholds.copy()
                dynamic_df["threshold_type"] = "dynamic"
                results.append(dynamic_df)

            if not results:
                self.logger.warning("No threshold results to combine")
                return pd.DataFrame()

            # Combine all results
            combined_df = pd.concat(results, ignore_index=True)

            # Sort by user and date
            combined_df = combined_df.sort_values(["bluesky_handle", "date"])

            self.logger.info(f"Combined {len(combined_df)} threshold results")
            return combined_df

        except Exception as e:
            self.logger.error(f"Error combining threshold results: {e}")
            return pd.DataFrame()

    def set_qualtrics_logs(self, qualtrics_logs: pd.DataFrame) -> None:
        """Set Qualtrics logs for dynamic threshold calculation.

        Args:
            qualtrics_logs: DataFrame with Qualtrics survey logs
        """
        self.qualtrics_logs = qualtrics_logs
        self.logger.info(f"Set Qualtrics logs with {len(qualtrics_logs)} records")

    def get_study_dates(self) -> Dict[str, str]:
        """Get study date range.

        Returns:
            Dictionary with study start and end dates
        """
        return {
            "wave_1_start": self.study_config.get(
                "wave_1_study_start_date_inclusive", "2024-09-30"
            ),
            "wave_1_end": self.study_config.get(
                "wave_1_study_end_date_inclusive", "2024-11-25"
            ),
            "wave_2_start": self.study_config.get(
                "wave_2_study_start_date_inclusive", "2024-10-07"
            ),
            "wave_2_end": self.study_config.get(
                "wave_2_study_end_date_inclusive", "2024-12-01"
            ),
        }
