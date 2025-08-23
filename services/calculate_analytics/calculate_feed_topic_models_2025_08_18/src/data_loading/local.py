"""
Local Data Loader Implementation for Topic Modeling Pipeline

This module provides a concrete implementation of the DataLoader interface
that loads preprocessed posts from local storage using the existing
load_data_from_local_storage function.

Author: AI Agent implementing MET-44
Date: 2025-08-22
"""

import logging

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.constants import study_start_date, study_end_date

from services.calculate_analytics.calculate_feed_topic_models_2025_08_18.src.data_loading.base import (
    DataLoader,
    DataLoadingError,
    ValidationError,
)


logger = logging.getLogger(__name__)


class LocalDataLoader(DataLoader):
    """
    Local data loader implementation using existing local storage infrastructure.

    This loader uses the `load_data_from_local_storage` function to load
    preprocessed posts from local storage, with support for date range filtering
    and data validation.

    Attributes:
        service (str): The service name for data loading (default: "preprocessed_posts")
        directory (str): Directory to load from ("cache" or "active")
    """

    def __init__(self, service: str = "preprocessed_posts", directory: str = "cache"):
        """
        Initialize the local data loader.

        Args:
            service: Service name for data loading (default: "preprocessed_posts")
            directory: Directory to load from ("cache" or "active")
        """
        super().__init__(
            name="Local Data Loader",
            description=f"Loads {service} data from local {directory} storage",
        )

        self.service = service
        self.directory = directory

        # Validate directory
        if directory not in ["cache", "active"]:
            raise ValueError(f"Directory must be 'cache' or 'active', got: {directory}")

        logger.info(f"Initialized LocalDataLoader for {service} from {directory}")

    def load_text_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Load text data for the specified date range.

        This method uses the existing `load_data_from_local_storage` function
        to load preprocessed posts from local storage, with proper date filtering
        and validation.

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            DataFrame with text content ready for BERTopic processing.
            The DataFrame must contain at least a 'text' column.

        Raises:
            DataLoadingError: If data loading fails for any reason
            ValidationError: If date parameters are invalid or unsupported
            ValueError: If the data source is not available or accessible
        """
        try:
            # Validate date range first
            self.validate_date_range(start_date, end_date)

            # Additional validation for study date range
            self._validate_study_date_range(start_date, end_date)

            logger.info(f"Loading {self.service} data from {start_date} to {end_date}")

            # Load data using existing function with only essential parameters
            df = load_data_from_local_storage(
                service=self.service,
                directory=self.directory,
                start_partition_date=start_date,
                end_partition_date=end_date,
            )

            # Validate that we got data
            if df is None or len(df) == 0:
                raise DataLoadingError(
                    f"No data found for {self.service} from {start_date} to {end_date}"
                )

            # Validate that we have the required text column
            if "text" not in df.columns:
                raise DataLoadingError(
                    f"Loaded data missing required 'text' column. Available columns: {list(df.columns)}"
                )

            # Basic data quality checks
            df = self._validate_and_clean_data(df)

            logger.info(f"Successfully loaded {len(df)} records from {self.service}")

            return df

        except Exception as e:
            if isinstance(e, (DataLoadingError, ValidationError)):
                raise
            else:
                raise DataLoadingError(
                    f"Failed to load data from {self.service}: {str(e)}"
                ) from e

    def _validate_study_date_range(self, start_date: str, end_date: str) -> None:
        """
        Validate that the requested date range is within the study period.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Raises:
            ValidationError: If date range is outside study period
        """
        try:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            study_start = pd.to_datetime(study_start_date)
            study_end = pd.to_datetime(study_end_date)

            if start_dt < study_start:
                raise ValidationError(
                    f"Start date {start_date} is before study start date {study_start_date}"
                )

            if end_dt > study_end:
                raise ValidationError(
                    f"End date {end_date} is after study end date {study_end_date}"
                )

        except Exception as e:
            raise ValidationError(f"Error validating study date range: {str(e)}")

    def _validate_and_clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and clean the loaded data.

        Args:
            df: Raw DataFrame from data loading

        Returns:
            Cleaned DataFrame ready for processing

        Raises:
            DataLoadingError: If data quality checks fail
        """
        # Remove rows with missing text
        initial_count = len(df)
        df = df.dropna(subset=["text"])

        if len(df) < initial_count:
            logger.warning(f"Removed {initial_count - len(df)} rows with missing text")

        # Remove rows with empty text
        df = df[df["text"].str.strip().str.len() > 0]

        if len(df) == 0:
            raise DataLoadingError("No valid text data found after cleaning")

        # Ensure text column is string type
        df["text"] = df["text"].astype(str)

        # Remove very long texts that might cause issues
        max_length = 10000  # Configurable
        df = df[df["text"].str.len() <= max_length]

        logger.info(f"Data cleaning complete. Final dataset: {len(df)} records")

        return df

    def __str__(self) -> str:
        """String representation of the local data loader."""
        return (
            f"LocalDataLoader(service='{self.service}', directory='{self.directory}')"
        )

    def __repr__(self) -> str:
        """Detailed string representation of the local data loader."""
        return (
            f"LocalDataLoader(service='{self.service}', directory='{self.directory}')"
        )
