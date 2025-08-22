"""
Abstract DataLoader Interface for Topic Modeling Pipeline

This module defines the abstract base class for data loaders in the topic modeling pipeline.
All data loaders must implement the load_text_data method to provide text data for BERTopic processing.

Author: AI Agent implementing MET-44
Date: 2025-08-22
"""

from abc import ABC, abstractmethod

import pandas as pd


class DataLoadingError(Exception):
    """Exception raised when data loading fails."""

    pass


class ValidationError(Exception):
    """Exception raised when input validation fails."""

    pass


class DataLoader(ABC):
    """
    Abstract base class for data loaders in the topic modeling pipeline.

    All data loaders must implement the load_text_data method to provide
    text data in the format expected by the BERTopic pipeline.

    The interface assumes all data is stored in .parquet format and focuses
    on the core functionality of loading text data for specific date ranges.
    """

    def __init__(self, name: str, description: str):
        """
        Initialize the data loader.

        Args:
            name: Human-readable name for this data loader
            description: Description of what this data loader does
        """
        self.name = name
        self.description = description

    @abstractmethod
    def load_text_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Load text data for the specified date range.

        This method must return a DataFrame with at least a 'text' column
        containing the text content to be processed by BERTopic.

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            DataFrame with text content ready for BERTopic processing.
            Must contain a 'text' column.

        Raises:
            DataLoadingError: If data loading fails for any reason
            ValidationError: If date parameters are invalid or unsupported
        """
        pass

    def validate_date_range(self, start_date: str, end_date: str) -> bool:
        """
        Validate the date range parameters.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            True if validation passes

        Raises:
            ValidationError: If validation fails
        """
        try:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)

            if start_dt > end_dt:
                raise ValidationError(
                    f"Start date {start_date} must be before or equal to end date {end_date}"
                )

            current_date = pd.Timestamp.now().date()
            if start_dt.date() > current_date or end_dt.date() > current_date:
                raise ValidationError(
                    f"Dates cannot be in the future: {start_date} to {end_date}"
                )

            return True

        except ValueError as e:
            raise ValidationError(
                f"Invalid date format. Expected YYYY-MM-DD, got: {start_date}, {end_date}"
            ) from e

    def get_info(self) -> dict:
        """
        Get information about this data loader.

        Returns:
            Dictionary containing loader information
        """
        return {
            "name": self.name,
            "description": self.description,
            "type": self.__class__.__name__,
        }

    def __str__(self) -> str:
        """String representation of the data loader."""
        return f"{self.__class__.__name__}(name='{self.name}', description='{self.description}')"

    def __repr__(self) -> str:
        """Detailed string representation of the data loader."""
        return f"{self.__class__.__name__}(name='{self.name}', description='{self.description}')"
