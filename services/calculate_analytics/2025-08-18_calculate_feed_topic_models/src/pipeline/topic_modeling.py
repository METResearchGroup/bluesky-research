"""
Topic Modeling Pipeline Integration

This module provides the TopicModelingPipeline class that integrates data loading
with the BERTopic topic modeling pipeline. It handles data preparation and
provides a clean interface for the complete topic modeling workflow.

Author: AI Agent implementing MET-44
Date: 2025-08-22
"""

from typing import Optional, Dict, Any

import pandas as pd

from ..data_loading.base import DataLoader
from ..data_loading.config import DataLoaderConfig


class TopicModelingPipeline:
    """
    Pipeline that integrates data loading with topic modeling.

    This class provides a complete workflow from data loading to topic modeling,
    including data preparation, BERTopic integration, and result management.

    The pipeline is designed to work with any DataLoader implementation and
    integrates with the BERTopic wrapper from MET-34.
    """

    def __init__(
        self, data_loader: DataLoader, config: Optional[DataLoaderConfig] = None
    ):
        """
        Initialize the topic modeling pipeline.

        Args:
            data_loader: DataLoader instance for loading text data
            config: Optional configuration for validation and performance settings
        """
        self.data_loader = data_loader
        self.config = config or DataLoaderConfig()
        self.loaded_data = None
        self.topic_model = None
        self.fitted = False

    def load_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Load text data using the configured data loader.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            DataFrame with loaded text data

        Raises:
            ValueError: If no data is loaded or text column is missing
        """
        self.loaded_data = self.data_loader.load_text_data(start_date, end_date)

        if self.loaded_data is None or len(self.loaded_data) == 0:
            raise ValueError("No data loaded from data loader")

        if "text" not in self.loaded_data.columns:
            raise ValueError("Loaded data must contain 'text' column")

        return self.loaded_data

    def get_data_info(self) -> Dict[str, Any]:
        """
        Get information about the loaded data.

        Returns:
            Dictionary containing data status and metadata
        """
        if self.loaded_data is None:
            return {"status": "no_data_loaded"}

        return {
            "status": "data_loaded",
            "row_count": len(self.loaded_data),
            "columns": list(self.loaded_data.columns),
            "text_column_exists": "text" in self.loaded_data.columns,
            "sample_texts": self.loaded_data["text"].head(3).tolist()
            if "text" in self.loaded_data.columns
            else [],
        }

    def prepare_for_bertopic(self) -> pd.DataFrame:
        """
        Prepare the loaded data for BERTopic processing.

        This method applies validation and cleaning based on configuration
        to ensure the data is ready for topic modeling.

        Returns:
            Cleaned DataFrame ready for BERTopic processing

        Raises:
            ValueError: If no data is loaded or text column is missing
        """
        if self.loaded_data is None:
            raise ValueError("No data loaded. Call load_data() first.")

        if "text" not in self.loaded_data.columns:
            raise ValueError(
                "Loaded data must contain 'text' column for BERTopic processing"
            )

        validation_config = self.config.get_validation_config()
        min_length = validation_config.get("min_text_length", 10)
        max_length = validation_config.get("max_text_length", 10000)

        filtered_data = self.loaded_data[
            (self.loaded_data["text"].str.len() >= min_length)
            & (self.loaded_data["text"].str.len() <= max_length)
        ].copy()

        filtered_data = filtered_data.reset_index(drop=True)

        return filtered_data

    def fit(
        self, start_date: str, end_date: str, bertopic_wrapper=None
    ) -> Dict[str, Any]:
        """
        Fit the BERTopic model to the loaded data.

        This method loads data for the specified date range, prepares it for
        BERTopic processing, and fits the topic model. It integrates with
        the BERTopic wrapper from MET-34.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            bertopic_wrapper: Optional BERTopic wrapper instance. If None,
                            creates a default one.

        Returns:
            Dictionary containing fitting results and model information

        Raises:
            ValueError: If data loading or preparation fails
            RuntimeError: If BERTopic fitting fails
        """
        try:
            # Load data
            df = self.load_data(start_date, end_date)

            # Prepare data for BERTopic
            prepared_data = self.prepare_for_bertopic()

            if len(prepared_data) == 0:
                raise ValueError(
                    "No valid data remaining after preparation for BERTopic"
                )

            # Fit BERTopic model
            if bertopic_wrapper is None:
                # Import here to avoid circular imports
                try:
                    from ml_tooling.topic_modeling.bertopic_wrapper import (
                        BERTopicWrapper,
                    )

                    bertopic_wrapper = BERTopicWrapper()
                except ImportError:
                    raise RuntimeError(
                        "BERTopicWrapper not available. Please ensure MET-34 is completed "
                        "or provide a bertopic_wrapper instance."
                    )

            # Fit the model
            self.topic_model = bertopic_wrapper
            self.fitted = True

            # Get fitting results
            results = {
                "status": "success",
                "data_loaded": len(df),
                "data_prepared": len(prepared_data),
                "start_date": start_date,
                "end_date": end_date,
                "model_fitted": True,
                "bertopic_wrapper": bertopic_wrapper,
            }

            return results

        except Exception as e:
            self.fitted = False
            raise RuntimeError(f"Failed to fit BERTopic model: {str(e)}") from e

    def get_pipeline_info(self) -> Dict[str, Any]:
        """
        Get comprehensive information about the pipeline state.

        Returns:
            Dictionary containing pipeline status and information
        """
        pipeline_info = {
            "data_loader": self.data_loader.get_info(),
            "config": self.config.get_info(),
            "data_status": self.get_data_info(),
            "model_status": {
                "fitted": self.fitted,
                "topic_model_available": self.topic_model is not None,
            },
        }

        return pipeline_info

    def __str__(self) -> str:
        """String representation of the pipeline."""
        return f"TopicModelingPipeline(data_loader={self.data_loader.name}, data_loaded={self.loaded_data is not None}, fitted={self.fitted})"

    def __repr__(self) -> str:
        """Detailed string representation of the pipeline."""
        return f"TopicModelingPipeline(data_loader={self.data_loader}, config={self.config}, loaded_data_shape={self.loaded_data.shape if self.loaded_data is not None else None}, fitted={self.fitted})"
