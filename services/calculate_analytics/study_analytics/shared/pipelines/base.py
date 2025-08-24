"""Simple base class for analytics with shared utilities.

This module defines the BaseAnalyzer class that provides common functionality
for all analytics classes without complex pipeline orchestration.
"""

from typing import Any, Dict, List, Optional

import pandas as pd

from lib.log.logger import get_logger


class BaseAnalyzer:
    """Simple base class for analytics with shared utilities.

    Focuses on common functionality without complex pipeline orchestration.
    Provides shared utility methods for configuration validation, logging,
    and data validation that all analysis classes can inherit.
    """

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """Initialize the analyzer.

        Args:
            name: Unique name for this analyzer instance
            config: Configuration dictionary for analyzer parameters
        """
        self.name = name
        self.config = config or {}
        self.logger = get_logger(f"analyzer.{name}")

    def validate_config(self, required_keys: List[str]) -> None:
        """Validate required configuration keys exist.

        Args:
            required_keys: List of configuration keys that must be present

        Raises:
            ValueError: If any required configuration keys are missing
        """
        missing = [key for key in required_keys if key not in self.config]
        if missing:
            raise ValueError(f"Missing required configuration: {missing}")

    def log_execution(self, method_name: str, **kwargs) -> None:
        """Log method execution with parameters.

        Args:
            method_name: Name of the method being executed
            **kwargs: Parameters being passed to the method
        """
        self.logger.info(f"Executing {method_name} with params: {kwargs}")

    def validate_data(self, df: pd.DataFrame, required_columns: List[str]) -> bool:
        """Validate DataFrame has required columns.

        Args:
            df: DataFrame to validate
            required_columns: List of column names that must be present

        Returns:
            True if all required columns are present, False otherwise
        """
        if df.empty:
            self.logger.warning("Empty DataFrame provided for validation")
            return False

        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            self.logger.warning(f"Missing required columns: {missing}")
            return False

        return True

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """Get configuration value with fallback.

        Args:
            key: Configuration key to retrieve
            default: Default value if key is not found

        Returns:
            Configuration value or default if not found
        """
        return self.config.get(key, default)

    def log_info(self, message: str) -> None:
        """Log informational message with analyzer context.

        Args:
            message: Message to log
        """
        self.logger.info(f"[{self.name}] {message}")

    def log_warning(self, message: str) -> None:
        """Log warning message with analyzer context.

        Args:
            message: Warning message to log
        """
        self.logger.warning(f"[{self.name}] {message}")

    def log_error(self, message: str) -> None:
        """Log error message with analyzer context.

        Args:
            message: Error message to log
        """
        self.logger.error(f"[{self.name}] {message}")

    def get_required_config(self, key: str) -> Any:
        """Get required configuration value, raising error if missing.

        Args:
            key: Configuration key to retrieve

        Returns:
            Configuration value

        Raises:
            ValueError: If configuration key is missing
        """
        if key not in self.config:
            raise ValueError(f"Required configuration key '{key}' not found")
        return self.config[key]
