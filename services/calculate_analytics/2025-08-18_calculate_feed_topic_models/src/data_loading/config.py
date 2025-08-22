"""
Configuration Management for Data Loaders

This module provides configuration management for data loaders in the topic modeling pipeline.
It handles YAML-based configuration files and provides a clean interface for accessing
loader-specific settings.

Author: AI Agent implementing MET-44
Date: 2025-08-22
"""

from pathlib import Path
from typing import Any, Dict, Optional, Type

import yaml

from .base import DataLoader


class DataLoaderConfig:
    """
    Configuration manager for data loaders in the topic modeling pipeline.

    This class handles loading, updating, and accessing configuration for different
    data loader types. It automatically creates default configurations if none exist
    and provides a clean interface for loader-specific settings.

    The configuration assumes all data is stored in .parquet format and focuses
    on the essential parameters for data loading.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.

        Args:
            config_path: Path to configuration file. If None, uses default path.
        """
        if config_path is None:
            default_config = (
                Path(__file__).parent.parent.parent / "config" / "data_loader.yaml"
            )
            config_path = str(default_config)

        self.config_path = Path(config_path)
        self.config_data = {}
        self.available_loaders = {}

        if self.config_path.exists():
            self.load_config()

    def load_config(self) -> None:
        """
        Load configuration from the YAML file.

        If the file doesn't exist, creates a default configuration.
        """
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                self.config_data = yaml.safe_load(f)
        except FileNotFoundError:
            self._create_default_config()
        except yaml.YAMLError as e:
            raise yaml.YAMLError(
                f"Error parsing configuration file {self.config_path}: {e}"
            )

    def _create_default_config(self) -> None:
        """Create a default configuration file."""
        default_config = {
            "data_loader": {
                "type": "local",
                "local": {
                    "enabled": True,
                    "service": "preprocessed_posts",
                    "directory": "cache",
                },
                "production": {
                    "enabled": False,
                    "endpoint": "",
                    "api_key": "",
                    "timeout": 30,
                },
            },
            "validation": {
                "max_date_range_days": 365,
                "min_text_length": 10,
                "max_text_length": 10000,
            },
            "performance": {
                "batch_size": 1000,
                "memory_limit_gb": 8,
                "enable_compression": True,
            },
        }

        self.config_path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.config_path, "w", encoding="utf-8") as f:
            yaml.dump(default_config, f, default_flow_style=False, indent=2)

        self.config_data = default_config

    def register_loader(self, name: str, loader_class: Type[DataLoader]) -> None:
        """
        Register a data loader class.

        Args:
            name: Name for the loader
            loader_class: Class that inherits from DataLoader
        """
        if not issubclass(loader_class, DataLoader):
            raise ValueError(
                f"Loader class must inherit from DataLoader, got {loader_class}"
            )

        self.available_loaders[name] = loader_class

    def get_loader_config(self, loader_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Get configuration for a specific loader type.

        Args:
            loader_type: Type of loader to get config for. If None, uses default.

        Returns:
            Dictionary containing loader configuration

        Raises:
            KeyError: If configuration not found for loader type
        """
        if loader_type is None:
            loader_type = self.config_data.get("data_loader", {}).get("type", "local")

        loader_config = self.config_data.get("data_loader", {}).get(loader_type, {})
        if not loader_config:
            raise KeyError(f"Configuration not found for loader type: {loader_type}")

        return loader_config

    def is_loader_enabled(self, loader_type: str) -> bool:
        """
        Check if a specific loader type is enabled.

        Args:
            loader_type: Type of loader to check

        Returns:
            True if loader is enabled, False otherwise
        """
        try:
            loader_config = self.get_loader_config(loader_type)
            return loader_config.get("enabled", False)
        except KeyError:
            return False

    def get_validation_config(self) -> Dict[str, Any]:
        """
        Get validation configuration.

        Returns:
            Dictionary containing validation settings
        """
        return self.config_data.get("validation", {})

    def get_performance_config(self) -> Dict[str, Any]:
        """
        Get performance configuration.

        Returns:
            Dictionary containing performance settings
        """
        return self.config_data.get("performance", {})

    def update_config(self, updates: Dict[str, Any]) -> None:
        """
        Update configuration with new values.

        Args:
            updates: Dictionary containing configuration updates
        """
        self._deep_merge(self.config_data, updates)

        with open(self.config_path, "w", encoding="utf-8") as f:
            yaml.dump(self.config_data, f, default_flow_style=False, indent=2)

    def _deep_merge(self, base: Dict[str, Any], updates: Dict[str, Any]) -> None:
        """
        Deep merge updates into base dictionary.

        Args:
            base: Base dictionary to update
            updates: Updates to apply
        """
        for key, value in updates.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value

    def get_info(self) -> Dict[str, Any]:
        """
        Get information about the current configuration.

        Returns:
            Dictionary containing configuration information
        """
        return {
            "config_path": str(self.config_path),
            "available_loaders": list(self.available_loaders.keys()),
            "enabled_loaders": [
                loader_type
                for loader_type in self.available_loaders.keys()
                if self.is_loader_enabled(loader_type)
            ],
            "default_loader": self.config_data.get("data_loader", {}).get(
                "type", "local"
            ),
            "validation_config": self.get_validation_config(),
            "performance_config": self.get_performance_config(),
        }

    def __str__(self) -> str:
        """String representation of the configuration manager."""
        return f"DataLoaderConfig(config_path='{self.config_path}', available_loaders={list(self.available_loaders.keys())})"

    def __repr__(self) -> str:
        """Detailed string representation of the configuration manager."""
        return f"DataLoaderConfig(config_path='{self.config_path}', config_data={self.config_data}, available_loaders={self.available_loaders})"
