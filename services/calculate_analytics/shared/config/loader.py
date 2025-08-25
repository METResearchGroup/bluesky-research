"""Configuration loading and management utilities with Pydantic validation."""

import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml
from lib.log.logger import get_logger

from services.calculate_analytics.shared.config.models import (
    AnalyticsConfig,
    StudyConfig,
    FeatureConfig,
)

logger = get_logger(__name__)


class ConfigLoader:
    """Loads and validates configuration from YAML files using Pydantic models."""

    def __init__(self, config_dir: Optional[str] = None):
        if config_dir is None:
            config_dir = os.path.join(os.path.dirname(__file__), ".")
        self.config_dir = Path(config_dir)
        self._config_cache: Dict[str, Any] = {}
        self._validated_config_cache: Dict[str, AnalyticsConfig] = {}

    def load_config(self, config_name: str) -> Dict[str, Any]:
        """Load raw configuration from YAML file without validation."""
        if config_name in self._config_cache:
            return self._config_cache[config_name]

        config_path = self.config_dir / f"{config_name}.yaml"
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            self._config_cache[config_name] = config
            logger.info(f"Loaded raw configuration: {config_name}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration {config_name}: {e}")
            raise

    def load_validated_config(self, config_name: str) -> AnalyticsConfig:
        """Load and validate configuration using Pydantic models."""
        if config_name in self._validated_config_cache:
            return self._validated_config_cache[config_name]

        raw_config = self.load_config(config_name)

        try:
            validated_config = AnalyticsConfig(**raw_config)
            self._validated_config_cache[config_name] = validated_config
            logger.info(f"Configuration validation passed for {config_name}")
            return validated_config
        except Exception as e:
            logger.error(f"Configuration validation failed for {config_name}: {e}")
            raise

    def get_study_config(self, study_id: str) -> StudyConfig:
        """Get validated configuration for a specific study."""
        config = self.load_validated_config("analytics")
        if study_id not in config.studies:
            raise KeyError(f"Study '{study_id}' not found in configuration")
        return config.studies[study_id]

    def get_feature_config(self, feature_type: str) -> FeatureConfig:
        """Get validated configuration for a specific feature type."""
        config = self.load_validated_config("analytics")
        if feature_type not in config.features:
            raise KeyError(f"Feature '{feature_type}' not found in configuration")
        return config.features[feature_type]

    def get_default_config(self) -> Dict[str, Any]:
        """Get default configuration values."""
        config = self.load_validated_config("analytics")
        return config.defaults.model_dump()

    def get_study_week_dates(self, study_id: str) -> Dict[str, list]:
        """Get week start and end dates for a specific study."""
        study_config = self.get_study_config(study_id)

        return {
            "week_start_dates": [week.start for week in study_config.weeks],
            "week_end_dates": [week.end for week in study_config.weeks],
        }

    def get_feature_columns(self, feature_type: str) -> list:
        """Get column names for a specific feature type."""
        feature_config = self.get_feature_config(feature_type)
        return feature_config.columns

    def get_feature_threshold(self, feature_type: str) -> float:
        """Get threshold value for a specific feature type."""
        feature_config = self.get_feature_config(feature_type)
        return feature_config.threshold

    def get_data_loading_config(self) -> Dict[str, Any]:
        """Get data loading configuration."""
        config = self.load_validated_config("analytics")
        return config.data_loading.model_dump()

    def get_all_studies(self) -> Dict[str, StudyConfig]:
        """Get all study configurations."""
        config = self.load_validated_config("analytics")
        return config.studies

    def get_all_features(self) -> Dict[str, FeatureConfig]:
        """Get all feature configurations."""
        config = self.load_validated_config("analytics")
        return config.features

    def validate_config_file(self, config_name: str) -> bool:
        """Validate a configuration file without caching the result."""
        try:
            raw_config = self.load_config(config_name)
            AnalyticsConfig(**raw_config)
            return True
        except Exception as e:
            logger.error(f"Configuration validation failed for {config_name}: {e}")
            return False


# Global configuration loader instance
config_loader = ConfigLoader()


def get_config(config_path: str = None) -> Union[AnalyticsConfig, Any]:
    """Get configuration value using dot notation with type safety.

    Args:
        config_path: Dot-separated path to configuration value (e.g., "features.toxicity.threshold")
                     If None, returns the full validated AnalyticsConfig object.

    Returns:
        Configuration value or None if not found. If config_path is None, returns AnalyticsConfig.
    """
    if config_path is None:
        return config_loader.load_validated_config("analytics")

    # Split the path and navigate through the config
    parts = config_path.split(".")
    config = config_loader.load_validated_config("analytics")

    for part in parts:
        if hasattr(config, part):
            config = getattr(config, part)
        elif isinstance(config, dict) and part in config:
            config = config[part]
        else:
            logger.warning(f"Configuration path '{config_path}' not found")
            return None

    return config


def get_study_config(study_id: str) -> StudyConfig:
    """Get validated configuration for a specific study."""
    return config_loader.get_study_config(study_id)


def get_feature_config(feature_type: str) -> FeatureConfig:
    """Get validated configuration for a specific feature type."""
    return config_loader.get_feature_config(feature_type)


def get_default_config() -> Dict[str, Any]:
    """Get default configuration values."""
    return config_loader.get_default_config()


def get_study_week_dates(study_id: str) -> Dict[str, list]:
    """Get week start and end dates for a specific study."""
    return config_loader.get_study_week_dates(study_id)


def get_feature_columns(feature_type: str) -> list:
    """Get column names for a specific feature type."""
    return config_loader.get_feature_columns(feature_type)


def get_feature_threshold(feature_type: str) -> float:
    """Get threshold value for a specific feature type."""
    return config_loader.get_feature_threshold(feature_type)


def validate_config() -> bool:
    """Validate the analytics configuration file."""
    return config_loader.validate_config_file("analytics")
