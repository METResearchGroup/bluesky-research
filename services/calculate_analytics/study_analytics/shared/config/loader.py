"""Configuration loading and management utilities."""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from lib.log.logger import get_logger

logger = get_logger(__name__)


class ConfigLoader:
    """Loads and validates configuration from YAML files."""

    def __init__(self, config_dir: Optional[str] = None):
        if config_dir is None:
            config_dir = os.path.join(os.path.dirname(__file__), ".")
        self.config_dir = Path(config_dir)
        self._config_cache: Dict[str, Any] = {}

    def load_config(self, config_name: str) -> Dict[str, Any]:
        """Load a configuration file by name."""
        if config_name in self._config_cache:
            return self._config_cache[config_name]

        config_path = self.config_dir / f"{config_name}.yaml"
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            self._config_cache[config_name] = config
            logger.info(f"Loaded configuration: {config_name}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration {config_name}: {e}")
            raise

    def get_study_config(self, study_id: str) -> Dict[str, Any]:
        """Get configuration for a specific study."""
        studies_config = self.load_config("analytics")
        return studies_config["studies"][study_id]

    def get_feature_config(self, feature_type: str) -> Dict[str, Any]:
        """Get configuration for a specific feature type."""
        analytics_config = self.load_config("analytics")
        return analytics_config["features"][feature_type]

    def get_default_config(self) -> Dict[str, Any]:
        """Get default configuration values."""
        analytics_config = self.load_config("analytics")
        return analytics_config["defaults"]

    def get_study_week_dates(self, study_id: str) -> Dict[str, list]:
        """Get week start and end dates for a specific study."""
        study_config = self.get_study_config(study_id)
        weeks = study_config.get("weeks", [])

        return {
            "week_start_dates": [week["start"] for week in weeks],
            "week_end_dates": [week["end"] for week in weeks],
        }

    def get_feature_columns(self, feature_type: str) -> list:
        """Get column names for a specific feature type."""
        feature_config = self.get_feature_config(feature_type)
        return feature_config["columns"]

    def get_feature_threshold(self, feature_type: str) -> float:
        """Get threshold value for a specific feature type."""
        feature_config = self.get_feature_config(feature_type)
        return feature_config["threshold"]


# Global configuration loader instance
config_loader = ConfigLoader()
