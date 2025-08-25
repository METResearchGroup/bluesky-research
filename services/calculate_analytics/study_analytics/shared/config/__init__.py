"""Configuration management for analytics system with Pydantic validation."""

from .loader import (
    ConfigLoader,
    config_loader,
    get_config,
    get_study_config,
    get_feature_config,
    get_default_config,
    get_study_week_dates,
    get_feature_columns,
    get_feature_threshold,
    validate_config,
)
from .models import (
    AnalyticsConfig,
    StudyConfig,
    FeatureConfig,
    WeekConfig,
    DataLoadingConfig,
    DefaultsConfig,
)

__all__ = [
    # Loader classes and instances
    "ConfigLoader",
    "config_loader",
    # Main configuration functions
    "get_config",
    "get_study_config",
    "get_feature_config",
    "get_default_config",
    "get_study_week_dates",
    "get_feature_columns",
    "get_feature_threshold",
    "validate_config",
    # Pydantic models
    "AnalyticsConfig",
    "StudyConfig",
    "FeatureConfig",
    "WeekConfig",
    "DataLoadingConfig",
    "DefaultsConfig",
]
