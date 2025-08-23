"""Configuration validation utilities."""

from typing import Any, Dict, List
from pydantic import BaseModel, Field, validator

from lib.log.logger import get_logger

logger = get_logger(__name__)


class StudyConfig(BaseModel):
    """Validation model for study configuration."""

    start_date: str
    end_date: str
    week_start_dates: List[str]
    week_end_dates: List[str]

    @validator("start_date", "end_date")
    def validate_date_format(cls, v):
        """Validate YYYY-MM-DD format."""
        from datetime import datetime

        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")

    @validator("week_start_dates", "week_end_dates")
    def validate_week_dates(cls, v):
        """Validate week dates are in correct format."""
        from datetime import datetime

        for date_str in v:
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Week date must be in YYYY-MM-DD format: {date_str}")
        return v


class FeatureConfig(BaseModel):
    """Validation model for feature configuration."""

    columns: List[str]
    threshold: float = Field(ge=0.0, le=1.0)


class AnalyticsConfig(BaseModel):
    """Validation model for main analytics configuration."""

    defaults: Dict[str, Any]
    studies: Dict[str, StudyConfig]
    features: Dict[str, FeatureConfig]
    data_loading: Dict[str, Any]


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate a configuration dictionary."""
    try:
        AnalyticsConfig(**config)
        logger.info("Configuration validation passed")
        return True
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        return False


def validate_study_config(study_id: str, config: Dict[str, Any]) -> bool:
    """Validate a specific study configuration."""
    try:
        StudyConfig(**config)
        logger.info(f"Study configuration validation passed for {study_id}")
        return True
    except Exception as e:
        logger.error(f"Study configuration validation failed for {study_id}: {e}")
        return False


def validate_feature_config(feature_type: str, config: Dict[str, Any]) -> bool:
    """Validate a specific feature configuration."""
    try:
        FeatureConfig(**config)
        logger.info(f"Feature configuration validation passed for {feature_type}")
        return True
    except Exception as e:
        logger.error(f"Feature configuration validation failed for {feature_type}: {e}")
        return False
