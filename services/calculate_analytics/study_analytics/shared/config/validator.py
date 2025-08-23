"""Configuration validation utilities."""

from typing import Any, Dict, List
from pydantic import BaseModel, Field, validator

from lib.log.logger import get_logger

logger = get_logger(__name__)


class WeekConfig(BaseModel):
    """Validation model for individual week configuration."""

    number: int
    start: str
    end: str

    @validator("start", "end")
    def validate_date_format(cls, v):
        """Validate YYYY-MM-DD format."""
        from datetime import datetime

        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")

    @validator("number")
    def validate_week_number(cls, v):
        """Validate week number is between 1 and 8."""
        if not 1 <= v <= 8:
            raise ValueError("Week number must be between 1 and 8")
        return v


class StudyConfig(BaseModel):
    """Validation model for study configuration."""

    start_date: str
    end_date: str
    weeks: List[WeekConfig]

    @validator("start_date", "end_date")
    def validate_date_format(cls, v):
        """Validate YYYY-MM-DD format."""
        from datetime import datetime

        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")

    @validator("weeks")
    def validate_weeks_structure(cls, v):
        """Validate weeks have correct structure and numbering."""
        if len(v) != 8:
            raise ValueError("Study must have exactly 8 weeks")

        # Check week numbers are sequential starting from 1
        week_numbers = [week.number for week in v]
        if week_numbers != list(range(1, 9)):
            raise ValueError("Week numbers must be sequential from 1 to 8")

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
