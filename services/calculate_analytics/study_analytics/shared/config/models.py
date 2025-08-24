"""Pydantic models for analytics configuration validation and structure."""

from datetime import date
from typing import Dict, List
from pydantic import BaseModel, Field, validator, root_validator

from lib.log.logger import get_logger

logger = get_logger(__name__)


class WeekConfig(BaseModel):
    """Validation model for individual week configuration."""

    number: int
    start: date
    end: date

    @validator("number")
    def validate_week_number(cls, v):
        """Validate week number is between 1 and 8."""
        if not 1 <= v <= 8:
            raise ValueError("Week number must be between 1 and 8")
        return v

    @root_validator
    def validate_week_dates(cls, values):
        """Validate that start date is before end date."""
        start = values.get("start")
        end = values.get("end")
        if start and end and start >= end:
            raise ValueError("Start date must be before end date")
        return values

    class Config:
        json_encoders = {date: lambda v: v.strftime("%Y-%m-%d")}


class StudyConfig(BaseModel):
    """Validation model for study configuration."""

    start_date: date
    end_date: date
    weeks: List[WeekConfig]

    @root_validator
    def validate_study_dates(cls, values):
        """Validate study start/end dates and weeks alignment."""
        start_date = values.get("start_date")
        end_date = values.get("end_date")
        weeks = values.get("weeks")

        if start_date and end_date and start_date >= end_date:
            raise ValueError("Study start date must be before end date")

        if weeks:
            if len(weeks) != 8:
                raise ValueError("Study must have exactly 8 weeks")

            # Check week numbers are sequential starting from 1
            week_numbers = [week.number for week in weeks]
            if week_numbers != list(range(1, 9)):
                raise ValueError("Week numbers must be sequential from 1 to 8")

            # Validate first week starts on study start date
            if weeks[0].start != start_date:
                raise ValueError("First week must start on study start date")

            # Validate last week ends on study end date
            if weeks[-1].end != end_date:
                raise ValueError("Last week must end on study end date")

        return values

    class Config:
        json_encoders = {date: lambda v: v.strftime("%Y-%m-%d")}


class FeatureConfig(BaseModel):
    """Validation model for feature configuration."""

    columns: List[str] = Field(..., min_items=1)
    threshold: float = Field(
        ..., ge=0.0, le=1.0, description="Threshold value between 0 and 1"
    )

    @validator("columns")
    def validate_columns(cls, v):
        """Validate that columns list is not empty and contains valid strings."""
        if not v:
            raise ValueError("Columns list cannot be empty")
        if not all(isinstance(col, str) and col.strip() for col in v):
            raise ValueError("All columns must be non-empty strings")
        return v


class DataLoadingConfig(BaseModel):
    """Validation model for data loading configuration."""

    default_columns: List[str] = Field(..., min_items=1)
    invalid_author_sources: List[str] = Field(default_factory=list)

    @validator("default_columns")
    def validate_default_columns(cls, v):
        """Validate that default columns list is not empty."""
        if not v:
            raise ValueError("Default columns list cannot be empty")
        return v


class DefaultsConfig(BaseModel):
    """Validation model for default configuration values."""

    lookback_days: int = Field(..., gt=0, description="Number of days to look back")
    label_threshold: float = Field(
        ..., ge=0.0, le=1.0, description="Default threshold for labeling"
    )
    exclude_partition_dates: List[str] = Field(
        default_factory=list, description="Dates to exclude from partitioning"
    )

    @validator("exclude_partition_dates")
    def validate_exclude_dates(cls, v):
        """Validate date format for excluded partition dates."""
        from datetime import datetime

        for date_str in v:
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                raise ValueError(
                    f"Invalid date format in exclude_partition_dates: {date_str}"
                )
        return v


class AnalyticsConfig(BaseModel):
    """Main validation model for analytics configuration."""

    defaults: DefaultsConfig
    studies: Dict[str, StudyConfig] = Field(..., min_items=1)
    features: Dict[str, FeatureConfig] = Field(..., min_items=1)
    data_loading: DataLoadingConfig

    @validator("studies")
    def validate_studies(cls, v):
        """Validate that studies dictionary is not empty."""
        if not v:
            raise ValueError("Studies configuration cannot be empty")
        return v

    @validator("features")
    def validate_features(cls, v):
        """Validate that features dictionary is not empty."""
        if not v:
            raise ValueError("Features configuration cannot be empty")
        return v

    class Config:
        # Allow extra fields for backward compatibility
        extra = "allow"
        # Use enum values for validation
        use_enum_values = True
