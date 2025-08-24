"""Pydantic models for analytics configuration validation and structure."""

from datetime import date
from typing import Dict, List
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict

from lib.log.logger import get_logger

logger = get_logger(__name__)


class WeekConfig(BaseModel):
    """Validation model for individual week configuration."""

    number: int
    start: date
    end: date

    @field_validator("number")
    @classmethod
    def validate_week_number(cls, v: int) -> int:
        """Validate week number is between 1 and 8."""
        if not 1 <= v <= 8:
            raise ValueError("Week number must be between 1 and 8")
        return v

    @model_validator(mode="after")
    def validate_week_dates(self) -> "WeekConfig":
        """Validate that start date is before end date."""
        if self.start >= self.end:
            raise ValueError("Start date must be before end date")
        return self

    model_config = ConfigDict(
        json_encoders={date: lambda v: v.strftime("%Y-%m-%d")},
        str_strip_whitespace=True,
    )


class StudyConfig(BaseModel):
    """Validation model for study configuration."""

    start_date: date
    end_date: date
    weeks: List[WeekConfig]

    @model_validator(mode="after")
    def validate_study_dates(self) -> "StudyConfig":
        """Validate study start/end dates and weeks alignment."""
        if self.start_date >= self.end_date:
            raise ValueError("Study start date must be before end date")

        if len(self.weeks) != 8:
            raise ValueError("Study must have exactly 8 weeks")

        # Check week numbers are sequential starting from 1
        week_numbers = [week.number for week in self.weeks]
        if week_numbers != list(range(1, 9)):
            raise ValueError("Week numbers must be sequential from 1 to 8")

        # Validate first week starts on study start date
        if self.weeks[0].start != self.start_date:
            raise ValueError("First week must start on study start date")

        # Validate last week ends on study end date
        if self.weeks[-1].end != self.end_date:
            raise ValueError("Last week must end on study end date")

        return self

    model_config = ConfigDict(
        json_encoders={date: lambda v: v.strftime("%Y-%m-%d")},
        str_strip_whitespace=True,
    )


class FeatureConfig(BaseModel):
    """Validation model for feature configuration."""

    columns: List[str] = Field(..., min_length=1)
    threshold: float = Field(
        ..., ge=0.0, le=1.0, description="Threshold value between 0 and 1"
    )

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, v: List[str]) -> List[str]:
        """Validate that columns list is not empty and contains valid strings."""
        if not v:
            raise ValueError("Columns list cannot be empty")
        if not all(isinstance(col, str) and col.strip() for col in v):
            raise ValueError("All columns must be non-empty strings")
        return v

    model_config = ConfigDict(str_strip_whitespace=True)


class DataLoadingConfig(BaseModel):
    """Validation model for data loading configuration."""

    default_columns: List[str] = Field(..., min_length=1)
    invalid_author_sources: List[str] = Field(default_factory=list)

    @field_validator("default_columns")
    @classmethod
    def validate_default_columns(cls, v: List[str]) -> List[str]:
        """Validate that default columns list is not empty."""
        if not v:
            raise ValueError("Default columns list cannot be empty")
        return v

    model_config = ConfigDict(str_strip_whitespace=True)


class DefaultsConfig(BaseModel):
    """Validation model for default configuration values."""

    lookback_days: int = Field(..., gt=0, description="Number of days to look back")
    label_threshold: float = Field(
        ..., ge=0.0, le=1.0, description="Default threshold for labeling"
    )
    exclude_partition_dates: List[str] = Field(
        default_factory=list, description="Dates to exclude from partitioning"
    )

    @field_validator("exclude_partition_dates")
    @classmethod
    def validate_exclude_dates(cls, v: List[str]) -> List[str]:
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

    model_config = ConfigDict(str_strip_whitespace=True)


class AnalyticsConfig(BaseModel):
    """Main validation model for analytics configuration."""

    defaults: DefaultsConfig
    studies: Dict[str, StudyConfig] = Field(..., min_length=1)
    features: Dict[str, FeatureConfig] = Field(..., min_length=1)
    data_loading: DataLoadingConfig

    @field_validator("studies")
    @classmethod
    def validate_studies(cls, v: Dict[str, StudyConfig]) -> Dict[str, StudyConfig]:
        """Validate that studies dictionary is not empty."""
        if not v:
            raise ValueError("Studies configuration cannot be empty")
        return v

    @field_validator("features")
    @classmethod
    def validate_features(cls, v: Dict[str, FeatureConfig]) -> Dict[str, FeatureConfig]:
        """Validate that features dictionary is not empty."""
        if not v:
            raise ValueError("Features configuration cannot be empty")
        return v

    model_config = ConfigDict(
        # Allow extra fields for backward compatibility
        extra="allow",
        # Use enum values for validation
        use_enum_values=True,
        str_strip_whitespace=True,
    )
