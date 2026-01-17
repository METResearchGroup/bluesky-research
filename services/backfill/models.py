from pydantic import BaseModel, Field, field_validator, model_validator

from enum import Enum


class BackfillPeriod(Enum):
    DAYS = "days"
    HOURS = "hours"


class PostScope(str, Enum):
    """Scope for selecting posts to load."""

    ALL_POSTS = "posts"  # all posts in the database
    FEED_POSTS = "posts_used_in_feeds"  # just posts used in feeds


class EnqueueServicePayload(BaseModel):
    """Payload for enqueuing records to integration queues.

    Validates:
    - record_type must be a valid PostScope
    - integrations list cannot be empty
    - start_date and end_date are required
    - start_date must be before end_date
    """

    record_type: str
    integrations: list[str] = Field(min_length=1)
    start_date: str
    end_date: str

    @field_validator("record_type")
    @classmethod
    def validate_record_type(cls, v: str) -> str:
        """Validate that record_type is a valid PostScope."""
        try:
            PostScope(v)
        except ValueError:
            raise ValueError(
                f"Invalid record type: {v}. "
                f"Must be one of: {[scope.value for scope in PostScope]}"
            )
        return v

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_dates_exist(cls, v: str) -> str:
        """Validate that dates are provided (not None or empty)."""
        if not v:
            raise ValueError("Start and end date are required")
        return v

    @model_validator(mode="after")
    def validate_date_range(self) -> "EnqueueServicePayload":
        """Validate that start_date is before end_date."""
        if self.start_date >= self.end_date:
            raise ValueError(
                f"Start date ({self.start_date}) must be before end date ({self.end_date})"
            )
        return self


class IntegrationRunnerConfigurationPayload(BaseModel):
    integration_name: str
    backfill_period: BackfillPeriod
    backfill_duration: int | None = Field(default=None)


class IntegrationRunnerServicePayload(BaseModel):
    integration_configs: list[IntegrationRunnerConfigurationPayload]


class PostToEnqueueModel(BaseModel):
    """A model for a post to be enqueued."""

    uri: str
    text: str
    preprocessing_timestamp: str


class PostUsedInFeedModel(BaseModel):
    """A bare-bones model for a post used in a feed."""

    uri: str
