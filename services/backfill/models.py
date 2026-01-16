from pydantic import BaseModel, Field

from enum import Enum


class BackfillPeriod(Enum):
    DAYS = "days"
    HOURS = "hours"


class PostScope(str, Enum):
    """Scope for selecting posts to load."""

    ALL_POSTS = "all_posts"  # all posts in the database
    FEED_POSTS = "feed_posts"  # just posts used in feeds


class EnqueueServicePayload(BaseModel):
    record_type: str
    integrations: list[str]
    start_date: str
    end_date: str


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
