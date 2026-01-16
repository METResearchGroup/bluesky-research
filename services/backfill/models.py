from pydantic import BaseModel, Field

from enum import Enum


class BackfillPeriod(Enum):
    DAYS = "days"
    HOURS = "hours"


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
