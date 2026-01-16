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


# TODO: remove the run_classification, because why are
# we adding it if we're not running classification. Need
# to make sure that other callers though don't use it.
class IntegrationRunnerConfigurationPayload(BaseModel):
    integration_name: str
    backfill_period: BackfillPeriod
    backfill_duration: int | None = Field(default=None)
    run_classification: bool = True


# TODO: I should just create a single payload that
# couples the integration name with their configurations,
class IntegrationRunnerServicePayload(BaseModel):
    integration_configs: list[IntegrationRunnerConfigurationPayload]
