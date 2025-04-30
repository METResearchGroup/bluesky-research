from pydantic import BaseModel
from typing import Optional


class SourceConfig(BaseModel):
    """Source configuration for the backfill."""

    type: str
    path: str
    query: Optional[str]


class TimeRangeConfig(BaseModel):
    """Time range to get records from for each DID."""

    start_date: str
    end_date: Optional[str]


class StorageConfig(BaseModel):
    """Storage configuration for the backfill."""

    type: str
    table_name: str
    metadata: str


class BackfillConfigSchema(BaseModel):
    """Backfill configuration schema."""

    name: str
    version: str
    source: SourceConfig
    record_types: list[str]
    time_range: TimeRangeConfig
    storage: StorageConfig
