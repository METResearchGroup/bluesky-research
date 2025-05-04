from pydantic import BaseModel
from typing import Optional


class SourceConfig(BaseModel):
    """Source configuration for the backfill. Location of the source DIDs."""

    type: str
    path: str
    metadata: str


class PlcStorageConfig(BaseModel):
    """PLC storage configuration for the backfill. Location of the results
    stored from querying the PLC endpoint with the DIDs in order to get the
    PDS endpoint information for the DIDs.
    """

    type: str
    path: str
    metadata: str


class TimeRangeConfig(BaseModel):
    """Time range to get records from for each DID when querying the PDS endpoint."""

    start_date: str
    end_date: Optional[str]


class SyncStorageConfig(BaseModel):
    """Storage configuration for the backfill. Location of the results
    stored from querying the PDS endpoint with the DIDs in order to get the
    records. By default, should be stored to Parquet, in which the other
    information is optional (since `manage_local_data.py` will manage the
    Parquet files itself).

    If the backfill is being run again, and the backfill was previously
    completed, then the `min_timestamp` can be provided to resume the backfill
    from the last timestamp.
    """

    type: str
    path: str
    metadata: str
    min_timestamp: Optional[str]


class BackfillConfigSchema(BaseModel):
    """Backfill configuration schema."""

    name: str
    version: str
    source: SourceConfig
    plc_storage: PlcStorageConfig
    record_types: list[str]
    time_range: TimeRangeConfig
    sync_storage: SyncStorageConfig
