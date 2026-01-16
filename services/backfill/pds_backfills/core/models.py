"""Pydantic models for backfill metadata."""

from pydantic import BaseModel, Field


class UserBackfillMetadata(BaseModel):
    """Metadata for a user backfill operation."""

    did: str = Field(..., description="The DID of the user.")
    bluesky_handle: str = Field(..., description="The Bluesky handle of the user.")
    types: str = Field(
        ..., description="Comma-separated list of record types that were backfilled."
    )
    total_records: int = Field(..., description="Total number of records backfilled.")
    total_records_by_type: str = Field(
        ...,
        description="JSON string representation of a dictionary mapping record types to counts.",
    )
    pds_service_endpoint: str = Field(
        ...,
        description="The PDS service endpoint for the user, obtained from the PLC directory.",
    )
    timestamp: str = Field(
        ..., description="Timestamp when the backfill was performed."
    )


class PlcResult(BaseModel):
    """Result from the PLC endpoint. Filtered version of the response object,
    only including the fields that are needed.
    """

    did: str = Field(..., description="The DID of the user.")
    pds_service_endpoint: str = Field(
        ...,
        description="The PDS service endpoint for the user, obtained from the PLC directory.",
    )
    pds_owner: str = Field(
        ...,
        description="The owner of the PDS. Possible values are 'bluesky' and 'not_bluesky'.",
    )
    handle: str = Field(..., description="The Bluesky handle of the user.")
