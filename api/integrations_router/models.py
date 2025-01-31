"""Models for the integrations router."""

from typing import Literal, Optional

from pydantic import BaseModel


class IntegrationRequest(BaseModel):
    """Class for defining the expected inputs in an integration request."""

    service: str
    payload: dict
    metadata: dict


class IntegrationPayload(BaseModel):
    """Class for defining the expected inputs in an integration request payload."""

    run_type: Literal["prod", "backfill"]
    payload: dict
    metadata: dict


class RunExecutionMetadata(BaseModel):
    """Class for defining the run execution metadata once a service is run and
    the run metadata is written to DynamoDB."""

    service: str
    timestamp: Optional[str]
    status_code: Optional[int]
    body: Optional[str]
    metadata_table_name: Optional[str]
    metadata: Optional[str]
