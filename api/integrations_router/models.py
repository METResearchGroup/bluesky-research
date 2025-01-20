"""Models for the integrations router."""

from typing import Literal

from pydantic import BaseModel


class IntegrationRequest(BaseModel):
    """Class for defining the expected inputs in an integration request."""

    service: str
    payload: dict
    metadata: dict


class IntegrationResponse(BaseModel):
    """Class for defining the expected outputs in an integration response."""

    pass


class IntegrationPayload(BaseModel):
    """Class for defining the expected inputs in an integration request payload."""

    run_type: Literal["prod", "backfill"]
    payload: dict
    metadata: dict


class RunExecutionMetadata(BaseModel):
    """Class for defining the run execution metadata once a service is run and
    the run metadata is written to DynamoDB."""

    request: IntegrationRequest
    execution_metadata: dict
