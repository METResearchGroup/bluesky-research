"""Models for the pipeline invocation module."""

from typing import Literal

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
