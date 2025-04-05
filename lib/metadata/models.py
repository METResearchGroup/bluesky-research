"""Models for managing generic session metadata."""

from typing import Optional

from pydantic import BaseModel


class RunExecutionMetadata(BaseModel):
    """Class for defining the run execution metadata once a service is run and
    the run metadata is written to DynamoDB."""

    service: str
    timestamp: Optional[str]
    status_code: Optional[int]
    body: Optional[str]
    metadata_table_name: Optional[str]
    metadata: Optional[str]
