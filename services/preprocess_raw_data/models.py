"""Pydantic models for the filtered posts."""

from pydantic import BaseModel, Field
from typing import Optional


class FilteredRawPostModel(BaseModel):
    uri: str = Field(..., description="Unique identifier for the post")
    passed_filters: bool = Field(..., description="Indicates if the post passed the filters") # noqa
    filtered_at: str = Field(..., description="Timestamp when the post was filtered") # noqa
    filtered_by_func: Optional[str] = Field(None, description="Function used to filter the post") # noqa
    created_at: str = Field(..., description="Timestamp when the post was created") # noqa
    text: str = Field(..., description="Text content of the post")
    embed: Optional[str] = Field(None, description="Embedded content in the post") # noqa
    langs: Optional[str] = Field(None, description="Languages of the post")
    entities: Optional[str] = Field(None, description="Entities mentioned in the post") # noqa
    facets: Optional[str] = Field(None, description="Facets of the post")
    labels: Optional[str] = Field(None, description="Labels assigned to the post") # noqa
    reply: Optional[str] = Field(None, description="Reply information for the post") # noqa
    reply_parent: Optional[str] = Field(None, description="Parent of the reply post") # noqa
    reply_root: Optional[str] = Field(None, description="Root of the reply chain") # noqa
    tags: Optional[str] = Field(None, description="Tags associated with the post") # noqa
    py_type: str = Field(..., description="Type of the post")
    cid: str = Field(..., description="CID of the post")
    author: str = Field(..., description="Author of the post")
    synctimestamp: str = Field(..., description="Synchronization timestamp of the post") # noqa
