"""Pydantic models for the filtered posts."""

from pydantic import BaseModel, Field
from typing import Optional


class FilteredRawPostModel(BaseModel):
    uri: str = Field(..., description="Unique identifier for the post")
    passed_filters: bool = Field(..., description="Indicates if the post passed the filters")
    filtered_at: str = Field(..., description="Timestamp when the post was filtered")
    filtered_by_func: Optional[str] = Field(None, description="Function used to filter the post")
    created_at: str = Field(..., description="Timestamp when the post was created")
    text: str = Field(..., description="Text content of the post")
    embed: Optional[str] = Field(None, description="Embedded content in the post")
    langs: Optional[str] = Field(None, description="Languages of the post")
    entities: Optional[str] = Field(None, description="Entities mentioned in the post")
    facets: Optional[str] = Field(None, description="Facets of the post")
    labels: Optional[str] = Field(None, description="Labels assigned to the post")
    reply: Optional[str] = Field(None, description="Reply information for the post")
    reply_parent: Optional[str] = Field(None, description="Parent of the reply post")
    reply_root: Optional[str] = Field(None, description="Root of the reply chain")
    tags: Optional[str] = Field(None, description="Tags associated with the post")
    py_type: str = Field(..., description="Type of the post")
    cid: str = Field(..., description="CID of the post")
    author: str = Field(..., description="Author of the post")
    synctimestamp: str = Field(..., description="Synchronization timestamp of the post")
