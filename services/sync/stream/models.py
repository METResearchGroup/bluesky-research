"""Pydantic models for our raw data."""

from pydantic import BaseModel, validator
import re
from typing import Optional


class RawPostModel(BaseModel):
    uri: str = "Unique identifier for the post."
    created_at: str = "Timestamp of when the post was created."
    text: str = "The main text content of the post."
    embed: Optional[str] = "The embedded content of the post, if any."
    langs: Optional[str] = "The language of the post, if provided."
    entities: Optional[str] = "The entities mentioned in the post, if any."
    facets: Optional[str] = "The facets of the post, if any."
    labels: Optional[str] = "The labels of the post, if any."
    reply: Optional[str] = "The reply to the post, if any."
    reply_parent: Optional[str] = "The parent of the reply, if any."
    reply_root: Optional[str] = "The root of the reply, if any."
    tags: Optional[str] = "The tags of the post, if any."
    py_type: str = "The type of the post."
    cid: str = "The unique identifier of the post."
    author: str = "The author of the post."
    synctimestamp: str = "The timestamp of when the post was synchronized."

    @validator('author')
    def validate_author(cls, v):
        if not v.startswith("did:"):
            raise ValueError("Author must start with 'did:'")
        return v

    @validator('uri')
    def validate_uri(cls, v):
        if not v.startswith("at://did:plc:"):
            raise ValueError("URI must start with 'at://did:plc:'")
        return v

    @validator('synctimestamp')
    def validate_synctimestamp(cls, v):
        if not re.match(r'^\d{4}-\d{2}-\d{2}-\d{2}:\d{2}:\d{2}$', v):
            raise ValueError("synctimestamp must be in 'YYYY-MM-DD-HH:MM:SS' format (e.g., '2024-04-23-04:41:17')")  # noqa
        return v
