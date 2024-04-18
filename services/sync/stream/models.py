from pydantic import BaseModel
from typing import Optional

class RawPost(BaseModel):
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

