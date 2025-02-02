from pydantic import BaseModel, Field


class PostInFeedModel(BaseModel):
    """Post used in a feed.

    Denotes when a post was used in a feed.
    """

    uri: str = Field(..., description="The URI of the post.")
    partition_date: str = Field(..., description="The partition date of the feed.")
