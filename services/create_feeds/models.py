"""Pydantic models for the feeds."""
from pydantic import BaseModel


class CreatedFeedModel(BaseModel):
    """Model for the created feed."""
    bluesky_user_did: str = "The Bluesky user DID."
    feed_uris: str = "The URIs of the feed. Comma-separated."
    timestamp: str = "The timestamp of the feed creation."
