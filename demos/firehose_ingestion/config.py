"""Configuration settings for the Bluesky firehose ingestion demo."""

from typing import List, Dict
from pydantic import BaseSettings

class Settings(BaseSettings):
    """Configuration settings for the firehose ingestion."""
    
    # Record types to track
    RECORD_TYPES: List[str] = [
        "app.bsky.feed.post",
        "app.bsky.feed.like",
        "app.bsky.graph.follow",
        "app.bsky.feed.repost"
    ]

    # Mapping of record types to queue names
    RECORD_TYPE_DIRS: Dict[str, str] = {
        "app.bsky.feed.post": "posts",
        "app.bsky.feed.like": "likes",
        "app.bsky.graph.follow": "follows",
        "app.bsky.feed.repost": "reposts"
    }

    # Batch size for writing records
    BATCH_SIZE: int = 1000

    class Config:
        """Pydantic configuration."""
        case_sensitive = True

# Create a global settings instance
settings = Settings() 