"""Configuration settings for the Bluesky firehose ingestion demo."""

import os
from pathlib import Path
from typing import List, Dict
from pydantic import BaseSettings

# Import the root data directory from the project constants
import sys
sys.path.append("../../")
from lib.constants import root_local_data_directory

class Settings(BaseSettings):
    """Configuration settings for the firehose ingestion."""
    
    # Record types to track
    RECORD_TYPES: List[str] = [
        "app.bsky.feed.post",
        "app.bsky.feed.like",
        "app.bsky.graph.follow",
        "app.bsky.feed.repost"
    ]

    # Mapping of record types to directory names
    RECORD_TYPE_DIRS: Dict[str, str] = {
        "app.bsky.feed.post": "posts",
        "app.bsky.feed.like": "likes",
        "app.bsky.graph.follow": "follows",
        "app.bsky.feed.repost": "reposts"
    }

    # Batch size for writing records
    BATCH_SIZE: int = 1000

    # Base directory for storing data
    BASE_DIR: Path = Path(root_local_data_directory) / "demo_firehose"

    # Create directories if they don't exist
    def create_directories(self) -> None:
        """Create the necessary directories for storing records."""
        self.BASE_DIR.mkdir(parents=True, exist_ok=True)
        for dir_name in self.RECORD_TYPE_DIRS.values():
            (self.BASE_DIR / dir_name).mkdir(parents=True, exist_ok=True)

    class Config:
        """Pydantic configuration."""
        case_sensitive = True

# Create a global settings instance
settings = Settings() 