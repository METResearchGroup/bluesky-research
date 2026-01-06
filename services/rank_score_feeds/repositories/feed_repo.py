from services.rank_score_feeds.models import (
    FeedGenerationSessionAnalytics,
    StoredFeedModel,
)
from services.rank_score_feeds.storage.base import FeedStorageAdapter


class FeedStorageRepository:
    """Repository for feed storage operations."""

    def __init__(self, adapter: FeedStorageAdapter):
        """Initialize feed storage repository."""
        from lib.log.logger import get_logger

        self.logger = get_logger(__name__)
        if not isinstance(adapter, FeedStorageAdapter):
            raise ValueError("Adapter must be an instance of FeedStorageAdapter.")
        self.adapter = adapter

    def write_feeds(self, feeds: list[StoredFeedModel], timestamp: str) -> None:
        """Write feeds to storage."""
        self.adapter.write_feeds(feeds=feeds, timestamp=timestamp)

    def write_feed_generation_session_analytics(
        self,
        feed_generation_session_analytics: FeedGenerationSessionAnalytics,
        timestamp: str,
    ) -> None:
        """Write feed generation session analytics to storage."""
        self.adapter.write_feed_generation_session_analytics(
            feed_generation_session_analytics=feed_generation_session_analytics,
            timestamp=timestamp,
        )
