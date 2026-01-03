from services.rank_score_feeds.models import FeedGenerationSessionAnalytics, StoredFeedModel
from services.rank_score_feeds.storage.base import FeedStorageAdapter


class FeedStorageRepository:
    """Repository for feed storage operations."""
    def __init__(self, adapter: FeedStorageAdapter):
        if not isinstance(adapter, FeedStorageAdapter):
            raise ValueError("Adapter must be a subclass of FeedStorageAdapter.")
        self.adapter = adapter

    def write_feeds(
        self,
        feeds: list[StoredFeedModel],
        partition_date: str,
        timestamp: str
    ) -> None:
        """Write feeds to storage."""
        self.adapter.write_feeds(
            feeds=feeds,
            partition_date=partition_date,
            timestamp=timestamp,
        )
    
    def write_feed_generation_session_analytics(
        self,
        feed_generation_session_analytics: FeedGenerationSessionAnalytics,
        timestamp: str
    ) -> None:
        """Write feed generation session analytics to storage."""
        self.adapter.write_feed_generation_session_analytics(
            feed_generation_session_analytics=feed_generation_session_analytics,
            timestamp=timestamp,
        )
