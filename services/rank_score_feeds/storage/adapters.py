"""Concrete feed storage adapter implementations."""

from services.rank_score_feeds.models import FeedGenerationSessionAnalytics, StoredFeedModel
from services.rank_score_feeds.storage.base import FeedStorageAdapter

class S3FeedStorageAdapter(FeedStorageAdapter):
    """S3 feed storage adapter implementation."""
    def __init__(self):
        """Initialize S3 feed storage adapter."""
        pass

    def write_feeds(
        self,
        feeds: list[StoredFeedModel],
        partition_date: str,
        timestamp: str
    ) -> None:
        """Write feeds to S3."""
        pass

    def write_feed_generation_session_analytics(
        self,
        feed_generation_session_analytics: FeedGenerationSessionAnalytics,
        timestamp: str
    ) -> None:
        """Write feed generation session analytics to S3."""
        pass

class LocalFeedStorageAdapter(FeedStorageAdapter):
    """Local feed storage adapter implementation."""
    def __init__(self):
        """Initialize local feed storage adapter."""
        pass

    def write_feeds(
        self,
        feeds: list[StoredFeedModel],
        partition_date: str,
        timestamp: str
    ) -> None:
        """Write feeds to local filesystem."""
        pass

    def write_feed_generation_session_analytics(
        self,
        feed_generation_session_analytics: FeedGenerationSessionAnalytics,
        timestamp: str
    ) -> None:
        """Write feed generation session analytics to local filesystem."""
        pass
