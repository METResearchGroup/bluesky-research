"""Concrete feed storage adapter implementations."""

import os

import pandas as pd

from lib.helper import get_partition_date_from_timestamp
from services.rank_score_feeds.models import (
    FeedGenerationSessionAnalytics,
    StoredFeedModel,
)
from services.rank_score_feeds.storage.base import FeedStorageAdapter


class S3FeedStorageAdapter(FeedStorageAdapter):
    """S3 feed storage adapter implementation."""

    def __init__(self):
        """Initialize S3 feed storage adapter."""
        from lib.aws.s3 import S3

        self.s3 = S3()
        self.feeds_root_s3_key = "custom_feeds"
        self.feed_analytics_root_s3_key = "feed_analytics"

    def write_feeds(self, feeds: list[StoredFeedModel], timestamp: str) -> None:
        """Write feeds to S3."""
        feeds_data: list[dict] = [feed.model_dump() for feed in feeds]
        s3_key: str = self._generate_feeds_s3_key(timestamp)
        self.s3.write_dicts_jsonl_to_s3(
            data=feeds_data,
            key=s3_key,
        )

    def _generate_feeds_s3_key(self, timestamp: str) -> str:
        partition_date: str = get_partition_date_from_timestamp(timestamp=timestamp)
        return os.path.join(
            self.feeds_root_s3_key,
            "active",
            partition_date,
            f"custom_feeds_{timestamp}.jsonl",
        )

    def write_feed_generation_session_analytics(
        self,
        feed_generation_session_analytics: FeedGenerationSessionAnalytics,
        timestamp: str,
    ) -> None:
        """Write feed generation session analytics to S3."""
        feed_analytics_data: dict = feed_generation_session_analytics.model_dump()
        s3_key: str = self._generate_feed_analytics_s3_key(timestamp)
        self.s3.write_dicts_jsonl_to_s3(
            data=[feed_analytics_data],
            key=s3_key,
        )

    def _generate_feed_analytics_s3_key(self, timestamp: str) -> str:
        partition_date: str = get_partition_date_from_timestamp(timestamp=timestamp)
        return os.path.join(
            self.feed_analytics_root_s3_key,
            "active",
            partition_date,
            f"feed_analytics_{timestamp}.jsonl",
        )


class LocalFeedStorageAdapter(FeedStorageAdapter):
    """Local feed storage adapter implementation."""

    def __init__(self):
        """Initialize local feed storage adapter."""
        # choosing to import here as the S3 adapter doesn't need to know
        # about these imports, so we keep these as class-level imports.
        from lib.db.manage_local_data import export_data_to_local_storage
        from lib.db.service_constants import MAP_SERVICE_TO_METADATA

        self.custom_feeds_dtype_map = MAP_SERVICE_TO_METADATA["custom_feeds"][
            "dtypes_map"
        ]
        self.feed_analytics_dtype_map = MAP_SERVICE_TO_METADATA["feed_analytics"][
            "dtypes_map"
        ]
        self.export_func = export_data_to_local_storage

    def write_feeds(self, feeds: list[StoredFeedModel], timestamp: str) -> None:
        """Write feeds to local filesystem."""
        feeds_data: list[dict] = [feed.model_dump() for feed in feeds]
        df: pd.DataFrame = pd.DataFrame(feeds_data)
        df = df.astype(self.custom_feeds_dtype_map)
        self.export_func(df=df, service="custom_feeds")

    def write_feed_generation_session_analytics(
        self,
        feed_generation_session_analytics: FeedGenerationSessionAnalytics,
        timestamp: str,
    ) -> None:
        """Write feed generation session analytics to local filesystem."""
        feed_analytics_data: dict = feed_generation_session_analytics.model_dump()
        df: pd.DataFrame = pd.DataFrame([feed_analytics_data])
        df = df.astype(self.feed_analytics_dtype_map)
        self.export_func(df=df, service="feed_analytics")
