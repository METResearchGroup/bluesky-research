"""Concrete feed storage adapter implementations."""

import os

import pandas as pd

from lib.helper import get_partition_date_from_timestamp
from lib.log.logger import get_logger
from services.rank_score_feeds.models import (
    StoredFeedModel,
    FeedGenerationSessionAnalytics,
)
from services.rank_score_feeds.storage.base import (
    FeedStorageAdapter,
    FeedTTLAdapter,
    SessionMetadataAdapter,
)
from services.rank_score_feeds.storage.exceptions import StorageError


class S3FeedStorageAdapter(FeedStorageAdapter):
    """S3 feed storage adapter implementation."""

    def __init__(self):
        """Initialize S3 feed storage adapter."""
        from lib.aws.s3 import S3

        self.s3 = S3()
        self.feeds_root_s3_key = "custom_feeds"
        self.feed_analytics_root_s3_key = "feed_analytics"
        self.logger = get_logger(__name__)

    def write_feeds(self, feeds: list[StoredFeedModel], timestamp: str) -> None:
        """Write feeds to S3."""
        try:
            feeds_data: list[dict] = [feed.model_dump() for feed in feeds]
            s3_key: str = self._generate_feeds_s3_key(timestamp)
            self.s3.write_dicts_jsonl_to_s3(
                data=feeds_data,
                key=s3_key,
            )
        except Exception as e:
            self.logger.error(f"Failed to write feeds to S3: {e}")
            raise StorageError(f"Failed to write feeds to S3: {e}")

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
        try:
            feed_analytics_data: dict = feed_generation_session_analytics.model_dump()
            s3_key: str = self._generate_feed_analytics_s3_key(timestamp)
            self.s3.write_dicts_jsonl_to_s3(
                data=[feed_analytics_data],
                key=s3_key,
            )
        except Exception as e:
            self.logger.error(
                f"Failed to write feed generation session analytics to S3: {e}"
            )
            raise StorageError(
                f"Failed to write feed generation session analytics to S3: {e}"
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
        self.logger = get_logger(__name__)

    def write_feeds(self, feeds: list[StoredFeedModel], timestamp: str) -> None:
        """Write feeds to local filesystem."""
        try:
            feeds_data: list[dict] = [feed.model_dump() for feed in feeds]
            df: pd.DataFrame = pd.DataFrame(feeds_data)
            df = df.astype(self.custom_feeds_dtype_map)
            self.export_func(df=df, service="custom_feeds")
        except Exception as e:
            self.logger.error(f"Failed to write feeds to local filesystem: {e}")
            raise StorageError(f"Failed to write feeds to local filesystem: {e}")

    def write_feed_generation_session_analytics(
        self,
        feed_generation_session_analytics: FeedGenerationSessionAnalytics,
        timestamp: str,
    ) -> None:
        """Write feed generation session analytics to local filesystem."""
        try:
            feed_analytics_data: dict = feed_generation_session_analytics.model_dump()
            df: pd.DataFrame = pd.DataFrame([feed_analytics_data])
            df = df.astype(self.feed_analytics_dtype_map)
            self.export_func(df=df, service="feed_analytics")
        except Exception as e:
            self.logger.error(
                f"Failed to write feed generation session analytics to local filesystem: {e}"
            )
            raise StorageError(
                f"Failed to write feed generation session analytics to local filesystem: {e}"
            )


class S3FeedTTLAdapter(FeedTTLAdapter):
    """S3 implementation of feed TTL adapter.

    Moves old feed files from S3 'active' to 'cache' directory,
    keeping only the most recent N files active.
    """

    def __init__(self):
        """Initialize S3 TTL adapter."""
        from lib.aws.s3 import S3

        self.s3 = S3()
        self.logger = get_logger(__name__)

    def move_to_cache(
        self,
        prefix: str,
        keep_count: int,
        sort_field: str = "Key",
    ) -> None:
        """Move old feeds from active to cache in S3."""
        try:
            self.logger.info(
                f"Moving old feeds to cache: prefix={prefix}, "
                f"keep_count={keep_count}, sort_field={sort_field}"
            )
            self.s3.sort_and_move_files_from_active_to_cache(
                prefix=prefix,
                keep_count=keep_count,
                sort_field=sort_field,
            )
            self.logger.info(
                f"Successfully moved old feeds to cache (kept {keep_count} most recent)"
            )
        except Exception as e:
            self.logger.error(f"Failed to move old feeds to cache: {e}")
            raise StorageError(f"Failed to move old feeds to cache: {e}")


class LocalFeedTTLAdapter(FeedTTLAdapter):
    """Local filesystem implementation of feed TTL adapter.

    For local development/testing. May be a no-op or move files
    between local directories.

    TODO: not implemented. Currently not needed, but can be useful for
    local development/testing.
    """

    def __init__(self):
        """Initialize local TTL adapter."""
        self.base_path = "./data/feeds"
        self.logger = get_logger(__name__)
        self.logger.info(
            f"LocalFeedTTLAdapter initialized (no-op for local testing). "
            f"base_path={self.base_path}"
        )

    def move_to_cache(
        self,
        prefix: str,
        keep_count: int,
        sort_field: str = "Key",
    ) -> None:
        """No-op for local filesystem (or implement local file moving)."""
        self.logger.info(
            f"Local TTL: Would move old feeds to cache "
            f"(prefix={prefix}, keep_count={keep_count})"
        )


class DynamoDBSessionMetadataAdapter(SessionMetadataAdapter):
    """DynamoDB implementation of session metadata adapter.

    Stores feed generation session metadata in DynamoDB table.
    """

    def __init__(self):
        """Initialize DynamoDB session metadata adapter."""
        from lib.aws.dynamodb import DynamoDB

        self.dynamodb = DynamoDB()
        self.table_name = "rank_score_feed_sessions"
        self.logger = get_logger(__name__)

    def insert_session_metadata(self, metadata: FeedGenerationSessionAnalytics) -> None:
        """Insert session metadata into DynamoDB."""
        self.logger.info(f"Inserting feed generation session metadata: {metadata}")
        try:
            self.dynamodb.insert_item_into_table(
                item=metadata.model_dump(),
                table_name=self.table_name,
            )
            self.logger.info(
                f"Successfully inserted feed generation session metadata "
                f"into table {self.table_name}"
            )
        except Exception as e:
            self.logger.error(f"Failed to insert feed generation session metadata: {e}")
            raise StorageError(
                f"Failed to insert feed generation session metadata: {e}"
            )
