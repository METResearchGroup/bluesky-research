"""ABC base class for backfill data adapters."""

from abc import ABC, abstractmethod

from lib.constants import FEED_LOOKBACK_DAYS_DURING_STUDY, study_start_date
from lib.datetime_utils import (
    calculate_start_end_date_for_lookback,
    get_partition_dates,
)
from services.backfill.exceptions import BackfillDataAdapterError
from services.backfill.models import PostToEnqueueModel, PostUsedInFeedModel


class BackfillDataAdapter(ABC):
    """ABC base class for backfill data adapters."""

    @abstractmethod
    def load_all_posts(
        self, start_date: str, end_date: str
    ) -> list[PostToEnqueueModel]:
        """Load all posts from the data source.

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            list[PostToEnqueueModel]: List of posts.
        """
        raise NotImplementedError

    def load_feed_posts(
        self, start_date: str, end_date: str
    ) -> list[PostToEnqueueModel]:
        """Load feed posts from the data source.

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            list[PostToEnqueueModel]: List of deduplicated feed posts.

        Raises:
            BackfillDataAdapterError: If loading feed posts fails.
        """
        try:
            results: list[PostToEnqueueModel] = []
            partition_dates: list[str] = get_partition_dates(
                start_date=start_date, end_date=end_date
            )
            for partition_date in partition_dates:
                results.extend(self._load_feed_posts_for_date(partition_date))
            deduplicated_results: list[PostToEnqueueModel] = (
                self._deduplicate_feed_posts(posts=results)
            )
            return deduplicated_results
        except BackfillDataAdapterError:
            raise
        except Exception as e:
            raise BackfillDataAdapterError(f"Failed to load feed posts: {e}") from e

    @abstractmethod
    def _load_posts_used_in_feeds_for_date(
        self, partition_date: str
    ) -> list[PostUsedInFeedModel]:
        """Load the URIs of the posts used in the feeds for a given date.

        Subclasses implement storage-specific logic.

        Args:
            partition_date: Partition date in YYYY-MM-DD format

        Returns:
            list[PostUsedInFeedModel]: List of posts used in feeds.
        """
        raise NotImplementedError

    def _load_feed_posts_for_date(
        self, partition_date: str
    ) -> list[PostToEnqueueModel]:
        """Load the posts used in the feeds for a given date."""
        posts_used_in_feeds: list[PostUsedInFeedModel] = (
            self._load_posts_used_in_feeds_for_date(partition_date)
        )

        lookback_start_date, lookback_end_date = calculate_start_end_date_for_lookback(
            partition_date=partition_date,
            num_days_lookback=FEED_LOOKBACK_DAYS_DURING_STUDY,
            min_lookback_date=study_start_date,
        )

        candidate_pool_posts: list[PostToEnqueueModel] = (
            self._load_candidate_pool_posts_for_date(
                lookback_start_date=lookback_start_date,
                lookback_end_date=lookback_end_date,
            )
        )

        candidate_pool_posts_used_in_feeds: list[PostToEnqueueModel] = (
            self._get_candidate_pool_posts_used_in_feeds_for_date(
                candidate_pool_posts=candidate_pool_posts,
                posts_used_in_feeds=posts_used_in_feeds,
            )
        )

        return candidate_pool_posts_used_in_feeds

    def _load_candidate_pool_posts_for_date(
        self,
        lookback_start_date: str,
        lookback_end_date: str,
    ) -> list[PostToEnqueueModel]:
        """For feeds from a given date, load the posts that would've been
        a part of the candidate pool for that date.

        We need to reconstruct in this way because at the level of the feeds,
        we only have the post URIs, so we need to artificially reconstruct
        the candidate pool posts from the post URIs in order to have fully
        hydrated posts.
        """
        return self.load_all_posts(
            start_date=lookback_start_date,
            end_date=lookback_end_date,
        )

    def _get_candidate_pool_posts_used_in_feeds_for_date(
        self,
        candidate_pool_posts: list[PostToEnqueueModel],
        posts_used_in_feeds: list[PostUsedInFeedModel],
    ) -> list[PostToEnqueueModel]:
        """Get the candidate pool posts used in the feeds for a given date."""
        uris_of_posts_used_in_feeds: set[str] = {
            post.uri for post in posts_used_in_feeds
        }
        candidate_pool_posts_used_in_feeds: list[PostToEnqueueModel] = [
            post
            for post in candidate_pool_posts
            if post.uri in uris_of_posts_used_in_feeds
        ]
        return candidate_pool_posts_used_in_feeds

    def _deduplicate_feed_posts(
        self, posts: list[PostToEnqueueModel]
    ) -> list[PostToEnqueueModel]:
        """Deduplicate feed posts. A post can be used in feeds across multiple
        dates, so we just want to grab one version of the post.
        """
        unique_uris: set[str] = set()
        filtered_results: list[PostToEnqueueModel] = []
        for post in posts:
            if post.uri not in unique_uris:
                unique_uris.add(post.uri)
                filtered_results.append(post)
        return filtered_results

    @abstractmethod
    def get_previously_labeled_post_uris(
        self,
        service: str,
        id_field: str,
        start_date: str,
        end_date: str,
    ) -> set[str]:
        """Load post URIs for a service from the data source.

        Args:
            service: Name of the service (e.g., "ml_inference_perspective_api")
            id_field: Name of the ID field
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            set[str]: Set of post URIs. Empty set if no data found or on error.

        Raises:
            NotImplementedError: If method is not implemented by concrete adapter.
        """
        raise NotImplementedError

    @abstractmethod
    def write_records_to_storage(self, integration_name: str, records: list[dict]):
        """Write records to storage using the configured adapter.

        Args:
            integration_name: Name of the integration (e.g., "ml_inference_perspective_api")
            records: List of records to write.
        """
        raise NotImplementedError
