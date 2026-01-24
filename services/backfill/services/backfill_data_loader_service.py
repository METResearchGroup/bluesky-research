from collections.abc import Callable

from services.backfill.models import PostScope, PostToEnqueueModel
from services.backfill.repositories.adapters import LocalStorageAdapter
from services.backfill.repositories.repository import BackfillDataRepository

DEFAULT_POST_ID_FIELD = "uri"


class BackfillDataLoaderService:
    """Service for loading data for backfilling."""

    def __init__(self, data_repository: BackfillDataRepository | None = None):
        """Initialize service with optional data repository.

        Args:
            data_repository: Repository for loading data. Defaults to BackfillDataRepository
                with LocalStorageAdapter if not provided (for backward compatibility
                with existing EnqueueService usage).
        """
        self.data_repository = data_repository or BackfillDataRepository(
            adapter=LocalStorageAdapter()
        )
        # Strategy mapping: PostScope -> loading method
        # Methods have signature: (start_date: str, end_date: str) -> list[PostToEnqueueModel]
        self._post_scope_loaders: dict[
            PostScope, Callable[..., list[PostToEnqueueModel]]
        ] = {
            PostScope.ALL_POSTS: self._load_all_posts,
            PostScope.FEED_POSTS: self._load_feed_posts,
        }

    def load_posts_to_enqueue(
        self,
        post_scope: PostScope,
        integration_name: str,
        start_date: str,
        end_date: str,
    ) -> list[PostToEnqueueModel]:
        """Load posts for a scope and remove ones already labeled by an integration."""
        posts = self.load_posts_by_scope(
            post_scope=post_scope,
            start_date=start_date,
            end_date=end_date,
        )
        return self.filter_out_previously_classified_posts(
            posts=posts,
            integration_name=integration_name,
            start_date=start_date,
            end_date=end_date,
        )

    def load_posts_by_scope(
        self,
        post_scope: PostScope,
        start_date: str,
        end_date: str,
    ) -> list[PostToEnqueueModel]:
        """Load base posts for a given scope (no integration-specific filtering)."""
        return self._load_posts(
            post_scope=post_scope,
            start_date=start_date,
            end_date=end_date,
        )

    def filter_out_previously_classified_posts(
        self,
        posts: list[PostToEnqueueModel],
        integration_name: str,
        start_date: str,
        end_date: str,
    ) -> list[PostToEnqueueModel]:
        """Filter out posts already classified by the integration."""
        return self._filter_posts(
            posts=posts,
            integration_name=integration_name,
            start_date=start_date,
            end_date=end_date,
        )

    def _load_posts(
        self,
        post_scope: PostScope,
        start_date: str,
        end_date: str,
    ) -> list[PostToEnqueueModel]:
        """Load posts based on post scope using strategy pattern.

        Args:
            post_scope: Scope for selecting posts (all_posts or feed_posts)
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            list[PostToEnqueueModel]: List of posts matching the scope

        Raises:
            ValueError: If post_scope is not supported
        """
        loader = self._post_scope_loaders.get(post_scope)
        if loader is None:
            raise ValueError(
                f"Invalid post scope: {post_scope}. "
                f"Supported scopes: {list(self._post_scope_loaders.keys())}"
            )
        return loader(start_date=start_date, end_date=end_date)

    def _load_all_posts(
        self, start_date: str, end_date: str
    ) -> list[PostToEnqueueModel]:
        return self.data_repository.load_all_posts(
            start_date=start_date,
            end_date=end_date,
        )

    def _load_feed_posts(
        self,
        start_date: str,
        end_date: str,
    ) -> list[PostToEnqueueModel]:
        return self.data_repository.load_feed_posts(
            start_date=start_date,
            end_date=end_date,
        )

    def _filter_posts(
        self,
        posts: list[PostToEnqueueModel],
        integration_name: str,
        start_date: str,
        end_date: str,
    ) -> list[PostToEnqueueModel]:
        """Filter out posts that have already been classified by the integration.

        Args:
            posts: List of posts to filter
            integration_name: Name of the integration (used to query classified posts)
            start_date: Start date for querying classified posts
            end_date: End date for querying classified posts

        Returns:
            List of posts that haven't been classified yet.
        """
        return self._remove_previously_classified_posts(
            posts=posts,
            integration_name=integration_name,
            start_date=start_date,
            end_date=end_date,
        )

    def _remove_previously_classified_posts(
        self,
        posts: list[PostToEnqueueModel],
        integration_name: str,
        start_date: str,
        end_date: str,
    ) -> list[PostToEnqueueModel]:
        classified_post_uris = self.data_repository.get_previously_labeled_post_uris(
            service=integration_name,
            id_field=DEFAULT_POST_ID_FIELD,
            start_date=start_date,
            end_date=end_date,
        )
        return [post for post in posts if post.uri not in classified_post_uris]
