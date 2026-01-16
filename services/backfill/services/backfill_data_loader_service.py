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

    def load_posts_to_enqueue(
        self,
        post_scope: PostScope,
        integration_name: str,
        start_date: str,
        end_date: str,
    ) -> list[PostToEnqueueModel]:
        posts: list[PostToEnqueueModel] = self._load_posts(
            post_scope=post_scope,
            start_date=start_date,
            end_date=end_date,
        )
        filtered_posts: list[PostToEnqueueModel] = self._filter_posts(
            posts=posts,
            integration_name=integration_name,
            start_date=start_date,
            end_date=end_date,
        )
        return filtered_posts

    def _load_posts(
        self,
        post_scope: PostScope,
        start_date: str,
        end_date: str,
    ) -> list[PostToEnqueueModel]:
        if post_scope == PostScope.ALL_POSTS:
            return self._load_all_posts(start_date=start_date, end_date=end_date)
        elif post_scope == PostScope.FEED_POSTS:
            return self._load_feed_posts(start_date=start_date, end_date=end_date)
        else:
            raise ValueError(f"Invalid post scope: {post_scope}") from ValueError(
                f"Invalid post scope: {post_scope}"
            )

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
