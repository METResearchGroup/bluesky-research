from services.backfill.models import PostToEnqueueModel


class BackfillDataLoaderService:
    """Service for loading data for backfilling."""

    def load_posts_to_enqueue(
        self,
        integration_name: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[PostToEnqueueModel]:
        posts: list[PostToEnqueueModel] = self._load_posts(
            integration_name=integration_name,
            start_date=start_date,
            end_date=end_date,
        )
        filtered_posts: list[PostToEnqueueModel] = self._filter_posts(posts=posts)
        return filtered_posts

    def _load_posts(
        self,
        integration_name: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[PostToEnqueueModel]:
        return []

    def _filter_posts(
        self, posts: list[PostToEnqueueModel]
    ) -> list[PostToEnqueueModel]:
        # TODO: e.g., filter out posts that have been classified by that integration.
        return []
