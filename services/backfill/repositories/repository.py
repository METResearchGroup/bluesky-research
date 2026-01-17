"""Repository for backfill data loading operations using adapter pattern."""

from services.backfill.models import PostToEnqueueModel
from services.backfill.repositories.base import BackfillDataAdapter


class BackfillDataRepository:
    """Repository for backfill data loading operations.

    Uses adapter pattern to abstract over different data sources
    (local storage, S3, etc.) without changing service code.
    """

    def __init__(self, adapter: BackfillDataAdapter):
        """Initialize backfill data repository with adapter.

        Args:
            adapter: Concrete backfill data adapter (LocalStorageAdapter, S3Adapter, etc.)

        Raises:
            ValueError: If adapter is not an instance of BackfillDataAdapter.
        """
        if not isinstance(adapter, BackfillDataAdapter):
            raise ValueError("Adapter must be an instance of BackfillDataAdapter.")
        self.adapter = adapter

    def load_all_posts(
        self, start_date: str, end_date: str
    ) -> list[PostToEnqueueModel]:
        """Load all posts using the configured adapter.

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            list[PostToEnqueueModel]: List of posts.
        """
        return self.adapter.load_all_posts(start_date=start_date, end_date=end_date)

    def load_feed_posts(
        self, start_date: str, end_date: str
    ) -> list[PostToEnqueueModel]:
        """Load feed posts using the configured adapter.

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            list[PostToEnqueueModel]: List of posts.
        """
        return self.adapter.load_feed_posts(start_date=start_date, end_date=end_date)

    def get_previously_labeled_post_uris(
        self,
        service: str,
        id_field: str,
        start_date: str,
        end_date: str,
    ) -> set[str]:
        """Get previously labeled post URIs for a service using the configured adapter.

        Args:
            service: Name of the service (e.g., "ml_inference_perspective_api")
            id_field: Name of the ID field
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            set[str]: Set of post URIs. Empty set if no data found or on error.
        """
        return self.adapter.get_previously_labeled_post_uris(
            service=service,
            id_field=id_field,
            start_date=start_date,
            end_date=end_date,
        )

    def write_records_to_storage(self, service: str, records: list[dict]):
        """Write records to storage using the configured adapter.

        Args:
            service: Name of the service (e.g., "ml_inference_perspective_api")
            records: List of records to write.
        """
        return self.adapter.write_records_to_storage(service=service, records=records)
