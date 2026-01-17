"""ABC base class for backfill data adapters."""

from abc import ABC, abstractmethod

from services.backfill.models import PostToEnqueueModel


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

    @abstractmethod
    def load_feed_posts(
        self, start_date: str, end_date: str
    ) -> list[PostToEnqueueModel]:
        """Load feed posts from the data source.

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            list[PostToEnqueueModel]: List of posts.
        """
        raise NotImplementedError

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
    def write_records_to_storage(self, service, records: list[dict]):
        """Write records to storage using the configured adapter.

        Args:
            service: Name of the service (e.g., "ml_inference_perspective_api")
            records: List of records to write.
        """
        raise NotImplementedError
