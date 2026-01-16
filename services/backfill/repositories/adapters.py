"""Concrete backfill data adapter implementations."""

from typing import Optional

import pandas as pd

from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.backfill.models import PostToEnqueueModel
from services.backfill.repositories.base import BackfillDataAdapter

logger = get_logger(__name__)

LOCAL_TABLE_NAME = "preprocessed_posts"
REQUIRED_COLUMNS = ["uri", "text", "preprocessing_timestamp"]


class LocalStorageAdapter(BackfillDataAdapter):
    """Local storage adapter implementation.

    Loads data from local filesystem using load_data_from_local_storage.
    """

    def load_all_posts(
        self,
        start_date: str,
        end_date: str,
    ) -> list[PostToEnqueueModel]:
        """Load all posts from local storage.

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            list[PostToEnqueueModel]: List of posts.
        """
        table_columns = REQUIRED_COLUMNS.copy()
        table_columns_str = ", ".join(table_columns)
        filter = "WHERE text IS NOT NULL AND text != ''"
        query = f"SELECT {table_columns_str} FROM {LOCAL_TABLE_NAME} {filter}".strip()
        query_metadata = {
            "tables": [{"name": LOCAL_TABLE_NAME, "columns": table_columns}]
        }
        # NOTE: no need to load active_df (which we do in other parts of the
        # codebase) as our code is currently only used for backwards-looking
        # backfills (e.g., there should be nothing in active/ for us to load
        # anyways). But just a note in case it comes up in the future.
        cached_df: pd.DataFrame = load_data_from_local_storage(
            service=LOCAL_TABLE_NAME,
            directory="cache",
            export_format="duckdb",
            duckdb_query=query,
            query_metadata=query_metadata,
            start_partition_date=start_date,
            end_partition_date=end_date,
        )
        dumped_posts: list[dict] = cached_df.to_dict(orient="records")
        return [PostToEnqueueModel(**post) for post in dumped_posts]

    def get_previously_labeled_post_uris(
        self,
        service: str,
        id_field: str,
        start_date: str,
        end_date: str,
    ) -> set[str]:
        """Load post URIs for a service from local storage.

        Loads from both cache and active directories, deduplicates,
        and returns a set of URIs. This is a port of load_service_post_uris
        from services/backfill/posts/load_data.py, decoupled from the data source.

        Args:
            service: Name of the service (e.g., "ml_inference_perspective_api")
            id_field: Name of the ID field
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            set[str]: Set of post URIs. Empty set if no data found or on error.
        """
        query = f"SELECT {id_field} FROM {service}"
        query_metadata = {"tables": [{"name": service, "columns": [id_field]}]}

        try:
            logger.info(
                f"Loading {service} post URIs from local storage "
                f"(start_date={start_date}, end_date={end_date})"
            )

            # Load from cache
            cached_df: pd.DataFrame = load_data_from_local_storage(
                service=service,
                directory="cache",
                export_format="duckdb",
                duckdb_query=query,
                query_metadata=query_metadata,
                start_partition_date=start_date,
                end_partition_date=end_date,
            )
            logger.info(f"Loaded {len(cached_df)} post URIs from cache")

            # Load from active
            active_df: pd.DataFrame = load_data_from_local_storage(
                service=service,
                directory="active",
                export_format="duckdb",
                duckdb_query=query,
                query_metadata=query_metadata,
                start_partition_date=start_date,
                end_partition_date=end_date,
            )
            logger.info(f"Loaded {len(active_df)} post URIs from active")

            # Combine and deduplicate
            df = pd.concat([cached_df, active_df]).drop_duplicates(
                subset=id_field, keep="first"
            )
            uris = set(df[id_field])

            logger.info(f"Loaded {len(uris)} unique post URIs from cache and active")
            return uris

        except Exception as e:
            logger.warning(
                f"Failed to load {service} post URIs from local storage: {e}. "
                "Returning empty set."
            )
            return set()


class S3Adapter(BackfillDataAdapter):
    """S3 adapter implementation.

    TODO: Implement S3 data loading in a future PR.
    This will mirror the LocalStorageAdapter interface but load data from S3.
    """

    def load_all_posts(
        self, start_date: str, end_date: str
    ) -> list[PostToEnqueueModel]:
        """Load all posts from S3.

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            list[PostToEnqueueModel]: List of posts.
        """
        logger.warning(
            "S3Adapter.load_all_posts() is not yet implemented. "
            "Will be implemented in a future PR."
        )
        raise NotImplementedError(
            "S3 data loading is not yet implemented. "
            "Use LocalStorageAdapter for now. "
            "S3 support will be added in a future PR."
        )

    def get_previously_labeled_post_uris(
        self,
        service: str,
        id_field: str = "uri",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> set[str]:
        """Load post URIs for a service from S3.

        Args:
            service: Name of the service (e.g., "ml_inference_perspective_api")
            id_field: Name of the ID field (default: "uri")
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)

        Returns:
            set[str]: Set of post URIs. Empty set if no data found or on error.

        Raises:
            NotImplementedError: S3 implementation not yet available.
        """
        logger.warning(
            "S3Adapter.get_previously_labeled_post_uris() is not yet implemented. "
            "Will be implemented in a future PR."
        )
        raise NotImplementedError(
            "S3 data loading is not yet implemented. "
            "Use LocalStorageAdapter for now. "
            "S3 support will be added in a future PR."
        )
