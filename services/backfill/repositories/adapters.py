"""Concrete backfill data adapter implementations."""

import pandas as pd

from lib.constants import FEED_LOOKBACK_DAYS_DURING_STUDY, study_start_date
from lib.datetime_utils import (
    calculate_start_end_date_for_lookback,
    get_partition_dates,
)
from lib.db.manage_s3_data import S3ParquetBackend, S3ParquetDatasetRef
from lib.db.manage_local_data import load_data_from_local_storage
from lib.log.logger import get_logger
from services.backfill.exceptions import BackfillDataAdapterError
from services.backfill.models import PostToEnqueueModel, PostUsedInFeedModel
from services.backfill.repositories.base import BackfillDataAdapter

logger = get_logger(__file__)

LOCAL_TABLE_NAME = "preprocessed_posts"
REQUIRED_COLUMNS = ["uri", "text", "preprocessing_timestamp"]
DEFAULT_POST_ID_FIELD = "uri"
POSTS_USED_IN_FEEDS_TABLE_NAME = "fetch_posts_used_in_feeds"


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
        filter_clause = "WHERE text IS NOT NULL AND text != ''"
        query = f"SELECT {table_columns_str} FROM {LOCAL_TABLE_NAME} {filter_clause}".strip()
        query_metadata = {
            "tables": [{"name": LOCAL_TABLE_NAME, "columns": table_columns}]
        }
        # NOTE: no need to load active_df (which we do in other parts of the
        # codebase) as our code is currently only used for backwards-looking
        # backfills (e.g., there should be nothing in active/ for us to load
        # anyways). But just a note in case it comes up in the future.
        cached_df: pd.DataFrame = load_data_from_local_storage(
            service=LOCAL_TABLE_NAME,
            storage_tiers=["cache"],
            duckdb_query=query,
            query_metadata=query_metadata,
            start_partition_date=start_date,
            end_partition_date=end_date,
        )
        dumped_posts: list[dict] = cached_df.to_dict(orient="records")
        return [PostToEnqueueModel(**post) for post in dumped_posts]

    def load_feed_posts(
        self, start_date: str, end_date: str
    ) -> list[PostToEnqueueModel]:
        """Load feed posts from local storage.

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)
        """
        results: list[PostToEnqueueModel] = []
        partition_dates: list[str] = get_partition_dates(
            start_date=start_date, end_date=end_date
        )
        for partition_date in partition_dates:
            results.extend(self._load_feed_posts_for_date(partition_date))
        deduplicated_results: list[PostToEnqueueModel] = self._deduplicate_feed_posts(
            posts=results
        )
        return deduplicated_results

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

    def _load_posts_used_in_feeds_for_date(
        self, partition_date: str
    ) -> list[PostUsedInFeedModel]:
        """Load the URIs of the posts used in the feeds for a given date."""
        query = f"SELECT {DEFAULT_POST_ID_FIELD} FROM {POSTS_USED_IN_FEEDS_TABLE_NAME}"
        query_metadata = {
            "tables": [
                {
                    "name": POSTS_USED_IN_FEEDS_TABLE_NAME,
                    "columns": [DEFAULT_POST_ID_FIELD],
                }
            ]
        }
        cached_df: pd.DataFrame = load_data_from_local_storage(
            service=POSTS_USED_IN_FEEDS_TABLE_NAME,
            storage_tiers=["cache"],
            duckdb_query=query,
            query_metadata=query_metadata,
            partition_date=partition_date,
        )
        dumped_posts: list[dict] = cached_df.to_dict(orient="records")
        return [PostUsedInFeedModel(**post) for post in dumped_posts]

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
                storage_tiers=["cache"],
                duckdb_query=query,
                query_metadata=query_metadata,
                start_partition_date=start_date,
                end_partition_date=end_date,
            )
            logger.info(f"Loaded {len(cached_df)} post URIs from cache")

            # Load from active
            active_df: pd.DataFrame = load_data_from_local_storage(
                service=service,
                storage_tiers=["active"],
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
            raise BackfillDataAdapterError(
                f"Failed to load {service} post URIs from local storage: {e}"
            ) from e

    def write_records_to_storage(self, integration_name: str, records: list[dict]):
        """Write records to storage using the local storage adapter.

        Args:
            integration_name: Name of the integration (e.g., "ml_inference_perspective_api")
            records: List of records to write.
        """
        from lib.db.manage_local_data import export_data_to_local_storage

        df = pd.DataFrame(records)
        total_records: int = len(df)

        logger.info(
            f"Exporting {total_records} records to local storage for integration {integration_name}..."
        )
        export_data_to_local_storage(
            service=integration_name,
            df=df,
            export_format="parquet",
        )
        logger.info(
            f"Finished exporting {total_records} records to local storage for integration {integration_name}..."
        )


class S3Adapter(BackfillDataAdapter):
    """S3 adapter implementation.

    Mirrors the LocalStorageAdapter interface but loads Parquet from the
    study dataset S3 layout via S3ParquetBackend + DuckDB.
    """

    def __init__(self, backend: S3ParquetBackend | None = None):
        self.backend = backend or S3ParquetBackend()

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
        table_columns = REQUIRED_COLUMNS.copy()
        table_columns_str = ", ".join(table_columns)
        filter_clause = "WHERE text IS NOT NULL AND text != ''"
        query = (
            f"SELECT {table_columns_str} FROM {LOCAL_TABLE_NAME} {filter_clause}"
        ).strip()
        query_metadata = {
            "tables": [{"name": LOCAL_TABLE_NAME, "columns": table_columns}]
        }

        df: pd.DataFrame = self.backend.query_dataset_as_df(
            dataset=S3ParquetDatasetRef(dataset=LOCAL_TABLE_NAME),
            storage_tiers=["cache"],
            start_partition_date=start_date,
            end_partition_date=end_date,
            query=query,
            query_metadata=query_metadata,
        )
        dumped_posts: list[dict] = df.to_dict(orient="records")
        return [PostToEnqueueModel(**post) for post in dumped_posts]

    def load_feed_posts(
        self, start_date: str, end_date: str
    ) -> list[PostToEnqueueModel]:
        """Load feed posts from S3.

        Args:
            start_date: Start date in YYYY-MM-DD format (inclusive)
            end_date: End date in YYYY-MM-DD format (inclusive)
        """
        logger.warning(
            "S3Adapter.load_feed_posts() is not yet implemented. "
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
        id_field: str,
        start_date: str,
        end_date: str,
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

    def write_records_to_storage(self, integration_name: str, records: list[dict]):
        """Write records to storage using the S3 adapter.

        Args:
            integration_name: Name of the integration (e.g., "ml_inference_perspective_api")
            records: List of records to write.
        """
        raise NotImplementedError(
            "S3 data writing is not yet implemented. "
            "Use LocalStorageAdapter for now. "
            "S3 support will be added in a future PR."
        )
