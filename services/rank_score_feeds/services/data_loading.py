import json

import pandas as pd

from lib.datetime_utils import TimestampFormat, calculate_lookback_datetime_str
from lib.db.data_processing import parse_converted_pandas_dicts
from lib.log.logger import get_logger
from services.calculate_superposters.load_data import load_latest_superposters
from services.calculate_superposters.models import CalculateSuperposterSource
from services.consolidate_enrichment_integrations.load_data import load_enriched_posts
from services.participant_data.helper import get_all_users
from services.participant_data.models import UserToBlueskyProfileModel
from services.participant_data.social_network import load_user_social_network_map
from services.preprocess_raw_data.classify_nsfw_content.manual_excludelist import (
    load_users_to_exclude,
)
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import (
    FeedInputData,
    LatestFeeds,
    RawFeedData,
    LoadedData,
)


class DataLoadingService:
    """Service for loading raw data from various sources.

    Handles loading:
    - Study users from participant data
    - Feed input data (enriched posts, social networks, superposters)
    - Latest feeds per user from storage
    """

    def __init__(self, feed_config: FeedConfig):
        """Initialize data loading service.

        Args:
            feed_config: Configuration for feed generation algorithm.
        """
        from lib.aws.athena import Athena

        self.config = feed_config
        self.athena = Athena()
        self.logger = get_logger(__name__)

    def load_study_users(
        self, test_mode: bool = False
    ) -> list[UserToBlueskyProfileModel]:
        """Load study users from participant data.

        Args:
            test_mode: If True, filter to test users only.

        Returns:
            List of study users.
        """
        self.logger.info(f"Loading study users (test_mode={test_mode}).")
        study_users = get_all_users(test_mode=test_mode)
        self.logger.info(f"Loaded {len(study_users)} study users.")
        return study_users

    def load_feed_input_data(self, lookback_days: int | None = None) -> FeedInputData:
        """Load feed input data from multiple services.

        Loads and returns the latest processed data from multiple services:
        - Consolidated enriched posts (filtered by lookback window)
        - User social network relationship mappings
        - Superposter DIDs for identifying high-volume authors

        Args:
            lookback_days: Number of days to look back when loading enriched posts.
                If None, uses feed_config.freshness_lookback_days.

        Returns:
            FeedInputData containing:
                - consolidate_enrichment_integrations: DataFrame of enriched posts
                - scraped_user_social_network: Mapping of user DIDs to their connection DIDs
                - superposters: Set of superposter author DIDs
        """
        if lookback_days is None:
            lookback_days = self.config.freshness_lookback_days

        self.logger.info(f"Loading feed input data with {lookback_days} day lookback.")

        lookback_datetime_str = calculate_lookback_datetime_str(
            lookback_days, format=TimestampFormat.BLUESKY
        )

        feed_input_data = FeedInputData(
            consolidate_enrichment_integrations=load_enriched_posts(
                latest_timestamp=lookback_datetime_str
            ),
            scraped_user_social_network=load_user_social_network_map(),
            superposters=load_latest_superposters(
                source=CalculateSuperposterSource.LOCAL,
                latest_timestamp=lookback_datetime_str,
            ),
        )

        self.logger.info(
            f"Loaded feed input data: {len(feed_input_data.consolidate_enrichment_integrations)} posts, "
            f"{len(feed_input_data.scraped_user_social_network)} users, "
            f"{len(feed_input_data.superposters)} superposters."
        )

        return feed_input_data

    def load_latest_feeds(self) -> LatestFeeds:
        """Loads the latest feeds per user from storage.

        Queries Athena to get the most recent feed for each user handle
        and extracts the post URIs from each feed.

        Returns:
            LatestFeeds containing a map of user handles to sets of post URIs
            from their latest feed.
        """
        self.logger.info("Loading latest feeds from storage.")

        query = """
        SELECT *
        FROM custom_feeds
        WHERE (bluesky_handle, feed_generation_timestamp) IN (
            SELECT bluesky_handle, MAX(feed_generation_timestamp)
            FROM custom_feeds
            GROUP BY bluesky_handle
        )
        """

        df = self.athena.query_results_as_df(query=query)
        df_dicts = df.to_dict(orient="records")
        df_dicts = parse_converted_pandas_dicts(df_dicts)

        feeds_dict: dict[str, set[str]] = {}
        for df_dict in df_dicts:
            handle = df_dict["bluesky_handle"]
            feed = json.loads(df_dict["feed"])
            uris = {post["item"] for post in feed}
            feeds_dict[handle] = uris

        self.logger.info(f"Loaded latest feeds for {len(feeds_dict)} users.")

        return LatestFeeds(feeds=feeds_dict)

    def load_raw_data(self, test_mode: bool = False) -> RawFeedData:
        """Load all raw data from sources.

        Loads data from all services without any transformation or filtering.
        This is the pure data loading step.

        Args:
            test_mode: If True, filter to test users only when loading study users.

        Returns:
            RawFeedData containing:
                - study_users: All study users (potentially filtered by test_mode)
                - feed_input_data: Feed input data from multiple services
                - latest_feeds: Previous feeds per user handle
        """
        study_users = self.load_study_users(test_mode=test_mode)
        feed_input_data = self.load_feed_input_data()
        latest_feeds = self.load_latest_feeds()

        return RawFeedData(
            study_users=study_users,
            feed_input_data=feed_input_data,
            latest_feeds=latest_feeds,
        )


class DataTransformationService:
    """Service for transforming and filtering loaded data.

    Handles:
    - Filtering study users by handles
    - Deduplicating and filtering posts
    - Transforming raw data to LoadedData format
    """

    def __init__(self, feed_config: FeedConfig):
        """Initialize data transformation service.

        Args:
            feed_config: Configuration for feed generation algorithm.
        """
        self.config = feed_config
        self.logger = get_logger(__name__)

    def filter_study_users(
        self,
        study_users: list[UserToBlueskyProfileModel],
        users_to_create_feeds_for: list[str] | None,
    ) -> list[UserToBlueskyProfileModel]:
        """Filter study users to only those specified by handles.

        Args:
            study_users: List of all study users to filter from.
            users_to_create_feeds_for: Optional list of user handles to filter to.
                If None, returns all study users unchanged.

        Returns:
            Filtered list of study users matching the provided handles.
        """
        if not users_to_create_feeds_for:
            return study_users

        self.logger.info(
            f"Filtering to {len(users_to_create_feeds_for)} specified users."
        )
        return [
            user
            for user in study_users
            if user.bluesky_handle in users_to_create_feeds_for
        ]

    def deduplicate_and_filter_posts(
        self, consolidated_enriched_posts_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Deduplicates and filters posts.

        Performs two operations:
        1. Deduplicates posts by URI, keeping the most recent consolidation_timestamp
        2. Filters out posts from excluded authors (by DID or handle)

        Args:
            consolidated_enriched_posts_df: DataFrame of enriched posts.

        Returns:
            Deduplicated and filtered DataFrame.
        """
        # Deduplication based on unique URIs, keeping the most recent consolidation_timestamp
        len_before = consolidated_enriched_posts_df.shape[0]
        deduplicated_df: pd.DataFrame = consolidated_enriched_posts_df.sort_values(
            by="consolidation_timestamp", ascending=False
        ).drop_duplicates(subset="uri", keep="first")
        len_after = deduplicated_df.shape[0]

        if len_before != len_after:
            self.logger.info(f"Deduplicated posts from {len_before} to {len_after}.")

        # filter out excluded authors.
        users_to_exclude: dict[str, set[str]] = load_users_to_exclude()
        bsky_handles_to_exclude: set[str] = users_to_exclude["bsky_handles_to_exclude"]
        bsky_dids_to_exclude: set[str] = users_to_exclude["bsky_dids_to_exclude"]

        # keep posts where authors are NOT in the excludelists.
        # Convert sets to lists for pandas isin() compatibility
        mask: pd.Series = ~(
            deduplicated_df["author_did"].isin(
                list(bsky_dids_to_exclude)
            )  # convert set to list for pyright check; performance diff negligible.
            | deduplicated_df["author_handle"].isin(list(bsky_handles_to_exclude))
        )
        filtered_df: pd.DataFrame = deduplicated_df[mask]  # type: ignore[assignment] # pyright: ignore[reportUnknownReturnType]
        return filtered_df

    def transform_to_loaded_data(
        self,
        raw_data: RawFeedData,
        filtered_study_users: list[UserToBlueskyProfileModel],
    ) -> LoadedData:
        """Transform raw data to LoadedData format.

        Args:
            raw_data: Raw data loaded from all sources.
            filtered_study_users: Filtered list of study users.

        Returns:
            LoadedData containing all loaded and transformed inputs.
        """
        return LoadedData(
            posts_df=raw_data.feed_input_data.consolidate_enrichment_integrations,
            user_to_social_network_map=raw_data.feed_input_data.scraped_user_social_network,
            superposter_dids=raw_data.feed_input_data.superposters,
            previous_feeds=raw_data.latest_feeds,
            study_users=filtered_study_users,
        )


class FeedDataLoader:
    """Facade for loading and transforming feed generation data.

    Coordinates DataLoadingService and DataTransformationService to provide
    a simple interface for the orchestrator. This class serves as the single
    entry point for all data loading operations.
    """

    def __init__(self, feed_config: FeedConfig):
        """Initialize feed data loader.

        Args:
            feed_config: Configuration for feed generation algorithm.
        """
        self.config = feed_config
        self.logger = get_logger(__name__)
        self.data_loading_service = DataLoadingService(feed_config=feed_config)
        self.data_transformation_service = DataTransformationService(
            feed_config=feed_config
        )

    def load_complete_data(
        self,
        test_mode: bool = False,
        users_to_create_feeds_for: list[str] | None = None,
    ) -> LoadedData:
        """Load and transform all required input data.

        This is the main entry point for loading data. It:
        1. Loads raw data from all sources
        2. Filters study users if specified
        3. Transforms data to LoadedData format

        Args:
            test_mode: If True, filter to test users only.
            users_to_create_feeds_for: Optional list of user handles to filter to.

        Returns:
            LoadedData containing all loaded and transformed inputs.
        """
        # Step 1: Load raw data
        raw_data = self.data_loading_service.load_raw_data(test_mode=test_mode)

        # Step 2: Filter users if specified
        filtered_study_users = self.data_transformation_service.filter_study_users(
            raw_data.study_users, users_to_create_feeds_for
        )

        # Step 3: Transform to LoadedData
        loaded_data = self.data_transformation_service.transform_to_loaded_data(
            raw_data, filtered_study_users
        )

        return loaded_data

    def deduplicate_and_filter_posts(self, posts_df: pd.DataFrame) -> pd.DataFrame:
        """Deduplicate and filter posts.

        Delegates to DataTransformationService for deduplication and filtering.

        Args:
            posts_df: DataFrame of posts to deduplicate and filter.

        Returns:
            Deduplicated and filtered DataFrame.
        """
        return self.data_transformation_service.deduplicate_and_filter_posts(posts_df)
