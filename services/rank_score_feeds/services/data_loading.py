import json

from lib.datetime_utils import TimestampFormat, calculate_lookback_datetime_str
from lib.db.data_processing import parse_converted_pandas_dicts
from services.calculate_superposters.load_data import load_latest_superposters
from services.calculate_superposters.models import CalculateSuperposterSource
from services.consolidate_enrichment_integrations.load_data import load_enriched_posts
from services.participant_data.social_network import load_user_social_network_map
from services.rank_score_feeds.config import FeedConfig
from services.rank_score_feeds.models import FeedInputData, LatestFeeds


class DataLoadingService:
    """Service for loading feed input data and latest feeds from various sources.

    Handles loading:
    - Feed input data (enriched posts, social networks, superposters)
    - Latest feeds per user from storage
    """

    def __init__(self, feed_config: FeedConfig):
        """Initialize data loading service.

        Args:
            feed_config: Configuration for feed generation algorithm.
        """
        from lib.aws.athena import Athena
        from lib.log.logger import get_logger

        self.config = feed_config
        self.athena = Athena()
        self.logger = get_logger(__name__)

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
