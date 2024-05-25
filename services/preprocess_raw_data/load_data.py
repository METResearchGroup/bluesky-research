"""Load raw data for preprocessing"""
from typing import Optional

from lib.db.bluesky_models.transformations import (
    TransformedRecordWithAuthorModel, TransformedFeedViewPostModel
)
from lib.db.mongodb import get_mongodb_collection, load_collection
from lib.db.sql.preprocessing_database import get_filtered_posts
from lib.helper import track_performance
from lib.log.logger import get_logger
from services.consolidate_post_records.helper import consolidate_post_records
from services.consolidate_post_records.models import ConsolidatedPostRecordModel  # noqa
from lib.db.sql.preprocessing_database import (
    get_previously_filtered_post_uris, load_latest_preprocessing_timestamp
)

logger = get_logger(__file__)

mongodb_task_name = "get_most_liked_posts"
mongo_collection = get_mongodb_collection(mongodb_task_name)


def load_firehose_posts(
    latest_preprocessing_timestamp: Optional[str] = None
) -> list[ConsolidatedPostRecordModel]:
    """Loads latest synced firehose posts from SQLite and then consolidates
    their format."""
    posts: list[dict] = get_filtered_posts(
        k=None, latest_preprocessing_timestamp=latest_preprocessing_timestamp
    )
    transformed_posts: list[TransformedRecordWithAuthorModel] = [
        TransformedRecordWithAuthorModel(**post) for post in posts
    ]
    return consolidate_post_records(posts=transformed_posts)


def load_feedview_posts(
    latest_preprocessing_timestamp: Optional[str] = None
) -> list[ConsolidatedPostRecordModel]:
    """Loads latest synced feedview posts from MongoDB and then consolidates
    their format."""
    posts: list[dict] = load_collection(
        collection=mongo_collection,
        limit=None,
        latest_timestamp=latest_preprocessing_timestamp,
        timestamp_fieldname="metadata.synctimestamp"
    )
    transformed_posts: list[TransformedFeedViewPostModel] = [
        TransformedFeedViewPostModel(**post) for post in posts
    ]
    return consolidate_post_records(posts=transformed_posts)


def filter_previously_preprocessed_posts(
    posts: list[ConsolidatedPostRecordModel]
) -> list[ConsolidatedPostRecordModel]:
    previous_uris: set[str] = get_previously_filtered_post_uris()
    # OK for now, and will prob be OK, but in case this doesn't scale,
    # I could explore something like a Bloom filter.
    return [post for post in posts if post.uri not in previous_uris]


@track_performance
def load_latest_raw_posts(
    sources: list[str] = ["firehose", "most_liked"]
) -> list[ConsolidatedPostRecordModel]:
    """Loads raw data from the firehose DB.
    """
    logger.info("Loading latest raw data.")
    latest_preprocessing_timestamp: str = load_latest_preprocessing_timestamp()
    consolidated_raw_posts: list[ConsolidatedPostRecordModel] = []
    for source in sources:
        if source == "firehose":
            posts: list[ConsolidatedPostRecordModel] = load_firehose_posts(
                latest_preprocessing_timestamp=latest_preprocessing_timestamp
            )
        elif source == "most_liked":
            posts: list[ConsolidatedPostRecordModel] = load_feedview_posts(
                latest_preprocessing_timestamp=latest_preprocessing_timestamp
            )
        else:
            raise ValueError(f"Data source not recognized: {source}")
        consolidated_raw_posts.extend(posts)
    consolidated_raw_posts: list[ConsolidatedPostRecordModel] = (
        filter_previously_preprocessed_posts(posts=consolidated_raw_posts)
    )
    return consolidated_raw_posts
